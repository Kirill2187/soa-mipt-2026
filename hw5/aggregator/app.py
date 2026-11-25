from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import httpx
import psycopg
from fastapi import FastAPI, HTTPException, Query

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [aggregator] %(message)s",
)
logger = logging.getLogger("aggregator")

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://analytics:analytics@postgres:5432/analytics")
POSTGRES_INIT_SQL = Path(os.getenv("POSTGRES_INIT_SQL", "/init/postgres_init.sql"))

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")

AGGREGATION_INTERVAL_SECONDS = float(os.getenv("AGGREGATION_INTERVAL_SECONDS", "30"))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "1.0"))


def with_retry(action_name: str, fn):
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning("%s failed (attempt %d/%d): %s; retrying in %.1fs", action_name, attempt, RETRY_ATTEMPTS, exc, delay)
                time.sleep(delay)
            else:
                logger.error("%s failed after %d attempts: %s", action_name, RETRY_ATTEMPTS, exc)
    assert last_exc is not None
    raise last_exc


def ch_query(sql: str) -> list[dict[str, Any]]:
    resp = httpx.post(
        CLICKHOUSE_URL,
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        content=f"{sql} FORMAT JSONEachRow".encode("utf-8"),
        timeout=20.0,
    )
    resp.raise_for_status()
    return [json.loads(line) for line in resp.text.strip().splitlines() if line]


def ch_exec(sql: str) -> None:
    resp = httpx.post(
        CLICKHOUSE_URL,
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        content=sql.encode("utf-8"),
        timeout=20.0,
    )
    resp.raise_for_status()


def init_postgres_schema() -> None:
    if not POSTGRES_INIT_SQL.exists():
        return
    ddl = POSTGRES_INIT_SQL.read_text(encoding="utf-8")

    def _apply() -> None:
        with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
            conn.execute(ddl)

    with_retry("postgres schema migration", _apply)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_s3_bucket() -> None:
    def _ensure() -> None:
        s3 = get_s3_client()
        existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
        if S3_BUCKET not in existing:
            s3.create_bucket(Bucket=S3_BUCKET)
            logger.info("Created S3 bucket '%s'", S3_BUCKET)

    with_retry(f"s3 ensure bucket ({S3_BUCKET})", _ensure)


def refresh_retention_cohorts(computed_at_str: str) -> None:
    sql = f"""
    INSERT INTO analytics.retention_cohorts
        (cohort_date, day_num, cohort_size, returned_users, retention_pct, computed_at)
    WITH
        user_days AS (
            SELECT DISTINCT user_id, toDate(timestamp) AS activity_date
            FROM analytics.movie_events_raw
            WHERE event_type IN ('VIEW_STARTED', 'VIEW_FINISHED', 'VIEW_PAUSED', 'VIEW_RESUMED')
        ),
        user_cohorts AS (
            SELECT user_id, min(activity_date) AS cohort_date
            FROM user_days
            GROUP BY user_id
        ),
        cohort_sizes AS (
            SELECT cohort_date, uniq(user_id) AS cohort_size
            FROM user_cohorts
            GROUP BY cohort_date
        )
    SELECT
        c.cohort_date,
        toUInt8(dateDiff('day', c.cohort_date, d.activity_date)) AS day_num,
        any(s.cohort_size) AS cohort_size,
        uniq(d.user_id) AS returned_users,
        round(uniq(d.user_id) * 100.0 / nullIf(any(s.cohort_size), 0), 2) AS retention_pct,
        parseDateTime64BestEffort('{computed_at_str}', 3, 'UTC') AS computed_at
    FROM user_cohorts AS c
    INNER JOIN user_days AS d ON d.user_id = c.user_id
    INNER JOIN cohort_sizes AS s ON s.cohort_date = c.cohort_date
    WHERE dateDiff('day', c.cohort_date, d.activity_date) BETWEEN 0 AND 7
    GROUP BY c.cohort_date, day_num
    """
    ch_exec(sql)


def run_aggregation(target_date: date) -> dict[str, Any]:
    t0 = time.perf_counter()
    date_str = target_date.isoformat()
    computed_at = datetime.now(timezone.utc)
    computed_at_ch = computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    logger.info("Aggregation cycle started for date=%s", date_str)
    refresh_retention_cohorts(computed_at_ch)

    stats_rows = ch_query(
        f"""
        SELECT
            count() AS total_events,
            uniq(user_id) AS dau,
            coalesce(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 0.0) AS avg_watch_seconds,
            countIf(event_type = 'VIEW_STARTED') AS view_started,
            countIf(event_type = 'VIEW_FINISHED') AS view_finished,
            coalesce(
                countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0),
                0.0
            ) AS view_conversion
        FROM analytics.movie_events_raw
        WHERE toDate(timestamp) = toDate('{date_str}')
        """
    )
    stats = stats_rows[0] if stats_rows else {}
    total_events = int(stats.get("total_events", 0))
    dau = int(stats.get("dau", 0))
    avg_watch_seconds = round(float(stats.get("avg_watch_seconds", 0.0)), 2)
    view_started = int(stats.get("view_started", 0))
    view_finished = int(stats.get("view_finished", 0))
    view_conversion = round(float(stats.get("view_conversion", 0.0)), 4)

    ret_rows = ch_query(
        f"""
        SELECT day_num, retention_pct
        FROM analytics.retention_cohorts FINAL
        WHERE cohort_date = toDate('{date_str}') AND day_num IN (1, 7)
        """
    )
    ret_map = {int(r["day_num"]): round(float(r["retention_pct"]) / 100.0, 4) for r in ret_rows}
    retention_d1 = ret_map.get(1, 0.0)
    retention_d7 = ret_map.get(7, 0.0)

    ch_exec(
        f"""
        INSERT INTO analytics.daily_metrics
            (metric_date, dau, avg_watch_seconds, view_started, view_finished,
             view_conversion, retention_d1, retention_d7, computed_at)
        VALUES (
            toDate('{date_str}'), {dau}, {avg_watch_seconds}, {view_started}, {view_finished},
            {view_conversion}, {retention_d1}, {retention_d7},
            parseDateTime64BestEffort('{computed_at_ch}', 3, 'UTC')
        )
        """
    )

    top_movies = ch_query(
        f"""
        SELECT
            movie_id,
            countIf(event_type = 'VIEW_STARTED') AS views_count,
            countIf(event_type = 'VIEW_FINISHED') AS finished_count,
            coalesce(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 0.0) AS avg_watch_seconds
        FROM analytics.movie_events_raw
        WHERE toDate(timestamp) = toDate('{date_str}')
        GROUP BY movie_id
        HAVING views_count > 0
        ORDER BY views_count DESC, movie_id ASC
        LIMIT 20
        """
    )
    if top_movies:
        ch_exec(
            f"""
            INSERT INTO analytics.daily_top_movies
                (metric_date, movie_id, views_count, finished_count, avg_watch_seconds, computed_at)
            SELECT
                toDate('{date_str}'),
                movie_id,
                countIf(event_type = 'VIEW_STARTED') AS views_count,
                countIf(event_type = 'VIEW_FINISHED') AS finished_count,
                coalesce(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 0.0) AS avg_watch_seconds,
                parseDateTime64BestEffort('{computed_at_ch}', 3, 'UTC')
            FROM analytics.movie_events_raw
            WHERE toDate(timestamp) = toDate('{date_str}')
            GROUP BY movie_id
            HAVING views_count > 0
            """
        )

    metrics_dict = {
        "dau": float(dau),
        "avg_watch_seconds": avg_watch_seconds,
        "view_conversion": view_conversion,
        "retention_d1": retention_d1,
        "retention_d7": retention_d7,
    }

    def _upsert_postgres() -> None:
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                for name, value in metrics_dict.items():
                    cur.execute(
                        """
                        INSERT INTO daily_metrics (metric_date, metric_name, metric_value, computed_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (metric_date, metric_name)
                        DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = EXCLUDED.computed_at
                        """,
                        (target_date, name, value, computed_at),
                    )
                for rank, item in enumerate(top_movies, start=1):
                    cur.execute(
                        """
                        INSERT INTO daily_top_movies (metric_date, movie_id, views_count, rank, computed_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (metric_date, movie_id)
                        DO UPDATE SET
                            views_count = EXCLUDED.views_count,
                            rank = EXCLUDED.rank,
                            computed_at = EXCLUDED.computed_at
                        """,
                        (target_date, item["movie_id"], int(item["views_count"]), rank, computed_at),
                    )
            conn.commit()

    with_retry(f"postgres upsert ({date_str})", _upsert_postgres)

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
    logger.info(
        "Aggregation cycle finished for date=%s processed_records=%d dau=%d conversion=%.4f elapsed_ms=%.1f",
        date_str,
        total_events,
        dau,
        view_conversion,
        elapsed_ms,
    )
    return {
        "date": date_str,
        "processed_records": total_events,
        "elapsed_ms": elapsed_ms,
        "computed_at": computed_at.isoformat(),
        "metrics": metrics_dict,
        "top_movies_count": len(top_movies),
    }


def run_s3_export(target_date: date) -> dict[str, Any]:
    date_str = target_date.isoformat()
    key = f"daily/{date_str}/aggregates.json"
    logger.info("S3 export started for date=%s -> s3://%s/%s", date_str, S3_BUCKET, key)

    def _read_pg() -> tuple[dict[str, float], list[dict[str, Any]], str | None]:
        with psycopg.connect(POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT metric_name, metric_value, computed_at FROM daily_metrics WHERE metric_date = %s ORDER BY metric_name",
                    (target_date,),
                )
                m_rows = cur.fetchall()
                cur.execute(
                    "SELECT rank, movie_id, views_count FROM daily_top_movies WHERE metric_date = %s ORDER BY rank ASC",
                    (target_date,),
                )
                t_rows = cur.fetchall()
        metrics = {r[0]: float(r[1]) for r in m_rows}
        computed_at = max((r[2] for r in m_rows), default=None)
        top_movies = [{"rank": r[0], "movie_id": r[1], "views_count": r[2]} for r in t_rows]
        return metrics, top_movies, computed_at.isoformat() if computed_at else None

    metrics, top_movies, computed_at = with_retry(f"postgres read for export ({date_str})", _read_pg)

    payload = {
        "date": date_str,
        "computed_at": computed_at or datetime.now(timezone.utc).isoformat(),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics,
        "top_movies": top_movies,
    }
    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")

    def _upload() -> None:
        get_s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")

    with_retry(f"s3 put_object ({key})", _upload)
    logger.info("S3 export finished: s3://%s/%s (%d bytes)", S3_BUCKET, key, len(body))
    return {"s3_uri": f"s3://{S3_BUCKET}/{key}", "bytes": len(body), "payload": payload}


def run_scheduled_cycle() -> None:
    date_rows = ch_query(
        """
        SELECT DISTINCT toString(toDate(timestamp)) AS d
        FROM analytics.movie_events_raw
        WHERE toDate(timestamp) >= today() - 14
        ORDER BY d ASC
        """
    )
    dates = [date.fromisoformat(r["d"]) for r in date_rows]
    today = datetime.now(timezone.utc).date()
    if today not in dates:
        dates.append(today)

    for d in dates:
        try:
            run_aggregation(d)
            run_s3_export(d)
        except Exception as exc:
            logger.error("Scheduled cycle failed for date=%s: %s", d, exc)


_scheduler_task: asyncio.Task[None] | None = None


async def _scheduler_loop() -> None:
    logger.info("Aggregation scheduler started (interval=%.1fs)", AGGREGATION_INTERVAL_SECONDS)
    await asyncio.sleep(5)
    while True:
        try:
            await asyncio.to_thread(run_scheduled_cycle)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Scheduler loop error: %s", exc)
        await asyncio.sleep(AGGREGATION_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _scheduler_task
    init_postgres_schema()
    ensure_s3_bucket()
    _scheduler_task = asyncio.create_task(_scheduler_loop())
    yield
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Aggregation & Export Service", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/aggregate")
def aggregate_manual(target_date: date | None = Query(default=None, alias="date")) -> dict[str, Any]:
    d = target_date or datetime.now(timezone.utc).date()
    try:
        return run_aggregation(d)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/export")
def export_manual(target_date: date | None = Query(default=None, alias="date")) -> dict[str, Any]:
    d = target_date or datetime.now(timezone.utc).date()
    try:
        return run_s3_export(d)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
