from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import boto3
import httpx
import psycopg

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000").rstrip("/")
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8001").rstrip("/")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://analytics:analytics@localhost:5432/analytics")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9005")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000").rstrip("/")


def ch_query(sql: str) -> list[dict]:
    resp = httpx.post(
        CLICKHOUSE_URL,
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        content=f"{sql} FORMAT JSONEachRow".encode("utf-8"),
        timeout=15.0,
    )
    resp.raise_for_status()
    return [json.loads(line) for line in resp.text.strip().splitlines() if line]


def ch_exec(sql: str) -> None:
    resp = httpx.post(
        CLICKHOUSE_URL,
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        content=sql.encode("utf-8"),
        timeout=15.0,
    )
    resp.raise_for_status()


def wait_for_rows(sql: str, expected_count: int = 1, timeout: float = 20.0) -> list[dict]:
    deadline = time.time() + timeout
    rows: list[dict] = []
    while time.time() < deadline:
        rows = ch_query(sql)
        if len(rows) >= expected_count:
            return rows
        time.sleep(0.5)
    return rows


def test_schema_registered_in_schema_registry() -> None:
    resp = httpx.get(f"{SCHEMA_REGISTRY_URL}/subjects/movie-events-value/versions/latest", timeout=5.0)
    assert resp.status_code == 200
    data = resp.json()
    assert data["version"] >= 1
    schema = json.loads(data["schema"])
    assert {f["name"] for f in schema["fields"]} == {
        "event_id",
        "user_id",
        "movie_id",
        "event_type",
        "timestamp",
        "device_type",
        "session_id",
        "progress_seconds",
    }


def test_producer_rejects_invalid_event() -> None:
    bad_payload = {
        "user_id": "test-invalid",
        "movie_id": "movie_1",
        "event_type": "NOT_A_VALID_TYPE",
        "device_type": "TV",
        "session_id": "sess-bad",
        "progress_seconds": -5,
    }
    resp = httpx.post(f"{PRODUCER_URL}/events", json=bad_payload, timeout=5.0)
    assert resp.status_code == 422


def test_event_flows_from_producer_through_kafka_to_clickhouse() -> None:
    event_id = str(uuid4())
    user_id = f"test-user-{uuid4().hex[:10]}"
    session_id = f"test-sess-{uuid4().hex[:8]}"
    ts = datetime(2026, 11, 25, 12, 34, 56, 123000, tzinfo=timezone.utc)

    payload = {
        "event_id": event_id,
        "user_id": user_id,
        "movie_id": "movie_interstellar",
        "event_type": "VIEW_FINISHED",
        "timestamp": ts.isoformat(),
        "device_type": "TV",
        "session_id": session_id,
        "progress_seconds": 5432,
    }

    try:
        resp = httpx.post(f"{PRODUCER_URL}/events", json=payload, timeout=15.0)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["event_id"] == event_id
        assert body["partition"] in (0, 1, 2)

        rows = wait_for_rows(
            f"SELECT event_id, user_id, movie_id, event_type, "
            f"toUnixTimestamp64Milli(timestamp) AS ts_ms, "
            f"device_type, session_id, progress_seconds "
            f"FROM analytics.movie_events_raw "
            f"WHERE event_id = toUUID('{event_id}')",
            expected_count=1,
            timeout=20.0,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["event_id"] == event_id
        assert row["user_id"] == user_id
        assert row["movie_id"] == "movie_interstellar"
        assert row["event_type"] == "VIEW_FINISHED"
        assert int(row["ts_ms"]) == int(ts.timestamp() * 1000)
        assert row["device_type"] == "TV"
        assert row["session_id"] == session_id
        assert int(row["progress_seconds"]) == 5432
    finally:
        ch_exec(
            f"ALTER TABLE analytics.movie_events_raw "
            f"DELETE WHERE user_id = '{user_id}' SETTINGS mutations_sync = 1"
        )


def test_realistic_session_generator_ordering() -> None:
    user_id = f"test-gen-{uuid4().hex[:10]}"
    try:
        resp = httpx.post(f"{PRODUCER_URL}/generate", params={"sessions": 1, "user_id": user_id}, timeout=20.0)
        assert resp.status_code == 200, resp.text
        expected_n = resp.json()["events_published"]

        rows = wait_for_rows(
            f"SELECT event_type, progress_seconds "
            f"FROM analytics.movie_events_raw "
            f"WHERE user_id = '{user_id}' ORDER BY timestamp ASC",
            expected_count=expected_n,
            timeout=20.0,
        )
        assert len(rows) == expected_n
        types = [r["event_type"] for r in rows]
        assert "VIEW_STARTED" in types
        if "VIEW_FINISHED" in types:
            assert types.index("VIEW_STARTED") < types.index("VIEW_FINISHED")
        progresses = [int(r["progress_seconds"]) for r in rows]
        assert progresses == sorted(progresses)
    finally:
        ch_exec(
            f"ALTER TABLE analytics.movie_events_raw "
            f"DELETE WHERE user_id = '{user_id}' SETTINGS mutations_sync = 1"
        )


def test_aggregation_and_postgres_idempotency() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    wait_for_rows(
        f"SELECT 1 FROM analytics.movie_events_raw WHERE toDate(timestamp) = toDate('{today}') AND event_type = 'VIEW_STARTED' LIMIT 1",
        expected_count=1,
        timeout=20.0,
    )

    r1 = httpx.post(f"{AGGREGATOR_URL}/aggregate", params={"date": today}, timeout=30.0)
    assert r1.status_code == 200, r1.text
    r2 = httpx.post(f"{AGGREGATOR_URL}/aggregate", params={"date": today}, timeout=30.0)
    assert r2.status_code == 200, r2.text

    expected_metrics = {"dau", "avg_watch_seconds", "view_conversion", "retention_d1", "retention_d7"}
    assert set(r2.json()["metrics"].keys()) == expected_metrics

    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT metric_name, metric_value, computed_at FROM daily_metrics WHERE metric_date = %s",
                (today,),
            )
            pg_rows = cur.fetchall()
            cur.execute(
                "SELECT movie_id, views_count, rank FROM daily_top_movies WHERE metric_date = %s",
                (today,),
            )
            top_rows = cur.fetchall()

    assert len(pg_rows) == 5
    assert {r[0] for r in pg_rows} == expected_metrics
    assert all(r[2] is not None for r in pg_rows)
    assert len(top_rows) >= 1

    ch_daily = ch_query(f"SELECT dau FROM analytics.daily_metrics FINAL WHERE metric_date = toDate('{today}')")
    ch_cohorts = ch_query("SELECT count() AS cnt FROM analytics.retention_cohorts FINAL")
    assert len(ch_daily) == 1
    assert int(ch_cohorts[0]["cnt"]) >= 1


def test_s3_export_and_overwrite() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    key = f"daily/{today}/aggregates.json"

    r1 = httpx.post(f"{AGGREGATOR_URL}/export", params={"date": today}, timeout=30.0)
    assert r1.status_code == 200, r1.text
    r2 = httpx.post(f"{AGGREGATOR_URL}/export", params={"date": today}, timeout=30.0)
    assert r2.status_code == 200, r2.text

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )
    listed = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"daily/{today}/")
    assert [obj["Key"] for obj in listed.get("Contents", [])] == [key]

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    assert data["date"] == today
    assert "computed_at" in data
    assert {"dau", "avg_watch_seconds", "view_conversion", "retention_d1", "retention_d7"} <= set(
        data["metrics"].keys()
    )


def test_grafana_clickhouse_datasource_and_dashboard() -> None:
    auth = ("admin", "admin")

    ds_resp = httpx.get(f"{GRAFANA_URL}/api/datasources", auth=auth, timeout=10.0)
    assert ds_resp.status_code == 200
    assert any(
        ds["uid"] == "clickhouse" and ds["type"] == "grafana-clickhouse-datasource"
        for ds in ds_resp.json()
    )

    dash_resp = httpx.get(f"{GRAFANA_URL}/api/dashboards/uid/online-cinema-analytics", auth=auth, timeout=10.0)
    assert dash_resp.status_code == 200
    panels = dash_resp.json()["dashboard"]["panels"]
    assert any("Retention Cohort Heatmap" in p["title"] for p in panels)
    assert len(panels) >= 3

    query_resp = httpx.post(
        f"{GRAFANA_URL}/api/ds/query",
        auth=auth,
        json={
            "queries": [
                {
                    "refId": "A",
                    "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"},
                    "editorType": "sql",
                    "format": 1,
                    "queryType": "table",
                    "rawSql": (
                        "SELECT toString(cohort_date) AS cohort, "
                        "maxIf(retention_pct, day_num = 0) AS day_0 "
                        "FROM analytics.retention_cohorts FINAL "
                        "GROUP BY cohort_date ORDER BY cohort_date DESC"
                    ),
                }
            ]
        },
        timeout=15.0,
    )
    assert query_resp.status_code == 200, query_resp.text
    frame = query_resp.json()["results"]["A"]["frames"][0]
    assert len(frame["data"]["values"][0]) >= 1
