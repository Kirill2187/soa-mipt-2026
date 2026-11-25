"""Integration tests for the Online Cinema pipeline (Producer -> Kafka -> ClickHouse)."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000").rstrip("/")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123").rstrip("/")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")


def ch_query(sql: str) -> list[dict]:
    """Execute a SELECT in ClickHouse over HTTP and return parsed JSONEachRow rows."""
    resp = httpx.post(
        CLICKHOUSE_URL,
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        content=f"{sql} FORMAT JSONEachRow".encode("utf-8"),
        timeout=10.0,
    )
    resp.raise_for_status()
    lines = [line for line in resp.text.strip().splitlines() if line]
    return [json.loads(line) for line in lines]


def ch_exec(sql: str) -> None:
    """Execute a DDL/DML statement in ClickHouse over HTTP."""
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
    """Step 1: Verify Avro schema is registered in Schema Registry with version >= 1."""
    resp = httpx.get(f"{SCHEMA_REGISTRY_URL}/subjects/movie-events-value/versions/latest", timeout=5.0)
    assert resp.status_code == 200
    data = resp.json()
    assert data["version"] >= 1
    schema = json.loads(data["schema"])
    field_names = {f["name"] for f in schema["fields"]}
    assert field_names == {
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
    """Step 2: Producer HTTP API validates incoming JSON and rejects invalid enum/fields."""
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
    """Step 4: Publish an event via Producer HTTP API and verify all 8 fields in ClickHouse."""
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
        assert len(rows) == 1, f"Event {event_id} did not arrive in ClickHouse"

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
        # Keep the database clean so the test can be repeated indefinitely
        ch_exec(
            f"ALTER TABLE analytics.movie_events_raw "
            f"DELETE WHERE user_id = '{user_id}' SETTINGS mutations_sync = 1"
        )


def test_realistic_session_generator_ordering() -> None:
    """Step 2 & 4: Generated session events arrive in ClickHouse with realistic ordering."""
    user_id = f"test-gen-{uuid4().hex[:10]}"
    try:
        resp = httpx.post(
            f"{PRODUCER_URL}/generate",
            params={"sessions": 1, "user_id": user_id},
            timeout=20.0,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        expected_n = body["events_published"]
        assert expected_n >= 1

        rows = wait_for_rows(
            f"SELECT event_type, progress_seconds, toUnixTimestamp64Milli(timestamp) AS ts_ms "
            f"FROM analytics.movie_events_raw "
            f"WHERE user_id = '{user_id}' "
            f"ORDER BY timestamp ASC",
            expected_count=expected_n,
            timeout=20.0,
        )
        assert len(rows) == expected_n

        types = [r["event_type"] for r in rows]
        assert "VIEW_STARTED" in types
        if "VIEW_FINISHED" in types:
            assert types.index("VIEW_STARTED") < types.index("VIEW_FINISHED")

        # Progress should be non-decreasing across playback events
        progresses = [int(r["progress_seconds"]) for r in rows]
        assert progresses == sorted(progresses)
    finally:
        ch_exec(
            f"ALTER TABLE analytics.movie_events_raw "
            f"DELETE WHERE user_id = '{user_id}' SETTINGS mutations_sync = 1"
        )
