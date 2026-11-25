import json
import os
import sys
import time
import urllib.request
from pathlib import Path

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
SUBJECT = os.getenv("SCHEMA_SUBJECT", "movie-events-value")
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", "/schemas/movie_event.avsc"))


def wait_for_registry(timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=3) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Schema Registry at {SCHEMA_REGISTRY_URL} is not ready")


def register_schema() -> None:
    schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    json.loads(schema_text)

    payload = json.dumps({"schemaType": "AVRO", "schema": schema_text}).encode("utf-8")
    req = urllib.request.Request(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        reg = json.loads(resp.read().decode("utf-8"))

    with urllib.request.urlopen(f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest", timeout=10) as resp:
        latest = json.loads(resp.read().decode("utf-8"))

    print(f"Registered subject='{SUBJECT}' schema_id={reg['id']} version={latest['version']}", flush=True)


if __name__ == "__main__":
    try:
        wait_for_registry()
        register_schema()
    except Exception as exc:
        print(f"Failed to register schema: {exc}", file=sys.stderr)
        sys.exit(1)
