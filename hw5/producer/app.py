"""Movie Service (Producer): HTTP API + synthetic realistic session generator."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [producer] %(message)s",
)
logger = logging.getLogger("producer")

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", "/schemas/movie_event.avsc"))

MAX_PUBLISH_RETRIES = int(os.getenv("MAX_PUBLISH_RETRIES", "4"))
BASE_BACKOFF_SECONDS = float(os.getenv("BASE_BACKOFF_SECONDS", "0.5"))

GENERATOR_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
GENERATOR_INTERVAL_SECONDS = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "5.0"))

SAMPLE_MOVIES = [
    "movie_interstellar",
    "movie_dune_2",
    "movie_inception",
    "movie_matrix",
    "movie_arrival",
    "movie_blade_runner",
    "movie_tenet",
    "movie_oppenheimer",
]
SAMPLE_USERS = [f"user_{i:03d}" for i in range(1, 51)]


# --- Pydantic Event Schema (matches schemas/movie_event.avsc) ---
class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(..., min_length=1)
    movie_id: str = Field(..., min_length=1)
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    device_type: DeviceType
    session_id: str = Field(..., min_length=1)
    progress_seconds: int = Field(default=0, ge=0)

    def to_avro_dict(self) -> dict[str, Any]:
        ts = self.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": ts,
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


# --- Kafka + Schema Registry Producer ---
class KafkaEventPublisher:
    def __init__(self) -> None:
        schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
        self.schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        # auto.register.schemas=True keeps startup resilient if Schema Registry restarted;
        # the schema is already registered by schema-init as version 1.
        self.avro_serializer = AvroSerializer(
            self.schema_registry,
            schema_str,
            conf={"auto.register.schemas": True},
        )
        # acks='all' waits for leader + all in-sync replicas before acknowledging.
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 3,
            }
        )

    def publish(self, event: MovieEvent) -> dict[str, Any]:
        # 1. Validate and encode against the Avro schema from Schema Registry
        ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
        payload = self.avro_serializer(event.to_avro_dict(), ctx)

        # Partition key = user_id -> guarantees strict event order per user session
        key = event.user_id.encode("utf-8")

        # 2. Publish with explicit exponential backoff retries
        last_error: str | None = None
        for attempt in range(1, MAX_PUBLISH_RETRIES + 1):
            delivery_error: Any = None
            delivery_meta: dict[str, int] = {}

            def on_delivery(err: Any, msg: Any) -> None:
                nonlocal delivery_error, delivery_meta
                if err is not None:
                    delivery_error = err
                else:
                    delivery_meta = {"partition": msg.partition(), "offset": msg.offset()}

            try:
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=payload,
                    on_delivery=on_delivery,
                )
                remaining = self.producer.flush(timeout=10.0)
                if remaining == 0 and delivery_error is None:
                    logger.info(
                        "Published event_id=%s event_type=%s timestamp=%s user_id=%s partition=%s offset=%s",
                        event.event_id,
                        event.event_type.value,
                        event.timestamp.isoformat(),
                        event.user_id,
                        delivery_meta.get("partition"),
                        delivery_meta.get("offset"),
                    )
                    return delivery_meta
                last_error = str(delivery_error or "Kafka flush timed out")
            except Exception as exc:
                last_error = str(exc)

            if attempt < MAX_PUBLISH_RETRIES:
                delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Publish failed for event_id=%s (attempt %d/%d): %s. Retrying in %.2fs...",
                    event.event_id,
                    attempt,
                    MAX_PUBLISH_RETRIES,
                    last_error,
                    delay,
                )
                time.sleep(delay)

        logger.error("Failed to publish event_id=%s after %d attempts: %s", event.event_id, MAX_PUBLISH_RETRIES, last_error)
        raise RuntimeError(f"Kafka publish failed: {last_error}")


# --- Realistic Synthetic Session Generator ---
def build_realistic_session(
    user_id: str | None = None,
    movie_id: str | None = None,
    base_time: datetime | None = None,
) -> list[MovieEvent]:
    """Generate a realistic viewing session with strictly increasing timestamps and progress."""
    uid = user_id or random.choice(SAMPLE_USERS)
    mid = movie_id or random.choice(SAMPLE_MOVIES)
    device = random.choice(list(DeviceType))
    session_id = f"sess-{uuid4().hex[:12]}"
    t = base_time or datetime.now(timezone.utc)

    events: list[MovieEvent] = []

    # Optional search before starting playback
    if random.random() < 0.5:
        events.append(
            MovieEvent(
                user_id=uid,
                movie_id=mid,
                event_type=EventType.SEARCHED,
                timestamp=t,
                device_type=device,
                session_id=session_id,
                progress_seconds=0,
            )
        )
        t += timedelta(seconds=random.randint(5, 30))

    # VIEW_STARTED always precedes pauses/resumes/finish
    events.append(
        MovieEvent(
            user_id=uid,
            movie_id=mid,
            event_type=EventType.VIEW_STARTED,
            timestamp=t,
            device_type=device,
            session_id=session_id,
            progress_seconds=0,
        )
    )

    progress = 0
    # 0 to 2 pause/resume cycles with monotonically growing progress_seconds
    for _ in range(random.randint(0, 2)):
        watched = random.randint(180, 900)
        progress += watched
        t += timedelta(seconds=watched)
        events.append(
            MovieEvent(
                user_id=uid,
                movie_id=mid,
                event_type=EventType.VIEW_PAUSED,
                timestamp=t,
                device_type=device,
                session_id=session_id,
                progress_seconds=progress,
            )
        )
        t += timedelta(seconds=random.randint(15, 120))
        events.append(
            MovieEvent(
                user_id=uid,
                movie_id=mid,
                event_type=EventType.VIEW_RESUMED,
                timestamp=t,
                device_type=device,
                session_id=session_id,
                progress_seconds=progress,
            )
        )

    # ~70% of sessions finish the movie
    if random.random() < 0.7:
        remaining = random.randint(300, 1800)
        progress += remaining
        t += timedelta(seconds=remaining)
        events.append(
            MovieEvent(
                user_id=uid,
                movie_id=mid,
                event_type=EventType.VIEW_FINISHED,
                timestamp=t,
                device_type=device,
                session_id=session_id,
                progress_seconds=progress,
            )
        )
        if random.random() < 0.5:
            t += timedelta(seconds=random.randint(2, 20))
            events.append(
                MovieEvent(
                    user_id=uid,
                    movie_id=mid,
                    event_type=EventType.LIKED,
                    timestamp=t,
                    device_type=device,
                    session_id=session_id,
                    progress_seconds=progress,
                )
            )

    return events


publisher: KafkaEventPublisher | None = None
_generator_task: asyncio.Task[None] | None = None


async def _background_generator() -> None:
    logger.info("Background event generator started (interval=%.1fs)", GENERATOR_INTERVAL_SECONDS)
    while True:
        try:
            session_events = build_realistic_session()
            for ev in session_events:
                assert publisher is not None
                await asyncio.to_thread(publisher.publish, ev)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Background generator error: %s", exc)
        await asyncio.sleep(GENERATOR_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global publisher, _generator_task
    publisher = KafkaEventPublisher()
    if GENERATOR_ENABLED:
        _generator_task = asyncio.create_task(_background_generator())
    yield
    if _generator_task is not None:
        _generator_task.cancel()
        try:
            await _generator_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Online Cinema Movie Service (Producer)", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
def ingest_event(event: MovieEvent) -> dict[str, Any]:
    """Validate an incoming JSON event and publish it to Kafka."""
    assert publisher is not None
    try:
        meta = publisher.publish(event)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "event_id": str(event.event_id),
        "partition": meta["partition"],
        "offset": meta["offset"],
    }


@app.post("/generate")
def generate_sessions(
    sessions: int = Query(default=1, ge=1, le=200),
    user_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """Generate N realistic user sessions on demand and publish them to Kafka."""
    assert publisher is not None
    event_ids: list[str] = []
    session_ids: list[str] = []
    for _ in range(sessions):
        batch = build_realistic_session(user_id=user_id)
        session_ids.append(batch[0].session_id)
        for ev in batch:
            publisher.publish(ev)
            event_ids.append(str(ev.event_id))
    return {
        "sessions_generated": sessions,
        "events_published": len(event_ids),
        "session_ids": session_ids,
        "event_ids": event_ids,
    }
