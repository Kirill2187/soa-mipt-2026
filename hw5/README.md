# ДЗ №5 — Online Cinema: Event Streaming + Analytics Pipeline (Шаги 1–4)

Базовый ingestion pipeline для аналитики онлайн-кинотеатра: **FastAPI Producer → Kafka + Schema Registry (Avro) → ClickHouse (`Kafka Engine → Materialized View → MergeTree`)** + воспроизводимые интеграционные тесты.

## Архитектура (шаги 1–4)

```text
HTTP JSON / Generator ──▶ Movie Service (FastAPI, :8000)
                               │  1. Pydantic + Avro (Schema Registry :8081)
                               │  2. key = user_id, acks = all, exponential backoff retry
                               ▼
                     Kafka (movie-events, 3 partitions)
                               │
                               ▼
                   ClickHouse (:8123)
                   ├─ movie_events_kafka (ENGINE = Kafka, AvroConfluent)
                   ├─ movie_events_mv    (MATERIALIZED VIEW — триггер вставки)
                   └─ movie_events_raw   (ENGINE = MergeTree, хранение)
```

## Ключевые архитектурные решения

1. **Формат схемы — Avro + Confluent Schema Registry (`schemas/movie_event.avsc`):**
   - Схема регистрируется при старте в Schema Registry под субъектом `movie-events-value` (версия `1`).
   - Выбран Avro (вместо Protobuf), так как ClickHouse `Kafka Engine` нативно поддерживает формат `AvroConfluent` с автоматической загрузкой схемы из Schema Registry по `schema_id` в заголовке сообщения.
2. **Ключ партиционирования — `user_id` (3 партиции):**
   - Kafka гарантирует строгий порядок сообщений только внутри одной партиции. Использование `user_id` гарантирует, что события сессии одного пользователя (`VIEW_STARTED → VIEW_PAUSED → VIEW_RESUMED → VIEW_FINISHED`) попадают в одну партицию в хронологическом порядке.
   - В отличие от `movie_id`, распределение по `user_id` избегает эффекта **hot partition (data skew)** на премьерах популярных фильмов.
3. **ClickHouse `MergeTree` (`init/clickhouse_init.sql`):**
   - `PARTITION BY toYYYYMM(timestamp)` — помесячное партиционирование (удобное удаление старых партиций без чрезмерного числа parts).
   - `ORDER BY (toDate(timestamp), event_type, user_id, timestamp)` — оптимизировано под будущие дневные запросы DAU, конверсии, среднего времени просмотра и когортного retention.
   - `LowCardinality(String)` для `event_type` и `device_type` даёт словарное сжатие для перечислимых полей.

## Запуск и проверка

Поднять весь стек (топик, схема в Schema Registry и таблицы ClickHouse создаются автоматически):

```bash
docker compose up -d --build
# или: make up
```

Запустить интеграционные тесты одной командой (можно запускать многократно — тестовые данные изолированы и очищаются):

```bash
docker compose --profile test run --rm --build tests
# или: make test
```

Примеры ручных запросов к продюсеру:

```bash
# Отправить одно событие
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_42",
    "movie_id": "movie_interstellar",
    "event_type": "VIEW_STARTED",
    "device_type": "TV",
    "session_id": "sess_1",
    "progress_seconds": 0
  }'

# Сгенерировать 5 реалистичных пользовательских сессий
curl -X POST "http://localhost:8000/generate?sessions=5"
```

Остановить стек:

```bash
docker compose down -v
```
