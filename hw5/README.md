# HW5 — Online Cinema Analytics Pipeline

## Запуск и тесты

```bash
docker compose up -d --build
make test
```

Сервисы после запуска:
- Producer API: `http://localhost:8000/docs`
- Aggregator & Export API: `http://localhost:8001/docs`
- Grafana (`admin`/`admin`): `http://localhost:3000` (дашборд **Online Cinema Analytics**)
- S3 Console (`minioadmin`/`minioadmin`): `http://localhost:9001` (бакет `movie-analytics`)
