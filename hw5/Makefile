.PHONY: up down test logs clean

up:
	docker compose up -d --build

test:
	docker compose --profile test run --rm --build tests

logs:
	docker compose logs -f --tail=100

down:
	docker compose down

clean:
	docker compose down -v
