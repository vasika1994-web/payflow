.DEFAULT_GOAL := help
# порты из .env, те же что у compose
-include .env
export POSTGRES_PORT RABBITMQ_PORT RABBITMQ_MANAGEMENT_PORT API_PORT WEBHOOK_SINK_PORT
POSTGRES_PORT ?= 5433
TEST_DATABASE_URL ?= postgresql+asyncpg://postgres:password@127.0.0.1:$(POSTGRES_PORT)/payments_test
.PHONY: help up down logs restart test lint format migrate demo dlq psql clean

help: ## Показать список команд
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## Поднять всё: базу, брокер, api, consumer, relay и приёмник вебхуков
	docker compose --profile demo up --build -d
	@echo ""
	@echo "  Swagger        http://localhost:$${API_PORT:-8000}/docs   (X-API-Key: local-dev-api-key)"
	@echo "  RabbitMQ UI    http://localhost:$${RABBITMQ_MANAGEMENT_PORT:-15672}   (guest / guest)"
	@echo "  Webhook sink   http://localhost:$${WEBHOOK_SINK_PORT:-9000}/received"
	@echo ""

down: ## Остановить и убрать контейнеры вместе с данными
	docker compose --profile demo down -v

logs: ## Логи consumer и relay
	docker compose logs -f consumer outbox-relay

restart: ## Пересобрать и перезапустить приложение (без базы и брокера)
	docker compose --profile demo up --build -d api consumer outbox-relay webhook-sink

test: ## Прогнать тесты (нужна поднятая база)
	DATABASE_URL=$(TEST_DATABASE_URL) pytest -v

check-migrations: ## Убедиться, что миграции не разошлись с моделями
	DATABASE_URL=$(TEST_DATABASE_URL) alembic upgrade head && DATABASE_URL=$(TEST_DATABASE_URL) alembic check

lint: ## Проверить стиль и формат
	ruff check . && ruff format --check .

format: ## Отформатировать код
	ruff format . && ruff check --fix .

migrate: ## Накатить миграции
	alembic upgrade head

demo: ## Сквозная проверка: платёж → webhook, платёж → retry → DLQ
	bash ./scripts/demo.sh

dlq-replay: ## Вернуть сообщения из DLQ в обработку
	bash ./scripts/dlq_replay.sh

dlq: ## Показать сообщения в DLQ (не забирая их)
	@curl -s -u guest:guest -X POST "http://localhost:$${RABBITMQ_MANAGEMENT_PORT:-15672}/api/queues/%2F/payments.dlq/get" \
		-H 'Content-Type: application/json' -d '{"count":50,"ackmode":"ack_requeue_true","encoding":"auto"}' \
		| jq '.[] | {payload: (.payload | fromjson), headers: .properties.headers}'

psql: ## Открыть psql в контейнере базы
	docker compose exec postgres psql -U postgres -d payments

clean: ## Убрать временные файлы
	find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .ruff_cache \) -prune -exec rm -rf {} +
