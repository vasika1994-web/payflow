# Асинхронный сервис процессинга платежей

Принимает запросы на оплату, обрабатывает их через платёжный шлюз (эмуляция) и уведомляет
клиента о результате через webhook.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ (FastStream), Alembic,
Docker Compose.

## Запуск

```bash
docker compose --profile demo up --build
```

Профиль `demo` добавляет приёмник вебхуков, чтобы было куда слать уведомления. Без него
поднимаются postgres, rabbitmq, api, consumer и outbox-relay.

| Что | Адрес |
|---|---|
| Swagger | http://localhost:8000/docs (ключ уже подставлен) |
| API | http://localhost:8000, заголовок `X-API-Key: local-dev-api-key` |
| RabbitMQ management | http://localhost:15672 (guest / guest) |
| Приёмник вебхуков | http://localhost:9000/received |
| PostgreSQL | localhost:5433 (postgres / password) |

Порты меняются через `.env` (см. `.env.example`), их читают и compose, и `make`,
и `scripts/demo.sh`. База снаружи на 5433, потому что 5432 обычно занят локальным Postgres.
`make help` покажет остальные команды.

## Как проверить

Скрипт (нужен `jq`):

```bash
bash scripts/demo.sh
```

Создаёт платёж, ждёт обработки и уведомления, потом создаёт платёж с адресом, который
всегда отвечает 500, и ждёт трёх попыток и сообщения в DLQ. Каждый шаг сверяется
с ожидаемым, при расхождении скрипт падает. Он же запускается в CI.

Руками:

```bash
# создать платёж
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: local-dev-api-key" \
  -H "Idempotency-Key: order-1042" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Заказ #1042",
       "metadata": {"order_id": 1042}, "webhook_url": "http://webhook-sink:9000/webhook"}'
# 202 {"payment_id": "...", "status": "pending", "created_at": "..."}

# через 2-5 секунд
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: local-dev-api-key"
# {"status": "succeeded", "processed_at": "...", "webhook_delivered_at": "...", ...}

# что получил клиент
curl -s http://localhost:9000/received
```

Повтор первого запроса вернёт тот же `payment_id` и заголовок `Idempotent-Replayed: true`.
Тот же ключ с другой суммой даст 409 `idempotency_conflict`.

Retry и DLQ: указать `"webhook_url": "http://webhook-sink:9000/fail"`. В логах консьюмера
будут три попытки с задержками 1 и 2 секунды, потом сообщение уедет в `payments.dlq`
(`make dlq` или management UI). `GET` покажет `succeeded` и `webhook_delivered_at: null`.

## Как устроено

```
POST /api/v1/payments -> [payments] + [outbox] в одной транзакции, ответ 202
                              |
                   outbox-relay (poll, FOR UPDATE SKIP LOCKED)
                              | publish + confirm -> published_at
                              v
                  RabbitMQ: payments --payments.new--> [payments.new]
                                                            |
                                                        consumer
                                            1. шлюз (2-5 с, 90% успех)
                                            2. UPDATE status WHERE status = 'pending'
                                            3. POST webhook_url -> webhook_delivered_at
                                                            | ошибка стадии
                   [payments.retry.1s] <-- попытка 1 -------+
                   [payments.retry.2s] <-- попытка 2 -------+   (TTL истёк -> обратно в payments.new)
                   [payments.dlq]      <-- попытка 3 -------+
```

| Процесс | Команда | Что делает |
|---|---|---|
| `api` | `uvicorn app.main:app` | принимает платежи, при старте накатывает миграции |
| `outbox-relay` | `python -m app.outbox.relay` | переносит события из таблицы `outbox` в RabbitMQ |
| `consumer` | `faststream run app.consumer.main:app` | шлюз, статус, webhook, retry/DLQ |

Слои: `api/` (роутеры, зависимости, формат ошибок), `schemas/`, `services/` (создание
платежа, обработка, шлюз, отправка webhook), `repositories/`, `models/`, `broker/`
(топология RabbitMQ), `consumer/` и `outbox/` (точки входа), `tools/webhook_sink.py`.

## API

Все эндпоинты требуют `X-API-Key`. В Swagger ключ подставляется автоматически, если
включена `DOCS_PREAUTHORIZE_API_KEY` (в compose включена, в проде выключить: ключ попадает
в HTML страницы). Сама страница `/docs` без ключа доступна.

`POST /api/v1/payments`. Заголовок `Idempotency-Key` обязателен, до 128 символов
из `A-Za-z0-9_.:-`. Тело:

| Поле | Тип | Ограничения |
|---|---|---|
| `amount` | decimal | > 0, до двух знаков после запятой, лучше строкой `"1500.00"` |
| `currency` | `RUB` / `USD` / `EUR` | |
| `description` | string | до 1000 символов, по умолчанию пусто |
| `metadata` | object | любой JSON-объект до 16 КБ, по умолчанию `{}` |
| `webhook_url` | url | http или https |

Ответ 202: `payment_id`, `status: pending`, `created_at`.

`GET /api/v1/payments/{payment_id}`: все поля платежа плюс `status`, `idempotency_key`,
`failure_reason`, `created_at`, `processed_at`, `webhook_delivered_at`.

`GET /health`: 200 если база отвечает, иначе 503.

Суммы в ответах и в webhook всегда строкой с двумя знаками.

### Ошибки

Формат один: `{"error": {"code": "...", "message": "...", ...}}`.

| Код | Статус | Когда |
|---|---|---|
| `unauthorized` | 401 | нет или неверный `X-API-Key` |
| `idempotency_key_missing` / `idempotency_key_invalid` | 400 | нет заголовка / не по формату |
| `idempotency_conflict` | 409 | тот же ключ с другим телом |
| `validation_failed` | 422 | поля перечислены в `fields` |
| `payment_not_found` | 404 | нет такого платежа (невалидный UUID тоже 404) |
| `not_found` | 404 | нет такого пути |
| `body_too_large` | 413 | тело больше 256 КБ |
| `internal_error` | 500 | `X-Request-Id` в заголовке ответа для поиска в логах |

### Webhook

`POST` на `webhook_url`:

```json
{"event": "payment.processed", "payment_id": "...", "status": "succeeded",
 "amount": "1500.00", "currency": "RUB", "description": "Заказ #1042",
 "metadata": {"order_id": 1042}, "failure_reason": null,
 "created_at": "...", "processed_at": "..."}
```

Заголовки `X-Payment-Id` и `X-Attempt` (номер попытки обработки). Доставлено = любой 2xx,
всё остальное (таймаут 5 с, отказ соединения, не-2xx) идёт в retry. Редиректам не следуем.

Доставка at-least-once: если consumer упадёт между ответом получателя и отметкой
`webhook_delivered_at`, уведомление придёт ещё раз. Повтор отличается по `X-Payment-Id`.

## Обработка, retry, DLQ

Consumer идёт по стадиям и смотрит на состояние в базе, а не на счётчик в сообщении:

1. `status = pending`: вызвать шлюз, записать результат условным
   `UPDATE ... WHERE status = 'pending'`.
2. `webhook_delivered_at IS NULL`: отправить уведомление, записать `webhook_delivered_at`.

Повтор сообщения (из retry-очереди, редоставка брокером, дубль от relay) продолжает
с первой незавершённой стадии.

Отказ шлюза (те самые 10%) это штатный исход: платёж `failed`, клиенту уходит webhook
со `status: failed`, повторов нет. Ретраятся только сбои: шлюз не ответил, база недоступна,
webhook не доставлен.

Retry: всего `CONSUMER_MAX_ATTEMPTS` попыток (3), задержка перед попыткой n равна
`RETRY_BASE_DELAY_SECONDS * 2^(n-2)`, то есть 1 и 2 секунды. После неудачи consumer
публикует сообщение с `attempt + 1` в `payments.retry.<задержка>`. У этих очередей нет
потребителей, по истечении `x-message-ttl` брокер сам возвращает сообщение в `payments.new`.
Очередь на каждую задержку, а не одна с per-message TTL, потому что RabbitMQ проверяет TTL
только у головы очереди. Секундные задержки взяты для наглядности, для реальных сбоев
базовую задержку стоит увеличить.

DLQ: после последней попытки сообщение уходит в `payments.dlx` -> `payments.dlq`
с заголовками `x-attempt`, `x-last-error` и теми же полями в теле. Туда же сразу попадает
сообщение о платеже, которого нет в базе. На `payments.new` стоит `x-dead-letter-exchange`
как страховка: если handler упал сам (например, брокер не принял публикацию в retry),
сообщение отвергается и уходит в ту же DLQ, но с `x-death` вместо `x-last-error`.

По `GET` у платежа из DLQ: `pending` значит база или шлюз не отвечали все попытки,
`succeeded`/`failed` с пустым `webhook_delivered_at` значит не доставлено уведомление.
Разбор DLQ ручной: посмотреть причину (`make dlq`), починить, переложить в `payments.new`
через management UI.

Редоставка брокером (consumer упал, не подтвердив сообщение) считается потраченной попыткой,
иначе сообщение, на котором процесс падает, крутилось бы бесконечно.

## Решения

Outbox и отдельный relay. Платёж и событие пишутся одной транзакцией, в RabbitMQ внутри
неё никто не ходит, поэтому недоступный брокер не мешает принимать платежи, а событие
не теряется между коммитом и публикацией. Relay читает пачку под `FOR UPDATE SKIP LOCKED`
(можно запустить несколько), публикует с confirm и `mandatory=true` (нет очереди = ошибка)
и только потом ставит `published_at`. Если упала середина пачки, подтверждённые до неё
строки всё равно коммитятся. Publish идёт при открытой транзакции, иначе строку заберёт
второй relay, поэтому у publish есть таймаут, а у транзакции
`idle_in_transaction_session_timeout`. Гарантия at-least-once, дубли закрывает
идемпотентность консьюмера. В api relay не встроен: api не должен зависеть от брокера,
а relay должен перезапускаться отдельно.

Идемпотентность API. Ключ в колонке с уникальным индексом, вставка через
`INSERT ... ON CONFLICT DO NOTHING RETURNING`, без SELECT перед INSERT. Десять одновременных
запросов с одним ключом дают один платёж (есть тест). Рядом хранится sha256 нормализованного
тела: тот же ключ с тем же телом это 202 и `Idempotent-Replayed: true`, с другим телом 409.
`1500`, `1500.0` и `"1500.00"` считаются одним телом.

Гонка двух консьюмеров. Условный `UPDATE ... WHERE status = 'pending'` не даст перезаписать
чужой результат. Двойной вызов шлюза полностью исключить можно только блокировкой на время
его ответа, держать транзакцию 2-5 секунд ради этого не хочется, поэтому `payment_id`
уходит в шлюз как ключ идемпотентности.

Ключ и `/health`. В ТЗ "для всех эндпоинтов", так и сделано, healthcheck в compose передаёт
заголовок из окружения. Сравнение через `hmac.compare_digest`.

HTTP вне транзакций. Шлюз и webhook не вызываются внутри транзакции. Все транзакции идут
через один helper с `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`.

Деньги: `NUMERIC(18,2)` в базе, `Decimal` в коде, строка в JSON, `CHECK (amount > 0)`.

Не как в проде: миграции накатывает `api` при старте (удобно для `docker compose up`),
ключ и пароли в compose демонстрационные. С `ENV=production` и дефолтным ключом приложение
не поднимется.

## Настройки

Переменные окружения или `.env`, у всех есть дефолты.

| Переменная | По умолчанию | Что |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:password@localhost:5432/payments` | |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | |
| `API_KEY` | `local-dev-api-key` | ключ для `X-API-Key` |
| `DOCS_PREAUTHORIZE_API_KEY` | `false` | подставлять ключ в Swagger (в compose `true`) |
| `ENV` | `development` | `development` / `test` / `production`; в production JSON-логи и запрет дефолтного ключа |
| `GATEWAY_DELAY_MIN_SECONDS` / `_MAX_SECONDS` | 2 / 5 | задержка эмуляции шлюза |
| `GATEWAY_SUCCESS_RATE` | 0.9 | доля успешных платежей |
| `GATEWAY_UNAVAILABLE_RATE` | 0 | доля вызовов, где шлюз не отвечает (retry) |
| `CONSUMER_MAX_ATTEMPTS` | 3 | попыток всего, потом DLQ |
| `RETRY_BASE_DELAY_SECONDS` | 1 | первая задержка, дальше x2 |
| `CONSUMER_PREFETCH` | 10 | сколько сообщений consumer держит в работе |
| `WEBHOOK_TIMEOUT_SECONDS` | 5 | таймаут доставки уведомления |
| `OUTBOX_POLL_INTERVAL_SECONDS` / `OUTBOX_BATCH_SIZE` | 0.5 / 100 | relay |
| `STATEMENT_TIMEOUT_MS` / `LOCK_TIMEOUT_MS` / `IDLE_IN_TRANSACTION_TIMEOUT_MS` | 5000 / 3000 / 30000 | таймауты транзакций |
| `MAX_BODY_BYTES` | 262144 | предел размера тела запроса |

## Тесты

```bash
pip install -r requirements-dev.txt
docker compose up -d postgres
pytest -v                              # база payments_test создастся сама
ruff check . && ruff format --check .
alembic check
```

По умолчанию тесты ходят на `127.0.0.1:5433`, другой адрес через `DATABASE_URL`. В имени базы
должно быть `test`, прогон пересоздаёт схему.

Покрыто (71 тест): валидация, ключ на каждой ручке, формат ошибок включая 500 и 503,
идемпотентность (повтор, конфликт, формат ключа, одновременные запросы), outbox и relay
(публикация, отказ брокера, частичный сбой пачки), consumer (успех, отказ и недоступность
шлюза, недоставленный webhook и таймаут, DLQ, повтор сообщения, гонка двух консьюмеров,
неизвестный платёж), топология очередей, валидация настроек.

Брокер в тестах `TestRabbitBroker`, в памяти. TTL, dead-lettering и `mandatory` он
не эмулирует, поэтому цепочка retry -> TTL -> DLQ проверяется на настоящем RabbitMQ
скриптом `scripts/demo.sh`, в CI это отдельный job.

## За рамками

- Подпись webhook (HMAC).
- Фильтр `webhook_url` от внутренних адресов (SSRF), в demo приёмник как раз внутри сети compose.
- `LISTEN/NOTIFY` вместо поллинга outbox.
- Инструмент для разбора DLQ, сейчас через management UI.
- Метрики.
- Чистка `outbox`: опубликованные строки не удаляются.
