from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse
from starlette.exceptions import HTTPException

from app.api.docs import swagger_ui_html
from app.api.errors import (
    ApiError,
    error_response,
    handle_api_error,
    handle_http_exception,
    handle_validation_error,
)
from app.api.routers import health, payments
from app.core.config import get_settings
from app.core.db import dispose_engine
from app.core.logging import configure_logging, get_logger

API_DESCRIPTION = """
Сервис принимает запросы на оплату, обрабатывает их асинхронно через платёжный шлюз
(эмуляция) и уведомляет клиента о результате через webhook.

### Как это работает

1. `POST /api/v1/payments` записывает платёж в статусе `pending` и событие для брокера
   в одной транзакции (outbox). Ответ **202** приходит сразу.
2. Relay публикует событие в RabbitMQ (`payments.new`), consumer обрабатывает платёж
   через шлюз (2-5 секунд, 90% успех), сохраняет статус и шлёт уведомление на `webhook_url`.
3. `GET /api/v1/payments/{payment_id}` показывает текущее состояние: статус, время обработки,
   доставлено ли уведомление.

### Аутентификация

Все эндпоинты требуют заголовок `X-API-Key`. Если замок на этой странице уже закрыт,
ключ подставлен автоматически (настройка `DOCS_PREAUTHORIZE_API_KEY`) и можно сразу
жать **Try it out**. Иначе нажмите **Authorize** и введите ключ.

### Ошибки

Тело ошибки всегда одно: `{"error": {"code": "...", "message": "...", ...}}`. Решение
принимается по `code`:

| Код | Статус | Когда |
|---|---|---|
| `unauthorized` | 401 | нет или неверный `X-API-Key` |
| `idempotency_key_missing` | 400 | нет заголовка `Idempotency-Key` |
| `idempotency_key_invalid` | 400 | ключ не по формату |
| `idempotency_conflict` | 409 | тот же ключ с другим телом запроса |
| `validation_failed` | 422 | тело не прошло валидацию, поля в `fields` |
| `payment_not_found` | 404 | нет платежа с таким id |
| `not_found` | 404 | нет такого пути |
| `body_too_large` | 413 | тело больше предела |
| `internal_error` | 500 | непредвиденный сбой, `X-Request-Id` в заголовке ответа |
"""


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.env != "development")
    get_logger(__name__).info("api.started", env=settings.env)
    yield
    await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Процессинг платежей",
        description=API_DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
        # своя страница /docs ниже
        docs_url=None,
        redoc_url=None,
        openapi_tags=[
            {"name": "Платежи", "description": "Создание и просмотр платежей."},
            {"name": "Служебное", "description": "Проверка живости."},
        ],
    )

    app.add_exception_handler(ApiError, handle_api_error)
    app.add_exception_handler(HTTPException, handle_http_exception)
    app.add_exception_handler(RequestValidationError, handle_validation_error)

    @app.middleware("http")
    async def refuse_oversized_body(request: Request, call_next):
        # тело буферизуется до проверки ключа; chunked без Content-Length не ловим
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > settings.max_body_bytes:
            return error_response(
                status.HTTP_413_CONTENT_TOO_LARGE, "body_too_large", "тело запроса слишком большое"
            )
        return await call_next(request)

    @app.middleware("http")
    async def attach_request_id(request: Request, call_next):
        # 500 ловим здесь, а не в exception_handler: тот снаружи middleware и потерял бы X-Request-Id
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        except Exception as exc:
            get_logger(__name__).error("api.unhandled_error", error=str(exc), exc_info=exc)
            response = error_response(
                status.HTTP_500_INTERNAL_SERVER_ERROR, "internal_error", "внутренняя ошибка сервиса"
            )
        finally:
            structlog.contextvars.unbind_contextvars("request_id")
        response.headers["X-Request-Id"] = request_id
        return response

    @app.get("/docs", include_in_schema=False)
    async def docs() -> HTMLResponse:
        return swagger_ui_html(
            openapi_url=app.openapi_url or "/openapi.json",
            title=f"{app.title} - Swagger UI",
            preauthorized_key=settings.api_key if settings.docs_preauthorize_api_key else None,
        )

    app.include_router(payments.router)
    app.include_router(health.router)

    return app


app = create_app()
