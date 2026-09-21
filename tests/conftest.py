"""База настоящая (схема через alembic), брокер в памяти (TestRabbitBroker)."""

from __future__ import annotations

import asyncio
import os
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

PROJECT_DIR = Path(__file__).resolve().parents[1]

# до импорта приложения: настройки кешируются. 5433 - порт базы из docker-compose
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://postgres:password@127.0.0.1:5433/payments_test")
os.environ.setdefault("ENV", "test")
os.environ.setdefault("LOG_LEVEL", "WARNING")

import asyncpg  # noqa: E402
import httpx  # noqa: E402
import pytest  # noqa: E402
from alembic.config import Config  # noqa: E402
from sqlalchemy import text  # noqa: E402

from alembic import command  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.core.db import dispose_engine, session_scope, transaction  # noqa: E402
from app.models import Currency  # noqa: E402
from app.services.gateway import GatewayResult  # noqa: E402
from app.services.webhook import WebhookSender, make_http_client  # noqa: E402

API_KEY_HEADER = {"X-API-Key": get_settings().api_key}
TABLES_TO_CLEAN = "outbox, payments"


def _plain_dsn() -> str:
    return get_settings().database_url.replace("postgresql+asyncpg://", "postgresql://")


def _refuse_non_test_database() -> None:
    # прогон делает DROP SCHEMA
    name = urlsplit(get_settings().database_url).path.lstrip("/")
    if "test" not in name.lower():
        raise RuntimeError(
            f"Тесты удаляют схему целиком, а DATABASE_URL указывает на базу '{name}'. "
            "В имени тестовой базы должно быть 'test'."
        )


async def _create_database_if_absent() -> None:
    dsn = _plain_dsn()
    name = urlsplit(dsn).path.lstrip("/")
    try:
        conn = await asyncpg.connect(dsn)
    except asyncpg.exceptions.InvalidCatalogNameError:
        pass
    else:
        await conn.close()
        return
    admin = await asyncpg.connect(dsn.rsplit("/", 1)[0] + "/postgres")
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> None:
    _refuse_non_test_database()

    async def reset_schema() -> None:
        await _create_database_if_absent()
        conn = await asyncpg.connect(_plain_dsn())
        try:
            await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        finally:
            await conn.close()

    asyncio.run(reset_schema())

    config = Config(str(PROJECT_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_DIR / "alembic"))
    command.upgrade(config, "head")


@pytest.fixture(autouse=True)
async def clean_tables(migrated_database):
    async with session_scope() as session, transaction(session):
        await session.execute(text(f"TRUNCATE {TABLES_TO_CLEAN} RESTART IDENTITY CASCADE"))
    yield


@pytest.fixture(scope="session", autouse=True)
async def close_pool():
    yield
    await dispose_engine()


@pytest.fixture
async def client() -> httpx.AsyncClient:
    from app.main import app

    # raise_app_exceptions=False: проверяем и ответ 500
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=API_KEY_HEADER) as http:
        yield http


def payment_body(**overrides: Any) -> dict[str, Any]:
    body = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #1042",
        "metadata": {"order_id": 1042},
        "webhook_url": "http://client.example/webhook",
    }
    body.update(overrides)
    return body


async def create_payment_via_api(client: httpx.AsyncClient, key: str = "key-1", **overrides: Any) -> dict:
    response = await client.post(
        "/api/v1/payments", json=payment_body(**overrides), headers={"Idempotency-Key": key}
    )
    assert response.status_code == 202, response.text
    return response.json()


async def fetch_one(sql: str, **params):
    async with session_scope() as session:
        return (await session.execute(text(sql), params)).first()


async def scalar(sql: str, **params):
    async with session_scope() as session:
        return (await session.execute(text(sql), params)).scalar()


async def execute(sql: str, **params) -> None:
    async with session_scope() as session, transaction(session):
        await session.execute(text(sql), params)


class FakeGateway:
    def __init__(self, *, succeeded: bool = True, error: Exception | None = None) -> None:
        self.succeeded = succeeded
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def charge(self, *, payment_id: uuid.UUID, amount: Decimal, currency: Currency) -> GatewayResult:
        self.calls.append(payment_id)
        if self.error is not None:
            raise self.error
        if self.succeeded:
            return GatewayResult(succeeded=True)
        return GatewayResult(succeeded=False, failure_reason="declined by gateway")


class WebhookReceiver:
    """Получатель на MockTransport. status_code=0 - отказ соединения, error - своё исключение."""

    def __init__(self, status_code: int = 200, *, error: httpx.HTTPError | None = None) -> None:
        self.status_code = status_code
        self.error = error
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        if self.status_code == 0:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(self.status_code, json={"status": "ok"})

    def sender(self) -> WebhookSender:
        return WebhookSender(
            make_http_client(timeout_seconds=1.0, transport=httpx.MockTransport(self.handler))
        )
