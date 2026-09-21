from __future__ import annotations

import pytest

from app.core.config import get_settings
from tests.conftest import create_payment_via_api, fetch_one, payment_body, scalar


async def test_accepts_payment_and_records_it_as_pending(client):
    response = await client.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "k1"})

    assert response.status_code == 202, response.text
    body = response.json()
    assert body["status"] == "pending"
    assert "payment_id" in body and "created_at" in body

    row = await fetch_one("SELECT amount, currency, status, metadata, webhook_url FROM payments")
    assert str(row.amount) == "1500.00"
    assert row.currency == "RUB"
    assert row.status == "pending"
    assert row.metadata == {"order_id": 1042}
    assert row.webhook_url == "http://client.example/webhook"


async def test_amount_is_kept_exact(client):
    await create_payment_via_api(client, "s", amount="0.10")
    await create_payment_via_api(client, "n", amount=19.99)

    amounts = await fetch_one("SELECT array_agg(amount::text ORDER BY amount) AS a FROM payments")
    assert amounts.a == ["0.10", "19.99"]


async def test_get_returns_full_details(client):
    created = await create_payment_via_api(client, "k1")

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    details = response.json()
    assert details["payment_id"] == created["payment_id"]
    assert details["amount"] == "1500.00"
    assert details["currency"] == "RUB"
    assert details["description"] == "Заказ #1042"
    assert details["metadata"] == {"order_id": 1042}
    assert details["idempotency_key"] == "k1"
    assert details["status"] == "pending"
    assert details["processed_at"] is None
    assert details["webhook_delivered_at"] is None
    assert details["failure_reason"] is None


@pytest.mark.parametrize("payment_id", ["00000000-0000-0000-0000-000000000000", "not-a-uuid", "42"])
async def test_get_unknown_payment_is_404(client, payment_id):
    response = await client.get(f"/api/v1/payments/{payment_id}")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "payment_not_found"


@pytest.mark.parametrize(
    ("override", "field"),
    [
        ({"amount": "0"}, "amount"),
        ({"amount": "-5"}, "amount"),
        ({"amount": "1.005"}, "amount"),
        ({"amount": "abc"}, "amount"),
        ({"amount": "99999999999999.99"}, "amount"),
        ({"currency": "GBP"}, "currency"),
        ({"webhook_url": "not a url"}, "webhook_url"),
        ({"webhook_url": "ftp://host/x"}, "webhook_url"),
        ({"metadata": ["list"]}, "metadata"),
        ({"metadata": {"blob": "x" * 20_000}}, "metadata"),
        ({"description": "d" * 1_001}, "description"),
        ({"unexpected": 1}, "unexpected"),
    ],
)
async def test_invalid_body_is_422_with_field_names(client, override, field):
    response = await client.post(
        "/api/v1/payments", json=payment_body(**override), headers={"Idempotency-Key": "k"}
    )

    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["code"] == "validation_failed"
    assert field in error["fields"]
    assert await scalar("SELECT count(*) FROM payments") == 0


async def test_missing_required_fields_are_listed(client):
    response = await client.post("/api/v1/payments", json={}, headers={"Idempotency-Key": "k"})

    assert response.status_code == 422
    assert set(response.json()["error"]["fields"]) == {"amount", "currency", "webhook_url"}


async def test_malformed_json_is_422_and_points_at_body(client):
    response = await client.post(
        "/api/v1/payments",
        content=b"{not json",
        headers={"Idempotency-Key": "k", "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_failed"
    assert list(error["fields"]) == ["body"]


async def test_oversized_body_is_413(client):
    huge = payment_body(description="x" * 300_000)
    response = await client.post("/api/v1/payments", json=huge, headers={"Idempotency-Key": "k"})

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "body_too_large"


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
async def test_every_endpoint_requires_api_key(client, headers):
    client.headers.pop("X-API-Key")

    post = await client.post(
        "/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "k", **headers}
    )
    get = await client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000", headers=headers)
    health = await client.get("/health", headers=headers)

    for response in (post, get, health):
        assert response.status_code == 401, response.text
        assert response.json()["error"]["code"] == "unauthorized"
    assert await scalar("SELECT count(*) FROM payments") == 0


async def test_unknown_path_uses_same_error_envelope(client):
    response = await client.get("/api/v1/nothing")

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "not_found", "message": "Not Found"}}


async def test_unexpected_failure_is_500_in_same_envelope_with_request_id(client, monkeypatch):
    from app.api.routers import payments as payments_router

    async def broken(*_args, **_kwargs):
        raise RuntimeError("база взорвалась")

    monkeypatch.setattr(payments_router, "create_payment", broken)

    response = await client.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "k"})

    assert response.status_code == 500
    assert response.json() == {"error": {"code": "internal_error", "message": "внутренняя ошибка сервиса"}}
    assert response.headers["X-Request-Id"]
    assert await scalar("SELECT count(*) FROM payments") == 0


async def test_health_is_503_when_database_is_down(client, monkeypatch):
    from app.api.routers import health as health_router

    def no_database():
        raise ConnectionError("нет базы")

    monkeypatch.setattr(health_router, "session_scope", no_database)

    response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}


async def test_health_reports_ok_and_request_id(client):
    response = await client.get("/health", headers={"X-Request-Id": "trace-1"})

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["X-Request-Id"] == "trace-1"


async def test_docs_do_not_leak_key_by_default(client):
    client.headers.pop("X-API-Key")

    response = await client.get("/docs")

    assert response.status_code == 200
    assert "swagger-ui" in response.text
    assert get_settings().api_key not in response.text


async def test_docs_preauthorize_key_when_enabled(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "docs_preauthorize_api_key", True)
    client.headers.pop("X-API-Key")

    response = await client.get("/docs")

    assert 'preauthorizeApiKey("ApiKeyAuth", preauthorizedKey)' in response.text
    assert get_settings().api_key in response.text
