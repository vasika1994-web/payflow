from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.conftest import API_KEY_HEADER, create_payment_via_api, payment_body, scalar


async def test_same_key_and_body_returns_same_payment_without_new_rows(client):
    first = await create_payment_via_api(client, "order-1")

    response = await client.post(
        "/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "order-1"}
    )

    assert response.status_code == 202
    assert response.json() == first
    assert response.headers["Idempotent-Replayed"] == "true"
    assert await scalar("SELECT count(*) FROM payments") == 1
    assert await scalar("SELECT count(*) FROM outbox") == 1


async def test_replay_ignores_key_order_and_number_formatting(client):
    await create_payment_via_api(client, "order-1")
    reordered = {
        "webhook_url": "http://client.example/webhook",
        "metadata": {"order_id": 1042},
        "currency": "RUB",
        "amount": 1500,
        "description": "Заказ #1042",
    }

    response = await client.post("/api/v1/payments", json=reordered, headers={"Idempotency-Key": "order-1"})

    assert response.status_code == 202
    assert response.headers.get("Idempotent-Replayed") == "true"


async def test_same_key_with_different_body_is_409(client):
    await create_payment_via_api(client, "order-1")

    response = await client.post(
        "/api/v1/payments", json=payment_body(amount="1.00"), headers={"Idempotency-Key": "order-1"}
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "idempotency_conflict"
    assert await scalar("SELECT count(*) FROM payments") == 1


async def test_different_keys_create_different_payments(client):
    a = await create_payment_via_api(client, "a")
    b = await create_payment_via_api(client, "b")

    assert a["payment_id"] != b["payment_id"]
    assert await scalar("SELECT count(*) FROM payments") == 2


async def test_missing_key_is_400(client):
    response = await client.post("/api/v1/payments", json=payment_body())

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_key_missing"
    assert await scalar("SELECT count(*) FROM payments") == 0


@pytest.mark.parametrize("key", ["", "with space", "x" * 129, "a/b", "key!"])
async def test_malformed_key_is_400(client, key):
    response = await client.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": key})

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "idempotency_key_invalid"


async def test_concurrent_requests_with_same_key_create_one_payment():
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers=API_KEY_HEADER
    ) as client:
        responses = await asyncio.gather(
            *(
                client.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "race"})
                for _ in range(10)
            )
        )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await scalar("SELECT count(*) FROM payments") == 1
    assert await scalar("SELECT count(*) FROM outbox") == 1
