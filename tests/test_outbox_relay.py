from __future__ import annotations

import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker

from app.broker.connection import make_broker
from app.broker.topology import NEW_QUEUE, PAYMENTS_EXCHANGE
from app.core.config import get_settings
from app.outbox import relay
from tests.conftest import create_payment_via_api, execute, fetch_one, scalar


def broker_with_listener(settings=None) -> tuple[RabbitBroker, list[dict]]:
    # без слушателя publish в TestRabbitBroker падает (SubscriberNotFound)
    broker = make_broker(settings or get_settings())
    received: list[dict] = []

    @broker.subscriber(NEW_QUEUE, PAYMENTS_EXCHANGE)
    async def listen(body: dict) -> None:
        received.append(body)

    return broker, received


async def test_creating_payment_writes_outbox_event_in_same_transaction(client):
    created = await create_payment_via_api(client, "k1")

    event = await fetch_one("SELECT event_type, payload, published_at, publish_attempts FROM outbox")
    assert event.event_type == "payment.created"
    assert event.payload == {"payment_id": created["payment_id"]}
    assert event.published_at is None
    assert event.publish_attempts == 0


async def test_relay_publishes_and_marks_events(client):
    first = await create_payment_via_api(client, "k1")
    second = await create_payment_via_api(client, "k2")
    broker, received = broker_with_listener()

    async with TestRabbitBroker(broker):
        published = await relay.relay_once(broker, get_settings())
        again = await relay.relay_once(broker, get_settings())

    assert published == 2
    assert again == 0
    assert received == [{"payment_id": first["payment_id"]}, {"payment_id": second["payment_id"]}]
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0


async def test_publish_error_leaves_event_unpublished(client):
    # здесь ошибку даёт отсутствие подписчика; настоящий mandatory/NO_ROUTE проверяет e2e
    await create_payment_via_api(client, "k1")
    broker = make_broker(get_settings())

    async with TestRabbitBroker(broker):
        published = await relay.relay_once(broker, get_settings())

    assert published == 0
    event = await fetch_one("SELECT published_at, publish_attempts FROM outbox")
    assert event.published_at is None
    assert event.publish_attempts == 1


async def test_failed_publish_keeps_event_and_counts_attempt(client, monkeypatch):
    await create_payment_via_api(client, "k1")
    broker = make_broker(get_settings())

    async def broken_publish(*_args, **_kwargs):
        raise ConnectionError("broker is down")

    monkeypatch.setattr(relay, "publish_event", broken_publish)
    async with TestRabbitBroker(broker):
        published = await relay.relay_once(broker, get_settings())

    assert published == 0
    event = await fetch_one("SELECT published_at, publish_attempts, last_error FROM outbox")
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert "broker is down" in event.last_error


async def test_partial_batch_failure_keeps_confirmed_events(client, monkeypatch):
    for key in ("k1", "k2", "k3"):
        await create_payment_via_api(client, key)
    broker, _received = broker_with_listener()
    real_publish = relay.publish_event

    async def flaky_publish(broker_, event):
        if event.id == 2:
            raise ConnectionError("broker hiccup")
        await real_publish(broker_, event)

    monkeypatch.setattr(relay, "publish_event", flaky_publish)
    async with TestRabbitBroker(broker):
        published = await relay.relay_once(broker, get_settings())

    assert published == 1
    rows = await fetch_one(
        "SELECT array_agg(id ORDER BY id) FILTER (WHERE published_at IS NOT NULL) AS done, "
        "array_agg(id ORDER BY id) FILTER (WHERE published_at IS NULL) AS pending FROM outbox"
    )
    assert rows.done == [1]
    assert rows.pending == [2, 3]


@pytest.mark.parametrize("batch_size", [1, 2])
async def test_relay_respects_batch_size(client, monkeypatch, batch_size):
    for key in ("k1", "k2", "k3"):
        await create_payment_via_api(client, key)
    settings = get_settings().model_copy(update={"outbox_batch_size": batch_size})
    broker, _received = broker_with_listener(settings)

    async with TestRabbitBroker(broker):
        first = await relay.relay_once(broker, settings)

    assert first == batch_size
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 3 - batch_size


async def test_relay_skips_already_published(client):
    await create_payment_via_api(client, "k1")
    await execute("UPDATE outbox SET published_at = now()")
    broker, received = broker_with_listener()

    async with TestRabbitBroker(broker):
        assert await relay.relay_once(broker, get_settings()) == 0

    assert received == []
    event = await fetch_one("SELECT publish_attempts, last_error FROM outbox")
    assert (event.publish_attempts, event.last_error) == (0, None)
