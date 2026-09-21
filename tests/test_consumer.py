from __future__ import annotations

import json
import uuid

import httpx
import pytest
from faststream.rabbit import TestRabbitBroker

from app.broker.topology import NEW_QUEUE, PAYMENTS_EXCHANGE
from app.consumer import main as consumer
from app.services.gateway import GatewayUnavailableError
from app.services.process_payment import ProcessingDeps
from tests.conftest import FakeGateway, WebhookReceiver, create_payment_via_api, execute, fetch_one

RETRY_1S = "payments.retry.1s"
RETRY_2S = "payments.retry.2s"


@pytest.fixture
async def running_consumer():
    async with TestRabbitBroker(consumer.broker) as broker:
        for publisher in (*consumer.retry_publishers.values(), consumer.dlq_publisher):
            publisher.mock.reset_mock()
        yield broker


def use(gateway: FakeGateway, receiver: WebhookReceiver) -> None:
    consumer.broker.context.set_global("deps", ProcessingDeps(gateway=gateway, webhooks=receiver.sender()))


async def deliver(broker, payment_id: str, attempt: int = 1) -> None:
    await broker.publish(
        {"payment_id": payment_id, "attempt": attempt}, queue=NEW_QUEUE, exchange=PAYMENTS_EXCHANGE
    )


def sent_to(publisher) -> dict | None:
    if not publisher.mock.called:
        return None
    publisher.mock.assert_called_once()
    return publisher.mock.call_args.args[0]


async def test_success_updates_status_and_delivers_webhook(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    gateway, receiver = FakeGateway(succeeded=True), WebhookReceiver(200)
    use(gateway, receiver)

    await deliver(running_consumer, created["payment_id"])

    row = await fetch_one("SELECT status, processed_at, webhook_delivered_at, failure_reason FROM payments")
    assert row.status == "succeeded"
    assert row.processed_at is not None
    assert row.webhook_delivered_at is not None
    assert row.failure_reason is None

    assert gateway.calls == [uuid.UUID(created["payment_id"])]
    assert len(receiver.requests) == 1
    request = receiver.requests[0]
    assert str(request.url) == "http://client.example/webhook"
    assert request.headers["X-Payment-Id"] == created["payment_id"]
    assert request.headers["X-Attempt"] == "1"
    payload = json.loads(request.content)
    assert payload["event"] == "payment.processed"
    assert payload["status"] == "succeeded"
    assert payload["amount"] == "1500.00"
    assert payload["metadata"] == {"order_id": 1042}
    assert payload["processed_at"] is not None

    assert sent_to(consumer.dlq_publisher) is None
    assert all(sent_to(p) is None for p in consumer.retry_publishers.values())


async def test_gateway_decline_is_failed_status_not_retry(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    receiver = WebhookReceiver(200)
    use(FakeGateway(succeeded=False), receiver)

    await deliver(running_consumer, created["payment_id"])

    row = await fetch_one("SELECT status, failure_reason, webhook_delivered_at FROM payments")
    assert row.status == "failed"
    assert row.failure_reason == "declined by gateway"
    assert row.webhook_delivered_at is not None
    assert json.loads(receiver.requests[0].content)["status"] == "failed"
    assert all(sent_to(p) is None for p in consumer.retry_publishers.values())
    assert sent_to(consumer.dlq_publisher) is None


async def test_webhook_failure_schedules_retry_without_recharging(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    gateway = FakeGateway(succeeded=True)
    use(gateway, WebhookReceiver(500))

    await deliver(running_consumer, created["payment_id"], attempt=1)

    row = await fetch_one("SELECT status, webhook_delivered_at FROM payments")
    assert row.status == "succeeded"
    assert row.webhook_delivered_at is None

    retry = sent_to(consumer.retry_publishers[RETRY_1S])
    assert retry["payment_id"] == created["payment_id"]
    assert retry["attempt"] == 2
    assert "500" in retry["last_error"]
    assert sent_to(consumer.retry_publishers[RETRY_2S]) is None
    assert sent_to(consumer.dlq_publisher) is None

    # вторая попытка: шлюз не трогаем, только webhook
    receiver = WebhookReceiver(200)
    use(gateway, receiver)
    await deliver(running_consumer, created["payment_id"], attempt=2)

    assert len(gateway.calls) == 1
    assert receiver.requests[0].headers["X-Attempt"] == "2"
    assert (await fetch_one("SELECT webhook_delivered_at FROM payments")).webhook_delivered_at is not None


async def test_second_attempt_failure_goes_to_longer_delay(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    use(FakeGateway(succeeded=True), WebhookReceiver(0))

    await deliver(running_consumer, created["payment_id"], attempt=2)

    assert sent_to(consumer.retry_publishers[RETRY_1S]) is None
    retry = sent_to(consumer.retry_publishers[RETRY_2S])
    assert retry["attempt"] == 3
    assert "ConnectError" in retry["last_error"]


async def test_last_attempt_failure_goes_to_dlq(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    use(FakeGateway(succeeded=True), WebhookReceiver(503))

    await deliver(running_consumer, created["payment_id"], attempt=3)

    dead = sent_to(consumer.dlq_publisher)
    assert dead["payment_id"] == created["payment_id"]
    assert dead["attempt"] == 3
    assert "503" in dead["last_error"]
    assert all(sent_to(p) is None for p in consumer.retry_publishers.values())
    row = await fetch_one("SELECT status, webhook_delivered_at FROM payments")
    assert (row.status, row.webhook_delivered_at) == ("succeeded", None)


async def test_gateway_outage_is_retried_and_payment_stays_pending(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    receiver = WebhookReceiver(200)
    use(FakeGateway(error=GatewayUnavailableError("timeout")), receiver)

    await deliver(running_consumer, created["payment_id"])

    assert (await fetch_one("SELECT status FROM payments")).status == "pending"
    assert receiver.requests == []
    retry = sent_to(consumer.retry_publishers[RETRY_1S])
    assert retry["attempt"] == 2
    assert "GatewayUnavailableError" in retry["last_error"]


async def test_gateway_outage_on_last_attempt_goes_to_dlq_with_pending_payment(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    use(FakeGateway(error=GatewayUnavailableError("timeout")), WebhookReceiver(200))

    await deliver(running_consumer, created["payment_id"], attempt=3)

    assert (await fetch_one("SELECT status FROM payments")).status == "pending"
    dead = sent_to(consumer.dlq_publisher)
    assert dead["attempt"] == 3
    assert "GatewayUnavailableError" in dead["last_error"]


async def test_webhook_timeout_is_a_delivery_error(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    use(FakeGateway(succeeded=True), WebhookReceiver(error=httpx.ReadTimeout("slow receiver")))

    await deliver(running_consumer, created["payment_id"])

    retry = sent_to(consumer.retry_publishers[RETRY_1S])
    assert "ReadTimeout" in retry["last_error"]
    assert (await fetch_one("SELECT webhook_delivered_at FROM payments")).webhook_delivered_at is None


async def test_redelivered_message_does_not_repeat_finished_stages(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    gateway, receiver = FakeGateway(succeeded=True), WebhookReceiver(200)
    use(gateway, receiver)

    await deliver(running_consumer, created["payment_id"])
    await deliver(running_consumer, created["payment_id"])

    assert len(gateway.calls) == 1
    assert len(receiver.requests) == 1
    assert sent_to(consumer.dlq_publisher) is None


async def test_payment_finished_elsewhere_is_not_overwritten(client, running_consumer):
    created = await create_payment_via_api(client, "k1")
    receiver = WebhookReceiver(200)

    class RacingGateway(FakeGateway):
        async def charge(self, **kwargs):
            await execute(
                "UPDATE payments SET status = 'failed', failure_reason = 'other consumer', "
                "processed_at = now()"
            )
            return await super().charge(**kwargs)

    use(RacingGateway(succeeded=True), receiver)

    await deliver(running_consumer, created["payment_id"])

    row = await fetch_one("SELECT status, failure_reason, webhook_delivered_at FROM payments")
    assert (row.status, row.failure_reason) == ("failed", "other consumer")
    assert row.webhook_delivered_at is not None
    assert json.loads(receiver.requests[0].content)["status"] == "failed"


async def test_unknown_payment_goes_straight_to_dlq(running_consumer):
    use(FakeGateway(), WebhookReceiver(200))
    missing = str(uuid.uuid4())

    await deliver(running_consumer, missing)

    dead = sent_to(consumer.dlq_publisher)
    assert dead["payment_id"] == missing
    assert "PaymentNotFoundError" in dead["last_error"]
    assert all(sent_to(p) is None for p in consumer.retry_publishers.values())


@pytest.mark.parametrize(
    ("attempt", "redelivered", "expected"),
    [(1, False, 1), (1, True, 2), (3, True, 4)],
)
def test_redelivery_counts_as_spent_attempt(attempt, redelivered, expected):
    assert consumer.effective_attempt(attempt, redelivered=redelivered) == expected
