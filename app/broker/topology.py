"""Топология RabbitMQ.

payments (direct) --payments.new--> [payments.new] --> consumer
                                        | reject -> payments.dlx --payments.dlq--> [payments.dlq]
[payments.retry.1s], [payments.retry.2s]: без потребителей, по x-message-ttl
уходят обратно в payments/payments.new.

Retry-очередь на каждую задержку, а не одна с per-message TTL: RabbitMQ смотрит TTL
только у головы очереди. Объявляют топологию и consumer, и relay, чтобы порядок
старта не имел значения.
"""

from __future__ import annotations

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import Settings

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.dlq"

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        # страховка: reject без requeue уводит сообщение в DLQ. Routing key задать обязательно,
        # иначе в dlx оно придёт с ключом payments.new и потеряется
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_queue(delay_seconds: float) -> RabbitQueue:
    label = f"{delay_seconds:g}s"
    return RabbitQueue(
        f"payments.retry.{label}",
        durable=True,
        routing_key=f"payments.retry.{label}",
        arguments={
            "x-message-ttl": int(delay_seconds * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )


@dataclass(frozen=True, slots=True)
class Topology:
    new: RabbitQueue
    dlq: RabbitQueue
    retries: tuple[RabbitQueue, ...]  # retries[0] - задержка перед второй попыткой и т.д.

    def retry_for_attempt(self, failed_attempt: int) -> RabbitQueue | None:
        """None, если попытки кончились."""
        index = failed_attempt - 1
        if 0 <= index < len(self.retries):
            return self.retries[index]
        return None


def build_topology(settings: Settings) -> Topology:
    return Topology(
        new=NEW_QUEUE,
        dlq=DLQ_QUEUE,
        retries=tuple(retry_queue(delay) for delay in settings.retry_delays_seconds()),
    )


async def declare_topology(broker: RabbitBroker, topology: Topology) -> None:
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx = await broker.declare_exchange(DLX_EXCHANGE)

    for queue, exchange in [
        (topology.new, payments),
        (topology.dlq, dlx),
        *((retry, payments) for retry in topology.retries),
    ]:
        declared = await broker.declare_queue(queue)
        await declared.bind(exchange, routing_key=queue.routing_key)
