"""Consumer очереди payments.new. Запуск: faststream run app.consumer.main:app

REJECT_ON_ERROR: вернулись - ack, исключение - reject без requeue, брокер уводит
сообщение в DLQ. Исключение наружу выходит только если не удалось опубликовать
retry/DLQ самим handler'ом.

Сообщение: {"payment_id", "attempt", "last_error"}. Попытка и ошибка дублируются
в заголовках, чтобы их было видно в management UI без разбора тела.
"""

from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from faststream import AckPolicy, Context, ContextRepo, FastStream
from faststream.rabbit import RabbitMessage
from pydantic import BaseModel, Field

from app.broker.connection import PUBLISH_TIMEOUT_SECONDS, make_broker
from app.broker.topology import DLX_EXCHANGE, PAYMENTS_EXCHANGE, build_topology, declare_topology
from app.core.config import get_settings
from app.core.db import dispose_engine
from app.core.logging import configure_logging, get_logger
from app.services.gateway import EmulatedGateway
from app.services.process_payment import PaymentNotFoundError, ProcessingDeps, process_payment
from app.services.webhook import WebhookSender, make_http_client

log = get_logger(__name__)

settings = get_settings()
broker = make_broker(settings)
topology = build_topology(settings)
app = FastStream(broker)


class PaymentMessage(BaseModel):
    payment_id: uuid.UUID
    attempt: int = Field(default=1, ge=1)
    last_error: str | None = None


# retry_publishers[n] - куда класть сообщение после неудачной попытки n+1
retry_publishers = [
    broker.publisher(
        exchange=PAYMENTS_EXCHANGE,
        routing_key=queue.routing_key,
        persist=True,
        timeout=PUBLISH_TIMEOUT_SECONDS,
        title=f"Повтор через {queue.name.rsplit('.', 1)[-1]}",
    )
    for queue in topology.retries
]
dlq_publisher = broker.publisher(
    exchange=DLX_EXCHANGE,
    routing_key=topology.dlq.routing_key,
    persist=True,
    timeout=PUBLISH_TIMEOUT_SECONDS,
    title="Dead letter queue",
)


@app.on_startup
async def startup(context: ContextRepo) -> None:
    configure_logging(settings.log_level, json_output=settings.env != "development")
    deps = ProcessingDeps(
        gateway=EmulatedGateway(
            delay_min=settings.gateway_delay_min_seconds,
            delay_max=settings.gateway_delay_max_seconds,
            success_rate=settings.gateway_success_rate,
            unavailable_rate=settings.gateway_unavailable_rate,
        ),
        webhooks=WebhookSender(make_http_client(settings.webhook_timeout_seconds)),
    )
    context.set_global("deps", deps)
    # retry-очереди должны существовать до первого сообщения
    await broker.connect()
    await declare_topology(broker, topology)
    log.info(
        "consumer.started",
        max_attempts=settings.consumer_max_attempts,
        retry_queues=[q.name for q in topology.retries],
    )


@app.on_shutdown
async def shutdown(context: ContextRepo) -> None:
    deps: ProcessingDeps | None = context.get("deps")
    if deps is not None:
        await deps.webhooks.aclose()
    await dispose_engine()


@broker.subscriber(
    topology.new,
    PAYMENTS_EXCHANGE,
    ack_policy=AckPolicy.REJECT_ON_ERROR,
    title="Обработка платежа",
    description="Проводит платёж через шлюз, сохраняет статус и уведомляет клиента.",
)
async def handle_new_payment(
    body: PaymentMessage,
    message: RabbitMessage,
    deps: Annotated[ProcessingDeps, Context()],
) -> None:
    attempt = effective_attempt(body.attempt, redelivered=message.raw_message.redelivered)
    structlog.contextvars.bind_contextvars(payment_id=str(body.payment_id), attempt=attempt)
    try:
        outcome = await process_payment(body.payment_id, attempt=attempt, deps=deps)
        log.info("payment.done", outcome=outcome)
    except PaymentNotFoundError as exc:
        # повтор не поможет
        await send_to_dlq(body, attempt=attempt, error=f"PaymentNotFoundError: {exc}")
    except Exception as exc:
        await schedule_retry_or_dlq(body, attempt=attempt, error=f"{type(exc).__name__}: {exc}")
    finally:
        structlog.contextvars.unbind_contextvars("payment_id", "attempt")


def effective_attempt(attempt: int, *, redelivered: bool) -> int:
    # redelivered = процесс упал, не подтвердив сообщение (при этом флаг получат все prefetch'нутые).
    # Считаем попыткой, иначе poison message крутился бы вечно
    return attempt + 1 if redelivered else attempt


async def schedule_retry_or_dlq(body: PaymentMessage, *, attempt: int, error: str) -> None:
    index = attempt - 1
    if index >= len(retry_publishers):
        log.error("payment.attempts_exhausted", error=error, attempts=attempt)
        await send_to_dlq(body, attempt=attempt, error=error)
        return

    next_attempt = attempt + 1
    log.warning(
        "payment.retry_scheduled", error=error, next_attempt=next_attempt, queue=topology.retries[index].name
    )
    await retry_publishers[index].publish(
        PaymentMessage(payment_id=body.payment_id, attempt=next_attempt, last_error=error),
        headers={"x-attempt": str(next_attempt), "x-last-error": error},
    )


async def send_to_dlq(body: PaymentMessage, *, attempt: int, error: str) -> None:
    await dlq_publisher.publish(
        PaymentMessage(payment_id=body.payment_id, attempt=attempt, last_error=error),
        headers={"x-attempt": str(attempt), "x-last-error": error},
    )
