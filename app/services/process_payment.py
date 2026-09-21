"""Обработка платежа: шлюз -> статус в базе -> webhook.

Стадии определяются по состоянию в базе, поэтому повтор сообщения продолжает
с первой незавершённой. Гарантия at-least-once: упав между действием и его отметкой,
стадию выполним ещё раз.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from app.core.db import session_scope, transaction
from app.core.logging import get_logger
from app.models import PaymentStatus
from app.repositories.payments import PaymentRepository
from app.schemas.payments import WebhookPayload
from app.services.gateway import PaymentGateway
from app.services.webhook import WebhookSender

log = get_logger(__name__)


class PaymentNotFoundError(Exception):
    pass


class Outcome(StrEnum):
    PROCESSED = "processed"
    ALREADY_DONE = "already_done"  # повтор, ничего не менялось
    TAKEN_OVER = "taken_over"  # платёж параллельно провёл другой consumer


@dataclass(slots=True)
class ProcessingDeps:
    gateway: PaymentGateway
    webhooks: WebhookSender


async def process_payment(payment_id: uuid.UUID, *, attempt: int, deps: ProcessingDeps) -> Outcome:
    """Любое исключение = попытка не удалась. HTTP только вне транзакций."""
    async with session_scope() as session:
        payments = PaymentRepository(session)
        async with transaction(session):
            payment = await payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(str(payment_id))

        changed = False

        if payment.status == PaymentStatus.PENDING:
            result = await deps.gateway.charge(
                payment_id=payment.id, amount=payment.amount, currency=payment.currency
            )
            status = PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED
            async with transaction(session):
                applied = await payments.finish_if_pending(
                    payment.id,
                    status=status,
                    processed_at=datetime.now(UTC),
                    failure_reason=result.failure_reason,
                )
                await session.refresh(payment)
            if not applied:
                # тот же платёж обрабатывает другой consumer, webhook тоже за ним
                log.warning("payment.taken_over", payment_id=str(payment.id))
                return Outcome.TAKEN_OVER
            log.info("payment.gateway_result", payment_id=str(payment.id), status=status, attempt=attempt)
            changed = True

        if payment.webhook_delivered_at is None:
            payload = WebhookPayload.model_validate(payment, from_attributes=True)
            code = await deps.webhooks.send(payment.webhook_url, payload, attempt=attempt)
            async with transaction(session):
                await payments.mark_webhook_delivered(payment.id, datetime.now(UTC))
            log.info(
                "payment.webhook_delivered", payment_id=str(payment.id), http_status=code, attempt=attempt
            )
            changed = True

        return Outcome.PROCESSED if changed else Outcome.ALREADY_DONE
