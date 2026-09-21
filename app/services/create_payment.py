from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentCreate

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool  # платёж уже был создан запросом с тем же ключом


def request_fingerprint(data: PaymentCreate) -> str:
    # по нормализованным данным, а не по сырым байтам: "10" и 10.0 - один запрос
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> CreatePaymentResult:
    """Платёж + событие в outbox одной транзакцией. Вызывать внутри transaction()."""
    payments = PaymentRepository(session)
    fingerprint = request_fingerprint(data)

    payment = await payments.insert_if_absent(
        payment_id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata=data.metadata,
        webhook_url=data.webhook_url,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
    )
    if payment is not None:
        OutboxRepository(session).add(
            event_type=PAYMENT_CREATED_EVENT, payload={"payment_id": str(payment.id)}
        )
        return CreatePaymentResult(payment=payment, replayed=False)

    existing = await payments.get_by_idempotency_key(idempotency_key)
    if existing is None:
        # платежи не удаляются, так что это сбой, а не сценарий
        raise RuntimeError(f"Платёж с ключом {idempotency_key!r} исчез между INSERT и SELECT")
    if existing.request_hash != fingerprint:
        raise IdempotencyConflictError(idempotency_key)
    return CreatePaymentResult(payment=existing, replayed=True)
