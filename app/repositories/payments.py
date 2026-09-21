from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Currency, Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_if_absent(
        self,
        *,
        payment_id: uuid.UUID,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
        idempotency_key: str,
        request_hash: str,
    ) -> Payment | None:
        """INSERT ... ON CONFLICT DO NOTHING. None, если ключ уже занят."""
        stmt = (
            pg_insert(Payment)
            .values(
                id=payment_id,
                amount=amount,
                currency=currency,
                description=description,
                metadata_=metadata,
                webhook_url=webhook_url,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = sa.select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def finish_if_pending(
        self,
        payment_id: uuid.UUID,
        *,
        status: PaymentStatus,
        processed_at: datetime,
        failure_reason: str | None,
    ) -> bool:
        """Условный UPDATE: если платёж уже провёл другой консьюмер, вернёт False."""
        stmt = (
            sa.update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at, failure_reason=failure_reason)
        )
        result = await self.session.execute(stmt)
        return result.rowcount == 1

    async def mark_webhook_delivered(self, payment_id: uuid.UUID, delivered_at: datetime) -> None:
        stmt = (
            sa.update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=delivered_at)
        )
        await self.session.execute(stmt)
