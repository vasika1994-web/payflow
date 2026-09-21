from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, *, event_type: str, payload: dict[str, Any]) -> OutboxEvent:
        event = OutboxEvent(event_type=event_type, payload=payload)
        self.session.add(event)
        await self.session.flush()
        return event

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        # SKIP LOCKED: несколько relay не возьмут одну и ту же строку
        stmt = (
            sa.select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self.session.execute(stmt)).scalars().all()

    async def mark_published(self, event_id: int, published_at: datetime) -> None:
        stmt = sa.update(OutboxEvent).where(OutboxEvent.id == event_id).values(published_at=published_at)
        await self.session.execute(stmt)

    async def record_failure(self, event_id: int, error: str) -> None:
        stmt = (
            sa.update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(publish_attempts=OutboxEvent.publish_attempts + 1, last_error=error[:1_000])
        )
        await self.session.execute(stmt)
