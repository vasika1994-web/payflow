"""Outbox relay: переносит события из таблицы outbox в RabbitMQ. Запуск: python -m app.outbox.relay

Берём пачку под FOR UPDATE SKIP LOCKED, публикуем с confirm, ставим published_at, коммитим.
Publish идёт внутри транзакции, чтобы строку не забрал второй relay. At-least-once:
упав между confirm и коммитом, отправим событие ещё раз.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker

from app.broker.connection import PUBLISH_TIMEOUT_SECONDS, make_broker
from app.broker.topology import NEW_ROUTING_KEY, PAYMENTS_EXCHANGE, build_topology, declare_topology
from app.core.config import Settings, get_settings
from app.core.db import dispose_engine, session_scope, transaction
from app.core.logging import configure_logging, get_logger
from app.models import OutboxEvent
from app.repositories.outbox import OutboxRepository

log = get_logger(__name__)


async def publish_event(broker: RabbitBroker, event: OutboxEvent) -> None:
    await broker.publish(
        event.payload,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_ROUTING_KEY,
        persist=True,
        mandatory=True,  # нет очереди - исключение, см. on_return_raises
        timeout=PUBLISH_TIMEOUT_SECONDS,
        message_id=str(event.id),
        headers={"x-attempt": "1", "x-event-type": event.event_type},
    )


async def relay_once(broker: RabbitBroker, settings: Settings) -> int:
    """Один проход. Возвращает число опубликованных событий."""
    published_ids: list[int] = []
    failed: tuple[int, str] | None = None

    async with session_scope() as session:
        outbox = OutboxRepository(session)
        async with transaction(session):
            events = await outbox.lock_unpublished(settings.outbox_batch_size)
            for event in events:
                try:
                    await publish_event(broker, event)
                except Exception as exc:
                    failed = (event.id, f"{type(exc).__name__}: {exc}")
                    break
                published_ids.append(event.id)

            now = datetime.now(UTC)
            for event_id in published_ids:
                await outbox.mark_published(event_id, now)

        if failed is not None:
            event_id, error = failed
            log.error("outbox.publish_failed", event_id=event_id, error=error)
            async with transaction(session):
                await outbox.record_failure(event_id, error)

    if published_ids:
        log.info("outbox.published", count=len(published_ids), last_id=published_ids[-1])
    return len(published_ids)


async def run(stop: asyncio.Event) -> None:
    settings = get_settings()
    broker = make_broker(settings)
    await broker.connect()
    await declare_topology(broker, build_topology(settings))
    log.info("relay.started", poll_interval=settings.outbox_poll_interval_seconds)
    try:
        while not stop.is_set():
            try:
                count = await relay_once(broker, settings)
            except Exception as exc:
                log.error("relay.tick_failed", error=f"{type(exc).__name__}: {exc}")
                count = 0
            if count < settings.outbox_batch_size:
                # полная пачка = есть ещё, идём сразу
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval_seconds)
    finally:
        await broker.stop()
        await dispose_engine()
        log.info("relay.stopped")


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, json_output=settings.env != "development")

    async def _main() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await run(stop)

    asyncio.run(_main())


if __name__ == "__main__":
    main()
