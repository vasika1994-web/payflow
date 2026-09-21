from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            get_settings().database_url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False, autoflush=False)
    return _session_factory


async def dispose_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    factory = get_session_factory()
    async with factory() as session:
        yield session


@asynccontextmanager
async def transaction(session: AsyncSession) -> AsyncIterator[AsyncSession]:
    """Транзакция с таймаутами. Единственный способ открыть транзакцию в проекте.

    Внутри блока не должно быть сетевых вызовов: они держат соединение и блокировки.
    """
    settings = get_settings()
    if session.in_transaction():
        # autobegin: первое же чтение до блока открыло бы транзакцию без таймаутов
        raise RuntimeError("На сессии уже открыта транзакция, перенесите чтение внутрь transaction().")
    async with session.begin():
        # SET LOCAL не принимает bind-параметры
        await session.execute(text(f"SET LOCAL statement_timeout = {int(settings.statement_timeout_ms)}"))
        await session.execute(text(f"SET LOCAL lock_timeout = {int(settings.lock_timeout_ms)}"))
        idle_timeout = int(settings.idle_in_transaction_timeout_ms)
        await session.execute(text(f"SET LOCAL idle_in_transaction_session_timeout = {idle_timeout}"))
        yield session
