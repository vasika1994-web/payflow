from __future__ import annotations

import asyncio
import hashlib
import random
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from app.models import Currency


class GatewayUnavailableError(Exception):
    """Шлюз не ответил, исход неизвестен, нужен retry."""


@dataclass(frozen=True, slots=True)
class GatewayResult:
    succeeded: bool
    failure_reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(
        self, *, payment_id: uuid.UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult: ...


class EmulatedGateway:
    """Эмуляция по ТЗ: 2-5 секунд, 90% успеха.

    Исход зависит от payment_id, а не от броска монеты: настоящий шлюз по ключу
    идемпотентности вернёт тот же результат на повторный запрос, эмулятор тоже.
    Отказ (declined) - штатный исход, платёж станет failed. Недоступность
    (unavailable_rate, по умолчанию 0) - исключение и retry.
    """

    def __init__(
        self,
        *,
        delay_min: float,
        delay_max: float,
        success_rate: float,
        unavailable_rate: float = 0.0,
        rng: random.Random | None = None,
    ) -> None:
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.success_rate = success_rate
        self.unavailable_rate = unavailable_rate
        self.rng = rng or random.Random()

    async def charge(self, *, payment_id: uuid.UUID, amount: Decimal, currency: Currency) -> GatewayResult:
        await asyncio.sleep(self.rng.uniform(self.delay_min, self.delay_max))
        if self.rng.random() < self.unavailable_rate:
            raise GatewayUnavailableError(f"шлюз не ответил на платёж {payment_id}")
        if _roll(payment_id) < self.success_rate:
            return GatewayResult(succeeded=True)
        return GatewayResult(succeeded=False, failure_reason="declined by gateway")


def _roll(payment_id: uuid.UUID) -> float:
    """Число в [0, 1), одно и то же для одного payment_id."""
    digest = hashlib.sha256(payment_id.bytes).digest()
    return int.from_bytes(digest[:8], "big") / 2**64
