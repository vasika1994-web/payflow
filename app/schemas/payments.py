"""Схемы API и тела webhook-уведомления."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.models.enums import Currency, PaymentStatus

# NUMERIC(18,2): предел ниже, чтобы получать 422, а не ошибку базы
MAX_AMOUNT = Decimal("9999999999999.99")
MAX_DESCRIPTION_LENGTH = 1_000
MAX_METADATA_BYTES = 16 * 1024


class PaymentCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.00",
                    "currency": "RUB",
                    "description": "Заказ #1042",
                    "metadata": {"order_id": 1042, "customer": "ivan@example.com"},
                    "webhook_url": "http://webhook-sink:9000/webhook",
                }
            ]
        },
    )

    amount: Decimal = Field(gt=0, le=MAX_AMOUNT, decimal_places=2, examples=["1500.00"])
    currency: Currency
    description: str = Field(default="", max_length=MAX_DESCRIPTION_LENGTH)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        # 1500, 1500.0 и "1500.00" должны давать один отпечаток для идемпотентности
        return value.quantize(Decimal("0.01"))

    @field_validator("metadata")
    @classmethod
    def limit_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata не должно превышать {MAX_METADATA_BYTES} байт в JSON")
        return value


class PaymentAccepted(BaseModel):
    """Ответ 202: платёж принят, обрабатывается асинхронно."""

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


class WebhookPayload(BaseModel):
    """Тело уведомления, которое консьюмер отправляет на webhook_url."""

    event: str = "payment.processed"
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime | None
