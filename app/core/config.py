from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEMO_API_KEY = "local-dev-api-key"


class Settings(BaseSettings):
    """Общие настройки для api, consumer и outbox-relay. Дефолты рассчитаны на локальный запуск."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    env: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://postgres:password@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    api_key: str = DEMO_API_KEY
    # Подставлять ключ в Swagger. Ключ попадает в HTML /docs, так что только для dev/демо.
    docs_preauthorize_api_key: bool = False

    statement_timeout_ms: int = Field(default=5_000, ge=100)
    lock_timeout_ms: int = Field(default=3_000, ge=100)
    idle_in_transaction_timeout_ms: int = Field(default=30_000, ge=1_000)

    # Тело буферизуется целиком до проверки ключа, поэтому предел нужен.
    max_body_bytes: int = Field(default=256 * 1024, ge=1024, le=32 * 1024 * 1024)

    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)

    # Эмуляция шлюза
    gateway_delay_min_seconds: float = Field(default=2.0, ge=0)
    gateway_delay_max_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    # Доля вызовов, где шлюз "не отвечает" (исключение -> retry). Нужно чтобы показать retry на шлюзе.
    gateway_unavailable_rate: float = Field(default=0.0, ge=0, le=1)

    # Всего попыток, включая первую. При 3 будет два повтора: через 1 и 2 секунды.
    consumer_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    consumer_prefetch: int = Field(default=10, ge=1)

    webhook_timeout_seconds: float = Field(default=5.0, gt=0)

    @model_validator(mode="after")
    def check_gateway_delay(self) -> Settings:
        if self.gateway_delay_max_seconds < self.gateway_delay_min_seconds:
            raise ValueError("GATEWAY_DELAY_MAX_SECONDS не может быть меньше GATEWAY_DELAY_MIN_SECONDS")
        return self

    @model_validator(mode="after")
    def refuse_unsafe_production(self) -> Settings:
        # С демо-ключом в прод не выпускаем
        if self.env == "production" and self.api_key == DEMO_API_KEY:
            raise ValueError("API_KEY по умолчанию годится только для локального запуска. Задайте свой.")
        return self

    def retry_delays_seconds(self) -> tuple[float, ...]:
        """1, 2, 4, ... секунд; повторов на один меньше, чем попыток."""
        return tuple(self.retry_base_delay_seconds * 2**n for n in range(self.consumer_max_attempts - 1))


@lru_cache
def get_settings() -> Settings:
    return Settings()
