from __future__ import annotations

import logging

from faststream.rabbit import Channel, RabbitBroker

from app.core.config import Settings

# зависший publish в relay держит транзакцию, в консьюмере - слот prefetch
PUBLISH_TIMEOUT_SECONDS = 10.0


def make_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        graceful_timeout=30.0,  # больше максимальной задержки шлюза
        # встроенный логгер FastStream пишет своим форматом и без propagate
        logger=logging.getLogger("app.broker"),
        # on_return_raises: publish без подходящей очереди - ошибка, а не тихий успех
        default_channel=Channel(
            prefetch_count=settings.consumer_prefetch, publisher_confirms=True, on_return_raises=True
        ),
    )
