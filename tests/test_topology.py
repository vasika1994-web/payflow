from __future__ import annotations

from app.broker.topology import DLX_EXCHANGE, PAYMENTS_EXCHANGE, build_topology
from app.core.config import Settings


def test_retry_queues_follow_exponential_delays():
    settings = Settings(consumer_max_attempts=4, retry_base_delay_seconds=1.5)
    topology = build_topology(settings)

    assert settings.retry_delays_seconds() == (1.5, 3.0, 6.0)
    assert [q.name for q in topology.retries] == [
        "payments.retry.1.5s",
        "payments.retry.3s",
        "payments.retry.6s",
    ]
    for queue, ttl in zip(topology.retries, (1500, 3000, 6000), strict=True):
        assert queue.arguments["x-message-ttl"] == ttl
        assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE.name
        assert queue.arguments["x-dead-letter-routing-key"] == "payments.new"


def test_single_attempt_means_no_retry_queues():
    assert build_topology(Settings(consumer_max_attempts=1)).retries == ()


def test_main_queue_dead_letters_into_dlq():
    topology = build_topology(Settings())

    assert topology.new.arguments["x-dead-letter-exchange"] == DLX_EXCHANGE.name
    assert topology.new.arguments["x-dead-letter-routing-key"] == topology.dlq.routing_key
