from __future__ import annotations

import pytest

from app.core.config import DEMO_API_KEY, Settings


def test_production_refuses_demo_api_key():
    with pytest.raises(ValueError, match="API_KEY"):
        Settings(env="production", api_key=DEMO_API_KEY)


def test_production_accepts_own_key():
    assert Settings(env="production", api_key="s3cret-enough").api_key == "s3cret-enough"


def test_gateway_delay_bounds_are_checked():
    with pytest.raises(ValueError, match="GATEWAY_DELAY"):
        Settings(gateway_delay_min_seconds=5, gateway_delay_max_seconds=2)


def test_unknown_env_is_rejected():
    with pytest.raises(ValueError):
        Settings(env="Production")
