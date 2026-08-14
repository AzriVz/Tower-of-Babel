from __future__ import annotations

import pytest

from src.core.circuit_breaker import CircuitBreaker, CircuitState


@pytest.mark.asyncio
async def test_success_resets_closed_failure_count() -> None:
    breaker = CircuitBreaker(failure_threshold=5)
    for _ in range(4):
        await breaker.record_failure()
    await breaker.record_success()
    await breaker.record_failure()
    snapshot = await breaker.snapshot()
    assert snapshot["state"] == CircuitState.CLOSED
    assert snapshot["failure_count"] == 1


@pytest.mark.asyncio
async def test_half_open_allows_only_one_probe() -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_time_s=0)
    await breaker.record_failure()
    assert await breaker.allow_request() is True
    assert await breaker.allow_request() is False
    await breaker.record_success()
    assert await breaker.allow_request() is True
