from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class CircuitState:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_time_s: float = 5.0,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_time_s = recovery_time_s
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_state_change = time.monotonic()
        self._half_open_probe_in_flight = False
        self._lock = asyncio.Lock()

    async def allow_request(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            if self.state == CircuitState.OPEN:
                if now - self.last_state_change >= self.recovery_time_s:
                    self.state = CircuitState.HALF_OPEN
                    self.last_state_change = now
                    self._half_open_probe_in_flight = True
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                    return True
                return False
            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_probe_in_flight:
                    return False
                self._half_open_probe_in_flight = True
                return True
            return True

    async def record_success(self) -> None:
        async with self._lock:
            if self.state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
                logger.info("Circuit breaker recovering to CLOSED")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self._half_open_probe_in_flight = False
            self.last_state_change = time.monotonic()

    async def record_failure(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.failure_count = self.failure_threshold
            else:
                self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                if self.state != CircuitState.OPEN:
                    logger.warning(f"Circuit breaker tripped to OPEN after {self.failure_count} failures")
                self.state = CircuitState.OPEN
                self._half_open_probe_in_flight = False
                self.last_state_change = time.monotonic()

    async def snapshot(self) -> Dict[str, object]:
        async with self._lock:
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "failure_threshold": self.failure_threshold,
            }


class CircuitBreakerManager:
    def __init__(self) -> None:
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    async def get_breaker(self, service_id: str) -> CircuitBreaker:
        async with self._lock:
            if service_id not in self._breakers:
                self._breakers[service_id] = CircuitBreaker()
            return self._breakers[service_id]

    async def states(self) -> Dict[str, str]:
        async with self._lock:
            items = list(self._breakers.items())
        result: Dict[str, str] = {}
        for service_id, breaker in items:
            snapshot = await breaker.snapshot()
            result[service_id] = str(snapshot["state"])
        return result
