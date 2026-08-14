from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict

import pytest

from src.adapters.base import AdapterError, BackendTimeoutError, BaseAdapter
from src.adapters.plugin_loader import AdapterManager
from src.core.circuit_breaker import CircuitBreakerManager
from src.core.registry import ServiceRegistry
from src.core.router import CapabilityRouter
from src.models.gateway_api import ExecuteRequest


class FakeAdapter(BaseAdapter):
    def __init__(
        self,
        service_id: str,
        protocol: str,
        *,
        result: Dict[str, Any] | None = None,
        error: AdapterError | None = None,
    ) -> None:
        self._service_id = service_id
        self._protocol = protocol
        self.result = result or {"value": service_id}
        self.error = error
        self.calls = 0

    @property
    def service_id(self) -> str:
        return self._service_id

    @property
    def protocol_name(self) -> str:
        return self._protocol

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        self.calls += 1
        if self.error:
            raise self.error
        await asyncio.sleep(0)
        return self.result


async def build_router(tmp_path: Path):
    registry = ServiceRegistry(tmp_path / "registry.json")
    manager = AdapterManager()
    router = CapabilityRouter(registry, manager, CircuitBreakerManager())
    return registry, manager, router


def request(operation: str, preferred: str | None = None) -> ExecuteRequest:
    arguments = {"value": "x"} if operation != "sum" else {"values": [1, 2]}
    return ExecuteRequest.model_validate(
        {
            "request_id": f"request-{operation}",
            "operation": operation,
            "arguments": arguments,
            "options": {"preferred_service": preferred, "timeout_ms": 1000},
        }
    )


@pytest.mark.asyncio
async def test_router_uses_persisted_capability_metadata(tmp_path: Path) -> None:
    registry, manager, router = await build_router(tmp_path)
    await registry.update_metadata("service-c", capabilities=["echo", "metadata"])
    fake_b = FakeAdapter("service-b", "tcp-frame-json", result={"value": 3})
    await manager.register_adapter("service-b", fake_b, expected_protocol="tcp-frame-json")

    response = await router.execute_request(request("sum"))
    assert response.status == "success"
    assert response.service_id == "service-b"
    assert fake_b.calls == 1


@pytest.mark.asyncio
async def test_safe_pre_dispatch_failure_falls_back(tmp_path: Path) -> None:
    _, manager, router = await build_router(tmp_path)
    fake_a = FakeAdapter(
        "service-a",
        "http-json",
        error=AdapterError(
            "BACKEND_UNAVAILABLE",
            "connect failed",
            retryable=True,
            safe_to_fallback=True,
        ),
    )
    fake_b = FakeAdapter("service-b", "tcp-frame-json")
    await manager.register_adapter("service-a", fake_a, expected_protocol="http-json")
    await manager.register_adapter("service-b", fake_b, expected_protocol="tcp-frame-json")

    response = await router.execute_request(request("echo", "service-a"))
    assert response.status == "success"
    assert response.service_id == "service-b"


@pytest.mark.asyncio
async def test_ambiguous_timeout_does_not_duplicate_execution(tmp_path: Path) -> None:
    _, manager, router = await build_router(tmp_path)
    fake_a = FakeAdapter(
        "service-a",
        "http-json",
        error=BackendTimeoutError("read timed out", safe_to_fallback=False),
    )
    fake_b = FakeAdapter("service-b", "tcp-frame-json")
    await manager.register_adapter("service-a", fake_a, expected_protocol="http-json")
    await manager.register_adapter("service-b", fake_b, expected_protocol="tcp-frame-json")

    response = await router.execute_request(request("echo", "service-a"))
    assert response.status == "error"
    assert response.error is not None and response.error.code == "BACKEND_TIMEOUT"
    assert fake_a.calls == 1
    assert fake_b.calls == 0
