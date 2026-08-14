from __future__ import annotations

from typing import Any, Dict
import os

import pytest

from src.adapters.base import BaseAdapter
from src.adapters.plugin_loader import AdapterManager
from src.adapters.base import AdapterError, BackendTimeoutError
from src.adapters.subprocess_proxy import SubprocessAdapterProxy
from src.core.registry import ServiceEntry


class WrongServiceAdapter(BaseAdapter):
    @property
    def service_id(self) -> str:
        return "service-c"

    @property
    def protocol_name(self) -> str:
        return "http-json"

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        return {}


class GoodServiceAAdapter(BaseAdapter):
    @property
    def service_id(self) -> str:
        return "service-a"

    @property
    def protocol_name(self) -> str:
        return "http-json"

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        return {"plugin": True}


class CrashingServiceAAdapter(GoodServiceAAdapter):
    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        os._exit(7)


class BlockingServiceAAdapter(GoodServiceAAdapter):
    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        while True:
            pass


@pytest.mark.asyncio
async def test_broken_adapter_is_rejected_without_replacing_old() -> None:
    manager = AdapterManager()
    original = await manager.get_adapter("service-a", "1")
    accepted = await manager.register_adapter(
        "service-a",
        WrongServiceAdapter(),
        version="1",
        expected_protocol="http-json",
    )
    assert accepted is False
    assert await manager.get_adapter("service-a", "1") is original


@pytest.mark.asyncio
async def test_persisted_plugin_descriptor_is_restored() -> None:
    manager = AdapterManager()
    entry = ServiceEntry(
        service_id="service-a",
        protocol="http-json",
        endpoint="http://unused:8101",
        capabilities=["echo"],
        version="2",
        meta={
            "adapter_plugins": {
                "2": {
                    "module_path": "tests.unit.test_plugin_loader",
                    "class_name": "GoodServiceAAdapter",
                }
            }
        },
    )
    await manager.configure_from_registry([entry])
    restored = await manager.get_adapter("service-a", "2")
    assert isinstance(restored, SubprocessAdapterProxy)
    assert await manager.get_adapter_spec("service-a", "2") == {
        "module_path": "tests.unit.test_plugin_loader",
        "class_name": "GoodServiceAAdapter",
    }


@pytest.mark.asyncio
async def test_crashing_plugin_process_does_not_crash_gateway_process() -> None:
    proxy = await SubprocessAdapterProxy.create(
        "tests.unit.test_plugin_loader",
        "CrashingServiceAAdapter",
    )
    with pytest.raises(AdapterError, match="status 7"):
        await proxy.execute("echo", {"value": "x"}, "request", 1)


@pytest.mark.asyncio
async def test_blocking_plugin_process_is_killed_at_deadline() -> None:
    proxy = await SubprocessAdapterProxy.create(
        "tests.unit.test_plugin_loader",
        "BlockingServiceAAdapter",
        timeout_s=10,
    )
    with pytest.raises(BackendTimeoutError):
        await proxy.execute("echo", {"value": "x"}, "request", 0.1)
