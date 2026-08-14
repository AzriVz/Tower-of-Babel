from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest

from src.adapters.base import BaseAdapter
from src.adapters.plugin_loader import AdapterManager
from src.admin.migration_api import MigrationRequest, handle_protocol_migration
from src.core.registry import ServiceRegistry


class VersionAwareAdapter(BaseAdapter):
    def __init__(self, accepted_versions: set[str]) -> None:
        self.accepted_versions = accepted_versions

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
        if version not in self.accepted_versions:
            raise ValueError("unsupported version")
        return {"version": version}

    async def health(self, timeout_s: float, version: str = "1") -> bool:
        return version in self.accepted_versions


@pytest.mark.asyncio
async def test_successful_migration_keeps_both_versions(tmp_path: Path) -> None:
    registry = ServiceRegistry(tmp_path / "registry.json")
    await registry.load_state()
    manager = AdapterManager()
    adapter = VersionAwareAdapter({"1", "2"})
    await manager.register_adapter("service-a", adapter, expected_protocol="http-json")

    response = await handle_protocol_migration(
        "service-a",
        MigrationRequest(target_version="2"),
        registry,
        manager,
    )
    assert response.status == "success"
    assert response.current_version == "2"
    assert response.available_versions == ["1", "2"]
    restored = ServiceRegistry(tmp_path / "registry.json")
    await restored.load_state()
    entry = await restored.get_service("service-a")
    assert entry is not None and entry.version == "2"


@pytest.mark.asyncio
async def test_failed_canary_preserves_old_version(tmp_path: Path) -> None:
    registry = ServiceRegistry(tmp_path / "registry.json")
    await registry.load_state()
    manager = AdapterManager()
    adapter = VersionAwareAdapter({"1"})
    await manager.register_adapter("service-a", adapter, expected_protocol="http-json")

    response = await handle_protocol_migration(
        "service-a",
        MigrationRequest(target_version="2"),
        registry,
        manager,
    )
    assert response.status == "error"
    assert response.current_version == "1"
    assert await manager.get_adapter("service-a", "2") is None
    entry = await registry.get_service("service-a")
    assert entry is not None and entry.version == "1"
