from __future__ import annotations

from pathlib import Path

import pytest

from src.core.registry import ServiceRegistry


@pytest.mark.asyncio
async def test_registry_metadata_survives_restart(tmp_path: Path) -> None:
    state_file = tmp_path / "registry.json"
    first = ServiceRegistry(state_file)
    await first.load_state()
    assert await first.update_metadata(
        "service-b",
        capabilities=["echo", "sum"],
        meta={"routing_priority": 7},
    )

    second = ServiceRegistry(state_file)
    await second.load_state()
    restored = await second.get_service("service-b")
    assert restored is not None
    assert restored.capabilities == ["echo", "sum"]
    assert restored.meta == {"routing_priority": 7}


@pytest.mark.asyncio
async def test_registry_returns_copies(tmp_path: Path) -> None:
    registry = ServiceRegistry(tmp_path / "registry.json")
    entry = await registry.get_service("service-a")
    assert entry is not None
    entry.capabilities.clear()
    unchanged = await registry.get_service("service-a")
    assert unchanged is not None
    assert "echo" in unchanged.capabilities
