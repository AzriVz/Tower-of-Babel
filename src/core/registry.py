from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
from typing import Any, Dict, List, Optional
from pathlib import Path

from src.core.config import settings

logger = logging.getLogger(__name__)


class ServiceEntry:
    def __init__(
        self,
        service_id: str,
        protocol: str,
        endpoint: str,
        capabilities: List[str],
        status: str = "available",
        version: str = "1",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.service_id = service_id
        self.protocol = protocol
        self.endpoint = endpoint
        self.capabilities = capabilities
        self.status = status
        self.version = version
        self.meta = meta or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service_id": self.service_id,
            "protocol": self.protocol,
            "endpoint": self.endpoint,
            "capabilities": list(self.capabilities),
            "status": self.status,
            "version": self.version,
            "meta": copy.deepcopy(self.meta),
        }

    def copy(self) -> "ServiceEntry":
        return ServiceEntry.from_dict(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ServiceEntry:
        required = {"service_id", "protocol", "endpoint"}
        missing = required - set(data)
        if missing:
            raise ValueError(f"Registry entry missing fields: {sorted(missing)}")
        capabilities = data.get("capabilities", [])
        if not isinstance(capabilities, list) or not all(
            isinstance(item, str) and item for item in capabilities
        ):
            raise ValueError("Registry capabilities must be a list of non-empty strings")
        status = data.get("status", "unknown")
        if status not in {"available", "unavailable", "unknown"}:
            raise ValueError(f"Invalid registry health status '{status}'")
        version = str(data.get("version", "1"))
        if not version.isdigit() or not 1 <= int(version) <= 255:
            raise ValueError(f"Invalid registry protocol version '{version}'")
        meta = data.get("meta", {})
        if not isinstance(meta, dict):
            raise ValueError("Registry meta must be an object")
        return cls(
            service_id=data["service_id"],
            protocol=data["protocol"],
            endpoint=data["endpoint"],
            capabilities=capabilities,
            status=status,
            version=version,
            meta=meta,
        )


class ServiceRegistry:
    def __init__(self, persistence_file: Optional[Path] = None) -> None:
        self.persistence_file = persistence_file or settings.REGISTRY_FILE
        self._lock = asyncio.Lock()
        self._services: Dict[str, ServiceEntry] = {}
        self._init_defaults()

    def _init_defaults(self) -> None:
        # Default registered backends per specification
        defaults = [
            ServiceEntry(
                service_id="service-a",
                protocol="http-json",
                endpoint=settings.SERVICE_A_URL,
                capabilities=["echo", "metadata", "uppercase"],
                version="1",
            ),
            ServiceEntry(
                service_id="service-b",
                protocol="tcp-frame-json",
                endpoint=f"{settings.SERVICE_B_HOST}:{settings.SERVICE_B_PORT}",
                capabilities=["echo", "metadata", "reverse", "sum", "uppercase"],
                version="1",
            ),
            ServiceEntry(
                service_id="service-c",
                protocol="udp-crc-json",
                endpoint=f"{settings.SERVICE_C_HOST}:{settings.SERVICE_C_PORT}",
                capabilities=["echo", "metadata", "sum"],
                version="1",
            ),
        ]
        for entry in defaults:
            self._services[entry.service_id] = entry

    async def load_state(self) -> None:
        async with self._lock:
            if not self.persistence_file or not self.persistence_file.exists():
                logger.info("No persisted registry found. Using default service configurations.")
                await self._persist_unlocked()
                return
            try:
                content = self.persistence_file.read_text(encoding="utf-8")
                data = json.loads(content)
                services = data.get("services")
                if not isinstance(services, list):
                    raise ValueError("Persisted registry must contain a services array")
                loaded: Dict[str, ServiceEntry] = {}
                for item in services:
                    if not isinstance(item, dict):
                        raise ValueError("Persisted registry entries must be objects")
                    entry = ServiceEntry.from_dict(item)
                    loaded[entry.service_id] = entry
                if not loaded:
                    raise ValueError("Persisted registry cannot be empty")
                self._services = loaded
                logger.info(f"Loaded registry state with {len(self._services)} services.")
            except Exception as e:
                logger.error(f"Error loading registry state: {e}. Keeping defaults.")

    async def save_state(self) -> bool:
        async with self._lock:
            return await self._persist_unlocked()

    async def _persist_unlocked(self) -> bool:
        if not self.persistence_file:
            return True
        try:
            self.persistence_file.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "services": [s.to_dict() for s in self._services.values()]
            }
            tmp_file = self.persistence_file.with_suffix(".tmp")
            serialized = json.dumps(data, indent=2, sort_keys=True)
            with tmp_file.open("w", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.flush()
                os.fsync(handle.fileno())
            tmp_file.replace(self.persistence_file)
            return True
        except Exception as e:
            logger.error(f"Failed to persist registry: {e}")
            return False

    async def get_services(self) -> List[ServiceEntry]:
        async with self._lock:
            return [service.copy() for service in self._services.values()]

    async def get_service(self, service_id: str) -> Optional[ServiceEntry]:
        async with self._lock:
            service = self._services.get(service_id)
            return service.copy() if service else None

    async def update_status(self, service_id: str, status: str) -> bool:
        if status not in {"available", "unavailable", "unknown"}:
            raise ValueError(f"Invalid health status '{status}'")
        async with self._lock:
            if service_id in self._services:
                if self._services[service_id].status == status:
                    return True
                previous = self._services[service_id].status
                self._services[service_id].status = status
                if await self._persist_unlocked():
                    return True
                self._services[service_id].status = previous
            return False

    async def update_version(
        self,
        service_id: str,
        new_version: str,
        *,
        meta: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if not new_version.isdigit() or not 1 <= int(new_version) <= 255:
            return False
        async with self._lock:
            if service_id in self._services:
                previous = self._services[service_id].version
                previous_meta = copy.deepcopy(self._services[service_id].meta)
                self._services[service_id].version = new_version
                if meta is not None:
                    self._services[service_id].meta = copy.deepcopy(meta)
                if await self._persist_unlocked():
                    return True
                self._services[service_id].version = previous
                self._services[service_id].meta = previous_meta
            return False

    async def register_service(self, entry: ServiceEntry) -> bool:
        async with self._lock:
            previous = self._services.get(entry.service_id)
            self._services[entry.service_id] = entry
            if await self._persist_unlocked():
                return True
            if previous is None:
                self._services.pop(entry.service_id, None)
            else:
                self._services[entry.service_id] = previous
            return False

    async def update_metadata(
        self,
        service_id: str,
        *,
        protocol: Optional[str] = None,
        endpoint: Optional[str] = None,
        capabilities: Optional[List[str]] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> bool:
        async with self._lock:
            current = self._services.get(service_id)
            if current is None:
                return False
            previous = current.copy()
            if protocol is not None:
                current.protocol = protocol
            if endpoint is not None:
                current.endpoint = endpoint
            if capabilities is not None:
                current.capabilities = list(capabilities)
            if meta is not None:
                current.meta = dict(meta)
            if await self._persist_unlocked():
                return True
            self._services[service_id] = previous
            return False
