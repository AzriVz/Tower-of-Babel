from __future__ import annotations

import asyncio
import inspect
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional, Tuple

from src.adapters.base import BaseAdapter
from src.adapters.service_a import ServiceAAdapter
from src.adapters.service_b import ServiceBAdapter
from src.adapters.service_c import ServiceCAdapter
from src.adapters.subprocess_proxy import SubprocessAdapterProxy
from src.core.registry import ServiceEntry

logger = logging.getLogger(__name__)


class AdapterCompatibilityError(ValueError):
    pass


class AdapterManager:
    """Versioned adapter registry with atomic swaps and in-flight draining."""

    def __init__(self) -> None:
        self._adapters: Dict[Tuple[str, str], BaseAdapter] = {}
        self._previous_adapters: Dict[Tuple[str, str], BaseAdapter] = {}
        self._adapter_specs: Dict[Tuple[str, str], Optional[Dict[str, str]]] = {}
        self._previous_adapter_specs: Dict[Tuple[str, str], Optional[Dict[str, str]]] = {}
        self._in_flight: Dict[Tuple[str, str], int] = {}
        self._drained: Dict[Tuple[str, str], asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._migration_locks: Dict[str, asyncio.Lock] = {}
        self._load_defaults()

    def _load_defaults(self) -> None:
        self._adapters[("service-a", "1")] = ServiceAAdapter()
        self._adapters[("service-b", "1")] = ServiceBAdapter()
        self._adapters[("service-c", "1")] = ServiceCAdapter()

    @staticmethod
    def _validate_adapter(
        service_id: str,
        adapter: BaseAdapter,
        expected_protocol: Optional[str] = None,
    ) -> None:
        if not isinstance(adapter, BaseAdapter):
            raise AdapterCompatibilityError("Adapter must inherit from BaseAdapter")
        if adapter.service_id != service_id:
            raise AdapterCompatibilityError(
                f"Adapter service_id '{adapter.service_id}' does not match '{service_id}'"
            )
        if expected_protocol is not None and adapter.protocol_name != expected_protocol:
            raise AdapterCompatibilityError(
                f"Adapter protocol '{adapter.protocol_name}' does not match '{expected_protocol}'"
            )
        signature = inspect.signature(adapter.execute)
        required = {"operation", "arguments", "request_id", "timeout_s", "version"}
        if not required.issubset(signature.parameters):
            raise AdapterCompatibilityError(
                f"Adapter execute signature is missing {sorted(required - set(signature.parameters))}"
            )
        if not inspect.iscoroutinefunction(adapter.execute):
            raise AdapterCompatibilityError("Adapter execute method must be async")

    async def configure_from_registry(self, entries: list[ServiceEntry]) -> None:
        """Make persisted endpoints/protocol metadata effective after restart."""
        replacements: Dict[Tuple[str, str], BaseAdapter] = {}
        for entry in entries:
            try:
                if entry.protocol == "http-json":
                    adapter: BaseAdapter = ServiceAAdapter(endpoint=entry.endpoint)
                elif entry.protocol in {"tcp-frame-json", "udp-crc-json"}:
                    host, port_text = entry.endpoint.rsplit(":", 1)
                    port = int(port_text)
                    if entry.protocol == "tcp-frame-json":
                        adapter = ServiceBAdapter(host=host, port=port)
                    else:
                        adapter = ServiceCAdapter(host=host, port=port)
                else:
                    logger.warning("No built-in adapter for protocol %s", entry.protocol)
                    continue
                self._validate_adapter(entry.service_id, adapter, entry.protocol)
                replacements[(entry.service_id, entry.version)] = adapter
            except (ValueError, AdapterCompatibilityError) as exc:
                logger.error("Cannot configure adapter for %s: %s", entry.service_id, exc)

        async with self._lock:
            for key, adapter in replacements.items():
                self._adapters[key] = adapter
                self._adapter_specs[key] = None

        # Plugin descriptors are persisted in registry metadata. Import errors
        # leave the built-in adapter untouched.
        for entry in entries:
            plugin_specs = entry.meta.get("adapter_plugins", {})
            if not isinstance(plugin_specs, dict):
                continue
            for version, spec in plugin_specs.items():
                if not isinstance(spec, dict):
                    continue
                module_path = spec.get("module_path")
                class_name = spec.get("class_name")
                if not isinstance(module_path, str) or not isinstance(class_name, str):
                    continue
                try:
                    instance = await self.instantiate_adapter_from_module(
                        entry.service_id,
                        module_path,
                        class_name,
                        entry.protocol,
                    )
                    await self.register_adapter(
                        entry.service_id,
                        instance,
                        version=str(version),
                        expected_protocol=entry.protocol,
                        source_spec={"module_path": module_path, "class_name": class_name},
                    )
                except Exception as exc:
                    logger.error(
                        "Persisted plugin for %s v%s was rejected: %s",
                        entry.service_id,
                        version,
                        exc,
                    )

    async def get_adapter(self, service_id: str, version: str = "1") -> Optional[BaseAdapter]:
        async with self._lock:
            return self._adapters.get((service_id, version))

    async def get_versions(self, service_id: str) -> list[str]:
        async with self._lock:
            versions = [version for sid, version in self._adapters if sid == service_id]
        return sorted(versions, key=lambda value: int(value) if value.isdigit() else value)

    async def get_adapter_spec(
        self,
        service_id: str,
        version: str,
    ) -> Optional[Dict[str, str]]:
        async with self._lock:
            spec = self._adapter_specs.get((service_id, version))
            return dict(spec) if spec is not None else None

    @asynccontextmanager
    async def acquire_adapter(
        self,
        service_id: str,
        version: str,
    ) -> AsyncIterator[Optional[BaseAdapter]]:
        key = (service_id, version)
        async with self._lock:
            adapter = self._adapters.get(key)
            if adapter is not None:
                self._in_flight[key] = self._in_flight.get(key, 0) + 1
                event = self._drained.setdefault(key, asyncio.Event())
                event.clear()
        try:
            yield adapter
        finally:
            if adapter is not None:
                async with self._lock:
                    remaining = self._in_flight.get(key, 1) - 1
                    if remaining <= 0:
                        self._in_flight.pop(key, None)
                        self._drained.setdefault(key, asyncio.Event()).set()
                    else:
                        self._in_flight[key] = remaining

    async def wait_for_drain(self, service_id: str, version: str, timeout_s: float) -> bool:
        key = (service_id, version)
        async with self._lock:
            if self._in_flight.get(key, 0) == 0:
                return True
            event = self._drained.setdefault(key, asyncio.Event())
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
            return True
        except TimeoutError:
            return False

    async def register_adapter(
        self,
        service_id: str,
        adapter: BaseAdapter,
        *,
        version: str = "1",
        expected_protocol: Optional[str] = None,
        dry_run: bool = True,
        source_spec: Optional[Dict[str, str]] = None,
    ) -> bool:
        try:
            if not version.isdigit() or not 1 <= int(version) <= 255:
                raise AdapterCompatibilityError("Adapter version must be from 1 to 255")
            if dry_run:
                self._validate_adapter(service_id, adapter, expected_protocol)
        except (ValueError, AdapterCompatibilityError) as exc:
            logger.error("Adapter validation failed for %s v%s: %s", service_id, version, exc)
            return False

        key = (service_id, version)
        async with self._lock:
            if key in self._adapters:
                self._previous_adapters[key] = self._adapters[key]
                self._previous_adapter_specs[key] = self._adapter_specs.get(key)
            self._adapters[key] = adapter
            self._adapter_specs[key] = dict(source_spec) if source_spec is not None else None
        logger.info("Registered adapter for %s v%s", service_id, version)
        return True

    async def remove_adapter(self, service_id: str, version: str) -> bool:
        key = (service_id, version)
        async with self._lock:
            if self._in_flight.get(key, 0):
                return False
            self._adapter_specs.pop(key, None)
            return self._adapters.pop(key, None) is not None

    async def rollback_adapter(self, service_id: str, version: str = "1") -> bool:
        key = (service_id, version)
        async with self._lock:
            previous = self._previous_adapters.get(key)
            if previous is None or self._in_flight.get(key, 0):
                return False
            self._adapters[key] = previous
            self._adapter_specs[key] = self._previous_adapter_specs.pop(key, None)
            self._previous_adapters.pop(key, None)
        logger.info("Rolled back adapter for %s v%s", service_id, version)
        return True

    async def instantiate_adapter_from_module(
        self,
        service_id: str,
        module_path: str,
        class_name: str,
        expected_protocol: Optional[str] = None,
    ) -> BaseAdapter:
        instance = await SubprocessAdapterProxy.create(module_path, class_name)
        self._validate_adapter(service_id, instance, expected_protocol)
        return instance

    async def load_adapter_from_module(
        self,
        service_id: str,
        module_path: str,
        class_name: str,
        *,
        version: str = "1",
        expected_protocol: Optional[str] = None,
    ) -> bool:
        try:
            instance = await self.instantiate_adapter_from_module(
                service_id,
                module_path,
                class_name,
                expected_protocol,
            )
            return await self.register_adapter(
                service_id,
                instance,
                version=version,
                expected_protocol=expected_protocol,
                source_spec={"module_path": module_path, "class_name": class_name},
            )
        except Exception as exc:
            logger.error("Adapter load failed for %s v%s: %s", service_id, version, exc)
            return False

    async def migration_lock(self, service_id: str) -> asyncio.Lock:
        async with self._lock:
            return self._migration_locks.setdefault(service_id, asyncio.Lock())
