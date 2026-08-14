from __future__ import annotations

import asyncio
import logging
from typing import List

from pydantic import BaseModel, Field, field_validator

from src.adapters.base import AdapterError
from src.adapters.plugin_loader import AdapterManager
from src.core.registry import ServiceRegistry

logger = logging.getLogger(__name__)


class MigrationRequest(BaseModel):
    target_version: str
    drain_in_flight: bool = True
    validation_timeout_ms: float = Field(default=1000, gt=0, le=30_000)
    drain_timeout_ms: float = Field(default=5000, gt=0, le=120_000)

    @field_validator("target_version")
    @classmethod
    def validate_target_version(cls, value: str) -> str:
        if not value.isdigit() or not 1 <= int(value) <= 255:
            raise ValueError("target_version must be an integer string from 1 to 255")
        return value


class MigrationResponse(BaseModel):
    status: str
    service_id: str
    previous_version: str
    current_version: str
    available_versions: List[str] = Field(default_factory=list)
    message: str


class AdapterLoadRequest(BaseModel):
    version: str
    module_path: str = Field(min_length=1, max_length=512)
    class_name: str = Field(min_length=1, max_length=128)
    validation_timeout_ms: float = Field(default=1000, gt=0, le=30_000)

    @field_validator("version")
    @classmethod
    def validate_version(cls, value: str) -> str:
        if not value.isdigit() or not 1 <= int(value) <= 255:
            raise ValueError("version must be an integer string from 1 to 255")
        return value


class AdapterActionResponse(BaseModel):
    status: str
    service_id: str
    version: str
    available_versions: List[str] = Field(default_factory=list)
    message: str


async def handle_protocol_migration(
    service_id: str,
    req: MigrationRequest,
    registry: ServiceRegistry,
    adapter_manager: AdapterManager,
) -> MigrationResponse:
    migration_lock = await adapter_manager.migration_lock(service_id)
    async with migration_lock:
        entry = await registry.get_service(service_id)
        if not entry:
            return MigrationResponse(
                status="error",
                service_id=service_id,
                previous_version="unknown",
                current_version="unknown",
                message=f"Service '{service_id}' not found in registry.",
            )

        previous_version = entry.version
        if req.target_version == previous_version:
            return MigrationResponse(
                status="success",
                service_id=service_id,
                previous_version=previous_version,
                current_version=previous_version,
                available_versions=await adapter_manager.get_versions(service_id),
                message="Service already uses the requested protocol version.",
            )

        target_adapter = await adapter_manager.get_adapter(service_id, req.target_version)
        source_adapter = await adapter_manager.get_adapter(service_id, previous_version)
        candidate = target_adapter or source_adapter
        candidate_spec = await adapter_manager.get_adapter_spec(
            service_id,
            req.target_version if target_adapter is not None else previous_version,
        )
        if candidate is None:
            return MigrationResponse(
                status="error",
                service_id=service_id,
                previous_version=previous_version,
                current_version=previous_version,
                available_versions=await adapter_manager.get_versions(service_id),
                message="No adapter is available to validate the target protocol version.",
            )

        try:
            timeout_s = req.validation_timeout_ms / 1000.0
            async with asyncio.timeout(timeout_s):
                compatible = await candidate.health(timeout_s, req.target_version)
            if not compatible:
                raise AdapterError(
                    "UNSUPPORTED_PROTOCOL_VERSION",
                    f"Backend rejected protocol version {req.target_version}.",
                )
        except (AdapterError, TimeoutError, OSError, ValueError) as exc:
            logger.warning(
                "Migration validation failed for %s v%s: %s",
                service_id,
                req.target_version,
                exc,
            )
            return MigrationResponse(
                status="error",
                service_id=service_id,
                previous_version=previous_version,
                current_version=previous_version,
                available_versions=await adapter_manager.get_versions(service_id),
                message=f"Target version validation failed; version {previous_version} remains active.",
            )
        except Exception as exc:
            logger.exception("Unexpected migration validation failure for %s", service_id)
            return MigrationResponse(
                status="error",
                service_id=service_id,
                previous_version=previous_version,
                current_version=previous_version,
                available_versions=await adapter_manager.get_versions(service_id),
                message=f"Adapter validation failed safely: {type(exc).__name__}.",
            )

        newly_registered = target_adapter is None
        if newly_registered:
            registered = await adapter_manager.register_adapter(
                service_id,
                candidate,
                version=req.target_version,
                expected_protocol=entry.protocol,
                source_spec=candidate_spec,
            )
            if not registered:
                return MigrationResponse(
                    status="error",
                    service_id=service_id,
                    previous_version=previous_version,
                    current_version=previous_version,
                    available_versions=await adapter_manager.get_versions(service_id),
                    message="Compatible target adapter could not be published atomically.",
                )

        updated_meta = dict(entry.meta)
        if candidate_spec is not None:
            plugin_specs = dict(updated_meta.get("adapter_plugins", {}))
            plugin_specs[req.target_version] = candidate_spec
            updated_meta["adapter_plugins"] = plugin_specs
        if not await registry.update_version(
            service_id,
            req.target_version,
            meta=updated_meta,
        ):
            if newly_registered:
                await adapter_manager.remove_adapter(service_id, req.target_version)
            return MigrationResponse(
                status="error",
                service_id=service_id,
                previous_version=previous_version,
                current_version=previous_version,
                available_versions=await adapter_manager.get_versions(service_id),
                message="Registry persistence failed; the previous version remains active.",
            )

        drained = True
        if req.drain_in_flight:
            drained = await adapter_manager.wait_for_drain(
                service_id,
                previous_version,
                req.drain_timeout_ms / 1000.0,
            )

        message = (
            f"Migrated {service_id} from version {previous_version} to {req.target_version}."
        )
        if req.drain_in_flight and not drained:
            message += " Old-version requests are still draining safely on the retained adapter."

        logger.info(message)
        return MigrationResponse(
            status="success",
            service_id=service_id,
            previous_version=previous_version,
            current_version=req.target_version,
            available_versions=await adapter_manager.get_versions(service_id),
            message=message,
        )


async def handle_adapter_load(
    service_id: str,
    req: AdapterLoadRequest,
    registry: ServiceRegistry,
    adapter_manager: AdapterManager,
) -> AdapterActionResponse:
    entry = await registry.get_service(service_id)
    if entry is None:
        return AdapterActionResponse(
            status="error",
            service_id=service_id,
            version=req.version,
            message="Service is not registered.",
        )

    try:
        previous_adapter = await adapter_manager.get_adapter(service_id, req.version)
        candidate = await adapter_manager.instantiate_adapter_from_module(
            service_id,
            req.module_path,
            req.class_name,
            entry.protocol,
        )
        timeout_s = req.validation_timeout_ms / 1000.0
        async with asyncio.timeout(timeout_s):
            healthy = await candidate.health(timeout_s, req.version)
        if not healthy:
            raise AdapterError("ADAPTER_CANARY_FAILED", "Adapter health canary returned false.")
        registered = await adapter_manager.register_adapter(
            service_id,
            candidate,
            version=req.version,
            expected_protocol=entry.protocol,
            source_spec={"module_path": req.module_path, "class_name": req.class_name},
        )
        if not registered:
            raise AdapterError("ADAPTER_INCOMPATIBLE", "Adapter compatibility validation failed.")
        updated_meta = dict(entry.meta)
        plugin_specs = dict(updated_meta.get("adapter_plugins", {}))
        plugin_specs[req.version] = {
            "module_path": req.module_path,
            "class_name": req.class_name,
        }
        updated_meta["adapter_plugins"] = plugin_specs
        if not await registry.update_metadata(service_id, meta=updated_meta):
            if previous_adapter is None:
                await adapter_manager.remove_adapter(service_id, req.version)
            else:
                await adapter_manager.rollback_adapter(service_id, req.version)
            raise AdapterError(
                "ADAPTER_PERSISTENCE_FAILED",
                "Adapter loaded but its persistent descriptor could not be saved.",
            )
    except Exception as exc:
        logger.warning("Rejected runtime adapter for %s v%s: %s", service_id, req.version, exc)
        return AdapterActionResponse(
            status="error",
            service_id=service_id,
            version=req.version,
            available_versions=await adapter_manager.get_versions(service_id),
            message=f"Adapter rejected safely; existing adapter retained: {type(exc).__name__}.",
        )

    return AdapterActionResponse(
        status="success",
        service_id=service_id,
        version=req.version,
        available_versions=await adapter_manager.get_versions(service_id),
        message="Adapter validated by contract and backend canary, then published atomically.",
    )


async def handle_adapter_rollback(
    service_id: str,
    version: str,
    registry: ServiceRegistry,
    adapter_manager: AdapterManager,
) -> AdapterActionResponse:
    rolled_back = await adapter_manager.rollback_adapter(service_id, version)
    if rolled_back:
        entry = await registry.get_service(service_id)
        if entry is not None:
            updated_meta = dict(entry.meta)
            plugin_specs = dict(updated_meta.get("adapter_plugins", {}))
            previous_spec = await adapter_manager.get_adapter_spec(service_id, version)
            if previous_spec is None:
                plugin_specs.pop(version, None)
            else:
                plugin_specs[version] = previous_spec
            if plugin_specs:
                updated_meta["adapter_plugins"] = plugin_specs
            else:
                updated_meta.pop("adapter_plugins", None)
            if not await registry.update_metadata(service_id, meta=updated_meta):
                logger.error("Adapter rollback metadata could not be persisted for %s", service_id)
    return AdapterActionResponse(
        status="success" if rolled_back else "error",
        service_id=service_id,
        version=version,
        available_versions=await adapter_manager.get_versions(service_id),
        message=(
            "Adapter rollback completed atomically and its descriptor was persisted."
            if rolled_back
            else "No rollback candidate exists, or requests are currently in flight."
        ),
    )
