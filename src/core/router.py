from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from src.adapters.base import AdapterError, BackendTimeoutError, UnsupportedOperationError
from src.adapters.plugin_loader import AdapterManager
from src.core.circuit_breaker import CircuitBreakerManager
from src.core.config import settings
from src.core.registry import ServiceEntry, ServiceRegistry
from src.models.gateway_api import ExecuteRequest, ExecuteResponse

logger = logging.getLogger(__name__)

DEFAULT_SERVICE_PRIORITY = {
    "service-a": 10,
    "service-b": 20,
    "service-c": 30,
}
OPERATION_PRIORITY = {
    "sum": {"service-c": 10, "service-b": 20},
}


class CapabilityRouter:
    def __init__(
        self,
        registry: ServiceRegistry,
        adapter_manager: AdapterManager,
        cb_manager: CircuitBreakerManager,
    ) -> None:
        self.registry = registry
        self.adapter_manager = adapter_manager
        self.cb_manager = cb_manager

    @staticmethod
    def _priority(entry: ServiceEntry, operation: str) -> tuple[int, str]:
        operation_priorities = OPERATION_PRIORITY.get(operation, {})
        default = operation_priorities.get(
            entry.service_id,
            DEFAULT_SERVICE_PRIORITY.get(entry.service_id, 1000),
        )
        configured = entry.meta.get("routing_priority", default)
        try:
            numeric_priority = int(configured)
        except (TypeError, ValueError):
            numeric_priority = default
        return numeric_priority, entry.service_id

    async def _get_candidates(
        self,
        operation: str,
        preferred: Optional[str] = None,
    ) -> tuple[List[ServiceEntry], bool]:
        services = await self.registry.get_services()
        capable = [entry for entry in services if operation in entry.capabilities]
        available = [entry for entry in capable if entry.status != "unavailable"]
        available.sort(key=lambda entry: self._priority(entry, operation))
        if preferred:
            for index, entry in enumerate(available):
                if entry.service_id == preferred:
                    available.insert(0, available.pop(index))
                    break
        return available, bool(capable)

    async def refresh_health(self, timeout_s: Optional[float] = None) -> None:
        timeout = timeout_s or settings.HEALTHCHECK_TIMEOUT_S
        entries = await self.registry.get_services()

        async def probe(entry: ServiceEntry) -> tuple[str, bool]:
            async with self.adapter_manager.acquire_adapter(
                entry.service_id,
                entry.version,
            ) as adapter:
                if adapter is None:
                    return entry.service_id, False
                try:
                    async with asyncio.timeout(timeout):
                        healthy = await adapter.health(timeout, entry.version)
                    return entry.service_id, bool(healthy)
                except (AdapterError, TimeoutError, OSError, ValueError):
                    return entry.service_id, False
                except Exception:
                    logger.exception("Unexpected health-check failure for %s", entry.service_id)
                    return entry.service_id, False

        results = await asyncio.gather(*(probe(entry) for entry in entries))
        for service_id, healthy in results:
            await self.registry.update_status(
                service_id,
                "available" if healthy else "unavailable",
            )

    async def execute_request(self, req: ExecuteRequest) -> ExecuteResponse:
        started_at = time.monotonic()
        timeout_budget_ms = (
            req.options.timeout_ms
            if req.options.timeout_ms is not None
            else settings.DEFAULT_TIMEOUT_MS
        )
        deadline = started_at + timeout_budget_ms / 1000.0

        candidates, operation_known = await self._get_candidates(
            req.operation,
            req.options.preferred_service,
        )
        if not operation_known:
            return ExecuteResponse.make_error(
                request_id=req.request_id,
                service_id=None,
                operation=req.operation,
                code="UNSUPPORTED_OPERATION",
                message=f"Operation '{req.operation}' is not supported by any registered backend.",
                retryable=False,
            )
        if not candidates:
            return ExecuteResponse.make_error(
                request_id=req.request_id,
                service_id=None,
                operation=req.operation,
                code="BACKEND_UNAVAILABLE",
                message=f"No healthy backend is available for operation '{req.operation}'.",
                retryable=True,
            )

        last_error_code = "BACKEND_UNAVAILABLE"
        last_error_msg = "No safe backend route completed the request."
        last_retryable = True
        last_service_id: Optional[str] = None

        for service_entry in candidates:
            service_id = service_entry.service_id
            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                return ExecuteResponse.make_error(
                    request_id=req.request_id,
                    service_id=last_service_id,
                    operation=req.operation,
                    code="BACKEND_TIMEOUT",
                    message=f"Total timeout budget of {timeout_budget_ms}ms was exhausted.",
                    retryable=True,
                )

            version = req.options.version_preference or service_entry.version
            last_service_id = service_id

            cb = await self.cb_manager.get_breaker(service_id)
            if not await cb.allow_request():
                last_error_code = "CIRCUIT_OPEN"
                last_error_msg = f"Circuit breaker for {service_id} is open."
                last_retryable = True
                continue

            async with self.adapter_manager.acquire_adapter(service_id, version) as adapter:
                if adapter is None:
                    last_error_code = "UNSUPPORTED_PROTOCOL_VERSION"
                    last_error_msg = (
                        f"No compatible adapter for {service_id} protocol version {version}."
                    )
                    last_retryable = False
                    continue

                try:
                    async with asyncio.timeout(remaining_s):
                        result = await adapter.execute(
                            operation=req.operation,
                            arguments=req.arguments,
                            request_id=req.request_id,
                            timeout_s=remaining_s,
                            version=version,
                        )
                    if not isinstance(result, dict):
                        raise AdapterError(
                            "PROTOCOL_VALIDATION_ERROR",
                            f"Adapter for {service_id} returned a non-object result.",
                        )
                    await cb.record_success()
                    return ExecuteResponse.make_success(
                        request_id=req.request_id,
                        service_id=service_id,
                        operation=req.operation,
                        result=result,
                    )
                except TimeoutError:
                    await cb.record_failure()
                    last_error_code = "BACKEND_TIMEOUT"
                    last_error_msg = f"{service_id} exceeded the remaining timeout budget."
                    last_retryable = True
                    # Dispatch state is unknown, so cross-backend fallback could
                    # duplicate an execution. Stop conservatively.
                    break
                except UnsupportedOperationError as exc:
                    last_error_code = exc.code
                    last_error_msg = exc.message
                    last_retryable = False
                    continue
                except BackendTimeoutError as exc:
                    await cb.record_failure()
                    last_error_code = exc.code
                    last_error_msg = exc.message
                    last_retryable = exc.retryable
                    if exc.safe_to_fallback:
                        continue
                    break
                except AdapterError as exc:
                    if exc.retryable or exc.code == "PROTOCOL_VALIDATION_ERROR":
                        await cb.record_failure()
                    last_error_code = exc.code
                    last_error_msg = exc.message
                    last_retryable = exc.retryable
                    logger.warning("Execution on %s failed: %s", service_id, exc)
                    if exc.safe_to_fallback:
                        continue
                    break
                except Exception as exc:
                    await cb.record_failure()
                    logger.exception("Adapter for %s raised an unexpected exception", service_id)
                    last_error_code = "ADAPTER_INTERNAL_ERROR"
                    last_error_msg = f"Adapter for {service_id} failed safely: {type(exc).__name__}."
                    last_retryable = False
                    # The request may already have been dispatched.
                    break

        return ExecuteResponse.make_error(
            request_id=req.request_id,
            service_id=last_service_id,
            operation=req.operation,
            code=last_error_code,
            message=last_error_msg,
            retryable=last_retryable,
        )
