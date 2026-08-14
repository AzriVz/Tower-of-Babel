from __future__ import annotations

import asyncio
import time
import logging
from contextlib import asynccontextmanager
from contextlib import suppress

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.admin.migration_api import (
    AdapterLoadRequest,
    MigrationRequest,
    handle_adapter_load,
    handle_adapter_rollback,
    handle_protocol_migration,
)
from src.adapters.plugin_loader import AdapterManager
from src.core.circuit_breaker import CircuitBreakerManager
from src.core.config import settings
from src.core.registry import ServiceRegistry, ServiceEntry
from src.core.router import CapabilityRouter
from src.models.gateway_api import (
    ExecuteRequest,
    ExecuteResponse,
    ServiceInfo,
    ServicesResponse,
    StatusResponse,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("babel-gateway")

start_time = time.time()

registry = ServiceRegistry()
adapter_manager = AdapterManager()
cb_manager = CircuitBreakerManager()
router = CapabilityRouter(registry, adapter_manager, cb_manager)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global start_time
    start_time = time.time()
    logger.info("Initializing Babel Gateway state...")
    await registry.load_state()
    await adapter_manager.configure_from_registry(await registry.get_services())
    await router.refresh_health()

    async def monitor_health() -> None:
        while True:
            await asyncio.sleep(settings.HEALTHCHECK_INTERVAL_S)
            try:
                await router.refresh_health()
            except Exception:
                logger.exception("Health monitor iteration failed safely")

    health_task = asyncio.create_task(monitor_health(), name="babel-health-monitor")
    try:
        yield
    finally:
        health_task.cancel()
        with suppress(asyncio.CancelledError):
            await health_task
        logger.info("Saving Babel Gateway state...")
        await registry.save_state()


app = FastAPI(
    title="Babel Gateway",
    description="Heterogeneous Backend Protocol Translation Gateway",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # Try to extract request_id if available in body
    request_id = "unknown"
    operation = "unknown"
    try:
        body = await request.json()
        request_id = body.get("request_id", "unknown")
        operation = body.get("operation", "unknown")
    except Exception:
        pass

    resp = ExecuteResponse.make_error(
        request_id=request_id,
        service_id=None,
        operation=operation,
        code="INVALID_REQUEST",
        message=f"Request validation error: {exc}",
        retryable=False,
    )
    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=resp.model_dump())


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled request failure on %s", request.url.path, exc_info=exc)
    if request.url.path == "/execute":
        resp = ExecuteResponse.make_error(
            request_id="unknown",
            service_id=None,
            operation="unknown",
            code="INTERNAL_GATEWAY_ERROR",
            message="The gateway rejected an unexpected internal failure safely.",
            retryable=False,
        )
        return JSONResponse(status_code=500, content=resp.model_dump())
    return JSONResponse(
        status_code=500,
        content={"status": "error", "message": "Internal gateway error."},
    )


@app.post("/execute", response_model=ExecuteResponse)
async def execute_operation(request_data: ExecuteRequest):
    response = await router.execute_request(request_data)
    
    # Determine appropriate HTTP status code
    http_code = status.HTTP_200_OK
    if response.status == "error" and response.error:
        code = response.error.code
        if code in ("INVALID_REQUEST", "UNSUPPORTED_OPERATION"):
            http_code = status.HTTP_400_BAD_REQUEST
        elif code in ("BACKEND_TIMEOUT", "CLIENT_TIMEOUT"):
            http_code = status.HTTP_408_REQUEST_TIMEOUT
        elif code in ("BACKEND_UNAVAILABLE", "CIRCUIT_OPEN"):
            http_code = status.HTTP_503_SERVICE_UNAVAILABLE
        elif code in ("ADAPTER_INTERNAL_ERROR", "INTERNAL_GATEWAY_ERROR"):
            http_code = status.HTTP_500_INTERNAL_SERVER_ERROR

    return JSONResponse(status_code=http_code, content=response.model_dump())


@app.get("/services", response_model=ServicesResponse)
async def list_services():
    entries = await registry.get_services()
    service_infos = []
    for entry in entries:
        service_infos.append(
            ServiceInfo(
                service_id=entry.service_id,
                protocol=entry.protocol,
                endpoint=entry.endpoint,
                status=entry.status,
                capabilities=entry.capabilities,
                version=entry.version,
                supported_versions=await adapter_manager.get_versions(entry.service_id),
            )
        )
    return ServicesResponse(services=service_infos)


@app.get("/status", response_model=StatusResponse)
async def get_gateway_status():
    await router.refresh_health()
    uptime_ms = int((time.time() - start_time) * 1000)
    entries = await registry.get_services()
    backends = {e.service_id: e.status for e in entries}
    services = []
    for entry in entries:
        services.append(
            ServiceInfo(
                service_id=entry.service_id,
                protocol=entry.protocol,
                endpoint=entry.endpoint,
                status=entry.status,
                capabilities=entry.capabilities,
                version=entry.version,
                supported_versions=await adapter_manager.get_versions(entry.service_id),
            )
        )

    return StatusResponse(
        status="ok",
        gateway_id=settings.GATEWAY_ID,
        uptime_ms=uptime_ms,
        backends=backends,
        services=services,
        circuit_breakers=await cb_manager.states(),
    )


@app.post("/admin/services/{service_id}/migrate")
async def migrate_service_protocol(service_id: str, req: MigrationRequest):
    return await handle_protocol_migration(service_id, req, registry, adapter_manager)


@app.post("/admin/services/{service_id}/adapters/load")
async def load_runtime_adapter(service_id: str, req: AdapterLoadRequest):
    return await handle_adapter_load(service_id, req, registry, adapter_manager)


@app.post("/admin/services/{service_id}/adapters/{version}/rollback")
async def rollback_runtime_adapter(service_id: str, version: str):
    return await handle_adapter_rollback(service_id, version, registry, adapter_manager)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
