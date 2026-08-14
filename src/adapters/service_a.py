from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict
import httpx

from src.adapters.base import (
    AdapterError,
    BackendTimeoutError,
    BaseAdapter,
    ProtocolValidationError,
    UnsupportedOperationError,
)
from src.core.config import settings

logger = logging.getLogger(__name__)


class ServiceAAdapter(BaseAdapter):
    def __init__(
        self,
        endpoint: str = settings.SERVICE_A_URL,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self._transport = transport

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
        if operation not in {"echo", "uppercase", "metadata"}:
            raise UnsupportedOperationError(f"Service A does not support operation '{operation}'")

        if not version.isdigit() or not 1 <= int(version) <= 255:
            raise AdapterError(
                "UNSUPPORTED_PROTOCOL_VERSION",
                f"Invalid Service A protocol version '{version}'.",
                safe_to_fallback=True,
            )

        payload = {
            "operation_name": operation,
            "operation_arguments": arguments,
        }
        headers = {
            "Content-Type": "application/json",
            "X-Protocol-Version": str(version),
            "X-Request-ID": request_id,
        }

        url = f"{self.endpoint}/v1/execute"

        dispatched = False
        try:
            async with asyncio.timeout(timeout_s):
                async with httpx.AsyncClient(
                    timeout=timeout_s,
                    transport=self._transport,
                ) as client:
                    dispatched = True
                    resp = await client.post(url, json=payload, headers=headers)
                
                    if resp.status_code == 200:
                        content_type = resp.headers.get("content-type", "").lower()
                        if "application/json" not in content_type:
                            raise ProtocolValidationError(
                                f"Service A returned unexpected Content-Type '{content_type}'"
                            )
                        try:
                            data = resp.json()
                        except (ValueError, UnicodeDecodeError) as exc:
                            raise ProtocolValidationError(
                                f"Service A returned malformed JSON: {exc}"
                            ) from exc

                        if not isinstance(data, dict):
                            raise ProtocolValidationError("Service A response must be a JSON object")
                        if data.get("request_id") != request_id:
                            raise ProtocolValidationError(
                                f"Request ID mismatch: expected {request_id}, got {data.get('request_id')}"
                            )
                        if data.get("service_name") not in (None, "service-a"):
                            raise ProtocolValidationError("Service A response has an invalid service_name")
                        if data.get("operation_name") not in (None, operation):
                            raise ProtocolValidationError("Service A response operation does not match request")

                        response_version = resp.headers.get("x-protocol-version")
                        if response_version is not None and response_version != version:
                            raise ProtocolValidationError(
                                f"Protocol version mismatch: expected {version}, got {response_version}"
                            )

                        result = data.get("operation_result")
                        if not isinstance(result, dict):
                            raise ProtocolValidationError(
                                "Service A response requires an 'operation_result' object"
                            )
                        return result
                
                    try:
                        err_data = resp.json()
                        if not isinstance(err_data, dict):
                            raise ValueError("error body is not an object")
                        response_request_id = err_data.get("request_id")
                        if response_request_id is not None and response_request_id != request_id:
                            raise ProtocolValidationError(
                                f"Request ID mismatch: expected {request_id}, got {response_request_id}"
                            )
                        code = err_data.get("error_code", f"HTTP_{resp.status_code}")
                        msg = err_data.get("error_message", f"HTTP Error {resp.status_code}")
                        retryable = bool(
                            err_data.get("retryable", resp.status_code in (429, 500, 503))
                        )
                    except ProtocolValidationError:
                        raise
                    except (ValueError, UnicodeDecodeError):
                        code = f"HTTP_{resp.status_code}"
                        msg = f"HTTP Error {resp.status_code} with malformed error body"
                        retryable = resp.status_code in (429, 500, 503)

                    raise AdapterError(
                        str(code),
                        str(msg),
                        retryable=retryable,
                        safe_to_fallback=resp.status_code in (400, 415, 429, 503),
                    )

        except (TimeoutError, httpx.TimeoutException) as exc:
            raise BackendTimeoutError(
                f"Service A timed out after {timeout_s}s",
                safe_to_fallback=not dispatched,
            ) from exc
        except httpx.ConnectError as exc:
            raise AdapterError(
                "BACKEND_UNAVAILABLE",
                f"Service A connection error: {exc}",
                retryable=True,
                safe_to_fallback=True,
            ) from exc
        except httpx.RequestError as exc:
            raise AdapterError(
                "BACKEND_UNAVAILABLE",
                f"Service A network error: {exc}",
                retryable=True,
                safe_to_fallback=not dispatched,
            ) from exc

    async def health(self, timeout_s: float, version: str = "1") -> bool:
        try:
            # /v1/health only proves transport liveness and may ignore protocol
            # headers. A metadata execute is side-effect-free and proves that
            # the requested protocol version is actually accepted.
            return await super().health(timeout_s, version)
        except (AdapterError, TimeoutError, httpx.HTTPError):
            return False
