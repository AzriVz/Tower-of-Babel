from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Any, Dict

from src.adapters.base import (
    AdapterError,
    BackendTimeoutError,
    BaseAdapter,
    ProtocolValidationError,
    UnsupportedOperationError,
)
from src.core.config import settings
from src.core.request_ids import backend_request_ids

logger = logging.getLogger(__name__)

HEADER_FORMAT = "!2sBBIQ"  # magic(2s), version(B), flags(B), payload_length(I), request_id(Q)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAGIC_BYTES = b"\xBA\xBE"
MAX_FRAME_LENGTH = 65536


class ServiceBAdapter(BaseAdapter):
    def __init__(
        self,
        host: str = settings.SERVICE_B_HOST,
        port: int = settings.SERVICE_B_PORT,
    ) -> None:
        self.host = host
        self.port = port

    @property
    def service_id(self) -> str:
        return "service-b"

    @property
    def protocol_name(self) -> str:
        return "tcp-frame-json"

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        op_upper = operation.upper()
        if op_upper not in {"ECHO", "UPPERCASE", "SUM", "REVERSE", "METADATA"}:
            raise UnsupportedOperationError(f"Service B does not support operation '{operation}'")

        # Prepare request arguments
        b_args: Dict[str, Any] = {}
        if op_upper == "SUM":
            b_args["numberList"] = arguments.get("values", arguments.get("numberList", []))
        elif op_upper in {"ECHO", "UPPERCASE", "REVERSE"}:
            b_args["value"] = arguments.get("value", "")
        
        req_payload = {
            "opCode": op_upper,
            "args": b_args,
        }
        payload_bytes = json.dumps(req_payload).encode("utf-8")
        if len(payload_bytes) > MAX_FRAME_LENGTH:
            raise ProtocolValidationError(f"Request payload size {len(payload_bytes)} exceeds max {MAX_FRAME_LENGTH}")

        if not version.isdigit() or not 1 <= int(version) <= 255:
            raise AdapterError(
                "UNSUPPORTED_PROTOCOL_VERSION",
                f"Invalid Service B protocol version '{version}'.",
                safe_to_fallback=True,
            )
        req_id_num = backend_request_ids.next()
        version_num = int(version)
        flags = 0

        header = struct.pack(HEADER_FORMAT, MAGIC_BYTES, version_num, flags, len(payload_bytes), req_id_num)
        frame = header + payload_bytes

        writer: asyncio.StreamWriter | None = None
        dispatched = False
        try:
            async with asyncio.timeout(timeout_s):
                reader, writer = await asyncio.open_connection(self.host, self.port)
                writer.write(frame)
                # Once bytes enter the transport buffer, delivery/execution is
                # ambiguous even if drain later raises.
                dispatched = True
                await writer.drain()

                header_data = await reader.readexactly(HEADER_SIZE)
                magic, r_ver, r_flags, p_len, r_req_id = struct.unpack(HEADER_FORMAT, header_data)

                if magic != MAGIC_BYTES:
                    raise ProtocolValidationError(
                        f"Invalid magic bytes in Service B frame: {magic.hex()}"
                    )
                if r_ver != version_num:
                    raise ProtocolValidationError(
                        f"Protocol version mismatch: expected {version_num}, got {r_ver}"
                    )
                if r_flags != 0:
                    raise ProtocolValidationError(f"Invalid reserved flags in Service B frame: {r_flags}")
                if p_len > MAX_FRAME_LENGTH:
                    raise ProtocolValidationError(
                        f"Excessive frame length from Service B: {p_len} > {MAX_FRAME_LENGTH}"
                    )
                if r_req_id != req_id_num:
                    raise ProtocolValidationError(
                        f"Request ID mismatch: expected {req_id_num}, got {r_req_id}"
                    )

                payload_data = await reader.readexactly(p_len)
                body = json.loads(payload_data.decode("utf-8"))
                if not isinstance(body, dict):
                    raise ProtocolValidationError("Service B response payload must be an object")
                if body.get("requestId") != req_id_num:
                    raise ProtocolValidationError("Service B payload requestId does not match frame")
                if body.get("serviceId") != "service-b":
                    raise ProtocolValidationError("Service B payload has an invalid serviceId")

                if body.get("errorData") is not None:
                    err = body["errorData"]
                    if not isinstance(err, dict):
                        raise ProtocolValidationError("Service B errorData must be an object or null")
                    raise AdapterError(
                        str(err.get("code", "SERVICE_B_ERROR")),
                        str(err.get("message", "Service B error")),
                        retryable=bool(err.get("retryable", False)),
                        safe_to_fallback=err.get("code") in {
                            "OPERATION_NOT_SUPPORTED",
                            "UNSUPPORTED_PROTOCOL_VERSION",
                            "RATE_LIMITED",
                            "SERVICE_UNAVAILABLE",
                        },
                    )

                res_data = body.get("resultData")
                if not isinstance(res_data, dict):
                    raise ProtocolValidationError("Service B response requires a resultData object")

                if "numericResult" in res_data:
                    return {"value": res_data["numericResult"]}
                if "value" in res_data:
                    return {"value": res_data["value"]}
                return res_data

        except TimeoutError as exc:
            raise BackendTimeoutError(
                f"Service B operation timed out after {timeout_s}s",
                safe_to_fallback=not dispatched,
            ) from exc
        except (asyncio.IncompleteReadError, ConnectionResetError) as exc:
            raise AdapterError(
                "CONNECTION_RESET",
                f"Service B connection closed/reset: {exc}",
                retryable=True,
                safe_to_fallback=not dispatched,
            ) from exc
        except OSError as exc:
            raise AdapterError(
                "BACKEND_UNAVAILABLE",
                f"Service B TCP connection error: {exc}",
                retryable=True,
                safe_to_fallback=not dispatched,
            ) from exc
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ProtocolValidationError(f"Service B returned invalid JSON payload: {exc}") from exc
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
