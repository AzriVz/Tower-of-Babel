from __future__ import annotations

import asyncio
import json
import logging
import struct
from typing import Any, Dict, Optional, Tuple
import zlib

from src.adapters.base import (
    AdapterError,
    BackendTimeoutError,
    BaseAdapter,
    ProtocolValidationError,
    UnsupportedOperationError,
)
from src.core.config import settings
from src.core.request_ids import backend_request_ids, udp_sequence_numbers
from src.protocols.udp_reliable import ReliableUDPTransport

logger = logging.getLogger(__name__)

HEADER_FORMAT = "!2sBBIQBBH"  # magic(2s), version(B), msg_type(B), seq_num(I), req_id(Q), op_code(B), flags(B), payload_len(H)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAGIC_BYTES = b"\xC0\xDE"
MAX_PAYLOAD_LENGTH = 4096

class ServiceCAdapter(BaseAdapter):
    def __init__(
        self,
        host: str = settings.SERVICE_C_HOST,
        port: int = settings.SERVICE_C_PORT,
    ) -> None:
        self.host = host
        self.port = port
        self.transport = ReliableUDPTransport(host, port)

    @property
    def service_id(self) -> str:
        return "service-c"

    @property
    def protocol_name(self) -> str:
        return "udp-crc-json"

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        op_map = {"echo": 1, "sum": 2, "metadata": 3}
        if operation not in op_map:
            raise UnsupportedOperationError(f"Service C does not support operation '{operation}'")

        op_code = op_map[operation]
        
        # Prepare request arguments
        c_args: Dict[str, Any] = {}
        if operation == "sum":
            c_args["values"] = arguments.get("values", arguments.get("numberList", []))
        elif operation == "echo":
            c_args["value"] = arguments.get("value", "")

        payload_bytes = json.dumps(c_args).encode("utf-8")
        if len(payload_bytes) > MAX_PAYLOAD_LENGTH:
            raise ProtocolValidationError(f"Payload size {len(payload_bytes)} exceeds max {MAX_PAYLOAD_LENGTH}")

        if not version.isdigit() or not 1 <= int(version) <= 255:
            raise AdapterError(
                "UNSUPPORTED_PROTOCOL_VERSION",
                f"Invalid Service C protocol version '{version}'.",
                safe_to_fallback=True,
            )
        req_id_num = backend_request_ids.next()
        seq_num = udp_sequence_numbers.next()
        version_num = int(version)
        msg_type = 1  # 1 = request
        flags = 0

        header = struct.pack(
            HEADER_FORMAT,
            MAGIC_BYTES,
            version_num,
            msg_type,
            seq_num,
            req_id_num,
            op_code,
            flags,
            len(payload_bytes),
        )
        packet_prefix = header + payload_bytes
        crc = zlib.crc32(packet_prefix) & 0xFFFFFFFF
        packet = packet_prefix + struct.pack("!I", crc)

        def validate_packet(data: bytes, exp_req_id: int, exp_seq: int) -> Tuple[bool, Optional[bytes], str]:
            if len(data) < HEADER_SIZE + 4:
                return False, None, "Packet too short"
            
            # Check CRC32 checksum
            expected_crc = zlib.crc32(data[:-4]) & 0xFFFFFFFF
            received_crc = struct.unpack("!I", data[-4:])[0]
            if expected_crc != received_crc:
                return False, None, f"CRC32 mismatch: expected {expected_crc}, got {received_crc}"
            
            magic, ver, m_type, r_seq, r_req_id, r_op, r_flags, p_len = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
            if magic != MAGIC_BYTES:
                return False, None, f"Invalid magic: {magic.hex()}"
            if ver != version_num:
                return False, None, f"Version mismatch: expected {version_num}, got {ver}"
            if m_type not in (2, 3):
                return False, None, f"Unexpected message type: {m_type}"
            if r_seq != exp_seq:
                return False, None, f"Sequence mismatch: expected {exp_seq}, got {r_seq}"
            if r_req_id != exp_req_id:
                return False, None, f"Request ID mismatch: expected {exp_req_id}, got {r_req_id}"
            if r_op != op_code:
                return False, None, f"Operation code mismatch: expected {op_code}, got {r_op}"
            if r_flags != 0:
                return False, None, f"Invalid reserved flags: {r_flags}"
            if p_len > MAX_PAYLOAD_LENGTH:
                return False, None, f"Payload length exceeds limit: {p_len}"

            payload = data[HEADER_SIZE:-4]
            if len(payload) != p_len:
                return False, None, f"Payload length mismatch: expected {p_len}, got {len(payload)}"

            return True, payload, ""

        try:
            resp_bytes = await self.transport.send_and_receive(
                packet=packet,
                expected_req_id=req_id_num,
                expected_seq_num=seq_num,
                timeout_budget_s=timeout_s,
                validate_fn=validate_packet,
            )

            payload_data = resp_bytes[HEADER_SIZE:-4]
            _, _, response_message_type, _, _, _, _, _ = struct.unpack(
                HEADER_FORMAT,
                resp_bytes[:HEADER_SIZE],
            )
            body = json.loads(payload_data.decode("utf-8"))
            if not isinstance(body, dict):
                raise ProtocolValidationError("Service C response payload must be an object")
            if body.get("serviceId") != "service-c":
                raise ProtocolValidationError("Service C response has an invalid serviceId")
            if response_message_type == 2 and body.get("error") is not None:
                raise ProtocolValidationError("Service C success packet contains an error")
            if response_message_type == 3 and body.get("error") is None:
                raise ProtocolValidationError("Service C error packet is missing an error object")

            if body.get("error") is not None:
                err = body["error"]
                if not isinstance(err, dict):
                    raise ProtocolValidationError("Service C error must be an object or null")
                raise AdapterError(
                    str(err.get("code", "SERVICE_C_ERROR")),
                    str(err.get("message", "Service C error")),
                    retryable=bool(err.get("retryable", False)),
                    safe_to_fallback=err.get("code") in {
                        "OPERATION_NOT_SUPPORTED",
                        "UNSUPPORTED_PROTOCOL_VERSION",
                        "RATE_LIMITED",
                        "SERVICE_UNAVAILABLE",
                    },
                )

            if not isinstance(body.get("result"), dict):
                raise ProtocolValidationError("Service C response missing 'result' object")

            return body["result"]

        except TimeoutError as exc:
            raise BackendTimeoutError(
                f"Service C operation timed out after {timeout_s}s",
                safe_to_fallback=False,
            ) from exc
        except OSError as exc:
            raise AdapterError(
                "BACKEND_UNAVAILABLE",
                f"Service C UDP transport error: {exc}",
                retryable=True,
                # Conservative: the datagram may already have left the host.
                safe_to_fallback=False,
            ) from exc
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ProtocolValidationError(f"Service C returned invalid JSON payload: {exc}") from exc
