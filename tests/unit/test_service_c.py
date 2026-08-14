from __future__ import annotations

import json
import struct
import zlib

import pytest

from src.adapters.base import AdapterError
from src.adapters.service_c import HEADER_FORMAT, MAGIC_BYTES, ServiceCAdapter


class PacketTransport:
    def __init__(self, mutate_field: str | None = None) -> None:
        self.mutate_field = mutate_field

    async def send_and_receive(
        self,
        packet: bytes,
        expected_req_id: int,
        expected_seq_num: int,
        timeout_budget_s: float,
        validate_fn,
    ) -> bytes:
        _, version, _, _, _, opcode, _, _ = struct.unpack(HEADER_FORMAT, packet[:20])
        fields = {
            "version": version,
            "message_type": 2,
            "sequence": expected_seq_num,
            "request_id": expected_req_id,
            "opcode": opcode,
            "flags": 0,
        }
        if self.mutate_field:
            fields[self.mutate_field] = fields[self.mutate_field] + 1
        payload = json.dumps(
            {"serviceId": "service-c", "result": {"value": 6}, "error": None}
        ).encode()
        prefix = struct.pack(
            HEADER_FORMAT,
            MAGIC_BYTES,
            fields["version"],
            fields["message_type"],
            fields["sequence"],
            fields["request_id"],
            fields["opcode"],
            fields["flags"],
            len(payload),
        ) + payload
        response = prefix + struct.pack("!I", zlib.crc32(prefix) & 0xFFFFFFFF)
        valid, _, _ = validate_fn(response, expected_req_id, expected_seq_num)
        if not valid:
            raise TimeoutError("invalid packet was ignored")
        return response


@pytest.mark.asyncio
async def test_service_c_accepts_strictly_valid_response() -> None:
    adapter = ServiceCAdapter()
    adapter.transport = PacketTransport()
    assert await adapter.execute("sum", {"values": [1, 2, 3]}, "opaque", 1) == {"value": 6}


@pytest.mark.parametrize(
    "field",
    ["version", "message_type", "sequence", "request_id", "opcode", "flags"],
)
@pytest.mark.asyncio
async def test_service_c_rejects_mismatched_header_fields(field: str) -> None:
    adapter = ServiceCAdapter()
    adapter.transport = PacketTransport(field)
    with pytest.raises(AdapterError):
        await adapter.execute("sum", {"values": [1, 2, 3]}, "opaque", 1)
