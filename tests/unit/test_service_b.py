from __future__ import annotations

import json
import struct

import pytest

from src.adapters.base import ProtocolValidationError
from src.adapters.service_b import HEADER_FORMAT, HEADER_SIZE, MAGIC_BYTES, ServiceBAdapter


class FakeWriter:
    def __init__(self) -> None:
        self.request = b""

    def write(self, data: bytes) -> None:
        self.request = data

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class FakeReader:
    def __init__(self, writer: FakeWriter, response_version: int = 1) -> None:
        self.writer = writer
        self.response_version = response_version
        self.response = b""

    def _build(self) -> None:
        _, _, _, _, request_id = struct.unpack(HEADER_FORMAT, self.writer.request[:HEADER_SIZE])
        body = json.dumps(
            {
                "requestId": request_id,
                "serviceId": "service-b",
                "resultData": {"numericResult": 6},
                "errorData": None,
            }
        ).encode()
        self.response = struct.pack(
            HEADER_FORMAT,
            MAGIC_BYTES,
            self.response_version,
            0,
            len(body),
            request_id,
        ) + body

    async def readexactly(self, size: int) -> bytes:
        if not self.response:
            self._build()
        chunk, self.response = self.response[:size], self.response[size:]
        return chunk


@pytest.mark.asyncio
async def test_service_b_normalizes_valid_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    writer = FakeWriter()
    reader = FakeReader(writer)

    async def open_connection(host: str, port: int):
        return reader, writer

    monkeypatch.setattr("src.adapters.service_b.asyncio.open_connection", open_connection)
    result = await ServiceBAdapter().execute("sum", {"values": [1, 2, 3]}, "opaque", 1)
    assert result == {"value": 6}


@pytest.mark.asyncio
async def test_service_b_rejects_response_version(monkeypatch: pytest.MonkeyPatch) -> None:
    writer = FakeWriter()
    reader = FakeReader(writer, response_version=2)

    async def open_connection(host: str, port: int):
        return reader, writer

    monkeypatch.setattr("src.adapters.service_b.asyncio.open_connection", open_connection)
    with pytest.raises(ProtocolValidationError, match="version mismatch"):
        await ServiceBAdapter().execute("sum", {"values": [1, 2, 3]}, "opaque", 1)
