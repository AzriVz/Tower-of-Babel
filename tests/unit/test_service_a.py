from __future__ import annotations

import httpx
import pytest

from src.adapters.base import ProtocolValidationError
from src.adapters.service_a import ServiceAAdapter


@pytest.mark.asyncio
async def test_service_a_requires_response_request_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            json={
                "service_name": "service-a",
                "operation_name": "uppercase",
                "operation_result": {"value": "BABEL"},
            },
        )

    adapter = ServiceAAdapter(
        endpoint="http://service-a",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(ProtocolValidationError, match="Request ID mismatch"):
        await adapter.execute("uppercase", {"value": "babel"}, "opaque", 1)


@pytest.mark.asyncio
async def test_service_a_normalizes_correlated_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json", "X-Protocol-Version": "1"},
            json={
                "request_id": request.headers["X-Request-ID"],
                "service_name": "service-a",
                "operation_name": "uppercase",
                "operation_result": {"value": "BABEL"},
            },
        )

    adapter = ServiceAAdapter(
        endpoint="http://service-a",
        transport=httpx.MockTransport(handler),
    )
    assert await adapter.execute("uppercase", {"value": "babel"}, "opaque", 1) == {
        "value": "BABEL"
    }
