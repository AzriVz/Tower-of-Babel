from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import httpx

BASE_URL = os.getenv("BABEL_GATEWAY_URL", "http://localhost:8080").rstrip("/")
CONTROL_URL = os.getenv("BABEL_CONTROL_URL", "http://localhost:8090").rstrip("/")
TOKEN = os.getenv("BABEL_CONTROL_TOKEN", "babel-local-dev")


def print_step(title: str) -> None:
    print(f"\n{'=' * 64}\n[DEMO] {title}\n{'=' * 64}")


def show(label: str, payload: Any) -> None:
    print(f"{label}: {json.dumps(payload, indent=2, sort_keys=True)}")


async def control(client: httpx.AsyncClient, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = await client.request(
        method,
        f"{CONTROL_URL}{path}",
        headers={"X-Babel-Control-Token": TOKEN},
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


async def execute(
    client: httpx.AsyncClient,
    request_id: str,
    operation: str,
    arguments: dict[str, Any],
    *,
    preferred_service: str | None = None,
    timeout_ms: int = 2000,
) -> tuple[int, dict[str, Any]]:
    response = await client.post(
        f"{BASE_URL}/execute",
        json={
            "request_id": request_id,
            "operation": operation,
            "arguments": arguments,
            "options": {
                "preferred_service": preferred_service,
                "timeout_ms": timeout_ms,
            },
        },
    )
    return response.status_code, response.json()


def configuration_snapshot(services: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ("service_id", "protocol", "endpoint", "capabilities", "version")
    return sorted(
        [{key: service[key] for key in keys} for service in services],
        key=lambda item: item["service_id"],
    )


async def wait_for_gateway(timeout_s: float = 30) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.get(f"{BASE_URL}/status")
                if response.status_code == 200:
                    return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
        await asyncio.sleep(0.5)
    raise RuntimeError(f"Gateway did not become ready: {last_error}")


async def restart_gateway_and_verify(before: list[dict[str, Any]]) -> None:
    if os.getenv("BABEL_DEMO_SKIP_RESTART") == "1":
        print("Restart skipped by BABEL_DEMO_SKIP_RESTART=1")
        return
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "restart",
        "gateway",
    )
    if await process.wait() != 0:
        raise RuntimeError("docker compose restart gateway failed")
    status = await wait_for_gateway()
    async with httpx.AsyncClient(timeout=5) as client:
        after_response = await client.get(f"{BASE_URL}/services")
        after_response.raise_for_status()
        after = configuration_snapshot(after_response.json()["services"])
    assert after == before, "Registry configuration changed across gateway restart"
    show("Post-restart status", status)
    print("Registry protocol, endpoint, capability, routing metadata, and version persisted.")


async def main() -> None:
    print("Starting strict Babel Gateway demonstration suite...")
    status = await wait_for_gateway()

    async with httpx.AsyncClient(timeout=12) as client:
        await control(client, "POST", "/reset")

        print_step("1-2. Startup, live health checks, and connection to all backends")
        show("Gateway status", status)
        assert set(status["backends"]) == {"service-a", "service-b", "service-c"}
        assert all(value == "available" for value in status["backends"].values())
        services_response = await client.get(f"{BASE_URL}/services")
        services_response.raise_for_status()
        services = services_response.json()["services"]
        assert len(services) == 3
        before_restart = configuration_snapshot(services)

        print_step("3. HTTP, framed TCP, and CRC32 UDP translation")
        cases = [
            ("demo-http", "uppercase", {"value": "tower of babel"}, "service-a", {"value": "TOWER OF BABEL"}),
            ("demo-tcp", "reverse", {"value": "babel"}, "service-b", {"value": "lebab"}),
            ("demo-udp", "sum", {"values": [10, 20, 30]}, "service-c", {"value": 60}),
        ]
        for request_id, operation, arguments, service_id, expected in cases:
            code, body = await execute(
                client,
                request_id,
                operation,
                arguments,
                preferred_service=service_id,
            )
            show(request_id, body)
            assert code == 200 and body["status"] == "success"
            assert body["request_id"] == request_id
            assert body["service_id"] == service_id and body["result"] == expected

        print_step("4. Routing from persisted capability metadata")
        code, body = await execute(
            client,
            "demo-capability-route",
            "sum",
            {"values": [5, 15]},
            preferred_service="service-a",
        )
        show("Capability route", body)
        assert code == 200 and body["service_id"] == "service-c"

        print_step("5. Concurrent correlation safety across all protocols")
        services_for_echo = ("service-a", "service-b", "service-c")
        tasks = [
            execute(
                client,
                f"demo-concurrent-{index:03d}",
                "echo",
                {"value": f"value-{index}"},
                preferred_service=services_for_echo[index % 3],
                timeout_ms=4000,
            )
            for index in range(60)
        ]
        concurrent = await asyncio.gather(*tasks)
        for index, (code, body) in enumerate(concurrent):
            assert code == 200 and body["status"] == "success"
            assert body["request_id"] == f"demo-concurrent-{index:03d}"
            assert body["result"] == {"value": f"value-{index}"}
        print("60/60 concurrent responses correlated to the correct request and value.")

        print_step("6. Global timeout budget without unsafe cross-backend duplication")
        await control(
            client,
            "POST",
            "/services/service-a/faults",
            json={"faults": [{
                "mode": "delayed_response",
                "delay_ms": 3000,
                "remaining_occurrences": 1,
                "request_id": "demo-timeout",
            }]},
        )
        started = time.monotonic()
        code, body = await execute(
            client,
            "demo-timeout",
            "uppercase",
            {"value": "fast"},
            preferred_service="service-a",
            timeout_ms=500,
        )
        elapsed = time.monotonic() - started
        show("Timeout response", body)
        assert code == 408 and body["error"]["code"] == "BACKEND_TIMEOUT"
        assert elapsed < 1.5
        await control(client, "POST", "/reset")

        print_step("7. Corrupt UDP response rejection and same-attempt retransmission")
        await control(
            client,
            "POST",
            "/services/service-c/faults",
            json={"faults": [{
                "mode": "corrupt_checksum",
                "remaining_occurrences": 1,
                "operation": "sum",
            }]},
        )
        code, body = await execute(
            client,
            "demo-corrupt-crc",
            "sum",
            {"values": [1, 1]},
            preferred_service="service-c",
            timeout_ms=2500,
        )
        show("Recovered UDP response", body)
        assert code == 200 and body["service_id"] == "service-c"
        assert body["result"] == {"value": 2}
        await control(client, "POST", "/reset")

        print_step("8. Clear unsupported-operation error")
        code, body = await execute(client, "demo-unsupported", "teleport", {})
        show("Unsupported response", body)
        assert code == 400 and body["error"]["code"] == "UNSUPPORTED_OPERATION"

        print_step("9. Failed migration and broken adapter retain the old implementation")
        migration = await client.post(
            f"{BASE_URL}/admin/services/service-a/migrate",
            json={"target_version": "2", "drain_in_flight": True},
        )
        migration.raise_for_status()
        migration_body = migration.json()
        show("Rejected incompatible migration", migration_body)
        assert migration_body["status"] == "error"
        assert migration_body["current_version"] == "1"

        broken_adapter = await client.post(
            f"{BASE_URL}/admin/services/service-a/adapters/load",
            json={
                "version": "1",
                "module_path": "src.adapters.service_c",
                "class_name": "ServiceCAdapter",
            },
        )
        broken_adapter.raise_for_status()
        broken_body = broken_adapter.json()
        show("Rejected incompatible adapter", broken_body)
        assert broken_body["status"] == "error"

        crashing_adapter = await client.post(
            f"{BASE_URL}/admin/services/service-a/adapters/load",
            json={
                "version": "1",
                "module_path": "crashing_adapter",
                "class_name": "CrashingServiceAAdapter",
                "validation_timeout_ms": 3000,
            },
        )
        crashing_adapter.raise_for_status()
        crashing_body = crashing_adapter.json()
        show("Isolated crashing adapter", crashing_body)
        assert crashing_body["status"] == "error"
        still_alive = await client.get(f"{BASE_URL}/status")
        still_alive.raise_for_status()
        assert still_alive.json()["status"] == "ok"

        valid_adapter = await client.post(
            f"{BASE_URL}/admin/services/service-a/adapters/load",
            json={
                "version": "1",
                "module_path": "service_a_passthrough",
                "class_name": "PassthroughServiceAAdapter",
                "validation_timeout_ms": 3000,
            },
        )
        valid_adapter.raise_for_status()
        valid_body = valid_adapter.json()
        show("Hot-loaded compatible adapter", valid_body)
        assert valid_body["status"] == "success"

        code, body = await execute(
            client,
            "demo-old-adapter-intact",
            "uppercase",
            {"value": "intact"},
            preferred_service="service-a",
        )
        assert code == 200 and body["service_id"] == "service-a"
        assert body["result"] == {"value": "INTACT"}

        rollback = await client.post(
            f"{BASE_URL}/admin/services/service-a/adapters/1/rollback"
        )
        rollback.raise_for_status()
        rollback_body = rollback.json()
        show("Runtime adapter rollback", rollback_body)
        assert rollback_body["status"] == "success"
        await control(client, "POST", "/reset")

    print_step("10. Gateway restart with persisted registry configuration")
    await restart_gateway_and_verify(before_restart)
    print("\nAll mandatory and implemented bonus demo assertions passed.")


if __name__ == "__main__":
    asyncio.run(main())
