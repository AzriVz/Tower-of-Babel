from __future__ import annotations

import asyncio
import json
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from src.main import app, registry, settings


@pytest.fixture
def client(tmp_path: Path):
    settings.STATE_DIR = tmp_path
    settings.REGISTRY_FILE = tmp_path / "gateway_registry.json"
    registry.persistence_file = settings.REGISTRY_FILE
    
    with TestClient(app) as test_client:
        yield test_client


def test_get_services(client: TestClient):
    response = client.get("/services")
    assert response.status_code == 200
    data = response.json()
    assert "services" in data
    service_ids = [s["service_id"] for s in data["services"]]
    assert "service-a" in service_ids
    assert "service-b" in service_ids
    assert "service-c" in service_ids


def test_get_status(client: TestClient):
    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["gateway_id"] == "candidate-gateway"
    assert "backends" in data
    assert data["backends"]["service-a"] in {"available", "unavailable", "unknown"}
    assert {item["service_id"] for item in data["services"]} == {
        "service-a",
        "service-b",
        "service-c",
    }
    assert all("protocol" in item and "capabilities" in item and "version" in item for item in data["services"])


def test_execute_unsupported_operation(client: TestClient):
    payload = {
        "request_id": "test-req-001",
        "operation": "non_existent_op",
        "arguments": {},
        "options": {}
    }
    response = client.post("/execute", json=payload)
    assert response.status_code == 400
    data = response.json()
    assert data["status"] == "error"
    assert data["request_id"] == "test-req-001"
    assert data["error"]["code"] == "UNSUPPORTED_OPERATION"


def test_execute_malformed_json(client: TestClient):
    response = client.post("/execute", content="invalid json", headers={"Content-Type": "application/json"})
    assert response.status_code == 400
    data = response.json()
    assert data["status"] == "error"
    assert data["error"]["code"] == "INVALID_REQUEST"


def test_admin_migration(client: TestClient):
    response = client.post("/admin/services/service-a/migrate", json={"target_version": "2"})
    assert response.status_code == 200
    data = response.json()
    # The supplied backend only supports v1. A safe migration must reject v2
    # without corrupting or replacing the active v1 configuration.
    assert data["status"] == "error"
    assert data["current_version"] == "1"

    # Check services endpoint reflects updated version
    services_resp = client.get("/services")
    services = services_resp.json()["services"]
    service_a = next(s for s in services if s["service_id"] == "service-a")
    assert service_a["version"] == "1"


@pytest.mark.parametrize(
    "payload",
    [
        {"request_id": "missing-fields", "operation": "echo"},
        {
            "request_id": "negative-timeout",
            "operation": "echo",
            "arguments": {"value": "x"},
            "options": {"timeout_ms": -1},
        },
        {
            "request_id": "invalid-sum",
            "operation": "sum",
            "arguments": {"values": [1, "2"]},
            "options": {},
        },
        {
            "request_id": "out-of-range-sum",
            "operation": "sum",
            "arguments": {"values": [2147483648]},
            "options": {},
        },
    ],
)
def test_execute_contract_validation(client: TestClient, payload: dict):
    response = client.post("/execute", json=payload)
    assert response.status_code == 400
    body = response.json()
    assert body["status"] == "error"
    assert body["result"] is None
    assert body["error"]["code"] == "INVALID_REQUEST"
