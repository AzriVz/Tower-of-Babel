from __future__ import annotations

import asyncio
from contextlib import redirect_stdout
import importlib
import inspect
import json
import sys
from typing import Any, Dict

from src.adapters.base import AdapterError, BaseAdapter


def load_adapter(module_path: str, class_name: str) -> BaseAdapter:
    # Plugin print output is discarded by the parent. It cannot corrupt the
    # JSON control channel used by the gateway.
    with redirect_stdout(sys.stderr):
        module = importlib.import_module(module_path)
        adapter_class = getattr(module, class_name)
        adapter = adapter_class()
    if not isinstance(adapter, BaseAdapter):
        raise TypeError("Adapter must inherit from BaseAdapter")
    signature = inspect.signature(adapter.execute)
    required = {"operation", "arguments", "request_id", "timeout_s", "version"}
    if not required.issubset(signature.parameters):
        raise TypeError("Adapter execute signature is incompatible")
    if not inspect.iscoroutinefunction(adapter.execute):
        raise TypeError("Adapter execute must be async")
    return adapter


async def run(payload: Dict[str, Any]) -> Dict[str, Any]:
    module_path = payload["module_path"]
    class_name = payload["class_name"]
    if not isinstance(module_path, str) or not isinstance(class_name, str):
        raise TypeError("module_path and class_name must be strings")
    adapter = load_adapter(module_path, class_name)
    action = payload.get("action")
    if action == "describe":
        return {
            "service_id": adapter.service_id,
            "protocol_name": adapter.protocol_name,
        }
    if action == "health":
        with redirect_stdout(sys.stderr):
            healthy = await adapter.health(
                float(payload["timeout_s"]),
                str(payload["version"]),
            )
        return {"healthy": bool(healthy)}
    if action == "execute":
        arguments = payload.get("arguments")
        if not isinstance(arguments, dict):
            raise TypeError("arguments must be an object")
        with redirect_stdout(sys.stderr):
            result = await adapter.execute(
                operation=str(payload["operation"]),
                arguments=arguments,
                request_id=str(payload["request_id"]),
                timeout_s=float(payload["timeout_s"]),
                version=str(payload["version"]),
            )
        if not isinstance(result, dict):
            raise TypeError("Adapter result must be an object")
        return result
    raise ValueError(f"Unknown plugin worker action '{action}'")


def main() -> int:
    try:
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("Worker payload must be an object")
        result = asyncio.run(run(payload))
        response = {"ok": True, "result": result}
    except AdapterError as exc:
        response = {
            "ok": False,
            "error": {
                "code": exc.code,
                "message": exc.message,
                "retryable": exc.retryable,
                "safe_to_fallback": exc.safe_to_fallback,
            },
        }
    except Exception as exc:
        response = {
            "ok": False,
            "error": {
                "code": "PLUGIN_EXECUTION_ERROR",
                "message": f"{type(exc).__name__}: {exc}",
                "retryable": False,
                "safe_to_fallback": False,
            },
        }
    try:
        sys.stdout.write(json.dumps(response, allow_nan=False))
        sys.stdout.flush()
        return 0
    except Exception:
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
