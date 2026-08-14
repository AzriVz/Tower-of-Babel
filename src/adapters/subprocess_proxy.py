from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from typing import Any, Dict

from src.adapters.base import AdapterError, BackendTimeoutError, BaseAdapter


async def invoke_plugin_worker(payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    serialized = json.dumps(payload, allow_nan=False).encode("utf-8")

    def run_worker() -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [sys.executable, "-m", "src.adapters.plugin_worker"],
            input=serialized,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=timeout_s,
            check=False,
        )

    try:
        completed = await asyncio.to_thread(run_worker)
    except subprocess.TimeoutExpired as exc:
        raise BackendTimeoutError(
            "Runtime plugin subprocess exceeded its deadline.",
            safe_to_fallback=False,
        ) from exc

    if completed.returncode != 0:
        raise AdapterError(
            "PLUGIN_PROCESS_FAILED",
            f"Runtime plugin process exited with status {completed.returncode}.",
            retryable=False,
            safe_to_fallback=False,
        )
    stdout = completed.stdout
    if len(stdout) > 1_048_576:
        raise AdapterError(
            "PLUGIN_INVALID_RESPONSE",
            "Runtime plugin response exceeded the 1 MiB isolation limit.",
        )
    try:
        response = json.loads(stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdapterError(
            "PLUGIN_INVALID_RESPONSE",
            "Runtime plugin produced an invalid worker response.",
        ) from exc
    if not isinstance(response, dict):
        raise AdapterError("PLUGIN_INVALID_RESPONSE", "Plugin worker response must be an object.")
    if not response.get("ok"):
        error = response.get("error", {})
        if not isinstance(error, dict):
            error = {}
        raise AdapterError(
            str(error.get("code", "PLUGIN_EXECUTION_ERROR")),
            str(error.get("message", "Runtime plugin execution failed.")),
            retryable=bool(error.get("retryable", False)),
            safe_to_fallback=bool(error.get("safe_to_fallback", False)),
        )
    result = response.get("result")
    if not isinstance(result, dict):
        raise AdapterError("PLUGIN_INVALID_RESPONSE", "Plugin worker result must be an object.")
    return result


class SubprocessAdapterProxy(BaseAdapter):
    """Execute a hot-loaded plugin outside the gateway event-loop process."""

    def __init__(
        self,
        module_path: str,
        class_name: str,
        service_id: str,
        protocol_name: str,
    ) -> None:
        self.module_path = module_path
        self.class_name = class_name
        self._service_id = service_id
        self._protocol_name = protocol_name

    @classmethod
    async def create(
        cls,
        module_path: str,
        class_name: str,
        timeout_s: float = 3.0,
    ) -> "SubprocessAdapterProxy":
        description = await invoke_plugin_worker(
            {
                "action": "describe",
                "module_path": module_path,
                "class_name": class_name,
            },
            timeout_s,
        )
        service_id = description.get("service_id")
        protocol_name = description.get("protocol_name")
        if not isinstance(service_id, str) or not isinstance(protocol_name, str):
            raise AdapterError(
                "PLUGIN_INVALID_CONTRACT",
                "Plugin description is missing service_id or protocol_name.",
            )
        return cls(module_path, class_name, service_id, protocol_name)

    @property
    def service_id(self) -> str:
        return self._service_id

    @property
    def protocol_name(self) -> str:
        return self._protocol_name

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        return await invoke_plugin_worker(
            {
                "action": "execute",
                "module_path": self.module_path,
                "class_name": self.class_name,
                "operation": operation,
                "arguments": arguments,
                "request_id": request_id,
                "timeout_s": timeout_s,
                "version": version,
            },
            timeout_s,
        )

    async def health(self, timeout_s: float, version: str = "1") -> bool:
        result = await invoke_plugin_worker(
            {
                "action": "health",
                "module_path": self.module_path,
                "class_name": self.class_name,
                "timeout_s": timeout_s,
                "version": version,
            },
            timeout_s,
        )
        return result.get("healthy") is True
