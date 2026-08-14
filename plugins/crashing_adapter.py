from __future__ import annotations

import os
from typing import Any, Dict

from src.adapters.service_a import ServiceAAdapter


class CrashingServiceAAdapter(ServiceAAdapter):
    """Fault fixture proving a plugin process cannot terminate the gateway."""

    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        os._exit(7)
