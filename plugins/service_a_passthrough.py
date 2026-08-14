from __future__ import annotations

from src.adapters.service_a import ServiceAAdapter


class PassthroughServiceAAdapter(ServiceAAdapter):
    """Example hot-loadable adapter using the documented Service A protocol."""

