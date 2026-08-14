from __future__ import annotations

from abc import ABC, abstractmethod
import uuid
from typing import Any, Dict, Optional


class AdapterError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        retryable: bool = False,
        safe_to_fallback: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        # True only when the adapter knows the request was not dispatched, or
        # when the backend explicitly rejected it before execution.
        self.safe_to_fallback = safe_to_fallback


class BackendTimeoutError(AdapterError):
    def __init__(
        self,
        message: str = "Backend operation timed out.",
        safe_to_fallback: bool = False,
    ) -> None:
        super().__init__(
            "BACKEND_TIMEOUT",
            message,
            retryable=True,
            safe_to_fallback=safe_to_fallback,
        )


class ProtocolValidationError(AdapterError):
    def __init__(self, message: str = "Invalid response from backend protocol validation.") -> None:
        super().__init__(
            "PROTOCOL_VALIDATION_ERROR",
            message,
            retryable=False,
            safe_to_fallback=False,
        )


class UnsupportedOperationError(AdapterError):
    def __init__(self, message: str = "Operation not supported by backend.") -> None:
        super().__init__(
            "UNSUPPORTED_OPERATION",
            message,
            retryable=False,
            safe_to_fallback=True,
        )


class BaseAdapter(ABC):
    @property
    @abstractmethod
    def service_id(self) -> str:
        pass

    @property
    @abstractmethod
    def protocol_name(self) -> str:
        pass

    @abstractmethod
    async def execute(
        self,
        operation: str,
        arguments: Dict[str, Any],
        request_id: str,
        timeout_s: float,
        version: str = "1",
    ) -> Dict[str, Any]:
        """
        Executes a normalized operation and returns normalized result dict:
        { "value": ... } or metadata dict.
        Raises AdapterError on error.
        """
        pass

    async def health(self, timeout_s: float, version: str = "1") -> bool:
        """Run a side-effect-free metadata request using the requested version."""
        await self.execute(
            operation="metadata",
            arguments={},
            request_id=f"gateway-health-{uuid.uuid4().hex}",
            timeout_s=timeout_s,
            version=version,
        )
        return True
