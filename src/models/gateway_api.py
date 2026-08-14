from __future__ import annotations

import math
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


KNOWN_OPERATIONS = {"echo", "uppercase", "sum", "reverse", "metadata"}
INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1


def _reject_non_finite_numbers(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    if isinstance(value, dict):
        for nested in value.values():
            _reject_non_finite_numbers(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_non_finite_numbers(nested)


class ExecuteOptions(BaseModel):
    model_config = ConfigDict(extra="allow", allow_inf_nan=False)

    preferred_service: Optional[str] = None
    timeout_ms: Optional[float] = Field(default=None, gt=0)
    version_preference: Optional[str] = None

    @field_validator("preferred_service")
    @classmethod
    def validate_preferred_service(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.strip():
            raise ValueError("preferred_service cannot be empty")
        return value

    @field_validator("version_preference")
    @classmethod
    def validate_version(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        if not value.isdigit() or not 1 <= int(value) <= 255:
            raise ValueError("version_preference must be an integer string from 1 to 255")
        return value


class ExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    request_id: str = Field(min_length=1, max_length=512)
    operation: str = Field(min_length=1, max_length=64)
    arguments: Dict[str, Any]
    options: ExecuteOptions

    @field_validator("request_id", "operation")
    @classmethod
    def reject_blank_strings(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("field cannot be blank")
        return value

    @model_validator(mode="after")
    def validate_operation_arguments(self) -> "ExecuteRequest":
        _reject_non_finite_numbers(self.arguments)

        if self.operation in {"uppercase", "reverse"}:
            if not isinstance(self.arguments.get("value"), str):
                raise ValueError(f"{self.operation} requires a string 'value'")
        elif self.operation == "echo":
            if "value" not in self.arguments:
                raise ValueError("echo requires a 'value' field")
        elif self.operation == "sum":
            values = self.arguments.get("values")
            if not isinstance(values, list):
                raise ValueError("sum requires a 'values' array")
            for item in values:
                if isinstance(item, bool) or not isinstance(item, (int, float)):
                    raise ValueError("sum values must be JSON numbers")
                if not math.isfinite(float(item)) or not INT32_MIN <= item <= INT32_MAX:
                    raise ValueError("sum values must be finite signed 32-bit numbers")
        elif self.operation == "metadata" and self.arguments:
            raise ValueError("metadata requires an empty arguments object")

        # Unknown operations remain valid at the schema layer so the router can
        # return the documented UNSUPPORTED_OPERATION error envelope.
        return self


class ErrorDetail(BaseModel):
    code: str
    message: str
    retryable: bool = False


class ExecuteResponse(BaseModel):
    request_id: str
    status: Literal["success", "error"]
    service_id: Optional[str] = None
    operation: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[ErrorDetail] = None

    @model_validator(mode="after")
    def validate_envelope_invariant(self) -> "ExecuteResponse":
        if self.status == "success" and (self.result is None or self.error is not None):
            raise ValueError("success response requires result and forbids error")
        if self.status == "error" and (self.result is not None or self.error is None):
            raise ValueError("error response requires error and forbids result")
        return self

    @classmethod
    def make_success(cls, request_id: str, service_id: str, operation: str, result: Dict[str, Any]) -> ExecuteResponse:
        return cls(
            request_id=request_id,
            status="success",
            service_id=service_id,
            operation=operation,
            result=result,
            error=None,
        )

    @classmethod
    def make_error(cls, request_id: str, service_id: Optional[str], operation: str, code: str, message: str, retryable: bool = False) -> ExecuteResponse:
        return cls(
            request_id=request_id,
            status="error",
            service_id=service_id,
            operation=operation,
            result=None,
            error=ErrorDetail(code=code, message=message, retryable=retryable),
        )


class ServiceInfo(BaseModel):
    service_id: str
    protocol: str
    status: str
    capabilities: List[str]
    version: str = "1"
    endpoint: Optional[str] = None
    supported_versions: List[str] = Field(default_factory=list)


class ServicesResponse(BaseModel):
    services: List[ServiceInfo]


class StatusResponse(BaseModel):
    status: str
    gateway_id: str
    uptime_ms: int
    backends: Dict[str, str]
    services: List[ServiceInfo] = Field(default_factory=list)
    circuit_breakers: Dict[str, str] = Field(default_factory=dict)
