from __future__ import annotations

import os
from pathlib import Path


class Settings:
    GATEWAY_ID: str = os.getenv("BABEL_GATEWAY_ID", "candidate-gateway")
    DEFAULT_TIMEOUT_MS: float = float(os.getenv("BABEL_DEFAULT_TIMEOUT_MS", "2000"))
    HEALTHCHECK_TIMEOUT_S: float = float(os.getenv("BABEL_HEALTHCHECK_TIMEOUT_S", "0.75"))
    HEALTHCHECK_INTERVAL_S: float = float(os.getenv("BABEL_HEALTHCHECK_INTERVAL_S", "5"))
    STATE_DIR: Path = Path(os.getenv("BABEL_STATE_DIR", "/state"))

    SERVICE_A_URL: str = os.getenv("SERVICE_A_URL", "http://localhost:8101")
    SERVICE_B_HOST: str = os.getenv("SERVICE_B_HOST", "localhost")
    SERVICE_B_PORT: int = int(os.getenv("SERVICE_B_PORT", "8201"))
    SERVICE_C_HOST: str = os.getenv("SERVICE_C_HOST", "localhost")
    SERVICE_C_PORT: int = int(os.getenv("SERVICE_C_PORT", "8301"))

    # State file path
    REGISTRY_FILE: Path = STATE_DIR / "gateway_registry.json"


settings = Settings()
