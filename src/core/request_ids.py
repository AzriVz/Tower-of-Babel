from __future__ import annotations

import secrets
import threading


class MonotonicIdentifierAllocator:
    """Allocate non-zero identifiers without collisions inside one process."""

    def __init__(self, bits: int) -> None:
        if bits < 2 or bits > 64:
            raise ValueError("bits must be between 2 and 64")
        self._mask = (1 << bits) - 1
        self._value = secrets.randbits(bits) or 1
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            result = self._value
            self._value = (self._value + 1) & self._mask
            if self._value == 0:
                self._value = 1
            return result


backend_request_ids = MonotonicIdentifierAllocator(64)
udp_sequence_numbers = MonotonicIdentifierAllocator(32)
