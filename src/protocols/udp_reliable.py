from __future__ import annotations

import asyncio
import time
import logging
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class AdaptiveRTT:
    """Implements Jacobson/Karn's algorithm for dynamic Retransmission Timeout (RTO) estimation."""
    def __init__(self, initial_rto: float = 0.5, min_rto: float = 0.1, max_rto: float = 2.0) -> None:
        self.srtt: Optional[float] = None
        self.rttvar: Optional[float] = None
        self.rto: float = initial_rto
        self.min_rto: float = min_rto
        self.max_rto: float = max_rto
        self.alpha: float = 0.125
        self.beta: float = 0.25

    def update(self, measured_rtt: float) -> None:
        if self.srtt is None:
            self.srtt = measured_rtt
            self.rttvar = measured_rtt / 2
        else:
            delta = measured_rtt - self.srtt
            self.srtt += self.alpha * delta
            self.rttvar += self.beta * (abs(delta) - self.rttvar)
        self.rto = max(self.min_rto, min(self.max_rto, self.srtt + 4 * self.rttvar))

    def backoff(self) -> None:
        self.rto = min(self.max_rto, self.rto * 2)


class UDPProtocolHandler(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.transport: Optional[asyncio.DatagramTransport] = None
        # Bound hostile/duplicate traffic so one request cannot grow memory
        # without limit while waiting for its correlated response.
        self.incoming_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=16)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            self.incoming_queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning("Dropping UDP datagram because the receive queue is full")

    def error_received(self, exc: Exception) -> None:
        logger.warning(f"UDP datagram error received: {exc}")


class ReliableUDPTransport:
    """
    Provides reliable UDP datagram transmission with sequence numbering,
    adaptive retransmission, duplicate suppression, and checksum validation.
    Compatible with uvloop and standard asyncio event loops.
    """
    def __init__(self, host: str, port: int, max_in_flight: int = 128) -> None:
        self.host = host
        self.port = port
        self.rtt_estimator = AdaptiveRTT()
        self._rtt_lock = asyncio.Lock()
        self._in_flight = asyncio.Semaphore(max_in_flight)

    async def send_and_receive(
        self,
        packet: bytes,
        expected_req_id: int,
        expected_seq_num: int,
        timeout_budget_s: float,
        validate_fn: Callable[[bytes, int, int], Tuple[bool, Optional[bytes], str]],
    ) -> bytes:
        if timeout_budget_s <= 0:
            raise TimeoutError("UDP timeout budget must be positive")

        deadline = time.monotonic() + timeout_budget_s
        loop = asyncio.get_running_loop()

        try:
            async with asyncio.timeout(timeout_budget_s):
                await self._in_flight.acquire()
        except TimeoutError as exc:
            raise TimeoutError("UDP backpressure queue exhausted the timeout budget") from exc

        transport: Optional[asyncio.DatagramTransport] = None
        retransmit_count = 0

        try:
            transport, protocol = await loop.create_datagram_endpoint(
                UDPProtocolHandler,
                remote_addr=(self.host, self.port),
            )

            while time.monotonic() < deadline:
                async with self._rtt_lock:
                    current_rto = self.rtt_estimator.rto
                rto = min(current_rto, max(0.01, deadline - time.monotonic()))
                start_time = time.monotonic()

                # Send datagram
                transport.sendto(packet)

                # Wait for response with current RTO budget
                while time.monotonic() - start_time < rto and time.monotonic() < deadline:
                    step_timeout = min(
                        rto - (time.monotonic() - start_time),
                        max(0.001, deadline - time.monotonic()),
                    )
                    try:
                        data = await asyncio.wait_for(protocol.incoming_queue.get(), timeout=step_timeout)
                        sample_rtt = time.monotonic() - start_time

                        # Validate response
                        is_valid, body, err_msg = validate_fn(data, expected_req_id, expected_seq_num)
                        if is_valid and body is not None:
                            # Update RTT estimation on successful response
                            if retransmit_count == 0:
                                async with self._rtt_lock:
                                    self.rtt_estimator.update(sample_rtt)
                            return data

                        logger.warning(f"UDP datagram received but rejected: {err_msg}")
                    except asyncio.TimeoutError:
                        break

                retransmit_count += 1
                async with self._rtt_lock:
                    self.rtt_estimator.backoff()
                logger.debug(f"UDP timeout ({rto:.3f}s), retransmitting attempt #{retransmit_count}")

            raise TimeoutError(f"Reliable UDP transport timed out after budget {timeout_budget_s}s ({retransmit_count} retries)")

        finally:
            if transport is not None:
                transport.close()
            self._in_flight.release()
