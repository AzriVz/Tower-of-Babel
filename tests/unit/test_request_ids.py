from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from src.core.request_ids import MonotonicIdentifierAllocator


def test_identifier_allocator_is_unique_under_concurrency() -> None:
    allocator = MonotonicIdentifierAllocator(64)
    with ThreadPoolExecutor(max_workers=16) as executor:
        values = list(executor.map(lambda _: allocator.next(), range(10_000)))
    assert len(values) == len(set(values))
    assert 0 not in values
