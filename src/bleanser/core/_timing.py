from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter, process_time

try:
    import resource
except ImportError:  # pragma: no cover -- resource is Unix-only
    resource = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]


def _child_cpu_time() -> float | None:
    if resource is None:
        return None
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


@contextmanager
def timed(label: str) -> Iterator[None]:
    wall_started = perf_counter()
    process_started = process_time()
    child_started = _child_cpu_time()
    try:
        yield
    finally:
        wall = perf_counter() - wall_started
        process_cpu = process_time() - process_started
        msg = f'[bleanser timing] {label}: wall={wall:.3f}s process_cpu={process_cpu:.3f}s'
        if child_started is not None:
            child_cpu = _child_cpu_time()
            assert child_cpu is not None
            msg += f' child_cpu={child_cpu - child_started:.3f}s'
        print(msg, file=sys.stderr, flush=True)
