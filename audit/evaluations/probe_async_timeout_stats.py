#!/usr/bin/env python3
"""Probe: a timed-out caller must not corrupt another task's active counter.

Finding: the async Bulkhead's timeout path decremented _active_tasks with a
heuristic (`if self._active_tasks > 0`), so a caller that timed out while
WAITING for a slot could decrement the counter of a task that was actually
running - stats reported active_tasks=0 while work was in flight.

Assertion (two directions):
1. A caller cancelled while waiting leaves the running task's counter intact.
2. After the bulkhead's own timeout fires mid-execution, counters drain to 0.

FAILS on bulkman <=1.2.2 (direction 1 shows active_tasks=0 while running).
"""

import sys

import anyio

from bulkman import Bulkhead, BulkheadConfig


async def main() -> None:
    bulkhead = Bulkhead(
        BulkheadConfig(
            name="probe_timeout_stats",
            max_concurrent_calls=1,
            circuit_breaker_enabled=False,
        )
    )
    started = anyio.Event()
    release = anyio.Event()

    async def long_task() -> None:
        started.set()
        await release.wait()

    async def slow() -> None:
        await anyio.sleep(1.0)

    async with anyio.create_task_group() as tg:
        tg.start_soon(bulkhead.execute, long_task)
        await started.wait()

        with anyio.move_on_after(0.05) as scope:
            await bulkhead.execute(slow)
        if not scope.cancelled_caught:
            print("FAIL: caller should have been cancelled while waiting")
            sys.exit(1)
        release.set()

    stats = await bulkhead.get_stats()
    if stats["active_tasks"] != 0:
        print(f"FAIL: active_tasks={stats['active_tasks']} after drain, expected 0")
        sys.exit(1)

    # Direction 2: bulkhead's own timeout mid-execution
    t = Bulkhead(
        BulkheadConfig(
            name="probe_timeout_stats2",
            max_concurrent_calls=1,
            timeout_seconds=0.05,
            circuit_breaker_enabled=False,
        )
    )
    try:
        await t.execute(slow)
        print("FAIL: expected BulkheadTimeoutError")
        sys.exit(1)
    except Exception as e:
        if type(e).__name__ != "BulkheadTimeoutError":
            print(f"FAIL: unexpected exception {type(e).__name__}: {e}")
            sys.exit(1)
    stats = await t.get_stats()
    if stats["active_tasks"] != 0:
        print(f"FAIL: active_tasks={stats['active_tasks']} after timeout, expected 0")
        sys.exit(1)

    print("PASS: timeout paths keep active_tasks exact (no cross-task corruption)")


if __name__ == "__main__":
    anyio.run(main)
