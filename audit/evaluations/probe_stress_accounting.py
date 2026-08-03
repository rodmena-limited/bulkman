#!/usr/bin/env python3
"""Probe: counter invariants hold under heavy mixed load with concurrent
stats reads (async backend).

Invariants asserted while 300 mixed tasks (sync/async, successes/failures,
timeouts, capacity rejections) run against a capacity-15 bulkhead with a
concurrent get_stats storm:
  - total_executions == successful + failed
  - active_tasks == 0 and in_flight_count == 0 after the storm
  - at least one execution was recorded

FAILS if the round-2 accounting drifts under load (it did not; this locks
the invariant in as a regression probe).
"""

import random
import sys
import time

import anyio

from bulkman import Bulkhead, BulkheadConfig


async def main() -> None:
    rng = random.Random(42)
    bulkhead = Bulkhead(
        BulkheadConfig(
            name="probe_stress",
            max_concurrent_calls=5,
            max_queue_size=10,
            timeout_seconds=0.2,
            circuit_breaker_enabled=False,
        )
    )

    def sync_task(i: int) -> int:
        if rng.random() < 0.2:
            time.sleep(0.05)
        if rng.random() < 0.1:
            raise ValueError(f"sync fail {i}")
        return i

    async def async_task(i: int) -> int:
        await anyio.sleep(0.02)
        if rng.random() < 0.1:
            raise ValueError(f"async fail {i}")
        return i

    async def run_one(i: int) -> None:
        func = sync_task if i % 2 == 0 else async_task
        try:
            await bulkhead.execute(func, i)
        except Exception:
            pass

    async def statter() -> None:
        for _ in range(30):
            await bulkhead.get_stats()
            await anyio.sleep(0.01)

    async with anyio.create_task_group() as tg:
        tg.start_soon(statter)
        for i in range(300):
            tg.start_soon(run_one, i)

    stats = await bulkhead.get_stats()
    ok = (
        stats["total_executions"] == stats["successful_executions"] + stats["failed_executions"]
        and stats["active_tasks"] == 0
        and stats["in_flight_count"] == 0
        and stats["total_executions"] > 0
    )
    if not ok:
        print(f"FAIL: counters drifted: {stats}")
        sys.exit(1)
    print(
        f"PASS: accounting consistent under load "
        f"(total={stats['total_executions']}, rejected={stats['rejected_executions']})"
    )


if __name__ == "__main__":
    anyio.run(main)
