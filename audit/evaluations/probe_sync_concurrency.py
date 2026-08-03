#!/usr/bin/env python3
"""Probe: async bulkhead sync-function concurrency must equal
max_concurrent_calls, not the anyio shared pool cap (40).

Finding (round 3): sync functions ran in anyio's shared thread pool (40
threads), so with max_concurrent_calls=100 the peak real concurrency was 40,
while the other 60 admitted tasks held semaphore permits waiting for pool
threads.

Assertion: with max_concurrent_calls=100, 100 concurrent sync tasks reach a
peak > 40 (the dedicated pool serves the configured limit).
FAILS on the round-2 code (peak == 40). PASSES on the fix.
"""

import sys
import threading
import time

import anyio

from bulkman import Bulkhead, BulkheadConfig

active = 0
peak = 0
lock = threading.Lock()


def sync_work():
    global active, peak
    with lock:
        active += 1
        peak = max(peak, active)
    time.sleep(0.3)
    with lock:
        active -= 1


async def main() -> None:
    bulkhead = Bulkhead(
        BulkheadConfig(
            name="probe_sync_pool",
            max_concurrent_calls=100,
            circuit_breaker_enabled=False,
        )
    )

    async def run_one():
        result = await bulkhead.execute(sync_work)
        if not result.success:
            print(f"FAIL: task failed: {result.error}")
            sys.exit(1)

    async with anyio.create_task_group() as tg:
        for _ in range(100):
            tg.start_soon(run_one)

    if peak <= 40:
        print(f"FAIL: peak concurrent sync executions = {peak} (shared pool cap)")
        sys.exit(1)
    print(f"PASS: peak concurrent sync executions = {peak} (config allows 100)")


if __name__ == "__main__":
    anyio.run(main)
