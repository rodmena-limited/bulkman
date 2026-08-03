#!/usr/bin/env python3
"""Probe: the async API works on a pure asyncio event loop, trio-free.

Finding: bulkman was built on trio; users on asyncio had to bridge through a
background trio thread even for the primary async API.

Assertion: a plain `asyncio.run(main())` program can create a Bulkhead,
execute sync and async functions, enforce capacity (BulkheadFullError), and
drain counters after a timeout - with no trio imported anywhere.
FAILS on bulkman <=1.2.2 (async API requires a trio loop).
"""

import asyncio
import sys

import bulkman
from bulkman import Bulkhead, BulkheadConfig, BulkheadFullError

if "trio" in sys.modules:
    print("FAIL: trio was imported by the asyncio probe harness")
    sys.exit(1)


async def main() -> None:
    bulkhead = Bulkhead(
        BulkheadConfig(
            name="probe_asyncio",
            max_concurrent_calls=2,
            max_queue_size=10,
            circuit_breaker_enabled=False,
        )
    )

    async def slow(x: int) -> int:
        await asyncio.sleep(0.05)
        return x * 2

    results = await asyncio.gather(*(bulkhead.execute(slow, i) for i in range(4)))
    if not all(r.success for r in results):
        print("FAIL: concurrent async executions failed")
        sys.exit(1)

    sync_result = await bulkhead.execute(lambda x: x + 1, 41)
    if not sync_result.success or sync_result.result != 42:
        print("FAIL: sync function via worker thread failed")
        sys.exit(1)

    tight = Bulkhead(
        BulkheadConfig(
            name="probe_asyncio_tight",
            max_concurrent_calls=1,
            max_queue_size=0,
            circuit_breaker_enabled=False,
        )
    )
    held = asyncio.Event()

    async def hold() -> None:
        held.set()
        await asyncio.sleep(0.3)

    task = asyncio.create_task(tight.execute(hold))
    await held.wait()
    try:
        await tight.execute(lambda: 1)
    except BulkheadFullError:
        pass
    else:
        print("FAIL: expected BulkheadFullError on asyncio")
        sys.exit(1)
    await task

    t = Bulkhead(
        BulkheadConfig(
            name="probe_asyncio_timeout",
            timeout_seconds=0.05,
            circuit_breaker_enabled=False,
        )
    )

    async def slow_forever() -> None:
        await asyncio.sleep(1.0)

    try:
        await t.execute(slow_forever)
    except bulkman.exceptions.BulkheadTimeoutError:
        pass
    else:
        print("FAIL: expected BulkheadTimeoutError on asyncio")
        sys.exit(1)
    if (await t.get_stats())["active_tasks"] != 0:
        print("FAIL: active_tasks did not drain after timeout on asyncio")
        sys.exit(1)

    print("PASS: async API works end-to-end on a pure asyncio loop (no trio)")


if __name__ == "__main__":
    asyncio.run(main())
