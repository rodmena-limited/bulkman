#!/usr/bin/env python3
"""Probe: async Bulkhead must enforce max_queue_size (BulkheadFullError).

Finding: the async Bulkhead ignored max_queue_size entirely - the documented
BulkheadFullError was never raised and the wait queue was unbounded, so a
burst of callers could pile up without limit.

Assertion: with max_concurrent_calls=1 and max_queue_size=0 (capacity 1), a
second execute() while the slot is held raises BulkheadFullError; once the
slot drains, execute() admits again.
FAILS on bulkman <=1.2.2 (second call blocks forever). PASSES on the fix.
"""

import sys

import anyio

from bulkman import Bulkhead, BulkheadConfig, BulkheadFullError


async def main() -> None:
    bulkhead = Bulkhead(
        BulkheadConfig(
            name="probe_async_full",
            max_concurrent_calls=1,
            max_queue_size=0,
            circuit_breaker_enabled=False,
        )
    )
    held = anyio.Event()
    release = anyio.Event()

    async def hold() -> None:
        held.set()
        await release.wait()

    async with anyio.create_task_group() as tg:
        tg.start_soon(bulkhead.execute, hold)
        await held.wait()

        try:
            await bulkhead.execute(lambda: 1)
        except BulkheadFullError:
            pass
        else:
            print("FAIL: expected BulkheadFullError at capacity")
            release.set()
            sys.exit(1)

        release.set()  # drain the slot

    # Capacity restored after drain
    result = await bulkhead.execute(lambda: 42)
    if not result.success or result.result != 42:
        print(f"FAIL: post-drain execute returned {result!r}")
        sys.exit(1)

    stats = await bulkhead.get_stats()
    if stats["rejected_executions"] != 1:
        print(f"FAIL: expected 1 rejected execution, got {stats['rejected_executions']}")
        sys.exit(1)

    print("PASS: async Bulkhead rejects at capacity and admits again after drain")


if __name__ == "__main__":
    anyio.run(main)
