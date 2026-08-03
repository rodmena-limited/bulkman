#!/usr/bin/env python3
"""Probe: the async bulkhead must honor timeout_seconds for SYNC functions.

Finding (round 3): sync functions ran through anyio.to_thread.run_sync with
the default abandon_on_cancel=False, whose shielded wait silently disables
the timeout on the asyncio backend - execute(time.sleep(2.0)) with
timeout_seconds=0.1 returned SUCCESS after 2.0s, no timeout at all.

Assertion: with timeout_seconds=0.1 and a sync function sleeping 1.0s,
execute() raises BulkheadTimeoutError within ~0.1s and all counters drain.
FAILS on the round-2 code (returns success after the function completes).
"""

import sys
import time

import anyio

from bulkman import Bulkhead, BulkheadConfig


async def main() -> None:
    bulkhead = Bulkhead(
        BulkheadConfig(
            name="probe_sync_timeout",
            timeout_seconds=0.1,
            circuit_breaker_enabled=False,
        )
    )
    start = time.monotonic()
    try:
        await bulkhead.execute(lambda: time.sleep(1.0))
    except Exception as e:
        elapsed = time.monotonic() - start
        if type(e).__name__ != "BulkheadTimeoutError":
            print(f"FAIL: unexpected exception {type(e).__name__}: {e}")
            sys.exit(1)
        if elapsed >= 0.5:
            print(f"FAIL: timeout fired late ({elapsed:.2f}s) for a sync function")
            sys.exit(1)
    else:
        print("FAIL: sync function ignored the timeout entirely")
        sys.exit(1)

    stats = await bulkhead.get_stats()
    if stats["active_tasks"] != 0 or stats["in_flight_count"] != 0:
        print(f"FAIL: counters did not drain: {stats}")
        sys.exit(1)

    print(f"PASS: sync function timed out after {elapsed:.2f}s; counters drained")


if __name__ == "__main__":
    anyio.run(main)
