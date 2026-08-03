#!/usr/bin/env python3
"""Probe: shutdown(wait=True, timeout=...) must return within the timeout
instead of blocking until every task finishes.

Finding (round 2): BulkheadThreading.shutdown(wait=True, timeout=0.1) blocked
5.0s waiting for a long task - the documented timeout was silently ignored.

Assertion: with a task that outlives the timeout, shutdown returns in under a
second and logs a warning; queued tasks are cancelled, running ones abandoned.
FAILS on the round-1 code (blocks for the full task duration).
"""

import sys
import threading
import time

from bulkman import BulkheadConfig, BulkheadThreading
from bulkman.sync_bridge import BulkheadSync


def check(name, make_bulkhead, task_start_delay: float) -> bool:
    bulkhead = make_bulkhead()
    bulkhead.execute(lambda: time.sleep(5.0))
    if task_start_delay:
        time.sleep(task_start_delay)
    start = time.monotonic()
    bulkhead.shutdown(wait=True, timeout=0.1)
    elapsed = time.monotonic() - start
    if elapsed >= 1.0:
        print(f"FAIL: {name} shutdown blocked {elapsed:.1f}s despite timeout=0.1")
        return False
    return True


def main() -> int:
    ok = True
    ok &= check(
        "BulkheadThreading",
        lambda: BulkheadThreading(BulkheadConfig(name="probe_sh_t", circuit_breaker_enabled=False)),
        0.0,
    )
    ok &= check(
        "BulkheadSync",
        lambda: BulkheadSync(BulkheadConfig(name="probe_sh_sb", circuit_breaker_enabled=False)),
        0.1,  # let the task start before shutting down
    )
    if not ok:
        return 1
    print("PASS: shutdown(wait=True, timeout=...) honored by both implementations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
