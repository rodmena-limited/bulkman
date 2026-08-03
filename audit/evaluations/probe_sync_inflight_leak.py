#!/usr/bin/env python3
"""Probe: BulkheadSync must not leak in-flight slots when the executor
cancels queued tasks at shutdown.

Finding (round 2): the slot was released from run_in_executor's finally, but
executor-cancelled work items never run, so 5 of 6 slots leaked permanently
after shutdown(wait=False).

Assertion: after shutdown with queued tasks cancelled, in_flight_count drops
to the number of still-running tasks and to 0 once they finish.
FAILS on the round-1 code. PASSES on the fix (release tied to the work future).
"""

import sys
import threading
import time

from bulkman import BulkheadConfig
from bulkman.sync_bridge import BulkheadSync


def main() -> int:
    bulkhead = BulkheadSync(
        BulkheadConfig(
            name="probe_sync_inflight",
            max_concurrent_calls=1,
            max_queue_size=5,
            circuit_breaker_enabled=False,
        )
    )
    hold = threading.Event()

    def blocking():
        hold.wait(timeout=30.0)
        return "done"

    for _ in range(6):  # 1 running + 5 queued = capacity 6
        bulkhead.execute(blocking)
    time.sleep(0.2)
    if bulkhead._in_flight_count != 6:
        print(f"FAIL: in_flight_count={bulkhead._in_flight_count} after fill, expected 6")
        return 1

    bulkhead.shutdown(wait=False)  # cancels the 5 queued work items
    time.sleep(0.2)
    if bulkhead._in_flight_count != 1:
        print(
            f"FAIL: in_flight_count={bulkhead._in_flight_count} after shutdown, "
            "expected 1 (only the running task)"
        )
        return 1

    hold.set()
    time.sleep(0.3)
    if bulkhead._in_flight_count != 0:
        print(f"FAIL: in_flight_count={bulkhead._in_flight_count} after drain, expected 0 (leak)")
        return 1

    print("PASS: no in-flight slot leak on executor cancellation at shutdown")
    return 0


if __name__ == "__main__":
    sys.exit(main())
