#!/usr/bin/env python3
"""Probe: BulkheadThreading must not deadlock under concurrent get_stats +
capacity-reject traffic (lock-order inversion).

Finding: get_stats() acquired _stats_lock then _in_flight_lock, while the
capacity-reject path acquired _in_flight_lock then _stats_lock - a classic
AB-BA inversion that can deadlock when the reject path is blocked on the
stats lock exactly while get_stats waits for the in-flight lock.

Assertion: hammering get_stats() from N threads while other threads submit
to a full bulkhead (reject path) must always complete.  Runs in a subprocess
with a watchdog so a deadlock is detected as a timeout, not a hang.
FAILS (deadlock) on bulkman <=1.2.2 under the right interleaving; the fixed
code never nests the two locks, so this cannot deadlock.
"""

import subprocess
import sys

STRESS = r"""
import sys
import threading
import time

from bulkman import BulkheadConfig, BulkheadThreading

bulkhead = BulkheadThreading(
    BulkheadConfig(
        name="probe_lock_order",
        max_concurrent_calls=1,
        max_queue_size=0,
        circuit_breaker_enabled=False,
    )
)
hold = threading.Event()
bulkhead.execute(lambda: hold.wait(timeout=30.0))
time.sleep(0.05)  # fill the single slot

stop = threading.Event()
errors = []

def statter():
    try:
        while not stop.is_set():
            bulkhead.get_stats()
    except Exception as e:
        errors.append(("stats", e))

def submitter():
    try:
        while not stop.is_set():
            try:
                bulkhead.execute(lambda: 1)  # reject path (capacity full)
            except Exception:
                pass
    except Exception as e:
        errors.append(("submit", e))

threads = [threading.Thread(target=statter) for _ in range(4)]
threads += [threading.Thread(target=submitter) for _ in range(4)]
for t in threads:
    t.start()
time.sleep(2.0)
stop.set()
for t in threads:
    t.join(timeout=5.0)
hold.set()
bulkhead.shutdown(wait=False)

if errors:
    print("STRESS-ERRORS:", errors)
    sys.exit(1)
print("STRESS-OK")
"""


def main() -> int:
    try:
        proc = subprocess.run(
            [sys.executable, "-c", STRESS],
            capture_output=True,
            text=True,
            timeout=30.0,
        )
    except subprocess.TimeoutExpired:
        print("FAIL: deadlock detected - stress run did not complete in 30s")
        return 1

    if proc.returncode != 0 or "STRESS-OK" not in proc.stdout:
        print(f"FAIL: stress run failed rc={proc.returncode}")
        print(proc.stdout[-2000:])
        print(proc.stderr[-2000:])
        return 1

    print("PASS: concurrent get_stats + reject traffic completed (no deadlock)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
