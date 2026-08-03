#!/usr/bin/env python3
"""Probe: BulkheadSync.shutdown() must work and be idempotent.

Findings:
1. BulkheadSync.shutdown() raised TypeError: ThreadPoolExecutor.shutdown()
   got an unexpected keyword argument 'timeout'.
2. The async Bulkhead had no shutdown() method at all (AttributeError).

Assertion: shutdown() completes without raising, twice in a row, and the
executor is actually shut down afterwards (further execute() raises).
FAILS on bulkman <=1.2.2. PASSES on the fixed version.
"""

import sys

from bulkman import BulkheadConfig
from bulkman.sync_bridge import BulkheadSync


def main() -> int:
    bulkhead = BulkheadSync(
        BulkheadConfig(name="probe_sync_shutdown", circuit_breaker_enabled=False)
    )
    try:
        future = bulkhead.execute(lambda: 42)
        if not future.result(timeout=5.0).success:
            print("FAIL: baseline execute did not succeed")
            return 1
        bulkhead.shutdown()
        bulkhead.shutdown()  # idempotent
    except Exception as e:
        print(f"FAIL: shutdown raised {type(e).__name__}: {e}")
        return 1

    try:
        bulkhead.execute(lambda: 1)
        print("FAIL: execute() after shutdown should raise (executor closed)")
        return 1
    except RuntimeError:
        pass

    print("PASS: BulkheadSync.shutdown() works, is idempotent, closes executor")
    return 0


if __name__ == "__main__":
    sys.exit(main())
