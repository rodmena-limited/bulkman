#!/usr/bin/env python3
"""Probe: a worker-thread BaseException must resolve the bridge future AND
keep the shared portal thread alive.

Findings (round 3):
1. BulkheadSync's returned future never resolved when the worker raised a
   BaseException (run_in_executor only caught Exception) - callers hung.
2. Fixing that naively exposed a worse bug: anyio's portal re-raises
   BaseException inside its task group, which crashed the shared portal
   thread and broke every later BulkheadSync in the process.

Assertion: execute(KeyboardInterrupt func) resolves the future with a failed
ExecutionResult containing a BulkheadError, and a fresh BulkheadSync still
works afterwards (portal survived).
FAILS on the round-2 code (hang) and on the naive fix (portal death).
"""

import sys

from bulkman import BulkheadConfig
from bulkman.exceptions import BulkheadError
from bulkman.sync_bridge import BulkheadSync


def main() -> int:
    bulkhead1 = BulkheadSync(BulkheadConfig(name="probe_ki_1", circuit_breaker_enabled=False))
    try:
        future = bulkhead1.execute(lambda: (_ for _ in ()).throw(KeyboardInterrupt("probe")))
        result = future.result(timeout=5.0)
    except BaseException as e:
        print(f"FAIL: future raised {type(e).__name__} instead of resolving: {e}")
        return 1
    finally:
        bulkhead1.shutdown()

    if result.success or not isinstance(result.error, BulkheadError):
        print(f"FAIL: unexpected result: success={result.success} error={result.error!r}")
        return 1
    if "KeyboardInterrupt" not in str(result.error):
        print(f"FAIL: error does not mention the interrupt: {result.error}")
        return 1

    # Portal survival: a fresh BulkheadSync must work end to end
    bulkhead2 = BulkheadSync(BulkheadConfig(name="probe_ki_2", circuit_breaker_enabled=False))
    try:
        result2 = bulkhead2.execute(lambda: 42).result(timeout=5.0)
    except Exception as e:
        print(f"FAIL: portal died - fresh bulkhead raised {type(e).__name__}: {e}")
        return 1
    finally:
        bulkhead2.shutdown()

    if not result2.success or result2.result != 42:
        print(f"FAIL: fresh bulkhead returned {result2!r}")
        return 1

    print("PASS: worker BaseException resolved; portal survived for later use")
    return 0


if __name__ == "__main__":
    sys.exit(main())
