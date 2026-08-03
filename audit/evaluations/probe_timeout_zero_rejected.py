#!/usr/bin/env python3
"""Probe: timeout_seconds=0.0 must be rejected, not interpreted differently
by the two implementations.

Finding (round 2): timeout_seconds=0.0 meant "instant timeout" on the async
Bulkhead (move_on_after(0)) but "no timeout" on BulkheadThreading (falsy
check) - one config value, two opposite meanings.

Assertion: constructing a config with timeout_seconds=0.0 raises ValueError.
FAILS on the round-1 code (accepted silently). PASSES on the fix.
"""

import sys

from bulkman import BulkheadConfig


def main() -> int:
    try:
        BulkheadConfig(name="probe_t0", timeout_seconds=0.0)
    except ValueError as e:
        print(f"PASS: timeout_seconds=0.0 rejected: {e}")
        return 0
    print("FAIL: timeout_seconds=0.0 accepted (ambiguous semantics)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
