#!/usr/bin/env python3
"""Probe: the bulkman package must not import or depend on trio.

Finding: bulkman was built on trio (core.py + sync_bridge.py imported it and
pyproject declared it as a runtime dependency).

Assertion: importing the whole package must not put 'trio' in sys.modules.
FAILS on bulkman <=1.2.2. PASSES on the fixed version (AnyIO-based).
"""

import sys

import bulkman  # noqa: F401  (import the package surface)
import bulkman.core  # noqa: F401
import bulkman.sync_bridge  # noqa: F401
import bulkman.threading  # noqa: F401


def main() -> int:
    if "trio" in sys.modules:
        print("FAIL: importing bulkman pulled in the trio module")
        return 1
    if "anyio" not in sys.modules:
        print("FAIL: expected anyio (the migration target) to be loaded")
        return 1
    print("PASS: no trio imported by bulkman (anyio present)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
