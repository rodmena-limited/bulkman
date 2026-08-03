#!/usr/bin/env python3
"""Probe: circuit breaker is OFF by default.

Finding: BulkheadConfig.circuit_breaker_enabled defaulted to True, so every
bulkhead silently dragged in resilient_circuit machinery (storage, state
machine) and apps had to fight the breaker to keep simple bulkheads simple.

Assertion: a default BulkheadConfig disables the circuit breaker; a Bulkhead
built from it has no CircuitProtectorPolicy.
FAILS on bulkman <=1.2.2 (default True). PASSES on the fixed version.
"""

import sys

from bulkman import Bulkhead, BulkheadConfig


def main() -> int:
    failures = []

    config = BulkheadConfig(name="probe_cb")
    if config.circuit_breaker_enabled is not False:
        failures.append(
            f"circuit_breaker_enabled default is {config.circuit_breaker_enabled!r}, expected False"
        )

    bulkhead = Bulkhead(config)
    if bulkhead._circuit_breaker is not None:
        failures.append("Bulkhead built from default config created a circuit breaker")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1
    print("PASS: circuit breaker is off by default (config + Bulkhead)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
