#!/usr/bin/env python3
"""Probe: shutdown() racing an in-flight execute() must not crash with
AttributeError (circuit-breaker TOCTOU).

Finding (round 2): _check_circuit / get_state / get_stats / _mark_* read
self._circuit_breaker OUTSIDE _circuit_lock, then used the attribute inside
the lock.  shutdown() nils the attribute between the check and the locked
use, so execute() crashed with "AttributeError: 'NoneType' object has no
attribute '_status'".

Assertion: force the exact interleaving (park the caller after its None-check,
run shutdown, release); the caller must complete without AttributeError.
FAILS on the round-1 code. PASSES on the fix (local capture before the lock).
"""

import sys
import threading
import time

import anyio

from bulkman import Bulkhead, BulkheadConfig, BulkheadThreading


def pause_lock(bulkhead):
    """Wrap the bulkhead's circuit lock so the FIRST acquisition blocks on a
    gate, letting the main thread run shutdown() between the caller's
    None-check and its locked use of the breaker."""
    inner = bulkhead._circuit_lock
    gate = threading.Event()
    state = {"blocked": False}

    class OncePausingLock:
        def __enter__(self):
            if not state["blocked"]:
                state["blocked"] = True
                gate.wait(timeout=5.0)
            return inner.__enter__()

        def __exit__(self, *args):
            return inner.__exit__(*args)

    bulkhead._circuit_lock = OncePausingLock()
    return gate


def check_threading() -> bool:
    bulkhead = BulkheadThreading(
        BulkheadConfig(name="probe_toctou_t", circuit_breaker_enabled=True)
    )
    gate = pause_lock(bulkhead)
    outcome = {}

    def t_execute():
        try:
            bulkhead.execute(lambda: 1)
            outcome["out"] = "ok"
        except Exception as e:
            outcome["out"] = type(e).__name__

    t = threading.Thread(target=t_execute)
    t.start()
    time.sleep(0.2)  # T parked after the None-check, before the lock
    bulkhead.shutdown(wait=False)
    gate.set()
    t.join(timeout=5.0)
    return outcome.get("out") != "AttributeError" and not t.is_alive()


async def check_async() -> bool:
    bulkhead = Bulkhead(BulkheadConfig(name="probe_toctou_a", circuit_breaker_enabled=True))
    gate = pause_lock(bulkhead)
    outcome = {}

    async def t_execute():
        try:
            result = await bulkhead.execute(lambda: 1)
            outcome["out"] = "ok" if result.success else "failed"
        except Exception as e:
            outcome["out"] = type(e).__name__

    async with anyio.create_task_group() as tg:
        tg.start_soon(t_execute)
        await anyio.sleep(0.2)
        await bulkhead.shutdown()
        gate.set()
    return outcome.get("out") != "AttributeError"


def main() -> int:
    threading_ok = check_threading()
    async_ok = anyio.run(check_async)
    if not threading_ok or not async_ok:
        print("FAIL: shutdown race crashed a caller (AttributeError)")
        return 1
    print("PASS: shutdown racing execute() no longer crashes (threading + async)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
