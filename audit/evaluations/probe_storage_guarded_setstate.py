#!/usr/bin/env python3
"""Probe: resilient_circuit's storage must refuse blind writes over a live OPEN.

Companion to probe_multiprocess_circuit.py.  That probe exercises the full
bulkman integration, but bulkman's own _persist_circuit_state guard masks the
upstream defect - it passes even on resilient-circuit 0.4.x, so it cannot
distinguish "upstream fixed" from "our belt held".  This probe talks to
resilient_circuit's public storage API directly, bypassing bulkman's guard,
and asserts the 0.5.0 storage-layer contract:

  1. A stale CLOSED write over a stored OPEN with unexpired open_until is
     REFUSED - the row stays OPEN.
  2. A second opener cannot move a live open_until - first opener wins.
  3. Both directions: once open_until expires, writes are accepted again
     (recovery OPEN -> CLOSED is never blocked).

RED on resilient-circuit 0.4.x (last-writer-wins), GREEN on >=0.5.0.
Validated red on the known-positive 0.4.7 wheel before its green was trusted.

Requires PostgreSQL (RC_DB_* env) - skipped otherwise.
"""

import os
import sys
import time
from pathlib import Path

_env = Path(__file__).parent.parent.parent / "test.env"
if _env.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_env)
    except ImportError:
        pass

from resilient_circuit.storage import create_storage

KEY = "guarded_setstate_probe"
NS = "audit_storage_guard"


def _clean() -> None:
    import psycopg

    with psycopg.connect(
        host=os.getenv("RC_DB_HOST"),
        port=os.getenv("RC_DB_PORT"),
        dbname=os.getenv("RC_DB_NAME"),
        user=os.getenv("RC_DB_USER"),
        password=os.getenv("RC_DB_PASSWORD"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rc_circuit_breakers WHERE namespace = %s", (NS,))
        conn.commit()


def main() -> int:
    if not os.getenv("RC_DB_HOST"):
        print("SKIP: PostgreSQL not configured (missing RC_DB_* environment variables)")
        return 0

    _clean()
    storage = create_storage(namespace=NS)

    # Known-positive first: a write to an absent row must land and read back.
    first_open_until = time.time() + 60.0
    storage.set_state(KEY, "OPEN", 5, first_open_until)
    row = storage.get_state(KEY)
    if row is None or row["state"] != "OPEN":
        print(f"FAIL: known-positive write did not land: {row}")
        return 1

    # 1. Stale CLOSED over live OPEN must be refused.
    storage.set_state(KEY, "CLOSED", 0, 0.0)
    row = storage.get_state(KEY)
    if row is None or row["state"] != "OPEN":
        print(f"FAIL: stale CLOSED clobbered a live OPEN: {row}")
        return 1

    # 2. Second opener must not move the live cooldown end.
    storage.set_state(KEY, "OPEN", 5, time.time() + 3600.0)
    row = storage.get_state(KEY)
    stored_until = float(row["open_until"])
    if abs(stored_until - first_open_until) > 1.0:
        print(
            f"FAIL: second opener moved open_until: stored={stored_until} "
            f"first={first_open_until}"
        )
        return 1

    # 3. Recovery direction: an EXPIRED open must accept writes again.
    _clean()
    storage.set_state(KEY, "OPEN", 5, time.time() + 1.0)
    time.sleep(1.5)
    storage.set_state(KEY, "CLOSED", 0, 0.0)
    row = storage.get_state(KEY)
    if row is None or row["state"] != "CLOSED":
        print(f"FAIL: recovery blocked - CLOSED refused after expiry: {row}")
        return 1

    print("PASS: live OPEN immutable to blind writers; recovery unblocked after expiry")
    return 0


if __name__ == "__main__":
    sys.exit(main())
