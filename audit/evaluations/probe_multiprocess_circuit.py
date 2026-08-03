#!/usr/bin/env python3
"""Probe: concurrent multi-process writes to a shared circuit key must not
lose the protection signal.

Finding (round 6): resilient_circuit's Postgres storage is last-writer-wins
with no cross-process read-modify-write serialization.  A process whose
LOCAL circuit is still CLOSED blindly overwrites another process's freshly
persisted OPEN with CLOSED - reproduced live: process A opened and persisted
OPEN, process B (constructed earlier) recorded successes, and the shared row
flipped to CLOSED; a restarted instance then admitted calls to the failing
dependency.

Assertion: after the same interleaving, the shared row stays OPEN and a
restarted instance blocks.

Requires PostgreSQL (RC_DB_* env) - skipped otherwise.
Fails on the pre-fix code (row ends CLOSED, instance C admitted).
"""

import os
import sys
import time
import multiprocessing as mp
from pathlib import Path

# Load the project's test env (same as tests/conftest.py) so the probe runs
# standalone against the local PostgreSQL.
_env = Path(__file__).parent.parent.parent / "test.env"
if _env.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(_env)
    except ImportError:
        pass

from bulkman import BulkheadConfig, BulkheadThreading
from resilient_circuit.storage import create_storage

KEY = "mp_clobber_probe"
NS = "audit_mp"


def _clean() -> None:
    if not os.getenv("RC_DB_HOST"):
        return
    import psycopg

    conn_params = {
        "host": os.getenv("RC_DB_HOST"),
        "port": os.getenv("RC_DB_PORT"),
        "dbname": os.getenv("RC_DB_NAME"),
        "user": os.getenv("RC_DB_USER"),
        "password": os.getenv("RC_DB_PASSWORD"),
    }
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rc_circuit_breakers WHERE namespace = %s", (NS,))
        conn.commit()


CFG = BulkheadConfig(
    name=KEY,
    failure_threshold=5,
    isolation_duration=60.0,
    circuit_breaker_enabled=True,
)


def _worker_a(opened_event, done) -> None:
    bulkhead = BulkheadThreading(CFG, circuit_storage=create_storage(namespace=NS))
    for _ in range(5):
        bulkhead.execute(lambda: 1 / 0).result(timeout=5)  # 5 consecutive failures
    opened_event.set()
    done.wait(15)
    bulkhead.shutdown(wait=False)


def _worker_b(opened_event, done) -> None:
    bulkhead = BulkheadThreading(CFG, circuit_storage=create_storage(namespace=NS))
    opened_event.wait(15)  # constructed while the row was absent -> CLOSED locally
    for _ in range(3):
        bulkhead.execute(lambda: 1 / 0).result(timeout=5)  # stays below threshold
    for _ in range(2):
        bulkhead.execute(lambda: 42).result(timeout=5)  # successes -> stale CLOSED writes
    done.set()
    bulkhead.shutdown(wait=False)


def main() -> int:
    if not os.getenv("RC_DB_HOST"):
        print("SKIP: PostgreSQL not configured (missing RC_DB_* environment variables)")
        return 0

    _clean()
    ctx = mp.get_context("fork")
    opened, done = ctx.Event(), ctx.Event()
    processes = [
        ctx.Process(target=_worker_a, args=(opened, done)),
        ctx.Process(target=_worker_b, args=(opened, done)),
    ]
    for p in processes:
        p.start()
    for p in processes:
        p.join(60)

    storage = create_storage(namespace=NS)
    row = storage.get_state(KEY)
    if row is None or row["state"] != "OPEN":
        print(f"FAIL: shared row lost the protection signal: {row}")
        return 1

    instance_c = BulkheadThreading(CFG, circuit_storage=storage)
    try:
        instance_c.execute(lambda: 1).result(timeout=5)
        print("FAIL: restarted instance admitted calls (protection lost)")
        instance_c.shutdown(wait=False)
        return 1
    except Exception:
        pass
    instance_c.shutdown(wait=False)

    print("PASS: shared OPEN survived concurrent stale-CLOSED writes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
