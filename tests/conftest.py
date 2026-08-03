"""Pytest configuration for bulkman tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv
from resilient_circuit.storage import create_storage

# Load test environment variables
test_env = Path(__file__).parent.parent / "test.env"
if test_env.exists():
    load_dotenv(test_env)


@pytest.fixture
def anyio_backend() -> str:
    """Use asyncio as the async backend (pytest-anyio)."""
    return "asyncio"


@pytest.fixture
def postgres_connection_params() -> dict[str, Any]:
    """Connection parameters for the test PostgreSQL (from test.env / CI service)."""
    if not os.getenv("RC_DB_HOST"):
        pytest.skip("PostgreSQL not configured (missing RC_DB_* environment variables)")
    return {
        "host": os.getenv("RC_DB_HOST"),
        "port": os.getenv("RC_DB_PORT", "5432"),
        "dbname": os.getenv("RC_DB_NAME"),
        "user": os.getenv("RC_DB_USER"),
        "password": os.getenv("RC_DB_PASSWORD"),
    }


@pytest.fixture
def postgres_storage(postgres_connection_params: dict[str, Any]) -> Any:
    """Create PostgreSQL storage for circuit breaker tests."""
    # Create storage with namespace
    storage = create_storage(namespace="bulkman_test")

    # Clean up any existing state from previous test runs
    import psycopg

    try:
        with psycopg.connect(**postgres_connection_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM rc_circuit_breakers WHERE namespace = %s",
                    ("bulkman_test",),
                )
            conn.commit()
    except Exception:
        # Table might not exist yet, that's okay
        pass

    return storage


@pytest.fixture(autouse=True)
def _clean_default_circuit_state() -> Any:
    """Isolate circuit-breaker tests from each other.

    Bulkheads built without an explicit storage use resilient_circuit's
    default storage, which (with the test env loaded) is PostgreSQL-backed
    and SHARED across bulkheads and across runs in the "default" namespace.
    Without cleanup, a failed run can leave a circuit OPEN and poison the
    next run of a same-named test.
    """
    if not os.getenv("RC_DB_HOST"):
        yield
        return
    try:
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
                cur.execute("DELETE FROM rc_circuit_breakers WHERE namespace = 'default'")
            conn.commit()
    except Exception:
        pass
    yield
