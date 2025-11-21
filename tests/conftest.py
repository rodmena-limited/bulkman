"""Pytest configuration for bulkman tests."""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from resilient_circuit.storage import create_storage

# Load test environment variables
test_env = Path(__file__).parent.parent / "test.env"
if test_env.exists():
    load_dotenv(test_env)


@pytest.fixture
def anyio_backend():
    """Use trio as the async backend for pytest-trio."""
    return "trio"


@pytest.fixture
def postgres_storage():
    """Create PostgreSQL storage for circuit breaker tests."""
    # Check if PostgreSQL env vars are set
    if not os.getenv("RC_DB_HOST"):
        pytest.skip("PostgreSQL not configured (missing RC_DB_* environment variables)")

    # Create storage with namespace
    storage = create_storage(namespace="bulkman_test")

    # Clean up any existing state from previous test runs
    import psycopg
    try:
        conn_params = {
            "host": os.getenv("RC_DB_HOST"),
            "port": os.getenv("RC_DB_PORT"),
            "dbname": os.getenv("RC_DB_NAME"),
            "user": os.getenv("RC_DB_USER"),
            "password": os.getenv("RC_DB_PASSWORD"),
        }
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Delete all circuit breaker states in our namespace
                cur.execute(
                    "DELETE FROM rc_circuit_breakers WHERE namespace = %s",
                    ("bulkman_test",)
                )
            conn.commit()
    except Exception:
        # Table might not exist yet, that's okay
        pass

    return storage
