"""Pytest configuration for bulkman tests."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import psycopg
import pytest
from resilient_circuit.storage import create_storage
from testcontainers.postgres import PostgresContainer  # pyright: ignore[reportMissingTypeStubs]


@pytest.fixture
def anyio_backend() -> str:
    """Use trio as the async backend for pytest-trio."""
    return "trio"


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a PostgreSQL container for the test session."""
    with PostgresContainer(
        image="postgres:16-alpine",
        username="postgres",
        password="postgres",
        dbname="bulkman_test_resilient_circuit_db",
        driver=None,
    ) as container:
        yield container


@pytest.fixture
def postgres_connection_params(postgres_container: PostgresContainer) -> dict[str, Any]:
    """Get connection parameters for the PostgreSQL container."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    return {
        "host": host,
        "port": str(port),
        "dbname": "bulkman_test_resilient_circuit_db",
        "user": "postgres",
        "password": "postgres",
    }


@pytest.fixture
def postgres_storage(
    postgres_connection_params: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    """Create PostgreSQL storage for circuit breaker tests.

    Sets RC_DB_* environment variables so resilient_circuit can connect,
    then creates storage and cleans up previous test state.
    """
    host = str(postgres_connection_params["host"])
    port = str(postgres_connection_params["port"])
    dbname = str(postgres_connection_params["dbname"])
    user = str(postgres_connection_params["user"])
    password = str(postgres_connection_params["password"])

    monkeypatch.setenv("RC_DB_HOST", host)
    monkeypatch.setenv("RC_DB_PORT", port)
    monkeypatch.setenv("RC_DB_NAME", dbname)
    monkeypatch.setenv("RC_DB_USER", user)
    monkeypatch.setenv("RC_DB_PASSWORD", password)

    # Create storage with namespace
    storage = create_storage(namespace="bulkman_test")

    # Clean up any existing state from previous test runs
    conn_params: dict[str, Any] = {
        "host": host,
        "port": int(port),
        "dbname": dbname,
        "user": user,
        "password": password,
    }
    try:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                _ = cur.execute(
                    "DELETE FROM rc_circuit_breakers WHERE namespace = %s",
                    ("bulkman_test",),
                )
            conn.commit()
    except Exception:
        # Table might not exist yet, that's okay
        pass

    return storage
