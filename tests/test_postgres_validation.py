"""Validation tests that verify PostgreSQL integration."""

from __future__ import annotations

from typing import Any

import psycopg
import trio

from bulkman import Bulkhead, BulkheadConfig


class TestPostgresValidation:
    """Validate PostgreSQL persistence by querying the database directly."""

    async def test_validate_circuit_state_in_database(
        self, postgres_storage: Any, postgres_connection_params: dict[str, Any]
    ) -> None:
        """Validate that circuit breaker state is actually stored in PostgreSQL."""
        config = BulkheadConfig(
            name="validation_test",
            circuit_breaker_enabled=True,
        )

        bulkhead = Bulkhead(config, circuit_storage=postgres_storage)

        result = await bulkhead.execute(lambda: "test_result")
        assert result.success is True

        await trio.sleep(0.1)

        host = str(postgres_connection_params["host"])
        port = int(postgres_connection_params["port"])
        dbname = str(postgres_connection_params["dbname"])
        user = str(postgres_connection_params["user"])
        password = str(postgres_connection_params["password"])

        with psycopg.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        ) as conn:
            with conn.cursor() as cur:
                _ = cur.execute(
                    """
                    SELECT resource_key, namespace, state, failure_count
                    FROM rc_circuit_breakers
                    WHERE resource_key = %s AND namespace = %s
                    """,
                    (config.name, "bulkman_test"),
                )
                row = cur.fetchone()

                assert row is not None, "Circuit breaker state not found in database"
                assert row[0] == "validation_test"
                assert row[1] == "bulkman_test"
                assert row[2] == "CLOSED"
                assert row[3] == 0

    async def test_circuit_state_survives_restart(
        self, postgres_storage: Any, postgres_connection_params: dict[str, Any]
    ) -> None:
        """Test that circuit state persists across application restarts."""
        config = BulkheadConfig(
            name="restart_test",
            failure_threshold=2,
            circuit_breaker_enabled=True,
        )

        bulkhead1 = Bulkhead(config, circuit_storage=postgres_storage)
        result1 = await bulkhead1.execute(lambda: 100)
        assert result1.success is True

        _bulkhead2 = Bulkhead(config, circuit_storage=postgres_storage)

        host = str(postgres_connection_params["host"])
        port = int(postgres_connection_params["port"])
        dbname = str(postgres_connection_params["dbname"])
        user = str(postgres_connection_params["user"])
        password = str(postgres_connection_params["password"])

        with psycopg.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        ) as conn:
            with conn.cursor() as cur:
                _ = cur.execute(
                    """
                    SELECT state, failure_count
                    FROM rc_circuit_breakers
                    WHERE resource_key = %s AND namespace = %s
                    """,
                    (config.name, "bulkman_test"),
                )
                row = cur.fetchone()

                assert row is not None
                assert row[0] == "CLOSED"
                assert row[1] == 0

        result2 = await _bulkhead2.execute(lambda: 200)
        assert result2.success is True
