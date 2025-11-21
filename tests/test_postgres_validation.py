"""Validation tests that verify PostgreSQL integration."""

import os

import trio

from bulkman import Bulkhead, BulkheadConfig


class TestPostgresValidation:
    """Validate PostgreSQL persistence by querying the database directly."""

    async def test_validate_circuit_state_in_database(self, postgres_storage):
        """Validate that circuit breaker state is actually stored in PostgreSQL."""
        import psycopg

        config = BulkheadConfig(
            name="validation_test",
            circuit_breaker_enabled=True,
        )

        bulkhead = Bulkhead(config, circuit_storage=postgres_storage)

        # Execute a successful operation
        result = await bulkhead.execute(lambda: "test_result")
        assert result.success is True

        # Give it a moment to persist
        await trio.sleep(0.1)

        # Directly query PostgreSQL to verify state
        conn_params = {
            "host": os.getenv("RC_DB_HOST"),
            "port": os.getenv("RC_DB_PORT"),
            "dbname": os.getenv("RC_DB_NAME"),
            "user": os.getenv("RC_DB_USER"),
            "password": os.getenv("RC_DB_PASSWORD"),
        }

        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
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
                assert row[3] == 0  # no failures

    async def test_circuit_state_survives_restart(self, postgres_storage):
        """Test that circuit state persists across application restarts."""
        import psycopg

        config = BulkheadConfig(
            name="restart_test",
            failure_threshold=2,
            circuit_breaker_enabled=True,
        )

        # First "application instance"
        bulkhead1 = Bulkhead(config, circuit_storage=postgres_storage)
        result1 = await bulkhead1.execute(lambda: 100)
        assert result1.success is True

        # Simulate application restart by creating new instance
        # The state should be loaded from PostgreSQL
        bulkhead2 = Bulkhead(config, circuit_storage=postgres_storage)

        # Verify the new instance loaded state from database
        conn_params = {
            "host": os.getenv("RC_DB_HOST"),
            "port": os.getenv("RC_DB_PORT"),
            "dbname": os.getenv("RC_DB_NAME"),
            "user": os.getenv("RC_DB_USER"),
            "password": os.getenv("RC_DB_PASSWORD"),
        }

        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
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

        # New instance should work with the persisted state
        result2 = await bulkhead2.execute(lambda: 200)
        assert result2.success is True
