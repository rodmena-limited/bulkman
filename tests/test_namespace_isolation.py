"""Tests for namespace isolation in circuit breakers."""

import os

from bulkman import Bulkhead, BulkheadConfig, BulkheadManager


class TestNamespaceIsolation:
    """Test that namespaces properly isolate circuit breaker state."""

    async def test_same_name_same_namespace_shares_state(self, postgres_storage):
        """Bulkheads with same name and namespace should share circuit state."""
        config = BulkheadConfig(
            name="shared_service",
            circuit_breaker_enabled=True,
        )

        # Create two bulkhead instances with same config
        bulkhead1 = Bulkhead(config, circuit_storage=postgres_storage)
        bulkhead2 = Bulkhead(config, circuit_storage=postgres_storage)

        # They should have the same resource_key
        assert bulkhead1._circuit_breaker.resource_key == "shared_service"
        assert bulkhead2._circuit_breaker.resource_key == "shared_service"

        # Execute on first instance
        result1 = await bulkhead1.execute(lambda: "from_bulkhead1")
        assert result1.success is True

        # Both should share the same state in database
        state1 = postgres_storage.get_state("shared_service")
        assert state1 is not None
        assert state1["state"] == "CLOSED"

    async def test_different_names_same_namespace_isolated(self, postgres_storage):
        """Bulkheads with different names should be isolated even in same namespace."""
        config1 = BulkheadConfig(
            name="service_a",
            circuit_breaker_enabled=True,
        )
        config2 = BulkheadConfig(
            name="service_b",
            circuit_breaker_enabled=True,
        )

        bulkhead1 = Bulkhead(config1, circuit_storage=postgres_storage)
        bulkhead2 = Bulkhead(config2, circuit_storage=postgres_storage)

        # Different resource keys
        assert bulkhead1._circuit_breaker.resource_key == "service_a"
        assert bulkhead2._circuit_breaker.resource_key == "service_b"

        # Execute on both
        await bulkhead1.execute(lambda: "a")
        await bulkhead2.execute(lambda: "b")

        # Both should have separate state
        state1 = postgres_storage.get_state("service_a")
        state2 = postgres_storage.get_state("service_b")
        assert state1 is not None
        assert state2 is not None

    async def test_same_name_different_namespace_isolated(self, postgres_storage):
        """Bulkheads with same name but different namespace should be isolated."""
        from resilient_circuit.storage import create_storage

        # Create storages with different namespaces
        storage_prod = create_storage(namespace="production")
        storage_staging = create_storage(namespace="staging")

        config = BulkheadConfig(
            name="api_service",
            circuit_breaker_enabled=True,
        )

        bulkhead_prod = Bulkhead(config, circuit_storage=storage_prod)
        bulkhead_staging = Bulkhead(config, circuit_storage=storage_staging)

        # Same resource key
        assert bulkhead_prod._circuit_breaker.resource_key == "api_service"
        assert bulkhead_staging._circuit_breaker.resource_key == "api_service"

        # Execute on both
        await bulkhead_prod.execute(lambda: "prod")
        await bulkhead_staging.execute(lambda: "staging")

        # Verify isolation in database
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
                cur.execute(
                    """
                    SELECT namespace, state
                    FROM rc_circuit_breakers
                    WHERE resource_key = %s
                    ORDER BY namespace
                    """,
                    ("api_service",),
                )
                rows = cur.fetchall()

                # Should have two separate records
                namespaces = [row[0] for row in rows]
                assert "production" in namespaces
                assert "staging" in namespaces

    async def test_manager_with_storage(self, postgres_storage):
        """Test BulkheadManager uses storage namespace."""
        manager = BulkheadManager(circuit_storage=postgres_storage)

        # Create bulkhead
        config = BulkheadConfig(
            name="managed_service",
            circuit_breaker_enabled=True,
        )

        bulkhead = await manager.create_bulkhead(config)
        await bulkhead.execute(lambda: "test")

        # Verify uses storage namespace
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
                cur.execute(
                    """
                    SELECT namespace
                    FROM rc_circuit_breakers
                    WHERE resource_key = %s
                    """,
                    ("managed_service",),
                )
                row = cur.fetchone()
                assert row is not None
                assert row[0] == "bulkman_test"  # From postgres_storage fixture
