"""Tests for namespace isolation in circuit breakers."""

from __future__ import annotations

from typing import Any

import psycopg
from resilient_circuit.storage import create_storage

from bulkman import Bulkhead, BulkheadConfig, BulkheadManager


class TestNamespaceIsolation:
    """Test that namespaces properly isolate circuit breaker state."""

    async def test_same_name_same_namespace_shares_state(self, postgres_storage: Any) -> None:
        """Bulkheads with same name and namespace should share circuit state."""
        config = BulkheadConfig(
            name="shared_service",
            circuit_breaker_enabled=True,
        )

        bulkhead1 = Bulkhead(config, circuit_storage=postgres_storage)
        bulkhead2 = Bulkhead(config, circuit_storage=postgres_storage)

        assert bulkhead1._circuit_breaker is not None
        assert bulkhead2._circuit_breaker is not None
        assert bulkhead1._circuit_breaker.resource_key == "shared_service"
        assert bulkhead2._circuit_breaker.resource_key == "shared_service"

        result1 = await bulkhead1.execute(lambda: "from_bulkhead1")
        assert result1.success is True

        state1 = postgres_storage.get_state("shared_service")
        assert state1 is not None
        assert state1["state"] == "CLOSED"

    async def test_different_names_same_namespace_isolated(self, postgres_storage: Any) -> None:
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

        assert bulkhead1._circuit_breaker is not None
        assert bulkhead2._circuit_breaker is not None
        assert bulkhead1._circuit_breaker.resource_key == "service_a"
        assert bulkhead2._circuit_breaker.resource_key == "service_b"

        _ = await bulkhead1.execute(lambda: "a")
        _ = await bulkhead2.execute(lambda: "b")

        state1 = postgres_storage.get_state("service_a")
        state2 = postgres_storage.get_state("service_b")
        assert state1 is not None
        assert state2 is not None

    async def test_same_name_different_namespace_isolated(
        self, postgres_storage: Any, postgres_connection_params: dict[str, Any]
    ) -> None:
        """Bulkheads with same name but different namespace should be isolated."""
        storage_prod = create_storage(namespace="production")
        storage_staging = create_storage(namespace="staging")

        config = BulkheadConfig(
            name="api_service",
            circuit_breaker_enabled=True,
        )

        bulkhead_prod = Bulkhead(config, circuit_storage=storage_prod)
        bulkhead_staging = Bulkhead(config, circuit_storage=storage_staging)

        assert bulkhead_prod._circuit_breaker is not None
        assert bulkhead_staging._circuit_breaker is not None
        assert bulkhead_prod._circuit_breaker.resource_key == "api_service"
        assert bulkhead_staging._circuit_breaker.resource_key == "api_service"

        _ = await bulkhead_prod.execute(lambda: "prod")
        _ = await bulkhead_staging.execute(lambda: "staging")

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
                    SELECT namespace, state
                    FROM rc_circuit_breakers
                    WHERE resource_key = %s
                    ORDER BY namespace
                    """,
                    ("api_service",),
                )
                rows = cur.fetchall()

                namespaces = [row[0] for row in rows]
                assert "production" in namespaces
                assert "staging" in namespaces

    async def test_manager_with_storage(
        self, postgres_storage: Any, postgres_connection_params: dict[str, Any]
    ) -> None:
        """Test BulkheadManager uses storage namespace."""
        manager = BulkheadManager(circuit_storage=postgres_storage)

        config = BulkheadConfig(
            name="managed_service",
            circuit_breaker_enabled=True,
        )

        bulkhead = await manager.create_bulkhead(config)
        _ = await bulkhead.execute(lambda: "test")

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
                    SELECT namespace
                    FROM rc_circuit_breakers
                    WHERE resource_key = %s
                    """,
                    ("managed_service",),
                )
                row = cur.fetchone()
                assert row is not None
                assert row[0] == "bulkman_test"
