"""Tests for PostgreSQL integration with circuit breaker."""

import os

from bulkman import Bulkhead, BulkheadConfig


class TestPostgresIntegration:
    """Test PostgreSQL circuit breaker persistence."""

    async def test_circuit_breaker_postgres_storage(self, postgres_storage):
        """Test circuit breaker state is persisted to PostgreSQL."""
        config = BulkheadConfig(
            name="postgres_test_bulkhead",
            failure_threshold=2,
            circuit_breaker_enabled=True,
        )

        # Create bulkhead with PostgreSQL storage
        bulkhead = Bulkhead(config, circuit_storage=postgres_storage)

        # Execute successful operation
        result = await bulkhead.execute(lambda: 42)
        assert result.success is True

        # Verify circuit breaker stats
        stats = await bulkhead.get_stats()
        assert stats["circuit_breaker_enabled"] is True
        assert stats["circuit_status"] == "CLOSED"

        # Verify state in database
        db_state = postgres_storage.get_state(config.name)
        assert db_state is not None
        assert db_state["state"] == "CLOSED"

    async def test_circuit_state_persistence_across_instances(self, postgres_storage):
        """Test circuit state persists across different bulkhead instances."""
        config = BulkheadConfig(
            name="persistence_test",
            failure_threshold=3,
            circuit_breaker_enabled=True,
        )

        # First instance - execute successful operation
        bulkhead1 = Bulkhead(config, circuit_storage=postgres_storage)

        # Execute successful operation first
        result1 = await bulkhead1.execute(lambda: "success")
        assert result1.success is True

        # Check database state - should be CLOSED
        db_state = postgres_storage.get_state(config.name)
        assert db_state is not None
        assert db_state["state"] == "CLOSED"

        # Create new instance with same name - should load state from DB
        bulkhead2 = Bulkhead(config, circuit_storage=postgres_storage)

        # New instance should have the same circuit state from DB (CLOSED)
        state = await bulkhead2.get_state()
        from bulkman import BulkheadState

        assert state == BulkheadState.HEALTHY

        # Execute another operation through the new instance
        result2 = await bulkhead2.execute(lambda: "success2")
        assert result2.success is True

    async def test_multiple_bulkheads_isolated_storage(self, postgres_storage):
        """Test multiple bulkheads have isolated storage."""
        config1 = BulkheadConfig(name="bulkhead_1", circuit_breaker_enabled=True)
        config2 = BulkheadConfig(name="bulkhead_2", circuit_breaker_enabled=True)

        bulkhead1 = Bulkhead(config1, circuit_storage=postgres_storage)
        bulkhead2 = Bulkhead(config2, circuit_storage=postgres_storage)

        # Execute in bulkhead1
        result1 = await bulkhead1.execute(lambda: "result1")
        assert result1.success is True

        # Execute in bulkhead2
        result2 = await bulkhead2.execute(lambda: "result2")
        assert result2.success is True

        # Verify both have separate states in DB
        state1 = postgres_storage.get_state("bulkhead_1")
        state2 = postgres_storage.get_state("bulkhead_2")

        assert state1 is not None
        assert state2 is not None
        assert state1["state"] == "CLOSED"
        assert state2["state"] == "CLOSED"

    async def test_postgres_connection_details(self):
        """Verify PostgreSQL connection is using correct database."""

        assert os.getenv("RC_DB_HOST") == "localhost"
        assert os.getenv("RC_DB_PORT") == "5432"
        assert os.getenv("RC_DB_NAME") == "bulkman_test_resilient_circuit_db"
        assert os.getenv("RC_DB_USER") == "postgres"
        assert os.getenv("RC_DB_PASSWORD") == "postgres"
