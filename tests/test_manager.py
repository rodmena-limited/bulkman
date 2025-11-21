"""Tests for the BulkheadManager class."""

import pytest
import trio

from bulkman import BulkheadConfig, BulkheadManager


class TestBulkheadManager:
    """Test BulkheadManager functionality."""

    async def test_manager_creation(self):
        """Test manager can be created."""
        manager = BulkheadManager()
        assert manager is not None

    async def test_create_bulkhead(self):
        """Test creating a bulkhead through manager."""
        manager = BulkheadManager()
        config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)

        bulkhead = await manager.create_bulkhead(config)
        assert bulkhead.name == "test1"

    async def test_create_duplicate_bulkhead_raises_error(self):
        """Test creating duplicate bulkhead raises error."""
        manager = BulkheadManager()
        config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)

        await manager.create_bulkhead(config)

        with pytest.raises(ValueError, match="already exists"):
            await manager.create_bulkhead(config)

    async def test_get_bulkhead(self):
        """Test getting an existing bulkhead."""
        manager = BulkheadManager()
        config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)

        created = await manager.create_bulkhead(config)
        retrieved = await manager.get_bulkhead("test1")

        assert retrieved is created

    async def test_get_nonexistent_bulkhead(self):
        """Test getting a non-existent bulkhead returns None."""
        manager = BulkheadManager()
        retrieved = await manager.get_bulkhead("nonexistent")
        assert retrieved is None

    async def test_get_or_create_bulkhead_existing(self):
        """Test get_or_create returns existing bulkhead."""
        manager = BulkheadManager()
        config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)

        created = await manager.create_bulkhead(config)
        retrieved = await manager.get_or_create_bulkhead(config)

        assert retrieved is created

    async def test_get_or_create_bulkhead_new(self):
        """Test get_or_create creates new bulkhead if not exists."""
        manager = BulkheadManager()
        config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)

        bulkhead = await manager.get_or_create_bulkhead(config)
        assert bulkhead.name == "test1"

    async def test_execute_in_bulkhead(self):
        """Test executing function through named bulkhead."""
        manager = BulkheadManager()
        config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)
        await manager.create_bulkhead(config)

        result = await manager.execute_in_bulkhead("test1", lambda: 42)
        assert result.success is True
        assert result.result == 42

    async def test_execute_in_nonexistent_bulkhead(self):
        """Test executing in non-existent bulkhead raises error."""
        manager = BulkheadManager()

        with pytest.raises(ValueError, match="not found"):
            await manager.execute_in_bulkhead("nonexistent", lambda: 42)

    async def test_get_all_stats(self):
        """Test getting stats for all bulkheads."""
        manager = BulkheadManager()

        config1 = BulkheadConfig(name="bulkhead1", circuit_breaker_enabled=False)
        config2 = BulkheadConfig(name="bulkhead2", circuit_breaker_enabled=False)

        await manager.create_bulkhead(config1)
        await manager.create_bulkhead(config2)

        # Execute some operations
        await manager.execute_in_bulkhead("bulkhead1", lambda: 1)
        await manager.execute_in_bulkhead("bulkhead2", lambda: 2)

        stats = await manager.get_all_stats()
        assert "bulkhead1" in stats
        assert "bulkhead2" in stats
        assert stats["bulkhead1"]["total_executions"] == 1
        assert stats["bulkhead2"]["total_executions"] == 1

    async def test_get_health_status(self):
        """Test getting health status for all bulkheads."""
        manager = BulkheadManager()

        config1 = BulkheadConfig(name="bulkhead1", circuit_breaker_enabled=False)
        config2 = BulkheadConfig(name="bulkhead2", circuit_breaker_enabled=False)

        await manager.create_bulkhead(config1)
        await manager.create_bulkhead(config2)

        health = await manager.get_health_status()
        assert health["bulkhead1"] is True
        assert health["bulkhead2"] is True

    async def test_manager_context_manager(self):
        """Test manager context manager."""
        manager = BulkheadManager()

        async with manager.context() as mgr:
            config = BulkheadConfig(name="test1", circuit_breaker_enabled=False)
            bulkhead = await mgr.create_bulkhead(config)
            result = await bulkhead.execute(lambda: 42)
            assert result.success is True

    async def test_multiple_bulkheads_isolation(self):
        """Test multiple bulkheads are isolated from each other."""
        manager = BulkheadManager()

        config1 = BulkheadConfig(
            name="fast",
            max_concurrent_calls=10,
            circuit_breaker_enabled=False,
        )
        config2 = BulkheadConfig(
            name="slow",
            max_concurrent_calls=1,
            circuit_breaker_enabled=False,
        )

        await manager.create_bulkhead(config1)
        await manager.create_bulkhead(config2)

        fast_completed = []
        slow_completed = []

        async def fast_task():
            result = await manager.execute_in_bulkhead("fast", lambda: "fast")
            fast_completed.append(result)

        async def slow_task():
            async def slow_func():
                await trio.sleep(0.1)
                return "slow"

            result = await manager.execute_in_bulkhead("slow", slow_func)
            slow_completed.append(result)

        async with trio.open_nursery() as nursery:
            # Start slow tasks
            for _ in range(3):
                nursery.start_soon(slow_task)
            # Start fast tasks
            for _ in range(10):
                nursery.start_soon(fast_task)

        # Fast tasks should complete much earlier than slow tasks
        assert len(fast_completed) == 10
        assert len(slow_completed) == 3
