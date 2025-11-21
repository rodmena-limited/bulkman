"""Tests for decorator functionality."""

import pytest
import trio

from bulkman import Bulkhead, BulkheadConfig, BulkheadError
from bulkman.core import with_bulkhead


class TestWithBulkheadDecorator:
    """Test the with_bulkhead decorator."""

    async def test_decorator_basic_usage(self):
        """Test basic decorator usage."""
        config = BulkheadConfig(name="test", circuit_breaker_enabled=False)
        bulkhead = Bulkhead(config)

        @with_bulkhead(bulkhead)
        async def add(x: int, y: int) -> int:
            return x + y

        result = await add(2, 3)
        assert result == 5

    async def test_decorator_with_async_function(self):
        """Test decorator with async function."""
        config = BulkheadConfig(name="test", circuit_breaker_enabled=False)
        bulkhead = Bulkhead(config)

        @with_bulkhead(bulkhead)
        async def slow_multiply(x: int, y: int) -> int:
            await trio.sleep(0.01)
            return x * y

        result = await slow_multiply(4, 5)
        assert result == 20

    async def test_decorator_propagates_exceptions(self):
        """Test decorator propagates exceptions."""
        config = BulkheadConfig(name="test", circuit_breaker_enabled=False)
        bulkhead = Bulkhead(config)

        @with_bulkhead(bulkhead)
        async def failing_func():
            raise ValueError("Test error")

        with pytest.raises(BulkheadError):
            await failing_func()

    async def test_decorator_updates_stats(self):
        """Test decorator updates bulkhead stats."""
        config = BulkheadConfig(name="test", circuit_breaker_enabled=False)
        bulkhead = Bulkhead(config)

        @with_bulkhead(bulkhead)
        async def simple_func():
            return 42

        await simple_func()
        await simple_func()

        stats = await bulkhead.get_stats()
        assert stats["total_executions"] == 2
        assert stats["successful_executions"] == 2

    async def test_decorator_respects_concurrency_limit(self):
        """Test decorator respects bulkhead concurrency limits."""
        config = BulkheadConfig(
            name="test",
            max_concurrent_calls=2,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)

        concurrent_count = 0
        max_concurrent = 0

        @with_bulkhead(bulkhead)
        async def slow_func():
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await trio.sleep(0.1)
            concurrent_count -= 1
            return "done"

        async with trio.open_nursery() as nursery:
            for _ in range(5):
                nursery.start_soon(slow_func)

        assert max_concurrent <= 2
