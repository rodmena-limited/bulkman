"""Tests for synchronous bridge functionality."""

from __future__ import annotations

import concurrent.futures
import time

import pytest

from bulkman import BulkheadConfig
from bulkman.exceptions import BulkheadError
from bulkman.sync_bridge import BulkheadSync, TrioThread, _get_trio_thread


class TestTrioThread:
    """Test TrioThread functionality."""

    def test_trio_thread_start_stop(self):
        """Test starting and stopping a Trio thread."""
        thread = TrioThread()
        thread.start()

        assert thread._portal is not None
        assert thread._thread is not None
        assert thread._thread.is_alive()

        thread.stop()
        assert thread._thread is None
        assert thread._portal is None

    def test_trio_thread_start_idempotent(self):
        """Test that calling start multiple times is safe."""
        thread = TrioThread()
        thread.start()
        portal1 = thread._portal

        # Call start again
        thread.start()
        portal2 = thread._portal

        # Should be same instance
        assert portal1 == portal2

        thread.stop()

    def test_trio_thread_stop_idempotent(self):
        """Test that calling stop multiple times is safe."""
        thread = TrioThread()
        thread.start()
        thread.stop()

        # Call stop again - should not raise
        thread.stop()

    def test_trio_thread_run_sync(self):
        """Test running async function from sync context."""
        thread = TrioThread()
        thread.start()

        async def async_add(a: int, b: int) -> int:
            return a + b

        result = thread.run_sync(async_add, 2, 3)
        assert result == 5

        thread.stop()

    def test_trio_thread_run_sync_not_started(self):
        """Test run_sync raises error if thread not started."""
        thread = TrioThread()

        async def dummy():
            return 42

        with pytest.raises(RuntimeError, match="not started"):
            thread.run_sync(dummy)

    def test_get_trio_thread_singleton(self):
        """Test global Trio thread is a singleton."""
        thread1 = _get_trio_thread()
        thread2 = _get_trio_thread()

        assert thread1 is thread2


class TestBulkheadSync:
    """Test synchronous bulkhead wrapper."""

    def test_bulkhead_sync_creation(self):
        """Test BulkheadSync can be created."""
        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        assert bulkhead.name == "test_sync"
        assert bulkhead._bulkhead is not None

    def test_bulkhead_sync_execute_sync_function(self):
        """Test executing a synchronous function."""
        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        def add(x: int, y: int) -> int:
            return x + y

        future = bulkhead.execute(add, 10, 20)
        result = future.result(timeout=5.0)

        assert result.success is True
        assert result.result == 30
        assert result.bulkhead_name == "test_sync"

    def test_bulkhead_sync_execute_async_function(self):
        """Test executing an async function."""
        import trio

        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        async def async_multiply(x: int, y: int) -> int:
            await trio.sleep(0.01)
            return x * y

        future = bulkhead.execute(async_multiply, 5, 6)
        result = future.result(timeout=5.0)

        assert result.success is True
        assert result.result == 30

    def test_bulkhead_sync_execute_failing_function(self):
        """Test executing a function that raises an exception."""
        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        def failing_func():
            raise ValueError("Test error")

        future = bulkhead.execute(failing_func)
        result = future.result(timeout=5.0)

        assert result.success is False
        assert result.error is not None
        assert "Test error" in str(result.error)

    def test_bulkhead_sync_multiple_executions(self):
        """Test multiple concurrent executions."""
        config = BulkheadConfig(
            name="test_sync",
            max_concurrent_calls=3,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadSync(config)

        def slow_func(n: int) -> int:
            time.sleep(0.1)
            return n * 2

        # Submit multiple tasks
        futures = [bulkhead.execute(slow_func, i) for i in range(5)]

        # Wait for all to complete
        results = [f.result(timeout=5.0) for f in futures]

        assert len(results) == 5
        assert all(r.success for r in results)
        assert [r.result for r in results] == [0, 2, 4, 6, 8]

    def test_bulkhead_sync_get_stats(self):
        """Test getting statistics from sync bulkhead."""
        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        # Execute some operations
        future1 = bulkhead.execute(lambda: 42)
        _ = future1.result(timeout=5.0)

        future2 = bulkhead.execute(lambda: 1 / 0)
        _ = future2.result(timeout=5.0)

        stats = bulkhead.get_stats()
        assert stats["name"] == "test_sync"
        assert stats["total_executions"] == 2
        assert stats["successful_executions"] == 1
        assert stats["failed_executions"] == 1

    def test_bulkhead_sync_with_timeout(self):
        """Test bulkhead respects timeout."""
        import trio

        config = BulkheadConfig(
            name="test_sync",
            timeout_seconds=0.1,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadSync(config)

        async def slow_func():
            await trio.sleep(1.0)
            return "done"

        future = bulkhead.execute(slow_func)

        # Should timeout
        with pytest.raises(BulkheadError):  # Will raise timeout or bulkhead error
            _ = future.result(timeout=5.0)

    def test_bulkhead_sync_concurrent_limit(self):
        """Test bulkhead limits concurrent executions."""
        import trio

        config = BulkheadConfig(
            name="test_sync",
            max_concurrent_calls=2,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadSync(config)

        execution_times: list[float] = []

        async def timed_func(n: int) -> int:
            start = time.time()
            await trio.sleep(0.1)
            duration = time.time() - start
            execution_times.append(duration)
            return n

        # Submit 4 tasks with concurrency limit of 2
        futures = [bulkhead.execute(timed_func, i) for i in range(4)]
        results = [f.result(timeout=5.0) for f in futures]

        assert len(results) == 4
        assert all(r.success for r in results)

    def test_bulkhead_sync_future_api(self):
        """Test that returned Future follows concurrent.futures API."""
        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        def simple_func():
            return "test_result"

        future = bulkhead.execute(simple_func)

        # Test Future API methods
        assert isinstance(future, concurrent.futures.Future)
        assert not future.done()  # Initially not done

        result = future.result(timeout=5.0)
        assert future.done()
        assert result.success is True
        assert result.result == "test_result"

    def test_bulkhead_sync_exception_handling(self):
        """Test proper exception handling in sync bridge."""
        config = BulkheadConfig(name="test_sync", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        class CustomError(Exception):
            pass

        def raising_func():
            raise CustomError("Custom exception")

        future = bulkhead.execute(raising_func)
        result = future.result(timeout=5.0)

        assert result.success is False
        assert isinstance(result.error, BulkheadError)
        assert "Custom exception" in str(result.error)


class TestSyncBridgeIntegration:
    """Integration tests for sync bridge."""

    def test_sync_bridge_with_threading(self):
        """Test sync bridge works correctly with threading."""
        import threading

        config = BulkheadConfig(name="thread_test", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        results: list[int] = []

        def double(x: int) -> int:
            return x * 2

        def worker(n: int) -> None:
            future = bulkhead.execute(double, n)
            result = future.result(timeout=5.0)
            assert isinstance(result.result, int)
            results.append(result.result)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(results) == 5
        assert sorted(results) == [0, 2, 4, 6, 8]

    def test_multiple_sync_bulkheads(self):
        """Test multiple sync bulkheads work independently."""
        config1 = BulkheadConfig(name="bulkhead1", circuit_breaker_enabled=False)
        config2 = BulkheadConfig(name="bulkhead2", circuit_breaker_enabled=False)

        bulkhead1 = BulkheadSync(config1)
        bulkhead2 = BulkheadSync(config2)

        future1 = bulkhead1.execute(lambda: "result1")
        future2 = bulkhead2.execute(lambda: "result2")

        result1 = future1.result(timeout=5.0)
        result2 = future2.result(timeout=5.0)

        assert result1.bulkhead_name == "bulkhead1"
        assert result2.bulkhead_name == "bulkhead2"
        assert result1.result == "result1"
        assert result2.result == "result2"

    def test_sync_bridge_stats_accuracy(self):
        """Test that stats are accurately tracked."""
        config = BulkheadConfig(name="stats_test", circuit_breaker_enabled=False)
        bulkhead = BulkheadSync(config)

        def identity(x: int) -> int:
            return x

        # Execute successful operations
        for i in range(3):
            future = bulkhead.execute(identity, i)
            _ = future.result(timeout=5.0)

        # Execute failing operations
        for _idx in range(2):
            future = bulkhead.execute(lambda: 1 / 0)
            _ = future.result(timeout=5.0)

        stats = bulkhead.get_stats()
        assert stats["total_executions"] == 5
        assert stats["successful_executions"] == 3
        assert stats["failed_executions"] == 2
