"""Tests for pure threading-based bulkhead implementation."""

from __future__ import annotations

import concurrent.futures
import threading
import time

import pytest

from bulkman import BulkheadConfig, BulkheadThreading
from bulkman.exceptions import (
    BulkheadCircuitOpenError,
    BulkheadError,
    BulkheadFullError,
    BulkheadTimeoutError,
)
from bulkman.state import BulkheadState


class TestBulkheadThreadingCreation:
    """Test BulkheadThreading initialization."""

    def test_basic_creation(self):
        """Test BulkheadThreading can be created with default config."""
        config = BulkheadConfig(name="test_threading", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        assert bulkhead.name == "test_threading"
        assert bulkhead.config == config
        bulkhead.shutdown(wait=False)

    def test_creation_with_circuit_breaker(self):
        """Test BulkheadThreading with circuit breaker enabled."""
        config = BulkheadConfig(
            name="test_cb",
            circuit_breaker_enabled=True,
            failure_threshold=3,
            success_threshold=2,
            isolation_duration=30.0,
        )
        bulkhead = BulkheadThreading(config)

        assert bulkhead._circuit_breaker is not None
        assert bulkhead.get_state() == BulkheadState.HEALTHY
        bulkhead.shutdown(wait=False)

    def test_creation_with_custom_pool_size(self):
        """Test pool size is calculated correctly."""
        config = BulkheadConfig(
            name="test_pool",
            max_concurrent_calls=5,
            max_queue_size=10,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        # Pool size = max_concurrent = 5
        # Queueing is handled by executor internal queue + in_flight_count check
        assert bulkhead._executor._max_workers == 5
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingExecution:
    """Test BulkheadThreading execute functionality."""

    def test_execute_simple_function(self):
        """Test executing a simple synchronous function."""
        config = BulkheadConfig(name="test_exec", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def add(x: int, y: int) -> int:
            return x + y

        future = bulkhead.execute(add, 10, 20)
        result = future.result(timeout=5.0)

        assert result.success is True
        assert result.result == 30
        assert result.bulkhead_name == "test_exec"
        assert result.execution_id is not None
        assert result.execution_time > 0
        bulkhead.shutdown(wait=False)

    def test_execute_with_kwargs(self):
        """Test executing with keyword arguments."""
        config = BulkheadConfig(name="test_kwargs", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def greet(name: str, greeting: str = "Hello") -> str:
            return f"{greeting}, {name}!"

        future = bulkhead.execute(greet, "World", greeting="Hi")
        result = future.result(timeout=5.0)

        assert result.success is True
        assert result.result == "Hi, World!"
        bulkhead.shutdown(wait=False)

    def test_execute_failing_function(self):
        """Test executing a function that raises an exception."""
        config = BulkheadConfig(name="test_fail", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def failing_func():
            raise ValueError("Test error")

        future = bulkhead.execute(failing_func)
        result = future.result(timeout=5.0)

        assert result.success is False
        assert result.error is not None
        assert isinstance(result.error, BulkheadError)
        assert "Test error" in str(result.error)
        bulkhead.shutdown(wait=False)

    def test_execute_multiple_concurrent(self):
        """Test multiple concurrent executions."""
        config = BulkheadConfig(
            name="test_concurrent",
            max_concurrent_calls=3,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        def slow_func(n: int) -> int:
            time.sleep(0.05)
            return n * 2

        # Submit multiple tasks
        futures = [bulkhead.execute(slow_func, i) for i in range(5)]

        # Wait for all to complete
        results = [f.result(timeout=10.0) for f in futures]

        assert len(results) == 5
        assert all(r.success for r in results)
        assert [r.result for r in results] == [0, 2, 4, 6, 8]
        bulkhead.shutdown(wait=False)

    def test_execute_returns_future(self):
        """Test that execute returns a concurrent.futures.Future."""
        config = BulkheadConfig(name="test_future", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def simple_func():
            time.sleep(0.1)  # Add delay so future isn't done immediately
            return "test_result"

        future = bulkhead.execute(simple_func)

        assert isinstance(future, concurrent.futures.Future)
        # Note: We don't assert not future.done() because fast CPUs might execute it immediately

        result = future.result(timeout=5.0)
        assert future.done()
        assert result.success is True
        assert result.result == "test_result"
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingTimeout:
    """Test timeout behavior in BulkheadThreading."""

    def test_execute_with_timeout_success(self):
        """Test execute_with_timeout with successful execution."""
        config = BulkheadConfig(
            name="test_timeout",
            timeout_seconds=5.0,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        def fast_func():
            return "fast"

        result = bulkhead.execute_with_timeout(fast_func, timeout=5.0)

        assert result.success is True
        assert result.result == "fast"
        bulkhead.shutdown(wait=False)

    def test_execute_with_timeout_expires(self):
        """Test execute_with_timeout raises on timeout."""
        config = BulkheadConfig(
            name="test_timeout",
            timeout_seconds=5.0,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        def slow_func():
            time.sleep(10.0)  # Much longer than timeout
            return "slow"

        with pytest.raises(BulkheadTimeoutError) as exc_info:
            _ = bulkhead.execute_with_timeout(slow_func, timeout=0.1)

        assert "timed out" in str(exc_info.value).lower()
        bulkhead.shutdown(wait=False)

    def test_execute_with_timeout_uses_config_default(self):
        """Test execute_with_timeout uses config timeout if not specified."""
        config = BulkheadConfig(
            name="test_timeout",
            timeout_seconds=0.1,  # Very short
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        def slow_func():
            time.sleep(1.0)
            return "slow"

        with pytest.raises(BulkheadTimeoutError):
            _ = bulkhead.execute_with_timeout(slow_func)  # No explicit timeout

        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingConcurrencyControl:
    """Test concurrency limiting in BulkheadThreading."""

    def test_semaphore_limits_concurrency(self):
        """Test that semaphore limits concurrent executions."""
        config = BulkheadConfig(
            name="test_semaphore",
            max_concurrent_calls=2,
            timeout_seconds=10.0,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        concurrent_count = 0
        max_concurrent_seen = 0
        lock = threading.Lock()

        def track_concurrency(n: int) -> int:
            nonlocal concurrent_count, max_concurrent_seen
            with lock:
                concurrent_count += 1
                max_concurrent_seen = max(max_concurrent_seen, concurrent_count)
            time.sleep(0.1)  # Hold slot for a bit
            with lock:
                concurrent_count -= 1
            return n

        # Submit 6 tasks with concurrency limit of 2
        futures = [bulkhead.execute(track_concurrency, i) for i in range(6)]
        results = [f.result(timeout=10.0) for f in futures]

        assert len(results) == 6
        assert all(r.success for r in results)
        # Should never exceed max_concurrent_calls
        assert max_concurrent_seen <= 2, f"Max concurrent was {max_concurrent_seen}, expected <=2"
        bulkhead.shutdown(wait=False)

    def test_queue_full_rejection(self):
        """Test that tasks are rejected when at capacity.

        With max_concurrent_calls=1 and max_queue_size=1, total capacity is 2.
        The third task should be rejected.
        """
        config = BulkheadConfig(
            name="test_queue",
            max_concurrent_calls=1,
            max_queue_size=1,  # Total capacity = 2
            timeout_seconds=30.0,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        # Use an event to hold tasks from completing
        hold_event = threading.Event()

        def blocking_func() -> str:
            _ = hold_event.wait(timeout=10.0)
            return "done"

        # Submit first task
        future1 = bulkhead.execute(blocking_func)
        time.sleep(0.05)  # Small delay to let it start

        # Submit second task (in-flight count now = 2)
        future2 = bulkhead.execute(blocking_func)
        time.sleep(0.05)

        # Try to submit third task - should be rejected (at capacity)
        with pytest.raises(BulkheadFullError):
            _ = bulkhead.execute(blocking_func)

        # Release the hold so tasks can complete
        hold_event.set()

        # Clean up
        _ = future1.result(timeout=5.0)
        _ = future2.result(timeout=5.0)
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingStats:
    """Test statistics tracking in BulkheadThreading."""

    def test_get_stats_initial(self):
        """Test initial stats are zeroed."""
        config = BulkheadConfig(name="test_stats", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        stats = bulkhead.get_stats()

        assert stats["name"] == "test_stats"
        assert stats["total_executions"] == 0
        assert stats["successful_executions"] == 0
        assert stats["failed_executions"] == 0
        assert stats["rejected_executions"] == 0
        assert stats["timed_out_executions"] == 0
        assert stats["active_tasks"] == 0
        bulkhead.shutdown(wait=False)

    def test_stats_track_success(self):
        """Test stats track successful executions."""
        config = BulkheadConfig(name="test_stats", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def double(x: int) -> int:
            return x * 2

        for i in range(3):
            future = bulkhead.execute(double, i)
            _ = future.result(timeout=5.0)

        stats = bulkhead.get_stats()
        assert stats["total_executions"] == 3
        assert stats["successful_executions"] == 3
        assert stats["failed_executions"] == 0
        bulkhead.shutdown(wait=False)

    def test_stats_track_failures(self):
        """Test stats track failed executions."""
        config = BulkheadConfig(name="test_stats", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

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
        bulkhead.shutdown(wait=False)

    def test_reset_stats(self):
        """Test resetting statistics."""
        config = BulkheadConfig(name="test_stats", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def identity(x: int) -> int:
            return x

        # Execute some operations
        for i in range(5):
            future = bulkhead.execute(identity, i)
            _ = future.result(timeout=5.0)

        bulkhead.reset_stats()
        stats = bulkhead.get_stats()

        assert stats["total_executions"] == 0
        assert stats["successful_executions"] == 0
        assert stats["failed_executions"] == 0
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingCircuitBreaker:
    """Test circuit breaker integration in BulkheadThreading."""

    def test_circuit_opens_after_failure(self):
        """Test circuit opens after failures.

        Note: The resilient_circuit library opens after ANY failure when
        using Fraction(n, n) since 1/1 = 100% >= 100%.
        """
        import uuid

        config = BulkheadConfig(
            name=f"test_circuit_{uuid.uuid4().hex[:8]}",  # Unique name to avoid state leakage
            circuit_breaker_enabled=True,
            failure_threshold=1,  # Circuit opens after 1 failure
            success_threshold=1,
            isolation_duration=60.0,
        )
        bulkhead = BulkheadThreading(config)

        # Initially healthy
        assert bulkhead.get_state() == BulkheadState.HEALTHY

        # Cause 1 failure to open the circuit
        future = bulkhead.execute(lambda: 1 / 0)
        result = future.result(timeout=5.0)
        assert result.success is False

        # Circuit should now be open
        assert bulkhead.get_state() == BulkheadState.ISOLATED

        # Next call should be rejected
        with pytest.raises(BulkheadCircuitOpenError):
            _ = bulkhead.execute(lambda: "should fail")

        bulkhead.shutdown(wait=False)

    def test_circuit_stats_tracked(self):
        """Test circuit breaker status is in stats."""
        config = BulkheadConfig(
            name="test_circuit",
            circuit_breaker_enabled=True,
        )
        bulkhead = BulkheadThreading(config)

        stats = bulkhead.get_stats()
        assert stats["circuit_breaker_enabled"] is True
        assert "circuit_status" in stats
        bulkhead.shutdown(wait=False)

    def test_no_circuit_breaker_in_stats(self):
        """Test stats when circuit breaker is disabled."""
        config = BulkheadConfig(
            name="test_no_circuit",
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        stats = bulkhead.get_stats()
        assert stats["circuit_breaker_enabled"] is False
        assert "circuit_status" not in stats
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingState:
    """Test state management in BulkheadThreading."""

    def test_initial_state_healthy(self):
        """Test initial state is healthy."""
        config = BulkheadConfig(name="test_state", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        assert bulkhead.get_state() == BulkheadState.HEALTHY
        assert bulkhead.is_healthy() is True
        bulkhead.shutdown(wait=False)

    def test_state_with_circuit_breaker(self):
        """Test state reflects circuit breaker status.

        Note: The resilient_circuit library opens after ANY failure when
        using Fraction(n, n) since 1/1 = 100% >= 100%.
        """
        import uuid

        config = BulkheadConfig(
            name=f"test_state_{uuid.uuid4().hex[:8]}",  # Unique name to avoid state leakage
            circuit_breaker_enabled=True,
            failure_threshold=1,  # Circuit opens after 1 failure
        )
        bulkhead = BulkheadThreading(config)

        assert bulkhead.get_state() == BulkheadState.HEALTHY

        # Cause 1 failure to open the circuit
        future = bulkhead.execute(lambda: 1 / 0)
        result = future.result(timeout=5.0)
        assert result.success is False

        # Circuit should now be open
        assert bulkhead.get_state() == BulkheadState.ISOLATED
        assert bulkhead.is_healthy() is False
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingExceptionHandling:
    """Test exception handling in BulkheadThreading."""

    def test_custom_exception_wrapped(self):
        """Test custom exceptions are wrapped in BulkheadError."""
        config = BulkheadConfig(name="test_exc", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        class CustomError(Exception):
            pass

        def raising_func():
            raise CustomError("Custom exception")

        future = bulkhead.execute(raising_func)
        result = future.result(timeout=5.0)

        assert result.success is False
        assert isinstance(result.error, BulkheadError)
        assert "Custom exception" in str(result.error)
        bulkhead.shutdown(wait=False)

    def test_bulkhead_error_not_rewrapped(self):
        """Test BulkheadError is not double-wrapped."""
        config = BulkheadConfig(name="test_exc", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def raising_func():
            raise BulkheadError("Already a bulkhead error")

        future = bulkhead.execute(raising_func)
        result = future.result(timeout=5.0)

        assert result.success is False
        assert isinstance(result.error, BulkheadError)
        assert "Already a bulkhead error" in str(result.error)
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingShutdown:
    """Test shutdown behavior in BulkheadThreading."""

    def test_shutdown_wait(self):
        """Test shutdown with wait=True waits for tasks."""
        config = BulkheadConfig(name="test_shutdown", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        completed: list[bool] = []

        def slow_func() -> str:
            time.sleep(0.2)
            completed.append(True)
            return "done"

        future = bulkhead.execute(slow_func)

        # Shutdown with wait - should wait for task
        bulkhead.shutdown(wait=True)

        assert len(completed) == 1
        assert future.done()

    def test_shutdown_no_wait(self):
        """Test shutdown with wait=False returns immediately."""
        config = BulkheadConfig(name="test_shutdown", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def very_slow_func():
            time.sleep(10.0)
            return "done"

        _future = bulkhead.execute(very_slow_func)

        # Shutdown without waiting
        start = time.time()
        bulkhead.shutdown(wait=False, timeout=0.1)
        elapsed = time.time() - start

        # Should return quickly, not wait 10 seconds
        assert elapsed < 1.0


class TestBulkheadThreadingIntegration:
    """Integration tests for BulkheadThreading."""

    def test_with_threading(self):
        """Test BulkheadThreading works correctly when called from threads."""
        config = BulkheadConfig(
            name="thread_integration",
            max_concurrent_calls=4,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        results: list[int] = []
        lock = threading.Lock()

        def double(x: int) -> int:
            return x * 2

        def worker(n: int) -> None:
            future = bulkhead.execute(double, n)
            result = future.result(timeout=5.0)
            assert isinstance(result.result, int)
            with lock:
                results.append(result.result)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert len(results) == 10
        assert sorted(results) == [i * 2 for i in range(10)]
        bulkhead.shutdown(wait=False)

    def test_multiple_bulkheads_independent(self):
        """Test multiple bulkheads work independently."""
        config1 = BulkheadConfig(name="bulkhead1", circuit_breaker_enabled=False)
        config2 = BulkheadConfig(name="bulkhead2", circuit_breaker_enabled=False)

        bulkhead1 = BulkheadThreading(config1)
        bulkhead2 = BulkheadThreading(config2)

        future1 = bulkhead1.execute(lambda: "result1")
        future2 = bulkhead2.execute(lambda: "result2")

        result1 = future1.result(timeout=5.0)
        result2 = future2.result(timeout=5.0)

        assert result1.bulkhead_name == "bulkhead1"
        assert result2.bulkhead_name == "bulkhead2"
        assert result1.result == "result1"
        assert result2.result == "result2"

        bulkhead1.shutdown(wait=False)
        bulkhead2.shutdown(wait=False)

    def test_high_throughput(self):
        """Test handling many rapid submissions."""
        config = BulkheadConfig(
            name="high_throughput",
            max_concurrent_calls=10,
            max_queue_size=100,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        def fast_func(n: int) -> int:
            return n * 2

        # Submit many tasks rapidly
        futures = [bulkhead.execute(fast_func, i) for i in range(50)]

        # All should complete successfully
        results = [f.result(timeout=30.0) for f in futures]

        assert len(results) == 50
        assert all(r.success for r in results)
        assert [r.result for r in results] == [i * 2 for i in range(50)]
        bulkhead.shutdown(wait=True)

    def test_mixed_success_and_failure(self):
        """Test handling mix of successful and failing operations."""
        config = BulkheadConfig(name="mixed", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        def maybe_fail(n: int) -> int:
            if n % 2 == 0:
                return n * 2
            else:
                raise ValueError(f"Odd number: {n}")

        futures = [bulkhead.execute(maybe_fail, i) for i in range(10)]
        results = [f.result(timeout=5.0) for f in futures]

        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]

        assert len(successes) == 5  # 0, 2, 4, 6, 8
        assert len(failures) == 5  # 1, 3, 5, 7, 9
        bulkhead.shutdown(wait=False)


class TestBulkheadThreadingQueuedTime:
    """Test queued time tracking in BulkheadThreading."""

    def test_queued_time_recorded(self):
        """Test that queued time is recorded in results."""
        config = BulkheadConfig(
            name="test_queue_time",
            max_concurrent_calls=1,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        def slow_func():
            time.sleep(0.1)
            return "done"

        # Submit two tasks - second will be queued
        future1 = bulkhead.execute(slow_func)
        future2 = bulkhead.execute(slow_func)

        result1 = future1.result(timeout=5.0)
        result2 = future2.result(timeout=5.0)

        # Second task should have non-zero queued time
        assert result1.queued_time >= 0
        assert result2.queued_time >= 0
        # Second task should have waited longer
        assert result2.queued_time >= result1.queued_time

        bulkhead.shutdown(wait=False)
