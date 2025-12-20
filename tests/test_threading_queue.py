"""Tests for BulkheadThreading queue behavior."""

import threading
import time

import pytest

from bulkman import BulkheadConfig, BulkheadThreading
from bulkman.exceptions import BulkheadTimeoutError


class TestBulkheadThreadingQueue:
    """Test queue behavior in BulkheadThreading."""

    def test_queue_timeout_avoids_execution(self):
        """Test that a task waiting in queue longer than timeout does not execute."""
        config = BulkheadConfig(
            name="test_queue_timeout",
            max_concurrent_calls=1,
            timeout_seconds=0.2,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        execution_flag = threading.Event()

        def blocking_func():
            time.sleep(0.5)  # Block longer than timeout
            return "blocked"

        def queued_func():
            execution_flag.set()  # Should NOT be called
            return "queued"

        # Submit blocking task
        future1 = bulkhead.execute(blocking_func)

        # Submit task that will queue
        # Queue wait will be approx 0.5s, which is > 0.2s timeout
        future2 = bulkhead.execute(queued_func)

        # Wait for both
        future1.result(timeout=1.0)
        result2 = future2.result(timeout=1.0)

        # Verify second task failed with timeout
        assert result2.success is False
        assert isinstance(result2.error, BulkheadTimeoutError)
        assert "Timeout waiting for execution slot" in str(result2.error)

        # Verify the function body was NEVER executed
        assert not execution_flag.is_set(), "Function executed despite timeout!"

        bulkhead.shutdown(wait=False)

    def test_executor_shutdown_decrements_inflight(self):
        """Test that failing to submit (due to shutdown) decrements in_flight_count."""
        config = BulkheadConfig(
            name="test_shutdown_leak",
            max_concurrent_calls=1,
            max_queue_size=10,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        # Shutdown the bulkhead/executor
        bulkhead.shutdown(wait=False)

        # Try to submit - should fail (RuntimeError from executor)
        with pytest.raises(RuntimeError):
            bulkhead.execute(lambda: None)

        # Verify in_flight_count is back to 0
        with bulkhead._in_flight_lock:
            assert bulkhead._in_flight_count == 0
