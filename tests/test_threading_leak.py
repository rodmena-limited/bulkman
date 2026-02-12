"""Test for resource leaks in BulkheadThreading."""

import threading
import time

import pytest

from bulkman import BulkheadConfig, BulkheadFullError, BulkheadThreading


class TestBulkheadThreadingLeak:
    """Test leak scenarios in BulkheadThreading."""

    def test_cancel_future_decrements_inflight(self):
        """
        Test that cancelling a pending future releases the in-flight slot.

        Scenario:
        1. Fill the bulkhead to capacity (max_concurrent + queue).
        2. Submit one more task (should fail, confirming full).
        3. Cancel one of the pending tasks.
        4. Submit one more task (should succeed now).
        """
        config = BulkheadConfig(
            name="test_cancel_leak",
            max_concurrent_calls=1,
            max_queue_size=1,  # Total capacity = 2
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        # Event to keep the worker busy
        worker_hold = threading.Event()

        def blocking_func() -> str:
            _ = worker_hold.wait(timeout=5.0)
            return "done"

        # 1. Fill capacity
        # Task A: Consumes the 1 worker thread
        future_a = bulkhead.execute(blocking_func)

        # Task B: Sits in the queue (size 1)
        future_b = bulkhead.execute(blocking_func)

        # Wait a tiny bit to ensure state settles
        time.sleep(0.05)

        # Verify full
        with bulkhead._in_flight_lock:
            assert bulkhead._in_flight_count == 2

        # 2. Verify rejection
        with pytest.raises(BulkheadFullError):
            _ = bulkhead.execute(lambda: "rejected")

        # 3. Cancel the queued task (Task B)
        cancelled = future_b.cancel()
        assert cancelled is True, "Task B should have been cancellable (still in queue)"

        # Wait for callback/cleanup (if implemented correctly)
        time.sleep(0.05)

        # 4. Check leak
        # If buggy, count is still 2, and this will fail.
        # If fixed, count is 1, and this will succeed.
        try:
            future_c = bulkhead.execute(lambda: "success")
        except BulkheadFullError:
            pytest.fail("In-flight count leaked! Capacity not restored after cancellation.")

        # Unblock the worker so Task A finishes and Task C can run
        worker_hold.set()

        # Now wait for C
        result = future_c.result(timeout=1.0)
        assert result.success is True

        # Ensure Task A also finished cleanly
        _ = future_a.result(timeout=1.0)
        bulkhead.shutdown(wait=False)

    def test_executor_shutdown_cleanup(self):
        """Test cleanup during executor shutdown."""
        config = BulkheadConfig(
            name="test_shutdown_cleanup",
            max_concurrent_calls=1,
            max_queue_size=1,
            circuit_breaker_enabled=False,
        )
        bulkhead = BulkheadThreading(config)

        worker_hold = threading.Event()

        _ = bulkhead.execute(lambda: worker_hold.wait(timeout=1.0))

        # Force shutdown without waiting
        bulkhead.shutdown(wait=False)

        # Allow worker to finish
        worker_hold.set()

        # Verify circuit breaker reference is cleared (part of previous fix)
        assert bulkhead._circuit_breaker is None
