"""Tests for context variable propagation in BulkheadThreading."""

import contextvars

from bulkman import BulkheadConfig, BulkheadThreading

# Define a context variable for testing
test_ctx_var = contextvars.ContextVar("test_ctx_var", default="default")


class TestBulkheadThreadingContext:
    """Test context propagation in BulkheadThreading."""

    def test_context_is_propagated(self):
        """Test that context variables set in main thread are visible in worker."""
        config = BulkheadConfig(name="test_ctx", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        # Set value in main thread
        token = test_ctx_var.set("custom_value")

        try:

            def check_context():
                # Verify we see the value set in the parent thread
                return test_ctx_var.get()

            # Execute via bulkhead
            future = bulkhead.execute(check_context)
            result = future.result(timeout=1.0)

            assert result.success is True
            assert result.result == "custom_value"

        finally:
            test_ctx_var.reset(token)
            bulkhead.shutdown(wait=False)

    def test_context_isolation(self):
        """Test that changes in worker do not affect parent context."""
        config = BulkheadConfig(name="test_ctx_isolation", circuit_breaker_enabled=False)
        bulkhead = BulkheadThreading(config)

        token = test_ctx_var.set("parent_value")

        try:

            def modify_context():
                # Verify initial value
                initial = test_ctx_var.get()
                # Modify in worker
                test_ctx_var.set("worker_value")
                return initial, test_ctx_var.get()

            future = bulkhead.execute(modify_context)
            result = future.result(timeout=1.0)

            assert result.success is True
            initial_in_worker, final_in_worker = result.result

            assert initial_in_worker == "parent_value"
            assert final_in_worker == "worker_value"

            # Parent should still see original value
            assert test_ctx_var.get() == "parent_value"

        finally:
            test_ctx_var.reset(token)
            bulkhead.shutdown(wait=False)
