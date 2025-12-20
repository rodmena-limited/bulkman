"""Pure threading-based bulkhead implementation.

This module provides a bulkhead implementation using only Python threading
primitives, without Trio or any async framework. Designed for sync workloads
where the async overhead of Trio is unnecessary.

Key differences from BulkheadSync:
- No Trio dependency or async wrapper
- Direct ThreadPoolExecutor usage
- Simpler execution path
- Same timeout limitation (threads can't be killed, but control returns)

Usage:
    from bulkman import BulkheadThreading, BulkheadConfig

    config = BulkheadConfig(
        name="my_bulkhead",
        max_concurrent_calls=4,
        timeout_seconds=30.0,
    )
    bulkhead = BulkheadThreading(config)

    future = bulkhead.execute(my_function, arg1, arg2)
    result = future.result(timeout=30)  # ExecutionResult
"""

from __future__ import annotations

import concurrent.futures
import contextvars
import logging
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from fractions import Fraction
from typing import Any, Callable

from resilient_circuit import CircuitProtectorPolicy, CircuitState
from resilient_circuit.exceptions import ProtectedCallError
from resilient_circuit.storage import CircuitBreakerStorage

from bulkman.config import BulkheadConfig, ExecutionResult
from bulkman.exceptions import (
    BulkheadCircuitOpenError,
    BulkheadError,
    BulkheadFullError,
    BulkheadTimeoutError,
)
from bulkman.state import BulkheadState

logger = logging.getLogger("bulkman.threading")


class BulkheadThreading:
    """Pure threading-based bulkhead for sync workloads.

    Unlike BulkheadSync (which wraps async Bulkhead via Trio), this
    implementation uses only Python threading primitives for simpler,
    more predictable behavior with sync code.

    Features:
    - Concurrency limiting via ThreadPoolExecutor size
    - Queue management via executor internal queue and capacity tracking
    - Circuit breaker integration (same as async version)
    - Execution statistics and metrics
    - Timeout support (returns control, thread continues - Python limitation)
    - Context propagation (via contextvars) for tracing/logging

    Timeout Behavior:
        Python threads cannot be forcibly killed. When a timeout occurs:
        1. Control returns to the caller with TimeoutError
        2. The thread continues running in the background (or aborts if still in queue)
        3. The result is discarded when it eventually completes

        This is identical to Trio's behavior for sync code, but without
        the complexity of the async wrapper.

    Example:
        config = BulkheadConfig(
            name="database",
            max_concurrent_calls=10,
            max_queue_size=50,
            timeout_seconds=30.0,
            circuit_breaker_enabled=True,
        )
        bulkhead = BulkheadThreading(config)

        # Execute with timeout
        future = bulkhead.execute(db_query, "SELECT * FROM users")
        try:
            result = future.result(timeout=30)
            if result.success:
                print(result.result)
        except TimeoutError:
            print("Query timed out")
    """

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ):
        """Initialize the threading-based bulkhead.

        Args:
            config: Bulkhead configuration
            circuit_storage: Optional storage for circuit breaker persistence.
                           If None, circuit breaker uses in-memory state.
        """
        self.config = config
        self.name = config.name

        # Thread pool for execution
        # Size = max_concurrent_calls.
        # Queueing is handled by the executor's internal queue, but we limit
        # the depth of that queue using _in_flight_count and max_queue_size.
        self._executor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_calls,
            thread_name_prefix=f"Bulkhead-{config.name}",
        )

        # Queue tracking for rejection
        # _in_flight_count tracks tasks that are submitted but not completed
        # This includes both executing tasks and queued tasks
        self._in_flight_count = 0
        self._in_flight_lock = threading.Lock()

        # Circuit breaker integration
        # Note: failure_limit is a Fraction representing the failure rate threshold.
        # e.g., Fraction(3, 3) means "open circuit if 3 out of last 3 calls fail"
        # We use failure_threshold for both numerator and denominator to require
        # consecutive failures equal to the threshold before opening.
        self._circuit_breaker: CircuitProtectorPolicy | None = None
        if config.circuit_breaker_enabled:
            self._circuit_breaker = CircuitProtectorPolicy(
                resource_key=config.name,
                storage=circuit_storage,
                cooldown=timedelta(seconds=config.isolation_duration),
                failure_limit=Fraction(
                    config.failure_threshold, config.failure_threshold
                ),
                success_limit=Fraction(
                    config.success_threshold, config.success_threshold
                ),
                on_status_change=self._on_circuit_status_change,
            )

        # Statistics
        self._total_executions = 0
        self._successful_executions = 0
        self._failed_executions = 0
        self._rejected_executions = 0
        self._timed_out_executions = 0
        self._active_tasks = 0
        self._stats_lock = threading.Lock()

        logger.info(
            "BulkheadThreading '%s' initialized: max_concurrent=%d, queue_size=%d, timeout=%s",
            self.name,
            config.max_concurrent_calls,
            config.max_queue_size,
            config.timeout_seconds,
        )

    def _on_circuit_status_change(
        self,
        policy: CircuitProtectorPolicy,
        old_status: CircuitState,
        new_status: CircuitState,
    ) -> None:
        """Callback for circuit breaker status changes."""
        logger.info(
            "BulkheadThreading '%s' circuit breaker: %s -> %s",
            self.name,
            old_status.value,
            new_status.value,
        )

    def _check_circuit(self) -> None:
        """Check if circuit breaker allows execution.

        Raises:
            BulkheadCircuitOpenError: If circuit is open
        """
        if self._circuit_breaker:
            try:
                self._circuit_breaker._status.validate_execution()
            except ProtectedCallError:
                with self._stats_lock:
                    self._rejected_executions += 1
                raise BulkheadCircuitOpenError(
                    f"BulkheadThreading '{self.name}' circuit is open - requests blocked"
                )

    def _check_queue_capacity(self) -> None:
        """Check if queue has capacity for new task.

        Total capacity = max_concurrent_calls + max_queue_size
        If in_flight_count >= total capacity, reject the task.

        Raises:
            BulkheadFullError: If at capacity (no room for more tasks)
        """
        total_capacity = self.config.max_concurrent_calls + self.config.max_queue_size
        with self._in_flight_lock:
            if self._in_flight_count >= total_capacity:
                with self._stats_lock:
                    self._rejected_executions += 1
                raise BulkheadFullError(
                    f"BulkheadThreading '{self.name}' is at capacity "
                    f"({self._in_flight_count}/{total_capacity})"
                )
            self._in_flight_count += 1

    def _decrement_in_flight(self, future: Future[ExecutionResult] | None = None) -> None:
        """Decrement in-flight count after task completes/cancels.

        This is designed to be a Future done_callback.
        """
        with self._in_flight_lock:
            if self._in_flight_count > 0:
                self._in_flight_count -= 1

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future[ExecutionResult]:
        """Execute a function through the bulkhead.

        The function is submitted to a thread pool and executed when a
        thread becomes available. Returns a Future that can be
        awaited with an optional timeout.

        Args:
            func: The function to execute (must be sync, not async)
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Future[ExecutionResult]: A future that resolves to ExecutionResult

        Raises:
            BulkheadCircuitOpenError: If circuit breaker is open
            BulkheadFullError: If queue is at capacity

        Example:
            future = bulkhead.execute(requests.get, "https://api.example.com")
            result = future.result(timeout=10)  # Wait up to 10 seconds
        """
        # Check circuit breaker first
        self._check_circuit()

        # Prepare context and metadata BEFORE checking queue capacity.
        # This prevents a leak where we increment capacity but then fail to
        # submit due to an error in uuid/context creation.
        submission_time = time.monotonic()
        execution_id = str(uuid.uuid4())

        # Capture the current context (for tracing/logging propagation)
        ctx = contextvars.copy_context()

        # Capture circuit breaker reference to avoid race with shutdown
        # If shutdown() runs and sets self._circuit_breaker = None, this local
        # reference keeps the object alive for this execution.
        circuit_breaker = self._circuit_breaker

        # Check queue capacity (Increments _in_flight_count)
        self._check_queue_capacity()

        def wrapper() -> ExecutionResult:
            """Wrapper that handles timing and circuit breaker."""

            # Check if we waited too long in the queue
            start_time = time.monotonic()
            queued_time = start_time - submission_time

            if (
                self.config.timeout_seconds
                and queued_time > self.config.timeout_seconds
            ):
                with self._stats_lock:
                    self._timed_out_executions += 1
                return ExecutionResult(
                    success=False,
                    result=None,
                    error=BulkheadTimeoutError(
                        f"Timeout waiting for execution slot in '{self.name}' "
                        f"(queued for {queued_time:.2f}s)"
                    ),
                    execution_time=0.0,
                    bulkhead_name=self.name,
                    queued_time=queued_time,
                    execution_id=execution_id,
                )

            with self._stats_lock:
                self._total_executions += 1
                self._active_tasks += 1

            try:
                # Execute the function
                result = func(*args, **kwargs)

                execution_time = time.monotonic() - start_time

                # Record success in circuit breaker
                if circuit_breaker:
                    try:
                        circuit_breaker._status.mark_success()
                        circuit_breaker._save_state()
                    except Exception as e:
                        logger.warning("Failed to mark circuit success: %s", e)

                with self._stats_lock:
                    self._successful_executions += 1
                    self._active_tasks -= 1

                return ExecutionResult(
                    success=True,
                    result=result,
                    error=None,
                    execution_time=execution_time,
                    bulkhead_name=self.name,
                    queued_time=queued_time,
                    execution_id=execution_id,
                )

            except Exception as e:
                execution_time = time.monotonic() - start_time

                # Record failure in circuit breaker
                if circuit_breaker:
                    try:
                        circuit_breaker._status.mark_failure()
                        circuit_breaker._save_state()
                    except Exception as circuit_err:
                        logger.warning(
                            "Failed to mark circuit failure: %s", circuit_err
                        )

                with self._stats_lock:
                    self._failed_executions += 1
                    self._active_tasks -= 1

                # Wrap exception if needed
                if not isinstance(e, BulkheadError):
                    wrapped = BulkheadError(f"Execution failed: {e}")
                    wrapped.__cause__ = e
                    error = wrapped
                else:
                    error = e

                return ExecutionResult(
                    success=False,
                    result=None,
                    error=error,
                    execution_time=execution_time,
                    bulkhead_name=self.name,
                    queued_time=queued_time,
                    execution_id=execution_id,
                )

        def context_wrapper():
            """Runs the wrapper inside the captured context."""
            return ctx.run(wrapper)

        # Submit to thread pool
        try:
            future = self._executor.submit(context_wrapper)
            # CRITICAL: Ensure in_flight_count is always decremented,
            # even if the task is cancelled or fails.
            future.add_done_callback(self._decrement_in_flight)
            return future
        except BaseException:
            # Catch BaseException to include KeyboardInterrupt and SystemExit.
            # If submission fails, we must decrement the count we incremented
            # in _check_queue_capacity to prevent permanent leaks.
            self._decrement_in_flight()
            raise

    def execute_with_timeout(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Execute a function with explicit timeout, blocking until complete.

        Convenience method that handles the Future and timeout internally.

        Args:
            func: The function to execute
            *args: Positional arguments for the function
            timeout: Timeout in seconds (defaults to config.timeout_seconds)
            **kwargs: Keyword arguments for the function

        Returns:
            ExecutionResult with success/failure status

        Raises:
            BulkheadCircuitOpenError: If circuit breaker is open
            BulkheadFullError: If queue is at capacity
            BulkheadTimeoutError: If execution times out
        """
        effective_timeout = (
            timeout if timeout is not None else self.config.timeout_seconds
        )

        future = self.execute(func, *args, **kwargs)

        try:
            return future.result(timeout=effective_timeout)
        except concurrent.futures.TimeoutError:
            # Try to cancel the future to stop it from starting if it's queued.
            # If running, it won't be stopped (Python threading limitation),
            # but we can at least stop waiting.
            future.cancel()

            with self._stats_lock:
                self._timed_out_executions += 1

            raise BulkheadTimeoutError(
                f"BulkheadThreading '{self.name}' execution timed out after {effective_timeout}s"
            )

    def get_state(self) -> BulkheadState:
        """Get the current state of the bulkhead."""
        if self._circuit_breaker:
            circuit_status = self._circuit_breaker.status
            if circuit_status == CircuitState.CLOSED:
                return BulkheadState.HEALTHY
            elif circuit_status == CircuitState.HALF_OPEN:
                return BulkheadState.DEGRADED
            elif circuit_status == CircuitState.OPEN:
                return BulkheadState.ISOLATED
        return BulkheadState.HEALTHY

    def get_stats(self) -> dict[str, Any]:
        """Get statistics for the bulkhead."""
        with self._stats_lock:
            stats = {
                "name": self.name,
                "state": self.get_state().value,
                "total_executions": self._total_executions,
                "successful_executions": self._successful_executions,
                "failed_executions": self._failed_executions,
                "rejected_executions": self._rejected_executions,
                "timed_out_executions": self._timed_out_executions,
                "active_tasks": self._active_tasks,
                "max_concurrent_calls": self.config.max_concurrent_calls,
                "max_queue_size": self.config.max_queue_size,
            }

            with self._in_flight_lock:
                stats["in_flight_count"] = self._in_flight_count

            if self._circuit_breaker:
                stats["circuit_breaker_enabled"] = True
                stats["circuit_status"] = self._circuit_breaker.status.value
            else:
                stats["circuit_breaker_enabled"] = False

            return stats

    def reset_stats(self) -> None:
        """Reset bulkhead statistics."""
        with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0
            self._timed_out_executions = 0

    def is_healthy(self) -> bool:
        """Check if bulkhead is healthy."""
        state = self.get_state()
        return state in (BulkheadState.HEALTHY, BulkheadState.DEGRADED)

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        """Shutdown the bulkhead.

        Args:
            wait: If True, wait for pending tasks to complete
            timeout: Maximum time to wait (only if wait=True)
        """
        logger.info("Shutting down BulkheadThreading '%s' (wait=%s)", self.name, wait)

        if timeout is not None:
            # Python 3.9+ supports cancel_futures
            try:
                self._executor.shutdown(wait=wait, cancel_futures=not wait)
            except TypeError:
                # Fallback for older Python versions
                self._executor.shutdown(wait=wait)
        else:
            self._executor.shutdown(wait=wait)

        # Break potential reference cycles
        self._circuit_breaker = None

        logger.info("BulkheadThreading '%s' shutdown complete", self.name)
