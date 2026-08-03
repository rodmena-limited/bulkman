"""Pure threading-based bulkhead implementation.

This module provides a bulkhead implementation using only Python threading
primitives, without any async framework. Designed for sync workloads
where the async overhead of the event loop is unnecessary.

Key differences from BulkheadSync:
- No async wrapper or background event loop
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

    Unlike BulkheadSync (which wraps the async Bulkhead via a background
    AnyIO loop), this implementation uses only Python threading primitives
    for simpler, more predictable behavior with sync code.

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

        This is identical to the async implementation's behavior for sync
        code, but without the complexity of the event loop wrapper.

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
        # Futures of admitted tasks, so shutdown(wait=True, timeout=...) can
        # honor the timeout instead of blocking until every task finishes.
        self._futures: set[Future[ExecutionResult]] = set()
        self._futures_lock = threading.Lock()

        # Circuit breaker integration
        # Note: failure_limit is a Fraction representing the failure rate
        # threshold over a window of size = denominator.  Fraction(t, t)
        # reduces to Fraction(1, 1) - resilient_circuit's "any failure
        # opens" sentinel with a 1-slot window - so failure_threshold would
        # be ignored.  Fraction(t-1, t) gives a window of t calls: the
        # circuit opens when at least t-1 of the last t failed (i.e. after
        # t consecutive failures).
        # resilient_circuit's policy object is not thread-safe; all access is
        # serialized through _circuit_lock (used by caller threads, worker
        # threads, and shutdown alike).
        self._circuit_lock = threading.Lock()
        self._circuit_breaker: CircuitProtectorPolicy | None = None
        if config.circuit_breaker_enabled:
            self._circuit_breaker = CircuitProtectorPolicy(
                resource_key=config.name,
                storage=circuit_storage,
                cooldown=timedelta(seconds=config.isolation_duration),
                failure_limit=(
                    Fraction(1, 1)
                    if config.failure_threshold == 1
                    else Fraction(config.failure_threshold - 1, config.failure_threshold)
                ),
                # Half-open probe window sized by success_threshold; the
                # circuit closes once the window is full and any probe
                # succeeded (use_success path of resilient_circuit).
                success_limit=Fraction(1, config.success_threshold),
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
        breaker = self._circuit_breaker
        if breaker is not None:
            rejected = False
            with self._circuit_lock:
                try:
                    breaker._status.validate_execution()
                except ProtectedCallError:
                    rejected = True
            if rejected:
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
        if future is not None:
            with self._futures_lock:
                self._futures.discard(future)

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

        # Submit to thread pool
        try:
            future = self._executor.submit(
                ctx.run,
                lambda: self._execute_wrapper(
                    func,
                    args,
                    kwargs,
                    submission_time,
                    execution_id,
                    circuit_breaker,
                ),
            )
            # CRITICAL: Ensure in_flight_count is always decremented,
            # even if the task is cancelled or fails.  Register the future
            # before the done_callback so a fast task cannot complete
            # between the two (the callback would then fire immediately).
            with self._futures_lock:
                self._futures.add(future)
            future.add_done_callback(self._decrement_in_flight)
            return future
        except BaseException:
            # Catch BaseException to include KeyboardInterrupt and SystemExit.
            # If submission fails, we must decrement the count we incremented
            # in _check_queue_capacity to prevent permanent leaks.
            self._decrement_in_flight()
            raise

    def _execute_wrapper(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        submission_time: float,
        execution_id: str,
        circuit_breaker: CircuitProtectorPolicy | None,
    ) -> ExecutionResult:
        """Run the user function, applying queue-timeout, stats and circuit marks."""
        start_time = time.monotonic()
        queued_time = start_time - submission_time

        if self.config.timeout_seconds and queued_time > self.config.timeout_seconds:
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
            result = func(*args, **kwargs)
        except Exception as e:
            execution_time = time.monotonic() - start_time
            self._record_failure(circuit_breaker)
            with self._stats_lock:
                self._failed_executions += 1
                self._active_tasks -= 1
            error = e if isinstance(e, BulkheadError) else BulkheadError(f"Execution failed: {e}")
            if not isinstance(e, BulkheadError):
                error.__cause__ = e
            return ExecutionResult(
                success=False,
                result=None,
                error=error,
                execution_time=execution_time,
                bulkhead_name=self.name,
                queued_time=queued_time,
                execution_id=execution_id,
            )

        execution_time = time.monotonic() - start_time
        self._record_success(circuit_breaker)
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

    def _persist_circuit_state(self, circuit_breaker: CircuitProtectorPolicy) -> None:
        """Persist the circuit state without clobbering another process's OPEN.

        The shared storage is last-writer-wins: a process whose LOCAL circuit
        is still CLOSED would overwrite a stored OPEN/HALF_OPEN with CLOSED,
        silently losing the protection signal for restarted instances.  When
        the local state is CLOSED, only write when the stored state is not
        OPEN/HALF_OPEN.  (State TRANSITIONS - including probe-recovery to
        CLOSED - are persisted by resilient_circuit itself.)
        """
        if circuit_breaker._status.status_type is CircuitState.CLOSED:
            try:
                stored = circuit_breaker.storage.get_state(circuit_breaker.resource_key)
            except Exception:
                stored = None
            if stored and stored.get("state") in (
                CircuitState.OPEN.value,
                CircuitState.HALF_OPEN.value,
            ):
                return
        circuit_breaker._save_state()

    def _record_success(self, circuit_breaker: CircuitProtectorPolicy | None) -> None:
        if circuit_breaker:
            try:
                with self._circuit_lock:
                    circuit_breaker._status.mark_success()
                    self._persist_circuit_state(circuit_breaker)
            except Exception as e:
                logger.warning("Failed to mark circuit success: %s", e)

    def _record_failure(self, circuit_breaker: CircuitProtectorPolicy | None) -> None:
        if circuit_breaker:
            try:
                with self._circuit_lock:
                    circuit_breaker._status.mark_failure()
                    self._persist_circuit_state(circuit_breaker)
            except Exception as circuit_err:
                logger.warning("Failed to mark circuit failure: %s", circuit_err)

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
        effective_timeout = timeout if timeout is not None else self.config.timeout_seconds

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
        breaker = self._circuit_breaker
        if breaker is not None:
            with self._circuit_lock:
                circuit_status = breaker.status
            if circuit_status == CircuitState.CLOSED:
                return BulkheadState.HEALTHY
            elif circuit_status == CircuitState.HALF_OPEN:
                return BulkheadState.DEGRADED
            elif circuit_status == CircuitState.OPEN:
                return BulkheadState.ISOLATED
        return BulkheadState.HEALTHY

    def get_stats(self) -> dict[str, Any]:
        """Get statistics for the bulkhead."""
        with self._in_flight_lock:
            in_flight_count = self._in_flight_count
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
                "in_flight_count": in_flight_count,
            }

            breaker = self._circuit_breaker
            if breaker is not None:
                stats["circuit_breaker_enabled"] = True
                with self._circuit_lock:
                    stats["circuit_status"] = breaker.status.value
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
            timeout: Maximum time to wait (only if wait=True).  After it
                     expires, queued tasks are cancelled and running tasks
                     are abandoned (Python threads cannot be killed).
        """
        logger.info("Shutting down BulkheadThreading '%s' (wait=%s)", self.name, wait)

        if wait and timeout is not None:
            with self._futures_lock:
                pending = list(self._futures)
            if pending:
                _, still_pending = concurrent.futures.wait(pending, timeout=timeout)
                if still_pending:
                    logger.warning(
                        "BulkheadThreading '%s' shutdown timed out; " "%d task(s) still running",
                        self.name,
                        len(still_pending),
                    )
                    wait = False

        self._executor.shutdown(wait=wait, cancel_futures=not wait)

        # Break potential reference cycles
        with self._circuit_lock:
            self._circuit_breaker = None

        logger.info("BulkheadThreading '%s' shutdown complete", self.name)
