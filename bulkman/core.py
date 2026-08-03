"""Core bulkhead pattern implementation for the asyncio event loop.

The async implementation is written against AnyIO primitives.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import logging
import threading
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from fractions import Fraction
from functools import wraps
from typing import Any, Awaitable, Callable, TypeVar, cast

import anyio
import sniffio
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

logger = logging.getLogger("bulkman")

T = TypeVar("T")


class Bulkhead:
    """
    Implements the bulkhead pattern to isolate function executions
    and prevent cascading failures.

    Concurrency is governed by a semaphore of `max_concurrent_calls` permits,
    with at most `max_queue_size` additional tasks admitted to wait.  Tasks
    beyond that capacity are rejected with `BulkheadFullError`.

    Sync functions run in a dedicated thread pool of `max_concurrent_calls`
    threads (one pool per bulkhead); on timeout the caller returns and the
    thread finishes in the background, bounded by the pool size.
    """

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ):
        self.config = config
        self.name = config.name

        # Execution control using AnyIO primitives (bind to the running loop)
        self._semaphore = anyio.Semaphore(config.max_concurrent_calls)
        # Dedicated pool for sync functions: bounds the number of actually
        # running (or abandoned) threads to max_concurrent_calls, mirroring
        # BulkheadThreading.  The anyio shared pool (40 threads) would both
        # cap concurrency below the configured limit and ignore it above.
        self._sync_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.max_concurrent_calls,
            thread_name_prefix=f"Bulkhead-{config.name}",
        )
        # Admitted-but-not-finished count (running + waiting).  Guards queue
        # admission so that max_concurrent_calls + max_queue_size is enforced.
        # A plain threading.Lock is used deliberately: the critical sections
        # are tiny (int ops) and must remain checkpoint-free so cleanup runs
        # even while a cancellation is propagating (async locks re-raise the
        # cancellation at their await points and the release is skipped).
        self._queue_lock = threading.Lock()
        self._in_flight = 0
        self._shutdown = False

        # Circuit breaker integration
        # resilient_circuit's CircuitProtectorPolicy is not thread-safe: its
        # internal status object is swapped and mutated on every call.  All
        # access from bulkman is serialized with this plain lock (it is also
        # held by worker threads and the event loop thread alike).
        self._circuit_lock = threading.Lock()
        self._circuit_breaker: CircuitProtectorPolicy | None = None
        if config.circuit_breaker_enabled:
            # Namespace is controlled by the storage, not the config
            # Each bulkhead is identified by (resource_key, namespace)
            # - resource_key = config.name (unique per bulkhead)
            # - namespace = from storage (shared by app/environment)
            self._circuit_breaker = CircuitProtectorPolicy(
                resource_key=config.name,
                storage=circuit_storage,  # Namespace comes from storage
                cooldown=timedelta(seconds=config.isolation_duration),
                # failure_limit is a failure *rate* over a window of size
                # denominator.  Fraction(t, t) reduces to Fraction(1, 1) -
                # resilient_circuit's "any failure opens" sentinel with a
                # 1-slot window - so failure_threshold would be ignored.
                # Fraction(t-1, t) gives a real window of t calls: the
                # circuit opens when at least t-1 of the last t failed
                # (i.e. after t consecutive failures).
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
        self._active_tasks = 0
        # threading.Lock: see _queue_lock comment - stats updates must stay
        # checkpoint-free so they also run during cancellation unwinding.
        self._stats_lock = threading.Lock()

        logger.info(
            "Bulkhead '%s' initialized with %d concurrent calls and queue size %d",
            self.name,
            config.max_concurrent_calls,
            config.max_queue_size,
        )

    def _on_circuit_status_change(
        self,
        policy: CircuitProtectorPolicy,
        old_status: CircuitState,
        new_status: CircuitState,
    ) -> None:
        """Callback for circuit breaker status changes."""
        logger.info(
            "Bulkhead '%s' circuit breaker changed: %s -> %s",
            self.name,
            old_status.value,
            new_status.value,
        )

    async def _check_circuit(self) -> None:
        """Check if circuit breaker allows execution.

        Raises:
            BulkheadCircuitOpenError: If the circuit is open.
        """
        breaker = self._circuit_breaker
        if breaker is None:
            return
        rejected = False
        with self._circuit_lock:
            try:
                # Synchronous in-memory check; may auto-transition OPEN ->
                # HALF_OPEN when the cooldown has expired.
                breaker._status.validate_execution()
            except ProtectedCallError:
                rejected = True
        if rejected:
            with self._stats_lock:
                self._rejected_executions += 1
            raise BulkheadCircuitOpenError(
                f"Bulkhead '{self.name}' circuit is open - requests are blocked"
            )

    def _persist_circuit_state(self, breaker: CircuitProtectorPolicy) -> None:
        """Persist the circuit state without clobbering another process's OPEN.

        The shared storage is last-writer-wins: a process whose LOCAL circuit
        is still CLOSED would overwrite a stored OPEN/HALF_OPEN with CLOSED,
        silently losing the protection signal for restarted instances.  When
        the local state is CLOSED, only write when the stored state is not
        OPEN/HALF_OPEN.  (State TRANSITIONS - including probe-recovery to
        CLOSED - are persisted by resilient_circuit itself.)
        """
        if breaker._status.status_type is CircuitState.CLOSED:
            try:
                stored = breaker.storage.get_state(breaker.resource_key)
            except Exception:
                stored = None
            if stored and stored.get("state") in (
                CircuitState.OPEN.value,
                CircuitState.HALF_OPEN.value,
            ):
                return
        breaker._save_state()

    def _mark_success(self) -> None:
        """Record a success in the circuit breaker (thread-safe)."""
        breaker = self._circuit_breaker
        if breaker is None:
            return
        with self._circuit_lock:
            try:
                breaker._status.mark_success()
                self._persist_circuit_state(breaker)
            except Exception as e:
                logger.warning("Failed to mark circuit success: %s", e)

    def _mark_failure(self) -> None:
        """Record a failure in the circuit breaker (thread-safe)."""
        breaker = self._circuit_breaker
        if breaker is None:
            return
        with self._circuit_lock:
            try:
                breaker._status.mark_failure()
                self._persist_circuit_state(breaker)
            except Exception as e:
                logger.warning("Failed to mark circuit failure: %s", e)

    async def _run_sync(
        self, func: Callable[..., T], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> T:
        """Run a sync function in the bulkhead's dedicated thread pool.

        The wait is cancellable, so the bulkhead timeout fires promptly on
        both backends even for sync functions (anyio's to_thread.run_sync
        shields the wait by default, which on the asyncio backend silently
        disables the timeout).  On timeout the pool thread keeps running -
        bounded by the pool size - matching BulkheadThreading.

        The completion wait MUST go through the running event loop's own
        thread-safe machinery (asyncio.wrap_future, or a worker-thread wait
        on non-asyncio backends).  An anyio.Event set from a pool thread is
        unsafe - it wraps the raw backend primitive, which is not
        thread-safe (asyncio.Event.set() mutates loop waiters).
        """
        future = self._sync_executor.submit(func, *args, **kwargs)
        try:
            if sniffio.current_async_library() == "asyncio":
                await asyncio.wrap_future(future)
            else:
                return cast(
                    T, await anyio.to_thread.run_sync(future.result, abandon_on_cancel=True)
                )
            return future.result()
        except BaseException as e:
            if isinstance(e, anyio.get_cancelled_exc_class()):
                raise
            # A BaseException from a worker thread (e.g. KeyboardInterrupt)
            # must not masquerade as the caller's interrupt: converting it
            # also keeps the portal in BulkheadSync alive (anyio re-raises
            # BaseException inside its task group and would kill the loop).
            raise BulkheadError(f"Sync function raised {type(e).__name__}: {e}") from e

    async def _invoke(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run the wrapped function (sync functions run in the thread pool)."""
        if inspect.iscoroutinefunction(func):
            return cast(T, await func(*args, **kwargs))
        return cast(T, await self._run_sync(func, args, kwargs))

    async def _run(
        self,
        func: Callable[..., T],
        submission_time: float,
        execution_id: str,
        started: list[bool],
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Acquire a permit, execute the function, and record the outcome.

        The caller is responsible for queue admission (`_in_flight`) and the
        timeout scope.  Every task that starts (acquires the permit) records
        exactly one outcome, and the outcome + active-slot release are atomic
        under the stats lock, so `total == success + failed + active` holds
        at every observable instant.
        """
        async with self._semaphore:
            started[0] = True
            with self._stats_lock:
                self._total_executions += 1
                self._active_tasks += 1

            start_time = time.monotonic()
            queued_time = start_time - submission_time

            released = False
            try:
                try:
                    result = await self._invoke(func, *args, **kwargs)
                except Exception as e:
                    # Not a cancellation: record the failure.
                    self._mark_failure()
                    execution_time = time.monotonic() - start_time
                    with self._stats_lock:
                        self._failed_executions += 1
                        self._active_tasks -= 1
                        released = True
                    return ExecutionResult(
                        success=False,
                        result=None,
                        error=self._wrap_error(e),
                        execution_time=execution_time,
                        bulkhead_name=self.name,
                        queued_time=queued_time,
                        execution_id=execution_id,
                    )
                else:
                    self._mark_success()
                    execution_time = time.monotonic() - start_time
                    with self._stats_lock:
                        self._successful_executions += 1
                        self._active_tasks -= 1
                        released = True
                    return ExecutionResult(
                        success=True,
                        result=result,
                        error=None,
                        execution_time=execution_time,
                        bulkhead_name=self.name,
                        queued_time=queued_time,
                        execution_id=execution_id,
                    )
            except BaseException as e:
                if isinstance(e, anyio.get_cancelled_exc_class()):
                    # The execution started but was cut short: record the
                    # outcome so the ledger stays exact, then propagate.
                    with self._stats_lock:
                        self._failed_executions += 1
                        self._active_tasks -= 1
                        released = True
                raise
            finally:
                if not released:
                    with self._stats_lock:
                        self._active_tasks -= 1

    @staticmethod
    def _wrap_error(e: Exception) -> BulkheadError:
        """Wrap non-bulkhead exceptions so callers see a stable error type."""
        if isinstance(e, BulkheadError):
            return e
        wrapped = BulkheadError(f"Execution failed: {e}")
        wrapped.__cause__ = e
        return wrapped

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult:
        """
        Execute a function through the bulkhead with isolation.

        Args:
            func: The function to execute (can be sync or async)
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            ExecutionResult containing the result or error

        Raises:
            BulkheadCircuitOpenError: If circuit breaker is open
            BulkheadFullError: If the bulkhead is at capacity
            BulkheadTimeoutError: If operation times out
        """
        await self._check_circuit()

        if self._shutdown:
            raise RuntimeError(f"Bulkhead '{self.name}' is shut down")

        submission_time = time.monotonic()
        execution_id = str(uuid.uuid4())

        # Queue admission: capacity is max_concurrent_calls running plus
        # max_queue_size waiting.  Reject beyond that (never block unbounded).
        capacity = self.config.max_concurrent_calls + self.config.max_queue_size
        with self._queue_lock:
            if self._in_flight >= capacity:
                full = True
            else:
                self._in_flight += 1
                full = False
        if full:
            with self._stats_lock:
                self._rejected_executions += 1
            raise BulkheadFullError(
                f"Bulkhead '{self.name}' is at capacity " f"({self._in_flight}/{capacity})"
            )

        try:
            timeout = self.config.timeout_seconds
            started: list[bool] = [False]
            if timeout is None:
                return await self._run(
                    func, submission_time, execution_id, started, *args, **kwargs
                )

            with anyio.move_on_after(timeout) as cancel_scope:
                result = await self._run(
                    func, submission_time, execution_id, started, *args, **kwargs
                )
            if cancel_scope.cancelled_caught:
                # Only timeouts that never started count as rejections; a
                # started task already recorded its outcome inside _run.
                if not started[0]:
                    with self._stats_lock:
                        self._rejected_executions += 1
                raise BulkheadTimeoutError(
                    f"Bulkhead '{self.name}' operation timed out after "
                    f"{self.config.timeout_seconds} seconds"
                )
            return result
        finally:
            # Release the admitted slot on every path (success, failure, timeout).
            with self._queue_lock:
                self._in_flight -= 1

    @asynccontextmanager
    async def context(self) -> AsyncIterator[Bulkhead]:
        """
        Context manager for bulkhead operations.

        Example:
            async with bulkhead.context():
                result = await bulkhead.execute(my_function, arg1, arg2)
        """
        yield self

    async def get_state(self) -> BulkheadState:
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

    async def get_stats(self) -> dict[str, Any]:
        """Get statistics for the bulkhead."""
        state_value = (await self.get_state()).value
        with self._queue_lock:
            in_flight_count = self._in_flight
        with self._stats_lock:
            stats = {
                "name": self.name,
                "state": state_value,
                "total_executions": self._total_executions,
                "successful_executions": self._successful_executions,
                "failed_executions": self._failed_executions,
                "rejected_executions": self._rejected_executions,
                "active_tasks": self._active_tasks,
                "in_flight_count": in_flight_count,
                "max_concurrent_calls": self.config.max_concurrent_calls,
                "max_queue_size": self.config.max_queue_size,
            }

            # Add circuit breaker info if enabled
            breaker = self._circuit_breaker
            if breaker is not None:
                stats["circuit_breaker_enabled"] = True
                with self._circuit_lock:
                    stats["circuit_status"] = breaker.status.value
            else:
                stats["circuit_breaker_enabled"] = False

            return stats

    async def reset_stats(self) -> None:
        """Reset bulkhead statistics."""
        with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0

    async def is_healthy(self) -> bool:
        """Check if bulkhead is healthy."""
        state = await self.get_state()
        return state in (BulkheadState.HEALTHY, BulkheadState.DEGRADED)

    async def shutdown(self) -> None:
        """Shut the bulkhead down. Idempotent and terminal.

        Closes the sync-function thread pool (matching BulkheadThreading's
        contract: further execute() calls raise RuntimeError) and stops
        circuit breaker state access.
        """
        with self._circuit_lock:
            self._circuit_breaker = None
        self._sync_executor.shutdown(wait=False, cancel_futures=True)
        self._shutdown = True
        logger.info("Bulkhead '%s' shut down", self.name)


class BulkheadManager:
    """
    Manages multiple bulkheads for different system components.

    Args:
        circuit_storage: Optional PostgreSQL storage for circuit breaker persistence.
                        All bulkheads created by this manager will share this storage.
                        The namespace is set at storage creation time.

    Example:
        # Production environment
        storage = create_storage(namespace="production")
        manager = BulkheadManager(circuit_storage=storage)

        # All bulkheads share the "production" namespace
        # but are isolated by their names
    """

    def __init__(
        self,
        circuit_storage: CircuitBreakerStorage | None = None,
    ):
        self._bulkheads: dict[str, Bulkhead] = {}
        self._lock = anyio.Lock()
        self._circuit_storage = circuit_storage

    async def create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        """Create and register a new bulkhead."""
        async with self._lock:
            if config.name in self._bulkheads:
                raise ValueError(f"Bulkhead with name '{config.name}' already exists")

            bulkhead = Bulkhead(config, circuit_storage=self._circuit_storage)
            self._bulkheads[config.name] = bulkhead
            return bulkhead

    async def get_bulkhead(self, name: str) -> Bulkhead | None:
        """Get a bulkhead by name."""
        async with self._lock:
            return self._bulkheads.get(name)

    async def get_or_create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        """Get existing bulkhead or create new one."""
        async with self._lock:
            if config.name in self._bulkheads:
                return self._bulkheads[config.name]

            # Create bulkhead directly without acquiring lock again
            bulkhead = Bulkhead(config, circuit_storage=self._circuit_storage)
            self._bulkheads[config.name] = bulkhead
            return bulkhead

    async def execute_in_bulkhead(
        self,
        bulkhead_name: str,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Execute a function in a specific bulkhead."""
        bulkhead = await self.get_bulkhead(bulkhead_name)
        if not bulkhead:
            raise ValueError(f"Bulkhead '{bulkhead_name}' not found")
        return await bulkhead.execute(func, *args, **kwargs)

    async def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all bulkheads."""
        async with self._lock:
            stats = {}
            for name, bulkhead in self._bulkheads.items():
                stats[name] = await bulkhead.get_stats()
            return stats

    async def get_health_status(self) -> dict[str, bool]:
        """Get health status for all bulkheads."""
        async with self._lock:
            status = {}
            for name, bulkhead in self._bulkheads.items():
                status[name] = await bulkhead.is_healthy()
            return status

    @asynccontextmanager
    async def context(self) -> AsyncIterator[BulkheadManager]:
        """Context manager for bulkhead manager."""
        yield self


def with_bulkhead(
    bulkhead: Bulkhead,
) -> Callable[[Callable[..., T]], Callable[..., Awaitable[T]]]:
    """
    Decorator to execute an async function through a bulkhead.

    Example:
        @with_bulkhead(my_bulkhead)
        async def query_database(query):
            return await db.execute(query)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            result = await bulkhead.execute(func, *args, **kwargs)
            if not result.success:
                raise result.error or BulkheadError("Execution failed")
            return cast(T, result.result)

        return cast(Callable[..., Awaitable[T]], wrapper)

    return decorator
