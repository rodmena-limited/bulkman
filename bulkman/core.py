"""Core bulkhead pattern implementation using Trio."""

import inspect
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from fractions import Fraction
from functools import wraps
from typing import Any, Callable, TypeVar

import trio
from resilient_circuit import CircuitProtectorPolicy, CircuitState
from resilient_circuit.exceptions import ProtectedCallError
from resilient_circuit.storage import CircuitBreakerStorage

from bulkman.config import BulkheadConfig, ExecutionResult
from bulkman.exceptions import (
    BulkheadCircuitOpenError,
    BulkheadError,
    BulkheadTimeoutError,
)
from bulkman.state import BulkheadState

logger = logging.getLogger("bulkman")

T = TypeVar("T")


class Bulkhead:
    """
    Implements the bulkhead pattern to isolate function executions
    and prevent cascading failures using Trio for concurrency.
    """

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ):
        self.config = config
        self.name = config.name

        # Execution control using Trio primitives
        self._semaphore = trio.Semaphore(config.max_concurrent_calls)
        self._send_channel: trio.MemorySendChannel | None = None
        self._receive_channel: trio.MemoryReceiveChannel | None = None
        self._task_nursery: trio.Nursery | None = None

        # Circuit breaker integration
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
                failure_limit=Fraction(config.failure_threshold, config.failure_threshold),
                success_limit=Fraction(config.success_threshold, config.success_threshold),
                on_status_change=self._on_circuit_status_change,
            )

        # Statistics
        self._total_executions = 0
        self._successful_executions = 0
        self._failed_executions = 0
        self._rejected_executions = 0
        self._active_tasks = 0
        self._stats_lock = trio.Lock()

        # Running state
        self._running = False
        self._cancel_scope: trio.CancelScope | None = None

        logger.info(
            f"Bulkhead '{self.name}' initialized with {config.max_concurrent_calls} "
            f"concurrent calls and queue size {config.max_queue_size}"
        )

    def _on_circuit_status_change(
        self,
        policy: CircuitProtectorPolicy,
        old_status: CircuitState,
        new_status: CircuitState,
    ) -> None:
        """Callback for circuit breaker status changes."""
        logger.info(
            f"Bulkhead '{self.name}' circuit breaker changed: {old_status.value} -> {new_status.value}"
        )

    async def _check_circuit(self) -> None:
        """Check if circuit breaker allows execution."""
        if self._circuit_breaker:
            # Let circuit breaker validate (it will auto-transition from OPEN to HALF_OPEN after cooldown)
            try:
                # Run the validation in a thread since it's synchronous
                await trio.to_thread.run_sync(self._circuit_breaker._status.validate_execution)
            except ProtectedCallError:
                async with self._stats_lock:
                    self._rejected_executions += 1
                raise BulkheadCircuitOpenError(
                    f"Bulkhead '{self.name}' circuit is open - requests are blocked"
                )

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
        # Check circuit breaker
        await self._check_circuit()

        submission_time = trio.current_time()
        execution_id = str(uuid.uuid4())

        # Create timeout scope if needed
        timeout = self.config.timeout_seconds if self.config.timeout_seconds else float('inf')

        with trio.move_on_after(timeout) as cancel_scope:
            async with self._semaphore:
                # Update stats
                async with self._stats_lock:
                    self._total_executions += 1
                    self._active_tasks += 1

                start_time = trio.current_time()
                queued_time = start_time - submission_time

                try:
                    # Execute the function
                    if inspect.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        # trio.to_thread.run_sync doesn't support **kwargs, so wrap in lambda
                        if kwargs:
                            result = await trio.to_thread.run_sync(lambda: func(*args, **kwargs))
                        else:
                            result = await trio.to_thread.run_sync(func, *args)

                    execution_time = trio.current_time() - start_time

                    # Record success
                    if self._circuit_breaker:
                        try:
                            self._circuit_breaker._status.mark_success()
                            self._circuit_breaker._save_state()
                        except Exception as e:
                            logger.warning(f"Failed to mark circuit success: {e}")

                    async with self._stats_lock:
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
                    execution_time = trio.current_time() - start_time

                    # Record failure
                    if self._circuit_breaker:
                        try:
                            self._circuit_breaker._status.mark_failure()
                            self._circuit_breaker._save_state()
                        except Exception as circuit_err:
                            logger.warning(f"Failed to mark circuit failure: {circuit_err}")

                    async with self._stats_lock:
                        self._failed_executions += 1
                        self._active_tasks -= 1

                    # Wrap exception if needed
                    if not isinstance(e, BulkheadError):
                        wrapped_error = BulkheadError(f"Execution failed: {e}")
                        wrapped_error.__cause__ = e
                        error = wrapped_error
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

        # Check if we timed out
        if cancel_scope.cancelled_caught:
            async with self._stats_lock:
                self._rejected_executions += 1
                if self._active_tasks > 0:
                    self._active_tasks -= 1
            raise BulkheadTimeoutError(
                f"Bulkhead '{self.name}' operation timed out after {self.config.timeout_seconds} seconds"
            )

    @asynccontextmanager
    async def context(self):
        """
        Context manager for bulkhead operations.

        Example:
            async with bulkhead.context():
                result = await bulkhead.execute(my_function, arg1, arg2)
        """
        yield self

    async def get_state(self) -> BulkheadState:
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

    async def get_stats(self) -> dict[str, Any]:
        """Get statistics for the bulkhead."""
        async with self._stats_lock:
            stats = {
                "name": self.name,
                "state": (await self.get_state()).value,
                "total_executions": self._total_executions,
                "successful_executions": self._successful_executions,
                "failed_executions": self._failed_executions,
                "rejected_executions": self._rejected_executions,
                "active_tasks": self._active_tasks,
                "max_concurrent_calls": self.config.max_concurrent_calls,
                "max_queue_size": self.config.max_queue_size,
            }

            # Add circuit breaker info if enabled
            if self._circuit_breaker:
                stats["circuit_breaker_enabled"] = True
                stats["circuit_status"] = self._circuit_breaker.status.value

            return stats

    async def reset_stats(self) -> None:
        """Reset bulkhead statistics."""
        async with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0

    async def is_healthy(self) -> bool:
        """Check if bulkhead is healthy."""
        state = await self.get_state()
        return state in (BulkheadState.HEALTHY, BulkheadState.DEGRADED)


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
        self._lock = trio.Lock()
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
    async def context(self):
        """Context manager for bulkhead manager."""
        yield self


def with_bulkhead(
    bulkhead: Bulkhead,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to execute an async function through a bulkhead.

    Example:
        @with_bulkhead(my_bulkhead)
        async def query_database(query):
            return await db.execute(query)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            result = await bulkhead.execute(func, *args, **kwargs)
            if not result.success:
                raise result.error or BulkheadError("Execution failed")
            return result.result

        return wrapper

    return decorator
