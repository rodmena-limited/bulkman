"""Core bulkhead pattern implementation using Trio."""

from __future__ import annotations

import inspect
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from fractions import Fraction
from functools import wraps
from typing import Any, Callable

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


class Bulkhead:
    """Bulkhead pattern using Trio for structured concurrency."""

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ) -> None:
        self.config: BulkheadConfig = config
        self.name: str = config.name

        self._semaphore: trio.Semaphore = trio.Semaphore(config.max_concurrent_calls)
        self._send_channel: trio.MemorySendChannel[Any] | None = None
        self._receive_channel: trio.MemoryReceiveChannel[Any] | None = None
        self._task_nursery: trio.Nursery | None = None

        self._circuit_breaker: CircuitProtectorPolicy | None = None
        if config.circuit_breaker_enabled:
            self._circuit_breaker = CircuitProtectorPolicy(
                resource_key=config.name,
                storage=circuit_storage,
                cooldown=timedelta(seconds=config.isolation_duration),
                failure_limit=Fraction(config.failure_threshold, config.failure_threshold),
                success_limit=Fraction(config.success_threshold, config.success_threshold),
                on_status_change=self._on_circuit_status_change,
            )

        self._total_executions: int = 0
        self._successful_executions: int = 0
        self._failed_executions: int = 0
        self._rejected_executions: int = 0
        self._active_tasks: int = 0
        self._stats_lock: trio.Lock = trio.Lock()

        self._running: bool = False
        self._cancel_scope: trio.CancelScope | None = None

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
        logger.info(
            "Bulkhead '%s' circuit breaker changed: %s -> %s",
            self.name,
            old_status.value,
            new_status.value,
        )

    async def _check_circuit(self) -> None:
        if self._circuit_breaker:
            try:
                await trio.to_thread.run_sync(self._circuit_breaker._status.validate_execution)
            except ProtectedCallError:
                async with self._stats_lock:
                    self._rejected_executions += 1
                raise BulkheadCircuitOpenError(
                    f"Bulkhead '{self.name}' circuit is open - requests are blocked"
                )

    async def _run_func(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        if kwargs:
            return await trio.to_thread.run_sync(lambda: func(*args, **kwargs))
        return await trio.to_thread.run_sync(func, *args)

    async def _record_circuit_success(self) -> None:
        if self._circuit_breaker:
            try:
                self._circuit_breaker._status.mark_success()
                self._circuit_breaker._save_state()
            except Exception as e:
                logger.warning("Failed to mark circuit success: %s", e)

    async def _record_circuit_failure(self) -> None:
        if self._circuit_breaker:
            try:
                self._circuit_breaker._status.mark_failure()
                self._circuit_breaker._save_state()
            except Exception as circuit_err:
                logger.warning("Failed to mark circuit failure: %s", circuit_err)

    async def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult:
        await self._check_circuit()

        submission_time = trio.current_time()
        execution_id = str(uuid.uuid4())
        timeout = self.config.timeout_seconds if self.config.timeout_seconds else float("inf")

        with trio.move_on_after(timeout) as cancel_scope:
            async with self._semaphore:
                async with self._stats_lock:
                    self._total_executions += 1
                    self._active_tasks += 1

                start_time = trio.current_time()
                queued_time = start_time - submission_time

                try:
                    result = await self._run_func(func, *args, **kwargs)
                    execution_time = trio.current_time() - start_time

                    await self._record_circuit_success()

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

                    await self._record_circuit_failure()

                    async with self._stats_lock:
                        self._failed_executions += 1
                        self._active_tasks -= 1

                    if not isinstance(e, BulkheadError):
                        wrapped_error = BulkheadError(f"Execution failed: {e}")
                        wrapped_error.__cause__ = e
                        error: Exception = wrapped_error
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

        if cancel_scope.cancelled_caught:
            async with self._stats_lock:
                self._rejected_executions += 1
                if self._active_tasks > 0:
                    self._active_tasks -= 1
            raise BulkheadTimeoutError(
                f"Bulkhead '{self.name}' operation timed out after {self.config.timeout_seconds} seconds"
            )

        raise AssertionError("Unreachable: execute must return or raise")

    @asynccontextmanager
    async def context(self) -> AsyncIterator[Bulkhead]:
        yield self

    async def get_state(self) -> BulkheadState:
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
        async with self._stats_lock:
            stats: dict[str, Any] = {
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

            if self._circuit_breaker:
                stats["circuit_breaker_enabled"] = True
                stats["circuit_status"] = self._circuit_breaker.status.value

            return stats

    async def reset_stats(self) -> None:
        async with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0

    async def is_healthy(self) -> bool:
        state = await self.get_state()
        return state in (BulkheadState.HEALTHY, BulkheadState.DEGRADED)


class BulkheadManager:
    """Manages multiple bulkheads for different system components."""

    def __init__(
        self,
        circuit_storage: CircuitBreakerStorage | None = None,
    ) -> None:
        self._bulkheads: dict[str, Bulkhead] = {}
        self._lock: trio.Lock = trio.Lock()
        self._circuit_storage: CircuitBreakerStorage | None = circuit_storage

    async def create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        async with self._lock:
            if config.name in self._bulkheads:
                raise ValueError(f"Bulkhead with name '{config.name}' already exists")

            bulkhead = Bulkhead(config, circuit_storage=self._circuit_storage)
            self._bulkheads[config.name] = bulkhead
            return bulkhead

    async def get_bulkhead(self, name: str) -> Bulkhead | None:
        async with self._lock:
            return self._bulkheads.get(name)

    async def get_or_create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        async with self._lock:
            if config.name in self._bulkheads:
                return self._bulkheads[config.name]

            bulkhead = Bulkhead(config, circuit_storage=self._circuit_storage)
            self._bulkheads[config.name] = bulkhead
            return bulkhead

    async def execute_in_bulkhead(
        self,
        bulkhead_name: str,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult:
        bulkhead = await self.get_bulkhead(bulkhead_name)
        if not bulkhead:
            raise ValueError(f"Bulkhead '{bulkhead_name}' not found")
        return await bulkhead.execute(func, *args, **kwargs)

    async def get_all_stats(self) -> dict[str, dict[str, Any]]:
        async with self._lock:
            stats: dict[str, dict[str, Any]] = {}
            for name, bulkhead in self._bulkheads.items():
                stats[name] = await bulkhead.get_stats()
            return stats

    async def get_health_status(self) -> dict[str, bool]:
        async with self._lock:
            status: dict[str, bool] = {}
            for name, bulkhead in self._bulkheads.items():
                status[name] = await bulkhead.is_healthy()
            return status

    @asynccontextmanager
    async def context(self) -> AsyncIterator[BulkheadManager]:
        yield self


def with_bulkhead(
    bulkhead: Bulkhead,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = await bulkhead.execute(func, *args, **kwargs)
            if not result.success:
                raise result.error or BulkheadError("Execution failed")
            return result.result

        return wrapper

    return decorator
