"""Pure threading-based bulkhead implementation."""

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
    """Pure threading-based bulkhead for sync workloads."""

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ) -> None:
        self.config: BulkheadConfig = config
        self.name: str = config.name

        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_calls,
            thread_name_prefix=f"Bulkhead-{config.name}",
        )

        self._in_flight_count: int = 0
        self._in_flight_lock: threading.Lock = threading.Lock()

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
        self._timed_out_executions: int = 0
        self._active_tasks: int = 0
        self._stats_lock: threading.Lock = threading.Lock()

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
        logger.info(
            "BulkheadThreading '%s' circuit breaker: %s -> %s",
            self.name,
            old_status.value,
            new_status.value,
        )

    def _check_circuit(self) -> None:
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
        total_capacity = self.config.max_concurrent_calls + self.config.max_queue_size
        with self._in_flight_lock:
            if self._in_flight_count >= total_capacity:
                with self._stats_lock:
                    self._rejected_executions += 1
                raise BulkheadFullError(
                    f"BulkheadThreading '{self.name}' is at capacity ({self._in_flight_count}/{total_capacity})"
                )
            self._in_flight_count += 1

    def _decrement_in_flight(self, _future: Future[ExecutionResult] | None = None) -> None:
        with self._in_flight_lock:
            if self._in_flight_count > 0:
                self._in_flight_count -= 1

    def _record_circuit_success(self, circuit_breaker: CircuitProtectorPolicy | None) -> None:
        if circuit_breaker:
            try:
                circuit_breaker._status.mark_success()
                circuit_breaker._save_state()
            except Exception as e:
                logger.warning("Failed to mark circuit success: %s", e)

    def _record_circuit_failure(self, circuit_breaker: CircuitProtectorPolicy | None) -> None:
        if circuit_breaker:
            try:
                circuit_breaker._status.mark_failure()
                circuit_breaker._save_state()
            except Exception as circuit_err:
                logger.warning("Failed to mark circuit failure: %s", circuit_err)

    def _build_execution_wrapper(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        submission_time: float,
        execution_id: str,
        circuit_breaker: CircuitProtectorPolicy | None,
    ) -> Callable[[], ExecutionResult]:
        def wrapper() -> ExecutionResult:
            start_time = time.monotonic()
            queued_time = start_time - submission_time

            if self.config.timeout_seconds and queued_time > self.config.timeout_seconds:
                with self._stats_lock:
                    self._timed_out_executions += 1
                return ExecutionResult(
                    success=False,
                    result=None,
                    error=BulkheadTimeoutError(
                        f"Timeout waiting for execution slot in '{self.name}' (queued for {queued_time:.2f}s)"
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
                execution_time = time.monotonic() - start_time

                self._record_circuit_success(circuit_breaker)

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

                self._record_circuit_failure(circuit_breaker)

                with self._stats_lock:
                    self._failed_executions += 1
                    self._active_tasks -= 1

                if not isinstance(e, BulkheadError):
                    wrapped = BulkheadError(f"Execution failed: {e}")
                    wrapped.__cause__ = e
                    error: Exception = wrapped
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

        return wrapper

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future[ExecutionResult]:
        self._check_circuit()

        submission_time = time.monotonic()
        execution_id = str(uuid.uuid4())
        ctx = contextvars.copy_context()
        circuit_breaker = self._circuit_breaker

        self._check_queue_capacity()

        wrapper = self._build_execution_wrapper(
            func, args, kwargs, submission_time, execution_id, circuit_breaker
        )

        def context_wrapper() -> ExecutionResult:
            return ctx.run(wrapper)

        try:
            future = self._executor.submit(context_wrapper)
            future.add_done_callback(self._decrement_in_flight)
            return future
        except BaseException:
            self._decrement_in_flight()
            raise

    def execute_with_timeout(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> ExecutionResult:
        effective_timeout = timeout if timeout is not None else self.config.timeout_seconds

        future = self.execute(func, *args, **kwargs)

        try:
            return future.result(timeout=effective_timeout)
        except concurrent.futures.TimeoutError:
            _ = future.cancel()

            with self._stats_lock:
                self._timed_out_executions += 1

            raise BulkheadTimeoutError(
                f"BulkheadThreading '{self.name}' execution timed out after {effective_timeout}s"
            )

    def get_state(self) -> BulkheadState:
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
        with self._stats_lock:
            stats: dict[str, Any] = {
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
        with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0
            self._timed_out_executions = 0

    def is_healthy(self) -> bool:
        state = self.get_state()
        return state in (BulkheadState.HEALTHY, BulkheadState.DEGRADED)

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        logger.info("Shutting down BulkheadThreading '%s' (wait=%s)", self.name, wait)

        if timeout is not None:
            try:
                self._executor.shutdown(wait=wait, cancel_futures=not wait)
            except TypeError:
                self._executor.shutdown(wait=wait)
        else:
            self._executor.shutdown(wait=wait)

        self._circuit_breaker = None

        logger.info("BulkheadThreading '%s' shutdown complete", self.name)
