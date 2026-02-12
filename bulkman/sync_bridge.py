"""Synchronous bridge for Bulkman to enable threading-based usage."""

from __future__ import annotations

import atexit
import concurrent.futures
import logging
import threading
from typing import Any, Callable

import trio
from resilient_circuit.storage import CircuitBreakerStorage

from bulkman.config import BulkheadConfig, ExecutionResult
from bulkman.core import Bulkhead
from bulkman.exceptions import BulkheadError

logger = logging.getLogger("bulkman.sync")


class TrioThread:
    """Manages a background thread running a Trio event loop."""

    def __init__(self) -> None:
        self._portal: trio.lowlevel.TrioToken | None = None
        self._thread: threading.Thread | None = None
        self._started: threading.Event = threading.Event()
        self._stopping: threading.Event = threading.Event()
        self._lock: threading.Lock = threading.Lock()
        self._cancel_scope: trio.CancelScope | None = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return

            def trio_thread_main() -> None:
                async def trio_main() -> None:
                    self._portal = trio.lowlevel.current_trio_token()
                    self._started.set()

                    with trio.CancelScope() as cancel_scope:
                        self._cancel_scope = cancel_scope
                        await trio.sleep_forever()

                try:
                    trio.run(trio_main)
                except Exception as e:
                    logger.error("Trio thread crashed: %s", e)

            self._thread = threading.Thread(target=trio_thread_main, daemon=True, name="TrioRunner")
            self._thread.start()
            _ = self._started.wait(timeout=5.0)
            if not self._portal:
                raise RuntimeError("Failed to start Trio thread")

            logger.info("Trio runner thread started")

    def stop(self) -> None:
        with self._lock:
            if self._thread is None:
                return

            self._stopping.set()
            if self._cancel_scope is not None:
                self._cancel_scope.cancel()

            self._thread.join(timeout=5.0)
            self._thread = None
            self._portal = None
            logger.info("Trio runner thread stopped")

    def run_sync(self, async_fn: Callable[..., Any], *args: Any) -> Any:
        if not self._portal:
            raise RuntimeError("Trio thread not started")

        return trio.from_thread.run(async_fn, *args, trio_token=self._portal)


_trio_thread: TrioThread | None = None
_trio_lock = threading.Lock()


def _get_trio_thread() -> TrioThread:
    global _trio_thread  # noqa: PLW0603
    with _trio_lock:
        if _trio_thread is None:
            _trio_thread = TrioThread()
            _trio_thread.start()
            _ = atexit.register(_trio_thread.stop)
        return _trio_thread


class BulkheadSync:
    """Synchronous wrapper for Bulkman's async Bulkhead."""

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ) -> None:
        self.config: BulkheadConfig = config
        self.name: str = config.name
        self._trio_thread: TrioThread = _get_trio_thread()
        self._bulkhead: Bulkhead | None = None
        self._executor: concurrent.futures.ThreadPoolExecutor = (
            concurrent.futures.ThreadPoolExecutor(
                max_workers=config.max_concurrent_calls,
                thread_name_prefix=f"Bulkhead-{config.name}",
            )
        )

        _circuit_storage = circuit_storage

        async def create_bulkhead() -> None:
            self._bulkhead = Bulkhead(config, _circuit_storage)

        self._trio_thread.run_sync(create_bulkhead)
        logger.info("BulkheadSync '%s' initialized", self.name)

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> concurrent.futures.Future[ExecutionResult]:
        future: concurrent.futures.Future[ExecutionResult] = concurrent.futures.Future()

        def run_in_executor() -> None:
            try:
                bulkhead = self._bulkhead
                if bulkhead is None:
                    raise BulkheadError("Bulkhead not initialized")

                async def async_wrapper() -> ExecutionResult:
                    assert bulkhead is not None
                    return await bulkhead.execute(func, *args, **kwargs)

                execution_result: ExecutionResult = self._trio_thread.run_sync(async_wrapper)
                future.set_result(execution_result)

            except Exception as e:
                if not isinstance(e, BulkheadError):
                    e = BulkheadError(f"Bulkhead execution failed: {e}")
                future.set_exception(e)

        _ = self._executor.submit(run_in_executor)
        return future

    def get_stats(self) -> dict[str, Any]:
        bulkhead = self._bulkhead
        if bulkhead is None:
            raise BulkheadError("Bulkhead not initialized")

        async def async_get_stats() -> dict[str, Any]:
            assert bulkhead is not None
            return await bulkhead.get_stats()

        result: dict[str, Any] = self._trio_thread.run_sync(async_get_stats)
        return result

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)
