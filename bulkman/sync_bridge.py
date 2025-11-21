"""
Synchronous bridge for Bulkman to enable threading-based usage.

This module provides a synchronous wrapper around Bulkman's async API,
allowing it to be used in threading-based code (like Highway's orchestrator).
"""

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

    def __init__(self):
        self._portal: trio.lowlevel.TrioToken | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._stopping = threading.Event()
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the Trio thread if not already running."""
        with self._lock:
            if self._thread is not None:
                return  # Already running

            def trio_thread_main():
                async def trio_main():
                    # Store portal for cross-thread calls
                    self._portal = trio.lowlevel.current_trio_token()
                    self._started.set()

                    # Keep trio running until stopped
                    with trio.CancelScope() as cancel_scope:
                        self._cancel_scope = cancel_scope
                        await trio.sleep_forever()

                try:
                    trio.run(trio_main)
                except Exception as e:
                    logger.error(f"Trio thread crashed: {e}")

            self._thread = threading.Thread(target=trio_thread_main, daemon=True, name="TrioRunner")
            self._thread.start()
            self._started.wait(timeout=5.0)
            if not self._portal:
                raise RuntimeError("Failed to start Trio thread")

            logger.info("Trio runner thread started")

    def stop(self) -> None:
        """Stop the Trio thread."""
        with self._lock:
            if self._thread is None:
                return

            self._stopping.set()
            if hasattr(self, "_cancel_scope"):
                self._cancel_scope.cancel()

            self._thread.join(timeout=5.0)
            self._thread = None
            self._portal = None
            logger.info("Trio runner thread stopped")

    def run_sync(self, async_fn: Callable, *args: Any) -> Any:
        """Run an async function from a sync context."""
        if not self._portal:
            raise RuntimeError("Trio thread not started")

        return trio.from_thread.run(async_fn, *args, trio_token=self._portal)


# Global Trio thread instance
_trio_thread: TrioThread | None = None
_trio_lock = threading.Lock()


def _get_trio_thread() -> TrioThread:
    """Get or create the global Trio thread."""
    global _trio_thread
    with _trio_lock:
        if _trio_thread is None:
            _trio_thread = TrioThread()
            _trio_thread.start()
            atexit.register(_trio_thread.stop)
        return _trio_thread


class BulkheadSync:
    """
    Synchronous wrapper for Bulkman's async Bulkhead.

    Provides threading-based API compatible with concurrent.futures.
    """

    def __init__(
        self,
        config: BulkheadConfig,
        circuit_storage: CircuitBreakerStorage | None = None,
    ):
        self.config = config
        self.name = config.name
        self._trio_thread = _get_trio_thread()
        self._bulkhead: Bulkhead | None = None
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.max_concurrent_calls,
            thread_name_prefix=f"Bulkhead-{config.name}",
        )

        # Create async bulkhead in Trio thread
        async def create_bulkhead():
            self._bulkhead = Bulkhead(config, circuit_storage)

        self._trio_thread.run_sync(create_bulkhead)
        logger.info(f"BulkheadSync '{self.name}' initialized")

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> concurrent.futures.Future[ExecutionResult]:
        """
        Execute a function through the bulkhead.

        Returns:
            concurrent.futures.Future containing ExecutionResult
        """
        future: concurrent.futures.Future[ExecutionResult] = concurrent.futures.Future()

        def run_in_executor():
            """Run the function via Trio bulkhead and set result."""
            try:
                # Execute via async bulkhead from sync context
                async def async_wrapper():
                    return await self._bulkhead.execute(func, *args, **kwargs)

                execution_result = self._trio_thread.run_sync(async_wrapper)
                future.set_result(execution_result)

            except Exception as e:
                # Wrap in ExecutionResult for consistency
                if not isinstance(e, BulkheadError):
                    e = BulkheadError(f"Bulkhead execution failed: {e}")
                future.set_exception(e)

        # Submit to thread pool
        self._executor.submit(run_in_executor)
        return future

    def get_stats(self) -> dict[str, Any]:
        """Get bulkhead statistics."""
        async def async_get_stats():
            return await self._bulkhead.get_stats()

        return self._trio_thread.run_sync(async_get_stats)

    def shutdown(self, wait: bool = True, timeout: float = 5.0) -> None:
        """Shutdown the bulkhead."""
        async def async_shutdown():
            if self._bulkhead:
                await self._bulkhead.shutdown()

        try:
            self._trio_thread.run_sync(async_shutdown)
        finally:
            self._executor.shutdown(wait=wait, timeout=timeout)
