"""
Synchronous bridge for Bulkman to enable threading-based usage.

This module provides a synchronous wrapper around Bulkman's async API,
allowing it to be used in threading-based code (like Highway's orchestrator).

The background event loop runs on AnyIO's asyncio backend.
"""

from __future__ import annotations

import atexit
import concurrent.futures
import logging
import threading
from typing import Any, Callable

import anyio
from anyio.abc import BlockingPortal
from resilient_circuit.storage import CircuitBreakerStorage

from bulkman.config import BulkheadConfig, ExecutionResult
from bulkman.core import Bulkhead
from bulkman.exceptions import BulkheadError, BulkheadFullError

logger = logging.getLogger("bulkman.sync")


class PortalThread:
    """Manages a background thread running an AnyIO (asyncio) event loop."""

    def __init__(self):
        self._portal: BlockingPortal | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the portal thread if not already running."""
        with self._lock:
            if self._thread is not None:
                return  # Already running

            def portal_main() -> None:
                async def main() -> None:
                    async with BlockingPortal() as portal:
                        self._portal = portal
                        self._started.set()
                        # Keep the loop alive until stop() is called.
                        await portal.sleep_until_stopped()

                try:
                    anyio.run(main, backend="asyncio")
                except Exception as e:
                    logger.error("Portal thread crashed: %s", e)

            self._thread = threading.Thread(target=portal_main, daemon=True, name="PortalRunner")
            self._thread.start()
            self._started.wait(timeout=5.0)
            if self._portal is None:
                raise RuntimeError("Failed to start portal thread")

            logger.info("Portal runner thread started")

    def stop(self) -> None:
        """Stop the portal thread."""
        with self._lock:
            if self._thread is None:
                return

            portal = self._portal
            self._portal = None
            if portal is not None:
                try:
                    portal.call(portal.stop)
                except Exception:
                    pass

            self._thread.join(timeout=5.0)
            self._thread = None
            logger.info("Portal runner thread stopped")

    def run_sync(self, async_fn: Callable, *args: Any) -> Any:
        """Run an async function from a sync context."""
        portal = self._portal
        if portal is None:
            raise RuntimeError("Portal thread not started")

        return portal.call(async_fn, *args)


# Global portal thread instance
_portal_thread: PortalThread | None = None
_portal_lock = threading.Lock()


def _get_portal_thread() -> PortalThread:
    """Get or create the global portal thread."""
    global _portal_thread
    with _portal_lock:
        if _portal_thread is None:
            _portal_thread = PortalThread()
            _portal_thread.start()
            atexit.register(_portal_thread.stop)
        return _portal_thread


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
        self._portal_thread = _get_portal_thread()
        self._bulkhead: Bulkhead | None = None
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.max_concurrent_calls,
            thread_name_prefix=f"Bulkhead-{config.name}",
        )

        # Queue tracking for rejection (parity with BulkheadThreading).
        # _in_flight_count tracks tasks submitted but not completed
        # (both executing and queued).
        self._in_flight_count = 0
        self._in_flight_lock = threading.Lock()
        # Work futures tracked so shutdown(wait=True, timeout=...) can honor
        # the timeout, and so cancellation by the executor still releases
        # the admitted slot.
        self._work_futures: set[concurrent.futures.Future] = set()
        self._work_futures_lock = threading.Lock()

        # Create async bulkhead in the portal thread
        async def create_bulkhead():
            self._bulkhead = Bulkhead(config, circuit_storage)

        self._portal_thread.run_sync(create_bulkhead)
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

        Raises:
            BulkheadFullError: If the bulkhead is at capacity
        """
        # Reject when at capacity (max_concurrent_calls + max_queue_size)
        total_capacity = self.config.max_concurrent_calls + self.config.max_queue_size
        with self._in_flight_lock:
            if self._in_flight_count >= total_capacity:
                raise BulkheadFullError(
                    f"BulkheadSync '{self.name}' is at capacity "
                    f"({self._in_flight_count}/{total_capacity})"
                )
            self._in_flight_count += 1

        future: concurrent.futures.Future[ExecutionResult] = concurrent.futures.Future()

        def run_in_executor():
            """Run the function via async bulkhead and set result."""
            bulkhead = self._bulkhead
            assert bulkhead is not None
            try:
                # Execute via async bulkhead from sync context
                async def async_wrapper():
                    return await bulkhead.execute(func, *args, **kwargs)

                execution_result = self._portal_thread.run_sync(async_wrapper)
                future.set_result(execution_result)

            except Exception as e:
                # Wrap in ExecutionResult for consistency
                if not isinstance(e, BulkheadError):
                    e = BulkheadError(f"Bulkhead execution failed: {e}")
                future.set_exception(e)
            except BaseException as e:
                # BaseException (KeyboardInterrupt, SystemExit, CancelledError)
                # must still resolve the future so the caller never hangs;
                # keep the original exception type.
                future.set_exception(e)

        # Submit to thread pool
        try:
            work_future = self._executor.submit(run_in_executor)
            # The admitted slot is released when the WORK future completes
            # (including when the executor cancels it at shutdown - the
            # run_in_executor body never runs in that case).
            with self._work_futures_lock:
                self._work_futures.add(work_future)
            work_future.add_done_callback(self._on_work_done)
        except BaseException:
            # Submission failed (e.g. executor shut down): release the slot
            with self._in_flight_lock:
                self._in_flight_count -= 1
            raise
        return future

    def _on_work_done(self, work_future: concurrent.futures.Future) -> None:
        """Release the admitted slot and forget the work future."""
        with self._in_flight_lock:
            if self._in_flight_count > 0:
                self._in_flight_count -= 1
        with self._work_futures_lock:
            self._work_futures.discard(work_future)

    def get_stats(self) -> dict[str, Any]:
        """Get bulkhead statistics."""
        bulkhead = self._bulkhead
        assert bulkhead is not None

        async def async_get_stats():
            return await bulkhead.get_stats()

        return self._portal_thread.run_sync(async_get_stats)

    def get_state(self) -> Any:
        """Get the current bulkhead state."""
        bulkhead = self._bulkhead
        assert bulkhead is not None

        async def async_get_state():
            return await bulkhead.get_state()

        return self._portal_thread.run_sync(async_get_state)

    def is_healthy(self) -> bool:
        """Check if the bulkhead is healthy."""
        bulkhead = self._bulkhead
        assert bulkhead is not None

        async def async_is_healthy():
            return await bulkhead.is_healthy()

        return self._portal_thread.run_sync(async_is_healthy)

    def reset_stats(self) -> None:
        """Reset bulkhead statistics."""
        bulkhead = self._bulkhead
        assert bulkhead is not None

        async def async_reset_stats():
            await bulkhead.reset_stats()

        self._portal_thread.run_sync(async_reset_stats)

    def shutdown(self, wait: bool = True, timeout: float = 5.0) -> None:
        """Shutdown the bulkhead.

        Args:
            wait: If True, wait for pending tasks to complete
            timeout: Maximum time to wait when wait=True; after it expires,
                     queued tasks are cancelled and running tasks abandoned
        """

        async def async_shutdown():
            if self._bulkhead:
                await self._bulkhead.shutdown()

        try:
            self._portal_thread.run_sync(async_shutdown)
        except RuntimeError:
            # Portal already stopped; nothing left to shut down
            pass
        finally:
            if wait and timeout is not None:
                with self._work_futures_lock:
                    pending = list(self._work_futures)
                if pending:
                    _, still_pending = concurrent.futures.wait(pending, timeout=timeout)
                    if still_pending:
                        logger.warning(
                            "BulkheadSync '%s' shutdown timed out; " "%d task(s) still running",
                            self.name,
                            len(still_pending),
                        )
                        wait = False
            self._executor.shutdown(wait=wait, cancel_futures=not wait)
