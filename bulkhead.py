import asyncio
import concurrent.futures
import functools
import logging
import queue
import threading
import time
import uuid
from collections.abc import Callable, Coroutine, Iterator
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    TypeVar,
)

# Type variable for generic return type
T = TypeVar("T")

logger = logging.getLogger("BulkheadPattern")


class BulkheadState(Enum):
    """Enum representing the state of a bulkhead"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    ISOLATED = "isolated"
    FAILED = "failed"


@dataclass
class BulkheadConfig:
    """Configuration for a bulkhead"""

    name: str
    max_concurrent_calls: int = 10
    max_queue_size: int = 100
    timeout_seconds: float | None = None
    failure_threshold: int = 5
    success_threshold: int = 3
    isolation_duration: float = 30.0  # seconds
    # New configuration options
    circuit_breaker_enabled: bool = True
    health_check_interval: float = 5.0


@dataclass
class ExecutionResult:
    """Result of a function execution through bulkhead"""

    success: bool
    result: Any
    error: Exception | None
    execution_time: float
    bulkhead_name: str
    # New fields
    queued_time: float = 0.0
    execution_id: str = ""


class BulkheadError(Exception):
    """Base exception for all bulkhead-related errors"""


class BulkheadIsolationError(BulkheadError):
    """Exception raised when bulkhead is isolated"""


class BulkheadTimeoutError(BulkheadError):
    """Exception raised when bulkhead operation times out"""


class BulkheadFullError(BulkheadError):
    """Exception raised when bulkhead queue is full"""


class BulkheadCircuitOpenError(BulkheadError):
    """Exception raised when bulkhead circuit is open"""


@dataclass
class Task:
    """Represents a task to be executed by the bulkhead"""

    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    future: Future[Any]
    submission_time: float
    execution_id: str
    metadata: dict[str, Any] = field(default_factory=dict)


class CircuitBreaker:
    """Implements circuit breaker pattern for the bulkhead"""

    def __init__(
        self,
        failure_threshold: int,
        success_threshold: int,
        isolation_duration: float,
        name: str,
    ):
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.isolation_duration = isolation_duration
        self.name = name

        self._failure_count = 0
        self._success_count = 0
        self._state = BulkheadState.HEALTHY
        self._isolation_end_time: float | None = None
        self._lock = threading.RLock()
        self._last_state_change = time.time()

    def record_success(self) -> None:
        """Record a successful execution"""
        with self._lock:
            self._success_count += 1
            self._failure_count = 0

            if (
                self._state == BulkheadState.DEGRADED
                and self._success_count >= self.success_threshold
            ):
                self._transition_to(BulkheadState.HEALTHY)
                logger.info(
                    f"Circuit breaker '{self.name}' closed - returned to healthy state",
                )

    def record_failure(self) -> None:
        """Record a failed execution"""
        with self._lock:
            self._failure_count += 1
            self._success_count = 0

            if self._failure_count >= self.failure_threshold:
                if self._state != BulkheadState.ISOLATED:
                    self._transition_to(BulkheadState.ISOLATED)
                    logger.warning(
                        f"Circuit breaker '{self.name}' opened due to {self._failure_count} "
                        f"consecutive failures. Isolated for {self.isolation_duration}s",
                    )
            elif self._state == BulkheadState.HEALTHY and self._failure_count > 0:
                self._transition_to(BulkheadState.DEGRADED)
                logger.warning(
                    f"Circuit breaker '{self.name}' degraded due to failures",
                )

    def _transition_to(self, new_state: BulkheadState) -> None:
        """Transition to a new state"""
        self._state = new_state
        self._last_state_change = time.time()

        if new_state == BulkheadState.ISOLATED:
            self._isolation_end_time = time.time() + self.isolation_duration

    def is_request_allowed(self) -> bool:
        """Check if requests are allowed in current state"""
        with self._lock:
            if self._state == BulkheadState.ISOLATED:
                # Check if isolation period has ended
                if self._isolation_end_time and time.time() >= self._isolation_end_time:
                    # Auto-close circuit after isolation period
                    self._transition_to(BulkheadState.HEALTHY)
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info(
                        f"Circuit breaker '{self.name}' auto-closed after isolation period",
                    )
                    return True
                return False
            return True

    def get_state(self) -> BulkheadState:
        """Get current circuit breaker state"""
        with self._lock:
            return self._state

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics"""
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "isolation_end_time": self._isolation_end_time,
                "last_state_change": self._last_state_change,
            }


class Bulkhead:
    """
    Implements the bulkhead pattern to isolate function executions
    and prevent cascading failures.
    """

    def __init__(self, config: BulkheadConfig):
        self.config = config
        self.name = config.name

        # Execution control
        self._semaphore = threading.BoundedSemaphore(config.max_concurrent_calls)
        self._task_queue: queue.Queue[Task | None] = queue.Queue(
            maxsize=config.max_queue_size,
        )
        self._worker_threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._health_check_thread: threading.Thread | None = None

        # State management
        self._circuit_breaker = (
            CircuitBreaker(
                failure_threshold=config.failure_threshold,
                success_threshold=config.success_threshold,
                isolation_duration=config.isolation_duration,
                name=config.name,
            )
            if config.circuit_breaker_enabled
            else None
        )

        # Statistics
        self._total_executions = 0
        self._successful_executions = 0
        self._failed_executions = 0
        self._rejected_executions = 0
        self._stats_lock = threading.RLock()

        # Active task tracking
        self._active_tasks: dict[str, Task] = {}
        self._active_tasks_lock = threading.RLock()

        # Start worker threads and health monitoring
        self._start_workers()
        self._start_health_monitoring()

        logger.info(
            f"Bulkhead '{self.name}' initialized with {config.max_concurrent_calls} "
            f"concurrent calls and queue size {config.max_queue_size}",
        )

    def _start_workers(self) -> None:
        """Start worker threads to process queued tasks"""

        def worker() -> None:
            while not self._stop_event.is_set():
                try:
                    # Try non-blocking get first (drain pattern)
                    # This prevents 1-second delays when queue has tasks
                    try:
                        task = self._task_queue.get_nowait()
                    except queue.Empty:
                        # Queue is empty, block with timeout
                        task = self._task_queue.get(timeout=1.0)

                    if task is None:  # Poison pill
                        self._task_queue.task_done()
                        break

                    self._process_task(task)
                    self._task_queue.task_done()

                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error("Worker thread error in bulkhead '%s': %s", self.name, e)
                    # Brief pause to prevent tight error loops
                    time.sleep(0.1)

        # Create worker threads
        for i in range(self.config.max_concurrent_calls):
            thread = threading.Thread(
                target=worker,
                name=f"BulkheadWorker-{self.name}-{i}",
                daemon=True,
            )
            thread.start()
            self._worker_threads.append(thread)

    def _start_health_monitoring(self) -> None:
        """Start health monitoring thread"""
        if self.config.health_check_interval <= 0:
            return

        def health_monitor() -> None:
            while not self._stop_event.is_set():
                try:
                    self._check_health()
                except Exception as e:
                    logger.error(
                        f"Health monitoring error in bulkhead '{self.name}': {e}",
                    )

                # Wait for next health check
                self._stop_event.wait(self.config.health_check_interval)

        self._health_check_thread = threading.Thread(
            target=health_monitor,
            name=f"BulkheadHealth-{self.name}",
            daemon=True,
        )
        self._health_check_thread.start()

    def _check_health(self) -> None:
        """Perform health checks"""
        # Check for stuck tasks (tasks that have been running too long)
        if self.config.timeout_seconds:
            with self._active_tasks_lock:
                current_time = time.time()
                stuck_tasks = []
                for task_id, task in self._active_tasks.items():
                    if current_time - task.submission_time > self.config.timeout_seconds * 2:
                        stuck_tasks.append(task_id)

                for task_id in stuck_tasks:
                    try:
                        task = self._active_tasks.pop(task_id)
                        if task and not task.future.done():
                            logger.warning(
                                f"Terminating stuck task {task_id} in bulkhead '{self.name}'",
                            )
                            task.future.set_exception(
                                BulkheadTimeoutError(
                                    f"Task {task_id} exceeded maximum execution time",
                                ),
                            )
                    except KeyError:
                        # Task was already removed, continue
                        continue

    def _process_task(self, task: Task) -> None:
        """Process a single task from the queue"""
        start_time = time.time()
        execution_id = task.execution_id

        # Track active task
        with self._active_tasks_lock:
            self._active_tasks[execution_id] = task

        try:
            # Acquire semaphore to limit concurrent executions
            if not self._semaphore.acquire(timeout=self.config.timeout_seconds or 30):
                msg = f"Bulkhead '{self.name}' could not acquire execution slot"
                raise BulkheadTimeoutError(
                    msg,
                )

            try:
                # Execute the task with timeout
                with self._execute_with_timeout(
                    task.func,
                    task.args,
                    task.kwargs,
                ) as result:
                    execution_time = time.time() - start_time
                    queued_time = start_time - task.submission_time

                    # Record success
                    if self._circuit_breaker:
                        self._circuit_breaker.record_success()

                    # Update statistics
                    with self._stats_lock:
                        self._successful_executions += 1

                    task.future.set_result(
                        ExecutionResult(
                            success=True,
                            result=result,
                            error=None,
                            execution_time=execution_time,
                            bulkhead_name=self.name,
                            queued_time=queued_time,
                            execution_id=execution_id,
                        ),
                    )

            finally:
                self._semaphore.release()

        except Exception as e:
            execution_time = time.time() - start_time
            queued_time = start_time - task.submission_time

            # Record failure
            if self._circuit_breaker:
                self._circuit_breaker.record_failure()

            # Update statistics
            with self._stats_lock:
                self._failed_executions += 1

            # Wrap exception if needed
            if not isinstance(e, BulkheadError):
                if isinstance(e, (FutureTimeoutError, concurrent.futures.TimeoutError)):
                    e = BulkheadTimeoutError(
                        f"Bulkhead '{self.name}' operation timed out after {self.config.timeout_seconds} seconds",
                    )
                else:
                    # Keep original exception but wrap for bulkhead context
                    e = BulkheadError(f"Bulkhead execution failed: {e!s}")

            task.future.set_exception(e)

        finally:
            # Remove from active tasks
            with self._active_tasks_lock, suppress(KeyError):
                self._active_tasks.pop(execution_id)

    @contextmanager
    def _execute_with_timeout(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Iterator[Any]:
        """Execute function with timeout support"""
        if self.config.timeout_seconds:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    yield future.result(timeout=self.config.timeout_seconds)
                except FutureTimeoutError as e:
                    msg = f"Bulkhead '{self.name}' operation timed out after {self.config.timeout_seconds} seconds"
                    raise BulkheadTimeoutError(
                        msg,
                    ) from e
        else:
            yield func(*args, **kwargs)

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future[ExecutionResult]:
        """
        Execute a function through the bulkhead with isolation.

        Args:
            func: The function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Future that will contain the ExecutionResult

        Raises:
            BulkheadFullError: If the bulkhead queue is full
            BulkheadIsolationError: If the bulkhead is isolated
            BulkheadCircuitOpenError: If circuit breaker is open
        """
        # Check circuit breaker if enabled
        if self._circuit_breaker and not self._circuit_breaker.is_request_allowed():
            with self._stats_lock:
                self._rejected_executions += 1
            msg = f"Bulkhead '{self.name}' circuit is open - requests are blocked"
            raise BulkheadCircuitOpenError(
                msg,
            )

        # Create future for result
        future: Future[ExecutionResult] = Future()
        submission_time = time.time()
        execution_id = str(uuid.uuid4())

        # Create task
        task = Task(
            func=func,
            args=args,
            kwargs=kwargs,
            future=future,
            submission_time=submission_time,
            execution_id=execution_id,
            metadata=kwargs.pop("_bulkhead_metadata", {}),
        )

        # Try to add task to queue
        try:
            self._task_queue.put_nowait(task)

            # Update statistics
            with self._stats_lock:
                self._total_executions += 1

        except queue.Full as e:
            with self._stats_lock:
                self._rejected_executions += 1

            msg = f"Bulkhead '{self.name}' queue is full"
            raise BulkheadFullError(msg) from e

        # Set timeout handler if configured
        if self.config.timeout_seconds:

            def timeout_handler() -> None:
                if not future.done():
                    future.set_exception(
                        BulkheadTimeoutError(
                            f"Bulkhead '{self.name}' operation timed out after {self.config.timeout_seconds} seconds",
                        ),
                    )

            timer = threading.Timer(self.config.timeout_seconds, timeout_handler)
            timer.start()

            def _done_callback(fut: Future[ExecutionResult]) -> None:
                timer.cancel()

            future.add_done_callback(_done_callback)

        return future

    def execute_sync(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> ExecutionResult:
        """
        Synchronously execute a function through the bulkhead.

        Args:
            func: The function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            ExecutionResult containing the result or error

        Note: For backward compatibility, this method catches exceptions and returns
        them in ExecutionResult. Use execute() if you prefer exception propagation.
        """
        future = self.execute(func, *args, **kwargs)
        try:
            # Return the ExecutionResult directly from the future
            return future.result()
        except Exception as e:
            # Return ExecutionResult with error for backward compatibility
            return ExecutionResult(
                success=False,
                result=None,
                error=e,
                execution_time=0.0,
                bulkhead_name=self.name,
                execution_id=str(uuid.uuid4()),
            )

    async def execute_async(
        self,
        func: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Asynchronously execute a function through the bulkhead.

        Args:
            func: The function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            The result of the function execution

        Raises:
            BulkheadError: If bulkhead operation fails
            Exception: If the function execution fails
        """

        def run_async() -> Any:
            # Create a new event loop in the thread to run the async function
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(func(*args, **kwargs))
            finally:
                loop.close()

        # Use the regular execute method but with our async wrapper
        future = self.execute(run_async)

        # Wait for the result using run_in_executor to avoid blocking
        execution_result = await asyncio.get_event_loop().run_in_executor(
            None,
            future.result,
        )
        return execution_result.result  # type: ignore[no-any-return]

    @contextmanager
    def context(self, timeout: float | None = None) -> Iterator["Bulkhead"]:
        """
        Context manager for bulkhead operations.

        Example:
            with bulkhead.context() as bh:
                result = bh.execute_sync(my_function, arg1, arg2)
        """
        yield self

    def get_state(self) -> BulkheadState:
        """Get the current state of the bulkhead"""
        if self._circuit_breaker:
            return self._circuit_breaker.get_state()
        return BulkheadState.HEALTHY

    def get_stats(self) -> dict[str, Any]:
        """Get statistics for the bulkhead"""
        with self._stats_lock:
            queue_size = self._task_queue.qsize()
            stats = {
                "name": self.name,
                "state": self.get_state().value,
                "total_executions": self._total_executions,
                "successful_executions": self._successful_executions,
                "failed_executions": self._failed_executions,
                "rejected_executions": self._rejected_executions,
                "current_queue_size": queue_size,
                "max_queue_size": self.config.max_queue_size,
                "concurrent_capacity": self.config.max_concurrent_calls,
                "active_tasks": len(self._active_tasks),
            }

            if self._circuit_breaker:
                stats.update(self._circuit_breaker.get_stats())

            return stats

    def reset_stats(self) -> None:
        """Reset bulkhead statistics"""
        with self._stats_lock:
            self._total_executions = 0
            self._successful_executions = 0
            self._failed_executions = 0
            self._rejected_executions = 0

    def is_healthy(self) -> bool:
        """Check if bulkhead is healthy"""
        state = self.get_state()
        return state in (BulkheadState.HEALTHY, BulkheadState.DEGRADED)

    def shutdown(self, timeout: float = 5.0) -> None:
        """Shutdown the bulkhead and all worker threads gracefully"""
        logger.info("Shutting down bulkhead '%s'", self.name)
        self._stop_event.set()

        # Put poison pills for workers
        for _ in range(len(self._worker_threads)):
            with suppress(queue.Full):
                self._task_queue.put_nowait(None)

        # Cancel pending tasks
        # Use brief timeout instead of tight loop to prevent CPU spinning
        while True:
            try:
                task = self._task_queue.get(timeout=0.01)
                if task is not None and not task.future.done():
                    task.future.set_exception(
                        BulkheadError(
                            f"Bulkhead '{self.name}' shutdown during execution",
                        ),
                    )
                self._task_queue.task_done()
            except queue.Empty:
                break

        # Wait for worker threads
        for thread in self._worker_threads:
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning(
                    f"Worker thread {thread.name} did not shutdown gracefully",
                )

        # Cancel active tasks
        with self._active_tasks_lock:
            for task in self._active_tasks.values():
                if not task.future.done():
                    task.future.set_exception(
                        BulkheadError(
                            f"Bulkhead '{self.name}' shutdown during active execution",
                        ),
                    )

        logger.info("Bulkhead '%s' shutdown complete", self.name)


class BulkheadManager:
    """
    Manages multiple bulkheads for different system components.
    """

    def __init__(self) -> None:
        self._bulkheads: dict[str, Bulkhead] = {}
        self._lock = threading.RLock()
        self._shutdown = False

    def create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        """Create and register a new bulkhead"""
        if self._shutdown:
            msg = "BulkheadManager is shutdown"
            raise RuntimeError(msg)

        with self._lock:
            if config.name in self._bulkheads:
                msg = f"Bulkhead with name '{config.name}' already exists"
                raise ValueError(msg)

            bulkhead = Bulkhead(config)
            self._bulkheads[config.name] = bulkhead
            return bulkhead

    def get_bulkhead(self, name: str) -> Bulkhead | None:
        """Get a bulkhead by name"""
        with self._lock:
            return self._bulkheads.get(name)

    def get_or_create_bulkhead(self, config: BulkheadConfig) -> Bulkhead:
        """Get existing bulkhead or create new one"""
        with self._lock:
            if config.name in self._bulkheads:
                return self._bulkheads[config.name]
            return self.create_bulkhead(config)

    def execute_in_bulkhead(
        self,
        bulkhead_name: str,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[ExecutionResult]:
        """Execute a function in a specific bulkhead"""
        bulkhead = self.get_bulkhead(bulkhead_name)
        if not bulkhead:
            msg = f"Bulkhead '{bulkhead_name}' not found"
            raise ValueError(msg)
        return bulkhead.execute(func, *args, **kwargs)

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all bulkheads"""
        with self._lock:
            return {name: bulkhead.get_stats() for name, bulkhead in self._bulkheads.items()}

    def get_health_status(self) -> dict[str, bool]:
        """Get health status for all bulkheads"""
        with self._lock:
            return {name: bulkhead.is_healthy() for name, bulkhead in self._bulkheads.items()}

    def shutdown_all(self, timeout: float = 5.0) -> None:
        """Shutdown all bulkheads gracefully"""
        with self._lock:
            self._shutdown = True
            for name, bulkhead in self._bulkheads.items():
                try:
                    logger.info("Shutting down bulkhead '%s'", name)
                    bulkhead.shutdown(timeout)
                except Exception as e:
                    logger.error("Error shutting down bulkhead '%s': %s", name, e)

            # Clear the dictionary
            self._bulkheads.clear()

    def __enter__(self) -> "BulkheadManager":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        self.shutdown_all()


# Decorator for easy bulkhead usage
def with_bulkhead(
    bulkhead_name: str,
    manager: BulkheadManager,
    **bulkhead_kwargs: Any,
) -> Callable[[Callable[..., T]], Callable[..., Future[ExecutionResult]]]:
    """
    Decorator to execute a function through a bulkhead.

    Example:
        @with_bulkhead("database", bulkhead_manager)
        def query_database(query):
            return db.execute(query)
    """

    def decorator(
        func: Callable[..., T],
    ) -> Callable[..., Future[ExecutionResult]]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Future[ExecutionResult]:
            # Add bulkhead metadata to kwargs
            if bulkhead_kwargs:
                kwargs["_bulkhead_metadata"] = bulkhead_kwargs

            return manager.execute_in_bulkhead(bulkhead_name, func, *args, **kwargs)

        return wrapper

    return decorator


# Async decorator
def with_bulkhead_async(
    bulkhead_name: str,
    manager: BulkheadManager,
    **bulkhead_kwargs: Any,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, T]]],
    Callable[..., Coroutine[Any, Any, T]],
]:
    """
    Async decorator for bulkhead execution.

    Example:
        @with_bulkhead_async("database", bulkhead_manager)
        async def query_database(query):
            return await db.execute_async(query)
    """

    def decorator(
        func: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            if bulkhead_kwargs:
                kwargs["_bulkhead_metadata"] = bulkhead_kwargs

            bulkhead = manager.get_bulkhead(bulkhead_name)
            if not bulkhead:
                msg = f"Bulkhead '{bulkhead_name}' not found"
                raise ValueError(msg)

            return await bulkhead.execute_async(func, *args, **kwargs)

        return wrapper

    return decorator


# Global default manager for convenience
_default_manager: BulkheadManager | None = None
_default_manager_lock = threading.RLock()


def get_default_manager() -> BulkheadManager:
    """Get or create the default bulkhead manager"""
    global _default_manager
    with _default_manager_lock:
        if _default_manager is None:
            _default_manager = BulkheadManager()
        return _default_manager


def set_default_manager(manager: BulkheadManager) -> None:
    """Set the default bulkhead manager"""
    global _default_manager
    with _default_manager_lock:
        _default_manager = manager


def shutdown_default_manager() -> None:
    """Shutdown the default bulkhead manager"""
    global _default_manager
    with _default_manager_lock:
        if _default_manager is not None:
            _default_manager.shutdown_all()
            _default_manager = None
