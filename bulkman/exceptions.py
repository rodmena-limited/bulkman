"""Exception definitions for bulkhead pattern."""


class BulkheadError(Exception):
    """Base exception for all bulkhead-related errors."""


class BulkheadIsolationError(BulkheadError):
    """Exception raised when bulkhead is isolated."""


class BulkheadTimeoutError(BulkheadError):
    """Exception raised when bulkhead operation times out."""


class BulkheadFullError(BulkheadError):
    """Exception raised when bulkhead queue is full."""


class BulkheadCircuitOpenError(BulkheadError):
    """Exception raised when bulkhead circuit is open."""


class BulkheadShutdownError(BulkheadError):
    """Exception raised when executing on a shut-down bulkhead.

    Typed (rather than the executor's bare RuntimeError) so callers can
    distinguish "the bulkhead never ran this task because it is shut down"
    from an exception raised by the task itself - e.g. to redeliver a task
    that was submitted in a graceful-shutdown race.
    """
