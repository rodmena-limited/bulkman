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
