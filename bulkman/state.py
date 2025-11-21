"""State definitions for bulkhead pattern."""

from enum import Enum


class BulkheadState(Enum):
    """Enum representing the state of a bulkhead."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    ISOLATED = "isolated"
    FAILED = "failed"
