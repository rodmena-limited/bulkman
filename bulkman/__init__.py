"""
Bulkman - Bulkhead Pattern Implementation with Trio

A robust implementation of the Bulkhead pattern for isolating resources
and preventing cascading failures in distributed systems.
Built on Trio for structured concurrency and resilient_circuit for circuit breaking.
"""

from bulkman.config import BulkheadConfig, ExecutionResult
from bulkman.core import Bulkhead, BulkheadManager
from bulkman.exceptions import (
    BulkheadCircuitOpenError,
    BulkheadError,
    BulkheadFullError,
    BulkheadIsolationError,
    BulkheadTimeoutError,
)
from bulkman.state import BulkheadState
from bulkman.sync_bridge import BulkheadSync

__version__ = "1.0.3"

__all__ = [
    "Bulkhead",
    "BulkheadSync",
    "BulkheadConfig",
    "BulkheadManager",
    "BulkheadState",
    "BulkheadError",
    "BulkheadCircuitOpenError",
    "BulkheadFullError",
    "BulkheadIsolationError",
    "BulkheadTimeoutError",
    "ExecutionResult",
]
