"""
Bulkman - Bulkhead Pattern Implementation

A robust implementation of the Bulkhead pattern for isolating resources
and preventing cascading failures in distributed systems.

Provides three implementations:
- Bulkhead: Async implementation using Trio (for async workloads)
- BulkheadSync: Sync wrapper around async Bulkhead via Trio thread
- BulkheadThreading: Pure threading implementation (for sync workloads, recommended)

For sync workloads, prefer BulkheadThreading over BulkheadSync for simpler
execution path and more predictable behavior.
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
from bulkman.threading import BulkheadThreading

__version__ = "1.1.0"

__all__ = [
    # Core implementations
    "Bulkhead",
    "BulkheadSync",
    "BulkheadThreading",
    # Configuration
    "BulkheadConfig",
    "BulkheadManager",
    "BulkheadState",
    "ExecutionResult",
    # Exceptions
    "BulkheadError",
    "BulkheadCircuitOpenError",
    "BulkheadFullError",
    "BulkheadIsolationError",
    "BulkheadTimeoutError",
]
