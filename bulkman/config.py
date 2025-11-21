"""Configuration for bulkhead pattern."""

from dataclasses import dataclass
from typing import Any


@dataclass
class BulkheadConfig:
    """Configuration for a bulkhead.

    Isolation Strategy:
    ====================
    Circuit breaker state isolation is controlled by (resource_key, namespace):

    - **resource_key**: Set to bulkhead `name`. Different names = always isolated.
    - **namespace**: Set at storage level. Same namespace = shared state space.

    Examples:
    ---------
    1. Different bulkheads in same app:
       - Use different `name` for each bulkhead
       - Use same storage (same namespace)
       - Result: Isolated by resource_key

    2. Same bulkhead across app instances (persistence):
       - Use same `name` for the bulkhead
       - Use same storage (same namespace)
       - Result: Share circuit breaker state (INTENDED)

    3. Different environments:
       - Use storage with different namespace for each environment
       - Result: Fully isolated prod/staging/dev

    For distributed systems:
    ------------------------
    - Create storage with `create_storage(namespace="production")`
    - All instances of the same app share the same namespace
    - Different bulkheads (different names) are isolated within that namespace
    - Different environments use different namespaces
    """

    name: str
    max_concurrent_calls: int = 10
    max_queue_size: int = 100
    timeout_seconds: float | None = None
    failure_threshold: int = 5
    success_threshold: int = 3
    isolation_duration: float = 30.0  # seconds
    circuit_breaker_enabled: bool = True
    health_check_interval: float = 5.0


@dataclass
class ExecutionResult:
    """Result of a function execution through bulkhead."""

    success: bool
    result: Any
    error: Exception | None
    execution_time: float
    bulkhead_name: str
    queued_time: float = 0.0
    execution_id: str = ""
