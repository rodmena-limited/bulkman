"""Configuration for bulkhead pattern."""

from __future__ import annotations

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

       Note: shared state is read when each instance is constructed (so a
       restarted instance inherits the persisted state); running instances
       do not live-propagate state changes to each other - each instance
       also counts its own failures and opens its own circuit.

    3. Different environments:
       - Use storage with different namespace for each environment
       - Result: Fully isolated prod/staging/dev

    For distributed systems:
    ------------------------
    - Create storage with `create_storage(namespace="production")`
    - All instances of the same app share the same namespace
    - Different bulkheads (different names) are isolated within that namespace
    - Different environments use different namespaces

    Notes:
    ------
    - The circuit breaker is **disabled by default** (`circuit_breaker_enabled=False`).
      Enable it explicitly when you want the failure-isolation behaviour.
    - `failure_threshold` is interpreted as: the circuit opens when at least
      `failure_threshold - 1` of the last `failure_threshold` calls failed
      (equivalently: after `failure_threshold` consecutive failures).
    - `success_threshold` sizes the half-open probe window: the circuit closes when
      the probe window is full and any probe succeeded.
    """

    name: str
    max_concurrent_calls: int = 10
    max_queue_size: int = 100
    timeout_seconds: float | None = None
    failure_threshold: int = 5
    success_threshold: int = 3
    isolation_duration: float = 30.0  # seconds
    circuit_breaker_enabled: bool = False
    health_check_interval: float = 5.0

    def __post_init__(self) -> None:
        """Validate configuration values that would otherwise crash at runtime."""
        if self.max_concurrent_calls < 1:
            raise ValueError("max_concurrent_calls must be >= 1")
        if self.max_queue_size < 0:
            raise ValueError("max_queue_size must be >= 0")
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")
        if self.isolation_duration < 0:
            raise ValueError("isolation_duration must be >= 0")
        # 0.0 would mean "instant timeout" on the async bulkhead but "no
        # timeout" on the threading one; reject the ambiguity outright.
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be None or > 0")


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
