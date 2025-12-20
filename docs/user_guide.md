# User Guide

## Installation

```bash
pip install bulkman
```

For development with all dependencies:
```bash
pip install bulkman[dev]
```

## Quick Start (Async/Trio)

```python
import trio
from bulkman import Bulkhead, BulkheadConfig

async def main():
    # Create a bulkhead with configuration
    config = BulkheadConfig(
        name="api_calls",
        max_concurrent_calls=5,
        timeout_seconds=10.0,
        circuit_breaker_enabled=True,
    )
    bulkhead = Bulkhead(config)

    # Execute a function through the bulkhead
    result = await bulkhead.execute(my_function, arg1, arg2)

    if result.success:
        print(f"Result: {result.result}")
    else:
        print(f"Error: {result.error}")

trio.run(main)
```

## Advanced Usage

### Using Decorators

```python
from bulkman import Bulkhead, BulkheadConfig
from bulkman.core import with_bulkhead

config = BulkheadConfig(name="database", max_concurrent_calls=10)
bulkhead = Bulkhead(config)

@with_bulkhead(bulkhead)
async def query_database(query: str):
    # Your database query
    return await db.execute(query)

# The decorator automatically wraps execution
result = await query_database("SELECT * FROM users")
```

### Managing Multiple Bulkheads

```python
import trio
from bulkman import BulkheadManager, BulkheadConfig

async def main():
    manager = BulkheadManager()

    # Create multiple bulkheads for different resources
    await manager.create_bulkhead(
        BulkheadConfig(name="database", max_concurrent_calls=20)
    )
    await manager.create_bulkhead(
        BulkheadConfig(name="external_api", max_concurrent_calls=5)
    )

    # Execute in specific bulkhead
    result = await manager.execute_in_bulkhead(
        "database",
        lambda: db.query("SELECT * FROM users")
    )

    # Get health status of all bulkheads
    health = await manager.get_health_status()
    print(health)  # {'database': True, 'external_api': True}

trio.run(main)
```

## Configuration

### BulkheadConfig Options

```python
from bulkman import BulkheadConfig

config = BulkheadConfig(
    name="my_bulkhead",              # Unique name for the bulkhead
    max_concurrent_calls=10,         # Max concurrent executions
    max_queue_size=100,              # Max queued tasks (currently for reference)
    timeout_seconds=30.0,            # Execution timeout in seconds
    failure_threshold=5,             # Failures before circuit opens
    success_threshold=3,             # Successes to close circuit
    isolation_duration=30.0,         # Seconds circuit stays open
    circuit_breaker_enabled=True,    # Enable/disable circuit breaker
    health_check_interval=5.0,       # Health check interval (for reference)
)
```

## Circuit Breaker Integration

Bulkman integrates with `resilient-circuit` for sophisticated circuit breaking:

```python
from bulkman import Bulkhead, BulkheadConfig
from resilient_circuit.storage import create_storage

# Use PostgreSQL for distributed circuit breaker state
storage = create_storage(namespace="my_app")

config = BulkheadConfig(
    name="external_service",
    failure_threshold=3,
    isolation_duration=60.0,
)

bulkhead = Bulkhead(config, circuit_storage=storage)
```

The circuit breaker has three states:
- **CLOSED** (Healthy): Normal operation
- **OPEN** (Isolated): Blocking requests after failures
- **HALF_OPEN** (Degraded): Testing if service recovered

## Monitoring and Metrics

### Get Statistics

```python
stats = await bulkhead.get_stats()
print(stats)
# {
#     'name': 'api',
#     'state': 'healthy',
#     'total_executions': 150,
#     'successful_executions': 145,
#     'failed_executions': 5,
#     'rejected_executions': 0,
#     'active_tasks': 3,
#     'max_concurrent_calls': 10,
#     'max_queue_size': 100,
#     'circuit_breaker_enabled': True,
#     'circuit_status': 'CLOSED'
# }
```

## Error Handling

Bulkman provides specific exceptions for different failure scenarios:

```python
from bulkman.exceptions import (
    BulkheadError,
    BulkheadCircuitOpenError,
    BulkheadTimeoutError,
    BulkheadFullError,
)

try:
    result = await bulkhead.execute(my_function)
    if not result.success:
        print(f"Execution failed: {result.error}")
except BulkheadCircuitOpenError:
    print("Circuit breaker is open")
except BulkheadTimeoutError:
    print("Operation timed out")
except BulkheadFullError:
    print("Bulkhead is at capacity")
```
