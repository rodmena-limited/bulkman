# Bulkman

**Bulkman** is a robust implementation of the **Bulkhead Pattern** for Python, built on [Trio](https://trio.readthedocs.io/) for structured concurrency and [resilient-circuit](https://github.com/rodmena-limited/resilient-circuit) for circuit breaking.

The bulkhead pattern isolates resources and prevents cascading failures in distributed systems by limiting concurrent access to critical resources.

## Features

- **Structured Concurrency**: Built on Trio for proper async/await support
- **Circuit Breaker Integration**: Uses `resilient-circuit` with PostgreSQL support for distributed systems
- **Resource Isolation**: Limit concurrent executions to prevent resource exhaustion
- **Automatic Failure Detection**: Circuit breaker opens automatically after threshold failures
- **Comprehensive Metrics**: Track executions, failures, queue sizes, and circuit states
- **Type Safe**: Full type hints for better IDE support
- **Well Tested**: 92%+ test coverage

## Installation

```bash
pip install bulkman
```

For development with all dependencies:
```bash
pip install bulkman[dev]
```

## Quick Start

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

## Basic Usage

### Simple Function Execution

```python
import trio
from bulkman import Bulkhead, BulkheadConfig

async def fetch_data(url: str) -> dict:
    # Your async function
    await trio.sleep(0.1)
    return {"data": "example"}

async def main():
    config = BulkheadConfig(name="api", max_concurrent_calls=3)
    bulkhead = Bulkhead(config)

    result = await bulkhead.execute(fetch_data, "https://api.example.com")
    print(result.result)

trio.run(main)
```

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

### Check Health

```python
is_healthy = await bulkhead.is_healthy()
state = await bulkhead.get_state()  # BulkheadState enum
```

### Reset Statistics

```python
await bulkhead.reset_stats()
```

## Advanced Usage

### Context Manager

```python
async with bulkhead.context():
    result1 = await bulkhead.execute(func1)
    result2 = await bulkhead.execute(func2)
```

### Handling Sync and Async Functions

Bulkman automatically detects and handles both sync and async functions:

```python
# Async function
async def async_operation():
    await trio.sleep(1)
    return "async result"

# Sync function
def sync_operation():
    import time
    time.sleep(1)
    return "sync result"

# Both work seamlessly
result1 = await bulkhead.execute(async_operation)
result2 = await bulkhead.execute(sync_operation)
```

### Concurrent Execution

```python
import trio
from bulkman import Bulkhead, BulkheadConfig

async def main():
    config = BulkheadConfig(name="workers", max_concurrent_calls=5)
    bulkhead = Bulkhead(config)

    async def worker(task_id: int):
        result = await bulkhead.execute(process_task, task_id)
        return result

    # Run many tasks concurrently, bulkhead limits concurrency
    async with trio.open_nursery() as nursery:
        for i in range(100):
            nursery.start_soon(worker, i)

trio.run(main)
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

## Testing

Run the test suite:

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests with coverage
pytest tests/ --cov=bulkman --cov-report=term-missing

# Run specific test file
pytest tests/test_bulkhead.py -v
```

## Architecture

Bulkman uses:
- **Trio Semaphores** for concurrency control
- **Trio Locks** for thread-safe statistics
- **resilient-circuit** for circuit breaking logic
- **Structured concurrency** for clean resource management

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache Software License 2.0 - see the LICENSE file for details.

## Credits

- Built with [Trio](https://trio.readthedocs.io/) for structured concurrency
- Circuit breaker powered by [resilient-circuit](https://github.com/rodmena-limited/resilient-circuit)
- Inspired by Michael Nygard's "Release It!" and Martin Fowler's circuit breaker pattern

## Related Patterns

- **Circuit Breaker**: Prevents cascading failures (integrated via `resilient-circuit`)
- **Rate Limiting**: Controls request rate (complementary pattern)
- **Retry**: Automatically retries failed operations (can be combined)
- **Timeout**: Prevents indefinite waits (built-in via `timeout_seconds`)

## Links

- **GitHub**: [https://github.com/farshidashouri/bulkman](https://github.com/farshidashouri/bulkman)
- **PyPI**: [https://pypi.org/project/bulkman/](https://pypi.org/project/bulkman/)
- **Trio Documentation**: [https://trio.readthedocs.io/](https://trio.readthedocs.io/)
- **resilient-circuit**: [https://github.com/rodmena-limited/resilient-circuit](https://github.com/rodmena-limited/resilient-circuit)
