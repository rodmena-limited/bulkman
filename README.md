# Bulkman

**Enterprise-Grade Bulkhead Pattern for Python**

Bulkman is a robust, production-ready implementation of the Bulkhead pattern, designed to isolate resources and prevent cascading failures in mission-critical distributed systems. It supports both modern async (Trio) and traditional synchronous (Threading) workloads with zero overhead.

## Key Capabilities

- **Resource Isolation**: Limits concurrent access to prevent service exhaustion.
- **Circuit Breaker Integration**: Built-in distributed circuit breaking via `resilient-circuit`.
- **Dual Mode**:
    - **Async**: Native [Trio](https://trio.readthedocs.io/) support for high-concurrency IO.
    - **Sync**: Zero-overhead `ThreadPoolExecutor` implementation for CPU-bound or blocking IO tasks.
- **Observability**: Full `contextvars` support for distributed tracing (OpenTelemetry/Datadog).
- **Type Safe**: 100% type-hinted and rigorously tested (>92% coverage).

## Documentation

- **[User Guide](docs/user_guide.md)**: Installation, Quick Start (Async), and Configuration.
- **[Synchronous / Threading Guide](docs/threading_bulkhead.md)**: Best practices for blocking workloads (Flask, Django, Scripts).
- **[Contributing](docs/contributing.md)**: Setup, testing, and contribution guidelines.

## Quick Install

```bash
pip install bulkman
```

## Quick Example (Blocking/Threading)

For legacy or synchronous applications, use `BulkheadThreading`:

```python
from bulkman import BulkheadThreading, BulkheadConfig

# Configure
config = BulkheadConfig(name="db_writer", max_concurrent_calls=10, timeout_seconds=5.0)
bulkhead = BulkheadThreading(config)

# Execute
try:
    future = bulkhead.execute(my_blocking_function, data)
    result = future.result(timeout=5.0)
    print(result.result)
except TimeoutError:
    print("System overloaded")
```

👉 **[See the User Guide for Async/Trio examples](docs/user_guide.md)**

## License

Licensed under the [Apache License 2.0](LICENSE).