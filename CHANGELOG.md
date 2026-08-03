# Changelog

All notable changes to bulkman are documented here.

## [2.0.0] - 2026-08-03

### Breaking changes

- **Circuit breaker is now OFF by default.** `BulkheadConfig.circuit_breaker_enabled`
  defaults to `False`; enable it explicitly to opt in.
- **The library no longer depends on trio.** The async `Bulkhead` is built on
  AnyIO and runs on the asyncio event loop. `trio` was removed from the
  runtime dependencies; the test suite runs on asyncio.
- **`BulkheadSync`'s background thread was renamed** `TrioThread` -> `PortalThread`
  (and `_get_trio_thread` -> `_get_portal_thread`).
- **`timeout_seconds=0.0` is rejected** with `ValueError` - it previously meant
  "instant timeout" on the async bulkhead but "no timeout" on the threading one.
- **`Bulkhead.shutdown()` is terminal**: further `execute()` calls raise
  `RuntimeError`, matching `BulkheadThreading`.

### Fixed

- Sync functions now honor `timeout_seconds` on the async bulkhead (previously
  the timeout silently never fired on asyncio) and run in a dedicated thread
  pool sized to `max_concurrent_calls` (previously capped by anyio's shared
  40-thread pool).
- `BulkheadSync.shutdown()` no longer raises `TypeError`/`AttributeError` and
  honors its timeout.
- Timeout accounting is exact: a task that started and was cut by the timeout
  records a failure, and the ledger `total == success + failed + active` holds
  at every observable instant. A task that timed out before starting counts as
  rejected.
- Capacity (`max_queue_size`) is now enforced on the async `Bulkhead` -
  `BulkheadFullError` is raised beyond `max_concurrent_calls + max_queue_size`.
- Fixed a `BulkheadThreading` lock-order inversion (stats vs in-flight locks)
  that could deadlock under concurrent stats reads and rejections.
- Fixed a circuit-breaker TOCTOU crash when `shutdown()` races `_check_circuit()`.
- `failure_threshold` now means what it says: the circuit opens when at least
  `failure_threshold - 1` of the last `failure_threshold` calls failed
  (previously `Fraction(n, n)` reduced to resilient_circuit's "any failure
  opens" sentinel, so a single failure always tripped the circuit).
- The half-open success path is driven by `success_threshold` (probe window).
- Multi-process safety: a process whose local circuit is still CLOSED no longer
  overwrites a stored OPEN/HALF_OPEN in shared storage - the protection signal
  survives concurrent processes.
- Worker-thread `BaseException`s (e.g. `KeyboardInterrupt`) no longer hang the
  `BulkheadSync` future or kill the shared portal thread.
- Config validation fails fast (`max_concurrent_calls=0`, `failure_threshold=0`,
  negative queue/timeout values raise `ValueError` instead of crashing at use).
- `bulkman/py.typed` is shipped in the wheel (the "Type Safe" claim is now real).

### Other

- The async API runs on asyncio (AnyIO); trio is only referenced in history.
- `audit/evaluations/` contains the reusable probe/fuzz harness (17 probes).
- EARS specs for each audit round live in `SPECS/`.

[2.0.0]: https://github.com/rodmena-limited/bulkman/releases/tag/v2.0.0
