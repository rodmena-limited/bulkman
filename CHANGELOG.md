# Changelog

All notable changes to bulkman are documented here.

## [2.0.2] - 2026-08-07

### Changed

- **resilient-circuit pin raised to `>=0.5.0,<0.6`.** Upstream fixed the
  PostgreSQL storage cross-process write race bulkman reported (and mitigated
  in 2.0.0): `set_state` is now a guarded conditional upsert — a stored OPEN
  with an unexpired cooldown is immutable to blind writers — and an atomic
  `update_state` read-modify-write API exists. 0.5.0 also adds distributed
  admission (peer OPEN honored before executing the protected call).
  Verified here by re-running the original multiprocess reproduction against
  the published 0.5.0 wheel on live PostgreSQL.
- **bulkman keeps its own persistence guard** (`_persist_circuit_state`):
  upstream's guard cannot protect a stored HALF_OPEN from blind writes (a
  legitimate recovery close is indistinguishable from a stale writer in a
  single guarded statement), so bulkman's refusal to persist a locally-CLOSED
  state over a stored OPEN/HALF_OPEN still covers that window.

### Added

- `audit/evaluations/probe_storage_guarded_setstate.py`: storage-level probe
  asserting the upstream 0.5.0 contract directly (stale-CLOSED refusal,
  first-opener-wins `open_until`, recovery unblocked after expiry). Validated
  red against the pre-fix 0.4.7 wheel before its green was trusted.

## [2.0.1] - 2026-08-03

### Fixed

- Post-shutdown `execute()` now raises a typed `BulkheadShutdownError(BulkheadError)`
  instead of the executor's bare `RuntimeError`, in all three implementations
  (async `Bulkhead`, `BulkheadThreading`, `BulkheadSync`) — including the
  shutdown-race path. Callers can now classify "bulkhead is shut down" as a
  retryable condition, distinct from a `RuntimeError` raised by the task itself.

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
- **⚠ `failure_threshold` is now honored — before 2.0.0 the circuit opened on
  the FIRST failure regardless of the configured threshold.** If you ran
  1.x with `circuit_breaker_enabled=True` and, say, `failure_threshold=5`,
  your circuit was opening after a single failure (`Fraction(n, n)` reduced
  to resilient_circuit's "any failure opens" sentinel with a 1-slot window).
  From 2.0.0 the circuit opens only when at least `failure_threshold - 1` of
  the last `failure_threshold` calls failed. Upgrading therefore makes your
  breaker LESS eager to open than what you were actually running — review
  your thresholds if you had tuned them around the buggy behaviour. Callers
  with the breaker disabled (the default) are unaffected.

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

[2.0.2]: https://github.com/rodmena-limited/bulkman/releases/tag/v2.0.2
[2.0.1]: https://github.com/rodmena-limited/bulkman/releases/tag/v2.0.1
[2.0.0]: https://github.com/rodmena-limited/bulkman/releases/tag/v2.0.0
