# BulkheadSync releases the capacity slot after the caller's Future resolves

Ticket: issuedb #14
Status: measured only. No fix implemented, no approach chosen, not authorized as
work.
Discovered: while re-running the suite after the issue #12 verification probes.

## EARS Requirements

- R1 (Event-driven): When the `Future` returned by `BulkheadSync.execute`
  resolves, the bulkhead shall have already released that call's capacity slot.
- R2 (Unwanted behavior): If a caller submits a new call immediately after the
  previous call's `Future` has resolved, and total capacity is otherwise free,
  then `BulkheadSync` shall admit it and shall not raise `BulkheadFullError`.
- R3 (Quality, quantified): `tests/test_audit_fixes.py::TestSyncBridgeParity::
  test_capacity_reject_and_release` shall pass 25 of 25 consecutive isolated
  runs.

## Mechanism

In `bulkman/sync_bridge.py`, the caller-visible `Future` is resolved inside
`run_in_executor` via `future.set_result(...)`. The capacity slot is released in
`_on_work_done`, a done-callback registered on the WORK future, which fires only
after `run_in_executor` has returned.

So the caller's future can be resolved while `_in_flight_count` is still
incremented. A caller that submits again on the next statement races that
callback and receives `BulkheadFullError` while capacity is in fact free.

The existing test asserts the documented contract — its own comment reads "Slot
released: capacity restored" — and the implementation does not guarantee it.

## Measurement

Same test, isolated, 25 consecutive runs each:

    HEAD (2.0.4, tonight's commits)           7/25 failed
    99607f7 (2.0.3, before tonight's work)    4/25 failed

Pre-existing, not a regression from the #12 dependency bump: nothing in those
changes touches `sync_bridge.py`, the circuit breaker is disabled in this test,
and 7 versus 4 is not separable from machine load at n=25.

The full suite passed 154/154 on several runs tonight because suite timing
differs from isolated timing. A defect that hides at suite timing and appears at
caller timing is the kind that reaches a customer rather than CI.

## Not analysed

No alternatives considered. The obvious direction — release the slot before
resolving the caller's future rather than after — has to be checked against the
shutdown path, which currently relies on `_work_futures` and `_on_work_done` to
know what is still running.
