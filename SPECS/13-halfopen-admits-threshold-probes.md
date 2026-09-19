# HALF_OPEN admits failure_threshold probes per cooldown window, not one

Ticket: issuedb #13
Status: measured and isolated only. No fix implemented, no approach chosen, no
authorization to implement one. Opened as the record of a confirmed defect found
while verifying issue #12.
Originated: AgentBus thread 01M2VP09M8VAJNMWNA2BZBCN3N, recovery leg run by
infra-manager-c13110 on pg-nano-03; layer isolated in this repo.

## Context

The standard half-open shape admits exactly one trial call. If it fails, the
circuit re-opens immediately, because the dependency has just said it is still
broken.

bulkman admits `failure_threshold` calls per cooldown window instead, and the
count tracks the threshold exactly. A service that raises `failure_threshold`
to avoid flapping raises its recovery-probe traffic by the same factor, against
a downstream that is already failing. It is not an outage; it is the breaker
being more permissive during recovery than its configuration reads.

## EARS Requirements

- R1 (Unwanted behavior): If a bulkhead circuit is HALF_OPEN and a trial call
  fails, then the bulkhead shall return the circuit to OPEN and extend the
  cooldown without admitting a further call.
- R2 (State-driven, quantified): While a bulkhead circuit is HALF_OPEN, the
  bulkhead shall admit at most 1 trial call per cooldown window, independent of
  the configured `failure_threshold`.
- R3 (Ubiquitous): The bulkhead shall continue to require `failure_threshold`
  consecutive failures to open from CLOSED.
- R4 (Unwanted behavior): If `success_threshold` is 1, then the bulkhead shall
  close the circuit after 1 successful trial call, not 2.

## Measurement

Reported by infra-manager-c13110 (attributed; not reproduced on that host by
this repo): pg-nano-03, PostgreSQL 18, owner/app privilege split with the app
role holding no ownership, mutual TLS, caller-supplied `PostgresStorage`,
bulkman 2.0.3 + resilient-circuit 0.8.0, `failure_threshold=2`,
`success_threshold=1`, `isolation_duration=3.0`, measured through
`Bulkhead.execute`:

    trip                OPEN        open_until set
    fail 1 (the trial)  HALF_OPEN   failures=1  open_until=0      ADMITTED
    fail 2              OPEN        failures=2  open_until set    ADMITTED
    fail 3, 4           OPEN                                      rejected

Isolation performed here, local PostgreSQL, resilient-circuit 0.8.0, driving
`CircuitProtectorPolicy._status` directly (`validate_execution` /
`mark_failure` / `mark_success` / `_save_state`) with no bulkman in the path.
The harness is asserted to reach OPEN before it is permitted to report any
admission count, because an admission count reads as a clean small number when
nothing is happening at all:

    failure_limit=Fraction(1,1)      resilient-circuit's own sentinel
      trip from CLOSED: 1 failure    admitted per cooldown window after: 1

    failure_limit=Fraction(t-1,t)    bulkman's mapping
      failure_threshold=2    trip=2    admitted per window=2
      failure_threshold=3    trip=3    admitted per window=3
      failure_threshold=5    trip=5    admitted per window=5
      failure_threshold=10   trip=10   admitted per window=10

R4 reproduced here as well: with `success_threshold=1`, the first post-cooldown
success moves OPEN to HALF_OPEN and the second moves HALF_OPEN to CLOSED. The
transition call is consumed by the transition, the same shape as the failure
side. Whether `success_limit=Fraction(1, success_threshold)` is intended to mean
"successes required after the transition into HALF_OPEN" is not established.

## Cause

`bulkman/core.py:108-117`. `failure_limit=Fraction(t-1, t)` is chosen so that
`failure_threshold` is not ignored in the CLOSED state: `Fraction(t, t)` reduces
to `Fraction(1, 1)`, resilient-circuit's 1-slot "any failure opens" sentinel.
That is correct for CLOSED and the trip counts confirm it.

The same rate window is then in force in HALF_OPEN, where "at least t-1 of the
last t failed" requires the window to refill before it can re-open. One
parameter configures two different things and only one of them was reasoned
about. resilient-circuit's state machine behaves correctly under the input it
documents; the defect is in this repo.

## Why no test caught it

`tests/test_bulkhead.py:190`, `:222` and `:261` are the three circuit-breaker
state-machine tests, skipped with "Circuit breaker state machine behavior is
complex - manual testing recommended". The skip dates to commit d999e34,
2025-12-20; no manual testing followed. Every assertion that asks "did it
recover" passes. What catches this is counting what was ADMITTED.

## Design tension for whoever fixes it

`failure_threshold` must shape the CLOSED-state window and the HALF_OPEN probe
count, which have different semantics, and a single `Fraction` cannot express
both. No alternatives have been analysed yet; that analysis belongs in this
file before any code is written.
