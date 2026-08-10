# Widen resilient-circuit cap to <0.8 so 0.7.x (TLS-capable) is reachable; release 2.0.3

Ticket: issuedb #11
Status: closed (released bulkman 2.0.3 to PyPI)
Authorized by: Farshid Ashouri (direct, no bulkman agent exists)
Originated: stabilize thread with conductor-578aaf (RODMENA CI)

## Context

stabilize 0.21.1 (released 2026-08-10) widened its resilient-circuit cap to
`>=0.4.6,<0.8` after conductor-578aaf reported the `<0.5` pin was mutually
unsatisfiable with bulkman 2.0.2 (`>=0.5.0,<0.6`) and blocked resilient-circuit
0.7.0 — whose `RC_DB_DSN` / `RC_DB_SSL*` are the only way to express TLS to the
breaker's PostgreSQL storage.

bulkman 2.0.2 still caps `resilient-circuit[postgres]>=0.5.0,<0.6`, so a project
using both bulkman and stabilize resolves RC to 0.5.0 and cannot reach 0.7.0.
Farshid authorized stabilize-maintainer to fix bulkman directly.

## EARS Requirements

- R1 (Event-driven): When bulkman is installed alongside the latest
  resilient-circuit release (0.7.0, which introduces RC_DB_DSN / RC_DB_SSL*),
  then the bulkman dependency specification shall admit resilient-circuit
  0.7.x.
- R2 (Ubiquitous): The bulkman package shall remain co-satisfiable with
  stabilize 0.21.1's resilient-circuit range (>=0.4.6,<0.8).
- R3 (Ubiquitous): The resilient-circuit APIs bulkman imports (CircuitProtectorPolicy,
  CircuitState, ProtectedCallError, CircuitBreakerStorage, and the `_status`
  policy internals) shall continue to exist with the same behavior when bulkman
  runs against resilient-circuit 0.7.x.

## Verification

- R1: `resilient-circuit>=0.5.0,<0.8` admits 0.7.0; full bulkman suite run
  against the published 0.7.0 wheel — 154 passed, 3 pre-existing skips.
- R2: bulkman floor (>=0.5.0) and stabilize range (>=0.4.6,<0.8) intersect on
  [0.5.0, 0.8), so both libraries co-resolve.
- R3: source inspection of RC 0.7.0 — CircuitStatusBase/StatusClosed/StatusOpen/
  StatusHalfOpen with validate_execution/mark_failure/mark_success present;
  CircuitState enum present; _status attribute on CircuitProtectorPolicy present.
- Release: wheel+sdist built, twine check PASSED, uploaded to PyPI, tag v2.0.3.
