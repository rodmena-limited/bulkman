# HALF_OPEN probe window: success_threshold=1 is silently ignored

Ticket: issuedb #13
Status: measured only. No fix implemented, no approach chosen, not authorized as
work. SUPERSEDES the earlier framing of this ticket, which was wrong — see
"Correction" below.
Originated: AgentBus thread 01M2VP09M8VAJNMWNA2BZBCN3N.

## Correction to the first version of this spec

The first version claimed bulkman admits `failure_threshold` probes per cooldown
window, scaling 1:1 with `failure_threshold`, and treated that as a deviation
from single-probe half-open semantics. That was measured with `success_limit`
pinned to `Fraction(1,1)` in the isolation harness, so all four rows of the
"scaling" table were the `success_threshold=1` sentinel case. It is not the
general behaviour, and the general behaviour is documented rather than defective.

`bulkman/config.py:55` states the design plainly:

    `success_threshold` sizes the half-open probe window: the circuit closes
    when the probe window is full and any probe succeeded.

So a multi-probe half-open window is bulkman's documented design, not a bug. The
earlier requirement "shall admit at most 1 trial call per cooldown window"
contradicted the package's own documentation and has been withdrawn.

## EARS Requirements

- R1 (State-driven, quantified): While a bulkhead circuit is HALF_OPEN, the
  bulkhead shall admit exactly `success_threshold` trial calls per cooldown
  window, for every `success_threshold >= 1`.
- R2 (Unwanted behavior): If `success_threshold` is 1, then the bulkhead shall
  size the half-open probe window at 1 slot, and shall not size it from
  `failure_threshold`.
- R3 (Ubiquitous): The bulkhead shall continue to require `failure_threshold`
  consecutive failures to open from CLOSED.

## Measurement

All rows below: real `Bulkhead.execute`, PostgreSQL, a GENUINE owner/app
privilege split — `rc_circuit_breakers` owned by `bm_owner`, runtime role
`bm_app` holding only SELECT/INSERT/UPDATE/DELETE, `pg_has_role(bm_app,
bm_owner, 'member')` asserted false — schema provisioned by
`resilient-circuit-cli pg-setup --grant-to bm_app` as the owner, every state read
back on a THIRD connection as `bm_owner`. resilient-circuit 0.8.0. The harness
asserts the circuit reaches OPEN before it is permitted to report any admission
count.

Failures ADMITTED per cooldown window:

    failure_threshold   success_threshold   admitted
            2                   1               2
            2                   2               2
            2                   3               3
            2                   5               5
            3                   1               3
            3                   2               2
            3                   3               3
            3                   5               5
            5                   1               5
            5                   2               2
            5                   3               3
            5                   5               5
           10                   1              10
           10                   2               2
           10                   3               3
           10                   5               5

The probe count tracks `success_threshold` — as documented — EXCEPT at
`success_threshold=1`, where it tracks `failure_threshold`. That column is the
defect and it is the only one.

Successes required to CLOSE, same split:

    ft=2 st=1 -> 2     ft=3 st=1 -> 3     ft=5 st=1 -> 5
    ft=2 st=2 -> 2     ft=3 st=2 -> 2     ft=5 st=2 -> 2

Identical to the InMemoryStorage matrices run independently by
infra-manager-c13110 and resilient-circuit-08804c, cell for cell. Storage
visibility does not affect the state machine.

## Cause

`resilient_circuit/circuit_breaker.py:476`, `StatusHalfOpen.__init__`:

    self.use_success = policy.success_limit != policy.DEFAULT_THRESHOLD  # Fraction(1,1)
    self.execution_log = BinaryCircularBuffer(
        size=(policy.success_limit.denominator if self.use_success
              else policy.failure_limit.denominator))

bulkman passes `success_limit=Fraction(1, success_threshold)`
(`bulkman/core.py:116`). At `success_threshold=1` that is `Fraction(1,1)`, which
IS resilient-circuit's "no success ratio" sentinel, so the window falls back to
`failure_limit.denominator`.

`bulkman/core.py:101-107` documents this exact trap for `failure_limit` and works
around it with `Fraction(t-1, t)`. The same trap under `success_limit` was not
guarded. Upstream tracks the sentinel itself as resilient-circuit issuedb #9
(`success_limit` to `Optional[Fraction]`); bulkman can also guard it locally
without waiting for that release.

## Open design question, separate from the defect

A failing probe does not re-open the circuit immediately; the window must fill
first, so `success_threshold` failing calls reach a downstream that has already
said it is broken. That follows from the documented window design rather than
violating it, and standard half-open semantics would re-open on the first failed
probe. Whether bulkman wants the documented window or the standard single probe
on the FAILURE side is a design decision, not a bug report, and no alternatives
have been analysed.

## Why no test caught any of this

`tests/test_bulkhead.py:190`, `:222`, `:261` are the three circuit-breaker
state-machine tests, skipped with "Circuit breaker state machine behavior is
complex - manual testing recommended" since commit d999e34, 2025-12-20. No manual
testing followed.
