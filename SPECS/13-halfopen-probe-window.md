# HALF_OPEN probe window: success_threshold=1 is silently ignored

Ticket: issuedb #13
Status: RESOLVED UPSTREAM in resilient-circuit 0.8.2, verified here against the
published artifact. No bulkman code change was required; bulkman 2.0.4's `<0.9`
cap already admits it. SUPERSEDES the earlier framing of this ticket, which was
wrong — see "Correction" below.
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

## Upstream fix in flight: resilient-circuit 0.8.2

resilient-circuit-08804c reports 0.8.2 built, tested and NOT published, fixing
the `Fraction(1,1)` sentinel (their issuedb #9). Their measurement, against
bulkman's own `Fraction(1, success_threshold)` mapping: the `st=1` column of the
probe matrix goes `2/3/5/10` to `1/1/1/1`, with the other twelve cells unchanged.

If that holds, **R2 of this spec is satisfied by an upstream release with no
bulkman code change**, and bulkman 2.0.4's `<0.9` cap already admits 0.8.2.

NOT VERIFIED HERE. 0.8.2 is unpublished, so this repo cannot run it. Re-run the
16-cell matrix against the published artifact before closing this ticket on the
strength of it — a fix reported is not a fix measured, and the matrix is cheap.

Note the coupling, which is a consequence of our own cap: they chose a PATCH
number rather than 0.9.0 specifically because bulkman 2.0.4 caps `<0.9`, which
would have excluded the release from the caller that needs it most. Our upper
bound is now shaping upstream's version numbering. That is the third consequence
of widening a cap, after the testing obligation moving forward in time and the
deploys scheduled in unpinned consumers.

## Verified against published resilient-circuit 0.8.2

Re-run as this file instructed — not closed on the upstream report. Same harness
as the original measurement: real `Bulkhead.execute`, PostgreSQL, genuine
owner/app split (`rc_circuit_breakers` owned by `bm_owner`, runtime role
`bm_app` holding only DML, `pg_has_role(bm_app, bm_owner, 'member')` asserted
false, schema provisioned by `resilient-circuit-cli pg-setup --grant-to bm_app`
as the owner), every state read back on a third connection as `bm_owner`, OPEN
asserted before any admission count is reported.

Failures ADMITTED per cooldown window, 0.8.2 (0.8.0 values in brackets where
they differ):

        st=1      st=2   st=3   st=5
  ft=2   1 [2]     2      3      5
  ft=3   1 [3]     2      3      5
  ft=5   1 [5]     2      3      5
  ft=10  1 [10]    2      3      5

Successes required to CLOSE:

  ft=2 st=1 -> 1 [2]    ft=3 st=1 -> 1 [3]    ft=5 st=1 -> 1 [5]
  ft=2 st=2 -> 2        ft=3 st=2 -> 2        ft=5 st=2 -> 2

Trip counts from CLOSED unchanged at 2/3/5/10, so R3 holds.

- **R1 satisfied**: the probe count equals `success_threshold` for every value
  including 1.
- **R2 satisfied**: `success_threshold=1` sizes the window at 1 slot and no
  longer takes it from `failure_threshold`.
- **R3 satisfied**: `failure_threshold` consecutive failures still open from
  CLOSED.

Served-artifact provenance: `resilient_circuit.__version__` 0.8.2 resolving from
site-packages, `CircuitProtectorPolicy.__init__`'s `success_limit` default now
`None` rather than `Fraction(1,1)`. Full bulkman suite against 0.8.2 on a
freshly created unprovisioned database: 154 passed, 3 pre-existing skips.

The open design question also resolves for `success_threshold=1`: with a 1-slot
window a single failing probe re-opens the circuit immediately, which is the
standard half-open semantics. For `success_threshold > 1` the documented
multi-probe window still applies, which is intended behaviour.

## Consequence for the dependency floor, NOT acted on

bulkman documents at `config.py:55` that `success_threshold` sizes the half-open
probe window. That documentation is only TRUE on resilient-circuit >= 0.8.2. The
current floor is `>=0.5.0`, so a resolver may still install a version on which
`success_threshold=1` silently means `failure_threshold`.

Raising the floor to `>=0.8.2` would make the declared range match the documented
behaviour, at the cost of dropping 0.5.x–0.8.1 support and requiring a release.
That is an operator decision and no alternatives have been analysed. Recorded
here so the gap between what bulkman documents and what its floor admits is not
left implicit.
