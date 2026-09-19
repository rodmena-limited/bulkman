# Widen resilient-circuit cap to <0.9 so 0.8.0 (no-DDL PostgresStorage) is reachable; release 2.0.4

Ticket: issuedb #12
Status: RELEASED. bulkman 2.0.4 published to PyPI 2026-09-19, tag v2.0.4.
Authorized by Farshid Ashouri. infra-manager-c13110 reversed the original request
mid-thread, then withdrew that objection after surveying the fleet (see Fleet
survey); no objection stood at release time
Authorized by: Farshid Ashouri (direct)
Originated: AgentBus thread 01M2VP09M8VAJNMWNA2BZBCN3N with infra-manager-c13110,
resilient-circuit-08804c, stabilize-129111

## Context

resilient-circuit 0.8.0 shipped 2026-09-18. bulkman 2.0.3 capped
`resilient-circuit[postgres]>=0.5.0,<0.8`, so no project installing bulkman could
declare 0.8.0.

0.8.0 removes DDL from every `PostgresStorage` runtime path. Construction performs
one read-only catalog query and raises `SchemaNotReady` when the schema is missing
or drifted; schema work moved to `resilient-circuit-cli pg-setup`, with
`RC_DB_AUTO_CREATE=1` restoring the old behaviour explicitly. The change exists
because 0.7.x issued DDL from the runtime role, which failed under an owner/app
privilege split and was then swallowed into a silent in-memory fallback.

bulkman never constructs storage: it is caller-supplied through `circuit_storage`.
The behaviour change therefore lands in caller code, not in the package. It does
land in bulkman's own test fixture, which does call `create_storage()`.

## EARS Requirements

- R1 (Event-driven): When bulkman is installed alongside resilient-circuit 0.8.0,
  the bulkman dependency specification shall admit resilient-circuit 0.8.x.
- R2 (Ubiquitous): The bulkman requirement shall not be a constraint that excludes
  resilient-circuit 0.8.0 from a resolution; co-satisfiability of the whole house
  set additionally requires stabilize to widen its own `<0.8` cap, which is outside
  this repo.
- R3 (Ubiquitous): The resilient-circuit APIs bulkman imports
  (`CircuitProtectorPolicy`, `CircuitState`, `ProtectedCallError`,
  `CircuitBreakerStorage`, and the `_status` policy internals) shall continue to
  exist with the same behavior under resilient-circuit 0.8.x.
- R4 (Unwanted behavior): If resilient-circuit 0.8.0 `PostgresStorage` no longer
  issues DDL at construction and raises `SchemaNotReady` on a missing or drifted
  schema, then neither the bulkman package nor its test suite nor its documented
  examples shall depend on schema creation happening implicitly at storage
  construction.
- R5 (Ubiquitous): The bulkman default shall remain `circuit_breaker_enabled=False`.
- R6 (Quality, quantified): The full bulkman suite run against the published
  resilient-circuit 0.8.0 wheel shall report 0 failures and 0 errors, at least 154
  passed, and coverage at or above the configured 90 percent gate.

## Technical problems

1. Dependency-range admission: deciding whether a declared upper bound is
   load-bearing or precautionary, on evidence rather than on a peer assertion.
2. Behavioral compatibility verification for a dependency to whose private
   internals (`_status`) this package is deliberately coupled.
3. Schema lifecycle ownership: upstream moved DDL out of storage construction into
   an explicit provisioning step; establish whether bulkman or its fixtures relied
   on the implicit path.

## Solution domains

- PEP 440 version specifiers and pip resolution. A `--dry-run` must name every
  installed participant; one naming only the new package resolves nothing and
  exits 0 regardless.
- House prescription: resilience-patterns skill (bulkman for bulkheads,
  `circuit_breaker_enabled` always False; resilient-circuit for breaking).
- Repo precedent SPECS/10 and SPECS/11: a cap is widened only after the full suite
  runs against the published wheel, never on changelog reading.
- Upstream resilient-circuit 0.8.0 behaviour, measured directly here rather than
  read (see Verification).

## Alternatives

- Cap shape: `>=0.5.0,<0.9` [CHOSEN] vs `<1.0` [REJECTED: bulkman reads
  `CircuitProtectorPolicy._status`, a private attribute; a 0.9 minor may change it
  with no major bump, which is exactly why the cap sits at minor granularity] vs
  no upper bound [REJECTED: same coupling, unbounded blast radius].
- Evidence standard: run the full suite against the published 0.8.0 wheel on live
  PostgreSQL [CHOSEN] vs accept the static analysis offered on the bus (imports
  resolve, ABC method set identical, no `PostgresStorage` reference in package
  code) [REJECTED: four static facts are not a compatibility result; both peers
  said so themselves] vs delegate the run to infra-manager's pg-nano box
  [REJECTED as primary evidence: verification of this repo's claim belongs to this
  repo; offered as an independent second check].
- Test-fixture remedy: type-assert the object returned by `create_storage()`
  [CHOSEN: version-agnostic, works on every supported resilient-circuit, and fails
  with a message naming the fix] vs `RC_DB_STRICT=1` [REJECTED: 0.8.0-only, while
  bulkman supports >=0.5.0] vs leaving the fixture alone and relying on
  `RC_DB_AUTO_CREATE=1` [REJECTED: an env var that is absent on one CI box
  restores exactly the silent-pass failure, with no signal].
- Release vehicle: patch release 2.0.4 [CHOSEN: dependency metadata and test
  harness only, no API change] vs minor 2.1.0 [REJECTED: no new bulkman surface].

## Verification

Environment: Python 3.14.2, resilient-circuit 0.8.0 (published wheel), live local
PostgreSQL, bulkman working tree 2.0.4 (import provenance asserted in the run log,
not assumed).

- R1: `resilient-circuit[postgres]>=0.5.0,<0.9` admits 0.8.0.
- R3 + R6: full suite against 0.8.0 — **154 passed, 3 pre-existing skips, 0
  failures, 0 errors, coverage 93.88% (gate 90%)**. Re-run end to end against a
  freshly created, unprovisioned database, not a database left ready by an earlier
  leg.
- R4, measured in both directions on a virgin database, one fresh database per leg:
  - `create_storage()` on an unprovisioned database under 0.8.0 returns
    `InMemoryStorage`, logs at ERROR, and leaves `rc_circuit_breakers` absent.
  - With `RC_DB_AUTO_CREATE=1` it returns `PostgresStorage` and the table exists
    afterwards.
  - **Pre-change fixture on that virgin database: 4 passed** — the
    `test_postgres_integration` tests ran to green against process-local state
    without touching PostgreSQL.
  - **Post-change fixture on that virgin database: 4 errors** at setup, naming
    `pg-setup` and `RC_DB_AUTO_CREATE`.
  - Package code is unaffected: bulkman references neither `PostgresStorage`,
    `create_storage`, nor `resilient_circuit.storage.psycopg` (moved to
    `resilient_circuit.storage.postgres.psycopg` in 0.8.0).
- R5: default unchanged, `circuit_breaker_enabled=False`.
- R2: **not satisfied estate-wide by this change, and not claimed.** stabilize
  0.22.0 still caps `resilient-circuit<0.8`, so a resolution naming bulkman 2.0.4,
  stabilize 0.22.0 and resilient-circuit 0.8.0 still fails — on stabilize's bound
  alone. Recorded in the bus thread; stabilize's `except Exception` around
  `PostgresStorage(...)` swallows `SchemaNotReady` and degrades to in-memory, which
  must be fixed before that bound moves.

## Resolver measurement (pip --dry-run, every participant named)

    A  bulkman 2.0.3 + stabilize 0.22.0 + RC 0.8.0   exit 1   cause: bulkman <0.8
    B  bulkman 2.0.4 + stabilize 0.22.0 + RC 0.8.0   exit 1   cause: stabilize <0.8
    C  bulkman 2.0.4 +                    RC 0.8.0   exit 0
    D  bulkman 2.0.3 +                    RC 0.8.0   exit 1   cause: bulkman <0.8

Leg B is the decisive one: releasing 2.0.4 cannot move the estate to 0.8.x,
because stabilize 0.22.0's `<0.8` is strictly binding wherever stabilize is
installed. bulkman's bound is not the control that prevents the silent
regression discussed in the thread; it only blocks 0.8.0 for projects using
bulkman without stabilize, where there is nothing to protect them from.

## Not exercised by this repo; run independently by infra-manager-c13110

bulkman with `circuit_breaker_enabled=True` against a privilege-split database
with a non-owner application role and mutual TLS is NOT covered by this suite.
This suite runs as a superuser against a local instance, and the three
circuit-breaker state-machine tests are skipped in this repo and were skipped
before this change.

infra-manager-c13110 reported running that configuration on pg-nano-03
(PostgreSQL 18 / FreeBSD, estate client CA, `hostssl` with
`clientcert=verify-full`), with bulkman 2.0.3 and resilient-circuit 0.8.0:
`pg-setup` as the owner role granting DML to the app role; the breaker tripping
as the app role and the OPEN row readable from PostgreSQL; the same row read by
a separate interpreter and independently through `psql`; and `SchemaNotReady`
raised rather than degraded on a virgin database.

That result is recorded here as THEIR measurement, attributed. It has not been
reproduced in this repo and this repo has no access to that host, so it is not
counted as verification performed here. The configuration remains uncovered by
bulkman's own suite.

## RC_DB_STRICT (verified independently, not taken on report)

resilient-circuit-08804c pointed out that `RC_DB_STRICT=1` does at the library
level what the fixture's type assertion does at one call site. Measured here on
a fresh database per leg, resilient-circuit 0.8.0:

    STRICT unset, AUTO_CREATE off   -> InMemoryStorage returned
    STRICT=1,     AUTO_CREATE off   -> raises SchemaNotReady
    STRICT=1,     AUTO_CREATE=1     -> PostgresStorage returned

Both are kept. The flag is global and reaches call sites the fixture does not;
the assertion is local, explicit, and survives the variable being unset. Full
suite on a virgin database with both set: 154 passed, 3 pre-existing skips.

`RC_DB_STRICT` is a 0.8.0 variable and is ignored by the older
resilient-circuit releases this cap still admits, so setting it costs nothing
on 0.5.x–0.7.x.

## Fleet survey (infra-manager-c13110, attributed; not reproduced here)

The open question was whether any environment exists where bulkman's cap
governs alone AND something reaches the path 0.8.0 changed. infra-manager
surveyed every venv on the workstation:

    bulkman WITHOUT stabilize (9)  agentbus, agentbus-client, bulkman,
                                   container_registery_builder, datashard, futex,
                                   RunFlow, tokengate, rodmena-mail-api
    bulkman WITH stabilize (11)    ci, ci-builder, crypto-trader, ct-ooo, ct-ports,
                                   Haven, highway-infra, knowledge-base, pdfapi,
                                   procurement-desk, stabilize-mcp-server, stabilize

An AST walk over each project's own source in the first group found no
`PostgresStorage` construction and no `create_storage` call in any of the eight
walked. The ninth entry is this repo, which is the one case where such a call
exists (`tests/conftest.py`) and is fixed here.

Their stated limit, carried over rather than smoothed away: the walk covers
project source including tests, but they have not verified that every project's
CI provisions a database, so it reads as "no production code constructs
storage" and not as "no test anywhere would break".

This is their measurement, attributed. This repo has no access to those venvs
and has not reproduced it.

## Half-open and recovery

The three circuit-breaker state-machine tests skipped in this repo cover
half-open and recovery. infra-manager's pg-nano leg exercised trip and
cross-process read only, and has offered to run half-open and recovery on the
estate. Requested; not yet run. Until it is, neither this repo nor the estate
has exercised breaker recovery against a privilege-split TLS database.

## Release verification (the served artifact, not the build)

- PyPI JSON: `info.version` = 2.0.4, `requires_dist` carries
  `resilient-circuit[postgres]<0.9,>=0.5.0`, both the wheel and the sdist present.
- Clean venv, `pip install --no-cache-dir bulkman==2.0.4 resilient-circuit==0.8.0`
  from PyPI: exit 0. Import checked from a directory OUTSIDE this repo so the
  working tree cannot satisfy it — `bulkman.__version__` 2.0.4 resolving from
  that venv's site-packages, `resilient_circuit.__version__` 0.8.0,
  `Bulkhead`/`BulkheadConfig` import, `circuit_breaker_enabled` default False,
  `success_threshold` default 3.
- Control, against the PUBLISHED 2.0.4: `pip install --dry-run bulkman==2.0.4
  stabilize==0.22.0 resilient-circuit==0.8.0` still exits 1, pip naming
  `stabilize 0.22.0 depends on resilient-circuit<0.8`. The release does not move
  the estate to 0.8.x; stabilize's bound remains strictly binding wherever
  stabilize is installed, as designed.

## Known defects at release, both pre-existing and both unfixed

- #13 `success_threshold=1` yields a `failure_threshold`-sized half-open probe
  window via resilient-circuit's `Fraction(1,1)` sentinel. Unreachable on this
  estate: no deployment enables the breaker.
- #14 `BulkheadSync` releases the capacity slot after the caller's Future
  resolves; 7/25 isolated failures on this release, 4/25 on 2.0.3.

Neither is a regression from this change and neither was fixed by it. Released
knowingly, recorded here so nobody reads 2.0.4 as a clean bill of health.

## Post-release: resilient-circuit 0.8.1

resilient-circuit 0.8.1 was published shortly after this release. Because the
cap is `<0.9`, a fresh `pip install bulkman` now resolves **bulkman 2.0.4 +
resilient-circuit 0.8.1** — verified by dry-run in a clean venv. That is a pair
nobody had exercised at the moment 2.0.4 was published: the verification on this
ticket was against 0.8.0.

Run after the fact rather than assumed: full suite against the published 0.8.1
on a freshly created, unprovisioned database — **154 passed, 3 pre-existing
skips**. Import provenance asserted in the log (`bulkman` 2.0.4 from the working
tree, `resilient_circuit` 0.8.1 from site-packages).

The lesson for the cap, not just for this release: a `<0.9` upper bound means
every future 0.8.x resolves for bulkman users the day it ships, with no action
here. Widening a cap moves the testing obligation forward in time. Checking the
new minor after upstream ships it is now part of owning that bound.

The sharper form, from infra-manager-c13110 four hours after this release:
**a cap raised in one package is a deploy scheduled in every unpinned consumer.**
RunFlow declares `resilient-circuit>=0.7.0` with no upper bound and rebuilds its
venv from scratch on every deploy, so its next green pipeline moves both hosts to
0.8.1 with nobody deciding it. Before this release the estate could not reach
0.8.x at all. The upgrade looks safe there — RunFlow constructs no storage, so
0.8.x's breaking change is unreachable — but safety was not the issue; the issue
is that the decision stopped being anyone's. The obligation created by widening a
bound does not stop at the package that widened it.

## Estate state at release

    bulkman   2.0.4    resilient-circuit[postgres]>=0.5.0,<0.9   published
    stabilize 0.23.1   resilient-circuit>=0.4.6,<0.8             published

stabilize 0.23.1 shipped the handler fix (`_degrade_or_raise`, logging at ERROR
and naming the exception type) but still declares `<0.8`, so stabilize's bound is
now the sole remaining gate on 0.8.x estate-wide. Confirmed from PyPI metadata
here, not taken on report. bulkman no longer gates anything.

## A second `create_storage` call site, found by the broad-grep counter

infra-manager-c13110 named the habit behind three of their errors tonight:
searching for the thing you suspect and reporting the absence as though you had
searched for the category. Their counter — after a negative narrow grep, run the
BROADER term and confirm it returns something — applied here:

    grep -rn "resilient_circuit" bulkman/ tests/ --include="*.py"   -> 22 hits

That known-positive surfaced a call site the conftest fix did not cover:

    tests/test_namespace_isolation.py:69-70
        storage_prod    = create_storage(namespace="production")
        storage_staging = create_storage(namespace="staging")

Direct calls, not the guarded `postgres_storage` fixture. They are covered in
practice two ways: the test also requests `postgres_storage`, so the fixture's
type assertion runs first, and `RC_DB_STRICT=1` in `test.env` makes
`create_storage` raise rather than degrade at EVERY call site.

That is the concrete justification for keeping both controls rather than either
alone, which was the open question when the flag was adopted. The fixture
assertion covers one call site precisely; the flag covers the ones nobody
enumerated. Here there was one nobody had enumerated.

## The unenforced-cap mechanism has no confirmed sighting

Measured deliberately in a clean venv: with published bulkman 2.0.3 installed
(cap `<0.8`), `pip install -U resilient-circuit==0.8.0` exits 0, prints
`ERROR: ... which is incompatible`, and installs anyway. Post-state confirmed by
import, not by exit code.

Two claimed sightings of that happening in the wild were investigated. Neither is
an instance of the mechanism, for two different reasons:

- A RunFlow workstation checkout reported on resilient-circuit 0.8.1. This was a
  REAL state, correctly measured — runflow-3858c4 had installed 0.8.1
  deliberately to test the upgrade and restored 0.7.0 twenty minutes later.
  infra-manager-c13110 first reported it as an estate condition, then retracted
  it as their own mismeasurement, then corrected the retraction: the value was
  right, taken during a window, of an environment another agent was actively
  working in. It is a deliberate install by its owner, which is not the
  accidental-upgrade mechanism at all.
- This repo's own `.venv`, where `pip check` reported
  `bulkman 2.0.2 has requirement resilient-circuit[postgres]<0.8,>=0.5.0, but
  you have resilient-circuit 0.8.1`. Correct output, wrong cause: it is an
  EDITABLE install whose recorded metadata was a 2.0.2 snapshot, while the code
  that runs is the working tree at 2.0.4 with a real cap of `<0.9`, which 0.8.1
  satisfies. pip was enforcing a constraint that no longer exists against code
  that does not carry it. Refreshed with `pip install -e . --no-deps`; `pip
  check` now clean.

So the mechanism is real and measured; confirmed sightings of it happening to
anyone unintentionally are zero. Those are different claims and the second
should not be inflated into the first.

The general hazard behind the second case is worth stating on its own, because
resilient-circuit-08804c hit it in their own repo in the same hour: AN EDITABLE
INSTALL'S METADATA IS A FOSSIL. `pip show` and `importlib.metadata.version`
report the version recorded when `pip install -e` last ran, not the version the
tree carries now. Their venv reports 0.6.0 while the interpreter runs 0.8.2;
this one reported 2.0.2 while running 2.0.4. The discriminator they give is
cheap: a `dist-info` in site-packages with no matching package directory means
the metadata version is a fossil, and `module.__file__` plus the module's own
`__version__` are the only things that answer the question. Any survey keyed on
installed metadata will mis-bucket every editable dev checkout.

Related: that same stale editable metadata is why this venv reported bulkman
2.0.2 at the start of this work while imports returned 2.0.4, which is why every
suite result quoted on this ticket asserts import provenance in the run log. One
uncorrected cause surfaced twice — first as a reason to distrust a green suite,
then as a phantom constraint violation.
