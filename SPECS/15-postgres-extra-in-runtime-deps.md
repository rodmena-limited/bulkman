# Runtime dependency declares `resilient-circuit[postgres]`, forcing psycopg on every consumer

Ticket: issuedb #15
Status: measured only. No fix implemented, no approach chosen, not authorized as
work.
Origin: stabilize-129111 on AgentBus, thread 01M2Y8DEAK3A7TS1CCAMZRYZA7.

## EARS Requirements

- R1 (Ubiquitous): The bulkman runtime dependency specification shall declare
  only what bulkman's importable code requires.
- R2 (Unwanted behavior): If a consumer installs bulkman and supplies no
  PostgreSQL-backed circuit storage, then the install shall not require psycopg.
- R3 (Optional feature): Where a consumer wants PostgreSQL-backed circuit
  storage, the bulkman distribution shall offer an extra that pulls
  `resilient-circuit[postgres]`, so the capability remains available explicitly.
- R4 (Ubiquitous): The bulkman test suite shall continue to have psycopg
  available, via the `dev` extra rather than the runtime dependency.

## The chain

Confirmed from published PyPI metadata, not from the report:

    stabilize 0.27.0    bulkman>=2.0.4, resilient-circuit>=0.8.2
    bulkman 2.0.4       resilient-circuit[postgres]<0.9,>=0.5.0
    resilient-circuit   psycopg>=3.1.0; extra == "postgres"

bulkman's runtime dependency is what pulls psycopg. stabilize inherits it
transitively, and neither manifest says so. stabilize is the estate's most
transitively-installed consumer, so this reaches nearly everything.

## Measurement

Clean venv, published bulkman 2.0.4 from PyPI:

    with the extra as declared    psycopg 3.3.6 present
    psycopg force-uninstalled     import bulkman OK
                                  Bulkhead, BulkheadConfig, BulkheadSync,
                                  BulkheadManager all import
                                  await bh.execute(...) -> success True, result 42

bulkman's importable code does not need psycopg. Storage is always
caller-supplied through `circuit_storage`, and bulkman imports only the ABSTRACT
`CircuitBreakerStorage` alongside `CircuitProtectorPolicy`, `CircuitState` and
`ProtectedCallError`.

Scope of that leg, so it is not over-read: it exercised import and execute with
`circuit_breaker_enabled` at its default of False. It did not exercise a caller
passing a `PostgresStorage` — such a caller needs psycopg regardless and would
obtain it from `resilient-circuit[postgres]` themselves, which is what R3 is for.

## What the runtime dependency actually pulls

Full transitive closure of a bare `pip install bulkman==2.0.4`, clean venv:

    anyio  bulkman  idna  psycopg  python-dotenv  resilient-circuit
    sniffio  typing_extensions

So the `[postgres]` extra contributes **psycopg AND python-dotenv** — the latter
being the less obvious of the two and not mentioned by anyone so far.
`psycopg-pool` is NOT pulled: resilient-circuit 0.8.3's `postgres` extra is
`psycopg>=3.1.0` plus `python-dotenv>=1.0.0`, with no pool, and
`import psycopg_pool` in that venv raises `ModuleNotFoundError`. Anything
observing psycopg-pool downstream is getting it from its own `psycopg[pool]`,
not through bulkman.

## Risk

Any consumer currently relying on bulkman to pull psycopg implicitly breaks on
upgrade. That is why R3 proposes an extra rather than a bare removal, and why
this is a release decision rather than a cleanup.

One class of breakage is already ruled out: stabilize 0.27.0 declares
`psycopg[pool]>=3.0` in its own `postgres` extra, confirmed from published
metadata, so stabilize's PostgreSQL users never relied on bulkman's implicit
pull. The exposure is bare-stabilize installs that reach for the PostgreSQL store
without the extra, which is what `stabilize[postgres]` is already documented
for.
