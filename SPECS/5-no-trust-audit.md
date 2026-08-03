# EARS Spec — No-trust audit: fuzz, GIL-free, distributed state, packaging

Ticket: #5 (issuedb)

- Under randomized operation sequences, the bulkhead shall never lose or double-count admissions.
- With the GIL disabled, the bulkhead shall behave identically to GIL-enabled operation.
- Circuit state shared through PostgreSQL storage shall be observed consistently across instances.
- The package shall build and import on the declared Python versions.
- All existing tests shall pass without regression.
