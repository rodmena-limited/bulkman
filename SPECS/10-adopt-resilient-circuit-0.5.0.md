# Ticket #10 — Adopt resilient-circuit 0.5.0 (storage race fix), verify, release bulkman 2.0.2

EARS SPEC:

- When resilient-circuit 0.5.0 (storage-race fix + distributed admission) is
  published to PyPI, bulkman shall update its resilient-circuit pin to admit
  0.5.x.
- Before agreeing the upstream fix, bulkman shall re-run its original
  multiprocess reproduction (audit/evaluations/probe_multiprocess_circuit.py)
  against the published 0.5.0 wheel installed from PyPI against live
  PostgreSQL.
- If bulkman's private-API touchpoints in resilient_circuit (`_status`,
  `_save_state`, `storage.get_state`) changed incompatibly in 0.5.0, then
  bulkman shall adapt its integration before release.
- While upstream `set_state` cannot protect stored HALF_OPEN from blind
  writes, bulkman shall retain its stale-CLOSED persistence guard
  (`_persist_circuit_state`).
- When the pin is updated, the full bulkman test suite (unit + live
  PostgreSQL) and all audit probes shall pass against the installed 0.5.0
  wheel.
- The bulkman CHANGELOG shall bill the 2.0.0 `failure_threshold` fix
  prominently as a behaviour change for callers with the breaker enabled
  (Highway's request).
- When all checks pass, bulkman shall bump version to 2.0.2 and publish to
  PyPI.
- After publication, bulkman shall verify the served artifact by installing
  bulkman==2.0.2 from PyPI in a fresh venv and running a smoke check that
  resolves resilient-circuit>=0.5.0.
- When verification completes, bulkman shall send verify-result to Highway
  (threads thr-6e84571ac5eb46309456 / thr-f057272214d448a18af4), share the
  probe source, and answer mail-api's backlog question.
