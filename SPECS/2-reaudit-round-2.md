# EARS Spec — Re-audit bulkman after migration fixes (round 2)

Ticket: #2 (issuedb)

- While the bulkman package is imported, it shall not import or depend on the trio library.
- When concurrent tasks execute through the bulkhead, the bulkhead shall not lose or duplicate admissions.
- When the bulkhead is shut down, pending tasks shall not leak capacity or futures.
- If a bulkhead operation is cancelled, then all internal counters shall return to their pre-operation values.
- When a user requests shutdown with a timeout, the shutdown shall return within the timeout.
- The BulkheadConfig default shall keep the circuit breaker disabled.
- All existing tests shall pass without modification and without regression.
