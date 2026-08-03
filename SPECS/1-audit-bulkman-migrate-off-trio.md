# EARS Spec — Audit bulkman: migrate off trio, CB off by default, fix bugs/races

Ticket: #1 (issuedb)

- While the bulkman package is imported, it shall not import or depend on the trio library.
- When a user constructs a Bulkhead without explicitly enabling circuit breaking, the circuit breaker shall be disabled by default.
- When concurrent tasks execute through the bulkhead, the bulkhead shall not lose or duplicate admissions.
- If the bulkhead is closed, then tasks shall fail with the documented exception.
- When capacity is released, the bulkhead shall admit waiting tasks.
- All existing tests shall pass without modification and without regression.
- The async API shall work with asyncio and trio event loops.
- The sync API shall work without requiring an event loop.
