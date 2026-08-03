# EARS Spec — Eliminate trio from test stack; close remaining uncertainties

Ticket: #4 (issuedb)

- The test suite shall run on the asyncio backend without importing trio.
- The bulkman package shall contain no identifiers or documentation referencing trio.
- When a timeout storm occurs with sync functions, all callers shall receive the timeout promptly and the bulkhead shall recover.
- The half-open circuit breaker success path shall be verified behaviourally.
- All existing tests shall pass without regression.
