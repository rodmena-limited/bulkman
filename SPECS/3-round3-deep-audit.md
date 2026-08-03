# EARS Spec — Round-3 deep audit: timeout/threading semantics, load accounting

Ticket: #3 (issuedb)

- When a sync function executes through the async bulkhead with a timeout, the bulkhead shall return the timeout within the configured duration.
- The async bulkhead shall not allow more sync functions to run concurrently than max_concurrent_calls.
- When concurrent tasks execute through the bulkhead, all counters shall remain consistent (total == success + failure, active and in-flight drain to zero).
- If a bridge worker raises BaseException, the returned future shall resolve rather than hang.
- All existing tests shall pass without modification and without regression.
