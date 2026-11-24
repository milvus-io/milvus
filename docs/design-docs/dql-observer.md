# Optional DQL attempt and phase observation

The compiled-in request hook can measure the complete RPC, including readiness
and retry backoff. It cannot observe processing-attempt or scheduler boundaries.
Expose a request-scoped observer through `pkg/extension`, with no global callback,
exporter, new storage, payload capture or metric labels in stock Milvus.

## Contract

- A hook attaches `DQLObserver` to its returned context. Stock requests carry none.
- `BeginAttempt` brackets each `search`, `hybridSearch` or `Query` attempt,
  including preprocessing, queueing, reduction and internal shard retries.
  The retry loop's backoff is outside this interval. This is not QueryNode CPU time.
- An inherited nested call is part of its outer attempt and is not counted twice.
  Finish is deferred on success and errors, and is idempotent.
  Phase callbacks are serialized with completion; callbacks from an already
  completed attempt are ignored, including when a later retry is running.
- Accepted top-level scheduler tasks report `queue` before becoming visible and
  `execute` when processing begins. Rejected enqueues report neither transition.
- Requeries and synthesized reads such as search-by-primary-key vector fetching
  do not overwrite the outer request's phase. Their elapsed time remains included
  in the processing attempt.
- Implementations must be concurrent-safe, bounded and non-blocking. No synchronous
  I/O is permitted. The interface returns no admission decision or result override.
- Completed request history, aggregation, retention and tenant authorization remain
  the caller's responsibility. Nothing is persisted or replicated by this seam.

## Validation

Extension tests cover absence of an observer, nested calls, retries, concurrent
finish and supported phases. Scheduler tests cover accepted external tasks,
requeries, synthesized reads and a full queue. Existing query success, errors,
cancellation and retry behavior must remain unchanged. Full proxy/scheduler tests
run with the repository's native build dependencies and `dynamic,test` tags.
