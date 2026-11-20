# JSON projection prefetch and speculative query cleanup

Related issue: https://github.com/milvus-io/milvus/issues/50583

## Motivation

Driver prefetch overlaps storage reads with query execution. JSON stats predicates
also need their typed projected columns, or the shared BSON posting index when a
projection is unavailable. Resolving the predicate's execution path alone leaves
those reads on the demand path.

Waiting for every filter input before evaluating any of them prevents an AND/OR
short circuit from skipping a cold sibling. Similarly, a vector search that has
no surviving rows does not need its speculative vector read. Cleanup must still
protect the request context borrowed by active workers.

## Projection selection

`SegmentExpr` resolves the sealed segment's JSON stats layout and prefetches the
projection matching the predicate's JSON path and type. Numeric comparisons warm
both integer and floating-point projections; contains predicates use the array
projection. EXISTS warms matching path components. A missing typed projection
warms the BSON posting index used by shared-data fallback.

An expression set shares a `JsonStatsPrefetchBatch`. It claims each physical
column or posting-index slot once. Like a raw scalar column, a selected
projection submits all its cell IDs in one `PrefetchChunks` call so the cache
can batch cold loads. The call can pin the whole projection until it returns.
Evaluation waits for prefetch and acquires cells as it scans, including when
the cache could not admit the speculative batch.

The expression-result-cache probe uses the request's segment generation, the
expression signature and active row count. Retired generations are misses. The
probe avoids decoding a cached bitmap solely to decide whether to prefetch.

## Consumption and short circuits

Predicates join their own prefetch before using the selected reader. A compound
filter can evaluate its first input while another input is cold. An input skipped
for a whole batch records a deferred cursor advance; it catches up before later
evaluation. Deferred LIKE batching follows the same cursor rule.

The all-at-once optimization remains available when the required inputs are ready.
Otherwise the compound filter starts with normal batches. Vector prefetch is
joined immediately before ANN execution, after empty/fully-filtered exits.

Existing scalar metrics and trace fields keep their separation between prefetch
wait and scalar work. A consumer-scoped accumulator records waits and lazy
preparation at expression consumption. Nested wrapper/leaf waits count once;
other worker threads and driver cleanup do not charge this accumulator. The
filter exports `prefetch_wait_us` and subtracts the accumulated wait from scalar
work in both batched and all-at-once execution. This does not add a Prometheus
metric family or change any metric labels.

## Cancellation and failure handling

Each driver owns a speculative cancellation source, separate from the request
token. `PrefetchExecutor` retracts queued work by releasing its callable and
completing its future. Unregistering the cancellation callback synchronizes with
worker acquisition of that callable. A running worker observes the driver's
token between storage operations.

Driver close signals cancellation before joining operators. Task termination
signals every driver before draining any one driver. Expression cleanup joins
all siblings even when one future throws, then rethrows the first failure.
Cleanup never starts a new lazy lookup for an input that was skipped.

Already-issued or shared storage reads may finish. This design does not promise
immediate interruption of active I/O, and does not cancel the foreground request
when a successful driver stops speculative work.

JSON stats prefetch failures are speculative: a preceding predicate may make the
read unnecessary. If evaluation reaches the predicate, its normal demand reader
retries the read and reports a persistent failure. Existing error propagation
and request cancellation classification are retained. This is not a new retry
policy for storage errors or other kinds of prefetch.

## Scope and validation

This change is independent of the query/load/scheduler metrics work. New native
JSON-prefetch stage timers are tracked with
https://github.com/milvus-io/milvus/pull/55441, separately from this functional
change. No persisted state, storage layout, dependency pin or public API changes.

Regression coverage includes typed projection selection, shared physical reads,
transient and persistent JSON stats read failures, deferred cursor advances,
vector consumption, queued cancellation, stopping between projections, draining
after failure, cache generations, and preserving existing filter trace fields.

These tests establish functional behavior. Any production latency improvement
requires a baseline-versus-patch benchmark with identical prefetch settings.
