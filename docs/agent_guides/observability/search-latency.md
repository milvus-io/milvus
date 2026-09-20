# Native query stage latency

## Key packages

- `internal/core/src/monitor/QueryMetrics.{h,cpp}`: bounded stage names, histogram buckets, and scoped timers.
- `internal/core/src/segcore/segment_c.cpp`: search preparation and execution boundaries.
- `internal/core/src/exec/operator/{VectorSearchNode,MvccNode}.h`: prefetch queue, execution, and future waits.
- `internal/core/src/mmap/ChunkedColumnGroup.h`: lazy materialization mutex and cache-slot creation.
- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`: deferred reader/translator creation and field prefetch.
- `internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.cpp`: cell loading and chunk construction.
- `internal/core/src/segcore/memory_planner.cpp`: legacy batch admission, executor queue, and synchronous manifest reads.
- `internal/core/src/segcore/SegmentInterface.cpp`: primary-key filling.

## Metrics and populations

`internal_core_query_stage_duration_seconds{stage,result}` measures wall time in
seconds. Each completed instrumentation scope produces one observation, with
`result` equal to `success` or `error`. These are operation outcomes, not an
error-code classification or a retry decision. Labels never contain request,
collection, field, segment, or file IDs.

Finite buckets range from 1 microsecond to 600 seconds, followed by `+Inf`.
`internal_core_query_stage_inflight{stage}` counts active scoped timers, including
blocking waits. Queue stages use completed duration observations and do not
increment the inflight gauge; that gauge is not an executor queue length.

A timer records escaping exceptions as errors and decrements its gauge on scope
exit. `End(true)` handles an unsuccessful return value. `End()` is idempotent.
A failure caught and recovered inside a scope is not an error observation for
that scope. The returned-failure paths currently instrumented explicitly are
load admission cancellation and a failed synchronous chunk-reader result.

## Stage boundaries

| Stage | Boundary |
|---|---|
| `search_prepare` | Lazy schema check, segment read lease, and search access preparation in `AsyncSearch` |
| `search_execute` | `segment->Search`; skipped when an inaccessible vector field produces an empty result |
| `vector_prefetch_queue`, `mvcc_prefetch_queue` | Submission to prefetch worker entry |
| `vector_prefetch_run`, `mvcc_prefetch_run` | Worker prefetch call |
| `vector_prefetch_wait`, `mvcc_prefetch_wait` | Waiting on a present prefetch future; repeated cleanup waits do not add samples |
| `field_prefetch_prepare` | Resolve chunk count and build all-chunk IDs; may materialize a lazy column |
| `field_prefetch_load` | Column `PrefetchChunks`, including cache and storage work |
| `manifest_group_wait` | Acquire the lazy group's materialization mutex, including uncontended acquisitions |
| `manifest_reader_open` | Open a deferred column group's chunk reader, using the selected sync or async implementation |
| `manifest_translator` | Estimate column size and construct its deferred translator |
| `manifest_cache_slot` | Create the cache slot for a deferred group |
| `manifest_load_cells` | `ManifestGroupTranslator::get_cells`, including async-pipeline or legacy loading and failure propagation |
| `manifest_build_chunk` | Build one group chunk from its Arrow tables, on either loading path |
| `load_batch_budget_wait` | Legacy `LoadCellBatchAsync` admission; cancellation returning false records an error |
| `load_batch_queue` | Legacy admitted batch submission to worker entry |
| `manifest_read_batch` | Synchronous `MakeChunkReaderFactory` call to `get_chunks` |
| `fill_primary_keys` | `FillPrimaryKeys`, starting before its shared lock |

The new async load pipeline has its own admission/read scheduling. Its inner
queue and read calls are not covered by the three legacy batch stages above.
The enclosing `manifest_load_cells` and chunk-build stages cover both paths.
Reader/translator/cache-slot stages cover deferred materialization; they are
not totals for all eager segment loads.

Queue observations mean that a worker started, not that its work succeeded.
For example, a vector prefetch cancelled before its worker body records queue
time but no run timer. Tasks rejected before worker entry have no queue sample.
The lazy-group mutex wait is not cancellable in the current implementation;
`manifest_group_wait` does not imply cancellation-aware waiting.

## Reading the measurements

Stages overlap. A search can wait for prefetch while the prefetch worker loads
manifest cells; multiple batches can run in parallel. Do not sum their means
or quantiles to estimate request latency. Stage counts also differ: one search
can cause several reads, chunk builds, or no storage work at all.

For a mean over one stage and outcome population, use matching sums and counts:

```promql
sum by (stage, result) (
  rate(internal_core_query_stage_duration_seconds_sum[5m])
)
/
sum by (stage, result) (
  rate(internal_core_query_stage_duration_seconds_count[5m])
)
```

For p99, retain the histogram bucket label:

```promql
histogram_quantile(0.99,
  sum by (le, stage, result) (
    rate(internal_core_query_stage_duration_seconds_bucket[5m])
  )
)
```

Use the same instance/component/time filters when comparing stages. A zero
sample count means no observed completed operation, not zero latency. Samples
are emitted when operations finish; a stuck active scope is visible through
its inflight gauge instead.

## Validation

`QueryMetricsTest.cpp` checks exported finite buckets and seconds, idempotent
completion, thrown and returned failures, and concurrent count/gauge balance.
Full search/storage integration still needs the corresponding native suite;
these timer tests do not establish request-level coverage or performance gains.
