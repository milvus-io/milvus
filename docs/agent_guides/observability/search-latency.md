# Search latency attribution

Use request-level metrics to locate the component, then inspect the native stage
metrics below. Do not sum histogram means or quantiles from different populations.
A request can search several segments concurrently; its critical path follows the
slowest segment, while batch and column-group histograms count individual work units.

## Metric contract

`internal_core_query_stage_duration_seconds{stage,result}` records wall time in
seconds. `stage` is a fixed enum and `result` is `success` or `error` (including
cancellation). It has 20 explicit finite buckets from 1 microsecond to 600 seconds,
plus the implicit `+Inf` bucket. `_count` also counts completed stage attempts.

`internal_core_query_stage_inflight{stage}` tracks active scoped timers, including
blocked work. Prefetch/storage-batch queue stages are observed after dequeue;
the Load worker queue stages are observed at worker completion. They do not
track queue depth with this gauge. Use the existing executor/pool queue metrics
for queue depth. There are 28 stage values: at most 56 histograms and 28 gauges,
1316 exported time series per process. Labels contain no collection, segment,
field, column-group, path or trace identifiers and are unchanged by collection
metric aggregation mode. Handles and finite buckets are initialized together on
first use, with no label-map construction on the observation hot path.

| Stage | Observation population and boundary |
|---|---|
| `search_prepare` | Per native search: lazy schema check, read-lease acquisition, schema compatibility and external-field validation, before invoking `segment->Search`. |
| `search_execute` | Per executed native segment search, including operators, their initialization and prefetch waits. Inaccessible-field empty results skip this stage. |
| `vector_prefetch_queue`, `mvcc_prefetch_queue` | Per submitted prefetch task: submission to worker entry. A dequeued task records queue time even if it is then cancelled. |
| `vector_prefetch_run`, `mvcc_prefetch_run` | Per worker invocation of vector/timestamp prefetch. Includes any cold reader initialization and cache loading performed by the call. |
| `vector_prefetch_wait`, `mvcc_prefetch_wait` | Per valid prefetch future: blocking `get()` on the consumer, including cleanup waiting. The future is consumed once, so repeated `WaitPrefetch` calls do not duplicate observations. This overlaps background prefetch execution. |
| `field_prefetch_prepare` | Per whole-field prefetch with a column: `num_chunks()` and chunk-ID construction. Includes lazy proxy/column-group synchronization and first materialization. |
| `field_prefetch_load` | Per whole-field prefetch: the complete `PrefetchChunks` call. Includes cache admission, existing-load waits, data I/O, conversion and cell publication; warm-cache calls are also counted. |
| `manifest_reader_open` | Per attempted lazy V3 column-group construction: opening the chunk reader (synchronous `get_chunk_reader()` with its status check, or `OpenChunkReadersAsync` when async loading is enabled). Includes metadata-cache lookup, cold footer reads and format-reader initialization. |
| `manifest_translator` | Same construction, after reader open: `ManifestGroupTranslator` construction, including row counts, size estimates, cell planning and allocations. These metadata getters operate on the opened reader. |
| `manifest_cache_slot` | Same construction, after translator construction: cache-slot creation in `ChunkedColumnGroup::EnsureMaterialized`. This lazy path fixes warmup to disabled. |
| `manifest_group_wait` | Per caller that finds another caller holding the shared column group's construction mutex; the wait is not cancellable. Ready groups do not generate another build observation. |
| `manifest_load_cells` | Per `ManifestGroupTranslator::get_cells_async` (also used by its synchronous wrapper): planning, batch admission/submission, joins and result collection. Includes failed attempts. |
| `load_batch_budget_wait` | Per batch: `LoadAdmissionController` admission of the batch's transient memory, including cancellation before admission. This is distinct from milvus-common cache resource reservation. Applies to both file-reader and manifest-reader batches. |
| `load_batch_queue` | Per dequeued batch: storage-pool submission to worker entry, after budget admission. Pool submission failures are not dequeued samples. |
| `manifest_read_batch` | Per manifest batch: `ChunkReader::get_chunks`. Includes reader synchronization, remote/local reads and decoding; not pure S3 service latency. Returned Arrow errors are explicitly counted as errors. |
| `manifest_build_chunk` | Per loaded cell: Arrow normalization, chunk construction and optional mmap-file creation/population. Several cells may build in parallel. |
| `fill_primary_keys` | Per segment result: the entire PK fill call, including the segment read lock and any cold reads. |
| `load_indexes_batch`, `load_column_groups_batch` | Whole index/column-group batch wall time in Load or Reopen, including submission and worker waits. |
| `load_indexes_wait`, `load_column_groups_wait` | WaitAllFutures inside the corresponding batch. Overlaps queue/run work. |
| `load_index_queue/run`, `load_column_group_queue/run` | Per completed MIDDLE worker: submission to entry, then execution; both share the worker outcome. See [Cold Load](cold-load-stages.md) for the separate per-Load phase population. |

On master, the coroutine-based async load pipeline also records
`manifest_load_cells` and `manifest_build_chunk`. Its reader, admission, and
executor internals bypass the legacy `LoadCellBatchAsync`/`get_chunks` path, so
`load_batch_budget_wait`, `load_batch_queue`, and `manifest_read_batch` describe
only calls that enter those legacy functions. Missing samples from those three
stages do not mean async reads had no I/O or waiting.

The new native family is exported through the existing core registry,
`GetCoreMetrics()` and Go `CRegistry`; existing metric names/label sets are retained.
Existing `internal_cgo_queue_duration_seconds{pool="search"}` covers the executor
queue whose per-task INFO logs were removed.

The Go `milvus_qv_stage_duration_seconds` family retains the existing Search,
load-info conversion and reduction stages. Two missing stages replace timing-log
fields: `{component="queryNode",operation="search_task",stage="l0_rerank"}` and
`{component="queryNode",operation="reduce",stage="source_mapping"}`.

## Interpreting cold reads

A normal V3 data file carries `file_size` and `footer_size` in the manifest, so
reader initialization does not need a HEAD request to discover its size. A cold
footer can still require Range GET. `manifest_reader_open` deliberately measures
the whole call rather than naming all of that time S3 or HEAD latency.

The existing `internal_cache_load_latency_microseconds` measures `get_cells`, not
the full pin/load lifecycle. Cache resource reservation in milvus-common remains
outside that timer. `field_prefetch_load` covers the enclosing operation, and the
new manifest/batch stages locate work inside it; the remaining gap still combines
cache admission, synchronization and publication. Do not label a difference of
histogram means as exact reservation time: cache-hit/miss populations, concurrent
waiters and batch counts differ. Exact per-field latency is also unavailable for
fields sharing a column group; the group is constructed and loaded once.

## Queries

Mean stage duration in milliseconds (add the deployment's instance/pod selector):

```promql
1000 * sum by (pod, stage) (
  rate(internal_core_query_stage_duration_seconds_sum[5m])
) / sum by (pod, stage) (
  rate(internal_core_query_stage_duration_seconds_count[5m])
)
```

P95 of completed successful attempts:

```promql
1000 * histogram_quantile(0.95, sum by (pod, stage, le) (
  rate(internal_core_query_stage_duration_seconds_bucket{result="success"}[5m])
))
```

Stages still blocked or executing:

```promql
sum by (pod, stage) (internal_core_query_stage_inflight{stage!~".*_queue"})
```

Start with `search_prepare` versus `search_execute`; inside execution compare
prefetch queue/run/wait, then `field_prefetch_prepare` versus `field_prefetch_load`.
For preparation inspect reader/translator/cache-slot and shared-build waits. For
loading inspect batch budget/queue, reads and chunk construction. Check error
counts and inflight together so a stalled or failed attempt does not disappear
from the diagnosis. Request access/error logs and tracing remain available.
The per-field `convertJSONKeyStats` INFO log retains master's fields, including
the file list. Per-task successful start/end/timing INFO logs are replaced by
the stage metrics described above.

## Key packages and validation

- Native metric declaration, fixed labels, buckets and timers:
  `internal/core/src/monitor/QueryMetrics.{h,cpp}`.
- Lazy materialization / full-field prefetch:
  `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`,
  `internal/core/src/mmap/ChunkedColumnGroup.h`.
- Prefetch worker and consumer boundaries:
  `internal/core/src/exec/operator/{VectorSearchNode,MvccNode}.h`.
- Batch budget/queue/read and chunk construction:
  `internal/core/src/segcore/memory_planner.cpp`,
  `internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.cpp`.
- Go stage owners: `internal/querynodev2/tasks/search_task*.go`.

`QueryMetricsTest.cpp` checks exported finite buckets, seconds, one observation
per timer, exceptional and returned failures, concurrent recording and inflight
cleanup. `LazyManifestPreservesInitialMultiFieldTask` also verifies that concurrent
field access opens/builds the shared group only once and warm access does not
create another reader sample. `LazyManifestFirstNonCancellationFailureIsRetryable`
checks that the real failed load is observed without changing retry/error behavior.
