# Cold Load latency attribution

These metrics answer two separate questions: where each completed segment load
spent wall time, and which manifest caller spent time across the existing FFI.
They add observability only; they do not change load concurrency, cache capacity,
retry policy, or lazy loading.

## Completion populations and containment

`milvus_qv_stage_duration_seconds{component="queryNode",operation="segment_load_attempt",stage,result}`
records every segment read-preparation attempt (`segmentResource.prepare`), even
when the periodic timing log has not filled its batch. All 17 stages share the
attempt's final outcome and observation count. Stages not reached contribute zero.
The existing `segment_load/total` outcome follows the same rule.

```text
segment_load_attempt.total
├── reserve_resource
├── physical_load
│   ├── new_segment
│   │   └── existing segcore conversion/marshal/NewSegment timers
│   │       └── manifest_create (its own manifest-call population)
│   ├── load_segment
│   │   └── sealed_load
│   │       ├── sealed_prepare: StartLoadData + separateLoadInfoV2 + preparation
│   │       │   └── manifest_preload
│   │       ├── load_pool_queue: Go pool submission → worker entry
│   │       ├── local_segment_load
│   │       │   ├── csegment_load: Go/C++ boundary, native executor and Load
│   │       │   └── sync_json_stats
│   │       │       └── manifest_postsync
│   │       └── sealed_post_load: field index bookkeeping + patchEntryNumber
│   ├── delta_logs
│   └── pk_candidate
├── release_resource
└── on_loaded: read-state preparation for the view
```

`physical_load` and its children are reached only when no loaded base of the
segment can be reused for the requested read.

Parent stages include children. Small unlisted wrapper, logging and cleanup
intervals remain in their parents; these Go stages are not an exact partition.
Do not add parent and child means. Manifest reads, native Load calls, and Go
preparation attempts are different populations, especially on failures or retries.

## Native Load phases and parallel tasks

`internal_core_segment_load_duration_seconds{stage,result}` records one sample
for each of 14 phases and `total` per invocation of
`ChunkedSegmentSealedImpl::Load`. These phases partition native Load wall time,
share its final success/error outcome, and include time up to an exceptional
exit. Phases not reached are zero. Empty diff phases still include their checks.
Native cancellation is included in `error`, matching the native stage family.

| Phase, in execution order | Boundary |
|---|---|
| `lock_wait` | Existing reopen mutex acquisition. |
| `prepare` | Published snapshot, mutable load-info copy, GetLoadDiff. |
| `clone_state` | Clone runtime/published state, construct staged committer. |
| `indexes` | Load and replace index batches, including all worker waits. |
| `reload_columns` | Reload existing columns. |
| `column_groups` | Manifest column groups, reader creation, eager/lazy group batches. |
| `text_lob` | Initialize text LOB paths. |
| `field_data` | Load and replace legacy binlog batches. |
| `text_indexes` | Load text-index batches. |
| `json_stats` | Load and replace JSON stats/indexes. |
| `default_fields` | Fill default-value fields. |
| `create_text_indexes` | Create missing text indexes. |
| `finalize` | Drop/retire obsolete resources and finalize staged state. |
| `publish` | Compact runtime info, build delta, publish, return cleanup. |

The existing `internal_core_query_stage_duration_seconds` family adds:

- `load_indexes_batch`, `load_column_groups_batch`: whole batch wall time.
- `load_indexes_wait`, `load_column_groups_wait`: the contained WaitAllFutures.
- `load_index_queue/run`, `load_column_group_queue/run`: MIDDLE pool submission
  to worker entry, then the complete worker body, including staged commit.

Queue/run observations are emitted together at worker completion and share that
worker's success/error outcome, including cancellation at entry. Batch/worker
metrics also cover Reopen. Workers can run concurrently: their sums are work
time, not batch latency. Queue gauges do not measure outstanding queue depth.

## Manifest calls

The Go family uses `component="storage"` and one of five fixed operations:
`manifest_create`, `manifest_preload`, `manifest_postsync`,
`manifest_reopen`, `manifest_other`. Resolver-local repeated lookups reuse the
same result and do not produce another FFI read.

Each GetManifestStats attempt records all five stages once with its final outcome:

```text
total
├── properties: construct FFI properties in Go
├── transaction_begin: complete existing loon_transaction_open FFI call
├── get_manifest: convert native manifest to its C representation
└── extract_stats: copy the C stats into Go maps
```

Failures retain time spent in the completed or failed FFI calls; later stages
that were not reached contribute zero. A failed JSON-stats
lookup can be observed as a manifest error even when the existing caller fallback
allows its outer load to succeed.

`transaction_begin` includes native filesystem initialization, cache lookup,
file reads, deserialization, path resolution and any SDK retries performed by
the call. The existing FFI does not expose those internal boundaries or cache
hit/miss, byte and 503/retry counters. These metrics cannot separate them or
attribute the entire call to S3. All instrumentation stays in Milvus and uses
the unchanged dependency ABI. Storage locking and paths are unchanged; labels do not contain object IDs.

## Queries

Add instance/pod selectors and use a window containing only the target round,
including its completions. Mean Go stage duration in milliseconds:

```promql
1000 * sum by (stage) (
  increase(milvus_qv_stage_duration_seconds_sum{
    component="queryNode",operation="segment_load_attempt",result="success"}[5m])
) / sum by (stage) (
  increase(milvus_qv_stage_duration_seconds_count{
    component="queryNode",operation="segment_load_attempt",result="success"}[5m])
)
```

Use the same sum/count expression with
`internal_core_segment_load_duration_seconds` for native phase means.
For manifest attribution, filter operations to
`manifest_(create|preload|postsync)` and group by `operation,stage`.
Check `_count` and result distributions before comparing populations.
Prometheus can scrape between individual phase observations; allow a final
scrape after the workload settles. Historical runs cannot recover missing stages.

## Source and validation

- `internal/querynodev2/qnview/segment_{resource_manager,load_timing}.go`
- `internal/querynodev2/segments/{segment_loader,physical_load_timing}.go`
- `internal/storagev2/packed/{manifest_read_metrics,stats_resolver,ffi_stats,packed_reader_ffi}.go`
- `internal/core/src/monitor/{SegmentLoadMetrics,QueryMetrics}.{h,cpp}`
- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`

Third-party source, dependency versions and build integration remain unchanged.
Milvus tests cover existing FFI success/error paths, resolver origin and local
reuse, and native phase partitioning. The Go manifest tests can link against the unmodified storage
library; they do not validate native cache hit/miss or SDK retry attribution.
