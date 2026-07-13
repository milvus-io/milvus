# MEP: Object Storage Metrics and Opt-In Request/Task Storage Profiling

- **Created:** 2026-07-13
- **Author(s):** TBD
- **Status:** Draft
- **Component:** Proxy | QueryNode | DataNode | DataCoord | StreamingNode | Segcore | Storage | Metrics
- **Related Issues:** TBD
- **Released:** TBD

## Summary

This MEP introduces a unified object-storage observability framework for Milvus with two complementary capabilities:

1. Always-on, low-cardinality Prometheus metrics for object-storage operations, latency distributions, bytes, retries, failures, workload attribution, and relevant cache behavior.
2. An explicitly enabled `storage_profile` that collects a fixed-size, mergeable summary for one request or background task, including operation counts, bytes, average/minimum/maximum latency, approximate p50/p95/p99, cache usage, workload phase, and storage role.

The initial implementation instruments logical storage-operation boundaries owned by the Milvus repository. It does not modify milvus-storage. For this phase, the design assumes that one observed logical storage operation is sufficiently representative of one underlying object-storage operation. The model does not require strict distinction in user-facing naming between a logical operation and a provider operation.

The schema and interfaces nevertheless reserve a future provider-access layer for deeper instrumentation inside milvus-storage, including provider retries, internal range requests, time to first byte, and provider-transferred bytes.

The initial implementation does not:

- publish profiles to ClickHouse, Kafka, or another external analytics backend;
- add profile data or metrics to Proxy access logs;
- expose a public request response field, gRPC trailer, or administrator UI;
- place request IDs, task IDs, user IDs, collection IDs, or object paths in Prometheus labels.

Versioned sink, presentation, and access-log extension interfaces are reserved so these capabilities can be added later without changing the core collection model.

## 1. Motivation

Milvus uses object storage across many independent and highly concurrent execution paths:

- QueryNode loads sealed data, indexes, manifests, statistics, and LOB data.
- Search and Query can trigger cold tiered-storage cache-cell loads.
- StreamingNode and DataNode flush growing data into binlogs, statistics, manifests, and delete logs.
- Import reads user-supplied source files and writes Milvus-owned persistent segment data.
- Compaction reads source segments and writes replacement segments.
- Index, Analyze, and Stats jobs read source data and write derived artifacts.
- Load and warmup populate local memory or disk caches.
- Recovery reconstructs state from persisted checkpoints, manifests, and segment data.
- Snapshot, restore, copy-segment, external refresh, and garbage collection issue read, write, stat, list, copy, and delete operations.

Existing storage observability is useful but fragmented:

- Go `RemoteChunkManager` exposes aggregate operation, latency, and size metrics.
- milvus-storage exposes filesystem-global cumulative counts and bytes.
- Tiered-storage caching exposes cache-cell hit and miss bytes.
- `OpContext.storage_usage` can expose scanned total and cold bytes.
- QueryNode records selected segment-access, disk-cache load, and cache-wait metrics.

The existing signals cannot consistently answer:

- Which request or task caused a set of object-storage operations?
- What are the avg, p50, p95, and p99 storage latencies by workload?
- How much data did Import read from source storage versus write to Milvus persistent storage?
- How much storage work came from Flush, Compaction, Index, Load, Recovery, Snapshot, or GC?
- Did a Search wait because of a tiered-storage cache miss?
- Are retries and throttling creating the latency tail?
- Can a distributed Search merge storage usage from multiple QueryNodes?
- Is a reported zero truly zero, or is that path not instrumented?

Prometheus alone cannot safely store per-request or per-task detail because IDs are unbounded labels. Conversely, request/task summaries alone do not replace always-on aggregate dashboards and alerts. Milvus needs both outputs from one semantic instrumentation model.

## 2. Goals

1. Define one stable operation, byte, latency, retry, outcome, workload, phase, storage-role, cache, and coverage model across Go and Milvus-owned C++ code.
2. Add detailed Prometheus histograms and counters that support avg/p50/p95/p99 queries without high-cardinality labels.
3. Add an opt-in `storage_profile` for one request or task.
4. Make profile distributions mergeable across nodes and task attempts.
5. Cover Go `ChunkManager` operations and Milvus-owned C++ call boundaries around Storage V2/V3, segcore, load, compaction, and index workflows.
6. Avoid milvus-storage changes in the initial implementation.
7. Treat observed logical operations as representative object-storage operations for the initial product semantics.
8. Preserve a future provider-access extension without imposing its complexity now.
9. Keep Import distinct from Flush and split Import source reads from persistent writes.
10. Cover directly relevant tiered-storage cache usage and cache-wait signals without redesigning cache architecture.
11. Keep per-scope profiling explicitly enabled and resource bounded.
12. Reserve administrator TTL rules, invoker presentation, task-status presentation, external sinks, and access-log integration.
13. Remain safe during rolling upgrades and partial instrumentation.

## 3. Non-Goals

1. Modifying milvus-storage in the initial implementation.
2. Exact cloud-provider billing or raw HTTP request accounting.
3. Observing retries performed entirely inside a provider SDK or milvus-storage.
4. Exact provider-level GET/PUT/RangeGET counts when one Milvus operation expands internally.
5. Publishing summaries to ClickHouse, Kafka, Pulsar, or OTLP initially.
6. Adding fields or metrics to Proxy access logs initially.
7. Providing a final public SDK API, admin API, or WebUI initially.
8. Adding request, task, trace, user, collection, segment, bucket, endpoint, or object-path labels to Prometheus.
9. Replacing existing cache implementations with a unified cache framework.
10. Attributing OS page-cache or mmap page faults to a request.
11. Charging asynchronous Flush, Compaction, or Index work to one Insert request.
12. Changing cache admission, eviction, storage format, retry, scheduling, or I/O behavior for metrics.

## 4. Core Design Decisions

The initial implementation adopts the following decisions:

- The feature name is `storage_profile`; `analyze` is avoided because it conflicts with existing Analyze tasks and query-plan terminology.
- Aggregate Prometheus metrics are always enabled independently of per-scope profiling.
- Request/task profile detail is disabled by default and explicitly enabled.
- Instrumentation is added only at boundaries owned by the Milvus repository.
- milvus-storage remains unchanged and is treated as an opaque implementation dependency.
- One observed logical operation is treated as one representative object-storage operation for initial dashboards and profiles.
- Metric names may use `storage_operation` rather than exposing a strict `logical` qualifier everywhere.
- Internal data structures retain an access-layer/version field so provider-level data can be added later.
- Import-created SyncTasks inherit Import attribution and are not counted as Flush.
- Import source and persistent storage roles are distinct.
- `workload_kind` is a Prometheus label; `workload_subtype` and `phase` remain profile-only initially.
- Proxy access logs remain unchanged; a future access-log adapter is reserved.
- The initial production `SummarySink` is a noop.

## 5. Terminology

| Term | Meaning |
|---|---|
| Storage operation | One Milvus-observed logical read/write/stat/list/delete/copy operation. Initially assumed representative of one object-storage operation. |
| Provider operation | A future lower-level call inside milvus-storage or a cloud SDK. Not collected initially. |
| Scope | One explicitly profiled request or task. |
| Contribution | One node/component's mergeable profile fragment. |
| Workload kind | Stable request/task family such as `search`, `import`, or `compaction`. |
| Workload subtype | More specific execution type such as `l0_ingest` or `clustering`. |
| Phase | A bounded workload step such as `read_source`, `write_output`, or `warmup`. |
| Storage role | Semantic storage purpose such as `source` or `persistent`. |
| Backend kind | Bounded implementation family: `s3_compatible`, `azure`, `gcp`, `local`, or `unknown`. |
| Cold bytes | Cache-cell bytes that required loading rather than immediate cache service. |
| Cumulative latency | Sum of operation latencies; not necessarily request critical-path latency. |
| Cache wait | Time blocked waiting for cache load/fill. |

## 6. Measurement Model

### 6.1 Access Layer

The serialized model reserves an access layer:

```go
type AccessLayer int32

const (
    AccessLayerMilvus   AccessLayer = 1
    AccessLayerProvider AccessLayer = 2 // reserved, not produced initially
)
```

Only `AccessLayerMilvus` is produced in the initial implementation. `AccessLayerProvider` is reserved for future milvus-storage instrumentation.

Although the internal model retains this distinction, initial dashboards and presentation may call the observations simply “storage operations.” The implementation assumes representative 1:1 behavior unless a specific path documents otherwise.

### 6.2 Operation Taxonomy

```go
type StorageOperation int32

const (
    StorageOperationUnknown StorageOperation = iota
    StorageOperationRead
    StorageOperationRangeRead
    StorageOperationWrite
    StorageOperationStat
    StorageOperationList
    StorageOperationDelete
    StorageOperationCopy
    StorageOperationMultipartCreate
    StorageOperationMultipartWrite
    StorageOperationMultipartComplete
    StorageOperationMultipartAbort
    StorageOperationCount
)
```

Initial mappings:

| Milvus boundary | Operation |
|---|---|
| `ChunkManager.Read` / `Reader` | `read` |
| `ChunkManager.ReadAt` | `range_read` |
| `ChunkManager.Write` | `write` |
| `ChunkManager.Size` / `Exist` | `stat` |
| `ChunkManager.WalkWithPrefix` | `list` |
| `ChunkManager.Remove` | `delete` |
| `ChunkManager.Copy` | `copy` |
| Milvus C++ packed reader call | `read` or `range_read` according to caller intent |
| Milvus C++ writer/flush call | `write` |
| Milvus C++ metadata/file-info call | `stat` |

The operation is a semantic observation, not a claim about the exact number of internal provider RPCs.

### 6.3 Batch and Nested Calls

Metrics count object-level semantic operations rather than an extra outer batch operation.

- `MultiRead`, `MultiWrite`, and `MultiRemove` must not add one batch count in addition to their per-object operations.
- Current implementations that expand batches into unary methods should instrument the unary operations.
- `Path -> Exist -> Stat` and similar nested calls require a suppression/nesting token to avoid double counting.
- A wrapper must not invent per-object latency when it can only observe the total batch duration.
- A future real provider batch API may add a distinct provider-layer batch concept without changing initial logical semantics.

### 6.4 Outcome and Error Category

Outcomes are bounded:

```text
success
failure
canceled
timeout
```

Error categories are bounded:

```text
none
not_found
throttled
permission_denied
invalid_credentials
bucket_not_found
invalid_argument
invalid_range
entity_too_large
unexpected_eof
timeout
canceled
io_failed
unknown
```

Go classification uses typed `merr` errors after provider errors have been mapped at their existing source. It must not use error-message string matching. Metrics classification must never change the returned error, retriability, or Input-vs-System classification.

C++ classification uses the typed Milvus/segcore status available at the Milvus-owned call boundary. If only a generic error is visible, the profile records `io_failed` or `unknown` rather than guessing an internal provider cause.

### 6.5 Byte Semantics

```go
type ByteStats struct {
    Requested uint64
    Completed uint64
}
```

- `Requested` is the number of bytes requested or intended at the observed boundary.
- `Completed` is the number successfully read, written, or copied.

For C++ operations, bytes may come from existing buffer size, file metadata, manifest metadata, packed-reader accounting, output artifact size, or `OpContext.storage_usage`. Instrumentation must not issue an additional Stat/HEAD solely to discover a metric.

If bytes cannot be determined, operation count and latency remain valid while byte coverage is marked unavailable.

Provider-transferred bytes are reserved for the future and are not inferred from logical bytes.

### 6.6 Latency Semantics

The initial design records:

| Metric | Definition |
|---|---|
| Operation duration | End-to-end duration of the Milvus-observed storage call, including visible retry and waiting. |
| Time to first byte | For Milvus-owned streaming readers where observable, time until the first successful non-zero read. |
| Transfer duration | Time until EOF, success, failure, cancellation, or close. |
| Cache lookup duration | Time spent determining a relevant cache hit/miss where already observable. |
| Cache fill duration | Time spent filling a relevant cache where already observable. |
| Cache wait duration | Time a caller is blocked waiting for cache data. |

For parallel operations, cumulative storage duration can exceed the request wall time. Profile fields and future presentation must distinguish:

- operation count;
- cumulative operation latency;
- maximum operation latency;
- request/task wall time;
- cache wait time.

The initial design does not claim a precise storage critical-path contribution.

### 6.7 Backend Kind

```text
unknown
s3_compatible
azure
gcp
local
```

This enum distinguishes remote and standalone/local behavior without exposing endpoint, bucket, credentials, or object names.

## 7. Mergeable Distribution Contract

### 7.1 Motivation

A distributed Search can run on several QueryNodes. Node p99 values cannot be averaged. Each contribution therefore carries mergeable bucket counts, sum, count, min, and max. Proxy or a task coordinator merges distributions before calculating percentiles.

```go
const LatencyBucketSchemaV1 uint32 = 1

type LatencyDistribution struct {
    Count    uint64
    SumNanos uint64
    MinNanos uint64
    MaxNanos uint64
    Buckets  [LatencyBucketCount]uint64
}
```

Internal bucket counts are non-cumulative. Prometheus export converts them to cumulative classic Histogram buckets.

### 7.2 Latency Buckets

Seconds:

```text
0.00025
0.0005
0.001
0.002
0.005
0.010
0.025
0.050
0.100
0.250
0.500
1
2.5
5
10
30
60
120
300
+Inf
```

### 7.3 Size Buckets

```text
1 KiB
4 KiB
16 KiB
64 KiB
256 KiB
1 MiB
4 MiB
16 MiB
64 MiB
256 MiB
1 GiB
4 GiB
+Inf
```

### 7.4 Quantile Semantics

- `avg = sum/count`.
- `min` and `max` are exact at the observed boundary.
- p50/p95/p99 are approximate within bucket resolution.
- Very small sample sets must display count and max alongside percentiles.
- Empty distributions produce unavailable quantiles, not zero.

### 7.5 Schema Compatibility

Every serialized profile contains `schema_version` and `bucket_schema`.

Receivers:

1. Merge matching known bucket schemas.
2. Merge scalar counts/sums/bytes where compatible.
3. Mark quantiles incomplete if any required contribution uses an unknown schema.
4. Never reinterpret unknown bucket boundaries.

## 8. Attribution Model

```go
type Attribution struct {
    ScopeType ScopeType

    TenantID string
    UserID   string

    RequestID   string
    RequestType string
    TraceID     string

    TaskID      string
    TaskAttempt uint32

    Component    string
    NodeID       int64
    CollectionID int64

    WorkloadClass   WorkloadClass
    WorkloadKind    WorkloadKind
    WorkloadSubtype WorkloadSubtype
    Phase            WorkloadPhase
    StorageRole      StorageRole
    BackendKind      BackendKind
}
```

Identifiers are profile fields only. Prometheus receives bounded enum dimensions.

### 8.1 Aggregate Attribution When Profile Is Disabled

Always-on metrics still need workload attribution. Therefore:

- lightweight bounded attribution is propagated for normal requests/tasks;
- request/task IDs and per-scope distributions are allocated only when profiling is enabled;
- the disabled profile path uses a noop recorder but retains component/workload/storage-role/backend dimensions;
- task constructors must set workload attribution even when `storage.profile.enabled=false`.

### 8.2 Workload Classes

```text
request_path
background
recovery
system
```

### 8.3 Workload Kinds

Request kinds:

```text
search
query
insert
delete
upsert
```

Task kinds:

```text
flush
import
compaction
index
load
recovery
snapshot
gc
external_sync
replication   # reserved, not initially selected
```

### 8.4 Workload Subtypes

Examples retained in profiles:

- Flush: `auto`, `manual`, `streaming`.
- Import: `preimport`, `ingest`, `l0_preimport`, `l0_ingest`, `copy_segment`.
- Compaction: `mix`, `level0_delete`, `clustering`, `sort`, `partition_key_sort`, `bump_schema_version`, `single`, `minor`, `major`.
- Index: `build`, `analyze`, `stats_text`, `stats_bm25`, `stats_json_key`.
- Load: `segment_load`, `index_load`, `sync_warmup`, `async_warmup`, `cache_fill`.
- Snapshot: `create`, `restore`, `pin`, `unpin`, `copy`.

### 8.5 Phases

```text
read_source
read_metadata
write_output
write_metadata
copy_object
warmup
cache_lookup
cache_fill
cleanup
```

`phase` remains profile-only initially to avoid multiplying Prometheus time series.

### 8.6 Storage Roles

```text
unknown
source
persistent
```

Import and external-source workflows must distinguish source from persistent storage.

## 9. Import Attribution

Import is not Flush even when it reuses `SyncTask`, SyncManager, pack writers, or the same storage implementation.

### 9.1 PreImport

```text
workload_kind    = import
workload_subtype = preimport
phase            = read_source
storage_role     = source
```

PreImport can scan substantial source data while producing no persistent segment output.

### 9.2 Ingest

Source phase:

```text
workload_kind    = import
workload_subtype = ingest
phase            = read_source
storage_role     = source
```

Output phase:

```text
workload_kind    = import
workload_subtype = ingest
phase            = write_output
storage_role     = persistent
```

An Import-created SyncTask must inherit the parent attribution. Reusable write code must not reset `workload_kind` to `flush`.

### 9.3 L0 Import

`l0_preimport` and `l0_ingest` use the same source/output role split for delete data.

### 9.4 Copy Segment

```text
workload_kind    = import
workload_subtype = copy_segment
phase            = copy_object
storage_role     = persistent
operation        = copy
```

Copied bytes use existing metadata when available. No additional Stat/HEAD is issued for metrics.

### 9.5 Double-Counting Rule

One physical execution updates one active scope recorder. Nested reusable components do not create competing top-level scopes. Import output therefore appears once as Import, not once as Import plus once as Flush.

## 10. Cache Coverage

### 10.1 Included Signals

The initial implementation uses existing Milvus-visible cache signals:

- `OpContext.storage_usage.scanned_total_bytes`;
- `OpContext.storage_usage.scanned_cold_bytes`;
- existing tiered-cache aggregate hit/miss bytes;
- QueryNode segment cache-miss indication;
- cache-load wait duration;
- disk-cache load duration and bytes where already exposed;
- sync and async warmup workload attribution.

### 10.2 Tiered Storage Usage Tracking

Request-level scanned total/cold bytes are available only when the existing tiered-storage usage-tracking path supplies them. The initial enhancement does not modify the external caching layer solely to force collection.

If tracking is disabled or a path does not expose usage:

- cache coverage is `unavailable`;
- values are not reported as zero;
- aggregate cache metrics that already exist remain usable.

### 10.3 Cache Ratios

When data is available:

```text
cached_bytes = scanned_total_bytes - scanned_cold_bytes
byte_hit_ratio = cached_bytes / scanned_total_bytes
```

The ratio is a cache-cell usage measure, not exact provider-byte savings.

### 10.4 Warmup

```text
workload_kind  = load
phase          = warmup
storage_role   = persistent
workload_class = background
```

Async warmup does not inherit a triggering user request profile. It creates a background task scope when task policy enables profiling.

### 10.5 Optional Low-Intrusion Signals

TEXT LOB reader-cache hit/miss counts may be included as auxiliary metadata if available without dependency changes. They are not included in the main byte-hit ratio because reusing a reader does not prove that object data was cached.

DiskANN/index local availability may be represented as a Load subtype, but local index reads are not counted as remote object operations.

### 10.6 Excluded Caches

The initial implementation excludes:

- expression result cache;
- index offset cache;
- index attribute cache;
- geometry cache;
- result/iterator caches;
- filesystem client-instance cache;
- OS page cache and mmap page faults.

These optimize computation, metadata, local files, or client construction rather than directly exposing remote object access.

## 11. Profile and Coverage Data Model

```go
type StorageProfile struct {
    SchemaVersion uint32
    BucketSchema  uint32

    Attribution Attribution

    Operations [StorageOperationCount]OperationStats
    Cache      CacheStats
    Coverage   ProfileCoverage

    StartedAtUnixNano  int64
    FinishedAtUnixNano int64
}

type OperationStats struct {
    Count     uint64
    Success   uint64
    Failed    uint64
    Canceled  uint64
    TimedOut  uint64
    Retried   uint64
    Throttled uint64

    BytesRequested uint64
    BytesCompleted uint64

    Duration LatencyDistribution
    TTFB     LatencyDistribution
    Size     SizeDistribution
}
```

The actual implementation should use fixed arrays instead of maps in hot paths.

### 11.1 Coverage

```go
type CoverageState int32

const (
    CoverageNotApplicable CoverageState = 0
    CoverageInstrumented  CoverageState = 1
    CoveragePartial       CoverageState = 2
    CoverageUnavailable   CoverageState = 3
)

type ProfileCoverage struct {
    GoStorageOperations  CoverageState
    CppStorageOperations CoverageState
    StorageBytes         CoverageState
    StreamingTTFB        CoverageState
    TieredCacheUsage     CoverageState
    CacheWait            CoverageState
    ProviderAccess       CoverageState // unavailable initially
}
```

Coverage prevents unavailable instrumentation from appearing as a zero-value success.

## 12. Core Interfaces

### 12.1 Profile Level and Decision

```go
type StorageProfileLevel int32

const (
    StorageProfileDisabled StorageProfileLevel = 0
    StorageProfileSummary  StorageProfileLevel = 1
    StorageProfileDetailed StorageProfileLevel = 2 // reserved
)

type ProfileDecision struct {
    Requested StorageProfileLevel
    Effective StorageProfileLevel
    Source    ProfileSource
    Reason    ProfileDecisionReason
}

type ProfileDecider interface {
    Decide(ctx context.Context, meta ScopeMeta) ProfileDecision
}
```

`Detailed` remains reserved for future slow-operation samples or provider data.

### 12.2 Recorder

```go
type Recorder interface {
    BeginOperation(meta OperationMeta) OperationRecorder
    ObserveCache(event CacheEvent)
    Snapshot() *StorageProfile
}

type OperationRecorder interface {
    AddCompletedBytes(n uint64)
    FirstByte()
    Finish(result OperationResult)
}
```

Disabled scopes use a singleton noop recorder.

### 12.3 Context Propagation

```go
ctx = storageprofile.WithAttribution(ctx, boundedAttribution)
ctx = storageprofile.WithRecorder(ctx, recorder)
```

Background tasks copy immutable attribution into task metadata and create a fresh context. They must not retain a canceled request context.

### 12.4 Summary Sink

```go
type SummarySink interface {
    Publish(ctx context.Context, profile *StorageProfile) error
    Close(ctx context.Context) error
}
```

Initial production implementation: `NoopSummarySink`.

Future implementations: Kafka, OTLP, ClickHouse, bounded debug store, or another analytics backend.

Sink publication must not block request/task completion on external I/O.

### 12.5 Presentation and Access-Log Extensions

```go
type ProfilePresenter interface {
    Present(ctx context.Context, profile *StorageProfile) error
}

type AccessLogProfileAdapter interface {
    Fields(profile *StorageProfile) []AccessLogField
}
```

Both interfaces are reserved. The initial implementation registers no presenter and no access-log adapter. Proxy access-log format, fields, size, and performance remain unchanged.

A future access-log design must separately address:

- whether only explicitly profiled requests are logged;
- maximum field size;
- whether histogram summaries are allowed;
- tenant/user visibility;
- redaction and security;
- asynchronous background tasks that have no Proxy access log.

## 13. Go Instrumentation

### 13.1 ChunkManager Boundaries

Instrument:

- `Path`
- `Size`
- `Write`
- `MultiWrite`
- `Exist`
- `Read`
- `Reader`
- `MultiRead`
- `WalkWithPrefix`
- `ReadAt`
- `Remove`
- `MultiRemove`
- `RemoveWithPrefix`
- `Copy`

`RootPath` is metadata-only.

Instrumentation can be implemented using a decorator plus targeted unary-method hooks. It must follow the batch/nesting rules in Section 6.3.

### 13.2 Streaming FileReader

```go
type instrumentedFileReader struct {
    inner FileReader
    operation OperationRecorder

    firstByteOnce sync.Once
    finishOnce    sync.Once
    bytesRead     atomic.Uint64
}
```

Required behavior:

- start timing at reader creation/open;
- set TTFB on the first successful non-zero `Read` or `ReadAt`;
- add exactly the returned `n` for partial reads;
- finish success at EOF after valid completion;
- finish failure for non-EOF errors;
- distinguish timeout/cancellation;
- finalize unfinished operations on `Close`;
- avoid double finish under EOF/Close/error races;
- audit callers that do not close readers.

### 13.3 Retry Accounting

Only Milvus-visible retries are counted. A logical operation retains one total duration and a retry count. The implementation does not attempt to expose SDK-internal attempts.

### 13.4 Existing Metrics Migration

Existing `PersistentData*` metrics remain registered during a migration window. New metrics must not silently alter the unit or meaning of an existing histogram. Dashboards migrate explicitly.

## 14. Milvus-Owned C++ Instrumentation

### 14.1 Boundary Rule

The initial implementation changes only source code owned by this Milvus repository. It does not change milvus-storage source, generated headers, filesystem adapters, or provider clients.

Instrument around Milvus-visible calls such as:

- segcore Search/Query storage phases;
- Storage V2/V3 packed reader/writer entry points called from Milvus;
- segment and index Load operations;
- compaction readers/writers;
- index/analyze/stats readers and output writers;
- manifest/statistics operations visible in Milvus code;
- existing `OpContext.storage_usage` snapshots.

### 14.2 Scope Recorder

Milvus C++ can maintain a lightweight recorder in a request/task `OpContext` or operation-owned structure. It records duration/count/known bytes at the Milvus boundary.

Do not use thread-local attribution because work moves across thread pools and one request performs concurrent operations.

### 14.3 Go/C++ Handoff

Where a Go-to-C++ call already returns storage usage fields, extend the existing Milvus-owned result/FFI structure only if it does not require milvus-storage changes. Otherwise mark coverage partial.

Avoid per-operation callbacks from C++ to Go. Aggregate inside the C++ request/task scope and return one snapshot at completion.

### 14.4 Provider Extension Reservation

Future provider instrumentation may add:

- a scoped recorder inside milvus-storage;
- provider operation counts;
- internal range-read/write counts;
- provider TTFB and transfer duration;
- provider-transferred bytes;
- provider-visible retry and throttling;
- a profile snapshot FFI.

These additions use the reserved access layer and coverage fields rather than replacing the initial schema.

## 15. Distributed Request Profiling

### 15.1 Explicit Request Signal

Reserved metadata/header:

```text
x-milvus-storage-profile: summary
```

The initial server may propagate this internally before public SDK exposure. The server policy decides the effective level.

### 15.2 Propagation

Proxy creates request attribution and passes profile level, request ID, trace ID, workload kind, tenant/user identity, and schema version through internal RPCs.

Each QueryNode creates a contribution and attaches a recorder to the corresponding Search/Query execution.

### 15.3 Merge

Proxy merges:

- counts and bytes by addition;
- matching histogram buckets by index;
- min by minimum;
- max by maximum;
- coverage conservatively;
- contribution identities for deduplication.

### 15.4 Contribution Identity

```text
cluster_id / node_id / scope_id / task_attempt / execution_id
```

The merger must handle duplicate delivery, RPC retry, canceled hedged execution, partial node coverage, and old nodes that omit contributions.

### 15.5 Initial Result Handling

The merged profile is passed to `NoopSummarySink`. It is not added to:

- public API responses;
- gRPC trailers;
- Proxy access logs;
- administrator APIs.

These are future presenter integrations.

## 16. Task Profiling

### 16.1 Initial Task Families

```text
flush
import
compaction
index
load
recovery
snapshot
gc
external_sync
```

`replication` remains reserved. Secondary persistence is normally attributed to Flush with a replication origin/subtype.

### 16.2 Attempts

Task retry keeps `task_id` and increments `task_attempt`. Per-attempt summaries remain separate. Future presentation may show final logical usage and cumulative work across attempts.

### 16.3 No Global Counter Delta

Filesystem-global counters cannot be subtracted before/after a task because concurrent tasks share filesystems. Task profile data must come from the task's own Milvus recorder or be marked unavailable.

## 17. Prometheus Metrics

### 17.1 Labels

Bounded labels:

```text
component
workload_class
workload_kind
operation
outcome
error_category
storage_role
backend_kind
cache_tier
cache_result
```

Do not add IDs, names, object paths, buckets, endpoints, or raw errors.

### 17.2 Operation Metrics

```text
milvus_storage_operations_total{
  component, workload_class, workload_kind, operation, outcome,
  storage_role, backend_kind
}

milvus_storage_operation_duration_seconds{
  component, workload_class, workload_kind, operation, outcome,
  storage_role, backend_kind
}

milvus_storage_operation_size_bytes{
  component, workload_class, workload_kind, operation, outcome,
  storage_role, backend_kind
}

milvus_storage_bytes_total{
  component, workload_class, workload_kind, direction,
  storage_role, backend_kind
}

milvus_storage_retries_total{
  component, workload_class, workload_kind, operation, retry_reason,
  storage_role, backend_kind
}

milvus_storage_operation_errors_total{
  component, workload_class, workload_kind, operation, error_category,
  storage_role, backend_kind
}

milvus_storage_operations_inflight{
  component, workload_kind, operation
}
```

Metric help text must say these are Milvus-observed storage operations. It may describe them as representative object-storage operations, but not as exact provider billing requests.

### 17.3 Cache Metrics

Reuse or extend existing low-cardinality metrics where data is already available:

```text
milvus_storage_cache_lookups_total{
  component, workload_kind, cache_tier, cache_result
}

milvus_storage_cache_bytes_total{
  component, workload_kind, cache_tier, cache_action
}

milvus_storage_cache_wait_duration_seconds{
  component, workload_kind, cache_tier, outcome
}

milvus_storage_cache_load_duration_seconds{
  component, workload_kind, cache_tier, outcome
}
```

Cache actions are bounded: `requested`, `served`, `cold`, `filled`, `evicted`.

### 17.4 Profile Control Metrics

```text
milvus_storage_profile_decisions_total{
  source, requested_level, effective_level, reason
}

milvus_storage_profile_active_scopes{scope_type}

milvus_storage_profile_dropped_summaries_total{reason}

milvus_storage_profile_snapshot_duration_seconds{component}
```

### 17.5 PromQL

Average read latency:

```promql
sum(rate(milvus_storage_operation_duration_seconds_sum{
  operation="read", outcome="success"
}[5m]))
/
sum(rate(milvus_storage_operation_duration_seconds_count{
  operation="read", outcome="success"
}[5m]))
```

P95 read latency:

```promql
histogram_quantile(
  0.95,
  sum by (le, workload_kind) (
    rate(milvus_storage_operation_duration_seconds_bucket{
      operation="read", outcome="success"
    }[5m])
  )
)
```

P99 write latency:

```promql
histogram_quantile(
  0.99,
  sum by (le, workload_kind) (
    rate(milvus_storage_operation_duration_seconds_bucket{
      operation="write"
    }[5m])
  )
)
```

Storage throughput:

```promql
sum by (workload_kind, direction) (
  rate(milvus_storage_bytes_total[5m])
)
```

Cache byte hit ratio:

```promql
sum(rate(milvus_storage_cache_bytes_total{cache_action="served"}[5m]))
/
sum(rate(milvus_storage_cache_bytes_total{cache_action="requested"}[5m]))
```

### 17.6 Cardinality Budget

Every metric requires a worst-case Cartesian-product estimate. Enum conversion rejects arbitrary strings. New labels require design review. `workload_subtype` and `phase` are intentionally excluded initially.

`error_category` is exported on a failure-only counter rather than on duration
or size histograms, avoiding an error-category multiplier on histogram series.

## 18. Configuration and Policy

### 18.1 Initial Configuration

```yaml
storage:
  profile:
    enabled: false
    level: summary

    request:
      allowExplicit: false

    task:
      enabled: false
      types: []

    cache:
      enabled: true

    maxActiveScopes: 1024
    maxProfiledRequestsPerSecond: 10
    maxProfiledTasks: 128
```

ParamTable keys:

```text
storage.profile.enabled
storage.profile.level
storage.profile.request.allowExplicit
storage.profile.task.enabled
storage.profile.task.types
storage.profile.cache.enabled
storage.profile.maxActiveScopes
storage.profile.maxProfiledRequestsPerSecond
storage.profile.maxProfiledTasks
```

Aggregate Prometheus metrics are independent of profile enablement.

### 18.2 Task Selection

Use one validated list instead of one boolean per task:

```yaml
storage:
  profile:
    enabled: true
    task:
      enabled: true
      types:
        - flush
        - import
        - compaction
        - index
```

Subtype filters are reserved.

### 18.3 TTL Rule Extension

```go
type ProfileRule struct {
    ID        string
    Level     StorageProfileLevel
    ExpiresAt time.Time
    Selector  ProfileSelector
}

type ProfileRuleProvider interface {
    Match(meta ScopeMeta) *ProfileRule
}
```

The initial provider returns no rule. A later etcd-backed admin service can match tenant, collection, workload, subtype, task ID, request type, or node.

### 18.4 Decision Order

```text
global feature enabled?
  → administrator TTL rule
  → long-lived task config
  → explicit invoker request
  → permissions
  → active/rate limits
  → effective level
```

## 19. Performance and Safety

### 19.1 Disabled Path

- Use noop scope recorder.
- Do not allocate per-scope buckets.
- Do not serialize contributions.
- Continue aggregate metrics with bounded attribution.
- Do not create extra C++/FFI dependency handles.

### 19.2 Enabled Summary Path

- Use fixed arrays, not sample lists.
- Serialize only non-zero operation entries where practical.
- Maintain TTFB only for observable streaming reads.
- Maintain size distributions only where size is known.
- Keep provider coverage unavailable.

### 19.3 Limits

- maximum active scopes;
- maximum profiled request rate;
- maximum profiled tasks;
- maximum contribution size;
- bounded operation/phase entries;
- future sink queue capacity.

Limit exhaustion disables or downgrades profiling without failing the request/task.

### 19.4 Security and Privacy

- No credentials, endpoints, buckets, paths, expressions, vector data, or error messages in metrics.
- Profile identifiers remain internal until a presentation design defines authorization.
- Future invoker presentation can expose only the caller's request.
- Future admin presentation requires existing Milvus authorization.
- Future access-log integration requires separate redaction and size review.

### 19.5 Benchmarks

Benchmark:

- pre-change baseline;
- aggregate metrics with profile disabled;
- profile summary enabled;
- small streaming reads;
- large sequential reads;
- concurrent range reads;
- cache-hit and cache-miss Search;
- Go and C++ observed boundaries;
- high-concurrency Flush/Compaction/Import.

Performance budgets must be based on measured baselines.

## 20. Compatibility

### 20.1 Configuration

Defaults preserve current behavior. Unknown task types are rejected during validation.

### 20.2 RPC

Contribution fields are optional new protobuf fields. Older nodes omit them. Receivers treat omission as unavailable coverage, not zero usage.

### 20.3 Schema

Profiles carry schema and bucket versions. Unknown distributions do not merge silently.

### 20.4 Metrics

Existing storage metrics remain during migration. Units and meanings are never changed in place. Dashboards move to the new metric set explicitly.

### 20.5 milvus-storage

No initial dependency update is required. Future provider instrumentation must be separately reviewed and versioned.

## 21. Detailed Implementation Plan

### Phase 0: Semantic Contract

1. Land this MEP.
2. Freeze enums, bucket schema, workload kinds, phases, storage roles, outcomes, and error categories.
3. Decide the shared Go/C++ definition source for Milvus-owned code.
4. Document legacy metric migration.
5. Define representative-operation wording and explicitly disclaim billing accuracy.

### Phase 1: Common Profile Library

Choose `internal/storageprofile` unless cross-module use requires `pkg`. Respect the separate `pkg` Go module.

Implement:

- enums and validation;
- bounded attribution;
- fixed latency/size distributions;
- merge and quantile helpers;
- coverage model;
- noop and active recorders;
- decision/policy interfaces;
- noop sink;
- reserved presenter/access-log interfaces;
- profile-control metrics;
- unit and cardinality tests.

### Phase 2: Prometheus Metrics

1. Add operation Counters and Histograms.
2. Use seconds for duration Histogram names ending in `_seconds`.
3. Add bytes Counters and size Histograms.
4. Add retry/error/outcome dimensions.
5. Integrate existing cache signals without duplicating old metrics.
6. Register metrics in each component registry.
7. Add PromQL examples and dashboard panels.
8. Retain old metrics through a documented migration window.

### Phase 3: Go Storage Instrumentation

Primary areas:

```text
internal/storage/types.go
internal/storage/remote_chunk_manager.go
internal/storage/local_chunk_manager.go
internal/flushcommon/
internal/datanode/importv2/
internal/datanode/compactor/
internal/datacoord/
pkg/metrics/persistent_store_metrics.go
```

Tasks:

1. Add ChunkManager logical instrumentation.
2. Add streaming FileReader wrapper.
3. Audit batch/nested double counting.
4. Audit Milvus retry boundaries.
5. Attach bounded workload attribution.
6. Attach profile recorders when enabled.
7. Preserve typed error classification.
8. Cover Import source/persistent phases.
9. Add Local/MinIO/Azure/GCP behavior tests as applicable without provider-layer instrumentation.

### Phase 4: Milvus-Owned C++ Instrumentation

Primary areas:

```text
internal/core/src/segcore/
internal/core/src/storage/
internal/core/src/query/
internal/core/src/indexbuilder/
internal/storagev2/
internal/querynodev2/segments/
```

Tasks:

1. Identify each Milvus-owned call into packed reader/writer/storage functions.
2. Add operation-scope timers and known-byte accounting.
3. Attach storage usage to existing request/task OpContext where possible.
4. Return one scope snapshot using existing Milvus-owned result structures or minimal FFI additions.
5. Do not alter milvus-storage APIs or implementations.
6. Mark unavailable bytes/TTFB/cache data honestly.
7. Verify shared thread pools do not leak attribution.

### Phase 5: Task Attribution

Instrument:

1. Flush and streaming sync/flush.
2. PreImport, Import, L0 variants, CopySegment.
3. Compaction variants.
4. Index build, Analyze, Stats.
5. Load and warmup.
6. Recovery.
7. Snapshot/restore/pin/copy.
8. GC.
9. External refresh/backfill.

Parent scope inheritance is mandatory for reusable sub-tasks.

### Phase 6: Distributed Request Contributions

1. Accept internal explicit profile decision at Proxy.
2. Propagate through Search/Query RPCs.
3. Create QueryNode contributions.
4. Merge at Proxy.
5. Deduplicate retries/hedges.
6. Pass merged result to noop sink.
7. Do not modify Proxy access logs or public responses.

### Phase 7: Validation and Rollout

1. Compare new and legacy aggregate metrics.
2. Run fault injection for storage failures.
3. Validate Search, Import, Flush, Compaction, Index, Load, Recovery, Snapshot, and GC.
4. Measure disabled/enabled overhead.
5. Test rolling upgrades.
6. Add dashboard documentation.
7. Keep feature disabled by default until verification gates pass.

## 22. Verification Plan

### 22.1 Source Audit

Audit all Milvus-owned storage call sites:

- ChunkManager methods;
- FileReader usage and missing Close calls;
- retry wrappers;
- Storage V2/V3 call boundaries;
- segcore load/search/query calls;
- import readers and SyncTasks;
- flush pack writers;
- compaction readers/writers;
- index/analyze/stats tasks;
- snapshot/copy/GC/external paths;
- error construction and rewriting.

Each path must have coverage or an explicit exclusion.

### 22.2 Failure Matrix

| Failure | Outcome/category | Expected behavior |
|---|---|---|
| Key missing | failure/not_found | preserve existing non-retry semantics |
| Throttling visible to Milvus | failure/throttled | count visible retry |
| Permission denied | failure/permission_denied | no inappropriate retry |
| Invalid credentials | failure/invalid_credentials | no inappropriate retry |
| Timeout | timeout/timeout | preserve policy |
| Cancellation | canceled/canceled | do not classify as failure |
| Unexpected EOF | failure/unexpected_eof | count visible retry where allowed |
| Generic I/O | failure/io_failed | preserve typed error |

The design does not claim visibility into hidden SDK/milvus-storage retries.

### 22.3 End-to-End Scenarios

1. Search with all cache hits.
2. Search with partial cache misses across QueryNodes.
3. Search with parallel reads.
4. Search canceled during streaming read.
5. Query/retrieve cold scalar access.
6. Sync and async warmup.
7. Auto and manual Flush.
8. PreImport source scan.
9. Import source read plus persistent write.
10. L0 Import.
11. CopySegment.
12. Mix/L0/clustering/sort/schema compaction.
13. Index build, Analyze, Stats.
14. Segment/index Load.
15. Streaming recovery.
16. Snapshot create/restore/copy.
17. GC list/delete and partial failure.
18. External refresh/backfill.
19. Rolling upgrade with missing contributions.

### 22.4 Concurrency

- Concurrent tasks do not receive each other's profile data.
- One scope safely records concurrent operations.
- C++ worker pools do not leak attribution.
- EOF/Close/error races do not double-finish.
- Async warmup does not inherit user request identity.
- Duplicate contributions are deduplicated.

### 22.5 Distribution Tests

- Merge is associative and commutative.
- Merged buckets match a combined recorder.
- Average uses merged sum/count.
- Min/max merge correctly.
- Quantile error is bounded by bucket width.
- Empty/one-sample behavior is defined.
- Unknown bucket schema produces incomplete coverage.

### 22.6 Cardinality

- No ID/name/path is accepted as a metric label.
- Every label value is a bounded enum.
- Worst-case time-series count is calculated.
- New labels fail review without a budget.

### 22.7 Adversarial Review

Before review, answer:

- Which Milvus storage boundary remains uninstrumented?
- Which value is unavailable rather than zero?
- Could a batch or nested call double count?
- Could Import output also appear as Flush?
- Could a shared global counter be mistaken for task-local data?
- Which retry remains hidden in milvus-storage or an SDK?
- Which cache metric is only an approximation?
- Could a label receive an arbitrary string?
- Did any Proxy access-log path change accidentally?

## 23. Risks and Tradeoffs

### 23.1 Representative 1:1 Assumption

The initial design assumes one observed Milvus storage operation sufficiently represents one underlying object operation. This keeps naming and product semantics simple, but internal milvus-storage expansion can make counts approximate. Documentation must avoid billing claims.

### 23.2 Incomplete Provider Visibility

Provider retries, TTFB, range-request count, and transferred bytes remain unavailable. The reserved provider layer supports a future extension if optimization needs prove the logical boundary insufficient.

### 23.3 C++ Granularity

Some C++ operations are higher-level packed reads/writes. Their latency is still useful for workload optimization but may combine several lower-level operations.

### 23.4 Cache Interpretation

Scanned total/cold bytes are cache-cell accounting, not guaranteed network bytes. Cache coverage can be unavailable when existing tracking is disabled.

### 23.5 No Persistence or Presentation

The initial noop sink validates collection and merge behavior but provides no historical request/task view. This intentionally separates instrumentation from analytics storage and product UI.

### 23.6 Always-On Metric Cost

Histograms add per-operation cost. Benchmarks and a bounded label set are mandatory.

## 24. Alternatives Considered

### 24.1 Modify milvus-storage Immediately

Deferred. It would provide deeper accuracy but requires cross-repository API, FFI, dependency release, provider adapters, and rolling compatibility before the core model is validated.

### 24.2 Request/Task IDs in Prometheus

Rejected because of unbounded time-series cardinality.

### 24.3 Filesystem Global Counter Delta

Rejected because concurrent tasks share filesystem state.

### 24.4 Per-Operation Logs

Rejected as the primary mechanism because high-volume operations create excessive logging and ingestion cost.

### 24.5 Proxy Access-Log Fields Now

Deferred. Access-log size, latency, privacy, public semantics, and background tasks require a separate presentation decision. An adapter is reserved.

### 24.6 Distributed Tracing Only

Rejected because traces are sampled and do not replace aggregate metrics or deterministic opt-in summaries.

### 24.7 Ring-Buffer Percentiles

Rejected because samples do not merge deterministically across nodes. Fixed buckets do.

### 24.8 Central Aggregator Now

Rejected because it introduces HA, storage, backpressure, retry, deduplication, retention, and authorization before the data contract is proven.

## 25. Future Extensions

- Scoped provider instrumentation in milvus-storage.
- Provider GET/PUT/RangeGET/retry/TTFB/bytes metrics.
- Administrator TTL rules stored in etcd.
- Public invoker profile response/trailer.
- Admin profile API and UI.
- Task-status profile display.
- Kafka/OTLP/ClickHouse sinks.
- Bounded recent-profile debug store.
- Sampled detailed slow-operation records.
- Prometheus exemplars linked to trace IDs.
- Optional phase/subtype aggregate labels after cardinality measurement.
- Proxy access-log profile adapter after a separate access-log design review.

## 26. Open Implementation Questions

These do not block the semantic design:

1. Final Go package location (`internal` versus `pkg`) given module boundaries.
2. Which internal protobuf owns contribution messages.
3. Which existing C++ result structures can carry snapshots without broad FFI changes.
4. Whether initial task profiles are immediately discarded by noop sink or temporarily retained in tests/debug builds.
5. Measured overhead budgets for aggregate and summary modes.

## 27. Completion Criteria

The enhancement is ready for production review only when:

1. Go and C++ Milvus-owned operation semantics are consistent and tested.
2. All agreed Milvus storage boundaries have coverage or explicit unavailable state.
3. Prometheus dashboards correctly calculate avg/p50/p95/p99.
4. No high-cardinality identifiers appear in labels.
5. Distributed Search/Query contributions merge correctly.
6. Import source/persistent roles are distinct and Import output is not counted as Flush.
7. Cache total/cold/wait semantics are documented and verified where available.
8. Retry, throttle, timeout, cancel, partial-read, and EOF behavior is traced end to end at visible boundaries.
9. Disabled and enabled performance benchmarks meet agreed budgets.
10. Rolling upgrades report partial/unavailable coverage rather than false zero values.
11. milvus-storage remains unchanged in the initial release.
12. Proxy access logs remain unchanged in the initial release.
13. The production SummarySink is noop and introduces no external dependency.
