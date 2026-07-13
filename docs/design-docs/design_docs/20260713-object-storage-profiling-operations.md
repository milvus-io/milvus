# Object Storage Profiling Operations Guide

This guide accompanies `20260713-object-storage-profiling.md`. It defines the
initial dashboards, migration rules, cardinality budget, and coverage claims
for the first Milvus-owned-boundary implementation.

## Measurement disclaimer

`milvus_storage_*` counts Milvus-observed logical storage operations. One
observation is treated as representative of one object-storage operation for
initial optimization and capacity analysis, but it is not an exact provider
request count and must not be used for cloud billing reconciliation. A single
Milvus operation can expand into internal milvus-storage or provider-SDK
requests and retries that are intentionally unavailable in this release.

Completed bytes are logical bytes visible at the observed boundary. Tiered
cache scanned/cold bytes are cache-cell accounting, not guaranteed network
transfer bytes. Cumulative operation duration can exceed request wall time
when operations run in parallel.

## Shared Go/C++ contract decision

Go owns the versioned serialized profile taxonomy, fixed histograms, merging,
coverage, and Prometheus labels in `internal/storageprofile`. Milvus-owned C++
does not duplicate that full taxonomy: it emits a bounded raw read snapshot
through the nested `segcore.StorageProfileStats` handoff, and QueryNode folds
that snapshot immediately into the Go histogram schema. This keeps one
serialized definition source while avoiding per-operation C++→Go callbacks or
milvus-storage API changes.

## Dashboard panels

The initial dashboard uses the following panels. Every latency panel displays
operation count and maximum observed latency next to percentiles so small
sample sets are not over-interpreted.

| Panel | Query or source | Notes |
|---|---|---|
| Operation rate | `rate(milvus_storage_operations_total[5m])` | Split by workload, operation, outcome, role, and backend as needed. |
| Average latency | duration sum divided by duration count | Never average node-level averages. |
| p50/p95/p99 latency | `histogram_quantile` over merged `le` buckets | Filter by operation and outcome. |
| Read/write/copy throughput | `rate(milvus_storage_bytes_total[5m])` | Logical completed bytes. |
| Operation-size distribution | `milvus_storage_operation_size_bytes` | Available only when the boundary knows completed size. |
| Visible retry rate | `rate(milvus_storage_retries_total[5m])` | Excludes SDK-internal retries. |
| Error categories | `rate(milvus_storage_operation_errors_total[5m])` | Bounded Milvus-visible categories; raw provider error text is never a label. |
| In-flight operations | `milvus_storage_operations_inflight` | Useful for saturation and stuck-reader detection. |
| Cache byte hit ratio | served divided by requested cache bytes | Cache-cell usage ratio, not provider savings. |
| Cache wait/load p95 | cache wait/load histograms | Shown only where coverage is available. |
| Profile decisions/drops | profile decision and dropped-summary counters | Alert on active/rate/serialization drops. |
| Profile active scopes | `milvus_storage_profile_active_scopes` | Split by request/task. |

### PromQL examples

Average successful read latency:

```promql
sum(rate(milvus_storage_operation_duration_seconds_sum{
  operation="read", outcome="success"
}[5m]))
/
sum(rate(milvus_storage_operation_duration_seconds_count{
  operation="read", outcome="success"
}[5m]))
```

p50/p95/p99 read latency by workload:

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

Change `0.95` to `0.50` or `0.99` for p50 or p99.

p99 write latency:

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

Logical storage throughput:

```promql
sum by (workload_kind, direction) (
  rate(milvus_storage_bytes_total[5m])
)
```

Tiered-cache byte hit ratio:

```promql
sum(rate(milvus_storage_cache_bytes_total{cache_action="served"}[5m]))
/
sum(rate(milvus_storage_cache_bytes_total{cache_action="requested"}[5m]))
```

Visible retry ratio:

```promql
sum(rate(milvus_storage_retries_total[5m]))
/
sum(rate(milvus_storage_operations_total[5m]))
```

## Legacy metric migration

The existing `PersistentData*` metrics remain registered and retain their
current units and meaning. Initial dashboards add the new metrics without
rewriting legacy panels in place. After operators have compared both sets for
one complete minor-release window, dashboards may switch their default panels
to `milvus_storage_*`. Removing a legacy metric requires a separate compatibility
change after that window; this implementation does not remove or rename one.

Expected differences during comparison are documented rather than treated as
bugs: the new metrics use semantic unary-operation counting, suppress nested
operations, separate workload/storage role, and count only Milvus-visible
retries.

## Cardinality budget

All metric label values come from closed enums. Identifiers, paths, endpoints,
buckets, collection names, task IDs, request IDs, and raw errors are rejected
as labels.

For `milvus_storage_operations_total`, the deliberately conservative global
Cartesian upper bound is:

```text
12 components
* 5 workload classes
* 15 named workload kinds plus unknown
* 12 operation values
* 4 outcomes
* 3 storage roles
* 5 backend kinds
= 691,200 possible global label combinations
```

The code-level test derives the same bound from the enum counts and excludes
the enum sentinels. A normal
process emits one component and only the workload/operation combinations that
the component implements, so its reachable set is far smaller than the closed
Cartesian bound. Histogram bucket, sum, and count series multiply the reachable
duration/size combinations; dashboards must therefore aggregate existing
series and must not pre-initialize the Cartesian product.

Error categories use a separate counter rather than multiplying operation
latency and size histograms. Its conservative closed-world bound is 2,419,200
combinations (the operation bound without outcome, multiplied by 14 bounded
error categories), but only non-successful operations instantiate those
series.

Adding a label or allowing a free-form value requires a new budget calculation
and design review. `workload_subtype` and `phase` remain profile-only for this
reason.

## Coverage and exclusions

| Boundary or signal | Initial coverage | Notes |
|---|---|---|
| Local and remote ChunkManager unary operations | Instrumented | Batch methods count their unary object operations; nested Path/Exist and similar calls are suppressed. |
| Streaming ChunkManager readers | Instrumented | Bytes, terminal outcome, transfer duration, and observable TTFB; callers must close or consume to terminal EOF. |
| Milvus-visible ChunkManager retries | Instrumented | Only retry loops owned by Milvus are counted. |
| Storage V2/V3 packed Go/FFI reader and writer calls | Instrumented or partial | Logical Arrow bytes are used where available; packed calls may represent multiple provider operations. |
| Manifest/file-system FFI calls with propagated context | Instrumented or partial | Calls without a scoped context remain unavailable rather than charged through global counter deltas. |
| Segcore Search and Query cold materialization | Partial | Cold-read observations and known cold bytes are handed to Go. C++ failures that return no result snapshot remain unavailable. |
| Segment/index load C++ entry points | Partial | Milvus-owned call duration and metadata-known bytes are recorded; internal provider expansion and async warmup work are unavailable. |
| Import source reads and persistent output | Instrumented where using owned boundaries | Source and persistent roles are separate; inherited SyncTasks remain Import. |
| Flush, compaction, index/analyze/stats, load, recovery, snapshot, GC, external refresh | Task attribution instrumented | A path with no owned scoped I/O boundary reports unavailable coverage, not zero usage. |
| Tiered cache scanned total/cold bytes | Conditional | Available only when existing storage-usage tracking supplies the values. |
| Cache wait/load duration | Partial | Recorded only at already observable Milvus boundaries. |
| Provider requests/retries/TTFB/transferred bytes | Unavailable | Reserved provider access layer; no milvus-storage changes in this release. |
| OS page cache, mmap faults, computation/result caches | Not applicable | Explicitly excluded by the design. |
| Proxy access logs and public responses | Not applicable | No profile fields are added. The production summary sink is noop. |

Rolling upgrades treat an omitted contribution as unavailable. If any other
contribution is present, merged coverage and quantile completeness are marked
partial/incomplete; omission is never interpreted as zero storage usage.

## Rollout checks

Keep `storage.profile.enabled=false` by default until the following have been
checked in the target environment:

1. Compare new and legacy aggregate rates on representative Search, Query,
   Import, Flush, Compaction, Index, Load, Snapshot, and GC workloads.
2. Fault-inject missing keys, throttling, permission/credential failures,
   timeout, cancellation, unexpected EOF, and generic I/O at Milvus-visible
   boundaries.
3. Confirm disabled and summary-enabled benchmark overhead against the chosen
   release budget.
4. Verify mixed-version nodes produce partial/unavailable coverage rather than
   zero values.
5. Confirm Proxy access-log schemas and public API responses are unchanged.

## Initial microbenchmark baseline

Measured on 2026-07-13 on an Intel Core i7-8700 using the repository-required
`dynamic,test` tags and `-gcflags="all=-N -l"`:

| Mode | Time | Allocations |
|---|---:|---:|
| Loop baseline | 1.198 ns/op | 0 B/op, 0 allocs/op |
| Aggregate metrics, profile disabled | 3.553 us/op | 320 B/op, 1 alloc/op |
| Summary recorder enabled | 4.365 us/op | 432 B/op, 2 allocs/op |

Command:

```bash
go test -tags dynamic,test -gcflags='all=-N -l' \
  ./internal/storageprofile -run '^$' \
  -bench BenchmarkOperationRecorder -benchmem -benchtime=1s
```

These numbers establish an initial comparison point; they do not by themselves
satisfy the production performance gate. A release budget has not yet been
agreed, and representative cache-hit/miss, large-read, and concurrent task
benchmarks still require a running target environment.
