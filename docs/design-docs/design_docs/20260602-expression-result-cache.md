# MEP: Expression Result Cache

- **Created:** 2026-06-02
- **Author(s):** @luzhang
- **Status:** Under Review
- **Component:** QueryNode / Segcore
- **Related Issues:** N/A
- **Released:** N/A

## Summary

Expression Result Cache is a QueryNode-local cache for scalar filter expression bitmaps. It stores the full active-segment result bitmap produced by an expression and reuses it when the same expression is evaluated again on the same segment snapshot.

The cache is implemented in segcore through `ExprResCacheManager`, with two storage modes:

- **Memory mode:** heap memory, adaptive bitmap compression, frequency and latency admission, and Clock eviction.
- **Disk mode:** one fixed-slot cache file per sealed segment, direct `pread`/`pwrite`, frequency and latency admission, per-segment Clock eviction, and global segment-file eviction.

The feature is controlled by refreshable `queryNode.exprCache` parameters. It is disabled by default and does not change query semantics when disabled.

## Motivation

QueryNode can repeatedly evaluate the same scalar filter expression on the same segment. This happens in repeated search/query workloads, two-stage retrieval, text match filters, JSON path filters, and scalar index/statistics based predicates.

Some expressions are expensive because they need to:

- scan field data,
- evaluate JSON paths,
- access scalar or text indexes,
- build full result and validity bitmaps.

Recomputing the same bitmap wastes CPU and increases tail latency. Caching the full-segment bitmap avoids this work when the effective expression and segment snapshot are unchanged.

## Goals

- Cache reusable expression result bitmaps by segment and expression signature.
- Support both memory-backed and disk-backed cache modes.
- Preserve correctness when a segment's active row count changes.
- Make cache parameters refreshable without restarting QueryNode.
- Track memory and disk usage through existing cachinglayer metrics.
- Provide shared helpers for expression implementations to use the get-compute-put pattern.
- Keep the default disabled path close to the original execution path.

## Non-Goals

- Sharing cache entries across QueryNodes.
- Persisting disk cache files across process restarts or config rebuilds.
- Supporting growing segments in disk mode.
- Caching final search results or vector-search intermediate results.
- Replacing specialized scalar, text, JSON, or vector indexes.

## Public Interfaces

### QueryNode Configuration

```yaml
queryNode:
  exprCache:
    enabled: false
    mode: disk
    minEvalDurationUs: 1000
    admissionThreshold: 2
    memory:
      maxBytes: 268435456
      compressionEnabled: true
    disk:
      maxBytes: 10737418240
      maxFileSizeBytes: 268435456
```

| Key | Default | Hot-reload | Description |
|-----|---------|------------|-------------|
| `queryNode.exprCache.enabled` | `false` | yes | Enable expression result cache. |
| `queryNode.exprCache.mode` | `disk` | yes | Cache backend: `disk` or `memory`. |
| `queryNode.exprCache.minEvalDurationUs` | `1000` | yes | Skip caching expressions that evaluate faster than this threshold. `0` disables latency admission. |
| `queryNode.exprCache.admissionThreshold` | `2` | yes | Frequency admission threshold shared by memory and disk modes. `1` disables frequency admission. |
| `queryNode.exprCache.memory.maxBytes` | `268435456` | yes | Maximum memory budget for memory mode. |
| `queryNode.exprCache.memory.compressionEnabled` | `true` | yes | Enable adaptive bitmap compression in memory mode. |
| `queryNode.exprCache.disk.maxBytes` | `10737418240` | yes | Maximum logical used-slot disk budget for disk mode. |
| `queryNode.exprCache.disk.maxFileSizeBytes` | `268435456` | yes | Maximum cache file size per sealed segment in disk mode. |

### C API

Go-side paramtable refresh propagates config into C++ through:

```cpp
void SetExprResCacheEnable(bool val);

void SetExprResCacheConfig(const char* mode,
                           const char* disk_base_path,
                           int64_t mem_max_bytes,
                           bool compression_enabled,
                           int32_t admission_threshold,
                           int64_t mem_min_eval_duration_us,
                           int64_t disk_max_bytes,
                           int64_t disk_max_file_size,
                           int64_t disk_min_eval_duration_us);
```

### Internal Cache API

`ExprResCacheManager` exposes a mode-independent API:

```cpp
struct Key {
    int64_t segment_id;
    std::string signature;
};

struct Value {
    std::shared_ptr<TargetBitmap> result;
    std::shared_ptr<TargetBitmap> valid_result;
    int64_t active_count;
    size_t bytes;
    int64_t eval_duration_us;
};

bool Get(const Key& key, Value& out_value);
void Put(const Key& key, const Value& value);
void Clear();
size_t EraseSegment(int64_t segment_id);
bool SetConfig(const CacheConfig& config);
```

`Get` requires the caller to set `out_value.active_count` before calling. The cache uses it to reject stale entries.

## Design Details

### 1. Architecture

```text
Expression execution
  |
  | ExprCacheHelper::GetOrCompute
  | SegmentExpr::TryCacheGet / CachePut
  | FilterBitsNode whole-filter cache
  v
ExprResCacheManager
  |
  +-- Memory mode -> EntryPool
  |       +-- adaptive compression
  |       +-- latency and frequency admission
  |       +-- Clock eviction
  |
  +-- Disk mode -> DiskSlotFile per sealed segment
          +-- fixed-size raw bitmap slots
          +-- pread / pwrite
          +-- per-file Clock eviction
          +-- global segment-file Clock eviction
```

`ExprResCacheManager` owns mode selection, frequency admission, dynamic config rebuild, segment erasure, and usage metrics. Backend implementations focus on storage-specific behavior.

### 2. Cache Key and Value

The external cache key is `(segment_id, expression_signature)`.

The expression signature is normally `expr->ToString()` or `this->ToString()`. It must include every parameter that can affect the result:

- field id,
- operator type,
- literal values,
- JSON path,
- text query,
- match options,
- query-time context when relevant.

The cached value stores:

- result bitmap,
- validity bitmap,
- active row count,
- miss-path evaluation duration for admission decisions.

Correctness depends on:

- the same segment id, same signature, and same active count producing the same bitmaps;
- callers passing the current segment row count as `active_count`;
- cache hits verifying `active_count`.

If `active_count` mismatches, the entry is treated as stale and the request falls back to normal expression evaluation.

### 3. Memory Backend

Memory mode uses `EntryPool`.

It supports both sealed and growing segments. Internally, `EntryPool` keys entries by:

- segment id,
- signature hash,
- full signature,
- active count.

This isolates growing-segment snapshots with different active row counts.

Memory mode stores payloads in heap memory. `CacheCompressor` chooses the encoding:

| Bitmap pattern | Encoding |
|----------------|----------|
| Sparse result bitmap | Roaring |
| Very dense result bitmap | Inverted Roaring |
| Medium-density bitmap | Raw bytes |
| Compression disabled | Raw bytes |

When the validity bitmap is all ones, memory mode records that state as metadata and avoids storing a separate validity payload.

Eviction uses Clock. `Get` takes a shared lock and updates an atomic usage counter; `Put` takes an exclusive lock and may evict entries until the memory budget is satisfied.

### 4. Disk Backend

Disk mode uses `DiskSlotFile`. It is sealed-segment only because slot size is derived from the segment row count at file creation time.

File layout:

```text
[FileHeader 64B][slot_0][slot_1]...[slot_N-1]
```

Slot layout:

```text
[SlotHeader 17B][raw result bitmap][raw valid bitmap]
```

Each segment owns one cache file:

```text
<localStorage.path>/cache/<nodeID>/expr_cache/seg_<segment_id>.cache
```

Disk files are temporary process-local cache files. Signature-to-slot metadata is kept in memory, so old `.cache` files are removed when disk config is applied or rebuilt.

If the same segment later appears with a different row count, the fixed slot file no longer matches the bitmap shape. The manager removes the file and marks the segment ineligible for disk caching until the segment or config is reset.

Disk mode has two size limits:

- `queryNode.exprCache.disk.maxFileSizeBytes` limits one sealed segment file and determines how many fixed slots the segment file can hold.
- `queryNode.exprCache.disk.maxBytes` limits total logical used-slot bytes across disk cache files. The budget counts `FileHeader + used_slots * slot_size`, not the full preallocated file capacity.

Within one `DiskSlotFile`, slot eviction uses Clock. Across segment files, `Get` hits and `Put` writes touch a segment-level Clock usage counter. After a disk `Put`, the manager checks total used-slot bytes. If usage exceeds `disk.maxBytes`, it scans segment files with a second-chance Clock policy and evicts whole segment files whose usage counter has decayed to zero, skipping the segment that was just written.

### 5. Admission Control

Two admission policies are applied before writing a new entry:

- **Latency admission:** skip expressions whose miss-path evaluation duration is lower than `queryNode.exprCache.minEvalDurationUs`.
- **Frequency admission:** cache only after the expression has been observed at least `queryNode.exprCache.admissionThreshold` times.

Frequency admission is mode-independent and is owned by `ExprResCacheManager`, not by `EntryPool`, so memory and disk use the same policy.

Existing same-signature entries can be updated without re-running frequency admission. This allows refreshed snapshots for the same expression to update the cache after the expression has already been admitted.

### 6. Dynamic Refresh Semantics

All `queryNode.exprCache` parameters are refreshable.

Refresh behavior is conservative:

- `enabled=false` disables future cache get/put operations.
- `enabled=true` first applies the current config, then enables cache access.
- Changing any cache config while enabled calls `SetConfig`.
- `SetConfig` rebuilds the backend and clears existing cache entries.
- In disk mode, `SetConfig` removes old `.cache` files in the target cache directory.
- Invalid config or disk directory creation failure disables the cache and clears backend state.

When thresholds such as `admissionThreshold` or `minEvalDurationUs` change, existing entries are not reinterpreted. They are dropped, and future evaluations repopulate the cache under the new policy.

### 7. Expression Integration

The common integration path is `ExprCacheHelper::GetOrCompute`:

1. Check whether cache is enabled and the segment is eligible.
2. In disk mode, reject growing segments.
3. Build the `(segment_id, signature)` key.
4. Attempt cache `Get`.
5. On miss, compute the full-segment bitmap.
6. Measure evaluation duration when cache admission may need it.
7. Attempt cache `Put`.

For Volcano-style batched expressions, `BatchedCachedMixin` loads or computes the full-segment bitmap once, then slices it on later `Eval()` calls.

Current integration points include:

- `BinaryRangeExpr`
- `TermExpr`
- `JsonContainsExpr`
- `ExistsExpr`
- `UnaryExpr` TextMatch / PhraseMatch
- index/statistics paths through `SegmentExpr::TryCacheGet` and `SegmentExpr::CachePut`
- whole-filter reuse through `FilterBitsNode`

Integration rules:

- Cache the full active-segment bitmap, not a single batch.
- Include all result-affecting parameters in the signature.
- Cache both result and validity bitmaps for nullable expressions.
- Do not mutate shared bitmaps returned from cache.

### 8. Segment Lifecycle

Segment release can call:

```cpp
EraseSegmentCache(segment_id)
```

In memory mode, this removes all entries for the segment from `EntryPool`.

In disk mode, this closes and deletes the segment cache file, clears the segment's ineligible marker, and removes the segment from global disk Clock metadata.

### 9. Metrics and Resource Accounting

The cache reports usage through existing cachinglayer gauges:

| Metric | Meaning |
|--------|---------|
| `cache_loaded_bytes{cell_data_type="OTHER", storage_type="MEMORY"}` | Current expression cache memory usage. |
| `cache_loaded_bytes{cell_data_type="OTHER", storage_type="DISK"}` | Current expression cache disk used-slot logical bytes. |

`ExprResCacheManager::SyncUsageMetrics` tracks the last reported memory/disk bytes and updates gauges by delta. This avoids double counting.

Usage is synchronized after:

- memory put,
- disk put,
- segment erase,
- clear,
- config rebuild,
- disk config failure cleanup.

### 10. Concurrency

`ExprResCacheManager` uses:

- `state_mutex_` for config and active backend state;
- `disk_files_mutex_` for the disk segment-file map;
- `disk_clock_mutex_` for segment-level disk Clock metadata;
- atomic `enabled_`;
- atomic reported metric bytes.

`Get` and `Put` re-check `IsEnabled()` after acquiring `state_mutex_`, preventing requests that entered before a refresh from using a backend after the cache has been disabled.

Backend concurrency:

- `EntryPool` uses shared/exclusive locking around its entry index.
- `DiskSlotFile` uses a shared mutex around slot metadata and file operations.
- Disk used-byte accounting reads `DiskSlotFile` metadata under its shared mutex.

### 11. Disabled Path Performance

The feature is disabled by default.

When disabled, cache manager operations return before building cache keys or taking backend locks. Most expression integrations therefore add only an atomic enabled check and a branch.

Integration code should avoid doing cache-only work when disabled. In particular:

- do not build expression cache signatures before checking the query/context cache flag;
- do not clone bitmaps only for cache writes unless the cache is eligible;
- only measure miss-path evaluation time when the result may be admitted into the cache.

### 12. Failure Handling

The cache is best-effort and must not fail user queries.

Failure behavior:

- invalid mode disables cache;
- non-positive memory or disk size disables cache;
- disk directory creation failure disables cache and clears backend state;
- disk file removal failures are logged as warnings;
- cache miss, stale entry, or ineligible segment falls back to normal expression evaluation.

### 13. Relationship with Two-Stage FilterBits Cache

Expression result cache and `FilterBitsNode` cache are different cache layers.

- Expression-level cache key: a sub-expression signature, such as a TextMatch expression.
- `FilterBitsNode` cache key: the whole filter expression plus dynamic filter context such as entity TTL physical time.

Both can reuse `ExprResCacheManager`.

For two-stage search, `QueryContext` can allow whole-filter cache reads and writes while disabling sub-expression cache writes. This prevents caching both a full-filter bitmap and duplicate child-expression bitmaps for the same request path.

## Correctness Guarantees

- **No stale row-count reuse.** Every cache hit verifies `active_count`.
- **Nullable correctness.** Result and validity bitmaps are cached together.
- **Disk mode sealed-only.** Growing segments are rejected before disk cache usage.
- **Best-effort fallback.** Misses and cache failures fall back to regular expression evaluation.
- **Config changes do not reinterpret entries.** Config refresh rebuilds the backend and drops existing entries.

## Compatibility and Migration

The feature is controlled by `queryNode.exprCache.enabled`, defaulting to `false`.

All config values are refreshable. Operators can enable, disable, or tune the cache without restarting QueryNode.

The feature does not change client-facing API or query semantics.

## Test Plan

- `ExprResCacheManager` basic put/get.
- Enable/disable behavior.
- Segment erase in memory and disk mode.
- Memory mode Clock eviction.
- Memory mode active-count stale check.
- Memory and disk frequency admission.
- Memory and disk latency admission.
- Disk mode fixed-slot put/get.
- Disk mode global used-byte capacity eviction.
- Disk mode segment-level Clock eviction.
- Disk mode config rebuild and old `.cache` file cleanup.
- Disk directory creation failure disables cache.
- Disk row-count mismatch rejects unstable/growing segment usage.
- Concurrent `SetConfig` with get/put.
- Expression integration tests for TextMatch, JSON, Exists, Term, Range, and index/stat paths.
- Two-stage `FilterBitsNode` tests to verify outer filter cache does not duplicate sub-expression entries.

## Known Limitations and Follow-Ups

Current limitations:

- Disk cache files are temporary process-local cache files and are not reused after restart.
- Disk mode stores raw bitmaps; compression is memory-only.
- Disk mode supports sealed segments only.
- Cache key quality depends on expression signature stability and completeness.
- Current metrics report usage bytes only, not hit rate or admission/eviction counts.

Potential follow-ups:

- Add hit/miss/admission/eviction counters.
- Add memory/disk backend latency metrics.
- Evaluate compressed disk slots if disk footprint becomes a bottleneck.
- Introduce a structured expression signature builder to reduce reliance on hand-written `ToString()`.
- Add an operational option to choose whether disabling cache should clear existing entries.
