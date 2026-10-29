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
- Preserve the original RawData expression graph and
  `Eval()`/`MoveCursor()`/`PrefetchAsync()` hot paths when cache is disabled.

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
    materialization:
      maxBytes: 268435456
    memory:
      maxBytes: 268435456
      compressionEnabled: true
      enableGrowing: false
    disk:
      maxBytes: 10737418240
      maxFileSizeBytes: 268435456
```

| Key | Default | Hot-reload | Description |
|-----|---------|------------|-------------|
| `queryNode.exprCache.enabled` | `false` | yes | Enable expression result cache. |
| `queryNode.exprCache.mode` | `disk` | yes | Cache backend: `disk` or `memory`. |
| `queryNode.exprCache.minEvalDurationUs` | `1000` | yes | Skip caching expressions that evaluate faster than this threshold. `0` disables latency admission. |
| `queryNode.exprCache.admissionThreshold` | `2` | yes | Frequency admission threshold for each expression snapshot, shared by memory and disk modes. `1` disables frequency admission. |
| `queryNode.exprCache.materialization.maxBytes` | `268435456` | yes | Maximum heap bytes retained by decoded cache-hit bitmap pairs and RawData miss-capture pairs across both storage modes. |
| `queryNode.exprCache.memory.maxBytes` | `268435456` | yes | Maximum memory budget for memory mode. |
| `queryNode.exprCache.memory.compressionEnabled` | `true` | yes | Enable adaptive bitmap compression in memory mode. |
| `queryNode.exprCache.memory.enableGrowing` | `false` | yes | Allow memory mode to cache eligible growing-segment snapshots. Text-index expressions remain excluded. |
| `queryNode.exprCache.disk.maxBytes` | `10737418240` | yes | Maximum logical used-slot disk budget for disk mode. |
| `queryNode.exprCache.disk.maxFileSizeBytes` | `268435456` | yes | Maximum cache file size per sealed segment in disk mode. |

### C API

Go-side paramtable refresh propagates config into C++ through:

```cpp
void SetExprResCacheEnable(bool val);

void SetExprResCacheConfig(const char* mode,
                           const char* disk_base_path,
                           int64_t materialization_max_bytes,
                           int64_t mem_max_bytes,
                           bool compression_enabled,
                           bool mem_enable_growing,
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

struct AdmissionTicket {
    uint64_t config_epoch;
    uint64_t key_hash;
    bool admitted;
};

enum class LookupResult { Hit, Miss, ResourceLimit };

bool Get(const Key& key, Value& out_value);
LookupResult GetWithStatus(const Key& key, Value& out_value);
std::optional<MaterializationLease>
TryAcquireMaterialization(int64_t active_count);
size_t GetMaterializationBytes() const;
void Put(const Key& key, const Value& value);
AdmissionTicket ObserveMiss(const Key& key,
                            int64_t active_count,
                            SegmentType segment_type);
void PutAdmitted(const Key& key,
                 const Value& value,
                 const AdmissionTicket& ticket);
void Clear();
size_t EraseSegment(int64_t segment_id);
bool SetConfig(const CacheConfig& config);
```

`Get` and `GetWithStatus` require the caller to set
`out_value.active_count` before calling. The cache uses it to reject stale
entries and to reserve the decoded bitmap size. `Get` is the compatibility
wrapper; `GetWithStatus` lets batched callers distinguish a real miss from a
materialization-budget rejection.

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

RawData adapters use
`raw:null_rejecting=<true|false>:<leaf ToString()>`. The leaf's null-rejection
mode is part of the signature because a null-rejecting scan may omit validity
for skipped chunks. `ExprSet` propagates this mode before prefetch or evaluation;
propagation stops at `NOT`. Lookup, frequency admission, and insertion use the
same mode-specific key. Both modes have an explicit RawData prefix, separating
them from other execution paths and earlier keys that omitted this distinction.

The cached value stores:

- result bitmap,
- validity bitmap,
- active row count,
- miss-path evaluation duration for admission decisions.

Correctness depends on:

- the same segment id, same signature, and same active count producing equivalent results under the recorded evaluation semantics;
- callers passing the current segment row count as `active_count`;
- cache hits verifying `active_count`.

If `active_count` mismatches, the entry is treated as stale and the request falls back to normal expression evaluation.

### 3. Memory Backend

Memory mode uses `EntryPool`.

It always supports sealed segments. Growing-segment caching is opt-in through
`queryNode.exprCache.memory.enableGrowing`, which defaults to `false`.
Internally, `EntryPool` keys entries by:

- segment id,
- signature hash,
- full signature.

The active count is entry metadata used for staleness validation rather than
part of the hash key. A put for the same segment and signature replaces the
previous snapshot only when its active count is not older. This prevents an
out-of-order growing query from overwriting a newer snapshot. The memory
backend indexes at most one snapshot for that expression. An ongoing lookup
may still hold the previous snapshot until its decode finishes.

Memory mode stores payloads in heap memory. `CacheCompressor` independently
chooses the encoding for each of the result and validity bitmaps:

| Bitmap pattern | Encoding |
|----------------|----------|
| Sparse bitmap (density <= 3%) | Roaring |
| Very dense bitmap (density >= 97%) | Inverted Roaring |
| Medium-density bitmap | Raw bytes |
| Compression disabled | Raw bytes |

When the validity bitmap is all ones, memory mode records that state as metadata and avoids storing a separate validity payload.

Each bitmap uses its own density, so Raw/Roaring, Raw/inverted Roaring, and
all other combinations are supported. Independent entries record the result
codec, validity codec, and result payload length before the two payloads.
The decoder dispatches each payload using its recorded codec. When both
bitmaps select Raw, the existing Raw layout implies both codecs and avoids
the extra encoding header. Only Raw and Independent are accepted as entry
formats; Roaring and inverted Roaring are per-bitmap codecs inside Independent
entries. Memory entries are process-local and removed from the lookup index
on backend rebuild, so no legacy entry-format compatibility is needed.

Inverted bitmaps are complemented word by word during encoding, without
allocating a full temporary complement bitmap. Raw bitmap bytes are copied
directly into the final entry buffer, including when the companion bitmap
uses Roaring; they need no intermediate payload copy.

Decoding writes Roaring containers directly into the output bitmap: bitmap
containers are copied, array containers set their positions, and run containers
set their ranges. It never expands all set positions into a temporary row-id
array. The materialization budget reserves the two output bitmaps; temporary
Roaring objects created by deserialization are not included in that reservation.

Each entry owns an immutable payload through `shared_ptr<const Payload>`.
The payload contains the row count, encoding, compressed bytes, and its memory
reservation. `Lookup` checks the row count and copies this reference under the
pool's shared lock, then releases the lock. The manager also releases its
state lock before reserving output capacity and decoding. No compressed
buffer copy is needed on a hit.

Eviction uses an intrusive doubly linked Clock ring. Each entry links to its
neighbors and points to the key owned by its hash-map node; the full signature
is stored and charged once. Inserts and erases update only the affected links,
and replacement retains the node and its Clock position. Hash-map rehashing
preserves key addresses. Lookups update an atomic usage counter; puts take the
exclusive index lock. Clock skips payloads held by readers, since removing
their entries would not immediately free memory. A bounded sweep temporarily
moves candidates to another ring using the same links, without allocating a
key snapshot. It checks their combined reclaimable bytes against the shared
budget and removes them only if the put can fit; otherwise it rejoins the
candidates with all existing entries retained. New hash-map nodes are allocated
before eviction and remain provisional until admission succeeds.
Oversized entries are rejected before evicting existing entries. Replacement
preserves the old mapping if capacity is unavailable; when readers still hold
it, both old and new payloads must fit in the budget.

All memory-backend generations share one byte budget. Buffer capacity and
estimated entry, key, and shared-ownership metadata remain charged until the
last payload reference is released. Erase, clear, and backend rebuild remove
entries immediately; old readers can finish without accessing the retired
pool. The buffer is freed before its charge is returned. Lowering capacity
does not cancel existing readers, but prevents new entries from exceeding the
new limit. This is a retained-payload budget, not a bound on total process RSS
or temporary compression/deserialization allocations.

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

Never-used slots are allocated incrementally with a cursor. The free-slot list
stores only recycled IDs, so its memory does not scale with the file's maximum
slot count. Allocation reuses a recycled ID first and runs Clock eviction only
when no recycled or never-used slots remain.

If the same segment later appears with a different row count, the fixed slot file no longer matches the bitmap shape. The manager removes the file and marks the segment ineligible for disk caching until the segment or config is reset.

Disk mode has two size limits:

- `queryNode.exprCache.disk.maxFileSizeBytes` limits one sealed segment file and determines how many fixed slots the segment file can hold.
- `queryNode.exprCache.disk.maxBytes` limits total logical used-slot bytes across disk cache files. The budget counts `FileHeader + used_slots * slot_size`, not the full preallocated file capacity.

Within one `DiskSlotFile`, slot eviction uses Clock. Across segment files, `Get` hits and `Put` writes touch a segment-level Clock usage counter. After a disk `Put`, the manager checks total used-slot bytes. If usage exceeds `disk.maxBytes`, it scans segment files with a second-chance Clock policy and evicts whole segment files whose usage counter has decayed to zero, skipping the segment that was just written.

Disk puts complete segment-level Clock registration before creating a segment
file or writing a slot. If registration allocation fails, no file is created
and no slot is written. A new file remains provisional until its first slot
write completes: exceptions and empty writes caused by open/pwrite failures
remove both the new file and its Clock registration. Cleanup reuses the
prepared path without allocating another path string. Failed puts into an
existing file retain that file's Clock registration, so it remains eligible
for global eviction.

### 4.1 Full-Bitmap Materialization Budget

The storage budgets do not cover the decoded bitmaps returned by `Get`:
`memory.maxBytes` counts compressed `EntryPool` payloads and estimated metadata, while
`disk.maxBytes` counts disk slots. Both modes therefore use the independent
`materialization.maxBytes` heap budget.

Each backend first finds the matching entry or slot and checks its active
count under a shared lock. Missing or stale entries return `Miss` without
reserving bitmap capacity or allocating a result holder. Memory lookup retains
an immutable payload reference and releases both manager and pool locks;
disk lookup retains its locks to protect slot metadata and file access.
Immediately before decoding or reading the value, the manager
reserves `2 * ceil(active_count / 64) * 8` bytes for the result/validity pair
and allocates its holder. The reservation is non-blocking. If it would exceed
the configured limit, `GetWithStatus` returns `ResourceLimit` without decoding
or reading the value. A successful lookup returns two aliasing `shared_ptr`s
backed by the per-lookup holder. The holder owns both bitmaps and the
reservation, so the bytes remain charged until the last result or validity
reference is released, including
after cache eviction, `Clear()`, or config rebuild. Decoded bitmap pairs are
not shared across queries; their aggregate live bytes are bounded instead.

RawData passive miss capture reserves the same pair size before calling
`reserve()` on its accumulation buffers. If no reservation is available, the
adapter evaluates normally one batch at a time but neither decodes a cache hit
nor captures a full-segment miss. Allocation and exception paths destroy the
buffers before releasing the reservation.

The compressed payload reference is released when decoding finishes; returned
bitmaps own only their materialization reservation. Exceptions release the
lookup's payload reference and materialization reservation through ownership
cleanup. The storage reservation remains while the payload is still indexed
or held by another reader. Neither reservation's release path acquires the
manager or pool lock.

### 5. Admission Control

Two admission policies are applied before writing a new entry:

- **Latency admission:** skip expressions whose miss-path evaluation duration is lower than `queryNode.exprCache.minEvalDurationUs`.
- **Frequency admission:** count observations by segment id, expression signature, and active row count; admit the snapshot when its counter reaches `queryNode.exprCache.admissionThreshold`.

Frequency admission is mode-independent and is owned by `ExprResCacheManager`, not by `EntryPool`, so memory and disk use the same policy.

The tracker uses 262,144 one-byte counters (256 KiB per process). When frequency
admission is enabled, every 10,000 admission records trigger a synchronous
scan that halves all counters. The decay interval is independent of the slot
count, keeping old observations from accumulating as the table grows.

Existing signatures do not trigger a secondary cache scan or bypass latency
admission. A growing snapshot with a different active row count has its own
frequency hash; an admitted replacement still has to satisfy the latency
threshold. The bounded frequency tracker is approximate: hash-slot collisions
can share counters, as before. The storage key remains `(segment_id, signature)`
and retains only one snapshot per expression.

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
2. Reject growing segments unless memory-mode growing caching is enabled.
3. Build the `(segment_id, signature)` key.
4. Attempt cache `Get`.
5. On miss, compute the full-segment bitmap.
6. Measure evaluation duration when cache admission may need it.
7. Attempt cache `Put`.

For Volcano-style batched expressions, `BatchedCachedMixin` loads or computes the full-segment bitmap once, then slices it on later `Eval()` calls.

#### Current sub-expression support

This subsection describes the process-level sub-expression cache controlled by
`queryNode.exprCache`. It intentionally excludes the two-stage whole-filter
cache in `FilterBitsNode`, which is described separately in
[Relationship with Two-Stage FilterBits Cache](#13-relationship-with-two-stage-filterbits-cache).

The current implementation is not a generic cache around every expression
evaluator. It is integrated only into execution paths that produce a reusable
full-segment result bitmap. Selecting the same logical expression does not by
itself make it cacheable: the expression must also select one of the integrated
execution paths below.

| Execution path | Integrated expressions | Conditions and exclusions |
|----------------|------------------------|---------------------------|
| Scalar index, including JSON path indexes | `UnaryRangeExpr` (comparisons, supported string pattern operations, and indexed array equality), `BinaryRangeExpr`, `TermExpr`, `JsonContainsExpr` (`Contains`, `ContainsAny`, and `ContainsAll`), JSON `ExistsExpr`, and the scalar-index branches of `BinaryArithOpEvalRangeExpr` and `TimestamptzArithCompareExpr` | The concrete scalar index must accept the operator. The generic integration covers normal scalar indexes and compatible JSON flat/path indexes. An operator rejected by `ShouldUseOp` or an incompatible JSON path/type may fall back to `RawData`; it then participates only when it is in the RawData adapter's supported scope. NGRAM is explicitly routed to its own execution path. |
| JSON statistics | JSON `UnaryRangeExpr`, `BinaryRangeExpr`, `TermExpr`, all implemented `JsonContainsExpr` variants, and `ExistsExpr` | Sealed segments only. `plan_options.expr_use_json_stats` must be enabled, JSON stats must exist, and the nested path must be non-empty and contain no numeric path component. |
| Text index | `TextMatch`, `PhraseMatch`, and `TextMatchFuzzy` in `UnaryRangeExpr` | Uses `segment->GetTextIndex()`. Sealed segments only in both memory and disk modes. Growing text-index results bypass cache reads and writes even when `memory.enableGrowing=true`. |
| RawData, including scans assisted by `SkipIndex` | Row-level `UnaryRangeExpr`, `BinaryRangeExpr`, `TermExpr`, `BinaryArithOpEvalRangeExpr` (primitive scalars, JSON, and ARRAY), `JsonContainsExpr` (JSON and ARRAY contains/any/all), JSON `ExistsExpr`, and `TimestamptzArithCompareExpr` | The resolved path must be `RawData`. Element-level output, VECTOR_ARRAY arithmetic, text-index operators, and NGRAM execution remain unsupported. A miss is inserted only after contiguous, unmasked full-segment coverage. |

The following paths do **not** currently write or read sub-expression entries:

- primary-key index paths;
- NGRAM phase-1/phase-2 execution;
- GIS/RTree execution;
- `NullExpr`, Bloom-filter expressions, field-to-field comparisons, and other
  expression classes that neither call the cache helper nor opt in to the
  RawData adapter;
- logical `AND`, `OR`, and `NOT` results. Their integrated child expressions
  may be cached independently, but the combined result is not cached here.

Some of these implementations keep a bitmap inside one physical expression
object (for example PK and NGRAM execution). That state only avoids duplicate
work within one execution and must not be confused with
`ExprResCacheManager`, which reuses results across requests.

Most offset-input/iterative branches bypass the sub-expression cache because
they evaluate only the supplied candidates. `BinaryRangeExpr` has explicit
full-bitmap-and-gather exceptions for JSON statistics and compatible
`JsonFlatIndex` execution.

For `JsonFlatIndex`, the generic scalar-index integration can also store a
separate `json-flat-validity:*` bitmap artifact. This entry is shared across
different literals and operators for the same exact path and value family, and
it consumes cache capacity independently from the expression result entry.

#### Effective segment support by backend

The backend capability and the execution paths currently exposed by a segment
must both be considered:

| Result source | Memory, sealed | Memory, growing | Disk, sealed | Disk, growing |
|---------------|----------------|-----------------|--------------|---------------|
| Scalar / JSON path index | yes | no current production path | yes | no |
| JSON statistics | yes | no | yes | no |
| Text index | yes | no | yes | no |
| Eligible row-level RawData, with or without `SkipIndex` assistance | yes | opt-in | yes | no |
| PK / NGRAM / GIS and other excluded paths | no | no | no | no |

Memory mode can store one growing-segment snapshot per segment and signature
when `queryNode.exprCache.memory.enableGrowing=true`. Growing segments expose
interim indexes through `HasIndex()` only for vector and geometry fields,
growing segments do not expose JSON path indexes, and the geometry execution
path does not use the expression cache. Vector indexes are outside scalar
filter-expression caching. Consequently, excluding the whole-filter cache,
the currently effective opt-in growing-segment users are the eligible RawData
expressions. `TextMatch`, `PhraseMatch`, and `TextMatchFuzzy` bypass growing
cache reads and writes: commit/reload can change text-index visibility without
changing `active_count`, so row-count validation cannot detect stale results.
Their per-request bitmap reuse across batches remains unchanged.

Disk mode rejects growing segments before lookup or insertion. It can cache any
of the integrated scalar-index, JSON-statistics, text-index, or eligible
RawData expressions when the source segment is sealed.

Whole-filter reuse through `FilterBitsNode` is an additional integration point,
but it is not included in either table above.

#### RawData and SkipIndex integration

> **Status:** implemented. The support tables above include this path.

This implementation extends the memory and disk expression-cache modes to selected
row-level `SegmentExpr` implementations whose resolved execution path is
`ExprExecPath::RawData`. It does not change the two-stage whole-filter cache.
It also does not cache `SkipIndex` decisions. The reusable artifact remains the
complete expression result for `[0, active_count)`: one result bitmap and one
validity bitmap, interpreted under the null-rejection mode recorded in the key.

The main constraints are:

- preserve Volcano-style batching instead of eagerly evaluating the whole
  segment on the first `Eval()` call;
- preserve the existing raw evaluator and its `SkipIndex` optimization on a
  cache miss;
- avoid raw-data prefetch and all raw/SkipIndex work on a cache hit;
- write an entry only after one expression instance has observed every row's
  unmasked result in contiguous order;
- keep cache lookup enabled when a request disables sub-expression writes;
  read-only misses must not enter admission or allocate capture bitmaps;
- keep the cache best-effort: lookup, capture, admission, or storage failures
  must fall back to the normal raw path;
- when expression cache is disabled, preserve the original expression objects
  and hot execution path: no raw-cache state, per-batch branch, manager call,
  signature construction, timer, bitmap allocation, copy, or cursor bookkeeping.

##### Execution flow

```text
ExprSet construction, after normal compilation and optimization
  -> expression cache disabled: return the original expression tree unchanged
  -> expression cache enabled: decorate eligible SegmentExpr leaves

Driver prefetch
  -> cache adapter determines the wrapped expression's execution path
  -> RawData + eligible: adapter looks up the expression cache
       -> hit: retain the full result/validity bitmaps; do not prefetch raw data
       -> miss: delegate to the wrapped expression's existing raw-data prefetch

Eval
  -> adapter repeats the lookup lazily when the caller did not run prefetch
  -> hit: slice the next batch, or gather requested offsets
  -> miss: delegate to the existing RawData evaluator, including SkipIndex
           -> writes enabled: passively append the returned unmasked batch
              and put only after exact full-segment coverage
           -> writes disabled: do not observe admission, capture, or put

MoveCursor
  -> parent calls adapter: mark full-segment capture impossible, then delegate
  -> wrapped expression calls its own MoveCursor after producing output:
     adapter is not involved, so capture remains valid
```

The opt-in is implemented as a composition-based adapter around the original
physical expression, not as cache code embedded in every `SegmentExpr`. After
normal expression optimization, `CompileExpressions()` samples the process
cache enable flag once. If it is false, it returns immediately without walking
or rewriting the expression tree. If it is true, a decoration pass wraps only
supported leaves and passes the request's write policy into each adapter. The
policy gates admission, capture, and put, but not lookup. This keeps dynamic
casts and optimizer behavior unchanged and keeps the disabled Eval path
identical to the pre-feature path.

`Driver` already invokes expression prefetch, so it requires no new branch. The
adapter resolves the wrapped `SegmentExpr` execution path before delegating raw
prefetch. It performs lookup before that delegation, allowing a hit to suppress
`PrefetchRawData()`. An idempotent lazy lookup in the adapter's `Eval()` covers
unit tests and other direct callers that do not prefetch. A `std::once_flag`
owned by the adapter protects this lookup; it does not add state to an
undecorated expression and must not reuse execution-path initialization state.

On a hit, both cached bitmaps must have exactly `active_count` bits. A malformed
or stale value is a miss. The expression retains local `shared_ptr`s, so global
Clock eviction cannot invalidate a bitmap that is already being consumed.
After a valid hit, that expression instance keeps using the retained value in
the normal case. If allocating or copying a requested hit slice/gather fails,
the adapter discards the hit and evaluates that still-aligned batch through the
wrapped raw expression instead.

##### Per-expression state

Raw caching needs state separate from the existing index-result fields. It is
owned only by the cache adapter, which is not constructed when cache is
disabled. An illustrative adapter state is:

```cpp
enum class RawCacheLookupState { Unchecked, Ineligible, Miss, Hit };

struct RawExprCacheState {
    RawCacheLookupState lookup_state{RawCacheLookupState::Unchecked};

    // Full-segment value retained on a hit.
    std::shared_ptr<TargetBitmap> result;
    std::shared_ptr<TargetBitmap> valid_result;

    // Logical row position consumed by sequential Eval/MoveCursor calls.
    int64_t sequential_pos{0};

    // Frequency admission is observed once, on the first real Eval after miss.
    bool admission_checked{false};
    std::optional<ExprResCacheManager::AdmissionTicket> admission_ticket;

    // Passive miss-path capture.
    bool capturing{false};
    bool full_coverage_possible{true};
    std::optional<ExprResCacheManager::MaterializationLease> capture_lease;
    TargetBitmap captured_result;
    TargetBitmap captured_valid_result;
    int64_t eval_duration_us{0};
};
```

`full_coverage_possible` states the correctness fact that matters: whether this
instance can still reconstruct the complete leaf-expression result. No
`eval_in_progress` flag is needed. Such a name describes a transient call-stack
condition, while cache correctness depends on whether rows were actually
evaluated and captured.

##### Cursor contract

The public `MoveCursor()` signature and the wrapped expression's implementation
do not change. Composition gives the two call sites different receivers:

- a parent such as `ConjunctExpr` sees the adapter. Calling the adapter's
  `MoveCursor()` means that the leaf result was not produced, so the adapter
  breaks full-coverage capture and then delegates physical movement to the
  wrapped expression;
- calls made internally by the wrapped expression after it has produced a
  result execute on the wrapped object itself. They advance its existing cursor
  without entering the adapter and therefore do not invalidate capture;
- on a cache hit, the adapter returns the known bitmap slice, advances its
  logical position, and directly calls the wrapped expression's `MoveCursor()`
  to keep the physical cursor aligned.

This removes the need for an `eval_in_progress` flag, a new
`AdvanceCursorBy()` API, or an audit-and-rewrite of existing result-producing
`MoveCursor()` calls. In particular, existing calls in `SliceCachedResult()` and
constant-result branches retain their current meaning.

| Situation | Cursor operation | Capture effect |
|-----------|------------------|----------------|
| Wrapped raw evaluator returns a normal batch; its existing code advances its own physical cursor | Adapter advances only `sequential_pos` | Append may continue |
| Adapter serves a cache hit | Adapter advances `sequential_pos`, then calls the wrapped expression's `MoveCursor()` | No capture exists on a hit |
| `ConjunctExpr` short-circuits an entire child batch | Parent calls the adapter's `MoveCursor()` | Set `full_coverage_possible=false`, stop and clear capture, then delegate cursor movement |

The adapter computes the actual tail size before delegation and advances its
own `sequential_pos`. Once coverage is broken, capture must not restart later in
the same adapter instance. This prevents a query whose first batch was
short-circuited from writing a suffix bitmap under a full-segment cache key.

##### Cache-hit serving

A helper such as `TryServeRawExprCache(EvalCtx&, VectorPtr&)` should return
whether the request was handled, rather than whether its output pointer is
non-null. End-of-stream is a handled cache hit whose output is legitimately
`nullptr`.

For sequential input:

1. compute `actual_rows = min(batch_size, active_count - sequential_pos)`;
2. slice both result and validity bitmaps at `sequential_pos`;
3. advance the logical position and call the wrapped expression's existing
   `MoveCursor()` once when `actual_rows > 0`;
4. return `nullptr` when `actual_rows == 0`.

For offset input, gather both bitmaps in the supplied offset order. Shuffled
offsets and duplicates are valid, and offset evaluation does not advance the
sequential cursor.

A full cached leaf result may be returned even when `EvalCtx` has a
`bitmap_input`: the bitmap is an evaluation-work mask supplied by
`ConjunctExpr`, not part of the leaf's logical semantics. Returning the full
leaf value preserves three-valued `AND`/`OR`/`NOT` results. The asymmetric rule
is important: a cache **hit** may ignore the mask, but a cache **miss** must not
capture a masked result because inactive leaf values were not computed.

The first implementation keeps `CanExecuteAllAtOnce() == false` for `RawData`.
Raw cache support must not silently change the surrounding batched operator
lifecycle.

##### Passive miss-path capture

`BatchedCachedMixin` and `ExprCacheHelper::GetOrCompute` are not used for this
path because they compute a full-segment value eagerly. Instead, the adapter
wraps the supported expression as a whole:

1. try to serve a hit;
2. record the logical start row and start the evaluation timer;
3. invoke the unchanged raw evaluator, which may use `ProcessDataChunks()` and
   `SkipIndex`;
4. inspect and append the returned batch;
5. put the value after exact full coverage.

A returned batch may be appended only when all of the following hold:

- the expression is row-level, not element-level;
- there is no offset input;
- `bitmap_input` is absent or all true for the batch;
- the batch starts at `captured_result.size()`, with no gap or overlap;
- result and validity have the same size;
- appending cannot exceed `active_count`;
- `full_coverage_possible` is still true and frequency admission allowed
  capture.

Violation of a coverage condition clears both capture buffers and permanently
sets `full_coverage_possible=false` for that expression instance. A put is
allowed only when both buffers have exactly `active_count` bits. Early operator
termination, cancellation, an exception, or a limit that stops consumption
therefore leaves an incomplete buffer and performs no put.

`eval_duration_us` is the sum of actual raw evaluator time across captured
batches. It excludes cache lookup, bitmap append/copy, compression, and cache
write time so that `minEvalDurationUs` continues to describe the cost avoided
by a future hit.

##### Admission before allocation

The current frequency check occurs inside `Put()`. That is too late for passive
raw capture: a one-off expression would already have allocated and copied a
full-segment pair of bitmaps before being rejected. The manager should expose a
forward-admission API:

```cpp
struct AdmissionTicket {
    uint64_t config_epoch;
    uint64_t key_hash;
    bool admitted;
};

AdmissionTicket ObserveMiss(const Key& key,
                            int64_t active_count,
                            SegmentType segment_type);
void PutAdmitted(const Key& key,
                 const Value& value,
                 const AdmissionTicket& ticket);
```

`ObserveMiss()` is called exactly once per physical expression instance, when
the first real `Eval()` begins after a cache miss. A miss observed only during
prefetch does not count, and an expression skipped exclusively through
`MoveCursor()` does not count. A masked `Eval()` may count as an occurrence even
though it cannot be captured.

With `admissionThreshold=2`, the first actual occurrence of a snapshot is
observed but does not allocate capture buffers; its second occurrence receives
an admitted ticket and may capture, subject to the frequency tracker's
approximate counting. Observations of different growing row counts use
different frequency hashes. `PutAdmitted()` skips the duplicate frequency check
but retains latency admission, backend/mode eligibility, capacity checks,
compression, and eviction.

Every miss goes through frequency admission. A stale active-count snapshot does
not bypass frequency or latency admission. An admitted snapshot replaces the
previous memory entry only if its active count is not older.

Tickets are bound to the admission hash (segment id, expression signature,
and active count) and the manager's config epoch. `ObserveMiss()` checks segment
eligibility and stamps the epoch under the same `state_mutex_` lock. Growing
segments are admitted only in memory mode with growing caching enabled, even
if the config changed after the caller's earlier eligibility check.
`SetConfig()` increments the epoch and resets the frequency tracker;
`PutAdmitted()` rejects a stale ticket
or one issued for a different segment, signature, or active count. Both normal
`Put()` and forward admission use this hash. The storage key deliberately omits
active count so an admitted growing snapshot replaces the previous one instead
of accumulating versions. Including active count in admission prevents a hot
expression from automatically admitting every new growing snapshot, even when
none of those snapshots is queried twice. Latency admission cannot be decided
before evaluation, so it is still checked after the full accumulated duration
is known. Remembering that a frequent expression is consistently cheap is a
separate future optimization.

##### SkipIndex behavior

The cache-miss path keeps the existing skip callbacks unchanged:

| Expression | Existing raw skip predicate |
|------------|-----------------------------|
| `PhyUnaryRangeFilterExpr` | `CanSkipUnaryRange` |
| `PhyBinaryRangeFilterExpr` | `CanSkipBinaryRange` |
| `PhyTermFilterExpr` | `CanSkipInQuery` |
| `PhyBinaryArithOpEvalRangeExpr` | `CanSkipBinaryArithRange` |

When `SkipIndex` proves that a chunk cannot match, the raw processing helpers
leave its result bits false and invoke the evaluator callback with null data
where necessary to advance internal batch-mask cursors. A null-rejecting
consumer treats UNKNOWN and FALSE alike, so this path may omit column validity
and leave skipped NULL rows marked as valid FALSE. Such an entry is reusable
only by another null-rejecting evaluation. Under `NOT`, null rejection does not
propagate: nullable predicates retain exact validity, and their cache entries
use the separate three-valued key. Capturing complete row coverage alone does
not make entries interchangeable between these modes.

There is no separate cache for skip decisions. A skip decision is cheap,
chunk-scoped, and cannot replace a row-level result. On a result-cache hit,
`SkipIndex`, field validity, raw chunk access, and the evaluator are all bypassed.
An all-skipped expression is often cheap; latency admission should reject it
when reading a disk slot would cost more than re-running the skip checks.

For mutable columns, v1 must disable raw expression caching for predicates that
reference a patched column. A later version may include a per-(segment, column)
overlay version in the key. Independently, `SkipIndex` must remain patch-aware,
as specified in [Mutable Columns](20260709-mutable-columns.md#interaction-with-chunk-skip-stats-and-the-expression-cache).
The segment implementation in this branch does not yet expose mutable-column
patch/overlay state; adding that feature must add this eligibility guard before
RawData expression caching is enabled for patched columns.

##### Supported raw expression scope

The adapter uses `SupportsRawExprCache()` to opt in and the generic
`SegmentExpr` accessors `GetExecPath()`, `GetSegment()`, `GetActiveCount()`, and
`GetNextBatchSize()` to resolve the execution path, segment, active count, and
batch size. `GetSignatureForRawExprCache()` remains cache-specific because it
includes the RawData namespace and null-rejection mode. These interfaces add no
per-instance cache fields; when caching is disabled, expressions run without
the adapter. The adapter activates caching only when the resolved `exec_path_`
is actually `RawData`. This leaves existing scalar-index, JSON-statistics, primary-key,
text-index, and NGRAM branches unchanged.

The supported classes are row-oriented `SegmentExpr`s with stable expression
signatures and batch cursors compatible with the adapter. The original four
classes also cover the SkipIndex predicates above. JSON/ARRAY contains,
existence checks, JSON/ARRAY arithmetic and length comparisons, and timestamp
interval comparisons use the same capture/hit mechanism:

| Expression | Restrictions |
|------------|----------------------------|
| `PhyUnaryRangeFilterExpr` | Reject element-level mode, text-index operators, and the NGRAM special path. |
| `PhyBinaryRangeFilterExpr` | Reject element-level mode; cache only when its resolved path is RawData. |
| `PhyTermFilterExpr` | Reject element-level mode; cache only when its resolved path is RawData. |
| `PhyBinaryArithOpEvalRangeExpr` | Primitive scalar, JSON, and ARRAY fields, including JSON/ARRAY length comparisons. Reject element-level mode and VECTOR_ARRAY. |
| `PhyJsonContainsFilterExpr` | Row-level JSON/ARRAY `Contains`, `ContainsAny`, and `ContainsAll`; reject element-level mode. |
| `PhyExistsFilterExpr` | Row-level JSON existence checks; reject element-level mode. |
| `PhyTimestamptzArithCompareExpr` | Row-level TIMESTAMPTZ interval arithmetic and comparison; reject element-level mode. |

Decorate these expressions only after the normal compile and optimization
passes, and only when the setup-time cache gate is enabled. Do not add a cache
branch inside their top-level `Eval()`, templated evaluators, or skip callbacks.
A path selected as `ScalarIndex` but later using an offset-specific raw fallback
is outside the first version; it needs an explicit execution-path contract
before it can participate.

The JSON/ARRAY extension preserves the raw evaluator's result and validity
bits, including missing paths, JSON nulls, wrong value types, nullable fields,
mixed/nested JSON array values, and empty contains argument lists. The adapter
does not reinterpret these cases. Index and JSON-statistics branches retain
their existing integration; an adapter whose resolved path is not RawData
delegates to that branch without capturing a second entry.

`NullExpr` remains a follow-up: its non-nullable constant-result fast path has
an independent `precheck_pos_` cursor, which needs an explicit cache-hit and
external-`MoveCursor()` contract before opt-in.

The following remain excluded until they receive a dedicated design:

- element-level expressions, because their output domain is array elements
  rather than `[0, active_count)` and is incompatible with disk fixed slots;
- field-to-field `CompareExpr`, which owns multiple data cursors;
- GIS candidate/refine paths;
- Bloom expressions, whose current `IsCacheable()==false` protects against an
  incomplete key for large blobs;
- NGRAM phase-1/phase-2 execution;
- `MatchExpr`, `CallExpr`, and UDF-like execution until determinism, signature,
  and implementation-version requirements are defined.

Extensions use small opt-in overrides without changing the raw evaluators or
storage format. The manager continues to feed the same full result/validity
bitmap pair into the existing memory or disk backend.

##### Current backend matrix

| Raw result source | Memory, sealed | Memory, growing | Disk, sealed | Disk, growing |
|-------------------|----------------|-----------------|--------------|---------------|
| Eligible row-level RawData expression, with or without SkipIndex assistance | yes | opt-in, one validated `active_count` snapshot | yes | no |

This target matrix does not override expression-level exclusions or the mutable
column restriction. Disk remains sealed-only and retains its fixed row-count
validation.

##### Verification matrix

Raw-cache support is a behavioral change, so tests must prove both the stored
value and the work bypassed by a hit:

- **Admission:** threshold 2 observes once per expression instance rather than
  once per batch; frequency hashes include segment id, signature, and active
  count; a new growing snapshot skips full-bitmap capture until admitted;
  never-evaluated `MoveCursor()` instances do not count; tickets reject a
  different segment, signature, active count, or config epoch; latency tests use
  synthetic durations rather than sleeps.
- **Sequential lifecycle:** for `active_count=2500` and `batch_size=1024`, a hit
  returns 1024, 1024, 452, then handled EOF; skipping a batch before evaluation
  positions the next batch correctly; retained hit bitmaps survive global
  eviction.
- **Coverage poisoning:** whole-batch conjunct short-circuit, a skipped first
  batch, and partial `bitmap_input` produce no entry; a later standalone leaf
  evaluation must still be correct. An internal result-producing
  `MoveCursor()` call on the wrapped expression must not poison adapter capture
  or double-advance; an external `MoveCursor()` call on the adapter must poison
  it.
- **SkipIndex:** all-skip, partial-skip, and no-skip cases for all four initial
  expressions, including chunk boundaries, the final partial batch, and
  nullable result/validity pairs.
- **Offsets:** a hit gathers shuffled and duplicate offsets from both bitmaps;
  an offset-input miss does not put.
- **Three-valued logic:** cached and uncached leaf results must be identical
  under `AND`, `OR`, and `NOT`, comparing both result and validity bitmaps.
- **JSON/ARRAY and timestamps:** exercise typed, mixed, and nested JSON
  contains values; empty contains argument lists; missing/null/non-array JSON
  paths; nullable ARRAY fields and out-of-bounds element arithmetic;
  JSON/ARRAY length comparisons; and timestamp interval comparisons. Check
  disabled evaluation, full-segment capture, read-only hits, offset gathers,
  skipped/masked batches, NOT, distinct signatures, and growing snapshot
  invalidation. Counters must prove that hits bypass the actual evaluator and
  raw prefetch. Invalid JSON arithmetic must still throw without caching.
- **Backend/segment matrix:** memory sealed, memory growing disabled by
  default, explicit memory growing enablement, disk sealed, and disk growing
  rejection.
- **Bypassed work:** mocks or counters prove a hit does not call
  `PrefetchRawData`, `prefetch_chunks`, `chunk_data`, `get_batch_views`,
  `GetSkipIndex`, or the raw evaluator.
- **Incomplete execution:** early stop, cancellation, and exceptions never put
  a partial value.
- **Concurrency:** simultaneous misses may duplicate capture work in v1, but
  the final entry and every returned result must be valid; single-flight
  suppression is a follow-up.
- **Isolation and performance:** singleton manager tests clear and restore
  enable/config state and use temporary disk directories. Benchmarks compare a
  disk hit with an all-skipped raw scan to tune `minEvalDurationUs`.
- **Disabled-path equivalence:** with cache disabled, assert that no adapter or
  `RawExprCacheState` is constructed and no manager, signature, timer, or bitmap
  capture hook is reached. Compare CPU time and allocations against the
  pre-feature RawData microbenchmark; any statistically significant regression
  blocks rollout.

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
Lookups that already acquired a payload reference may finish; new lookups
miss. Reader-held payloads remain charged until decoding finishes.

In disk mode, this closes and deletes the segment cache file, clears the segment's ineligible marker, and removes the segment from global disk Clock metadata.

### 9. Metrics and Resource Accounting

The cache reports storage and materialization usage, and successful lookups:

| Metric | Meaning |
|--------|---------|
| `internal_cache_loaded_bytes{data_type="OTHER", location="MEMORY"}` | Live compressed payload capacity plus estimated entry metadata, including reader-held retired payloads, and reserved bytes for decoded hit bitmaps and RawData capture buffers in both backends. |
| `internal_cache_loaded_bytes{data_type="OTHER", location="DISK"}` | Current expression cache disk used-slot logical bytes. |
| `internal_expr_cache_hit_total` | Total successful expression cache lookups across memory and disk modes. |

The shared memory budget updates the memory gauge when a payload's reservation
is acquired or released. Clear, rebuild, config failure, and switching to disk
do not reset charges for payloads still held by readers. `GetMemoryBytes()`
exposes this counter even when the current backend is disk.
`ExprResCacheManager::SyncDiskUsageMetrics` tracks the last reported disk usage
and updates its gauge by delta.

`internal_expr_cache_hit_total` increments exactly once when
`ExprResCacheManager::GetWithStatus` returns `Hit`. Misses, stale entries, and
materialization-budget rejections do not increment it.

Decoded hit values and RawData capture buffers are accounted separately by
the materialization lease counter and bounded by
`queryNode.exprCache.materialization.maxBytes`. This counter is available
through `GetMaterializationBytes()` for tests and diagnostics. Successful lease
acquisition adds its reserved bytes to the cachinglayer MEMORY gauge; lease
release subtracts them. Lookup misses and rejected reservations do not add a
materialization charge; an admitted miss capture is charged when it reserves
its buffers. Clear, disable, and reconfiguration retain the charge
until outstanding bitmap owners or capture buffers release their leases.
Disk-backed materializations also contribute to MEMORY, not DISK. The storage
and materialization budgets remain independent despite sharing this gauge.

Disk usage is synchronized after:

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
- the shared compressed-payload budget mutex for short accounting updates;
- the materialization-budget mutex for non-blocking byte reservations;
- atomic `enabled_`;
- atomic reported disk metric bytes.

`Get` and `Put` re-check `IsEnabled()` after acquiring `state_mutex_`. Memory
lookups that already acquired an immutable snapshot may finish after disable,
clear, or config refresh; they do not access the backend again.

Backend concurrency:

- `EntryPool` retains a shared index lock only while finding and retaining the
  payload. Decoding holds neither the pool lock nor the manager state lock;
  puts and erases take the exclusive pool lock.
- `DiskSlotFile` uses a shared mutex around slot metadata and file operations.
- Disk used-byte accounting reads `DiskSlotFile` metadata under its shared mutex.

### 11. Disabled Path Performance

The feature is disabled by default.

For the RawData integration, returning early inside cache-manager
methods is not sufficient: reaching those methods or a per-batch cache wrapper
would already add work to the hottest path. `CompileExpressions()` checks the
process enable flag once, after existing compilation and optimization. When it
is false, it returns the original expression tree unchanged. No raw-cache
adapter or state is created, and `Eval()`, `MoveCursor()`, and `PrefetchAsync()`
dispatch exactly as they did before this feature.

The single setup-time enable check is the only allowed new operation for a
query compiled while cache is disabled. In particular, the disabled RawData
path has:

- no per-expression or per-batch atomic load or cache branch;
- no cache signature construction or hashing;
- no manager lookup, lock, admission observation, or config-epoch read;
- no timer calls;
- no capture bitmap allocation, reserve, append, copy, or compression;
- no extra cursor state or cursor update;
- no expression-tree decoration or additional virtual dispatch.

Enablement is therefore snapshotted at expression-tree construction for the
RawData adapter. Enabling cache affects newly compiled expression trees; it does
not retrofit an in-flight undecorated tree. Disabling cache still takes effect
immediately in the manager for get/put safety. An already decorated in-flight
tree may fall back to its wrapped evaluator, but this cost applies only to a
tree created while cache was enabled.

Disabled-path equivalence is an acceptance gate, not an informal goal. Unit
tests verify that no adapter/cache hook is reached, and allocation plus CPU
microbenchmarks must show no statistically significant regression from the
pre-feature RawData path.

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
- query-path cache-only key construction, bitmap cloning, lookup,
  materialization, compression, and storage exceptions are contained at the
  cache boundary; source-of-truth expression evaluation remains outside that
  boundary and its exceptions continue to propagate normally.

### 13. Relationship with Two-Stage FilterBits Cache

Expression result cache and `FilterBitsNode` cache are different cache layers.

- Expression-level cache key: a sub-expression signature, such as a TextMatch expression.
- `FilterBitsNode` cache key: the whole filter expression plus dynamic filter context such as entity TTL physical time.

Both can reuse `ExprResCacheManager`.

For two-stage search, `QueryContext` can allow whole-filter cache reads and
writes while disabling sub-expression cache writes. This prevents caching both
a full-filter bitmap and duplicate child-expression bitmaps for the same
request path. Existing sub-expression entries remain readable; on a miss, the
request evaluates normally without affecting admission or populating the
sub-expression cache.

## Correctness Guarantees

- **No stale row-count reuse.** Every cache hit verifies `active_count`.
- **Nullable correctness.** Result and validity bitmaps are cached together.
- **Growing is opt-in.** Memory mode rejects growing segments unless
  `queryNode.exprCache.memory.enableGrowing=true`.
- **Growing text indexes are excluded.** Text-index expressions bypass cache
  reads and writes regardless of `memory.enableGrowing`, because index
  visibility can change without changing the active row count.
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
- Growing TextMatch, PhraseMatch, and TextMatchFuzzy bypass both cache writes
  and pre-existing stale entries even when growing caching is enabled.
- RawData disabled-path structural, allocation, and CPU equivalence tests.
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
