# MEP: Expression Result Cache

- **Created:** 2026-06-02
- **Author(s):** @luzhang
- **Status:** Under Review
- **Component:** QueryNode / Segcore
- **Related Issues:** #55658
- **Follow-up design:** [sealed leaf cache, disk lifetime, signatures and selective reads](20261118-expression-cache-followups.md)
- **Released:** N/A

## Summary

Expression Result Cache is a QueryNode-local cache for scalar filter expression bitmaps. It stores the full active-segment result bitmap produced by an expression and reuses it when the same expression is evaluated again on the same segment snapshot.

The cache is implemented in segcore through `ExprResCacheManager`, with two storage modes:

- **Memory mode:** heap memory, adaptive bitmap compression, frequency and latency admission, and Clock eviction.
- **Disk mode:** one fixed-slot cache file per sealed segment generation, direct `pread`/`pwrite`, frequency and latency admission, per-segment Clock eviction, and global segment-file eviction.

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
- Supporting growing segments in either backend.
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
    ExprCacheSignature signature; // XXH3-128 + shared canonical identity
    ExprCacheGenerationPtr generation;
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
RetiredGeneration RetireGeneration(int64_t segment_id,
                                   const ExprCacheGenerationPtr& generation);
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
  | RawExprCacheAdapter
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

The external cache key is `(segment_id, generation, expression_signature)`.
Sealed queries take `generation` from their pinned `SegmentReadSnapshot`;
missing snapshots and growing segments bypass caching. Manager entries without
a live snapshot token are rejected. `MATCH` signatures include the complete child
predicate as well as struct name, match type, and count.

The expression signature retains a shared immutable canonical input alongside
its XXH3-128 hash, lazily memoized with `call_once` on the logical expression and
reused across segment executions. Most leaves use `ToString()`; IN signatures
sort and deduplicate typed serialized values without changing evaluation order.
Hashes locate candidate entries, but equality checks the full canonical input
and derivation domains. Hash collisions cannot by themselves establish a hit.
The signature input must include every parameter that can affect the result:

- field id,
- operator type,
- literal values,
- JSON path,
- text query,
- match options,
- query-time context when relevant.

RawData adapters derive a domain-specific hash from the logical signature and
null-rejection mode, retaining both the shared canonical input and the complete
domain sequence for exact comparison. A null-rejecting scan may omit validity for skipped chunks;
its identity must differ from a scan preserving UNKNOWN and from index results.
`ExprSet` propagates the mode before prefetch/evaluation and stops at `NOT`.
Lookup, admission and insertion use the same derived identity.

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

Both backends require sealed snapshot generations. `EntryPool` keys contain
segment id, generation, a 128-bit hash and shared exact signature storage. Active
count remains entry metadata for defensive shape validation. The memory budget
conservatively charges the full retained key per entry, even when its string is
shared; replacing an entry charges the capacity of the map key that survives. In-flight readers can retain a
replaced immutable payload until their last use.

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
neighbors and points to the key owned by its hash-map node. Inserts and erases update only the affected links,
and replacement retains the node and its Clock position. Hash-map rehashing
preserves key addresses. Lookups update an atomic usage counter; puts take the
exclusive index lock. Clock skips payloads held by readers, since removing
their entries would not immediately free memory. A sweep examines at most
3072 entries (including second-chance visits), independent of cache size. It temporarily
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
[SlotHeader 25B][raw result bitmap][raw valid bitmap]
```

Each segment owns one cache file:

```text
<localStorage.path>/cache/<nodeID>/expr_cache/seg_<segment_id>_<generation>_p<pid>_<process_nonce>_<file_sequence>.cache
```

The in-memory slot index retains the exact canonical identity and derivation
domains, and verifies them before selecting a slot. The header hash is an
additional check; it is not the sole expression identity. Colliding signatures
can occupy distinct slots. Disk files are temporary and never restored.
Config rebuild detaches current
files and cleans abandoned files outside manager locks. Unique process/file
identities prevent delayed cleanup from deleting a replacement file; files
retained by in-flight readers are closed and unlinked by their last owner.

Never-used slots are allocated incrementally with a cursor. The free-slot list
stores only recycled IDs, so its memory does not scale with the file's maximum
slot count. Allocation reuses a recycled ID first and runs Clock eviction only
when no recycled or never-used slots remain.

If the same segment later appears with a different row count, the fixed slot file no longer matches the bitmap shape. The manager removes the file and marks the segment ineligible for disk caching until the segment or config is reset.

Disk mode has two size limits:

- `queryNode.exprCache.disk.maxFileSizeBytes` limits one sealed segment file and determines how many fixed slots the segment file can hold.
- `queryNode.exprCache.disk.maxBytes` limits total logical used-slot bytes across disk cache files. The budget counts `FileHeader + used_slots * slot_size`, not the full preallocated file capacity.

Within one `DiskSlotFile`, slot eviction uses Clock. Across segment files, `Get` hits and `Put` writes touch a segment-level Clock usage counter. After a disk `Put`, the manager checks total used-slot bytes. If usage exceeds `disk.maxBytes`, it scans segment files with a second-chance Clock policy and evicts whole segment files whose usage counter has decayed to zero, skipping the segment that was just written.

Global manager locks cover metadata only. A file is created outside those locks
and registered after its config epoch and generation are rechecked. Readers
retain shared file ownership and release manager locks before taking a file lock.
Writers serialize per file, reserve an unpublished slot, release the file index
lock for pwrite, then publish under that lock. Replacement retains its old slot
until success and is skipped when no staging slot exists. New empty/failed files
are detached; delayed writes cannot republish a retired generation or modify a
replacement backend. Detachment/eviction releases the last owner outside locks.

### 4.1 Full-Bitmap Materialization Budget

The storage budgets do not cover the decoded bitmaps returned by `Get`:
`memory.maxBytes` counts compressed `EntryPool` payloads and estimated metadata, while
`disk.maxBytes` counts disk slots. Both modes therefore use the independent
`materialization.maxBytes` heap budget.

Each backend first finds the matching entry or slot and checks its active
count under a shared lock. Missing or stale entries return `Miss` without
reserving bitmap capacity or allocating a result holder. Memory lookup retains
an immutable payload reference and releases both manager and pool locks;
disk lookup retains the file owner and uses only its per-file shared lock
during pread. Prefetch retains a metadata handle without allocating bitmaps.
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

Returned bitmaps own their materialization reservation. Raw adapters may also
retain a compressed read handle for later offset batches. Exceptions release the
lookup's payload reference and materialization reservation through ownership
cleanup. The storage reservation remains while the payload is still indexed
or held by another reader. Neither reservation's release path acquires the
manager or pool lock.

For offset inputs, reservation size is based on the requested row count. Raw
payloads are read by bit, Roaring payloads by membership lookup, and disk slots
by touched words; caller order and duplicates are preserved. A sequential batch
covering the entire segment moves the private pair into its output, retaining
the lease with that output rather than copying the bitmap buffers.

### 5. Admission Control

Two admission policies are applied before writing a new entry:

- **Latency admission:** skip expressions whose miss-path evaluation duration is lower than `queryNode.exprCache.minEvalDurationUs`.
- **Frequency admission:** count observations by segment id, generation, expression signature, and active row count; admit the snapshot when its counter reaches `queryNode.exprCache.admissionThreshold`.

Frequency admission is mode-independent and is owned by `ExprResCacheManager`, not by `EntryPool`, so memory and disk use the same policy.

The tracker uses 262,144 one-byte counters (256 KiB per process). When frequency
admission is enabled, every 10,000 admission records trigger a synchronous
scan that halves all counters. The decay interval is independent of the slot
count, keeping old observations from accumulating as the table grows.

Existing keys do not bypass admission. The bounded tracker is approximate:
hash-slot collisions can share counters. Hashes combine segment id, generation,
the 128-bit hash and active count; exact storage identity also checks canonical
input and domains, but excludes active count.

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
2. Reject growing segments, missing snapshots and non-cacheable system TTL predicates.
3. Reuse the logical signature to form `(segment_id, generation, signature)`.
4. Attempt cache `Get`.
5. On miss, compute the full-segment bitmap.
6. Measure evaluation duration when cache admission may need it.
7. Attempt cache `Put`.

For Volcano-style batched expressions, `BatchedCachedMixin` loads or computes the full-segment bitmap once, then slices it on later `Eval()` calls.

#### Current sub-expression support

This subsection describes the process-level leaf cache controlled by
`queryNode.exprCache`. Two-stage search uses this same cache, as described in
[Two-Stage Search and Entity TTL](#13-two-stage-search-and-entity-ttl).

The current implementation is not a generic cache around every expression
evaluator. It is integrated only into execution paths that produce a reusable
full-segment result bitmap. Selecting the same logical expression does not by
itself make it cacheable: the expression must also select one of the integrated
execution paths below.

| Execution path | Integrated expressions | Conditions and exclusions |
|----------------|------------------------|---------------------------|
| Scalar index, including JSON path indexes | `UnaryRangeExpr` (comparisons, supported string pattern operations, and indexed array equality), `BinaryRangeExpr`, `TermExpr`, `JsonContainsExpr` (`Contains`, `ContainsAny`, and `ContainsAll`), JSON `ExistsExpr`, and the scalar-index branches of `BinaryArithOpEvalRangeExpr` and `TimestamptzArithCompareExpr` | The concrete scalar index must accept the operator. The generic integration covers normal scalar indexes and compatible JSON flat/path indexes. An operator rejected by `ShouldUseOp` or an incompatible JSON path/type may fall back to `RawData`; it then participates only when it is in the RawData adapter's supported scope. NGRAM is explicitly routed to its own execution path. |
| JSON statistics | JSON `UnaryRangeExpr`, `BinaryRangeExpr`, `TermExpr`, all implemented `JsonContainsExpr` variants, and `ExistsExpr` | Sealed segments only. `plan_options.expr_use_json_stats` must be enabled, JSON stats must exist, and the nested path must be non-empty and contain no numeric path component. |
| Text index | `TextMatch`, `PhraseMatch`, and `TextMatchFuzzy` in `UnaryRangeExpr` | Uses `segment->GetTextIndex()`. Sealed segments only in both memory and disk modes. Growing text-index results bypass cache reads and writes. |
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
| Eligible row-level RawData, with or without `SkipIndex` assistance | yes | no | yes | no |
| PK / NGRAM / GIS and other excluded paths | no | no | no | no |

Both backends reject growing segments before lookup or insertion. Scalar-index,
JSON-statistics, text-index and eligible RawData expressions can use the sealed
leaf cache. Two-stage requests can additionally use whole-filter caching.

#### RawData and SkipIndex integration

> **Status:** implemented. The support tables above include this path.

This implementation extends the memory and disk expression-cache modes to selected
row-level `SegmentExpr` implementations whose resolved execution path is
`ExprExecPath::RawData`. Two-stage search can read these leaf entries, while
requesting whole-filter caching and suppressing raw leaf writes.
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
- retain an internal read-only adapter policy for callers that disable writes;
  ordinary requests enable raw leaf writes; two-stage requests suppress them
  to favor whole-filter entries;
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
       -> hit: retain a metadata read handle; do not prefetch raw data
       -> miss: delegate to the wrapped expression's existing raw-data prefetch

Eval
  -> adapter repeats the lookup lazily when the caller did not run prefetch
  -> hit: decode on demand; move a whole batch, slice batches, or read requested bits
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
supported leaves. Ordinary request compilation enables leaf writes. The internal write
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

    // Metadata retained by prefetch; a sequential hit decodes lazily.
    ExprResCacheManager::ReadHandle reader;
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
approximate counting. `PutAdmitted()` skips only the duplicate frequency check;
latency, shape, generation, capacity, compression and eviction checks remain.
Tickets bind the admission hash to the config epoch. Growing segments and
retired/missing tokens cannot receive an admitted ticket. A config refresh or
disable/enable transition invalidates previous tickets. Latency admission still
runs after evaluation because its full duration is not known at first admission.

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
| Eligible row-level RawData expression, with or without SkipIndex assistance | yes | no | yes | no |

This target matrix does not override expression-level exclusions or the mutable
column restriction. Disk remains sealed-only and retains its fixed row-count
validation.

##### Verification matrix

Raw-cache support is a behavioral change, so tests must prove both the stored
value and the work bypassed by a hit:

- **Admission:** threshold 2 observes once per expression instance rather than
  once per batch; frequency hashes include segment id, signature, and active
  count; one-off keys skip full-bitmap capture until admitted;
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
  skipped/masked batches, NOT, distinct signatures, and sealed generation
  invalidation. Counters must prove that hits bypass the actual evaluator and
  raw prefetch. Invalid JSON arithmetic must still throw without caching.
- **Backend/segment matrix:** memory sealed, disk sealed, and growing rejection
  in both backends.
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

Each sealed `PublishedSegmentState` owns an `ExprCacheGeneration` token with a
process-unique, monotonically allocated ID. Staging a state can consume an ID;
only the published snapshot is visible to queries. IDs need not be contiguous,
are never reused within the process, and are not stored in segment metadata.
Disk cache files are not reused across restarts.

Read-state forks and adoption clone the published metadata with a fresh token
while sharing column, index, and reader resources. Publishing or destroying one
instance therefore does not retire another instance's cache generation.

All online state publications pass through `PublishStateOnline`. After acquiring
the exclusive `PublishLease` and draining old readers, it retires the previous
token with an atomic flag and publishes the new state before releasing the
lease. Backend detachment happens after releasing the lease, because acquiring
backend cleanup must not hold a segment publication lease. Gate acquisition failures
and cancellation leave the previous token and
cache intact. Successful publications conservatively invalidate the entire
segment generation, including schema/index-only changes. The sealed destructor
retires its own final token; it never erases a newer instance with the same ID.
There is no generation-zero erase API; cleanup uses the exact sealed token.

`Get`, admission, and `Put` check the atomic retirement flag under the manager
state lock. Puts that passed the check before retirement may finish in the old
generation. Disk detachment does not wait for their I/O; they retain an owner
but cannot republish themselves. Memory detachment drains its pool operations.
Retirement remains effective across `Clear`, config rebuild, and disable/enable
because the flag belongs to the token rather than the cache backend. A delayed
writer carries its original snapshot token; it cannot recreate retired entries
or write its bitmap under the latest generation. Admission tickets also include
generation in their key hash.

Memory entries have a secondary intrusive list per `(segment_id, generation)`.
Retirement visits only that generation, detaches entries from the lookup map and
Clock, and returns an allocation-free ownership chain. Disk retirement removes
that generation's file and Clock/ineligible metadata from the manager. Payload
and file destruction happen after both cache locks and the PublishLease are
released. Reader-held payloads remain charged until their last owner releases
them. Logical disk usage drops at detachment; physical unlink can finish later.
Generation-specific filenames keep delayed cleanup from deleting a new file.
Failed unlinks leave disposable files for directory cleanup on rebuild.

The tradeoffs are extra per-entry list links/index metadata, a shared token per
snapshot, and cache warming after every publication. Retiring a heavily cached
segment still takes time proportional to its own entry count under the manager
lock; it does not scan unrelated segment entries. Existing frequency counters
are bounded/aged and can retain old generation hashes; they cannot authorize a
retired token. Out-of-band object overwrites without a Milvus state publication
are outside this mechanism: refresh/reopen must publish the new reader state.

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
`ExprResCacheManager::ReadWithStatus` returns `Hit`. Metadata lookup alone does
not increment the hit counter. Misses, stale entries, and
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
- `DiskSlotFile` readers hold a shared file lock during pread; writers hold a
  separate writer mutex and publish completed slots under the index lock.
- Global accounting reads atomic used-slot counts without acquiring file locks.

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

### 13. Two-Stage Search and Entity TTL

`FilterBitsNode` retains the existing whole-predicate bitmap cache. The two-stage
request builders set `EnableExprCache` in both stages; stage 1 can insert the
filtered-row bitmap and stage 2 can reuse it before vector search. This skips
combining/evaluating the predicate tree again. Ordinary leaf entries are not
an equivalent replacement, since masked children of a compound expression may
not produce a cacheable full-segment result.

`autoIndex.twoStageSearch.enabled` controls the two-stage algorithm (default
false, also subject to eligibility thresholds). `queryNode.exprCache.enabled`
controls the shared expression cache backend (default false). Whole-filter
reuse requires both the backend and request opt-in. The two-stage algorithm
can execute without caching; ordinary requests can use leaf caching without
enabling two-stage search. Request/proto flags and merge compatibility checks
are retained. Two-stage requests suppress raw/TextMatch leaf writes to avoid
duplicating these entries with whole-filter results.

Whole-filter entries use domain 3 of the memoized logical signature and include
the TTL field and physical time when applicable. They share snapshot generation
identity, admission, storage and materialization budgets with leaf entries.
Hits move the private decoded pair into the output with its lease. Misses and
budget failures evaluate the predicate normally; downstream MVCC/deletion
visibility still applies.

Generated TTL comparison/OR nodes carry a local non-cacheable policy,
honored by raw decoration and indexed leaf paths. This does not propagate to
the user filter or its enclosing AND, nor disable the whole-filter cache;
whole-filter TTL correctness relies on the dynamic context in its key.
The stable `TTL IS NULL` child retains its default cache policy. Currently
`PhyNullExpr` reads field/index validity without using the shared result cache;
removing a policy restriction does not add a new cache integration.

## Correctness Guarantees

- **No stale row-count reuse.** Every cache hit verifies `active_count`.
- **Nullable correctness.** Result and validity bitmaps are cached together.
- **Sealed only.** Both backends require a live sealed snapshot generation.
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
- Disk row-count mismatch rejects inconsistent snapshot shapes.
- Concurrent `SetConfig` with get/put.
- Expression integration tests for TextMatch, JSON, Exists, Term, Range, and index/stat paths.
- Growing TextMatch, PhraseMatch, and TextMatchFuzzy bypass both cache writes
  and unrelated backend entries with the same segment ID.
- RawData disabled-path structural, allocation, and CPU equivalence tests.
- Two-stage requests preserve whole-filter reuse, dynamic TTL keys and snapshot
  invalidation, while ordinary requests retain leaf caching.
- TTL evaluation, typed IN normalization, sparse hit budgets, moved-output leases,
  staging failures and retirement during blocked disk I/O.

## Known Limitations and Follow-Ups

Current limitations:

- Disk cache files are temporary process-local cache files and are not reused after restart.
- Disk mode stores raw bitmaps; compression is memory-only.
- Disk mode supports sealed segments only.
- Cache key quality depends on canonical signature stability and completeness;
  complete identity is checked even when 128-bit hashes collide.
- Metrics report usage bytes and successful reads; misses, admission and eviction
  counters remain follow-ups.

Potential follow-ups:

- Add hit/miss/admission/eviction counters.
- Add memory/disk backend latency metrics.
- Evaluate compressed disk slots if disk footprint becomes a bottleneck.
- Introduce a structured expression signature builder to reduce reliance on hand-written `ToString()`.
- Add an operational option to choose whether disabling cache should clear existing entries.
