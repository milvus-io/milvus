# Growing mutable scalar index

## Purpose

Accelerate scalar predicates on Growing segments as inserts arrive, using the existing query API.
The draft is opt-in through `queryNode.segcore.mutableScalarIndex.enabled=true`; it defaults to
false and requires a restart to change. Existing specialized Growing indexes remain in place.

Ordinary Growing scalar fields otherwise rely on raw evaluation. An ordered value directory can
avoid inspecting every value for selective equality, IN, range, and literal prefix predicates.
Broad predicates still pay for postings traversal and result bitmap construction. Query speedup,
insert overhead, and memory cost require measurement; this draft makes no performance claim.

## Data flow and ownership

```text
Existing insert / Growing recovery
             |
      SegmentGrowingImpl -------> retained raw columns -------> value lookup / fallback
             |
       GrowingIndexSet
             |
    per-field appendable owner
             |
     published prefix reader -------------------------------> Expr + visibility masks
```

| Component | Responsibility |
| --- | --- |
| `SegmentGrowingImpl` | Keep its existing resource accounting, feed contiguous raw-ready rows, then publish the visible insert prefix. |
| `GrowingIndexSet` | Select eligible fields, own their appenders, and expose existing abstract reader capabilities. |
| `MutableScalarGrowingIndex<T>` | Maintain value memberships and publish fixed-prefix readers over shared storage. |
| Expr | Use existing predicate dispatch, bound results to the query prefix, and retain existing NULL and visibility semantics. |

The owner is ephemeral derived state. Existing insert/recovery paths reconstruct it; no new WAL
message, persisted index format, CDC state, or seal-time release mechanism is introduced. Its
lifetime follows the physical Growing resource and its outstanding readers.

## Field storage and append

- Initial types are BOOL, INT8/16/32/64, TIMESTAMPTZ, and VARCHAR. Primary-key fields, system fields,
  and VARCHAR fields with text match are excluded. PK lookup retains the existing PK-to-offsets
  mapping without building another B-tree. FLOAT/DOUBLE, ARRAY, JSON, TEXT, and spatial/vector
  types keep their existing paths. This feature is append-only and does not implement PK-update histories.
- The first backend uses `absl::btree_map<value, stable Posting handle>`. Postings hold increasing `uint32_t`
  row offsets in an inline-capacity vector; append rejects row ranges exceeding 2^32 rows before
  modifying storage. Distinct string keys are owned; borrowed append batches are not
  retained. Raw columns remain the value source, without a second full values array.
- Membership uses offsets lists only. There is no per-value cached bitmap, admission/retention
  threshold, query-hit counter, LRU, or bitmap-specific budget/configuration.
- Numeric and string feed allocate a temporary validity array only for nullable fields;
  non-nullable batches pass `valid == nullptr` through the existing append contract.
- NULL rows advance coverage and are represented by `SharedValidityBitmap`, independently of
  non-NULL value keys. Readers freeze validity, including a private partial tail word.
- Reuse the current `IGrowingIndex`, `IAppendable<ScalarBatch<T>>`, and request-reader contracts.
  Concrete data structures stay behind the construction factory; Segment and Expr require no
  B-tree query API. ART and specialized BOOL/bitmap backends are follow-up comparisons.

Adopt the existing [Abseil B-tree implementation](https://abseil.io/about/design/btree), including
its iterator invalidation rules. Adapt it with synchronized append-only postings and Milvus's
[Growing publication contract](../../../internal/core/src/index/contracts/README.md#growing-publication-protocol).
Do not add a second index manager or per-posting MVCC timestamps.

## Synchronization

- A per-field writer mutex serializes append and publication. A separate tree reader/writer lock
  protects structural insertion and query lookup/traversal. An existing-value writer lookup can
  proceed without a tree read lock because the writer mutex excludes every structural writer.
- Append reuses `lower_bound` for both membership lookup and hinted new-key insertion. Only an
  absent VARCHAR value creates an owned key. Tree insertion completes before locking a Posting.
- Each Posting has its own reader/writer lock for offsets traversal and append/reallocation.
  Heap-allocated Postings keep stable addresses through Abseil node movement; the storage owns
  them and each reader retains the storage. Posting allocations use the existing charged allocator.
  Keep InlinedVector and this lock: append-only values do not prevent vector reallocation from
  invalidating a concurrent reader. A separate completed length alone would not make it safe.
- Point queries take a Posting address under the tree read lock, then release that lock before
  emission. Range/prefix queries collect at most 256 addresses per lock interval, copy a resume
  key, and emit after unlocking. They retain no tree iterators or key references across unlocks;
  an initial upper key prevents following a continuously growing right edge.
- Index execution waits on the required locks. No lock-contention-triggered raw scan fallback
  is introduced. The existing nonblocking publication check on reader pin is unchanged.

## Range complement execution

- Adapt the existing BitmapIndex `UnionKeyRange` rule: either set matching non-NULL offsets
  in an empty result, or start with the pinned validity bitmap and clear excluded offsets.
  Single-bound and interval ranges estimate the cheaper side. Equality/IN
  and literal prefix retain their existing positive paths; NOT IN/inequality retain inversion.
- Do not rebuild the sealed index's static `range_prefix` table on append. Query-local cursors
  enumerate the inside and the two outside ranges, advancing the side with the smaller count
  accumulated so far. Once a complete side's estimate is no larger than the other's accumulated
  estimate, counting stops. Each Posting exposes a separate atomic count, updated after a
  successful offsets append and read with relaxed ordering without taking the Posting lock.
- Counts are cost hints: they may lag an append or include rows newer than the pinned prefix.
  They never authorize access to offsets or determine result visibility. An inaccurate estimate
  may choose a more expensive direction; both directions remain correct. The chosen cursor(s)
  rewind for emission using the same owned upper-key bound. Emit retains its Posting read lock
  and checks the pinned row boundary, including when new keys appear between the two passes.
- Set and clear emission are separate compile-time specializations using the existing bitmap
  `set`/`reset` operations. The selected direction is fixed outside the Posting loop; query
  semantics, NULL handling, locks, and prefix checks are unchanged. Per-word grouping is deferred
  until its benefit is measured for sparse as well as clustered offsets.
- Memory stays bounded to three small cursor buffers; no per-key prefix statistics or bitmap
  cache is added. The query-local probing policy is an implementation choice to validate,
  not a claimed industry standard or measured speedup. Near-balanced ranges can count both
  sides before emission; old snapshots may encounter many keys with no visible offsets even
  though their latest count hints are nonzero.

## Publication and query bounds

The existing ordered feed accepts a contiguous range in every field owner before advancing the
Segment ACK. Each owner publishes a reader with fixed `Count()` and `CoveredRowEnd()`, while its
tree remains appendable with separate structural and Posting locks. Accepted-range retries do not append twice.
A publication allocation failure leaves the previous publication intact and retries publication
of accepted data. A failure during mutation poisons storage: subsequent reads and appends fail
closed rather than using a potentially partial tree. Existing live apply failure handling remains.
The serialized writer publishes the original exception with a release/acquire failure flag
before releasing the structural/Posting lock after a mutation failure;
queries check it without holding the writer mutex. Old readers still emit only offsets below
their captured count and use their immutable validity snapshot.

For a query timestamp, existing timestamp/ACK logic supplies `N = get_active_count(Q_TS)`.
The reader may cover more than `N` rows; Expr consumes only `[0, N)`, followed by existing
delete/TTL visibility masks. The explicit-offset path accepts a larger bitmap while checking
each requested offset against `N`. No timestamp is duplicated in B-tree postings.

`ReaderCaps::point_and_range` advertises equality/inequality, IN/NOT IN, and ordered ranges through
`IScalarPredicateReader<T>`; it does not advertise arbitrary expressions. NULL queries use
`INullReader`. For VARCHAR, `pattern_match` exposes the pattern interface, but
`ShouldUseForOp` accepts only literal `PrefixMatch`. Postfix, substring, general LIKE, regex,
other unsupported expressions, and all value lookup keep their raw-column path. Existing
shared expression-cache configuration is preserved; future POC runs should disable it explicitly
to isolate index execution.

## Configuration and memory

All supported predicates emit matching offsets into the existing query-private result bitmap.
NULL queries retain `SharedValidityBitmap`; result and validity bitmaps are query contracts,
not per-value membership caches. Cached membership and the experimental 0.5% density policy
are deferred to keep this iteration simple. The prior emission microbenchmarks remain useful
evidence for future work, not an enabled policy.

| Configuration suffix under `queryNode.segcore.mutableScalarIndex` | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Enable eligible field owners. |

Tree nodes, keys, stable Posting allocations, and postings capacity use one counter that
charges/refunds the cache manager directly on
allocation/release. Storage objects and shared validity buffers use the same accounting through
lifetime guards; old readers retain their buffer charges until release. Segment resource
estimation and its existing tracking calls remain unchanged and exclude these self-charged
bytes. The scalar owner's `IGrowingIndex::MemoryUsage()` returns this counter without charging
it again. Other owners inherit `nullopt` until they implement reporting; `GrowingIndexSet`
sums available reports for the segment's memory-size report, not a complete total for all index
families. The factory returns only the owner, with no separate accounting callback. These are
allocation counts, not process RSS. Automatic whole-index eviction is outside this draft.

## Draft verification and follow-up

The 2026-11-29 follow-up excludes primary keys at the shared initialization/reopen factory and
specializes set/clear emission. Local source hash `4bf04e44e693` passed the full Release C++/Go
build (GCC 12.3); runtime validation and performance measurements are pending. The results below
apply to the earlier atomic-count revision, not this follow-up.

On 2026-11-28, the atomic-count revision passed a full Release C++/Go build and static E2E
comparison against commit `938e3ae23ba87fb543a84bde42a230b938129757`. The new image used local
source hash `ad5f07a8031a`. Both 1-CU instances served the same frozen 10,570,000-row dataset in
one Growing segment, with no Sealed segments and expression cache disabled. IN and two range
cases each ran three single-client measurement windows after warmup, with alternating version
order. All queries returned the independent oracle's expected counts and used the scalar-index
path. For the historically named 10% and 90% range cases, p50 decreased by 18.2% and 12.7%, and
process CPU per query decreased by 16.8% and 12.9%; IN was essentially unchanged. These are
workload-specific static measurements, not concurrent-write or general performance guarantees.

Static ownership, lock-order, boundary, and failure-path review was performed. The earlier
independent Python model checked 14,641 cost-selection cases and 51,285 integer/string range
queries, including NULLs, inclusive/exclusive bounds, and older prefixes. An additional model
checked 10,000 cases with inaccurate count hints: 693 changed execution direction, with no
result mismatch. These are algorithm-model checks, not C++ concurrency or fault-injection tests.

Before production enablement, validate full reference-scan equivalence, concurrent append/read,
older snapshots, NULL/logical composition, explicit offsets, recovery/schema reopen,
allocation/publication failure, and lifetime/accounting behavior. Compare scan, B-tree, and ART
with identical postings and workloads, including insert cost and SN RSS.
