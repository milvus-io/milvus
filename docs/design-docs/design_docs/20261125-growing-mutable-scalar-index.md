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
| Mutable scalar owner | Maintain value memberships and publish fixed-prefix readers over shared storage. |
| Expr | Use existing predicate dispatch, bound results to the query prefix, and retain existing NULL and visibility semantics. |

The owner is ephemeral derived state. Existing insert/recovery paths reconstruct it; no new WAL
message, persisted index format, CDC state, or seal-time release mechanism is introduced. Its
lifetime follows the physical Growing resource and its outstanding readers.

## Field storage and append

- Initial types are BOOL, INT8/16/32/64, TIMESTAMPTZ, and VARCHAR. System fields and VARCHAR fields
  with text match are excluded. FLOAT/DOUBLE, ARRAY, JSON, TEXT, and spatial/vector types keep
  their existing paths. This feature is append-only and does not implement PK-update histories.
- The first backend uses `absl::btree_map<value, postings>`. Postings hold increasing row offsets
  in an inline-capacity vector. Distinct string keys are owned; borrowed append batches are not
  retained. Raw columns remain the value source, without a second full values array.
- NULL rows advance coverage and are represented by `SharedValidityBitmap`, independently of
  non-NULL value keys. Readers freeze validity, including a private partial tail word.
- Reuse the current `IGrowingIndex`, `IAppendable<ScalarBatch<T>>`, and request-reader contracts.
  Concrete data structures stay behind the construction factory; Segment and Expr require no
  B-tree query API. ART and specialized BOOL/bitmap backends are follow-up comparisons.

Adopt the existing [Abseil B-tree implementation](https://abseil.io/about/design/btree), including
its iterator invalidation rules. Adapt it with synchronized append-only postings and Milvus's
[Growing publication contract](../../../internal/core/src/index/contracts/README.md#growing-publication-protocol).
Do not add a second index manager or per-posting MVCC timestamps.

## Publication and query bounds

The existing ordered feed accepts a contiguous range in every field owner before advancing the
Segment ACK. Each owner publishes a reader with fixed `Count()` and `CoveredRowEnd()`, while its
tree remains appendable under a reader/writer lock. Accepted-range retries do not append twice.
A publication allocation failure leaves the previous publication intact and retries publication
of accepted data. A failure during mutation poisons storage: subsequent reads and appends fail
closed rather than using a potentially partial tree. Existing live apply failure handling remains.

For a query timestamp, existing timestamp/ACK logic supplies `N = get_active_count(Q_TS)`.
The reader may cover more than `N` rows; Expr consumes only `[0, N)`, followed by existing
delete/TTL visibility masks. The explicit-offset path accepts a larger bitmap while checking
each requested offset against `N`. No timestamp is duplicated in B-tree postings.

Supported readers expose equality, IN, ordered ranges, NULL, and literal VARCHAR prefix through
existing interfaces. Other expressions and all value lookup keep their raw-column path. Existing
shared expression-cache configuration is preserved; future POC runs should disable it explicitly
to isolate index execution.

## Optional hot-value bitmaps and memory

Point queries can admit frequently queried values with sufficiently large postings into an LRU
cache. Each cached bitmap stores **latest membership only**. Queries copy/OR its visible prefix
into a private result; cached data never includes query timestamp, delete, or TTL filtering.
Append updates cached membership within capacity, or evicts it before publishing the new prefix.
Ranges and prefixes retain the postings path and do not create per-value cache entries.

| Configuration suffix under `queryNode.segcore.mutableScalarIndex` | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Enable eligible field owners. |
| `hotBitmapBytes` | `0` | Per-field retained bitmap-buffer budget; zero disables the cache. |
| `hotQueryThreshold` | `8` | Point-query uses required before admission. |
| `hotPostingThreshold` | `256` | Minimum postings count for admission. |

The bitmap budget does not bound total index memory. Tree nodes, keys, postings capacity, and
hot-cache allocations use one counter that charges/refunds the cache manager directly on
allocation/release. Storage objects and shared validity buffers use the same accounting through
lifetime guards; old readers retain their buffer charges until release. Segment resource
estimation and its existing tracking calls remain unchanged and exclude these self-charged
bytes. The segment's memory-size report includes the counter without charging it again. These
are allocation counts, not process RSS. Automatic whole-index eviction is outside this draft.

## Draft verification and follow-up

This draft's verification scope is C++ and Go compilation plus static source review. No runtime
tests or performance POC are claimed. Before production enablement, validate reference-scan
equivalence, concurrent append/read, older snapshots, NULL/logical composition, explicit offsets,
recovery/schema reopen, allocation/publication failure, and lifetime/accounting behavior. Compare
scan, B-tree, and ART with identical postings and workloads, including insert cost and SN RSS.
