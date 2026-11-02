# Index ownership and request readers

## Motivation

The current inventory holds a cache slot whose cell is an `IIndexReaderBase`.
Pinning that cell both loads the index and obtains its query interface. This
couples the lifetime of the query object to the lifetime of the cached engine.
Passing an operation context through every query method would consequently
spread storage concerns across the scalar query contracts.

Separate the long-lived index version from the request view instead. A segment
snapshot retains an Index. A query creates its own Reader with its operation
context and uses the existing scalar query methods without context arguments.
The implementation starts from master `92a260c35e`; it does not transplant the
previous push-down branch.

## Public contracts

```cpp
class IIndex {
 public:
    virtual ~IIndex() = default;
    virtual IndexMetadata Metadata() const = 0;
    virtual IIndexReaderBasePtr CreateReader(OpContext* op_ctx) const = 0;
    virtual void CancelWarmup() const = 0;
};
```

`Metadata()` returns only already-known facts and performs no I/O. Its count is
optional: unknown and zero are distinct. Family-specific load metadata stays
inside the concrete Index. Reader `Count()`, `Caps()`, `ValueType()` and
`CoordDomain()` remain query interfaces; a JSON projection or growing snapshot
may describe a narrower view than the stored index.

`CreateReader()` creates a lightweight request view, without opening files or
pinning the whole engine. The reader retains shared ownership of that exact
Index version. It borrows `op_ctx`, which must outlive the reader and any deferred
operation using it. A null context denotes a deliberate context-free operation.
The Index never retains one query's context for another query.

`CancelWarmup()` requests cancellation of background prefetching. Replacement
and removal call it after publishing the new segment state. Readers and old
snapshots may still hold the old Index, so waiting for its destructor would
unnecessarily keep prefetching that retired version. Cancellation must not
invalidate existing readers or cancel their demand loads. No warmup means a
no-op; calling this method does not promise that all outstanding I/O has drained.

`IIndex` has no `In`, `Range`, `Search`, `PinEngine`, `PinCells`, `MemoryUsage` or
`CellByteSize` interface. In particular, an Index is neither a cache cell nor a
CacheSlot. The cached storage object (or backend engine) implements `CellByteSize()` for the existing cache
accounting contract. Cache-managed bytes are not charged again through each
request reader.

## Ownership and calls

```text
Segment snapshot
  -> shared Index for one version
       -> ready engine OR CacheSlot<Engine>

Request
  -> retains segment snapshot
  -> Index.CreateReader(op_ctx)
       -> Reader retains Index, borrows op_ctx
       -> Reader.In(values) / Range(...) / Search(...)
            -> obtain required engine/cells using the bound context
            -> execute query
            -> release temporary pins
```

Concrete Index and Reader implementations for one family live together. Query
mixins remain on Reader. Consumers may cross-cast the base reader to the required
query mixin, as they do on master. No query operations are added to `IIndex` to
avoid that cast.

A backend iterator must retain its engine pin for as long as it can access the
engine. The same applies to borrowed raw data. `Gather` callbacks may borrow
values only within the callback, while `Lookup` returns an owned value. Keeping
the Index alive alone does not keep an evictable cell pinned.

### Bitmap example

```cpp
auto index = CreateBitmapIndex<int64_t>(postings, options);
auto first = index->CreateReader(first_ctx);
auto second = index->CreateReader(second_ctx);
auto* predicates = dynamic_cast<IScalarPredicateReader<int64_t>*>(first.get());
auto matches = predicates->In(values.size(), values.data());
```

`BitmapIndex` directly owns `BitmapPostingStorage`, which contains postings,
validity and reverse lookup pointers and provides `CellByteSize()`. There is no
additional BitmapEngine wrapper. Other scalar families likewise keep their
existing storage types unless they actually use a separate backend engine.

The two readers share postings, validity and reverse lookup mappings. Creating
a reader does not copy or rebuild those structures. For an evictable bitmap,
`In` acquires the engine using its bound context and releases that pin after
materializing the result bitmap. Existing whole-engine loading remains valid;
this change does not implement block loading.

### JSON and ARRAY

A JSON Index owns child Index handles. Resolving a path creates a child request
reader with the same operation context, rather than caching request readers in
the parent. Path existence and validity retain their existing null semantics.
ARRAY predicates continue to return element coordinates. The query uses offsets
from the same segment snapshot to project those results into row coordinates;
the Index does not retain another generation's column offsets.

### Vector and native lazy loading

Vector readers also bind their context at creation. Dense vectors, sparse
vectors and VECTOR_ARRAY follow the same ownership rule. A resident native
lazy-loading engine can be held directly; an externally cached engine is acquired
through its internal slot. These are mutually exclusive ownership modes, not two
independent engine pointers that must be kept consistent.

An iterator retains the exact engine and its backend workspace, including any
required pin. This migration changes the Milvus ownership and access boundary;
it does not change Knowhere's internal loading algorithms or make synchronous
operations asynchronous.

## Load and publication boundaries

The final load path returns `IndexPtr` from IndexLoader and the registry's load
entry. Artifact conversion similarly returns an Index. The registry continues
to select family factories; it does not register per-request readers.

The inventory stores an Index or a lazy factory for it. Delayed initialization
must remain retryable after failure and must not publish a partially initialized
Index. Preparing a loader, parsing file metadata and creating a CacheSlot are
not side effects of `CreateReader()`. They belong to load/lazy initialization.
Known load metadata may avoid I/O; missing legacy metadata may require I/O in
that initialization path. No scalar index version or new format is required
solely for this object split.

Replacement publishes the new snapshot before cancelling the old Index's
warmup. An in-flight query continues against its captured snapshot. Failed
staging must not retire an Index still used by the published state.

## Future inverted-index integration

The reference proposal is [倒排索引新架构](https://zilliverse.feishu.cn/page/C1iNmyb5udcrNfaonmDcPPwGnch),
published source revision `8b8fce5141f45226e8ff5a323e7c2e615caa7e35`.
Its long-lived `InvertedIndex<Key>` fits inside the concrete Milvus Index. The
Milvus request Reader can delegate to library query methods with its bound
context. The library's cached storage maps byte ranges to cells and owns leases;
Milvus does not need to expose cell IDs through `IIndex`.

Do not put a second evictable whole-engine cache around a library that owns its
own cells. That would duplicate accounting and discard its cache with the outer
cell. The current entry-stream storage API reads an entire entry; true byte-range
access, partial integrity checks and encryption handling remain separate work.

## Incremental implementation and verification

1. Introduce the Index contract and split direct Bitmap ownership from its
   request readers. Keep old loader-facing factories temporarily so this commit
   remains independently usable. This step does not claim cache push-down.
2. Migrate the remaining scalar families, including ARRAY and JSON ownership.
3. Migrate vector and growing views, including sparse, VECTOR_ARRAY and native
   lazy-load paths.
4. Switch loaders, artifact conversion, inventory and query consumers to Index;
   move cache ownership inside implementations and remove transition factories
   and reader resource-accounting contracts.

Each commit gets a separate static review, with findings resolved before the
next step. Build and focused tests are performed separately from that review.
Verification must cover shared-resource lifetimes, owned/borrowed results,
three-valued null behavior, old snapshots surviving replacement, lazy-load
retry, query cancellation and warmup cancellation. Passing direct Bitmap tests
alone does not verify the later cache and publication changes.

Loader migration status: family loaders and `LoaderEntry::Load` now return
`IndexPtr`. JSON path indexes own the child Index and share path/existence
metadata across request readers. Segment translators still create a reader at
their existing cache boundary as a transitional adapter; the segment inventory
and concrete storage caches have not switched yet.

Marisa now supports resident storage or a typed `CacheSlot<MarisaIndexStorage>`.
Its request reader pins storage once per operation, including the complete
`Gather` callback scope. Known count/type/capabilities remain available without
loading; an unknown count is read from storage with the request context. A
permanently resident slot uses the cache's borrow fast path. This implements the
query-side boundary; production loader and segment construction of these slots
is still pending, so existing loads continue to use resident Marisa storage.
