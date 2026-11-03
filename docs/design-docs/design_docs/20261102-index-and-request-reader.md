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
metadata across request readers. The segment inventory accepts Index owners
and creates request readers. Reader cache slots remain temporarily supported
for families that have not yet migrated their concrete storage loading.

Marisa now supports resident storage or a typed `CacheSlot<MarisaIndexStorage>`.
Its request reader pins storage once per operation, including the complete
`Gather` callback scope. Known count/type/capabilities remain available without
loading; an unknown count is read from storage with the request context. A
permanently resident slot uses the cache's borrow fast path. Marisa loads, including complete JSON projections, now create this typed
storage cache through the production loading path.
The cache retains the source and fixed options, not the construction context.
Synchronous warmup runs outside the executor used to load storage, so it cannot
block that executor waiting for its own submitted task.

`SingleCellIndexStorageTranslator` is an optional adapter for loading a whole
storage object as one cache cell. Neither Index nor Reader depends on it.
Indexes with native lazy loading or their own slice caches use those mechanisms
directly. The adapter does not define a mandatory layer for all index storage.

Retirement retains the inventory entry rather than only an already-created
slot. If a lazy factory finishes after retirement, its Index receives warmup
cancellation too; existing snapshots can still create readers and demand-load
storage.

FM-index uses the same optional single-cell cache with `FmIndexStorage` as its
cell. Pattern operations retain one storage pin across engine calls and null
filtering. Cost guards reject wildcard-only LIKE and unselective regex conditions
before pinning, while count/type/capabilities come from validated packed metadata.
The existing heap/mmap decoding and candidate semantics are unchanged.

RTree 同样把整份 Boost 树和 validity 数据作为一个缓存 cell，由请求 Reader 在查询时 pin。Count 使用加载参数中已有的权威行数，不读取索引文件。向调用方返回 validity 时，使用共享句柄保留对应 cell 的 pin，直到调用方释放句柄；普通 Candidates 和空值查询只在本次调用期间持有 pin。Growing 的分片和 validity 共享方式保持不变。

Sorted 的数值和字符串索引均直接缓存各自的 `SortedIndexStorage<T>`，包括排序数据、validity 和反向坐标表。请求 Reader 的查询及 Gather 回调在同一次 storage pin 内完成；返回的 Lookup 值仍然拥有自己的数据。已解析的计数和能力保留在 Index 上，CreateReader 与元数据访问不加载 cell。完整 JSON projection 的缺失值 bitmap 随同一 Storage cell 加载和淘汰。

Bitmap 保持 Roaring 与 Bitset 两种具体 posting 表示，分别使用 `BitmapPostingStorage<T, Posting>` 作为缓存 cell。In、NotIn、Range、字符串匹配及 Gather 在一次 pin 内完成；内部辅助函数接收本次已固定的 Storage，避免重复 pin。加载器继续按已解析的基数选择表示，冻结 Roaring 的临时映射清理和取消检查保持在加载事务内。

Inverted 使用 `InvertedIndexStorage<T>` 保存 Tantivy engine、validity 和映射目录，并在每次谓词或模式查询中持有同一个 pin。普通标量索引优先使用加载参数中已知的行数；缺少该元数据时，Count 通过加载 Storage 得到。嵌套索引的元素计数不能使用 segment 行数代替，因此也保留加载后的计数。完整 JSON projection 的 sidecar 也在同一个 Storage cell 中加载。

Ngram 使用 `NgramIndexStorage` 保存 engine、gram 范围、平均行大小和 validity。每次候选查询只 pin 一次，正则分支与迭代过滤复用该 Storage。CanHandle 和 CandidatesAreExact 只读取 Index 保存的 gram 范围，不触发加载。已知行数直接用于元数据访问，旧记录缺少行数时通过加载 Storage 获取；加载器在发布缓存前校验已知行数。候选过滤算法与平均行大小 sidecar 保持原有行为。

Text 的文件加载入口直接创建 `TextIndex`，由它持有 `TextIndexStorage` 或对应的缓存 slot。Match、Phrase、Fuzzy 查询各自在一次 storage pin 内完成；返回 validity 的共享句柄保留该 pin。Growing 快照仍共享原有 validity 完整字，保留私有尾字。加载器继续在原来的事务内初始化 analyzer、校验 null offsets 和行数，并保留异步加载失败时的目录清理。

JSON flat 直接缓存 `JsonFlatIndexStorage`。根路径和已知行数保留在 Index 上，创建根 Reader 与 CastTypesOf 不加载 Storage。Resolve 是访问索引的一次操作，返回的路径 Reader 持有具体 Storage 的 pin，覆盖后续多次谓词查询；释放路径 Reader 后该 cell 可以被淘汰。Exists 与字段 null 查询只在本次操作期间持有 pin。

JSON projection 的存在性 bitmap 属于具体标量 Storage，与 postings 或 trie 在同一个 cell 内完成加载、校验和淘汰。JsonPathIndex 只保存路径路由与子 Index，通过子 Index 的 pin 获取 bitmap；不为 sidecar 创建第二个共享 FileSource 的 slot。Marisa、Ngram、Inverted、Sorted 和 Bitmap 均支持这个模式。Bitmap 的 bitset 与 Roaring 使用相同的存在性 bitmap 生命周期，HYBRID 选择的 JSON ARRAY 仍按父行查询。旧索引缺少行数时，路径与类型判断仍不做 I/O，Count 和 Exists 使用请求上下文加载实际 Storage。
