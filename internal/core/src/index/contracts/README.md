# Index contracts

For the cases and boundary matrix of the interface behavior tests, see [TESTING.md](TESTING.md).

This directory defines the query, build, load, and growing publication interfaces of indexes. The
persistence boundary is in [`storage/artifact/`](../../storage/artifact/), and constraints on growing
implementations are in [Growing publication protocol](#growing-publication-protocol). `query/`, `build/`, and
`growing/` split files by responsibility only and add no namespace level; the interfaces live in
`milvus::index`. The loader and builder registries are defined in `Registry.h` and `Registry.cpp`
at the root of this directory.

For segment registration, lazy initialization, replacement, and query selection, see
[Segment index architecture](../../segcore/indexing/README.md). The
[design rationale](../../../../../docs/design-docs/design_docs/20261102-index-and-request-reader.md)
explains why persistent Index ownership is separate from request Readers; this document defines
the current interfaces and ownership rules.

## Lifecycle and ownership

| Object | Responsibility | Ownership and lifetime |
|---|---|---|
| `IIndex` | Owns one index version, known metadata, and its Storage or Engine | Loads return `IndexPtr`; callers can also construct an Index from concrete Artifact storage or engine extraction; snapshots and request readers retain that version |
| `IIndexReaderBase` + query mixins | Queries through one bound operation context | `IIndex::CreateReader(op_ctx)` returns a `unique_ptr`; consumers borrow interfaces, and concrete operations retain the required storage pins |
| `IArtifactBuilder<Input>` | Runs one synchronous build over the complete input materialized by the caller | `Build(input) &&` consumes the builder and returns the finished Artifact; neither the builder nor the Artifact may retain the borrowed input |
| `storage::Artifact` | Holds the result of one build | Exposes `Serialize`; the family defines whether a concrete state can be serialized to a target storage generation |
| `LoaderEntry` | Derives metadata-only caps and creates an Index from persisted data | The registry stores a pair of static functions by value and creates no stateless loader object |
| `IGrowingIndex` + `IAppendable<Batch>` | Accepts incremental input and publishes read records that can be pinned | The Segment is the sole holder of the owner; `GrowingIndexSnapshotPin` retains one published record and its dependencies |

One-shot sealed builds, persisted loads, and growing appends are three independent lifecycles.
Query capabilities are mixins on the reader object; they do not give consumers shared ownership of
the reader.

## Index and request-reader boundary

`IIndex::Metadata()` returns known caps, coordinate domain, value type, and an optional count without
I/O. Unknown count and zero are distinct. Family-specific load metadata stays in the concrete Index.
`Count()`, `Caps()`, `ValueType()`, and `CoordDomain()` remain on the Reader for the actual query view;
a JSON projection or growing snapshot may describe a narrower view than the stored index.

`IIndex::CreateReader(op_ctx)` creates a separate lightweight reader per request, without opening
files or pinning storage. It requires shared ownership of the Index; the returned reader retains
that exact version. It borrows `op_ctx` until the reader and any deferred operations finish. A null
context denotes an explicit context-free operation. The Index never saves one query's context for
another request. `In()`, `Range()`, `Search()`, and other query methods use the bound context instead
of receiving it again.

```cpp
// Load once; the inventory retains the returned Index.
auto index = LoaderRegistry::Instance()
                 .Lookup(families::kBitmap)
                 .Load(std::move(request));

// Per query, after selecting this integer bitmap index.
auto reader = index->CreateReader(op_ctx);
auto* predicates = dynamic_cast<IScalarPredicateReader<int64_t>*>(reader.get());
AssertInfo(predicates != nullptr, "selected index has no integer predicates");
auto matches = predicates->In(values.size(), values.data());
```

`IIndexReaderBase::Prefetch()` explicitly prepares query storage using the bound context.
It is synchronous; the execution layer schedules it concurrently with other work. Single-cell
readers retain that same storage or engine pin for subsequent operations until destruction.
Already-resident views need no preparation. Loading failures propagate to the caller and a later
access may retry; creating a Reader alone still does not load storage.

`IIndex::CancelWarmup()` requests background prefetch cancellation without invalidating existing
readers or cancelling demand loads. It does not wait for all outstanding I/O to drain. Retirement
calls it after publication of the replacement; waiting for the Index destructor could leave an old
snapshot's warmup running unnecessarily. Without background warmup it is a no-op.

An Index is neither a cache cell nor a CacheSlot. It exposes no query methods, `PinEngine`,
`PinCells`, `MemoryUsage`, or `CellByteSize`. Its concrete implementation decides which storage
cells an operation needs. Keeping an Index alive does not itself pin an evictable cell. Deferred
iterators and borrowed storage views must retain their exact storage pin for their whole lifetime.

## Query interfaces (`query/`)

| File | Semantics |
|---|---|
| `IIndexReaderBase.h` | Type-erased base class, coordinate domain, count, value type, and query capabilities |
| `ReaderCaps.h` | Metadata-only capability description of a single inventory entry |
| `IScalarPredicateReader.h` | Point and range queries; string inputs are `string_view`s borrowed for the duration of the call |
| `INullReader.h` | Null queries, independent of point queries; no separate caps bit |
| `IPatternMatchReader.h` | String pattern matching and the adapter for typed readers; exactness and the per-call cost guard are expressed separately |
| `ITextMatchReader.h` | Tokenized full-text queries; supporting text match does not imply supporting null queries |
| `INgramReader.h` | ngram candidate superset; the consumer reads the raw values and verifies them exactly |
| `ISpatialReader.h` | MBR candidate superset; the consumer verifies the exact relation or distance against the raw geometry |
| `IScalarValueReader.h` | Reverse lookup; `Lookup` returns owned data, and the `Gather` callback may briefly borrow views |
| `IJsonIndexReader.h` | Routes a path/cast to an ordinary reader; defines no new predicate semantics |
| `IVectorReader.h` | Vector search, value retrieval, metadata, nullable mapping, refine, and embedding-list queries |

A concrete reader non-virtually inherits `IIndexReaderBase` and the pure query mixins it actually
supports; mixins do not inherit the base class. A consumer creates a request reader and converts to the
required interface; concrete operations pin storage internally, and callers do not depend on the family. A missing capability is
expressed through metadata, an empty resolve result, or an explicit Unsupported; an open failure
must not masquerade as a missing capability.

`ReaderCaps` describes only one entry; the bits of multiple indexes must not be ORed into a reader
that does not exist. The execution path first uses the caps derived from metadata and, after
opening, checks them against `reader.Caps()`. Decisions that depend on query input, such as literal
length, are made by calling the corresponding interface on the request reader.

Typed readers may inherit `PatternMatchReaderAdapter<Derived, T>` unconditionally, but the primary
template is an empty class; only the `T = std::string_view` specialization inherits
`IPatternMatchReader`. Numeric instantiations therefore do not expose the pattern-match capability,
even when they appear in the same template inheritance list.

## Bitmaps, NULL, and coverage boundaries

- In a scalar query bitmap, 1 means a hit. The bitmap size must equal the reader's `Count()`
  exactly, and coordinates are determined by `CoordDomain()`. This convention does not describe the
  shape of vector search results.
- `INullReader::IsNull()` and `IsNotNull()` answer only for the exact `Count()` and coordinate
  domain of the same reader; the reader does not receive the consumer's active row count and does
  not synthesize validity for rows outside its own domain.
- Predicate results express three-valued logic as `(data, valid)`; `UNKNOWN` is `(0, 0)`. Logical
  `NOT` may flip data only for valid rows and must not turn `UNKNOWN` into a hit.
- A growing reader's `CoveredRowEnd()` is the complete prefix `[0, covered)` in Segment row
  coordinates and is independent of `Reader::Count()`. Null rows count toward coverage; a nested
  reader's element count, the largest completed offset, or the cumulative append count cannot stand
  in for coverage.
- The consumer is responsible for splicing reader results into the query-visible range
  `[0, active)`. For `[covered, active)`, the tail's data and validity can be computed exactly only
  when a semantically equivalent raw evaluator exists and the raw data is readable; otherwise the
  tail must remain `UNKNOWN (data=0, valid=0)`. A candidate operation may temporarily set the tail
  to an all-ones superset, but only on paths that are guaranteed to run exact raw verification on
  those rows afterwards, and the result must not be output directly; validity must never be filled
  with true under any circumstances. Not every index operation has a raw fallback.
- `IS NULL`, `IS NOT NULL`, and an enclosing `NOT` follow the same rule. A row not covered by the
  index is not thereby NULL or non-NULL; without an exact raw null evaluator, uncovered rows remain
  `UNKNOWN`, and they are still `UNKNOWN` after a three-valued `NOT`.

## Coordinates, values, and JSON

- For a nested index, `CoordDomain()` is `Element`, and `Count()` counts elements rather than rows.
  The index holds no column offsets and does not fold element hits into rows; the execution layer
  uses the column offsets to perform the projection and preserves the validity of each nullable
  level.
- `CoordDomain` encodes only `Row`/`Element`, not nested depth. A multi-level projection must
  combine column offsets and validity level by level; if the projection context of any level is
  missing, that path must be explicitly rejected.
- Query semantics determine where the projection happens. Correlated struct predicates are first
  combined at the same element coordinate and then folded to rows; folding them separately would
  wrongly let different elements satisfy the two sides. The uncorrelated
  `contains(1) AND contains(2)` allows different elements to satisfy it, so each side is folded
  first and then combined. `NOT contains(1)` is `not exists i: x[i] == 1`, not
  `exists i: x[i] != 1`.
- Input views are borrowed only for the duration of the call. `owned_t<string_view>` is `string`,
  and every other type is `T`; compressed structures may reconstruct values on the call stack, so
  `Lookup` must not return a dangling view.
- `JsonResolvedReader` may own a temporary reader view or borrow a child reader, but both forms
  require the parent reader to stay alive. An empty `CastTypesOf(path)` means the shape is not
  supported; when it is non-empty and the path exists in no row, `Exists` returns an all-zero
  bitmap.
- `CompareOp`, `PatternOp`, and `SpatialOp` use enums local to the contract; plan or engine enums
  are converted at the boundary. JSON routing uses `JsonCastType`, which does not mean that every
  `DataType` is usable.

## Build, Artifact, and load

`IArtifactBuilder<Input>` is templated on the physical shape of the complete input. After reading a
stable `BuilderInputSpec`, the caller materializes the input once; the builder may traverse it
multiple times within the synchronous `Build`, but must not require a cursor, remote reads, or a
replay protocol.

- `ScalarBuildInput<T>` borrows stable typed batches; values are aligned with logical rows and
  include null rows. An empty validity view means all rows are valid and must not be subscripted.
  The backing storage passed for strings and arrays must stay alive until Build finishes.
- `VectorBuildInput<T>` borrows the complete dense tensor or sparse rows and states the logical row
  count, physical row count, parent validity, optional embedding offsets, and additional scalar
  categories. A valid empty list and a null row must be distinguishable. `T` is the registry/engine
  dispatch tag; the elements of a sparse span are owning `SparseRow`s.
- `PreparedVectorBuildFiles<T>` borrows the complete raw files and optional sidecars. The caller
  keeps the input files stable until Build returns or throws; the Artifact owns its output staging
  independently. `scalar_info_path` distinguishes three states: not delivered, delivered without a
  file, and an actual file path.

Hybrid selects the concrete family within one Build and has that family's builder consume the same
complete input; the caller does not need to replay the input or keep a second complete copy of the
column. The Artifact records the selector; the load side resolves the selector first and then looks
up the concrete loader.

`storage::Artifact::Serialize(FileSink&)` only hands logical named entries or local files to the
sink; the sink is responsible for transport, slicing, naming, and publication metadata, and upload
orchestration belongs to the caller. The existence of the interface does not mean that every
Artifact state supports every storage generation; unsupported combinations must fail explicitly.
Completed text and vector Artifacts expose concrete consuming methods:
`TextIndexArtifact::IntoStorage() &&` and `VectorMemArtifact::IntoEngine() &&`.
Neither method implicitly performs serialize/load or other I/O. A caller that needs both
persistence and direct queries must finish persisting before consuming the Artifact.

`LoaderEntry::Load` opens the persisted source, then calls `LoaderEntry::create` to create the
internal `IndexLoader`. After asynchronous preparation completes, its synchronous `Load`
constructs the Index and cache slot, returning an `IndexPtr`; synchronous warmup may load storage
before returning. Every persisted load
requires an explicit `IndexCacheConfig`; the Index owns the resulting Storage/Engine cache slot.
The loader consumes its prepared source once, including when construction fails. Recreate the
loader to retry construction; a successfully constructed slot retains its source for later loads
and retries. Warmup controls when storage is loaded, not whether a cache slot is used.
Growing publication and concrete Artifact storage/engine extraction can still create resident Index objects without
using the persisted-load interface. Callers create a
request reader with `index->CreateReader(op_ctx)`. The loader does not depend on the builder or the
original Artifact. `FileSource` resolves a logical entry into a buffer, a local file, or a
file-backed handle.
`PutMeta`/`GetMeta` preserve JSON types; `LoadOptions::params` are per-load parameters that storage
neither persists nor interprets.

A Builder/Reader does not receive a Segment, an executor, a column cursor, or a
`FileManagerContext`. JSON shredding, column zone maps, element offsets, and segment-level
capability aggregation are not part of the index query contract either. Vector contracts may use
knowhere types; shared/scalar contracts add no knowhere dependency.

## Storage access and resource accounting

Scalar Index implementations own their concrete Storage types directly or through typed cache
slots. Bitmap uses `BitmapPostingStorage`; no additional engine wrapper is needed. Vector Index
implementations own a resident `KnowhereEngine` or its cache slot. Native lazy-loading backends
manage their own cache without an outer evictable engine slot; see [Vector indexes](../vector/README.md).

`SingleCellIndexStorageTranslator` is an optional adapter for loading a whole Storage or Engine as
one cache cell. It is not a mandatory layer. An implementation with its own cells or slices can use
those directly without changing the query contracts. This migration does not implement block loading
or make synchronous query methods asynchronous.

Current single-cell readers acquire storage or the engine on the first operation that needs it
and retain that pin until Reader destruction. All operations reuse the same cell, including
predicates, null checks, `Lookup`, and `Gather`. Initialization is synchronized and failures can
be retried. Reader creation and metadata already available from the Index remain free of storage
I/O. A used Reader prevents eviction even between operations; consumers should release it when
finished. This is a concrete single-cell policy, not a requirement to load every cell of future
multi-cell indexes.
JSON path indexes retain child Index objects and create child request readers with the same context.
Projection existence metadata is loaded with the concrete scalar Storage. JSON flat resolution
returns a path reader retaining its Storage pin across subsequent predicates; callers still obey
the parent-reader lifetime contract above.

`CellByteSize()` belongs to the cached Storage or Engine, not to `IIndex` or its request Readers.
It describes owned heap and file-backed resources. Implementations using an unavailable-zero
sentinel must document it; CacheSlot uses the translator's estimate for such cells. These resources
must not be charged again for every request reader. Admission estimates remain separate from
post-load accounting. `IndexCacheConfig` preserves resident, private temporary, and shared temporary
resource estimates separately, together with the shared-overhead policy from load planning.

Known metadata may avoid I/O; missing legacy metadata may require a load during initialization or
Reader `Count()`. No scalar index version or new file format is required solely for the object split.

## Registry and extension rules

- `LoaderRegistry` stores the static function pair `{derive_caps, create}` keyed by family.
  `derive_caps` reads only the load metadata and does not open the payload; an unknown family
  returns an empty entry.
- `BuilderRegistry<Input>` keeps factories separate per complete input type. A factory parses its
  own typed parameters immediately; the registry neither interprets family parameters nor erases
  the input shape. An unknown family, or one that does not support the input shape, yields a null
  pointer.
- A new family registers its builder/loader in its own implementation translation unit and ensures
  that this translation unit is included in the final link output. The caps derived by the loader
  must match `Reader::Caps()` after opening.
- A new query capability should be a separate mixin; only capabilities that path selection must
  recognize before opening are added to `ReaderCaps`. The implementation must also declare whether
  its result is exact or a candidate superset, and which layer performs exact verification.
- An Artifact's serializable generations, direct-conversion capability, and failure semantics are
  defined explicitly by the concrete state; they are not inferred from the family name or filled in
  through hidden IO.

## Growing publication protocol

- `IAppendable<Batch>` is a separate input mixin. `ScalarBatch<T>`, `TextBatch`, and
  `VectorBatch<T>` are flat views borrowed for the duration of the call; they must be fully consumed
  or copied before `Append` returns. Nested input requires explicit offsets/validity.
- A successful Append means the input was accepted, not necessarily that it was published. The
  write side is responsible for serializing append/commit or satisfying the family's concurrency
  protocol; query correctness depends only on published records and coverage, not on a concrete
  family's commit cadence.
- Each published record retains one Index version and atomically fixes `Reader::Count()`,
  `CoveredRowEnd()`, the validity/offset mapping, and the lifetimes of its dependencies. `const`
  does not by itself guarantee that the underlying engine is immutable or that Add/Search are safe
  to run concurrently.
- `PinSnapshot(op_ctx)` retains an already published record and creates its request reader
  outside the publication lock. Separate calls create separate readers; copies of one pin share
  that request reader. The context must outlive the pin and its copies. An empty pin has coverage 0; a non-empty
  pin can be copied or moved and is empty after being moved from. Reader references and
  query-interface pointers must not outlive the pin. The owner must still be protected while the pin
  is acquired; once acquired, the pin can live independently.
- Coverage is monotonically non-decreasing and cannot cross a hole in row numbers. A failure while
  constructing, allocating, or validating a publication must not replace the current record; the old
  record is released outside the publication lock, and existing pins keep their reader and
  dependencies.
- `CommitIfNeeded()` gives an interval writer a commit point before a query; errors propagate
  unchanged and must not be masked by an old pin. `Flush()` is a strong boundary: when it returns
  successfully for an owner that has already been built, every accepted row must be in a published
  record; an owner that has not yet reached the family's build threshold may keep an empty pin.
- For the first cold build of a Knowhere vector index, an empty pin may be kept only if no engine
  has ever been published and the complete raw source can still be queried and retried. Once the
  engine is built, an Add failure must propagate and the failed input must not be published; if Add
  succeeded but publication failed, a retry may only publish the accepted state and must not Add
  again.
- A published record of an immutable engine fixes the engine view. A record of a shared, single-live
  Add/Search engine may observe later ANN state, but each pin's logical/physical prefix, Count,
  coverage, mapping, and dependencies must still be fixed, and the engine is responsible for
  concurrency safety.
- Growing vector ANN may be used only when the snapshot coverage includes the complete
  query-visible row prefix; otherwise the whole query takes the raw vector fallback, and an ANN
  prefix must not be spliced with an uncovered tail. When ANN is used, search, iterator, value
  lookup, and raw refine are all limited to the same query-visible prefix and hold the same pin.
- Growing scalar/text/spatial consumers merge the covered prefix and the uncovered tail as described
  in the "Bitmaps, NULL, and coverage boundaries" section. When the corresponding raw evaluator is
  missing, they fail closed to `UNKNOWN`; they must not assume that every capability has a raw
  fallback.
- Growing initialization, append, commit, and publication do not consume an Artifact; published
  records are managed by `IGrowingIndex`.
