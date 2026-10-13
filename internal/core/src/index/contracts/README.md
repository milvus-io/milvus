# Index contracts

For the cases and boundary matrix of the interface behavior tests, see [TESTING.md](TESTING.md).

This directory defines the query, build, load, and growing publication interfaces of indexes. The
persistence boundary is in [`storage/artifact/`](../../storage/artifact/), and constraints on growing
implementations are in [`index/growing/README.md`](../growing/README.md). `query/`, `build/`, and
`growing/` split files by responsibility only and add no namespace level; the interfaces live in
`milvus::index`. The loader and builder registries are defined in `Registry.h` and `Registry.cpp`
at the root of this directory.

## Lifecycle and ownership

| Object | Responsibility | Ownership and lifetime |
|---|---|---|
| `IIndexReaderBase` + query mixins | Queries an index that is already open | A loader or an Artifact conversion returns a `unique_ptr`; consumers borrow query interfaces and keep the corresponding pin alive |
| `IArtifactBuilder<Input>` | Runs one synchronous build over the complete input materialized by the caller | `Build(input) &&` consumes the builder and returns the finished Artifact; neither the builder nor the Artifact may retain the borrowed input |
| `storage::Artifact` | Holds the result of one build | Exposes `Serialize`; the family defines whether a concrete state can be serialized to a target storage generation |
| `LoaderEntry` | Derives metadata-only caps and opens a reader from persisted data | The registry stores a pair of static functions by value and creates no stateless loader object |
| `IGrowingIndex` + `IAppendable<Batch>` | Accepts incremental input and publishes read records that can be pinned | The Segment is the sole holder of the owner; `GrowingIndexSnapshotPin` retains one published record and its dependencies |

One-shot sealed builds, persisted loads, and growing appends are three independent lifecycles.
Query capabilities are mixins on the reader object; they do not give consumers shared ownership of
the reader.

## Query interfaces (`query/`)

| File | Semantics |
|---|---|
| `IIndexReaderBase.h` | Type-erased base class, coordinate domain, count, value type, and resource self-description |
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
supports; mixins do not inherit the base class. A consumer pins once and then converts to the
required interface; later queries do not depend on the concrete family. A missing capability is
expressed through metadata, an empty resolve result, or an explicit Unsupported; an open failure
must not masquerade as a missing capability.

`ReaderCaps` describes only one entry; the bits of multiple indexes must not be ORed into a reader
that does not exist. The execution path first uses the caps derived from metadata and, after
opening, checks them against `reader.Caps()`. Decisions that depend on query input, such as literal
length, are made by calling the corresponding interface after pinning.

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
  require the parent reader's pin to stay alive. An empty `CastTypesOf(path)` means the shape is not
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
`IReaderConvertible` is an optional consuming capability: `FromArtifact` takes over the Artifact,
checks the capability, and calls `IntoReader() &&`, without implicitly performing serialize/load or
any other IO. A caller that needs both persistence and direct queries must finish persisting before
consuming the Artifact.

`LoaderEntry::Load` opens the persisted source, then calls `LoaderEntry::create` to create the
internal `IndexLoader` and completes `Load`. The loader does not depend on the builder or the
original Artifact. `FileSource` resolves a logical entry into a buffer, a local file, or a
file-backed handle.
`PutMeta`/`GetMeta` preserve JSON types; `LoadOptions::params` are per-load parameters that storage
neither persists nor interprets.

A Builder/Reader does not receive a Segment, an executor, a column cursor, or a
`FileManagerContext`. JSON shredding, column zone maps, element offsets, and segment-level
capability aggregation are not part of the index query contract either. Vector contracts may use
knowhere types; shared/scalar contracts add no knowhere dependency.

`CellByteSize()` describes the heap and file-backed resources owned by the opened reader. An
implementation that uses an all-zero unavailable sentinel or an agreed post-load estimate must state
this in its contract; callers must not treat the sentinel as a measured zero, nor charge it again
for every pin. Pre-load admission estimates such as `LoadOptions::estimated_bytes` and the
ownership accounting after a reader is opened are two independent values.

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
- Each published record solely owns one const reader and atomically fixes `Reader::Count()`,
  `CoveredRowEnd()`, the validity/offset mapping, and the lifetimes of its dependencies. `const`
  does not by itself guarantee that the underlying engine is immutable or that Add/Search are safe
  to run concurrently.
- `PinSnapshot()` pins only an already published record. An empty pin has coverage 0; a non-empty
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
