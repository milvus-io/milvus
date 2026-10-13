# Scalar indexes

This directory implements the sealed scalar index families. `IArtifactBuilder`, Artifact, Loader and
Reader are responsible for building from the complete input, holding and serializing the build
result, opening persisted artifacts, and querying, respectively. Shared code is reused through
stateless helpers and family-internal templates; there is no stateful builder/reader base class
across families.

## Build, persistence and Reader

A sealed `IArtifactBuilder` accepts the complete `ScalarBuildInput<T>` in one call and returns a
completed Artifact. `ScalarBuildInput<T>` borrows stable typed batches: values are aligned with
logical rows and include null rows, an empty validity view means all rows are valid, and the backing
storage of variable-length values such as strings must stay alive until `Build` returns. The builder
may iterate the input repeatedly within the synchronous call, but neither the builder nor the
Artifact may retain the borrowed input.

The Artifact owns the build result and emits it through `Serialize`; the Loader opens a Reader
independently from the persisted artifact; the Reader owns the engine, mappings, backing files and
accounting state that queries need. `hybrid/` probes cardinality within the same `Build`, selects a
concrete family by field shape, nested state, version and the low/high-cardinality configuration,
and then hands the same complete input to that family; the Hybrid Artifact records the selector. The
platform AUTOINDEX configuration is not part of the Hybrid policy here.

`TextIndexArtifact` implements the optional consuming Reader conversion, handing the completed
Tantivy engine, the null state and the directory owner, when needed, to `TextIndexReader`. The
conversion consumes the Artifact; Hybrid and JSON projected Artifacts only wrap the serialized state
of the selection or projection and do not pass this capability through.

Numeric and string readers of the same family may share a query flow templated on the value type,
and all of them inherit `PatternMatchReaderAdapter<Derived, T>`. The adapter's primary template is
empty; only the `T = std::string_view` specialization inherits `IPatternMatchReader`, so numeric
instances do not expose that interface, and `Caps()` must match the actual inheritance. Bitmap
attaches the adapter at its final concrete reader; Inverted and Sorted attach it at their own typed
readers.

Sorted uses a single `SortedIndexReader<T>`; the layout, search and accounting of numeric pairs and
of string dictionary/postings are each encapsulated in a type-specific storage view. Bitmap posting
keys use `owned_t<T>`, and string lookups use transparent comparison; `Lookup` returns owned values,
and the `string_view` from `Gather` is valid only during the synchronous callback.

## V3 loading

V3 Artifacts write directly to `IndexEntryWriter`. Reads use `IndexEntryReader` or
`AsyncIndexEntryReader` directly; `FileSink` / `FileSource` handle only legacy files.
Each Loader's `PlanPacked` validates the directory and metadata and assigns the final memory/file
targets, and `FinishPacked` creates the Reader from the completed targets. The synchronous and
asynchronous entry points share these two steps. `PackedIndexLoad` handles reads, CRC, draining
after cancellation, target commit and failure cleanup; asynchronous file preparation and file-backed
engine initialization run on LocalFileIOPool. Before file targets are committed, the plan is
responsible for cleaning them up; after success, the Reader's mapping/directory owner keeps them
alive. Initialization without file targets stays on the async executor.

## Family boundaries

- Marisa, FM, Text, Ngram and RTree own their trie, FM, full-text, candidate and spatial algorithm
  objects, respectively, and do not share a stateful concrete Reader base class.
- NGRAM's `NgramIndexBuilder<T>` accepts two kinds of complete input: scalar `string_view` and
  `JsonProjectedString`. The two instantiations share the writer core; scalar validity and the JSON
  field-null/missing/value tri-state are handled separately, and each has its own registry entry.
- A JSON projected Artifact wraps a plain scalar Artifact and serializes the path, cast, row count
  and non-exist state; after loading, `JsonPathIndexReader` routes to the inner typed Reader, and the
  outer layer does not directly expose the inner predicate/pattern/ngram mixins.
- JsonFlat's root and path Readers share immutable field state; the bool, numeric and string path
  Readers implement boolean ranges, numeric bounds across int64/double, and string
  ownership/pattern routing, respectively.

## Parameter, storage and resource constraints

- `../ParamUtils.h` reads the nested aliases in the order `nested`, `is_nested`, `is_nested_index`
  and rejects values that conflict with each other. The schema/boundary value is authoritative and
  is written to the canonical runtime keys before entering a Loader that requires normalized
  parameters; the Loader treats a missing internal parameter as a contract violation.
- The parameter helpers handle only aliases, consistency and basic type decoding. Supported types,
  required/default values, explicit null semantics and field/element/value type relationships are
  defined by each family. Boolean parameters always use `GetValueFromConfig<bool>`.
- Bitmap's STRING/VARCHAR check does not include TEXT. Sorted reads exactly the declared file
  length, and a premature EOF is an error; Bitmap reports posting and file size overflow at its own
  format boundary.
- Each Tantivy family defines its own reserved file names, sidecar set and per-storage-generation
  validation. The shared file enumeration explicitly distinguishes regular files from all
  non-directory entries; JsonFlat performs its own file-set and sidecar-conflict validation.
- Explicit close/unlink failures on the normal path must be reported; cleanup during destruction and
  exception unwinding is best-effort. A Reader must keep file mappings, the directory owner and
  other backing resources alive for the full lifetime of its query objects, and destroy them in
  reverse dependency order.

## Reading order

1. `../ParamUtils.h`: parameter aliases, consistency checks and normalized integer `DataType`
   decoding.
2. The family's Params or Builder/Loader: supported types, defaults, null semantics and
   field/element/value type relationships.
3. `ScalarIndexUtils.h`: C++ scalar type mapping, type matching, string assignment and validity
   bitmap construction.
4. `../../storage/artifact/FileSourceUtils.h`, `LocalFileUtils.h`: file names, file enumeration,
   handles and temporary file lifetime.
5. The family's Builder/Artifact/Loader/Reader: input validation, algorithm state, sidecars,
   serialization, opening and querying.
