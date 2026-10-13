# Vector indexes

This directory splits vector index objects by query, build, artifact, and load. See
[`contracts/README.md`](../contracts/README.md) for the shared contracts and
[`growing/README.md`](../growing/README.md) for incremental writes. Splitting the responsibilities
does not by itself require changing knowhere algorithms or persisted formats; this does not mean
that implementations not yet wired in have completed behavior or performance verification.

## Files and responsibilities

| File group | Responsibility |
|---|---|
| `KnowhereEngine` | Holds the native engine, the complete backing owner, and the actual type, metric, dim, and physical/embedding-list state |
| `VectorIndexValidDataUtils` | Encodes and decodes the nullable row validity bitmap and publishes it into the knowhere IdMap (#50524; the mapping itself is owned by knowhere) |
| `contracts/query/IVectorReader.h` | Unified vector query interface covering search, value retrieval, metadata, nullable, refine, and embedding-list operations |
| `VectorIndexReader` | Non-template unified reader; at runtime it distinguishes only the memory/disk search shell and the DiskANN beamwidth, and dispatches on the physical type only at the retrieval leaf |
| `VectorMemBuilder` | `IArtifactBuilder<VectorBuildInput<T>>`; accepts the complete input in one call, and the caller owns the tensor and side inputs |
| `VectorDiskBuilder` | `IArtifactBuilder<PreparedVectorBuildFiles<T>>`; knowhere reads the prepared complete files |
| `VectorMemArtifact` | Named logical BinarySet entries; supports consuming the built engine/validity to produce a reader directly |
| `VectorDiskArtifact` | Serializes large files by path only; queries must open the persisted artifact through `VectorDiskLoader` |
| `VectorMemLoader` | Two open modes: materialize and mmap |
| `VectorDiskLoader` | Disk index loading and integration with streaming backends |
| `VectorLoadUtils` / `VectorParamUtils` | Shared integer syntax parsing; does not merge the separate mem/disk policies for missing values, aliases, and types |
| `VectorReaderUtils` | Shared dense and embedding-list retrieval flow; holds no engine or reader |
| `storage::LocalDirectory` | Directly owns the mmap or disk subdirectory newly created by the loader/builder; does not own the configured parent |
| `RangeSearchParams` | Vector-specific range-search parameter preparation; isolates the dependency on shared scalar helpers |
| `VectorFamilies` | Registers the stateless loaders and the wired typed builders |

## Query and lifecycle

`VectorIndexReader` inherits both `IIndexReaderBase` and the query-only mixin `IVectorReader`, and
holds the engine/validity by value.
Inside `KnowhereEngine`, the backing owner is declared before the native handle, so the native node
is destroyed before the final owner of the mmap files or the FileManager generation.
Search receives only the parameters, metric, topk, and trace from `VectorSearchParams`; visibility
filtering, logical/physical coordinate translation, element-to-row aggregation, and result
processing are done by the consumer.

`IVectorReader` also provides the metric/dim/knowhere type and iterator parameter preparation,
borrowed nullable offsets, distance recomputation, and embedding-list retrieval. A unified interface
does not mean that every backend or physical type supports every operation; runtime checks and the
existing Unsupported paths remain the actual capability boundary.
`CoordDomain` stays Row: element-level search on VECTOR_ARRAY is a per-query mode. The consumer
converts the element IDs returned by knowhere into `(row, element)` using that query's array
offsets, and the reader's inventory coordinates are not permanently changed to Element.

A Growing reader fixes the physical Count, the nullable mapping, and the default search parameters
when it is published. Before calling knowhere, Search/Range use the length of a non-empty bitset to
limit the visible physical prefix; a shorter query-visible prefix is still supplied by the consumer.
The shared live engine allows Add to change the traversal of approximate search, but an older reader
never returns IDs beyond its fixed prefix. This is a logical prefix pin and must not be described as
a physically immutable snapshot of the underlying ANN engine.

The knowhere iterators returned by `Iterators` carry no reader pin and borrow the bitset passed in;
a merge iterator consumed later may also hold a raw pointer to the reader's offset mapping. A Growing
consumer must keep the same `GrowingIndexSnapshotPin` and the materialized prefix bitset alive
together with the results until iterator consumption finishes; the iterator's shared handle alone is
not enough to conclude that the lifetime is safe.

## Persistence and IO

- The in-memory builder receives the complete tensor, parent validity, optional embedding offsets,
  and scalar category groups. The caller pre-checks against `InputSpec().side_inputs` and delivers
  the actual data; for production input the materializer owns the compact tensor, and the builder
  borrows the input during the synchronous build without keeping another raw buffer of its own.
- The caller keeps DiskANN's complete input files until the synchronous Build finishes; the output
  staging is owned separately. An Artifact/Reader that lives on afterwards does not borrow the input
  files or the validity metadata.
- The in-memory artifact writes logical entries; slicing, assembly, and transport naming belong to
  the source/sink. The packed V3 vector format is not implemented at present.
- An mmap load streams the ordered engine entries and concatenates them into a local file.
  Embedding-list sidecars stay separate files, and validity/empty-list metadata is parsed separately.
  The FileSource is borrowed only while opening; the reader must retain its backing files.
- Large DiskANN files are streamed by path; only a V1/V2 DiskFiles index source can create a
  disk-engine handle. The loader first probes capabilities with a LocalFiles handle, and a
  stream-load backend then switches to a RemoteStreams handle restricted to the engine inventory;
  the reader retains the finally selected handle together with its FileManager and local generation.
- `IArtifactBuilder` returns the finished Artifact from `Build(input)`; the Artifact is responsible
  for serialization, and the build service orchestrates the upload and `FileSink::Finish()`. Only
  `VectorMemArtifact` provides a one-shot consuming conversion, in which the engine and validity are
  moved into an independent reader; `VectorDiskArtifact` does not provide this capability and must
  first be serialized and then opened by `VectorDiskLoader`.
  Local staging is cleaned up by its actual owner, which must not delete files still in use by a
  reader.

## Known incomplete items

- Complete input and typed builder registration pass the `index_tests` build and behavior tests in
  the default configuration; HNSW scalar side inputs are covered by real build/load, and DiskANN
  scalar sidecars are skipped according to backend capability.
  Performance has not been verified. The currently unsupported combinations (multiple extra fields,
  and VECTOR_ARRAY with extra scalar fields) are still rejected.
- `ReaderCaps` lacks vector raw-value/refine capability bits; the default-false query bits and
  exact=true cannot replace a complete capability check.
- `IVectorReader` provides both dense and sparse getters, and sparse retrieval on the disk backend
  still returns Unsupported.
- The reader's `MemoryUsage` and `CellByteSize` return an explicitly recorded unavailable-zero
  sentinel; this does not mean the knowhere resident/file footprint was measured as zero.
  `LoadOptions::estimated_bytes` and `VectorLoadResource`/`IndexLoadResource` remain responsible for
  pre-load admission; the existing estimates and safety factors of the live sealed translator and
  the growing segment are unchanged. Splitting native memory/file usage exactly by backend is a
  follow-up and does not block verification of the current production wiring.
- The Growing owner's initial Build, subsequent Add, and publication have `index_tests` behavior
  coverage; consumer routing still has to be judged together with the `all_tests` results. A
  publication record fixes the reader Count, mapping, and CoveredRowEnd; it shares the live engine
  with later Adds, so it guarantees only the logical prefix described above and does not guarantee
  that approximate ANN traversal results are a physically stable snapshot.

The old VectorIndex/VectorMemIndex/VectorDiskIndex files referenced in migration TODOs are in the
historical commit `e255009e01` and can be viewed with
`git show e255009e01:internal/core/src/index/<file>`; the local notes above do not depend on the
migration design document.
