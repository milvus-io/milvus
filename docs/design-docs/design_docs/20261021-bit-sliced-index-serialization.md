# MEP: Serialize and reload the bit-sliced index

- **Created:** 2026-10-21
- **Author(s):** @IceCoooola
- **Status:** Under Review
- **Component:** Index (segcore, `internal/core/src/index/scalar/bsi/`)
- **Related Issues:** #53832 (this stage); #53831 (the kernel, previous stage)
- **Released:** N/A

## Summary

Persist the Int32 bit-sliced index (BSI) kernel from #53831 and load it back
without changing its comparison semantics. The kernel's state is written as
named byte entries (a header, the existence bitmap, one entry per slice)
through the index-file envelopes the other scalar families already use:
`storage::FileSink` for the V1/V2 layout and `storage::IndexEntryWriter` for
the packed V3 layout. Loading decodes the format version first, reads the
rest of the header, and checks every payload size against it before any
bitmap is allocated, then rebuilds the kernel through a validating factory.
Round trips preserve exact results; truncated data, inconsistent headers and
unsupported versions fail with a clear error.

## Scope

In scope:

- `BitSlicedIndexArtifact<T>`, a `storage::Artifact` that owns a built
  `BitSlicedIndex<T>` and writes it to either envelope.
- `LoadBitSlicedIndex<T>`, which reads the index back from a
  `storage::FileSource` or a `storage::IndexEntryReader`.
- `BitSlicedIndex<T>::State` and `BitSlicedIndex<T>::FromState`, the
  kernel-side description of its persisted state and the factory that
  validates it.
- Format version 1, for `int32_t` only.
- Unit tests for round trips through both envelopes and for every rejection.

Out of scope (later stages, see Future Work):

- A builder in `BuilderRegistry`, a loader in `LoaderRegistry`, an index type
  or any user-visible way to create the index. The index-file envelope is
  reused as the issue asks; no new persistence protocol is introduced.
- `IIndexReaderBase`, caps, mmap-backed loading, and the other integer widths.

## Motivation

Issue #53832 asks for the agreed BSI representation to be persisted so that a
built index can be reloaded, with exact results preserved and corrupt or
incompatible data rejected clearly. Without a persisted form the kernel from
#53831 can only live for the lifetime of the process that built it, which
rules out the sealed-segment build/upload/load lifecycle every other scalar
index follows.

## Public Interfaces

None that users or other components can observe. This PR changes no API,
proto, SDK, configuration, metric or query behaviour.

Internally it adds, in `internal/core/src/index/scalar/bsi/`:

- **`BitSlicedIndexArtifact<T>`** (`BitSlicedIndexArtifact.h`), a
  `storage::Artifact` constructed from a `BitSlicedIndex<T>`. It implements
  both `Serialize` overloads. `Index()` borrows the owned index, so a caller
  that needs to query after persisting can do so without a reload.
- **`LoadBitSlicedIndex<T>(storage::FileSource&)`** and
  **`LoadBitSlicedIndex<T>(storage::IndexEntryReader&)`**, returning a
  `BitSlicedIndex<T>`.
- **`bsi_format`**, the entry names, version and value-kind constants, and
  the `Header` struct of the layout.
- On the kernel, **`BitSlicedIndex<T>::State`**, **`FromState(State)`**,
  **`Existence()`** and **`Slice(i)`**. `FromState` is the only way to build
  an index from parts, and it rejects parts that break the encoding
  invariants.

Only `T = int32_t` is instantiated, as for the kernel.

## Design Details

### Persisted state

The kernel's state is `(num_rows, min, max, existence, slices[0..k))`, with
`k = bit_width(max - min)`. The valid count is not stored: it is the popcount
of the existence bitmap. `max` is stored because `k` only bounds it. When
there are no valid rows, `min = max = 0` and `k = 0`, which is what the
constructor produces for that input, so an empty or all-NULL index round
trips to an identical state.

### Layout (version 1)

`2 + k` named entries. The names are the same in both envelopes; only the
envelope differs.

| Entry           | Content                                                                                    |
|-----------------|--------------------------------------------------------------------------------------------|
| `bsi_header`    | `bsi_format::Header`, 40 bytes: version, value kind, value width in bits, slice count, row count, min, max |
| `bsi_existence` | The existence bitmap as `ceil(num_rows / 64)` 64-bit words                                 |
| `bsi_slice_NN`  | Slice `NN` (two decimal digits, `00 <= NN < k`), the same size as the existence bitmap     |

The header is a trivially copyable struct with explicit widths, a
`static_assert` on its size and one on the version being its first field, so
a layout change cannot go unnoticed and a reader of any version can always
identify the version of what it was handed. Bitmap words are the kernel's
in-memory words (row `r` is bit `r % 64` of word `r / 64`, bits past
`num_rows` in the last word are 0), so each bitmap entry is handed to the
sink as it is and loading needs no bit-level transcoding. Integers and words
are in host byte order, like the numeric keys of the bitmap family; Milvus
runs on little-endian hosts only.

The value kind (`kSignedInteger`) and width are recorded so that an artifact
written for one instantiation is never decoded by another, and so that a
future kind (unsigned, for example) cannot be mistaken for a signed one of
the same width. `int8_t` and `int16_t` would record 8 and 16 and, once
instantiated, load through the same code.

### Serialization

`BitSlicedIndexArtifact<T>::Serialize` is written once as a template over
the output type; both `storage::FileSink` and `storage::IndexEntryWriter`
accept `WriteEntry(name, data, size)`. Every bitmap is written from its own
words, so serialization allocates nothing beyond the 40-byte header. For the
V3 writer no `PutMeta` is used: keeping the header in an entry gives one
decoder for both envelopes.

### Bounded loading

Loading is written once over a thin adapter for each readable envelope
(`storage::FileSource` and `storage::IndexEntryReader`), which exposes
`Has`, `Size` and `Read` for an entry. The order is:

1. Read `bsi_header`. It must be present and hold at least the 4-byte
   version. An unknown `version` is rejected with `Unsupported` *before* the
   header's size is checked, so a later version with a header of another size
   is still reported as another version rather than as corruption.
2. Require exactly 40 bytes for version 1, else `DataFormatBroken`.
3. Reject a `value_kind` other than `kSignedInteger` or a `value_bits` other
   than `8 * sizeof(T)` with `Unsupported`.
4. Reject more than `kMaxSlices` slices or bounds outside the range of `T`
   with `DataFormatBroken`.
5. Reject any `bsi_slice_` entry whose name is not one of the `slice_count`
   declared slices with `DataFormatBroken`; the header and the payload must
   agree. Entries with other names belong to the envelope (V3 keeps its own
   metadata entry) and are ignored.
6. Compute the expected size of every bitmap entry (`words * 8`; `words` is
   computed without overflow near the top of `uint64_t`).
7. For `bsi_existence` and then each `bsi_slice_NN`, check that the entry
   exists and that the source reports exactly the expected size *before*
   reading it, and that the read returns exactly that many bytes. A
   truncated or inflated entry is rejected here, and the source never
   materializes more than the header accounts for.
8. Copy the words into a `TargetBitmap` and reject any set bit past the last
   row (the kernel keeps those bits clear and `count()` relies on it). The
   entry's bytes are released before the next entry is read.
9. Call `BitSlicedIndex<T>::FromState`.

Peak memory is the index plus either one bitmap entry in transit (while
reading) or a few bitmap-sized temporaries (the OR accumulator, the cloned
existence bitmap and the walk's working bitmap, while `FromState`
validates); the two phases do not overlap. A negative size from a `FileSource` is an internal assertion, not
`DataFormatBroken`: both `FileSource::EntrySize` implementations report
failures by throwing and never return a negative value, so a negative value
can only be a source bug.

### Kernel-side validation (`FromState`)

`FromState` owns the invariants of the encoding, independently of any
envelope, and throws `DataFormatBroken` when one fails:

- Every bitmap has `num_rows` bits.
- With no valid rows, `min = max = 0` and there are no slices: exactly the
  state the constructor produces for that input, so the encoding is unique.
- Otherwise `min <= max`, and `slices.size() == bit_width(max - min)`.
- The union of the slices is a subset of the existence bitmap (NULL rows are
  0 in every slice). One OR accumulator, so this allocates one bitmap for
  all `k` slices.
- No valid row encodes an offset above `max - min`. The `<=` slice walk with
  `max - min` as its constant selects exactly the rows that satisfy this.
- Some row holds `min` (offset 0) and some row holds `max` (offset
  `max - min`), checked with the equality walk. The writer always emits tight
  bounds, so looser bounds can only come from corruption, and without this
  check they would load and make `Min()`/`Max()` lie.

Together these cost about `5k` bitmap passes plus the OR pass, once per
load, and reuse the query walks. They go beyond what the issue requires, but
without them a corrupt artifact with correct sizes could load into an index
whose `Min()`/`Max()` or `NotIn` results are wrong, which would surface as a
query bug rather than a load failure.

### Errors

| Condition                                                                 | Error              |
|---------------------------------------------------------------------------|--------------------|
| Unknown format version (whatever the header's size); value kind or width of another instantiation | `Unsupported`      |
| Header too short to hold a version, or of the wrong size for its version; missing, truncated or inflated entry; too many slices; bounds outside `T`; bits past the last row; any `FromState` invariant | `DataFormatBroken` |

This follows the convention of the other scalar loaders (`Unsupported` for a
version mismatch, `DataFormatBroken` for corrupt data). Both are system
errors: no request content reaches this code.

## Compatibility, Deprecation, and Migration Plan

Nothing changes in existing persisted formats, public interfaces,
configuration or query behaviour. No artifact of this layout exists yet, so
version 1 has no predecessor to migrate from. A future layout change bumps
`bsi_format::kVersion`; version 1 readers reject it with `Unsupported`
whatever the size of the new header, because the version is the first field
and is decoded before the size is checked, and a version 2 reader decides
whether to keep reading version 1.

## Test Plan

Unit tests in `BitSlicedIndexArtifactTest.cpp`, part of `index_tests`.

Round trips, through the V1/V2 test sink and source and through the real
packed V3 envelope (`IndexEntryDirectStreamWriter` and `IndexEntryReader`,
including the directory and CRCs), over datasets that cover every shape of
the state: no rows; all rows NULL; a single distinct value (0 slices); the
type extremes (32 slices); a narrow range with NULLs; exactly one word of
rows; one word plus one row; and random narrow and full-range columns with
NULLs. For each, the loaded index must report the same counts, slice count,
bounds and memory usage, and answer the same for every `CompareOp` at every
boundary constant, every two-sided range between those constants, `In` and
`NotIn` over them, and `IsNull`/`IsNotNull`.

The layout is pinned by a test that decodes the header and bitmap words of a
known 65-row index, so an unintended format change fails the test.

Rejections, each checked for its error code: a missing entry (each of the
five of the sample); an entry one byte short or one byte long (each of the
five); an unknown version with the current header size, with a larger
header, and a header too short to hold a version; a 64-bit value width; an
unknown value kind; 33 slices; bounds outside `int32_t`; `min > max`; one
slice too many with an entry to match; a well-formed slice entry the header
does not declare; `max` looser than any row reaches
while the slice count still fits; a row count larger than the payload; with
no valid rows, slices present or non-zero bounds (and the empty state itself
loading); a slice bit on a NULL row; a row whose slice bits encode an offset
above `max - min`; a set bit past the last row in the existence bitmap and
in a slice. `FromState` is also tested directly for mismatched bitmap sizes,
for `min` and for `max` held by no row, and for a valid hand-built state.

## Rejected Alternatives

- **YAML (V1/V2) or `PutMeta` (V3) metadata, as the bitmap family does.**
  This would need two decoders. A header entry gives one layout and one
  decoder for both envelopes, and the fixed struct makes the bounds check a
  size comparison.
- **One `bsi_slices` entry holding every slice.** Fewer entries, but
  Serialize would have to copy all slices into one buffer (another full
  index worth of memory at build time) and the loader would hold the whole
  entry while decoding it. One entry per slice makes both directions
  copy-free per slice at the cost of up to 34 entries, which the envelopes
  already handle for other families.
- **Checking the header's size before its version.** A later version with a
  larger header would then be reported as corruption. Decoding the version
  first, from the fixed first field, keeps "unsupported version" a clear
  failure for every future layout.
- **Storing the valid count.** It is the popcount of the existence bitmap;
  storing it adds a field that can only disagree.
- **Masking bits past the last row instead of rejecting them.** The kernel
  never writes them, so their presence means the artifact was not written by
  this code; masking would hide that.
- **Skipping the subset and offset checks in `FromState`.** See
  "Kernel-side validation": they are cheap and turn a query-time wrong
  answer into a load-time error.
- **A per-slice subset check with a clone per slice.** Correct, but `k`
  allocations and copies; one OR accumulator gives the same answer with one.
- **Reconstructing the index through its build constructor from decoded
  values.** This would decode every row and rebuild the slices; copying the
  words is a `memcpy` per bitmap and keeps the state exactly as written.
- **Registering a loader and builder now.** Out of scope for this issue,
  which reuses the existing envelope and explicitly excludes a new
  persistence protocol; the registry entry is the next stage.

## References

- Issue #53832: serialize and reload the bit-sliced index (this stage).
- Issue #53831 and
  [`20261013-int32-bit-sliced-index-kernel.md`](20261013-int32-bit-sliced-index-kernel.md):
  the kernel and its encoding.
- `internal/core/src/index/contracts/README.md`: the build, Artifact and
  load contracts.
- `internal/core/src/storage/artifact/FileSink.h`, `FileSource.h`,
  `internal/core/src/storage/IndexEntryWriter.h`, `IndexEntryReader.h`: the
  envelopes.
- `internal/core/src/index/scalar/bitmap/BitmapIndexArtifact.cpp`: the
  artifact this one follows.

## Future Work

- Reader and lifecycle integration: `IIndexReaderBase`, caps, a builder in
  `BuilderRegistry<ScalarBuildInput<T>>`, a loader in `LoaderRegistry`, and
  the index type that lets users create the index.
- Loading with file-backed bitmaps (mmap) instead of heap copies.
- The remaining integer widths, which only need instantiation and tests.
