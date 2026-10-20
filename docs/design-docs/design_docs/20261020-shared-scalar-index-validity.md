# Borrow scalar-index validity through BitsetView

- Created: 2026-10-20
- Component: QueryNode scalar indexes and expression execution
- Tracking: [Milvus #54434](https://github.com/milvus-io/milvus/issues/54434)
- Master PR: [Milvus #54437](https://github.com/milvus-io/milvus/pull/54437)
- Branch: 2.6, based on `78388056176a20cec3e00aaadb59d34516e761c6`

## Problem and scope

Batched indexed filters currently retain a full hit bitmap and an owned
validity bitmap for each evaluated leaf. Sealed validity is identical across
leaves using the same index. Repeated owned copies can retain approximately
`leaves * ceil(rows / 8)` bytes even though the index already owns the mask.
This change removes those redundant caches without changing predicate
compilation, persisted formats, or the writable `IsNotNull()` API.

## Reuse the existing view and pin

The optional `GetValidityView()` capability returns the existing
`milvus::BitsetView` from `common/BitsetView.h`. It does not introduce an index
specific bitmap wrapper, an owning snapshot, a new shared control block, or
copy-on-write behavior.

The representation is explicit:

- `nullopt`: unsupported; use the original owned `IsNotNull()` result.
- An engaged view with `size() == 0`: all-valid, with logical coordinate count
  taken separately from the pinned index's stable `Count()`.
- An engaged nonempty view: dense, unmapped validity with `size() == Count()`.
  Its data pointer is const and its ID offset is zero.

`empty()` is deliberately not the all-valid test: Knowhere also uses it for
certain nonempty all-allowed filters. In particular, an all-zero dense validity
bitmap must continue to mean all-null. The Knowhere `v2.6.22` pointer/length
constructor only stores its fields; it does not scan or count the bitmap.

A borrowed view is valid only while the selected index remains pinned. Existing
`SegmentExpr::pinned_index_` retains the cache accessor across evaluation
batches. Sealed `PinIndex()` places the accessor in `PinWrapper` together with
the index pointer. Execution-path selection completes before a view is read;
paths that release the pin and switch to raw data never consume that index
view. The view is not stored in expression-result caches or returned as
writable output. It must not be used after the pin is released.

For unsupported/growing indexes, the expression retains its old owned bitmap
and borrows a view from that owner. Its coordinate count comes from the actual
owned bitmap size, preserving coverage checks rather than hiding an invalid
or concurrently changing result behind `Count()`.

## Independent writable output

An expression-local helper appends only the requested interval to an owned
output bitmap. For a nonempty source it uses the existing
`ElementWiseBitsetPolicy<uint8_t>::op_copy` with a const source pointer and
explicit source/destination bit offsets. All-valid appends extend the output
with true bits. Neither path casts away constness or mutates the index bytes.
Offset reads use the view's `test()` after selecting the proper coordinate
space. Partial batches and non-aligned appends preserve existing output bits.

The returned batch still needs writable validity storage. A batch covering the
whole segment therefore still materializes a full output bitmap. The saving is
in the retained per-leaf full validity cache, not the elimination of every
validity output allocation. Unlike this 2.6 batch path, newer branches also
have whole-range move paths and require their own adaptation and validation.

## 2.6 reader boundaries

2.6 uses `ScalarIndex`/`InvertedIndexTantivy`. Only finalized sealed Tantivy
validity opts in. Its existing shared-const storage remains unchanged; callers
borrow its bytes without retaining another bitmap owner. Growing indexes and
unsupported backends retain the owned fallback. ARRAY-cast JSON inverted indexes
may delegate to base field validity. Scalar JSON inverted indexes and JSON-flat
indexes/executors explicitly decline because their path/type comparable masks
have different semantics. TextMatch and Ngram preserve their existing base
validity semantics. Hybrid indexes retain the default unsupported capability.

Index selection and pinning occur during expression initialization. The pin is
kept as an expression member across every batch and offset evaluation. Mutable
raw-data/statistics caches retain their existing owned storage. This change does
not modify query scheduling, expression-result cache admission, or the old
writable `IsNotNull()` contract.

The 2.6 implementation consumes only `size()`, `data()`, `test()`, and the dense
mapping contract from the view. It does not use Knowhere's optional filtered-bit
count, or interpret its ANN exclusion-mask metadata as validity statistics.

## Validation

- Verify many leaves borrow the same buffer without invoking `IsNotNull()`.
- Keep a pin alive while borrowing; release it only after reads finish. Do not
  require a borrowed view to extend the reader's lifetime independently.
- Mutate returned batches and whole-range output, then confirm subsequent
  reads still see the original index validity and cached expression results.
- Cover all-valid, all-null, zero-size, non-aligned intervals, partial batches,
  repeated offsets, and owned growing fallback with its actual size.
- Check JSON ARRAY validity and scalar JSON/JSON-flat comparable-mask fallback,
  plus TextMatch and Ngram nullable-index regressions.
- Build with the independent 2.6 dependencies and repeat focused native tests
  with both the original parser and the independent ARRAY compilation backport.
  Native validation results are recorded separately; historical snapshot-based
  tests do not verify this borrowed-view revision.
