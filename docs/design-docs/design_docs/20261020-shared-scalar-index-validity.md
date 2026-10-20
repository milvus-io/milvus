# Share immutable scalar-index validity across filter expressions

- Created: 2026-10-20
- Status: Draft
- Component: QueryNode scalar indexes and expression execution
- Tracking: [Milvus #54434](https://github.com/milvus-io/milvus/issues/54434)

## Problem

Indexed scalar expressions commonly cache a full-segment hit bitmap and an
owned validity bitmap before returning batches. For `L` evaluated leaves over
`N` rows, the validity copies alone retain approximately `L * ceil(N / 8)`
bytes, even when every leaf reads the same sealed index. A non-nullable index
also materializes a full all-valid bitmap for each expression.

The hit bitmap depends on the predicate. Sealed index validity can be shared
because it is finalized before queries read it. This proposal removes repeated
full-segment validity materialization without changing the owned result
contract of `IsNotNull()`.

ARRAY contains compilation merging is a separate optimization implemented in
[Milvus #52138](https://github.com/milvus-io/milvus/pull/52138). It can reduce the
number of hit bitmaps, while this change independently reduces validity copies
for predicates that remain separate.

## Optional immutable validity capability

Expose an optional internal snapshot containing a row count and either an
owning `shared_ptr<const TargetBitmap>` or an explicit all-valid marker. The
snapshot owns its storage and can outlive the index reader. An unsupported
capability is different from all-valid; it must invoke the existing virtual
`IsNotNull()` path and retain the resulting owned snapshot.

On master, `INullReader::GetImmutableValidity() const` returns an optional
`ScalarIndexValidity`, with an implementation in the sealed inverted reader.
The master snapshot uses a null owner as its explicit all-valid marker. On 2.6
the capability belongs to `ScalarIndex`, with an
implementation in sealed `InvertedIndexTantivy`. The different index interfaces
require separate implementations rather than copying class hierarchies across
branches.

Only immutable, finalized validity may be shared. Growing indexes retain
snapshot behavior. The provider's row count and bitmap coordinate domain must
agree. An empty bitmap used internally as an all-valid sentinel is not an empty
row space.

## Expression consumption

Expressions retain the immutable handle and materialize only the requested
batch or offset selection. Returned data and validity remain independently
writable because logical operators and final exclusion-mask construction
mutate their operands. A full-range evaluation may move an owned hit bitmap,
but must materialize shared validity rather than move from its owner.

The all-valid path extends the result with true bits without allocating a
temporary full-segment bitmap. Existing output bits must survive non-aligned
appends. A snapshot is reused across batches, preserving the existing index
cursor and offset-evaluation behavior.

Raw-data and statistics paths retain their existing mutable caches. This
proposal does not change cache invalidation, query scheduling, early release,
or the ownership contract of the existing `IsNotNull()` API.

## JSON and derived-reader semantics

Field validity, JSON path existence, and typed comparable-value masks are
different concepts. A derived reader may expose the capability only when its
snapshot exactly matches its own `IsNotNull()` semantics.

For the 2.6 implementation, ARRAY-cast JSON inverted indexes may delegate to
their sealed base validity. Scalar JSON inverted indexes, JSON-flat indexes,
and JSON-flat query executors explicitly decline the capability. TextMatch
and Ngram readers inherit their existing base validity semantics; unfinalized
and growing instances continue to fall back.

For master, the sealed inverted reader provides its own coordinate-domain
validity. JSON-flat comparable/path-specific readers retain their existing
owned results. Reader selection must preserve row-versus-element coordinates
and must not substitute field-level validity for path/type validity.
Typed JSON-path resolution may reuse an inverted reader whose existing
validity already represents the resolved path/type; its coordinate and row
count checks still apply.

## Expression-result cache compatibility

Master's expression-result cache currently owns a writable result/validity
pair. The initial implementation preserves that contract: paths participating
in that cache retain their existing owned validity materialization. Ordinary
uncached index evaluation, validity-only evaluation, and offset reads may use
immutable snapshots where their reader supports them.

Changing the cache value representation is a separate follow-up. This proposal
does not claim to remove validity copies from every cache-enabled execution.

## Compatibility

No protobuf, persisted index format, public API, or configuration migration is
required. Unsupported readers use the existing behavior. New native binaries
must be rebuilt together because this is an internal C++ interface change.

The optimization is independent of ARRAY predicate merging. The 2.6 backport
must also pass with the original 2.6 parser, not only with a parser that merges
ARRAY predicates before execution.

## Validation

- Audit every concrete provider and derived override of `IsNotNull()` to verify
  that shared storage has the same semantics as the owned result.
- Verify that many independent leaves share one owner and never call the
  full-segment owned path when the capability is supported.
- Release the reader while retaining a handle; verify ownership remains valid.
- Mutate one returned batch and confirm neither the owner nor another batch
  changes. Cover non-aligned appends, partial final batches, arbitrary/repeated
  offsets, zero rows, nullable rows, and all-valid storage.
- Compare indexed and raw execution across multiple batches and NULL/ARRAY
  predicates. Exercise growing and JSON path/type fallback behavior.
- For master, test full-range materialization and expression-cache fallback,
  including subsequent reads that would expose moved-from shared storage.
- Run focused native index/expression tests and record source revisions,
  dependency configuration, and any unexecuted integration coverage.

Copy counters and ownership tests establish the eliminated validity copies.
They do not establish production peak-memory savings, latency, throughput, or
the cause of every OOM. Those outcomes require matched workload replay with
the same indexes, segment concurrency, and cache configuration.
