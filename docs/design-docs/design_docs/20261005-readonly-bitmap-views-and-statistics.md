# Bitmap ownership, read/write views and scalar-index validity

Related issue: #53669. Consolidates #53865 and the validity optimization from
#54437. Updated: 2026-10-21.

## Ownership contract

Bitset owns dense storage. BitsetView (ReadView) borrows a read-only interval;
WriteView borrows a writable interval. Views do not extend storage lifetime.
Only a mutable owner or WriteView can produce another write window. ReadView
and const owners cannot produce WriteView. Storage growth stays on the owner.

Expression inputs and published bitmap results are read-only, including data
and validity. Producers fill their private owning buffers, then move them into
ColumnVector. Bitmap column accessors expose only ReadView and const raw data.
AND/OR/NOT never modify child results. A conjunction creates one private
accumulator when a second child needs merging and reuses it for later children;
it does not clone once per child. SQL three-valued logic remains intact until
a consumer explicitly rejects UNKNOWN.

Filtering reads published predicates to construct its own output. MVCC owns the
final writable query mask, reads that filter output and adds timestamp/delete
exclusions. Shared deletion snapshots remain read-only. After writing, MVCC
publishes ReadView for ANN and subsequent consumers. Write + Read merges into
the existing Write destination, without making the source writable:

```cpp
auto write = query_mask.write_view();
write.inplace_or(filter_excluded, query_mask.size());
// Add timestamp and deletion exclusions, then finish the write phase.
auto read = query_mask.read_view();
```

Predicate data/validity use one for match/valid; final filters use one for
excluded. Conversion computes `~(data & validity)`. UNKNOWN is excluded along
with FALSE. Iterative filtering evaluates `data && validity` in candidate order
without writing the expression result. Row/element conversion and sampling
build independent outputs in their respective coordinate domains.

## Read-phase statistics

ReadView caches count and all/none locally for a stable, immutable interval.
A known count answers both predicates; a true all/none establishes count as the
interval size or zero. Cold all/none retain early exits instead of forcing a
complete count. Empty intervals have count zero and both all/none true.

Copying a ReadView copies its known facts. A subwindow has independent statistics.
Fresh views do not share statistics with the owner. Retain a view for repeated
reads. The atomic cached fields permit concurrent readers; writes concurrent
with reads and descriptor reassignment concurrent with reads are unsupported.

Bitset and WriteView do not cache counts, track generations, notify readers,
allocate cache state, or wrap single-bit writes. Raw pointers have no cache
side effects. Writing ends the current read phase; create a fresh ReadView
before reading the edited buffer. This rule also applies to private computation
buffers. Resizing or destroying storage invalidates every borrowed descriptor.

## Scalar-index validity

INullReader adds optional `GetValidityView()`. nullopt means unsupported and
retains the owned `IsNotNull()` fallback. An engaged empty view represents all
Count() coordinates valid; a nonempty view is dense, unmapped and has that
logical length. The sealed inverted reader exposes stable validity with the
same semantics as IsNotNull. JSON path/type validity is not interchangeable
with field-level validity; unsupported readers retain their existing behavior.

An expression-private state keeps borrowed view plus logical length mutually
exclusive with an owned read-only snapshot. Fallback length derives from the
snapshot, including mutable readers whose separately sampled Count can differ.
Borrowed views are cleared before the selected reader pin is released. They
are not published without an owner or stored as naked expression-cache entries.

Offsets gather only requested coordinates. BinaryRange's typed JSON index path
retains hits for repeated batches but avoids another full-segment validity
copy. All-valid uses the reader sentinel, without scanning all bits first.
Batch outputs remain independently owned. Nonaligned bit windows use the
existing bit-copy primitive rather than byte-aligned search subviews.

Expression-cache admission and storage remain unchanged: Put encodes owned
bytes; Get decodes query-local bitmaps. Result and validity may share a lifetime
control block without sharing storage. Reference count alone must not force
extra clones of these independently decoded buffers.

## Kernels and coordinate domains

Keep dense storage, scalar boundary handling, unaligned copy/Boolean paths,
fused AND-FLIP, and large-count SIMD dispatch. Scalar count uses independent
accumulators; VectorizedElementWise selects the architecture-specific kernel.
No separate count_bulk implementation wrapper or range-check template is needed;
range preconditions retain debug assertions.

Write callbacks receive bounded, destination-local windows and must preserve
neighboring bits. Sources can be read-only windows; AND/OR update only Write.
Knowhere mappings/windows and vector validity use backend ID domains. Their
prepared counts must not be substituted with a row-domain population count.
Raw serialization requires bit-zero payloads; nonzero-offset inputs are packed
and kept alive until encoding finishes. Query and iterator owners retain search
filter storage throughout asynchronous consumption.

## Verification scope

Verify immutable input reuse through logic and MVCC, all nine SQL truth-table
pairs, sequential and repeated/scattered offsets, nonaligned boundaries, growing
and sealed visibility, JSON/ARRAY validity, serialization and search ownership.
Test ReadView cache reuse during stable reads and fresh statistics after writes;
check single-bit and bulk writers have no statistics bookkeeping. Record native
source/dependency hashes and test process exits. Focused native checks do not
establish Milvus E2E correctness, concurrent eviction or production QPS gains.
