# MEP: NaN total order for scalar data

- **Created:** 2026-10-09
- **Author:** @xiaofan-luan
- **Status:** Draft
- **Component:** Proxy, Index, QueryNode, DataNode, Coordinator
- **Related PR:** https://github.com/milvus-io/milvus/pull/53972
- **Design document:** https://github.com/xiaofan-luan/milvus-design-docs/blob/design/nan-total-order-53972/design_docs/20261009-nan-total-order.md
- **Submission note:** The upstream milvus-design-docs repository is archived; GitHub rejected PR creation. The design is published on the author fork for review.

## Summary

Use one scalar floating-point comparison contract across raw scans, query constant
folding, arrays, every numeric scalar index, JSON stats and skip-index pruning:
all NaNs compare equal and sort after every non-NaN number, including positive
infinity. Keep NaN distinct from NULL. Canonicalize comparison and dictionary keys;
this does not rewrite raw source columns or array positions. Index-derived values
need not preserve each row's original floating bit pattern. This builds on ARRAY
and typed JSON ARRAY support merged in [#54505](https://github.com/milvus-io/milvus/pull/54505).
Its existing selection policy and 3.1.0 software gate remain unchanged. Writers
enable the new NaN key contract only when the negotiated scalar engine target
is at least 6.

## Motivation

Native IEEE comparisons do not provide a strict weak ordering for values that
include NaN. Combining them with ordered maps, binary search, range pruning or
term encodings can produce different results for a scan and an index query.
Skipping NaN postings requires separate validity holes and special handling for
negation and reverse lookup. A database-level total order, as used by PostgreSQL
and DuckDB, removes those holes and defines one observable behavior.

## Public behavior

For every supported floating scalar comparison, including values selected from
arrays and struct members:

| Expression | Result |
| --- | --- |
| NaN == NaN | true |
| NaN != NaN | false |
| NaN > +Inf | true |
| NaN < 3 | false |
| NaN IN [NaN] | true |
| NaN IN [3] | false |
| NaN IS NULL | false |
| -0 == +0 | true |

The sign and payload of NaN do not affect equality, ordering or hashing. Arithmetic
still computes floating-point results normally; only comparison and membership
interpret those results using the total order. NULL continues to use the existing
validity masks and three-valued expression semantics. Invalid member payloads do
not become valid because they contain NaN.

Numeric query templates accept NaN, including scalar, array and nested-array
values. Constant arithmetic may produce NaN, and constant comparisons use the
same order as execution. String "NaN" remains a string. Probability and distance
parameters still require finite numbers. Scalar FLOAT/DOUBLE payloads accept NaN
and infinities, matching floating array elements; vector data retains its finite
validation policy. The query grammar gains no reserved NaN identifier.

REST represents non-finite FLOAT/DOUBLE field values, including floating Array
and Struct members, as the JSON strings "NaN", "Infinity" and "-Infinity".
Finite values remain JSON numbers and NULL remains null. Typed floating fields
and arrays accept these strings on input; ordinary string fields and dynamic
JSON retain their string values. This keeps non-finite scalar results valid JSON
without losing NaN as NULL. Scalar/Array writes and bulk imports do not reject
floating NaN or infinities; vector validation retains its finite-value checks.

Integer columns and integer Array elements retain their typed-query constraints:
NaN and infinities cannot be represented as integers and are rejected during
query compilation, including nested whole-array values. FLOAT/DOUBLE and dynamic
JSON numeric queries retain the floating total-order contract.

ORDER BY and supported MIN/MAX use the same sortable-key equality/order. Existing
internal grouping and DISTINCT-style deduplication, including function-chain
grouping, use one key for all NaN payloads; NULL remains a separate group. This PR
does not add public FLOAT/DOUBLE GROUP BY or DISTINCT support; existing admission
constraints remain unchanged. Temporary group-key normalization may widen FLOAT
to DOUBLE without changing field storage widths. Go QUERY ORDER BY,
function-chain scalar sorting/grouping and numeric-combine MIN/MAX use the same
contract for scalar columns and intermediate values.
Shared array/object heap constructors use this order for floating keys, including
named floating types. Custom `HeapInterface` implementations retain their
caller-supplied comparison semantics.
The existing final `$score` validation still rejects nonfinite scores; NaN scalar
field support does not change that boundary or vector validation. Existing
finite-score epsilon tie behavior in segment merging remains unchanged. SUM,
AVG and other arithmetic retain their existing behavior; their results use total
ordering when subsequently compared.

This aligns scalar NaN equality and ordering with PostgreSQL `real` and `double
precision`; it does not claim full PostgreSQL SQL compatibility. NaN remains
distinct from NULL and cannot be converted to an integer. Standard JSON numeric
syntax still excludes NaN/Infinity; typed floating REST fields use the string
representation described above. Arithmetic errors, extension/model behavior and
vector validation remain separate contracts; XGBoost still interprets a NaN
feature through the model's missing-value branch.

## Index design

### Shared comparisons

A lightweight scalar comparison header defines FloatToSortableKey and derives
equality, ordering and hashing from that key. FLOAT maps to uint32 and DOUBLE
maps to uint64; zero normalizes to positive zero, negative bits are inverted,
positive bits have their sign bit flipped, and every NaN maps to the maximum key.
Go constant folding and query rewrites use the same typed key algorithm.
Non-floating types retain their existing behavior. SQL floating comparisons use
NaN equality and NaN-last ordering; signed zeros compare and hash equally.
Sorting, binary search, map lookup, query-value deduplication and skip-index
bounds use these helpers. SIMD comparison kernels apply NaN lane masks while
retaining vectorized evaluation. Exact JSON signed/unsigned integer comparisons
retain their existing precision rules.

### Tantivy numeric keys

Tantivy already encodes f64 terms as sortable uint64 keys. At every floating
binding writer and query boundary, normalize signed zero to positive zero and NaN to the positive NaN bit pattern
0x7fff_ffff_ffff_ffff. Both currently supported Tantivy encoders map that pattern
to UINT64_MAX. Thus every NaN shares one term and sorts after +Inf, without changing
the dependency revision, field type or posting format. Preserve actual unbounded
range endpoints; +Inf is a value rather than a synonym for an unbounded endpoint.

For scalar engine targets at least 6, scalar, batch, array and single-segment
writers use the same normalization as term, term-set and one/two-bound range
queries. Lower targets retain the dependency encoder used before this change.
Tantivy format versions V5/V7 are independent of this scalar engine capability;
V7 already normalized signed zero before this PR. JSON numeric query terms also
use this boundary. Current native numeric schemas are indexed without FAST fields;
any future FAST writer must use the same normalization rather than bypass it.
JSON flat indexes also have FAST columns; strict JSON input cannot represent
numeric NaN. NaN floating range bounds use typed inverted ranges instead of the
FAST integer conversion that could otherwise turn NaN into zero.

FLOAT source values remain 32-bit. Tantivy already widens FLOAT to f64 exactly;
this change does not increase its key width. DOUBLE remains 64-bit.

### SORT and BITMAP

STL_SORT retains floating values and ordinary source offsets, including valid
NaNs. From scalar engine version 6, its writer comparator places NaNs at the end;
all bound checks and searches use that same total order. Older build targets keep
the original writer comparator, and new readers use raw data for those indexes.
There are no unindexed valid NaN holes or NaN-row sidecars.

BITMAP retains its floating key type. A NaN-aware map comparator groups every
NaN into one key whose bitmap identifies matching source rows. HYBRID cardinality
selection uses the same comparator and may choose BITMAP for low-cardinality data
containing NaN; it does not force STL_SORT merely because NaN occurs. Older build
targets retain the original map/cardinality comparisons and postings; legacy
compatibility does not promise the new NaN query semantics.
Where value lookup is supported, BITMAP Lookup/Gather return a representative
posting key, so a row's NaN sign/payload or zero sign may change on index readback.
Raw source columns retain those original values; comparison equivalence does not
promise bitwise identity for index-derived values.

### Ordinary Array postings

Ordinary Array indexes map element values to parent-row IDs. Repeated NaNs in one
array contribute the same parent row, just as repeated ordinary values do. For
rows [1,NaN,3,NaN], [3,5], [NaN], postings are 1->[0], 3->[0,1], 5->[1], NaN->[0,2].
Contains-any unions postings; contains-all intersects them. Original array length
and element positions are preserved outside the contains index. A scalar reverse
lookup cannot reconstruct a whole array, so ordinary Array SORT reports
`Caps().value_lookup == false`. Nested struct-array indexes retain their flattened
element-offset domain.

Numeric Array range matches use forward matching, since complementing mismatching
element postings could remove a row that also has a matching element. Non-null
empty arrays have no postings and remain valid; NULL arrays remain invalid.
The parent-row posting implementation and ARRAY family selection come from
#54505 and remain unchanged. NaN canonicalization is the new v6 capability; it
does not replace the existing ARRAY software gate or alter the nested-index
coordinate domain.

### Builder, artifact, loader and reader integration

Writers receive the negotiated scalar version through the current family build
parameters. SORT and BITMAP preserve legacy writer comparisons below v6;
INVERTED and full JSON writers set the same capability on the Tantivy wrapper.
HYBRID scalar and ARRAY cardinality probes use the corresponding writer order.
All reader lookups, range bounds, key maps and query deduplication use the shared
comparison helpers. Legacy index eligibility is checked before those readers
are installed. No deleted monolithic index class is restored.

Tests build through the public registry, serialize actual artifacts, reload in
synchronous/asynchronous heap and mmap modes, and compare postings against an
independent total-order oracle. The matrix includes scalar rows, ordinary ARRAY
parent rows and nested element coordinates.

## Scalar clustering and partition statistics

Floating clustering keys use the same canonical integer keys for counting,
bucket planning and row-to-buffer lookup. Dictionary entries retain a source
value for bounds; output rows preserve their original values. This prevents a
NaN key from disappearing on lookup and places all NaN payloads in one bucket.

Fresh FLOAT/DOUBLE bounds carry an additive `floatBoundsComplete` marker. Old
bounds can silently omit stored NaNs even when both endpoints look finite, so
new QueryNodes keep segments with unmarked floating bounds instead of pruning
from them. Updating a legacy stats object does not promote its bounds to complete.

The existing partition-stats JSON format cannot encode nonfinite numeric bounds.
Keep exact bounds in memory, but omit the whole segment's optional stats entry
when serializing such bounds. Missing entries mean unknown and do not remove
logical segments or source rows. Finite stats retain ordinary numeric endpoints
that old readers can decode. New pruning also treats absent bounds as unknown;
term pruning must inspect later literals even if an earlier unsorted literal is
larger than the segment's maximum.

Older DataCoords turn a missing stats entry into an empty entry while remapping
output segment IDs, which old QueryNodes cannot safely read. New DataCoords
preserve missing entries and advertise `supports_partial_partition_stats` in
compaction request JSON. New DataNodes require that capability only when scalar
clustering encounters nonfinite values, before opening output writers. Worker
local defaults must not advertise it on an older coordinator's behalf. Finite
old-coordinator plans keep working. After the coordinator upgrade, a fresh
compaction request carries the capability; retry/scheduling policies are unchanged.
This gate does not support coordinator downgrade in the middle of unfinished
partial-stats compactions.

## Compatibility and migration

Scalar engine version 6 advertises the new persisted NaN key contract without a
new physical container layout. With registered QueryNodes, DataCoord starts from
their lowest current scalar version. A configured `targetScalarIndexVersion`
raises that version unless force-rebuild selects the target directly; the result
is clamped to the shared reader-supported range. The DataNode then caps the build
version to its own supported maximum and reports the actual version in metadata.
Canonical NaN keys are written only when that actual build version is at least 6.
Lower targets retain legacy writer comparisons/encoding; valid NaN and signed
zero do not fail a build merely because the target is older. New Proxy constant
comparisons and QueryNode raw scans use the new semantics immediately, without
waiting for a v6 index. The existing ARRAY 3.1.0 software gate is unchanged. No new
retry state or scheduler behavior is introduced.

Old Tantivy indexes may split NaNs across keys and across both ends of the numeric
order. Old SORT entries may be unordered. Old BITMAP maps may have merged NaN
with an ordinary key, losing distinctions that cannot be repaired from the index.
Therefore the production segment loader excludes pre-v6 floating scalar indexes,
floating Array/Struct indexes and numeric/flat JSON indexes from its index cache,
causing the raw field to load and execute instead. This applies even if an
affected index contains only finite values: its metadata cannot prove that its
old floating keys are lossless. String/integer indexes are not excluded. An
exception is an explicit JSON cast-function index such as
STRING_TO_DOUBLE: that function runs at index time and is not applied to original
JSON scans. Falling back would change even finite string values such as "42".
These old cast indexes report a rebuild-required error rather than silently
change results. Rebuild them at v6 before loading with the new reader. The same
comparison/key contract applies to the projected numeric value; original strings
remain strings and are not implicitly cast by raw queries or JSON stats. Raw files
must remain available, as required by normal scalar rebuilds.
The existing scalar-version auto-upgrade/force-rebuild mechanism can rebuild these
indexes from raw data at version 6; indexing resumes when new metadata is loaded.
Both `dataCoord.autoUpgradeSegmentIndex` and
`dataCoord.forceRebuildScalarSegmentIndex` default to false. Upgrading binaries
alone does not schedule migration of existing indexes; rebuilding requires an
explicit action or enabling the applicable existing policy and target. This PR
does not issue any production rebuild operation itself.

This is an intentional query semantic change. During a mixed-binary rolling
upgrade, old QueryNodes still evaluate IEEE comparisons; index engine negotiation
alone cannot make their raw scans use the new semantics. Consistent NaN results
require completing the QueryNode/Proxy upgrade. Floating clustering and its
statistics additionally require upgraded DataNodes and DataCoords. Rebuild affected indexes at v6
before relying on indexed performance. Downgrading to old binaries after writing
v6 NaN indexes requires rebuilding for the old version and reverts query semantics.
No compatibility is promised for intermediate, unpublished skip-NaN formats from
this PR's earlier revisions.

RLS policy evaluation is outside the scope of this change.

Loading admission and final resource estimates use the same index-eligibility
rule as the native loader. Skipped legacy indexes contribute no index reservation
and do not suppress raw-field memory or mmap disk accounting. Explicit old JSON
cast indexes report the same rebuild-required error during estimation and loading.

## End-to-end coverage boundaries

The intended comparison contract covers scalar fields and intermediate scalar
function-chain operations. Current master retains its finite final-score output
validation. This change does not make nonfinite ANN/model scores valid public
results. Scalar field ordering, grouping and raw/index parity must be checked
separately from metric-score validation.

Cached search iterators now use the public metric direction consistently for
heap ordering, batch sorting, and continuation/range bounds. Similarity metrics
(IP/COSINE) put NaN first under descending order; distance metrics put NaN last
under ascending order. Comparisons restore the original score before applying
the scalar order, because negating NaN does not reverse its position. A cached
head rejected by a changed bound still refills its source chunk. The existing
strict score-only continuation bound excludes equal scores, including equal
NaNs; this is not a new tie-complete cursor protocol. Individual Knowhere
iterators retain their existing ordered-input contract.

Native ANN merge/refine comparators outside this cached-iterator wrapper remain
separate from the scalar-function-chain contract. No universal claim about all
ANN implementations is made.

Typed protobuf floating results can carry NaN. JSON-owned SDK row APIs need
companion handling for nonfinite scalar/array/struct values as "NaN", "Infinity"
and "-Infinity", while actual NULL stays JSON null. Such SDK fixes and their
release status are outside this server PR and are not established by its tests.
HTTP clients may expose the quoted values as strings; JavaScript request JSON
serialization turns numeric NaN into null unless callers use the quoted typed
representation. Client serialization and SDK-specific row APIs require their own
validation before claiming full end-to-end coverage.

## Test plan

Recorded validation covers x86 native and Go regressions plus Go SDK raw/index
round trips. It does not establish coverage for other SDKs or their releases.
ARM SIMD execution and actual mixed-binary rolling-upgrade/downgrade deployments
have not been run; they remain validation work in addition to the matrix below.

Compare raw and SORT/BITMAP/INVERTED/HYBRID results for positive and negative NaNs
with several payloads, infinities, signed zeros, ordinary numbers and NULLs. Cover
EQ/NE, IN/NOT IN, all ordered comparisons, inclusive/exclusive range bounds,
negation, array contains-any/all, positional access, whole-array equality,
arithmetic comparisons and Struct MATCH_ANY/ALL. Verify parent versus element
posting domains, reloads in memory/mmap paths, legacy writer key/posting
compatibility, loader raw fallback and existing scalar rebuild negotiation. Test compiler folding and
query templates. Exercise SIMD scalar/tail/vector boundaries on supported CPU
architectures, with independent expected results rather than native NaN equality.

Exercise actual Go ORDER BY and function-chain operators, scalar clustering
analysis through row mapping, partition-stats serialization/remapping, and
segment pruning with legacy/missing bounds. Verify loading/final resource
estimates match raw-only loading when legacy indexes are excluded, including
independent index/raw mmap and tiered-eviction settings. Mixed-cluster rolling
upgrade and downgrade behavior requires deployment-level validation in addition
to unit-level compatibility tests.

## References

- https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT
- https://duckdb.org/docs/current/sql/data_types/numeric#floating-point-types
