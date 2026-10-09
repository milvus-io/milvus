# MEP: NaN total order and sorted ordinary Array indexes

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
infinity. Keep NaN distinct
from NULL. Normalize NaN only at numeric index-key boundaries; retain original
source values and array positions. Ordinary Array HYBRID indexes may select
STL_SORT at high cardinality on scalar engine version 6.

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

Integer columns and integer Array elements retain their typed-query constraints:
NaN and infinities cannot be represented as integers and are rejected during
query compilation, including nested whole-array values. FLOAT/DOUBLE and dynamic
JSON numeric queries retain the floating total-order contract.

ORDER BY and supported aggregation MIN/MAX, group keys and DISTINCT use the
same sortable-key equality/order. NaN payloads form one group rather than
separate buckets; NULL remains a separate group. Existing temporary group-key
normalization may widen FLOAT to DOUBLE without changing field storage widths.

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

Scalar, batch, array and single-segment writers use the same normalization as
term, term-set and one/two-bound range queries. JSON numeric query terms also use
this boundary. Current native numeric schemas are indexed without FAST fields;
any future FAST writer must use the same normalization rather than bypass it.
JSON flat indexes also have FAST columns; strict JSON input cannot represent
numeric NaN. NaN floating range bounds use typed inverted ranges instead of the
FAST integer conversion that could otherwise turn NaN into zero.

FLOAT source values remain 32-bit. Tantivy already widens FLOAT to f64 exactly;
this change does not increase its key width. DOUBLE remains 64-bit.

### SORT and BITMAP

STL_SORT retains floating values and ordinary source offsets, including valid
NaNs. Its comparator places NaNs at the end; all bound checks and searches use
that same comparator. There are no unindexed valid NaN holes or NaN-row sidecars.

BITMAP retains its floating key type. A NaN-aware map comparator groups every
NaN into one key whose bitmap identifies matching source rows. HYBRID cardinality
selection uses the same comparator and may choose BITMAP for low-cardinality data
containing NaN; it does not force STL_SORT merely because NaN occurs.

### Ordinary Array postings

Ordinary Array indexes map element values to parent-row IDs. Repeated NaNs in one
array contribute the same parent row, just as repeated ordinary values do. For
rows [1,NaN,3,NaN], [3,5], [NaN], postings are 1->[0], 3->[0,1], 5->[1], NaN->[0,2].
Contains-any unions postings; contains-all intersects them. Original array length
and element positions are preserved outside the contains index. A scalar reverse
lookup cannot reconstruct a whole array, so ordinary Array SORT keeps HasRawData
false. Nested struct-array indexes retain their flattened element-offset domain.

Numeric Array range matches use forward matching, since complementing mismatching
element postings could remove a row that also has a matching element. Non-null
empty arrays have no postings and remain valid; NULL arrays remain invalid.
Version 6 enables ordinary Array HYBRID high-cardinality sorted parent postings;
older versions retain the existing high-cardinality INVERTED selection.

## Compatibility and migration

Scalar engine version 6 advertises the new NaN key/comparison contract. No new
physical container layout is introduced. New floating index writers reject valid
NaN when explicitly asked to target an older reader that cannot implement this
contract. Tantivy also gates negative-zero key normalization for older readers;
ordinary data remains buildable at older negotiated versions.

Old Tantivy indexes may split NaNs across keys and across both ends of the numeric
order. Old SORT entries may be unordered. Old BITMAP maps may have merged NaN
with an ordinary key, losing distinctions that cannot be repaired from the index.
Therefore the production segment loader excludes pre-v6 floating scalar indexes,
floating Array/Struct indexes and numeric/flat JSON indexes from its index cache,
causing the raw field to load and execute instead. String/integer indexes are not
excluded. An exception is an explicit JSON cast-function index such as
STRING_TO_DOUBLE: that function runs at index time and is not applied to original
JSON scans. Falling back would change even finite string values such as "42".
These old cast indexes report a rebuild-required error rather than silently
change results. Rebuild them at v6 before loading with the new reader. The same
comparison/key contract applies to the projected numeric value; original strings
remain strings and are not implicitly cast by raw queries or JSON stats. Raw files
must remain available, as required by normal scalar rebuilds.
The existing scalar-version auto-upgrade/force-rebuild mechanism can rebuild these
indexes from raw data at version 6; indexing resumes when new metadata is loaded.
This implementation does not issue any production rebuild operation itself.

This is an intentional query semantic change. During a mixed-binary rolling
upgrade, old QueryNodes still evaluate IEEE comparisons; index engine negotiation
alone cannot make their raw scans use the new semantics. Consistent NaN results
require completing the QueryNode/Proxy upgrade. Rebuild affected indexes at v6
before relying on indexed performance. Downgrading to old binaries after writing
v6 NaN indexes requires rebuilding for the old version and reverts query semantics.
No compatibility is promised for intermediate, unpublished skip-NaN formats from
this PR's earlier revisions.

## Test plan

Compare raw and SORT/BITMAP/INVERTED/HYBRID results for positive and negative NaNs
with several payloads, infinities, signed zeros, ordinary numbers and NULLs. Cover
EQ/NE, IN/NOT IN, all ordered comparisons, inclusive/exclusive range bounds,
negation, array contains-any/all, positional access, whole-array equality,
arithmetic comparisons and Struct MATCH_ANY/ALL. Verify parent versus element
posting domains, reloads in memory/mmap paths, old-version writer guards, loader
raw fallback and existing scalar rebuild negotiation. Test compiler folding and
query templates. Exercise SIMD scalar/tail/vector boundaries on supported CPU
architectures, with independent expected results rather than native NaN equality.

## References

- https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT
- https://duckdb.org/docs/current/sql/data_types/numeric#floating-point-types
