# MEP: Batched membership in sorted scalar indexes

- **Created:** 2026-09-29
- **Author(s):** @KurodaKayn
- **Status:** Draft
- **Component:** Index
- **Related Issues:** #53853

## Summary

Optimize `IN` and `NOT IN` in `STL_SORT` by deduplicating query values and
reusing lookup progress through the sorted index. The design covers
Int8/Int16/Int32/Int64, Bool, Float/Double, and String/VarChar.

## Motivation

Independent binary searches repeat lookup and bitmap writes for duplicate
terms. Sorting distinct query values allows a forward cursor to reuse search
progress. For strings, deduplication also avoids traversing the same posting
list repeatedly.

## Design

A shared helper in `index/SortedMembership.h` handles matching for
`ScalarIndexSort<T>` and the memory/mmap implementations of `StringIndexSort`.
Query preparation depends on the type:

| Types | Query preparation |
| --- | --- |
| Integers | Copy, sort, and deduplicate typed values. |
| Bool | Track the presence of false and true with two flags; no allocation or sorting. |
| Float/Double | Copy, sort, and deduplicate non-NaN queries; preserve the original lookup loop when any query value is NaN. |
| String/VarChar | Sort and deduplicate borrowed `string_view`s without copying character buffers. |

For ordered distinct queries, the cursor advances through index values using
galloping to bound each search, followed by binary search within that range.
It visits each matching numeric entry or string posting list once. Numeric
matches report original row offsets; string matches expand dictionary entries
into their posting lists. Value accessors support heap and mmap storage without
materializing the index.

`IN` starts with an empty bitmap and sets matches. `NOT IN` starts from the
validity bitmap and clears matches, keeping NULL rows false. Empty queries
return the initialized bitmap; empty indexes require no query access. Neither
query buffers nor stored entries are modified.

### Floating-point and string semantics

Floating-point sorting, deduplication, and lookup use ordinary comparisons.
Positive and negative zero match each other; infinities retain numeric ordering.
The existing sorted-value precondition remains: this change does not define an
ordering for stored NaNs.

A NaN query has unusual existing behavior: the lower/upper-bound loop spans
all indexed values, so `IN` selects every valid row and `NOT IN` clears them.
Queries containing NaN retain that loop and its diagnostic callbacks to preserve
compatibility. NaNs never reach query sorting or deduplication.

String views borrow the caller's buffers only for the synchronous lookup.
Comparisons preserve full-length bytewise ordering, including embedded NULs.
Long common prefixes still incur character comparison costs.

### Cost and tradeoffs

For queries other than Bool or NaN-containing lists, preparation costs
`O(N log N)` comparisons and `O(N)` auxiliary elements, where `N` is the query
count. Bool preparation takes `O(N)` time and constant storage. Deduplication
reduces repeated bitmap writes; the monotonic cursor avoids restarting each
search over the full index.

Sorting and allocation can outweigh these savings for small queries or indexes.
The initial implementation omits the `N < 128` and `N > M` strategy fallbacks
(`M` is the valid entry count), prioritizing type coverage and correctness as
[agreed in review](https://github.com/milvus-io/milvus/pull/53901#issuecomment-5994670915).
The reported small-list/fallback regressions are non-blocking at this stage.
Per-type fast paths can be selected later from benchmark results.

## Compatibility

Public interfaces, persisted formats, and row-offset semantics are unchanged.
Existing legacy BinarySet and packed V3 indexes use the same lookup helper
after loading; no migration or rebuild is required. Other index strategies
are outside this change.

The related [SortedIndexReader refactor #53585](https://github.com/milvus-io/milvus/pull/53585)
will require adapting the value accessor and posting visitor if it lands first.

## Validation

Native CI passed Build, Code Check, UT Integration, UT Go, and UT C++
coverage. The C++ suite reported 9,734 of 9,734 tests passed.

The focused tests compare supported types with scan oracles and cover empty,
repeated, missing, extreme, and NULL values, query order, input immutability,
reloads, heap/mmap storage, and synchronous/asynchronous loading. Floating-point
tests cover signed zero, infinities, and NaN compatibility. String tests cover
empty strings, embedded NULs, high bytes, UTF-8, long common prefixes, and
repeated terms.

The standalone Int64 benchmark compares the original binary-search baseline
with the batched path, including query preparation and bitmap work. Across 576
main-sweep cases, average speedups by query size were:

| Query terms | Average speedup |
| ---: | ---: |
| 1 | 1.027x |
| 8 | 2.268x |
| 64 | 3.873x |
| 128 | 4.860x |
| 512 | 9.237x |
| 4,096 | 33.738x |

The benchmark also passed 2,400 scan/baseline correctness comparisons. Small
queries and small indexes contain regressions: 40 of 576 main-sweep cases and
64 of 144 small-index cases were slower. The initial implementation keeps the
same strategy for all query sizes; per-type and small-list fast paths remain
follow-up tuning based on these measurements.

## References

- [Issue #53853: IX01](https://github.com/milvus-io/milvus/issues/53853)
- [PR #53901](https://github.com/milvus-io/milvus/pull/53901)
