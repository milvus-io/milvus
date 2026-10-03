# MEP: Batched Int64 membership in sorted scalar indexes

- **Created:** 2026-09-29
- **Author(s):** @KurodaKayn
- **Status:** Draft
- **Component:** Index
- **Related Issues:** #53853

## Summary

Optimize `IN` and `NOT IN` for sorted Int64 scalar indexes.

- For `N < 128` or `N > M`, keep the existing lower/upper-bound lookup. `M` is
  the number of valid index entries, excluding NULL rows.
- For `128 <= N <= M`, sort and deduplicate a private query copy, then scan the
  sorted index forward while reporting the original row offsets.

Other scalar types use the shared binary-search helper. Query semantics,
bitmaps, stored formats, and public interfaces are unchanged.

## Motivation

The existing path performs an independent search for every term. Duplicate
terms repeat work, while sorted terms cannot reuse search progress. Sorting is
not worthwhile for small queries or when the valid index is smaller than the
query, so both query length and valid entry count select the path.

## Public Interfaces

No public API, index format, or loader interface changes. `In` and `NotIn`
retain their signatures and bitmap semantics.

## Design Details

`VisitSortedMatches` in `index/SortedInt64Lookup.h` contains the original
full-range lower/upper-bound algorithm. `VisitSortedInt64Matches` uses it for
the fallback cases and the batched cursor algorithm otherwise. Both helpers
accept a validator callback so `ScalarIndexSort` keeps its existing diagnostic
for an unexpected sorted entry.

The batched path copies, sorts, and deduplicates the query. It advances a
single cursor through the sorted entries, uses bounded exponential probing plus
`std::lower_bound` to find each value, and visits each equal run once. The
cursor never moves backwards. Comparisons do not subtract Int64 values or
increment query values, so signed extremes are safe.

`In` starts with an empty bitmap and sets matches. `NotIn` starts from
`valid_bitset_` and clears matches, leaving NULL rows false. Empty queries,
missing terms, duplicates, mixed NULLs, heap/mmap entries, and nested-array
offsets retain existing behavior. The caller's query buffer is never modified.

Batch preparation costs `O(N log N)` time and `O(N)` memory. The scan reuses
progress across distinct terms and visits each matching entry once.

## Compatibility, Deprecation, and Migration Plan

No migration or index rebuild is required. Legacy BinarySet and packed V3
indexes use the same sorted representation after loading.

The implementation currently targets `ScalarIndexSort`. If
[SortedIndexReader refactor #53585](https://github.com/milvus-io/milvus/pull/53585)
lands first, port the helper to its numeric reader and keep the same validity
initialization and test coverage.

## Test Plan

- Compare `IN` and `NOT IN` with a scan oracle across threshold boundaries,
  duplicates, missing terms, input order, Int64 extremes, empty queries, and
  NULL layouts.
- Cover heap and mmap storage, legacy BinarySet reloads, and packed V3 reloads.
- Run `ScalarIndexSortMembershipTest`, `ScalarIndexSortV3AsyncLoadTest`, and
  `StlSortIndexTest` in the C++ test suite.
- Run the standalone optimized benchmark against the original lookup and run
  its ASan/UBSan variant.

## Performance

Standalone component benchmark on Apple M1 Pro with `-O3`, compared with the
original full-range binary-search helper:

| Path | Result |
| --- | --- |
| Fallback (`N < 128` or `N > M`) | Sorting avoided; median is about baseline speed. |
| Batch (`N = 128`, `129`, `512`) | About `4.5x`, `4.5x`, and `5.6x` median speedup. |
| `N = 4,096` | Aggregate mixes both paths; same-path comparison is approximately neutral (`~0.99x`). |

These are scalar-index component measurements; server-level and Linux
performance remain unmeasured.

## Validation

The standalone correctness and ASan/UBSan checks pass. PR #53901's original
revision passed the full C++ test suite in CI; rerun the suite for this review
follow-up. Full index/reload validation remains a CI requirement for the
updated revision.

## Rejected Alternatives

- **Always batch:** adds sorting overhead for small queries and tiny valid
  indexes.
- **Search every term independently:** misses deduplication and cursor reuse.
- **Reuse `GrowingOffsetMapping::GallopLowerBound`:** it is private and tied to
  chunked offset storage.

## References

- [Issue #53853: IX01](https://github.com/milvus-io/milvus/issues/53853)
- [SortedIndexReader refactor #53585](https://github.com/milvus-io/milvus/pull/53585)
- [Async packed scalar index loading MEP](20260907-async-packed-scalar-index-loading.md)
