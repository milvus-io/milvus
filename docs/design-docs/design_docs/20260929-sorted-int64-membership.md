# MEP: Batched Int64 membership in sorted scalar indexes

- **Created:** 2026-09-29
- **Author(s):** @KurodaKayn
- **Status:** Draft
- **Component:** Index
- **Related Issues:** #53853

## Summary

Share a matching-row visitor between `ScalarIndexSort<int64_t>::In` and `NotIn`.
For query lists with 128 or more terms, up to the valid index entry count,
sort and deduplicate a private copy and search forward through the index.
Other lists use binary searches; other scalar types keep their implementation.

## Motivation

Full-range binary searches repeat work for duplicate terms and cannot reuse
progress between distinct terms. Processing sorted, distinct terms reduces that
work, but sorting can cost more than searching a small index. The batch path
therefore considers both query length and valid index size.

## Public Interfaces

No public interface changes. `In` and `NotIn` keep their signatures and bitmap
semantics; the batch threshold is an internal constant.

## Design Details

`VisitSortedInt64Matches` in `index/SortedInt64Lookup.h` reads the sorted
heap/mmap entry range and reports original `idx_` offsets. `In` starts with an
all-false bitmap and sets matches; `NotIn` clones `valid_bitset_` and clears them.

Let N be the query term count and M the valid index entry count (`last - first`).
An empty query or index returns before pointer arithmetic or allocation.
When N < 128 or N > M, use full-range lower/upper-bound searches without a query
copy. M excludes NULL rows: a 4,096-row index with only three valid entries must
use M = 3. This guard addresses the small-index sorting regression.

For the remaining lists, the visitor:

1. Copies the query values, sorts the copy, and removes duplicates.
2. Carries a cursor forward through the sorted entries. When the current entry
   is below the next query value, exponential probes bracket the search and
   `std::lower_bound` searches only that bracket.
3. Visits the equal run sequentially, advancing the cursor past every match.
4. Stops when the cursor reaches the end.

The cursor never moves backwards. After consuming an equal run, all entries
before the cursor are less than the next distinct query value. Exponential
probing caps its doubling by the remaining entry count. Comparisons never
subtract Int64 values or increment query values, so both Int64 extremes are safe.

For U distinct query terms and R matching entries, batch preparation costs
O(N log N) time and O(N) extra memory. Search costs
O(U log(1 + M/U) + U + R). Each matching entry is visited once on the batch path.
The threshold of 128 is an initial crossover that needs target-hardware
validation. The N > M guard is conservative: it can give up deduplication gains
for duplicate-heavy lists longer than the index.

`Build` and `BuildWithFieldData` omit invalid rows, retain original offsets, and
sort entries by value. Nested-array building uses its existing element-offset
space. Legacy and packed loaders restore the same sorted entry representation
and validity state. The visitor consumes these representations without changing
row or element validity semantics.

`FlatVectorElement` and `TermIndexFunc` supply query terms; `HybridScalarIndex`
delegates to its selected index. The visitor never modifies caller input.
Empty IN matches no rows; empty NOT IN returns validity. Missing terms change
no bits, duplicates are idempotent, and NULL rows remain false.

## Compatibility, Deprecation, and Migration Plan

Stored formats and loading interfaces are unchanged; no index rebuild or
migration is required. Legacy BinarySet and packed V3 indexes use the same
matching helper after loading.

This implementation targets the `ScalarIndexSort` code at master revision
`ad4887b38cbac0ab984d9c80241f35a331ca47a3`. The related SortedIndexReader
refactor, [#53585](https://github.com/milvus-io/milvus/pull/53585), replaces that
class. If the refactor lands first, integrate the helper with its numeric reader
and migrate the tests while preserving validity initialization. Compatibility
with that refactor remains unverified.

## Test Plan

- Compare IN and NOT IN results against a scan oracle across cardinalities,
  repeated and missing terms, input order, Int64 extremes, and no/mixed/all NULLs.
- Exercise list sizes immediately below, at, and above the batch threshold,
  plus empty lists and lists larger than the valid entry count. Include a
  4,096-row index with only three non-NULL entries to check the N > M guard.
- Verify that membership queries leave caller input unchanged.
- Repeat membership checks after legacy BinarySet and packed V3 reloads, with
  heap/mmap storage and synchronous/asynchronous packed loading.
- Compare the helper with the original binary-search loop in a standalone
  benchmark, including bitmap allocation or validity cloning and matching writes.

With a configured Milvus C++ build:

```sh
cmake --build cmake_build --target all_tests
cmake_build/unittest/all_tests --gtest_filter='ScalarIndexSortMembershipTest.*:ScalarIndexSortV3AsyncLoadTest.*:StlSortIndexTest.*'
```

The standalone benchmark uses the production helper and repository bitsets with
a scalar policy and `std::vector` storage. It is also available as the CMake
target `sorted_int64_lookup_benchmark` when `BUILD_UNIT_TEST=ON`. From the
repository root:

```sh
c++ -O3 -DNDEBUG -std=c++20 -I internal/core/src \
  internal/core/benchmark/SortedInt64LookupBenchmark.cpp \
  -o /tmp/sorted_int64_lookup_benchmark
/tmp/sorted_int64_lookup_benchmark > /tmp/sorted_int64_lookup_results.csv
/tmp/sorted_int64_lookup_benchmark --small-index-only > /tmp/sorted_int64_small_index.csv

c++ -O1 -g -std=c++20 -fsanitize=address,undefined \
  -fno-omit-frame-pointer -I internal/core/src \
  internal/core/benchmark/SortedInt64LookupBenchmark.cpp \
  -o /tmp/sorted_int64_lookup_sanitized
/tmp/sorted_int64_lookup_sanitized --verify-only
```

The executable checks 2,400 IN/NOT IN cases against scan and baseline results,
including empty pointer ranges. The main sweep measures 576 scenarios across
4,096/262,144 rows, varying cardinality, query length, repetition, and hit rate.
The small-index sweep adds 144 scenarios with 1–4,096 rows and lists up to
65,536 terms. Every timed scenario checks correctness before measurement.

Samples use process CPU time, with a minimum of 5 ms per sample and the median
of five alternating-order samples after warmup. Timing includes query
preparation, bitmap allocation or cloning, matching writes, and a bitmap count.
Index construction and correctness checks are outside timing.

Recorded results with the N > M guard on Apple M1 Pro, macOS 26.5.2,
Apple Clang 21.0.0, with `-O3 -DNDEBUG`. The main sweep below includes both
batched and fallback paths; query length alone does not select the batch path.

| List size | Minimum speedup | Median speedup |
| ---: | ---: | ---: |
| 1 | 0.747x | 0.990x |
| 8 | 0.692x | 0.988x |
| 64 | 0.754x | 1.005x |
| 127 | 0.822x | 0.991x |
| 128 | 1.526x | 4.357x |
| 129 | 1.491x | 4.524x |
| 512 | 1.744x | 5.468x |
| 4,096 | 0.928x | 1.629x |

Speedup is baseline CPU time divided by new CPU time; values below 1 indicate
regressions. The worst main-sweep ratio was 0.692x; the worst small-index ratio
was 0.835x. Avoiding sorting when N > M does not eliminate all measured
slowdowns. These scalar-bitset component results do not establish server-level
gains; Linux index-level performance remains unmeasured.

The standalone ASan/UBSan checks passed all 2,400 comparisons. Recorded runs of
the 576 main and 144 small-index scenarios also passed their scan and baseline
checks. Real index/reload tests have not been compiled or run locally; validation
is pending CI after the local dependency bootstrap was stopped. Formatting was
checked with Xcode's clang-format 21; the project's clang-format 15 check remains
pending.

## Rejected Alternatives

- Batch every query: allocating and sorting small lists adds avoidable overhead.
  Always sorting large lists also regresses tiny-index lookups, motivating the
  N > M guard.
- Probe each term independently: this retains repeated searches and duplicate
  bitmap writes for workloads that benefit from batching.
- Reuse `GrowingOffsetMapping::GallopLowerBound` directly: it is private and
  coupled to chunked offset storage. The new helper operates on sorted index
  entries using standard-library algorithms without adding a dependency.

## References

- [Issue #53853: IX01](https://github.com/milvus-io/milvus/issues/53853)
- [SortedIndexReader refactor #53585](https://github.com/milvus-io/milvus/pull/53585)
- [Async packed scalar index loading MEP](20260907-async-packed-scalar-index-loading.md)
