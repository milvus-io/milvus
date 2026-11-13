# Bit-sliced index (BSI) kernel

`BitSlicedIndex<T>` is an in-memory bit-sliced index. It evaluates equality,
membership and range predicates on signed integer values exactly, using only
bitmap operations. Only `BitSlicedIndex<int32_t>` (alias `BitSlicedIndexInt32`)
is instantiated and tested. For the design, its costs and the benchmark
results, see
[the design doc](../../../../../../docs/design-docs/design_docs/20261013-int32-bit-sliced-index-kernel.md).

This is a standalone kernel. It is not registered as an index type, and it has
no loader, serialization or growing-segment support, so no user query reaches
it yet.

## Usage

Build the index from a complete `ScalarBuildInput`. The input is borrowed only
during construction. Then query it through the `IScalarPredicateReader<T>` and
`INullReader` mixins:

```cpp
#include "index/scalar/bsi/BitSlicedIndex.h"

using namespace milvus::index;

std::vector<int32_t> values{5, 2, 7, 0, 2};          // row 3 is NULL
bool valid[] = {true, true, true, false, true};
const ScalarBuildBatch<int32_t> batch{
    std::span<const int32_t>(values),
    milvus::ValidityView::FromExpanded(valid)};      // empty view = no NULLs
const BitSlicedIndexInt32 index(
    ScalarBuildInput<int32_t>{std::span(&batch, 1)});

TargetBitmap eq = index.Range(7, CompareOp::Equal);              // 00100
TargetBitmap le = index.Range(4, CompareOp::LessEqual);          // 01001
TargetBitmap between = index.Range(2, true, 5, false);           // 01001
const int32_t keys[] = {2, 5};
TargetBitmap in = index.In(2, keys);                             // 11001
TargetBitmap nulls = index.IsNull();                             // 00010
```

The input may consist of several batches. Values in rows that are NULL are
ignored. A non-empty validity view must cover the batch's values, as the
`ScalarBuildInput` contract requires.

## Semantics

- Every result has `Count()` bits (all rows, including NULL rows), and 1 means
  a hit.
- NULL rows never match a predicate, including `!=` and `NotIn`. Use
  `IsNull()` / `IsNotNull()` to select them.
- Bounds and constants may be any `int32_t`, including `INT32_MIN` and
  `INT32_MAX`. An empty range, such as `lo > hi`, returns all zeros.
- `In` / `NotIn` with `n > 0` and `values == nullptr` raise `UnexpectedError`.
  An unknown `CompareOp` raises `OpTypeInvalid`.
- The index is immutable after construction, and const methods are safe to
  call concurrently.

## Size and cost

The index stores `k = bit_width(max - min)` slices plus an existence bitmap,
for a total of `(k + 1) * N / 8` bytes. `MemoryUsage()` reports it. A narrow
value range therefore stores less than the raw column: values in `[0, 1000)`
need 10 slices, about 1/3 of `4N` bytes. A full-range `int32` column needs 32
slices, about the same as the raw column.

- **Point and one-sided range queries:** about `k` to `3k` bitmap passes.
- **Two-sided ranges:** about twice that.
- **`In` with a few key runs:** one walk per run.
- **`In` with many key runs:** a fixed-cost decode instead (see
  `kInWalkSliceBudget` in the header).

Which queries are faster or slower than scanning the raw column depends on `k`
and on selectivity; the benchmark below measures this.

## Tests and benchmark

The tests are in `BitSlicedIndexInt32Test.cpp`, which is part of the
`index_tests` binary. To run them:

```bash
cmake_build/unittest/index_tests --gtest_filter='BitSlicedIndexInt32Test.*'
```

The benchmark is in `internal/core/benchmark/BitSlicedIndexBenchmark.cpp`
and is built as `bit_sliced_index_benchmark` along with the unit tests. For
each dataset it reports build time, index size and per-query latency, next to
the raw-column scan that Milvus uses for an unindexed field. It checks every
query result against that scan, and aborts if any result differs. To run it:

```bash
cmake_build/benchmark/bit_sliced_index_benchmark            # 1M and 8M rows
cmake_build/benchmark/bit_sliced_index_benchmark --quick    # 64K rows
cmake_build/benchmark/bit_sliced_index_benchmark --rows=N
```

The output is CSV, one row per (dataset, query).
