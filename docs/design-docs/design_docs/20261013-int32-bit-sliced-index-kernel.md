# MEP: Int32 bit-sliced index kernel

- **Created:** 2026-10-13
- **Author(s):** @IceCoooola
- **Status:** Under Review
- **Component:** Index (segcore, `internal/core/src/index/scalar/bsi/`)
- **Related Issues:** #53831 (this stage); #53832 (serialization, next stage)
- **Released:** N/A

## Summary

Add an in-memory bit-sliced index (BSI) kernel for signed 32-bit integers that
evaluates equality, membership and range predicates exactly, using only bitmap
operations. This stage covers only the kernel: it adds no index type, public
API, registry entry, serialization or load path.

## Scope

In scope:

- `BitSlicedIndexInt32`, built from a complete `ScalarBuildInput<int32_t>`.
- `IScalarPredicateReader<int32_t>`: `In`, `NotIn`, `Range(value, op)` for all
  six `CompareOp`s, and `Range(lo, lo_inc, hi, hi_inc)`.
- `INullReader`: `IsNull`, `IsNotNull`.
- Randomized tests that compare every operation against a full scan.
- A benchmark of build time, size and query latency against the raw-column
  scan, and usage notes.

Out of scope (later stages):

- Other integer widths and other types.
- Serialization and reload (#53832).
- `IIndexReaderBase`, `ReaderCaps`, the loader/builder registry, `IndexType`
  strings, Hybrid selection, and anything that exposes this to users.
- Value lookup (`IScalarValueReader`), growing segments, mmap.

The kernel implements only the two pure query mixins. Neither one inherits
`IIndexReaderBase` (see `index/contracts/README.md`), so the kernel stays out
of the lifecycle while keeping the same query signatures that the integration
stage will expose.

## Motivation

A BSI stores one bitmap per bit of the (encoded) value. With `k` slices, any
comparison against a constant takes `O(k)` word-parallel bitmap operations over
the rows, no matter how many distinct values there are or how selective the
range is. Range cost in the existing families scales differently:

- Bitmap index: unions one posting per distinct value inside the range, so
  wide ranges over high-cardinality data get expensive.
- Sorted index: binary search, then sets one bit per matching row.

That makes a BSI a plausible option for high-cardinality integer columns
filtered by ranges. **No speedup is claimed.** The benchmark in the Test Plan
measures build time, size and latency against a scan of the raw column,
regressions included. A comparison with the other index families is future
work.

## Public Interfaces

None that users or other components can observe. This PR changes no API,
proto, SDK, configuration, metric or query behaviour.

Internally it adds:

- **The class template `milvus::index::BitSlicedIndex<T>`**
  (`internal/core/src/index/scalar/bsi/`).
  - It implements the existing query mixins `IScalarPredicateReader<T>` and
    `INullReader`.
  - It is constrained to the types that the `BitSlicedIndexValue` concept
    accepts, which are signed integers of at most 32 bits.
  - Only `BitSlicedIndex<int32_t>`, with the alias `BitSlicedIndexInt32`, is
    instantiated and tested.
  - It is not registered as an index type, and it has no build, load or
    serialization entry point.
- **Usage notes** in `src/index/scalar/bsi/README.md`.
- **A developer benchmark,** `bit_sliced_index_benchmark`
  (`internal/core/benchmark/`). It is built only together with the unit tests
  and is not installed into release images.

## Design Details

### Interface for other integer widths

The issue asks for an interface that can later support other integer widths.
The kernel is a template, `BitSlicedIndex<T>`, so another width does not need
another class. Its arithmetic is written for every type that the
`BitSlicedIndexValue` concept accepts:

- Offsets `v - min` are kept in `uint32_t`.
- Bounds and the `+/- 1` that turns an exclusive bound into an inclusive one
  are computed in `int64_t`.
- At most `kMaxSlices = 8 * sizeof(T)` slices are stored.

Both stay overflow-free for signed integers of up to 32 bits. The concept
rejects `int64_t`, which would need a wider offset and bound type, as well as
unsigned types, `bool` and floating point, at compile time. The test pins this
with `static_assert`s.

Only `int32_t` is explicitly instantiated, in `BitSlicedIndex.cpp`. Adding
`int8_t` or `int16_t` means adding one instantiation and its tests. That is
left to the all-types stage, so this PR makes no claim about those widths.

### Encoding

The build makes two passes over the borrowed input; the input is not retained.

1. Count the rows and compute `min` and `max` over the valid rows.
2. For each valid row with value `v`, set the row's bit in the existence
   bitmap (EBM), then set bit `i` in slice `i` for every set bit of
   `u = (int64_t)v - (int64_t)min`. The bits for each 64-row word of the EBM
   and of every slice are assembled locally and stored as whole words, so the
   build makes no per-bit `set()` calls scattered over `k + 1` bitmaps.

The number of slices is `k = bit_width(max - min)`, which is in `[0, 32]`.

Why store the offset from `min`:

- **Signed order becomes unsigned order.** `u` lies in `[0, 2^32)` and is
  monotonic in `v`, so a single unsigned comparison algorithm covers negative
  values, zero and `INT32_MIN` without a separate sign slice. The subtraction
  is done in `int64_t`, so it cannot overflow.
- **Fewer slices for narrow ranges.** High slices that would be constant are
  dropped. For example, values in `[-50, 50]` need 7 slices, not 32. A column
  with a single distinct value needs none, and its equality result is just the
  EBM.

NULL rows are 0 in the EBM and in every slice. For NULL rows, the value in the
input buffer is ignored.

Precondition: a non-empty validity view must cover `batch.values.size()` rows,
as the `ScalarBuildInput` contract requires. `ValidityView` is a non-owning
view without a length, so the kernel cannot check this. The batch producer is
responsible for it, as with the other scalar builders.

Memory is `(k + 1) * ceil(N / 64) * 8` bytes, where `N` counts all rows
including NULL rows. `MemoryUsage()` reports this. For `k = 32` that is a
little more than the raw `int32` column. The saving comes only from narrow
ranges.

### Query evaluation

Every result is a `TargetBitmap` with `Count()` bits, where 1 means a hit.
NULL rows are never a hit, because every walk starts from a subset of the EBM,
and the complement operators (`NotIn`, `!=`) are computed as `EBM - hits`. This
matches `BitmapIndexReader` and `SortedIndexReader`.

**Constant handling.** Constants and bounds are converted to `int64_t` first.
That way an exclusive bound becomes an inclusive one without overflow
(`x < c` is `x <= c - 1`, `x > c` is `x >= c + 1`), even at `INT32_MIN` and
`INT32_MAX`. A bound outside `[min, max]` short-circuits to the empty bitmap or
to a copy of the EBM. Only constants inside `[min, max]` turn into an offset
and reach a slice walk.

**Slice walks** (O'Neil and Quass, *Improved Query Performance with Variant
Indexes*, SIGMOD 1997). Each walk takes a candidate set (a subset of the EBM),
starts with `eq = candidates` and visits slices from the most significant down.
`eq` holds the candidate rows whose higher bits equal the constant's. A walk
returns the candidates that satisfy its predicate, so walks can be chained.

| Walk | Constant bit is 1 | Constant bit is 0 | Finish |
|---|---|---|---|
| `EqualOffset` | `eq &= S[i]` | `eq -= S[i]` | `eq` |
| `LessEqualOffset` | `lt \|= eq; eq &= S[i]; lt -= eq` | `eq -= S[i]` | `lt \| eq` |
| `GreaterEqualOffset` | `eq &= S[i]` | `gt \|= eq; eq -= S[i]; gt -= eq` | `gt \| eq` |

Because `lt` (or `gt`) and `eq` are always disjoint, the textbook update
`lt |= eq & ~S[i]` can be written as the three in-place operations above, with
no temporary bitmap. Each walk also stops early once the remaining low bits of
the constant are all 1 (for `<=`) or all 0 (for `>=`). From that point every
row still in `eq` qualifies.

**Operators.**

- `==`: `EqualOffset`.
- `!=`: `EBM - (==)`.
- `<`, `<=`, `>`, `>=`: one `LessEqualOffset` or `GreaterEqualOffset` walk.
- Two-sided range: clamp `[lo, hi]` to `[min, max]`, then:
  - empty if `lo > hi`;
  - equality if `lo == hi`;
  - a single walk if one side reaches `min` or `max`;
  - otherwise a chained walk:
    `LessEqualOffset(hi, GreaterEqualOffset(lo, EBM))`. The `<=` walk starts
    from the `>=` result, so a two-sided range copies the EBM once and needs no
    final AND.
- `In`: keep the keys that lie in `[min, max]` as offsets, sort them, remove
  duplicates, and group consecutive offsets into runs. Then pick a strategy:
  - **Walk** while `runs * k <= kInWalkSliceBudget` (512). This means one
    walk per run: `Between()` sends a single-key run to the equality walk and
    a longer run to the chained range walk. The cost is about `runs * k`
    bitmap passes.
  - **Decode** for more runs. The offsets are decoded one 64-row word at a
    time:
    1. Load that word from each of the `k` slices into a 64x64 bit block.
    2. Transpose the block with a branchless bit-matrix transpose (six
       rounds of masked swaps), so that entry `r` becomes row `r`'s offset.
    3. Probe each valid row, and write the result word.

    A word with no valid rows is skipped. Probes use a dense bitmap over
    offsets when `max - min < kMaxDenseKeySpan` (2^20, so at most 128 KiB).
    Otherwise they use an `ankerl::unordered_dense` set of the keys, the same
    container `TermExpr` uses for `IN` on raw data. The cost is fixed per 64
    rows and does not depend on the number of keys. Extra memory does not grow
    with the row count.

  The switch is a slice budget, not a run count, because walk cost grows with
  `runs * k`, while decode cost is roughly fixed for a given `k`. The budget
  comes from measurements with `bit_sliced_index_benchmark`, comparing
  always-walk and always-decode builds at 1M rows. Walk cost was about 9 us
  per (run x slice). Decode cost was about 4 ms at `k = 10` and 6 to 7 ms at
  `k = 32`. The crossover was therefore about `runs * k = 470` at `k = 10` and
  `780` at `k = 32`, and 512 lies between them. At `k = 10`, `IN` therefore
  walks up to 51 runs, slightly past the measured crossover of about 47. At
  `k = 32` it switches to decode at 17 runs, before the crossover of about 24.
  These figures came from one local Linux/aarch64 run, and the crossover may
  differ on other hardware.

  `MaxInRunWalks(k)` exposes the switch as a run count for a given `k`, so the
  tests derive their boundary cases from the same function that the code uses.
- `NotIn`: `EBM - In`.
- `IsNull` / `IsNotNull`: `~EBM` / `EBM`.

Cost of each operation:

| Operation | Bitmap passes over `N` bits |
|---|---|
| `==`, `!=` | about `k` |
| One-sided range | about `3k` at most |
| Two-sided range | about `6k` at most (two chained walks, one EBM copy) |
| `In`, walk (`r * k <= 512`) | about `r * k`, after an `O(n log n)` sort of the `n` keys |
| `In`, decode | one read of each slice word and one 64x64 transpose per 64 rows, plus one `O(1)` probe per valid row |
| `IsNull`, `IsNotNull`, out-of-range constants | `O(1)` passes (an EBM copy or an empty bitmap) |

Bitmap operations use `TargetBitmap` (`CustomBitset`), so they get the
existing SIMD dispatch.

### Errors

The only rejected input is `In` or `NotIn` called with `n > 0` and
`values == nullptr`. That is an internal caller bug, so it raises
`AssertInfo`, as `BitmapIndexReader::In` does. An unknown `CompareOp` raises
`ThrowInfo(OpTypeInvalid, ...)`, as `InvertedIndexReader` and
`MarisaIndexReader` do for unknown comparison operators. Not every reader
rejects it: `BitmapIndexReader::Range` falls through its switch and returns
every valid row. Both paths are covered by `RejectsInvalidArguments`, which
pins the error codes. The build rejects nothing: every `int32_t` value and
every validity pattern is valid (see the precondition under Encoding).

## Compatibility, Deprecation, and Migration Plan

Nothing changes in persisted formats, public interfaces, configuration or
query behaviour, and nothing is deprecated. The new code is reachable only from
its unit test, so there is no upgrade, rollback or migration impact. Rolling
back this PR removes only the new directory and its test registration.

## Test Plan

All verification is through unit tests, since the kernel is not reachable
from any integration or end-to-end path yet.

`index/scalar/bsi/BitSlicedIndexInt32Test.cpp` is registered in
`index_tests`.

Deterministic cases:

- Empty input, all-NULL input, and a single distinct value (zero slices).
- `INT32_MIN`, `INT32_MIN + 1`, negative values, zero, `INT32_MAX - 1` and
  `INT32_MAX`, with every operator and every `lo`/`hi` pair from a set of edge
  constants.
- Exclusive bounds at the type extremes and inverted ranges.
- Slice trimming, and multiple batches, including empty batches and a batch
  that passes an empty validity view.
- Expanded, packed and bit-offset packed (`Subview`) validity encodings.
- `In` with duplicate keys, runs of consecutive keys and out-of-range keys.
- Both `In` strategies, on each side of the switch
  (`InWalkAndDecodeStrategiesMatchScan`). For the index under test, the cases
  are derived from `T = MaxInRunWalks(SliceCount())`:
  - exact in-range run counts of 1, `T - 1`, `T` (the last walk), `T + 1`
    (the first decode), `T + 2` and 1001;
  - two cases that mix isolated keys with one long run, for `T` and `T + 1`
    runs.

  They run on a narrow index (11 slices, `T = 46`, where decode probes a dense
  key bitmap) and a wide one (32 slices, `T = 16`, where decode probes a key
  hash set). Each case asserts its run count independently, so the boundary
  moves with the budget and cannot drift when the key construction changes.
  Out-of-range keys and duplicates are added without changing the count.
- Both sides of the dense / hash-set probe switch in the decode strategy, with
  `max - min` equal to `kMaxDenseKeySpan - 1` and to `kMaxDenseKeySpan`
  (`InDecodeProbesAgreeAtDenseSpanLimit`).
- The error paths, with their error codes pinned: a null key pointer with
  `n > 0` raises `UnexpectedError` (from `AssertInfo`), and an invalid
  `CompareOp` raises `OpTypeInvalid`.
- A partition invariant: `<`, `==`, `>` and `IS NULL` together cover every row
  exactly once.
- Exact `MemoryUsage`.

Randomized cases cover 6 value distributions, 3 NULL rates, 6 row counts and 3
validity encodings, with fixed seeds:

- **Distributions:** full `int32` range, around zero, clustered at the
  extremes, near `INT32_MAX`, near `INT32_MIN`, and `±2^k ± 1`.
- **NULL rates:** 0, 0.3 and 0.95.
- **Row counts:** 1, 63, 64, 65, 1000 and 4097, so word boundaries are hit.
- **Batches:** random sizes, including empty batches.

Constants are drawn mostly from stored values and their neighbours, from the
observed `min - 1` and `max + 1`, and from the type extremes. Each case checks
the following against a scan:

- all six operators;
- all four inclusive/exclusive combinations of a two-sided range;
- `In` and `NotIn`, with lists of 0 to 120 keys;
- `IsNull` and `IsNotNull`.

The check compares the result bit by bit and also verifies `count()`, which
catches stray bits past `size()`.

The unit tests above run in `index_tests` and are the evidence this PR can
reproduce. CI's C++ coverage job measures their coverage.

The author also ran the following checks locally. They are not part of this
PR or of CI, so treat them as unverified:

- a standalone build of the kernel and its test against the real `bitset`
  sources, run under AddressSanitizer and UndefinedBehaviorSanitizer;
- gcov line coverage of `BitSlicedIndex.cpp` in that build;
- fault injection: deliberate one-line bugs in the build, the walks, both
  `In` strategies and probes, the error paths and the NULL handling, checking
  that the suite fails for each one.

### Benchmark

As the issue requires, `bit_sliced_index_benchmark` reports build time, size
and query latency. Each query is compared with the path Milvus takes for an
**unindexed** `int32` field:

- comparisons and ranges use a SIMD scan of the raw column
  (`TargetBitmap::inplace_compare_val` / `inplace_within_range_val`, as in
  `UnaryExpr` / `BinaryRangeExpr`);
- `IN` uses a per-row `ankerl::unordered_dense` set probe (as in `TermExpr`).

Both are ANDed with the validity bitmap. Before timing a query, the benchmark
checks that the index result equals the scan result bit for bit, and it aborts
on any difference.

The datasets are:

- **Value shapes:** "narrow" (uniform in `[0, 1000)`, `k = 10`) and "wide"
  (uniform over all of `int32`, `k = 32`);
- **NULL rates:** 0 and 0.1;
- **Row counts:** 1M and 8M.

The queries are:

- `==` and `!=` at the median;
- `<` at the 1st percentile and at the median;
- `>=` at the 99th percentile;
- a two-sided range from the 45th to the 55th percentile;
- `IN` with 8, 32, 128 and 512 keys spread over the distribution;
- `NOT IN` with 8 keys.

The index and the scan for a query are timed in interleaved trials (index,
scan, index, scan, ...). A transient slowdown of the machine therefore affects
both sides, not one. Each reported latency is the median of 7 trials per side,
and each trial averages over at least 30 ms. Build time is the median of 5
builds.

### Results

Measured on Apple M2 (8 cores, 24 GB), macOS 26.5.1, Homebrew clang version
19.1.7, `Release` build, by running `bit_sliced_index_benchmark` with its
default settings. Times are wall-clock. A "scan / index" ratio above 1 means the
index is faster; below 1 it is a regression against the scan.

**Build time and size.** "raw" is the `int32` column plus a validity bitmap.

| Dataset | Rows | NULL rate | Slices | Build (ms) | Index (MiB) | Raw (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| narrow | 1,048,576 | 0 | 10 | 9.8 | 1.4 | 4.1 |
| narrow | 1,048,576 | 0.1 | 10 | 10.2 | 1.4 | 4.1 |
| wide | 1,048,576 | 0 | 32 | 18.7 | 4.1 | 4.1 |
| wide | 1,048,576 | 0.1 | 32 | 19.4 | 4.1 | 4.1 |
| narrow | 8,388,608 | 0 | 10 | 71.9 | 11.0 | 33.0 |
| narrow | 8,388,608 | 0.1 | 10 | 82.1 | 11.0 | 33.0 |
| wide | 8,388,608 | 0 | 32 | 155.7 | 33.0 | 33.0 |
| wide | 8,388,608 | 0.1 | 32 | 159.8 | 33.0 | 33.0 |

**Query latency, 8,388,608 rows, NULL rate 0.1.** Keys and bounds are
percentiles of the stored values.

| Query | narrow: index (us) | narrow: scan (us) | narrow: scan / index | wide: index (us) | wide: scan (us) | wide: scan / index |
|---|---:|---:|---:|---:|---:|---:|
| `== p50` | 294 | 832 | 2.83 | 903 | 831 | 0.92 |
| `!= p50` | 338 | 833 | 2.47 | 947 | 832 | 0.88 |
| `< p1` | 383 | 831 | 2.17 | 1,457 | 829 | 0.57 |
| `< p50` | 545 | 826 | 1.52 | 1,485 | 828 | 0.56 |
| `>= p99` | 353 | 827 | 2.34 | 1,541 | 828 | 0.54 |
| `[p45, p55]` | 1,106 | 1,126 | 1.02 | 3,556 | 1,125 | 0.32 |
| `IN` 8 keys | 2,586 | 19,302 | 7.46 | 7,459 | 19,142 | 2.57 |
| `IN` 32 keys | 10,337 | 19,956 | 1.93 | 51,858 | 21,960 | 0.42 |
| `IN` 128 keys | 25,859 | 27,082 | 1.05 | 44,864 | 21,949 | 0.49 |
| `IN` 512 keys | 25,867 | 52,527 | 2.03 | 45,500 | 21,123 | 0.46 |
| `NOT IN` 8 keys | 2,623 | 19,237 | 7.33 | 7,513 | 19,135 | 2.55 |

**Scan / index ratio, min to max over all 8 datasets** (both row counts, NULL
rates 0 and 0.1):

| Query | narrow (10 slices) | wide (32 slices) |
|---|---:|---:|
| `== p50` | 2.74 to 2.83 | 0.89 to 0.92 |
| `!= p50` | 2.41 to 2.47 | 0.85 to 0.88 |
| `< p1` | 2.11 to 2.27 | 0.43 to 0.57 |
| `< p50` | 1.23 to 1.52 | 0.42 to 0.56 |
| `>= p99` | 2.27 to 2.36 | 0.49 to 0.58 |
| `[p45, p55]` | 0.93 to 1.02 | 0.30 to 0.34 |
| `IN` 8 keys | 7.36 to 7.46 | 2.53 to 4.06 |
| `IN` 32 keys | 1.92 to 1.93 | 0.42 to 0.58 |
| `IN` 128 keys | 1.01 to 1.05 | 0.40 to 0.51 |
| `IN` 512 keys | 1.96 to 2.03 | 0.44 to 0.49 |
| `NOT IN` 8 keys | 7.23 to 7.34 | 2.52 to 4.03 |

The walk/decode crossover implied by this run, using walk cost per run from `IN`
8 and decode cost from `IN` 128 and 512, is about `runs * k = 806` to `812` at
`k = 10` and `runs * k = 1,435` to `1,601` at `k = 32`. `kInWalkSliceBudget`
(512) was set from a local Linux/aarch64 run, where the crossover was about 470
at `k = 10` and 780 at `k = 32`. Where this run's crossover is higher, 512
switches to decode earlier than optimal; the budget is not tuned per machine.

These numbers describe this kernel on one machine; they are not a claim
about end-to-end query performance. Where the kernel is slower than the scan,
that is reported as a regression, not hidden.

In short, on this machine:

- **Narrow value range (10 slices).** The index pays off. It is 3 times
  smaller than the raw column. Point and one-sided comparisons are 1.2 to 2.8
  times faster than the SIMD scan, and the two-sided range is on par. `IN` /
  `NOT IN` with 8 keys are about 7 times faster.
- **Full-range `int32` column (32 slices).** The index is the same size as the
  raw column, and the scan is faster for every comparison (0.30 to 0.92) and
  for `IN` with 32 or more keys (0.40 to 0.58). Short `IN` lists still favour
  the index: 8 keys are 2.5 to 4.1 times faster.

## Rejected Alternatives

- **Always store 32 slices, with the sign bit flipped (`v ^ 0x80000000`).**
  This gives the same unsigned ordering, but it always stores 32 slices. The
  offset from `min` gives the same ordering and drops the slices that would be
  constant, at the cost of storing `min`.
- **Two's-complement slices plus special handling of the sign slice.** This
  needs separate comparison logic for the sign bit. The offset encoding makes
  every walk a plain unsigned comparison.
- **Compressed slices (for example Roaring).** Dense `TargetBitmap` slices
  match the result type that the reader contract requires, and they use the
  existing SIMD bitmap operations. Compression should be decided with a
  benchmark, together with serialization (#53832).
- **One equality walk per `IN` key.** This costs `O(n * k)` full-bitmap passes
  and one EBM copy per key, so the cost grows without bound in the list length
  `n`. Run grouping plus the decode strategy bound it instead.
- **Decoding every row's offset into an `N`-entry array.** This is simpler,
  but it allocates `4N` bytes per query (40 MB for 10M rows) and makes a
  separate pass over each slice. Decoding one word at a time needs no
  row-proportional memory.
- **Decoding offsets one set slice bit at a time.** For `k = 32` this costs
  about 1000 operations per 64 rows. At 1M rows the transpose was 2 to 3 times
  faster.
- **A fixed run-count threshold for `In`.** Walk cost scales with `runs * k`,
  so any single threshold is wrong for some `k`. The budget follows `k`.
- **Binary search over the keys in the decode strategy.** This needs about
  `log2(n)` unpredictable branches per row. The hash set matches what
  `TermExpr` already uses.
- **Two-sided range as `GE(lo) & LE(hi)`.** This needs two EBM copies and a
  final AND. The chained walk produces the same result with one copy.
- **Checking the validity length in the kernel.** `ValidityView` has no length
  to check against. This would require a contract change for all scalar
  builders, which is outside this stage.
- **Implementing the full reader lifecycle now** (`IIndexReaderBase`, caps,
  registry). The issue stages the work, and the kernel only needs the query
  mixins to be testable.

## References

- Issue #53831: Int32 bit-sliced index kernel (this stage).
- Issue #53832: serialize and reload the bit-sliced index (next stage).
- P. O'Neil and D. Quass, *Improved Query Performance with Variant Indexes*,
  SIGMOD 1997.
- `internal/core/src/index/contracts/README.md`: reader, bitmap and NULL
  contracts.
- `internal/core/src/index/scalar/README.md`: scalar index families.
- `internal/core/src/index/scalar/bitmap/BitmapIndexReader.cpp` and
  `internal/core/src/index/scalar/sort/SortedIndexReader.cpp`: existing
  readers whose NULL semantics this kernel matches.

## Future Work

- **#53832:** serialize and reload the kernel through `IndexEntryWriter` and
  `IndexEntryReader`. The persisted state is `(num_rows, min, max, EBM,
  slices[0..k))`. `k` is `bit_width(max - min)`, and the valid count is the
  popcount of the EBM. `max` must be stored, because `k` only gives an upper
  bound on it.
- The remaining integer widths: instantiate and test `int8_t` / `int16_t`.
  `int64_t` needs a wider offset and bound type.
- Reader and lifecycle integration: `IIndexReaderBase`, caps, the registry
  entry and the index type.
- Extend the benchmark to `BITMAP` and `STL_SORT`.
- Optional: compressed slices (for example Roaring) for sparse slices.
