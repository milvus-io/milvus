# ScalarPredicateReader 契约覆盖审计

Baseline: `origin/master` pinned at `a876f471053edb2f68a06a9894afee9810ea7906`.
Validation mode: static source audit only. No configuration, compilation, test, or Milvus binary was run.

## 被测契约

`internal/core/src/index/contracts/query/IScalarPredicateReader.h` exposes:

- `In(size_t, const T*)`
- `NotIn(size_t, const T*)`
- unary `Range(const T&, CompareOp)` for Equal, NotEqual, GreaterThan, GreaterEqual, LessThan, LessEqual
- interval `Range(lo, lo_inc, hi, hi_inc)`

All results are row-domain bitmaps of `Count()` bits. Predicate results reject NULL rows, including negative predicates (`NotIn`, `NotEqual`).

Current typed registrations support bool, int8, int16, int32, int64, float, double, and string_view for bitmap, sort, inverted, and hybrid builders. Marisa supports string_view. The contract tests select actual backends through `ReaderCaps::predicate`; they do not hard-code an implementation expectation.

## 映射到声明式用例的 master 基线

### ScalarIndexTest.cpp

- `TypedScalarIndexTest.In` and `.NotIn`, instantiated by `ArithmeticCheck` for int8/int16/int32/int64/float/double: mapped to `InAllValues`, `InMissingOnly`, `NotInAllValues`, and `NotInMissingOnly` for every current predicate backend. The new oracle checks every row rather than only `any()`/`none()`.
- `TypedScalarIndexTest.Range`: mapped to all unary comparison and interval cases. The new suite includes Equal and NotEqual, which the master helper omitted.
- `TypedScalarIndexTest.Codec`: the new `ReaderBackend` always serializes the built Artifact and opens a Reader, so every declarative case covers the round trip. Lifecycle/upload mechanics remain outside this query contract.
- `ScalarTest.test_function_In`: its N=1000/cardinality 10,20,100 and N=10000/cardinality 1001,2000 bitmap-versus-sort comparisons for int8/int16/int32/int64/float/double/string are represented by exact-oracle small typed cases, `HundredThousandRows`, and a type-scoped `TenThousandHighCardinality` descriptor for every master type. The int8 descriptor uses all 200 representable values in `[-100,99]`; the other numeric/string descriptors use 2,000 distinct values. Large datasets use representative operations instead of multiplying every operation over every size/cardinality pair.
- `ScalarTest.test_function_range`: its low/high-cardinality LessThan and `[lo, hi)` bitmap-versus-sort comparisons for numeric types are mapped to `UnaryLessThan`, `IntervalClosedOpen`, `IntervalLargeRowCount`, and typed `UnaryHighCardinality`/`IntervalHighCardinality` cases for int8/int16/int32/int64/float/double.
- `Count`, `Constructor`, `HasRawData`, and `Reverse` are not ScalarPredicateReader operations. `RunFilterCase` still checks Count and row domain as invariants; construction/raw-data/value lookup remain other contracts.

### BoolIndexTest.cpp

- `BoolIndexTest.In` and `.NotIn` over all-true, all-false, and alternating values map to bool `PredicateAllEqual` (four true rows), `PredicateAllFalse` (four false rows), and mixed `PredicateEdges` cases. Both keys are queried against each constant dataset, so all-hit and no-hit posting distributions are explicit for In and NotIn.
- `BoolIndexTest.Codec` query checks are covered by the mandatory Artifact serialize/open path for each case.
- Master did not test bool Range. Current bitmap, sort, and inverted implementations instantiate bool and expose the full typed predicate contract, so the new suite adds all six unary operators and all interval shapes using the order `false < true`.

### BitmapIndexTest.cpp

- `BitmapIndexTest.INFuncTest`, `.NotINFuncTest`, and `.CompareValFuncTest` for `BitmapE2ECheck` (10,000 rows, cardinality 30; int8/int16/int32/int64/string) map to the typed edge membership and six unary cases.
- `BitmapIndexTestV2` / `BitmapIndexE2ECheck_HighCardinality` adds 10,000 rows/cardinality 2,000 and four interval inclusivity variants. It maps to `TenThousandHighCardinality` plus `IntervalOpenOpen`, `IntervalOpenClosed`, `IntervalClosedOpen`, and `IntervalClosedClosed`.
- `BitmapIndexTestV3` / mmap non-nullable maps to the ordinary bitmap mmap backend profile and the same query matrix.
- `BitmapIndexTestV4` / mmap nullable maps to the bitmap mmap profile with `PredicateEdges`, `PredicateAllNull`, and NULL-rejecting In/NotIn/unary/interval cases.
- `BitmapIndexTestV5` (missing binlog rows become NULL) and `BitmapIndexTestV6` (missing rows receive a default) test upstream build materialization, not a Reader input form. Their already-materialized predicate rules map to nullable/all-valid cases; missing-row synthesis itself is deferred to a builder/materializer contract.
- IsNull/IsNotNull and PatternMatch named variants belong to NullReader and PatternMatchReader suites.

### ScalarIndexSortTest.cpp

- `StlSortIndexTest.TestIn` checks exact row positions in heap and mmap loads; mapped to every `In*` case on sort heap/mmap profiles.
- `StlSortIndexTest.TestRange` checks `[3,7]` and a no-hit interval in heap and mmap loads; mapped to primary interval, reversed/equal/no-hit, and mmap profile cases.
- `MmapByteSizeCountsValidBitsetOnce` is resource accounting, outside the predicate contract.

### StringIndexTest.cpp (Marisa trie)

- `StringIndexMarisaTest.In`, `.InHasNull`, `.NotIn`, and `.NotInHasNull` map to string membership, nullable stored-value, all-valid, and all-null cases.
- `StringIndexMarisaTest.Range` maps to all six unary comparisons, all four interval inclusivity combinations, equal bounds, reversed bounds, and endpoints.
- `StringIndexMarisaTest.Query` repeats direct predicate behavior through the legacy plan wrapper; execution-plan routing is deferred, while the underlying direct semantics are covered.
- `StringIndexMarisaTest.Codec` and `.BaseIndexCodec` query checks are covered by the Artifact serialize/open path. `.UnifiedCodecRecreatesMissingLocalChunkDir` is lifecycle/filesystem behavior and is deferred.
- PrefixMatch and null-query named tests belong to other reader contracts.

### StringIndexSortTest.cpp

- `InMemory`, `InMmap`, and `NotInMemory` map to string membership on heap/mmap sort profiles, including all, subset, missing, repeated, empty-string, prefix-related, UTF-8, long-string, and embedded-NUL inputs.
- `RangeMemory` and `RangeBetweenMemory` map to six unary and all interval shapes.
- `SerializeDeserializeMemory`, `SerializeDeserializeMmap`, `MmapLoadAfterSerialize`, `LoadWithoutAssembleMmap`, `StringIndexSortBuildAndSearch`, `StringIndexSortWithNulls`, and `StringIndexSortSerialization` repeat the same predicates after load; the shared factory serializes and opens every case and adds explicit mmap profiles.
- `LoadRejectsOutOfRangePostingListRowId` is corruption handling and is deferred. PatternMatch and reverse-lookup named tests belong to their respective contracts.

### InvertedIndexTest.cpp

- `InvertedIndex.Naive` runs int8/int16/int32/int64/bool/float/double/string in non-nullable and nullable forms. Its In/NotIn and four relational/interval comparisons map to the full typed matrix, including float/double membership that master skipped as “hard to compare”. The new oracle uses exact inserted finite values.
- `InvertedIndex.LoadSlicedNullOffsets` is a load-slicing profile rather than a distinct predicate; nullable results are covered, while forced slice configuration is deferred to loader tests.
- `InvertedIndex.HasLackBinlogRows` tests upstream missing-row NULL/default synthesis. Reader-visible nullable/default rules are covered; synthesis is deferred.
- `SealedAllValidDoesNotRetainValidityBitmap` is validity-storage accounting under NullReader, outside ScalarPredicateReader.
- PatternMatchConsistency, PatternMatchEscaping, and benchmark tests belong to PatternMatchReader or performance suites.

### HybridScalarIndexTest.cpp

- `HybridIndexTestV1` / `HybridIndexE2ECheck_LowCardinality` maps In/NotIn/four unary/four interval operations for int8/int16/int32/int64/string to hybrid low-cardinality profiles.
- `HybridIndexTestV2` / `HybridIndexE2ECheck_HighCardinality` maps the same operations at 10,000 rows/cardinality 2,000 to `TenThousandHighCardinality` and hybrid high-cardinality profiles.
- `HybridIndexTestNullable` maps nullable high-cardinality query behavior to the shared null-rejecting oracle.
- `HybridIndexTestV3` and `V4` repeat missing-row NULL/default synthesis; Reader-visible results are represented, while synthesis is deferred.
- `HybridIndexTestInverted` contains resource/load-overhead tests only, so it has no ScalarPredicateReader case to migrate.

### FMIndexTest.cpp

- `FMIndex.InNotInDeclinedThrowUnsupported` proves FM does not implement ScalarPredicateReader. Capability selection correctly excludes it. FM pattern guards and exactness belong to PatternMatchReader.

## 超出 master 的新增缺口

- In/NotIn: zero keys passed as `(0, nullptr)`, missing-only, all values, repeated query keys, mixed hit/miss, NULL slot sharing a stored value with a valid row.
- Unary Range: Equal and NotEqual plus the four master relational operations; actual integer `lowest()`/`max()` endpoint checks; bool comparisons; float/double finite `lowest()`/`max()`, fractions, `-0.0`, and `+0.0`.
- Interval Range: all four inclusivity combinations, all four equal-bound combinations, all four reversed-bound combinations, and closed/open actual endpoints.
- Inputs: absent validity (all valid), packed mixed validity, packed all-NULL, single row, all equal, non-byte-aligned packed-validity subviews across multiple batches, and zero-length batches interspersed with nonempty batches without changing coordinates.
- Strings: empty string, exact prefix-related values (`"a"` versus `"ab"`), UTF-8 bytes, embedded NUL with full `string_view` length, 80-byte value, and nullable duplicate long value.
- Scale: 100,000 rows with repeated int64 values; 10,000-row high-cardinality datasets for int8/int16/int32/int64/float/double/string, with bounded 200-value int8 coverage and 2,000 distinct values for the other types.
- Floating non-finite edges: float/double membership for both infinities, unary comparisons against `-inf`/`+inf`, and open/closed `[-inf,+inf]` intervals. Static acceptance evidence is direct value insertion in bitmap (`BitmapIndexBuilder.cpp:109-133`), sort (`SortedIndexBuilder.cpp:273-295`), and inverted (`InvertedIndexBuilder.cpp:444-500`) builders, with inverted float/double values represented as Tantivy F64 (`InvertedIndexBuilder.cpp:252-259`) and no finite-value rejection in those input/query paths.

## 静态用例计数

These are declarative case descriptors inferred from registration code, not executed GTest counts. Each descriptor expands once per capability-matching backend for its type.

| Type | In | NotIn | Unary Range | Interval Range | Total descriptors |
| --- | ---: | ---: | ---: | ---: | ---: |
| bool | 11 | 11 | 15 | 19 | 56 |
| int8 | 12 | 11 | 16 | 20 | 59 |
| int16 | 12 | 11 | 16 | 20 | 59 |
| int32 | 12 | 11 | 16 | 20 | 59 |
| int64 | 17 | 13 | 17 | 22 | 69 |
| float | 13 | 12 | 20 | 22 | 67 |
| double | 13 | 12 | 20 | 22 | 67 |
| string_view | 16 | 13 | 19 | 22 | 70 |
| **Total** | **106** | **94** | **139** | **167** | **506** |

| Dataset | Descriptor count |
| --- | ---: |
| PredicateEdges | 295 |
| PredicateAllValid | 32 |
| PredicateAllNull | 32 |
| PredicateSingleRow | 32 |
| PredicateAllEqual | 57 |
| PredicateAllFalse | 4 |
| PredicateFloatInfinities | 12 |
| HundredThousandRows | 3 |
| TenThousandHighCardinality | 28 |
| PredicateEdgesMultiBatch | 8 |
| PredicateEdgesWithEmptyBatches | 1 |
| RepeatedNullable manual expected callbacks | 2 |
| **Total** | **506** |

For a null-bearing dataset, the centralized backend catalog contributes eight nullable predicate profiles for each non-string type (bitmap/sort/inverted/hybrid, each heap and mmap) and ten for string_view (those eight plus Marisa heap/mmap; FM has no predicate capability). All-valid datasets also select the corresponding non-nullable profiles, for 16 non-string or 20 string_view backends. Bitmap bool/int8 mmap-request profiles deliberately exercise the production bitset/heap fallback because those value domains cannot exceed the greater-than-500-distinct Roaring mmap threshold; int16 and wider numeric types plus string_view reach physical bitmap mmap through the high-cardinality fixtures.

| Type | Descriptors | Null-bearing × nullable backends | All-valid × both metadata profiles | Expanded GTest parameters |
| --- | ---: | ---: | ---: | ---: |
| bool | 56 | 36 × 8 | 20 × 16 | 608 |
| int8 | 59 | 40 × 8 | 19 × 16 | 624 |
| int16 | 59 | 40 × 8 | 19 × 16 | 624 |
| int32 | 59 | 40 × 8 | 19 × 16 | 624 |
| int64 | 69 | 47 × 8 | 22 × 16 | 728 |
| float | 67 | 42 × 8 | 25 × 16 | 736 |
| double | 67 | 42 × 8 | 25 × 16 | 736 |
| string_view | 70 | 51 × 10 | 19 × 20 | 890 |
| **Total** | **506** |  |  | **5,570** |

Expanded operation totals are In 1,242, NotIn 1,082, unary Range 1,504, and interval Range 1,742. Expanded dataset totals are PredicateEdges 2,446; PredicateAllValid 528; PredicateAllNull 264; PredicateSingleRow 528; PredicateAllEqual 940; PredicateAllFalse 64; PredicateFloatInfinities 192; HundredThousandRows 48; TenThousandHighCardinality 464; PredicateEdgesMultiBatch 72; PredicateEdgesWithEmptyBatches 8; and RepeatedNullable manual callbacks 16.

## 明确延后项

- Zero-row input: bitmap and numeric/string sort builders reject `total_num_rows == 0` with `DataIsEmpty`, so the declarative build-to-reader factory cannot produce a zero-count Reader. This is a builder-contract case, not claimed as covered here. Zero query keys are covered.
- NaN only: C++ equality and relational comparisons do not provide ordinary ordered-value semantics for NaN, while the indexed families may use different key-ordering/canonicalization rules. No shared product contract was found for NaN membership or range behavior, so NaN stays deferred. IEEE `-inf` and `+inf` have defined comparisons relative to finite values and are covered.
- Builder/materializer synthesis of missing binlog rows and schema defaults.
- Corrupt payload rejection, forced file slicing, upload/path lifecycle, resource accounting, executor plan/fallback, and nested element-domain candidate projection.
- Array predicates: this contract suite is for primitive scalar row-domain readers.

## 仅静态验证的剩余风险

- Marisa embedded-NUL truncation is an expected red contract failure and is documented in `21-scalar-predicate-issues.md`; it is intentionally not skipped or normalized.
- Backend/profile expansion and hybrid selector dispatch were reconciled statically against the shared catalog; runtime selection and results remain unexecuted.
- No runtime result is claimed because running tests/builds is prohibited for this task.
