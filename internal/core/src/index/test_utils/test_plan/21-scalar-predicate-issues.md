# 标量谓词生产问题

## Marisa 在嵌入 NUL 处截断 string_view 值

状态：已由源码和运行时确认。

The Marisa builder and reader advertise `ScalarPredicateReader<std::string_view>` and `ReaderCaps{.predicate = true}` without a C-string restriction, but the builder normalizes every stored value and the reader normalizes exact/prefix lookup values with `value.substr(0, value.find('\0'))`:

- `internal/core/src/index/scalar/marisa/MarisaIndexBuilder.cpp:42-44`, used while inserting keys and assigning row IDs at lines 119 and 137.
- `internal/core/src/index/scalar/marisa/MarisaIndexReader.cpp:59-61`, used by exact lookup at lines 412-420 and prefix lookup at lines 422-431. In, NotIn, Equal, and NotEqual use the exact lookup; other range/pattern operations still inspect already-collapsed stored keys.

Concrete effect: after building distinct valid values `"a"` and
`std::string("a\0b", 3)`, both rows are assigned to the trie key `"a"`.
Exact lookup of either logical value therefore returns both rows. Ordered
ranges compare the collapsed stored key, so a lower-exclusive bound of `"a"`
incorrectly excludes the embedded-NUL row. The equal-bound interval
`["a\0b", "a\0b"]` uses the full query bounds but cannot find the truncated
stored key.

The complete run in
`/tmp/segcore-index-test-run-20260914-230828/index-tests-second.xml`
executed all 7,960 registered tests and reported 24 Scalar failures attributable
to this defect: 12 semantic cases on both `MarisaVarchar` and
`MarisaVarcharMmap`. The driver logs only `actual == expected`, so the row sets
below are derived from the dataset, oracle, and the production lookup/range
code; the failing case identities themselves are runtime-observed.

| Case | Contract/oracle rows | Source-predicted Marisa rows |
|---|---:|---:|
| `PredicateEdges_InMixedHitAndMiss` | `{1}` | `{1,3}` |
| `PredicateEdges_NotInMixedHitAndMiss` | `{0,2,3,4,6}` | `{0,2,4,6}` |
| `PredicateEdges_IntervalOpenOpen` | `{2,3}` | `{2}` |
| `PredicateEdges_IntervalOpenClosed` | `{2,3,6}` | `{2,6}` |
| `PredicateEdges_InPrefixValueIsExact` | `{1}` | `{1,3}` |
| `PredicateEdges_InEmbeddedNulIsLengthAware` | `{3}` | `{1,3}` |
| `PredicateEdges_NotInEmbeddedNulIsLengthAware` | `{0,1,2,4,6}` | `{0,2,4,6}` |
| `PredicateEdges_UnaryEqualEmbeddedNulIsLengthAware` | `{3}` | `{1,3}` |
| `PredicateEdges_UnaryNotEqualEmbeddedNulIsLengthAware` | `{0,1,2,4,6}` | `{0,2,4,6}` |
| `PredicateEdges_IntervalEmbeddedNulIsLengthAware` | `{3}` | `{}` |
| `PredicateEdgesMultiBatch_InAcrossBatches` | `{1,6}` | `{1,3,6}` |
| `PredicateEdgesMultiBatch_NotInAcrossBatches` | `{0,2,3,4}` | `{0,2,4}` |

Rows are from `PredicateEdges`: 1 is `"a"`, 3 is `"a\0b"`, row 5 is
null, and row 6 is the valid long string. The identical heap/mmap failure set
shows that persistence/loading does not change the defect.

Expected contract semantics: scalar string predicates compare the full `std::string_view`, including embedded NUL bytes. The declarative cases `InEmbeddedNulIsLengthAware`, `NotInEmbeddedNulIsLengthAware`, `UnaryEqualEmbeddedNulIsLengthAware`, `UnaryNotEqualEmbeddedNulIsLengthAware`, and `IntervalEmbeddedNulIsLengthAware` preserve that expectation for every capable backend. Marisa is not skipped and the oracle is not changed to match truncation.

No production fix was made in this task.

## Inverted 标量查询在 Tantivy FFI 丢失嵌入 NUL 后缀

状态：已由源码和运行时确认。

The inverted string builder preserves full lengths: `TantivyIndexWrapper::add_data<std::string>` calls `tantivy_index_add_string(writer, s.data(), s.size(), offset)` in `internal/core/thirdparty/tantivy/tantivy-wrapper.h:351-364`. The Reader also copies the full `string_view` into an owning `std::string`.

The query boundary then loses the length:

- `terms_query<std::string>` builds `std::vector<const char*>` from `.c_str()` and calls `tantivy_terms_query_keyword` at `tantivy-wrapper.h:825-833`.
- keyword lower-bound, upper-bound, and interval range calls pass `.c_str()` at `tantivy-wrapper.h:891-893`, `939-941`, and `998-1001`.

Concrete effect: if the index stores distinct valid values `"a"` and
`std::string("a\0b", 3)`, `In("a\0b")` sends the query term `"a"` through
the C-string FFI and hits row 1 instead of row 3. NotIn and Equal/NotEqual
inherit that wrong term. An equal-bound interval truncates both FFI bounds to
`"a"` and likewise hits row 1.

The corrected full run reported 18 Scalar failures on inverted VARCHAR:

- The five explicit embedded-NUL cases fail on nullable heap and mmap profiles:
  10 failures.
- `PredicateEdges_InAllValues` and `PredicateEdges_NotInAllValues` fail on
  nullable heap and mmap profiles because the all-values key set cannot address
  row 3: 4 failures.
- `PredicateAllValid_InAllValuesWithoutValidity` fails on nullable/non-nullable
  metadata and heap/mmap profiles: 4 failures.

For the five explicit cases, the contract/source-predicted inverted row pairs
are: In and Equal `{3}` versus `{1}`; NotIn and NotEqual `{0,1,2,4,6}` versus
`{0,2,3,4,6}`; equal-bound interval `{3}` versus `{1}`. For `InAllValues`,
the expected valid rows include row 3 while the query result omits it;
`NotInAllValues` expects no rows and retains row 3. On the all-valid dataset,
`InAllValuesWithoutValidity` expects rows 0 through 6 and omits row 3. These row
sets are source-predicted because the driver reports only bitmap inequality;
the 18 case/profile failures are runtime-observed.

The same length-aware declarative cases listed above intentionally remain enabled for inverted. No production fix or backend skip was added.

## Inverted 浮点谓词区分负零和正零

状态：已由源码和运行时确认。

The scalar predicate contract uses ordinary C++ floating comparison, where
`-0.0 == +0.0`. The inverted path converts both FLOAT and DOUBLE input to
Tantivy `f64` terms:

- `InvertedIndexBuilder.cpp:252-259` selects `TantivyDataType::F64` for both
  Milvus types.
- `tantivy-wrapper.h:337-352` sends FLOAT/DOUBLE input to the respective Rust
  ingestion functions; `index_writer_c.rs:333-383` widens FLOAT to `f64` and
  stores both as `f64`.
- `tantivy-wrapper.h:808-823`, `882-887`, `930-935`, and `988-994` send exact
  membership and range bounds as `f64` terms.
- The default scalar engine version selects the minimum V5 Tantivy dependency
  at `InvertedIndexBuilder.cpp:220-235`. `Cargo.lock` resolves that dependency
  to `bc211a5a76930b120b1eeb25073be27f20ba0387`; its
  `common/src/lib.rs:96-102` maps the raw sign bit and therefore encodes
  `-0.0` immediately below, and distinctly from, `+0.0`.

The corrected full run reported 52 Scalar failures from this semantic split:
26 FLOAT and 26 DOUBLE. For each type, 11 `PredicateEdges` descriptors fail on
heap and mmap (22 failures), while
`PredicateAllValid_InAllValuesWithoutValidity` fails across nullable and
non-nullable metadata on heap and mmap (4 failures).

For `PredicateEdges`, rows 2 and 3 are `-0.0` and `+0.0`; row 5 is null. The
runtime-failing descriptor names and source-predicted difference are:

| Case | Contract/oracle rows involving zero | Source-predicted inverted difference |
|---|---:|---|
| `InAllValues` | includes 2 and 3 | omits 2 |
| `InRepeatedKeys` | `{2,3}` | `{3}` |
| `NotInAllValues` | excludes 2 and 3 | retains 2 |
| `NotInRepeatedKeys` | excludes 2 and 3 | retains 2 |
| `UnaryEqual` | `{2,3}` | `{3}` |
| `UnaryNotEqual` | excludes 2 and 3 | retains 2 |
| `UnaryGreaterEqual` | includes 2 and 3 | omits 2 |
| `UnaryLessThan` | excludes 2 and 3 | additionally includes 2 |
| `UnaryEqualNegativeZero` | `{2,3}` | `{2}` |
| `UnaryEqualPositiveZero` | `{2,3}` | `{3}` |
| `IntervalEqualBoundsClosedClosed` | `{2,3}` | `{3}` |

`PredicateAllValid_InAllValuesWithoutValidity` expects all seven rows and
omits row 2. The exact row differences are source-predicted because the driver
reports only bitmap inequality; the 52 case/profile failures are
runtime-observed. The identical FLOAT/DOUBLE and heap/mmap pattern rules out
FLOAT widening and persistence as separate causes.

No oracle, dataset, or Scalar predicate test error was found in these 94
runtime failures. The expected semantics and all affected backend profiles stay
enabled. No production fix was made in this task.
