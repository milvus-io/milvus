# 剩余标量契约生产问题

## JsonFlat cast 词汇表与 Resolve 不一致

状态：已由源码和运行时确认。

The public `JsonIndexReader` contract defines `CastTypesOf(path)` as the supported cast vocabulary and directs callers to check it before `Resolve`/`Exists` (`internal/core/src/index/contracts/query/IJsonIndexReader.h:111-130`). `JsonFlatIndexReader::CastTypesOf` returns only `JSON` for every supported path (`JsonFlatIndexReader.cpp:1193-1198`), but `Resolve(JSON)` returns empty while BOOL, DOUBLE, VARCHAR, and their ARRAY element casts resolve to path readers through `cast_type.element_type()` (`JsonFlatIndexReader.cpp:1152-1178`).

Correct contract tests remain enabled in `JsonIndexReaderTest.cpp` as
`CastVocabularyAdvertisesOnlyResolvableReaders` and
`ResolvableReadersAreInCastVocabulary`. With the four supported V7 JsonFlat
profiles, these are 8 expected failing GTest parameters until production is
corrected. The tests neither replace `JSON` with the implementation's actual
types nor skip the inconsistency.

The corrected centralized run in
`/tmp/segcore-index-test-run-20260915-041720/index-tests-remaining.xml` reports
exactly eight failures: each descriptor fails on the four V7 heap/mmap and
nullable/non-nullable profiles. `CastVocabularyAdvertisesOnlyResolvableReaders`
observes the advertised `JSON` cast cannot resolve;
`ResolvableReadersAreInCastVocabulary` observes each resolvable BOOL, DOUBLE,
VARCHAR, and ARRAY element cast is absent from the advertised vocabulary.

## 投影 Inverted VARCHAR 成员查询截断嵌入 NUL query keys

状态：已由源码和运行时确认。

`JsonProjectedVarcharAllValid` stores six values, including the distinct value
`std::string("a\0b", 3)` at row 3. The
`ProjectedVarcharEmbeddedNull` descriptor queries that full-length value and
correctly expects `{3}`.

The projected wrapper delegates to the ordinary Inverted VARCHAR reader. Its
builder passes each string with an explicit length through
`tantivy_index_add_string` (`internal/core/thirdparty/tantivy/tantivy-wrapper.h:351-364`),
but `terms_query<std::string>` builds C-string pointers and calls
`tantivy_terms_query_keyword` without lengths (`tantivy-wrapper.h:825-833`).
The query therefore becomes `"a"`; this dataset has no exact `"a"` row, so
the source-predicted result is empty rather than `{3}`.

Both the first full XML and the corrected remaining-run XML report exactly
four failures: nullable and non-nullable `JsonProjectedInvertedVarchar`, each
in heap and mmap mode. No Sorted, Bitmap, or Hybrid projected profile fails
this descriptor. The test
keeps the length-aware key and does not skip Inverted.

## Marisa ScalarValueReader 截断拥有和借用的嵌入 NUL 值

状态：已由源码和运行时确认。

The string `PredicateEdges` datasets contain `std::string("a\0b", 3)` at row
3. `MarisaIndexBuilder.cpp:42-44` truncates every key at the first NUL before
assigning row IDs. `MarisaIndexReader.cpp:52-60` also truncates the trie value
returned by `ReverseKey`; both `Lookup` (`MarisaIndexReader.cpp:293-307`) and
`Gather` (`MarisaIndexReader.cpp:310-334`) therefore return `"a"` rather than
the contract value `"a\0b"`.

The first centralized XML reports eight failing GTests:

- `PredicateEdges_LookupMixedValidity` on nullable Marisa heap/mmap: 2.
- `PredicateAllValid_LookupAbsentValidity` on nullable/non-nullable Marisa
  heap/mmap: 4.
- `PredicateEdgesMultiBatch_GatherAcrossBatches` on nullable Marisa heap/mmap:
  2.

The failure output directly reports actual `"a"` versus expected `"a\0b"`.
This is the same stored-value truncation already exposed by Marisa predicate
tests, now confirmed through both the owning `Lookup` return and callback-only
`Gather` view. Expected values remain length-aware and all eight cases stay
enabled.


## JsonFlat JSON Pointer 转义已验证但未用于 engine 路由

状态：已由源码和运行时确认。

`JsonEscapedPaths` stores `{"a/b":"slash","m~n":"tilde"}` at row 0. The
`EscapedSlashKey` and `EscapedTildeKey` descriptors use the standard pointer
spellings `/a~1b` and `/m~0n` and correctly expect row 0.

`ResolveTantivyPath` validates and parses the pointer, including iterating the
decoded tokens, but then discards those tokens and constructs the Tantivy path
from the original encoded string by replacing `/` with `.`
(`internal/core/src/index/scalar/json/JsonFlatIndexReader.cpp:70-117`). Thus the
engine receives literal `a~1b` or `m~0n` rather than the decoded object key
`a/b` or `m~n`.

The remaining-run XML reports exactly eight bitmap mismatches: the two
descriptors on all four V7 heap/mmap and nullable/non-nullable profiles. These
are valid JSON Pointer paths, so the tests retain their decoded-key expectations.

## JsonFlat keyword 成员查询截断嵌入 NUL query keys

状态：已由源码和运行时确认。

`JsonTypeFamilies` stores JSON string `"a\u0000b"` at row 11, and
`EmbeddedNullString` queries the length-aware `std::string("a\0b", 3)` and
correctly expects `{11}`. `JsonStringPathReader` first preserves the view length
when owning the query value (`JsonFlatIndexReader.cpp:144-158,962-969`). The
Tantivy wrapper then converts every owned string to `c_str()` and calls
`tantivy_json_terms_query_keyword` without a length array
(`internal/core/thirdparty/tantivy/tantivy-wrapper.h:1433-1437`). Rust converts
each pointer as a C string (`tantivy-binding/src/index_reader.rs:637-653`), so
the query becomes `"a"`.

The remaining-run XML reports exactly four JsonFlat bitmap mismatches, one on
each V7 profile. The dataset has no exact `"a"` JSON string, so the
source-predicted result is empty rather than `{11}`. This is the same
length-loss boundary as the four projected Inverted failures, reached through
the JsonFlat term-query entry point.

## JsonFlat LessEqual 将 inclusive 标志传给错误边界

状态：已由源码和运行时确认。

For `JsonEmployees`, first names are `Alice`, `Bob`, and `Charlie`.
`StringLessEqual` asks for values `<= "Bob"` and correctly expects rows `{0,1}`;
the sibling `< "Bob"`, `== "Bob"`, and inclusive interval cases establish the
same lexical ordering.

`JsonStringPathReader::Range(value, CompareOp::LessEqual)` calls
`json_range_query` with `lb_unbounded=true`, `ub_unbounded=false`,
`lb_inclusive=true`, and `ub_inclusive=false`
(`JsonFlatIndexReader.cpp:1019-1027`). The wrapper and Rust binding preserve that
argument order (`tantivy-wrapper.h:1468-1536` and
`tantivy-binding/src/index_reader.rs:705-731`), so the meaningful upper bound is
excluded while the inclusive bit is applied to an unused lower bound. The call
therefore implements `<` rather than `<=`.

The remaining-run XML reports exactly four bitmap mismatches, one on each V7
profile. The correct `{0,1}` expectation remains enabled.

## JsonFlat 暴露 bool Range，但 Tantivy 拒绝 JSON bool range terms

状态：已由源码和运行时确认。

`JsonBoolPathReader` publicly implements `ScalarPredicateReader<bool>` and sends
all four ordered unary comparisons and both-bound interval comparisons through
`json_range_query` (`JsonFlatIndexReader.cpp:408-449`). The common predicate
contract includes all six `CompareOp` values and a four-flag interval method
(`internal/core/src/index/contracts/query/IScalarPredicateReader.h:27-57`). For
the `true,false,true` employee values, the expected truth sets are:

- `> false` and `(false,true]`: `{0,2}`.
- `>= false` and `[false,true]`: `{0,1,2}`.
- `< false` and `(false,true)`: empty.
- `<= false` and `[false,true)`: `{1}`.

Equal/NotEqual already pass through the membership path. Every ordered bool call
reaches the Rust generic JSON range builder
(`tantivy-binding/src/index_reader.rs:676-703`) via the bool FFI entry point
(`tantivy-binding/src/index_reader_c.rs:607-635`), then fails before producing a
bitmap. The runtime exception is:

`unsupported value bytes type in json term value_bytes Json`

The remaining-run XML reports exactly 32 exceptions: eight descriptors on the
four V7 profiles. This is a production Tantivy JSON-range limitation/error in an
interface production advertises and implements, not an oracle or profile error;
all valid bool comparison expectations remain enabled.

## 剩余运行的标量/JSON 失败统计

`/tmp/segcore-index-test-run-20260915-041720/index-tests-remaining.xml` executes
12,520 GTests and reports 214 failures. The JSON reader suite contributes exactly
60, all classified above or by the projected-Inverted issue:

| 根因 | 描述符 | 每个描述符的后端配置 | 失败数 |
|---|---:|---:|---:|
| JsonFlat cast vocabulary/Resolve disagreement | 2 | 4 | 8 |
| JsonFlat escaped JSON Pointer routing | 2 | 4 | 8 |
| JsonFlat `LessEqual` bound flag | 1 | 4 | 4 |
| JsonFlat embedded-NUL keyword query | 1 | 4 | 4 |
| JsonFlat bool ordered range unsupported | 8 | 4 | 32 |
| Projected Inverted embedded-NUL query | 1 | 4 | 4 |
| **JSON 合计** | **15** |  | **60** |

No JSON failure in this XML is caused by a test oracle or remaining backend
profile/configuration error. No expected result was weakened or skipped.
