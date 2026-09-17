# 剩余专用标量契约覆盖

Date: 2026-09-15

Evidence baseline: master `a876f471053edb2f68a06a9894afee9810ea7906`, PR #64 `b7cfc84da8ad03ca3b33daca4242a73cacfa614c`, PR #65 `f672fd596774bb25f47a138a5ce59983932954d3`, integration `af90d325473284279ae680b14ddde1f9b9c82464`.

Validation status: source inspection and clang-format were followed by the
framework-owned centralized build and GTest listing. The final catalog list
completed with 12,524 unique tests; runtime execution is framework-owned.

## Task 3 源码矩阵

Owned source:

- `contracts/query/TextMatchReaderTest.cpp`
- `contracts/query/NgramReaderTest.cpp`
- `contracts/query/SpatialReaderTest.cpp`
- `scalar/ngram/NgramIndexReaderTest.cpp`
- `scalar/spatial/RTreeIndexReaderTest.cpp`
- `test_utils/TextAndCandidateDataSets.cpp`

Shared central profiles, registration, artifact IO, factory, driver, and CMake remain framework-owned.

Current declarative counts:

| Contract | Case descriptors | Backend-expanded GTest parameters | Compatible profile configs |
|---|---:|---:|---:|
| TextMatchReader | 39 | 345 | 24 Text configs: 18 standard and 6 Jieba across nullable/non-null, V5/V7, RAM/heap/mmap, VARCHAR/STRING/TEXT where supported |
| NgramReader | 108 | 988 | 16 scalar Ngram configs plus 2 projected-JSON configs |
| Concrete Ngram reader | 3 | 3 | Named central `NgramVarcharMin2Max4Heap` profile only |
| SpatialReader | 10 | 22 | 4 RTree configs: heap/mmap-request and nullable/non-null |
| Concrete RTree reader/build | 6 | 12 | Four exact MBR cases on named `SpatialRTreeHeap`; resource and empty-build cases over all 4 named RTree profiles |
| **Task 3 total** | **166** | **1,370** | **46 shared configs; named subsets reused by concrete regressions** |

The eight-parameter correction comes from
`NgramUtf8_Utf8ChinesePostfix`: the all-valid UTF-8 dataset and min-gram-2
selector legitimately expand across VARCHAR, STRING, and TEXT, heap and mmap,
and nullable and non-null profiles (12 parameters). The prior 980 estimate
counted only the four VARCHAR profiles for this descriptor.

The first full run exposed three matcher-agreement LIKE descriptors whose
two-character parts were incorrectly registered as positive queries on the two
min-gram-3 profiles. Each is now a min-gram-2 positive descriptor plus a
separately named min-gram-3 `CanHandle=false` descriptor. This adds three
descriptors while preserving the same 24 backend parameters.

The 19 lazy dataset descriptors are generated independently for expected data and build input. Text and scalar Ngram use string input with Row domain; projected Ngram uses `JsonProjectedString`; Spatial uses string-backed WKB with logical GEOMETRY and the `SpatialWkb` shape. Nullable, non-null, all-null, valid empty values, zero rows, and multi-batch packed validity offsets are distinct datasets.

## TextMatchReader

All successful observations check the backend-provided domain/value type, Count, caps, resource values, concrete `TextMatchReader`, concrete `NullReader`, and exact Count-sized query bitmap.

Queries use manual token answers:

- MatchQuery: one-term hit/miss, 2-of-3 and 3-of-3 `min_should_match`, 1-of-2 versus 2-of-2, empty query, second/third batch token offsets, all-valid, all-null, zero-row, punctuation, accented UTF-8, emoji-bearing rows, and long input.
- PhraseMatchQuery: one token; ordered phrase; reversed phrase at slop 0/1/2; intervening-token phrase at slop 0/1; all-valid and all-null.
- FuzzyMatchQuery: `footbal` at edit distance 0/1, `fotbal` at 1/2, and `basketbal` at 1.
- Jieba: Match and Phrase for unique `青铜`, repeated `黄金`, and common `时代`; exact `黄金时代` phrase; nullable input; RAM and persisted profiles.
- Input/profile coverage: multi-batch nullable global offsets, single-batch nullable, all-valid with absent validity, all-null, valid empty value, zero rows, VARCHAR V5/V7, VARCHAR RAM consumed artifact, STRING/TEXT V7, heap/mmap, standard/Jieba analyzer selection from configured params.

Pinned master mapping:

- `TextMatch.Index` -> football Match and Phrase cases, null checks.
- `TextMatch.FuzzyIndex` -> four edit-distance cases.
- `BuildIndexFromFieldDataMultiBatchNullable` -> packed multi-batch data, null bitmap on every query, explicit `foo` and `bar` later-batch hits.
- `BuildIndexFromFieldDataSingleBatchNullable` -> alpha/beta single-batch descriptors and null bitmap.
- `BuildIndexFromTextFieldData` -> TEXT logical profiles.
- `SealedNaive` / `SealedNaiveNullable` -> standard Match/Phrase and null rejection.
- `SealedJieBa` / `SealedJieBaNullable` -> all seven ordinary Jieba Match/Phrase descriptors.
- V5/V7 heap/mmap and RAM modes cover the ordinary query behavior reached through the old sealed construction/load cases.

Explicitly assigned elsewhere or deferred:

- Writer budgets, raw finalization, V2 validity materialization/sliced offsets, and translator accounting are Task 5 lifecycle coverage.
- JSON/tokenizer parsing helpers are helper implementation tests rather than reader contracts.
- upload/build services, LOB resolution, growing/reserved-offset/concurrency are #66 or service ownership.
- ExprResCache and execution negation/filter composition are #67.

## NgramReader

Every candidate query separately asserts:

- `caps.ngram_candidates=true`, `caps.exact=false`, Count/domain/value type, `NullReader`, and the concrete `NgramReader` reached directly or through JSON Resolve.
- `CanHandle` before query. False cases return without calling `Candidates`.
- output is a subset of the initial mask and never introduces a bit.
- every manually identified exact hit that was present initially remains.

The public contract intentionally permits arbitrary false positives. Three
concrete `NgramVarcharMin2Max4Heap` regressions separately pin current Phase-1
output for postfix-position false positives, prefix-position false positives,
and sparse-mask AND merging. Those checks do not apply to other
`NgramReader` implementations.

Coverage groups:

- all PatternOps: Match, RegexMatch, PrefixMatch, PostfixMatch, InnerMatch.
- min-gram 2/3 boundaries; empty/wildcard-only/short parts; all-long LIKE parts; Regex no usable literal, any usable literal, alternation, classes, escaped classes.
- one/two Chinese-character UTF-8 eligibility across Match/Regex/prefix/postfix/inner, including explicit two-character rejection by min-gram 3 for every operation; accented Latin, Chinese and repeated Unicode queries.
- all, sparse and empty initial masks; all-null and zero-row indexes; valid empty string and multi-batch null positions.
- exact-hit preservation for false-positive-prone inputs: prefix/postfix position, `elementary school secondary` versus `secondary school`, Wiki term ordering, repeated LIKE literal multiplicity, and regex class refinement.
- projected JSON `/a` VARCHAR: FieldNull, NoValue, valid empty and Value states; Resolve, CastTypesOf, Exists, NullReader and five master query shapes plus missing/Zilliz/Node cases.

Pinned master mapping:

- `TestNgramWikiEpisode` -> all ten Ngram-capable Wiki rows; Equal is a non-Ngram executor fallback listed below.
- `TestNgramSimple` -> `secondary school` false positive, `ele`, `%ary%sec%`, `%ary%s%` refusal, and postfix `ary`.
- `TestNgramJson` -> missing, `il`, `lliz`, `Zi`, `Zilliz`, `de`, `Node`, `%ery%ode%`, with tri-state JSON routing.
- `NgramPatternMatchConsistency.MatchersMustAgree` -> all 20 listed prefix/postfix/inner/LIKE entries as separate descriptors with manual exact hits; one named-family regression retains representative concrete candidate output.
- `OverlappingPatterns` -> all seven listed patterns with manual exact-hit preservation.
- `UTF8Patterns` -> all eight listed patterns.
- `CanHandleLiteralUsesUtf8CharacterCount` -> every single/two-Chinese PatternOp row, plus min-gram 3 distinction.
- `EscapeSequences` -> all three escaped patterns.

Explicitly deferred:

- `TestNonLikeExpressionsWithNgram` and `TestJsonNonLikeExpressionsWithNgram`, including Wiki Equal, are executor index-selection behavior: `NgramReader` exposes no Equal/In/NotIn API.
- Phase-2 LIKE/regex equality is an executor responsibility; this suite supplies its manual exact-hit invariant without invoking refinement.
- both benchmark groups remain performance machinery.

## SpatialReader

The 10 public reader descriptors check backend-provided domain/value type, authoritative Count, `caps.spatial=true`, `caps.exact=false`, and `NullReader`.

- Every SpatialOp is invoked separately: Equals, Touches, Overlaps, Crosses, Contains, Intersects, Within, DWithin.
- Each valid operation manually enumerates exact geometry hits that every legal candidate superset must retain. The public tests do not prescribe which false positives a reader returns.
- DWithin receives an already expanded polygon and requires the center point candidate, matching the contract boundary where the consumer expands distance before candidate lookup.
- valid points/polygons/line, boundary contact, overlap and disjoint boxes, null WKB, truncated WKB, empty geometry, multi-batch input, all-valid, and all-null are distinct public cases.
- Four concrete `SpatialRTreeHeap` regressions separately pin current point/contains/DWithin MBR intersections and the invalid-query all-non-null fallback. A concrete family observation checks the current heap resource accounting for all four named profiles.
- The concrete RTree empty-build suite invokes the builder separately for each of the four named profiles and requires exact `DataIsEmpty`.

Pinned master mapping:

- `Build_EmptyInput_ShouldThrow` -> exact empty-build error suite.
- `Build_WithInvalidWKB_Upload_Load`, `Build_VariousGeometries`, `Build_BulkLoad_Nulls_And_BadWKB` -> unified WKB datasets and Count/null/candidate checks.
- both `Query_*` coarse tests -> all eight public operation descriptors using manual required true hits, plus focused current-RTree MBR regressions.
- `AddGeometryClassifiesNullByValidityNotPayload` -> null row carries valid point payload but is excluded.
- wrapper `TestBuildAndLoad`, `TestQueryOperations`, `TestInvalidWKB`, `EmptyGeometryIsIndexedWithoutUndefinedMBR`, `CountTracksCommittedRowsOnBuildAndLoadPaths` -> persisted profile opening, query boxes, invalid/empty values, and authoritative Count.

Explicitly assigned elsewhere or deferred:

- filename/config/mixed-path, upload/load, slicing and serialization failures are Task 5 lifecycle coverage.
- remote missing objects, corrupt exact-refinement paths, bad-allocation injection, mutable wrapper recovery/starvation are specialized fault/concurrency tests.
- GIS exact filtering, split/fusion/legacy chunking and corrupt-row tolerance require #67 execution/refinement.
- growing concurrency/sparse publication belongs to #66.

## 等待集中式验证的静态风险

- Text token answers depend on the configured Tantivy standard/Jieba analyzers; manual answers follow the pinned master corpora and keep standard-CJK segmentation out of the assertions.
- Concrete Ngram and RTree result bitmaps pin named-family behavior only. The public reader contracts permit other false-positive sets while requiring all true hits.
- No production issue was recorded during source work because runtime evidence is not yet available.
