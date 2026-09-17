# JSON 读取器契约覆盖

Source snapshot: integration HEAD `af90d32547`, contract stack `b7cfc84da8` / `f672fd5967`, pinned master `a876f471053edb2f68a06a9894afee9810ea7906`. Source phase only: no configure, compile, GTest listing, or test execution.

## 已实现源码

- `contracts/query/JsonIndexReaderTest.cpp`: declarative `ReaderObservationCases` for JsonFlat and typed projected JSON, plus one local `JsonResolvedReader` ownership test.
- `test_utils/JsonDataSets.cpp`: lazy JSON-document, projected scalar/Ngram, and projected ARRAY datasets registered through `RegisterJsonDataSets(DataCatalog&)` with no static initializer.
- Shared profile/config hooks remain centralized in `ScalarReaderBackends.cpp`; the contract file contains no builder/loader/family configuration table.

Each descriptor expands to one independent GTest parameter per eligible backend. `ReaderObservationCases` generates expected data independently, destroys build inputs and serialized/open intermediates before observation, and checks the opened reader against the actual resolved loader.

## 数据集和状态模型

| Dataset | Input shape | Rows and purpose |
|---|---|---|
| `JsonEmployees` | `JsonDocument` | master employee strings, bools, int64 values, string/numeric arrays |
| `JsonTypeFamilies` | `JsonDocument` | integer, double, string, bool, typed arrays, object, empty array, JSON null, missing, embedded NUL, UTF-8, long string |
| `JsonPrecision` | `JsonDocument` | -10, 1, 10, 10.5, 2^63, UINT64_MAX, string "1", missing |
| `JsonFieldNullable` | `JsonDocument` | valid string, field-null, valid string, JSON null, missing; `{0,2,0,3,0}` batches |
| `JsonRootPrefix` | `JsonDocument` | build root `/profile`, exact-boundary sibling rejection, Unicode |
| `JsonEscapedPaths` | `JsonDocument` | JSON pointer escaped slash/tilde keys and numeric array segment rejection |
| `JsonAllMissing` | `JsonDocument` | supported `/a` shape with no values in any row |
| `JsonProjected{Double,Bool,Varchar}TriState` | `JsonProjected` | valid, missing, JSON null, cast failure, field null, valid |
| `JsonProjectedDoubleMultiBatch` | `JsonProjected` | same tri-state truth across interspersed empty batches |
| `JsonProjected{Double,Bool,Varchar}AllValid` | `JsonProjected` | absent validity and non-null metadata variants; string includes empty/NUL/UTF-8 |
| `JsonProjectedNgram{TriState,AllValid}` | `JsonProjected` | `JsonProjectedString` Value/NoValue/FieldNull states and wrapper routing |
| `JsonProjectedArray{Bool,Double,Varchar}` | `JsonProjected` | nonempty, valid empty, repeated elements, missing, field null, JSON null across batches |

Projected validity and existence remain separate. Missing, JSON null, and field null are in `non_exist_offsets`; cast failure is not. All four have invalid casted scalar/array validity. Thus `Exists(Any)` distinguishes cast failure from absence while resolved `NullReader` rejects every non-comparable value.

## JsonFlat 描述符

Routing and masks:

- `PreferredNameExists`, `ObjectSubpathExists`, `NumericExists`, `StringExists`, `BoolExists`, `SupportedPathWithNoValues` cover `Exists` Any/subpath behavior and each comparable family.
- `FieldNullIsIndependentOfJsonNullAndMissing` checks root `NullReader`; `ResolvedNullMeansNoComparableString`, `ResolvedNumericNullMask`, and `ResolvedBoolNullMask` check path-bound comparable masks.
- `CastVocabularyAdvertisesOnlyResolvableReaders` and `ResolvableReadersAreInCastVocabulary` preserve the public vocabulary/Resolve consistency requirement as red tests against the current production mismatch.
- `UnknownCastDoesNotResolve`, `NumericArrayPositionIsUnsupported`, `MalformedPointerIsRejected`, and `RootPrefixAcceptsExactSubpathAndRejectsSibling` cover ordinary unsupported shapes and exact DataTypeInvalid malformed-pointer classification.
- `EscapedSlashKey` and `EscapedTildeKey` retain JSON Pointer semantics with manual expected rows.

Typed predicates:

- VARCHAR: `StringIn`, `StringNotIn`, empty In/NotIn, all six unary CompareOps, all four endpoint-inclusive interval combinations plus reversed intervals, all five PatternOps, master LIKE `B_b`/`A%e`, embedded NUL, UTF-8, and root-prefix Unicode.
- BOOL: `BoolIn`, `BoolNotIn`, all six unary CompareOps around false, and all four false-to-true endpoint-inclusive interval combinations.
- INT64: In/NotIn, all six unary CompareOps, all four endpoint-inclusive interval combinations plus reversed intervals, numeric arrays In/range/interval.
- DOUBLE/mixed numeric: exact fractional In, all six unary CompareOps, all four endpoint-inclusive interval combinations plus reversed intervals.
- Precision: int64 NotIn(1), NotIn(INT64_MAX), >9, [0,9], double 10.5 equality and range cases preserve integer, f64, u64, and greater-than-2^53 behavior.
- String arrays: cpp and python any-element membership.

JsonFlat has 89 descriptors. They use 4 V7 x heap/mmap x nullable/non-nullable
profiles except the two `JsonFieldNullable` descriptors, which use the 2
nullable profiles: 352 parameterized tests. JsonFlat V5 is not a reader profile:
the current Tantivy binding explicitly rejects JSON `add_json_batch` for V5,
and pinned master builds JsonFlat with the latest V7 generation. The two
cast-vocabulary descriptors account for 8 source-expected production failures.

## 投影描述符

Every scalar/ARRAY type has separate `ProjectedExactPathAndCast` and `ProjectedComparableNullMask` parameters. They assert exact `CastTypesOf`, exact/wrong path and cast `Resolve`, `Exists(Any)`, comparable validity, and borrowed child routing while the parent remains alive. The outer JSON router must not sibling-cast to scalar predicate, pattern, or Ngram interfaces. Every resolved child reports the outer row count, Row domain, the logical cast value type, and caps consistent with its predicate/pattern/Ngram interfaces. Wrong-path and typed `Exists` calls reject with exact `UnexpectedError` as protocol violations.

- DOUBLE: In, three-valued NotIn, unary range, interval, absent validity, and multi-batch Exists across Sorted/Inverted/Hybrid heap/mmap and nullable/non-null layouts.
- BOOL: In, three-valued NotIn, range, and absent-validity interval across Bitmap/Inverted/Hybrid.
- VARCHAR: In, three-valued NotIn, range, interval, embedded NUL, and all five PatternOps across Sorted/Bitmap/Inverted/Hybrid.
- ARRAY_BOOL: In/NotIn/range; ARRAY_DOUBLE: In/NotIn/range/interval; ARRAY_VARCHAR: In/NotIn/range. Valid empty arrays are distinct from invalid rows and repeated elements do not change row semantics. Production-supported projected ARRAY profiles are Inverted only.
- Ngram: `ProjectedNgramRoutingAndExists` and `ProjectedNgramAbsentValidity` check exact path/cast routing to `NgramReader`, wrong path/cast rejection, and projected `Exists` for the two canonical heap/mmap profiles. Candidate set semantics remain in `NgramReaderTest`.

Projected scalar cases contain 26 descriptors / 240 parameters; projected ARRAY contains 16 / 32; projected Ngram contains 2 / 4.

`JsonResolvedReaderTest.EmptyBorrowedOwnedAndMovePreserveOwnership` independently covers empty state, Borrowed without deletion, Owned deletion, move construction, moved-from emptiness, and move-assignment replacement/destruction. Total JSON source accounting is 133 parameter descriptors / 628 parameters plus 1 ownership test = 629 GTests.

## 固定 master 映射

| Master file/test | New descriptor mapping |
|---|---|
| `JsonFlatIndexTest.TestInQuery`, `TestNotInQuery` | string membership groups on employee paths |
| `TestExistsQuery` | `PreferredNameExists` |
| `DistinguishesObjectSubpaths` | `ObjectSubpathExists` and typed Exists groups |
| `FiltersByComparableTypeFamily` | `NumericExists`, `StringExists`, `BoolExists`, resolved comparable Null masks |
| `TestComparableAndFieldValidityMasks`, `ExecutorReusesMaterializedFieldValidity` | field-level and resolved Null cases; `StringNotIn`/projected NotIn retain three-valued rejection |
| `TestRangeQuery` | all six string comparisons, all four interval endpoint combinations, and reversed intervals |
| `TestPrefixMatchQuery`, `TestLikePatternMatch`, `TestPatternMatchQuery` | five explicit PatternOp descriptors plus independent `A%ice`, `B_b`, and `A%e` LIKE cases |
| `TestBooleanInQuery`, `TestBooleanNotInQuery` | bool membership, all six comparisons, and four interval endpoint combinations |
| `TestInt64InQuery`, `TestInt64NotInQuery`, `TestInt64RangeQuery` | int64 membership, six comparisons, four interval endpoint combinations, and reversed interval |
| `TestNumericQueriesIncludeMixedIntegerTerms` | every `MixedInteger*` and DOUBLE precision descriptor over `JsonPrecision` |
| `TestArrayStringInQuery`, `TestArrayNumberInQuery`, `TestArrayNumberRangeQuery` | JsonFlat string/numeric array cases plus projected ARRAY matrices |
| `JsonPathIndexTest.ConvertDouble_NormalExtraction`, `PathNotExist`, `PathExistsButCastFails`, `MixedRows`, `ConvertVarchar` | pre-materialized projected tri-state/all-valid datasets and separate Exists/Null/predicate observations |
| `SortDouble_RangeQuery` | projected DOUBLE membership/range/interval |
| `BitmapVarchar_BuildAndCount` | projected VARCHAR routing/count and queries after serialize/open |
| `SortDouble_ExistsSemantics`, `BitmapBool_ExistsSemantics`, `Hybrid_ExistsSemantics` | per-type `ProjectedExactPathAndCast` Exists masks |
| `SortDouble_ComparisonUnknowns`, `InvertedDouble_ComparisonUnknowns`, `Hybrid_ComparisonUnknowns` | projected DOUBLE comparable Null, NotIn, range, interval across every listed family/load mode |
| `Hybrid_LowCardinalitySelectsBitmap`, `Hybrid_HighCardinalitySelectsSort`, `Hybrid_CardinalityIgnoresInvalidRows` | query result/caps paths are covered here; exact selector/delegate assertions remain centralized Hybrid lifecycle tests |
| `JsonIndexTest.TestJsonContains` | ARRAY_BOOL/DOUBLE/VARCHAR row-domain membership and repeated/empty/null handling; executor ContainsAny batching itself remains executor scope |
| `JsonIndexTest.TestJsonCast` | projected DOUBLE casted equality and type/path routing |

Additional gaps beyond master: absent-validity/non-null metadata layouts, JSON-document and projected zero-length batches, all-missing supported path, root-prefix boundary, escaped pointer keys, malformed pointer error code, unknown cast, numeric segment rejection, resolved-reader ownership/move and public metadata, projected outer-interface isolation, `Exists` protocol errors, embedded NUL/UTF-8/long strings, reversed intervals, empty membership keys, exact public cast-vocabulary consistency, and independent heap/mmap parameters.

## 已定义排除项

- `JsonFlatIndexTest.TestInApply`, `TestInApplyCallback`, and `TestQuery` are retired concrete-index callback/legacy query APIs absent from `JsonIndexReader`.
- `JsonFlatIndexContainsExprTest` and `JsonFlatIndexExprTest` build executor plans, combine JSON validity with raw fallback, or exercise cache/batching. Their direct reader truth tables are retained here; plan execution belongs to #67.
- `JsonIndexTest` sliced-offset cases and factory construction/cast conversion mechanics are artifact/registry/materializer coverage owned by the shared lifecycle task.
- JSON shredding, sibling-index selection, raw-data fallback, ARRAY row folding beyond the row-domain reader, and materializer cast functions are outside this query interface.
- Invalid/corrupt payload injection, remote storage, segment/cache pins, concurrency, and selector thresholds require their dedicated build/artifact/consumer suites.

## 静态检查和生产问题

`clang-format --dry-run --Werror` and `git diff --check` are clean for the two
owned JSON sources. Shared setup was corrected by removing unsupported JsonFlat
V5 positive profiles and limiting projection-completeness annotation to wrapped
projected artifacts; neither change altered a query expectation.

The corrected centralized remaining run
`/tmp/segcore-index-test-run-20260915-041720/index-tests-remaining.xml` executes
12,520 GTests and reports 214 failures. JSON contributes 60, all retained
production-contract failures: cast vocabulary 8, escaped JSON Pointer routing
8, string `LessEqual` 4, JsonFlat embedded-NUL membership 4, bool ordered ranges
32, and projected-Inverted embedded-NUL membership 4. There are no remaining
JSON oracle/profile/configuration failures in that XML. Exact source and runtime
classification is recorded in `23-remaining-scalar-json-issues.md`; correct
expectations remain enabled and unskipped.
