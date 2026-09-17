# PatternMatchReader 生产问题

## Marisa 违反 string-view 契约而截断嵌入 NUL 字节

Initially identified from source and later confirmed by both authorized full
test runs described below.

- `internal/core/src/index/scalar/marisa/MarisaIndexBuilder.cpp` applies
  `LegacyCStringValue(value)`, implemented as
  `value.substr(0, value.find('\0'))`, before inserting each valid
  `std::string_view` into the trie.
- `internal/core/src/index/scalar/marisa/MarisaIndexReader.cpp` applies the
  same truncation to exact lookup and prefix patterns.
- `MarisaIndexLoader::DeriveCaps()` advertises unqualified
  `pattern_match = true`; `PatternMatchReader` accepts length-aware
  `std::string_view` patterns and documents no C-string restriction.
- Pinned master test
  `a876f471053edb2f68a06a9894afee9810ea7906:internal/core/src/index/InvertedIndexTest.cpp`
  (`PatternMatchConsistency.NulByteHandling`) states and checks length-aware
  embedded-NUL LIKE behavior.

Consequence: distinct values such as `"a\0b"` and `"a"` collapse during a
Marisa build. Prefix patterns containing a NUL are truncated as well, so exact
PatternMatchReader expectations can produce false positives or false
negatives. The contract tests retain length-aware expected offsets and do not
skip or normalize Marisa. This is the same root cause recorded for the scalar
predicate/value contracts in `21-scalar-predicate-issues.md`.

### 运行时确认

The first authorized full `index_tests` run is recorded in
`/tmp/segcore-index-test-run-20260914-230828/index-tests.xml` and
`full-run.log`. The Pattern suite ran 2,372 parameters. Of its 732 failures,
682 are the separately owned Inverted `field_id` setup failure. All remaining
50 are result-bitmap mismatches on Marisa and match the truncation defect above:

- 23 `PatternBinaryNullable` cases fail on both nullable Marisa profiles
  (`MarisaVarchar` and `MarisaVarcharMmap`): 46 failures.
- `PredicateAllValid_AllValidPostfix` fails on all four nullable/non-nullable,
  memory/mmap Marisa profiles: 4 failures.
- No non-Marisa result-bitmap mismatch appears in this run.

After the shared `field_id` setup was corrected, the full suite was rerun into
`index-tests-second.xml` and `full-run-2.log`. It again registered all 2,372
Pattern parameters: 2,322 passed and the same 50 Marisa cases failed. The
second run has no Inverted or Hybrid Pattern failure. Failure distribution is
24 on `MarisaVarchar`, 24 on `MarisaVarcharMmap`, one on
`MarisaVarcharNonNull`, and one on `MarisaVarcharNonNullMmap`.

| Case | Failures | Correct length-aware rows | Rows predicted from current truncated storage/query source |
|---|---:|---|---|
| `NulPrefixLiteral` | 2 | 3, 6, 9 | 0, 3, 6, 9 |
| `NulPostfixLiteral` | 2 | 3, 6 | none |
| `NulInnerLiteral` | 2 | 3, 4, 5, 6, 7, 8, 9, 10 | none |
| `NulInnerMultiByteLiteral` | 2 | 3, 9 | none |
| `NulLikeGap` | 2 | 0, 3, 6 | 0 |
| `NulLikePostfix` | 2 | 0, 3, 6 | 0 |
| `NulLikeToSuffix` | 2 | 9 | none |
| `NulLikePostfixPastNul` | 2 | 1, 4 | 1, 5 |
| `NulLikeSingleWildcard` | 2 | 3 | none |
| `NulLikeLeadingWildcard` | 2 | 4 | none |
| `NulLikeTrailingWildcard` | 2 | 5 | none |
| `NulLikeTwoWildcards` | 2 | 6 | none |
| `NulLikeOneCodepoint` | 2 | 7 | 3, 6, 9 |
| `NulLikeTwoCodepoints` | 2 | 0, 8 | 0, 10 |
| `NulLikeExactLiteral` | 2 | 3 | none |
| `NulLikeLiteralThenPercent` | 2 | 3, 6, 9 | none |
| `NulLikePercentThenLiteral` | 2 | 3, 6 | none |
| `NulLikeLiteralPrefix` | 2 | 4, 7, 8 | none |
| `NulLikeLiteralPostfix` | 2 | 5, 7, 8 | none |
| `NulLikeLiteralInner` | 2 | 3, 4, 5, 6, 7, 8, 9, 10 | none |
| `NulLikeLiteralAndWildcard` | 2 | 3 | none |
| `NulRegexLiteralPartial` | 2 | 3, 9 | none |
| `NulRegexLiteralAnchored` | 2 | 3 | none |
| `AllValidPostfix` | 4 | 2, 3 | 2 |

The XML reports bitmap inequality without dumping set offsets. The last two
columns were independently derived from the frozen datasets and current source,
not inferred from a changed expectation. `MarisaIndexBuilder.cpp:42-45` defines
the truncation and lines 117-124 plus 135-142 apply it before trie insertion and
row-id lookup. `MarisaIndexReader.cpp:338-386` evaluates Match/Postfix/Inner/
Regex against those truncated reverse-looked-up keys; lines 411-428 truncate
exact/prefix lookup arguments as well. The predicted wrong rows account for
every observed mismatch. No test expectation was changed.

## 畸形 regex 被分类为意外系统错误

Initially identified from source; the precise current error behavior was later
confirmed by the second authorized full run described below.

- Every current direct `RegexMatch` implementation constructs
  `PartialRegexMatcher` before evaluating values.
- `internal/core/src/common/RegexQuery.h` rejects an invalid RE2 expression via
  `AssertInfo(re2_->ok(), "Failed to compile regex pattern: ...")`.
- `AssertInfo` uses its default `ErrorCode::UnexpectedError`, so a malformed
  user-supplied regex such as the unsupported backreference `(a)\\1` throws a
  `SegcoreError` with code `UnexpectedError`.

Malformed regex syntax is forced by request content, so the generic system
classification does not reflect the input cause. The contract case asserts the
precise current rejection code; the static trace above records its construction
path. This records current behavior rather than claiming the category is
correct. Production code is not changed in this test-only task.

The second authorized full run executed all 12
`RegexBackreferenceRejected` parameters successfully, including both Inverted
profiles after their shared metadata correction. Ten implemented direct-query
profiles observed the expected current `UnexpectedError`; the two FM profiles
observed their declared `Unsupported` result. This runtime result confirms the
current classification but does not make that classification appropriate for
malformed request input.
