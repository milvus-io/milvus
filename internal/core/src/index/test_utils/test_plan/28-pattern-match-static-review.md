# PatternMatchReader 契约测试静态审查

Scope: `internal/core/src/index/contracts/query/PatternMatchReaderTest.cpp`, `11-pattern-match-coverage.md`, shared pattern datasets, and pinned master `a876f471053edb2f68a06a9894afee9810ea7906`. Review followed `~/.claude/CODE_REVIEW_GUIDE.md`. No configuration, compilation, test, or Milvus binary was run.

## 审查期间已关闭的发现

1. Invalid-regex handling no longer catches broad exception classes or fabricates an empty bitmap. `RegexBackreferenceRejected` invokes the normal `PatternQuery::Run` path and requests exact `ErrorCode::UnexpectedError`. The shared driver catches only that operation, checks the precise `SegcoreError` code, rejects non-`SegcoreError` failures, and maps FM's declared unsupported policy to exact `ErrorCode::Unsupported`.

2. Six focused Hybrid cases now exercise and assert both selector outcomes for Postfix, Inner, and Regex. `PatternBinaryNullable` requires the Bitmap delegate and `expected_should_use = true`; `PatternStringsNullable` requires the Inverted delegate and `expected_should_use = false`. Every case continues into the normal result check, with explicit correct regex offsets where no generic oracle exists.

3. The pinned-master traceability table now names `FMIndexTest.cpp::MatchOracleEqualsBruteForce`, `MatchGuardDeclinesUnselectiveAndStillExact`, `MatchGuardAcceptsRareHitOnLongRows`, and `MatchGuardAcceptsRareFragmentOnShortRows` and explains which exact-reader behavior is represented and which candidate/refinement behavior is deferred.

4. `LikeEstInner` now includes row 38 (`emoji` plus `test`) as well as rows 61-64. Its expected list matches the current shared dataset.

5. The final nine-case LIKE helper delta is consistent with the shared corpus and pinned helper contracts. Ordered `a%b%c` selects rows 21, 26, 84, 85, 100, and 101; literal dot and regex metacharacters select rows 53 and 55; the 3-byte CJK and 4-byte emoji wildcard cases each consume one code point; and `%©%` rejects the continuation-byte decoy in row 35. The three trailing-backslash forms execute normal `Match` queries and require exact `ErrorCode::ExprInvalid`, matching all current parser construction paths. The coverage table names the corresponding pinned-master helper groups.

## 已审查期望

No additional finding was found in the manual row lists checked against `MakePatternStringsNullable` (`ScalarDataSets.cpp:206-317`) and `MakePatternBinaryNullable` (`ScalarDataSets.cpp:319-336`). The audit covered LIKE overlap/escape/UTF-8/special-byte groups, embedded-NUL patterns, regex anchors/classes/groups/Unicode/newline behavior, null exclusion, all-valid rows, and literal Prefix/Postfix/Inner semantics. This is static evidence only.

The final source-derived matrix records 207 descriptors and 2,372 expanded parameters: Prefix 24/280, Postfix 19/204, Inner 29/264, Match 89/1,080, and Regex 46/544. The nine new nullable Match descriptors contribute 108 parameters, or 18 to each family, and the operation and family totals reconcile to 2,372 in the coverage report. No current actionable pattern-test finding remains.

## 单独保留的已知生产缺陷

Marisa embedded-NUL truncation remains documented in `22-pattern-match-issues.md`. The review did not treat its contract-correct failing expectations as a test defect.
