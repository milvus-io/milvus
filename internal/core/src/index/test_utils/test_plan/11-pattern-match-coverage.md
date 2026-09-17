# PatternMatchReader 契约覆盖

Pinned pre-refactor/master source: `a876f471053edb2f68a06a9894afee9810ea7906`.

The coverage inventory and expected results were derived from source. A later
authorized full `index_tests` run executed all 2,372 registered Pattern
parameters; runtime failure classification is recorded in
`22-pattern-match-issues.md`.

## 契约维度

- Value type: `std::string_view` / VARCHAR.
- Coordinate domain: row offsets, bitmap size equal to `Count()`.
- Null semantics: every pattern result clears invalid rows, including patterns
  that otherwise match every string.
- `Match`: raw SQL LIKE syntax; `%` is zero-or-more code points, `_` is one
  UTF-8 code point, and backslash escapes the next pattern character.
- `PrefixMatch`, `PostfixMatch`, `InnerMatch`: their pattern is a literal, so
  `%`, `_`, and backslash have no wildcard meaning.
- `RegexMatch`: raw RE2 expression with partial/substring semantics, UTF-8,
  and dot matching newline.
- `ShouldUseForOp`: a planner decision checked separately from direct exact
  query behavior. A false result can mean an optimization decline or an
  unsupported operation, depending on the backend operation promise.

## 固定 master 可追溯性

| Master source/test | Contract cases replacing or extending it |
|---|---|
| `BitmapIndexTest.cpp::PatternMatchFuncTest` | LIKE fixed-prefix hit/miss and `RegexMatch` anchored-start cases over nullable data; every bitmap memory/mmap profile |
| `StringIndexSortTest.cpp::PrefixMatchMemory`, `PrefixMatchMmap` | literal prefix hit/miss/empty/Unicode/long cases over every accepting backend profile |
| `StringIndexSortTest.cpp::PatternMatchBasicMemory` | LIKE `app%`, `app%ion`, `%ana%`, `cat%` with duplicate offsets retained |
| `StringIndexSortTest.cpp::PatternMatchWithUnderscoreMemory` | LIKE `a_c`, `a_c%`, Unicode one-code-point underscore |
| `StringIndexSortTest.cpp::PatternMatchEscapeMemory`, `PatternMatchComplexEscape` | `100\\%`, `%\\%%`, `a\\_b`, `a\\%b`, `10\\%\\_off`, `10\\%_off` with manual offsets |
| `StringIndexSortTest.cpp::PatternMatchNoPrefix` | LIKE `%hello` and `%world%` |
| `StringIndexSortTest.cpp::PatternMatchMmap`, `PostfixMatchMmap`, `InnerMatchMmap` | the same semantic cases expand through ordinary mmap profiles in the shared backend catalog |
| `StringIndexSortTest.cpp::PatternMatchPrefixOp` | direct `PrefixMatch("app")` |
| `StringIndexSortTest.cpp::PatternMatchDuplicateValues` | duplicate-value LIKE/literal queries verify every duplicate row is set |
| `StringIndexSortTest.cpp::PostfixMatch`, `InnerMatch` | literal suffix/substring hit and miss cases |
| `StringIndexTest.cpp::PrefixMatch`, `Query`, `Codec`, `BaseIndexCodec` | literal prefix query through the shared builder-artifact-named-buffer-loader round trip for Marisa memory/mmap profiles |
| `ScalarIndexTest.cpp` string `ShouldUseOp` checks | the current per-operation backend policies assert unconditional acceptance, implemented-but-declined operations, data-selective routing, and unsupported operations independently of result checking; numeric pattern checks are outside the VARCHAR-only contract |
| `InvertedIndexTest.cpp::PatternMatchPlannerPolicy` | explicit per-operation `ShouldUseForOp` expectations, separate from direct query correctness |
| `InvertedIndexTest.cpp::PatternMatchConsistency.AllMatchersMustAgree` | LIKE exact/prefix/suffix/inner/gapped/underscore; overlap; UTF-8; escapes; empty/percent-only; whitespace and special characters, using manual offsets for complex syntax |
| `InvertedIndexTest.cpp::PatternMatchConsistency.OpTypeMatchersMustAgree` | literal Prefix/Postfix/Inner with common terms and exact expected rows |
| `InvertedIndexTest.cpp::PatternMatchConsistency.SpecialByteHandling` | LIKE over tab/newline/CRLF with manual offsets |
| `InvertedIndexTest.cpp::PatternMatchConsistency.NulByteHandling` | length-aware embedded-NUL data and patterns for literal and LIKE operations; correct expectations retained for every capable backend |
| `InvertedIndexTest.cpp::PatternMatchEscaping.*` | literal Postfix `%a` and Inner `_`, proving anchored operation arguments are literals |
| `FMIndexTest.cpp::PrefixInfixSuffixRouting` | accepted Prefix/Postfix/Inner exact offsets on a selective corpus |
| `FMIndexTest.cpp::EmptyPatternMatchesAllRows`, `NullVsEmptyStringDistinctAfterReload` | empty literal matches every non-null row, including genuine empty strings, and excludes nulls after the standard artifact/load round trip |
| `FMIndexTest.cpp::ShouldUseOpDeclinesRangeAndRegex` | Pattern contract covers the five pattern operations: FM `Match`/`RegexMatch` decline and throw `Unsupported`; FM range/equality checks are excluded because `FmIndexReader` exposes neither `ScalarPredicateReader` nor any range/equality query contract |
| `FMIndexTest.cpp::CountFirstGuardDeclinesHighHitPatterns` | independently known high-frequency literal fixture expects planner decline; exact direct result remains checked where implemented |
| `FMIndexTest.cpp::CountFirstGuardAcceleratesLowHitAndMatchesAreExact` | rare `ZEBRA`/`QOP` fixture reaches and validates Prefix/Postfix/Inner through FM memory/mmap profiles; missing `QUOKKA` accepts and returns no hits |
| `FMIndexTest.cpp::CountFirstGuardAcceleratesZeroHitOnZeroTokenSegment` | all-empty/all-null and missing-literal cases cover zero-token/zero-hit routing without reproducing the cost formula |
| `FMIndexTest.cpp::InnerMatchDedupsRepeatedOccurrences` | repeated-substring data verifies one result bit per matching row |
| `FMIndexTest.cpp::DifferentialOracleRandomBytes` | deterministic fixed-seed byte dataset plus representative Prefix/Postfix/Inner cases using the independent standard-library literal oracle; scoped to FM because 20 rows contain arbitrary bytes that need not be valid UTF-8, matching the source test's byte-engine scope |
| `FMIndexTest.cpp::SerializeLoadRoundTripMmap`, `SerializeLoadRoundTripNoMmap` | every FM case expands through named-buffer memory and mmap load profiles |
| `FMIndexTest.cpp::MatchOracleEqualsBruteForce` | explicitly deferred: legacy `FMIndex::PatternMatch(Match)` returns candidates and `RecheckedMatch` applies phase-two LIKE verification; current `FmIndexReader` exposes no exact `Match` result and throws `Unsupported`, which ordinary contract cases assert |
| `FMIndexTest.cpp::MatchGuardDeclinesUnselectiveAndStillExact` | explicitly deferred for its candidate-routing and phase-two behavior; the corresponding common/rare/empty anchored-literal guards and direct results are covered |
| `FMIndexTest.cpp::MatchGuardAcceptsRareHitOnLongRows` | its anchored `InnerMatch("RARE")` acceptance/result behavior maps to the selective rare fixture; its general-LIKE candidate/refinement portion is deferred |
| `FMIndexTest.cpp::MatchGuardAcceptsRareFragmentOnShortRows` | explicitly deferred because it exercises the legacy general-LIKE candidate/refinement path rather than an exact current reader operation |
| `HybridScalarIndexTest.cpp::HybridScalarIndexPlannerPolicy.ShouldUseOpDelegatesToInternalIndex` | six focused Hybrid cases assert the selected Bitmap/Inverted delegates' Postfix/Inner/Regex routing decisions and exact direct results |
| `NgramInvertedIndexTest.cpp::NgramPatternMatchConsistency.*` | final literal/LIKE rules map to the ordinary cases above; ngram acceptance, candidate supersets, phase-two refinement, JSON paths, and benchmark mechanics are explicitly deferred |
| `JsonFlatIndexTest.cpp::TestPrefixMatchQuery`, `TestLikePatternMatch`, `TestPatternMatchQuery` | ordinary string rules map to Prefix/LIKE cases; path-addressed JSON projection and element/row coordinate handling are explicitly deferred |
| `skipindex_stats/SkipIndexStatsTest.cpp` pattern skip checks | explicitly excluded because skip statistics expose a pruning/candidate decision, not a `PatternMatchReader` result bitmap |
| `RegexQueryUtilTest.cpp::TranslatePatternMatchToRegexTest.*`, `MultiWildcardMatcherTest.*`, `LikePatternMatcherTest.*` | direct LIKE exact/prefix/suffix/multi-segment, escaped wildcard/backslash, literal regex metacharacter, empty, long, and string-view cases; malformed trailing-backslash patterns assert exact `ExprInvalid` |
| `RegexQueryUtilTest.cpp::UnderscoreCharacterSemanticsTest.*`, `SqlStandardTest.*`, `UTF8ConsistencyTest.*` | `_` over 2-byte, 3-byte, and 4-byte code points, mixed-width strings, `%` with Unicode, and a no-hit mid-codepoint literal boundary case |
| `RegexQueryUtilTest.cpp::NulByteConsistencyTest.*` | length-bearing NUL LIKE and literal-operation cases over `PatternBinaryNullable` |
| `RegexQueryUtilTest.cpp::LiteralWildcardMatch.*` | direct Prefix/Postfix/Inner literal `%` and `_` cases with wildcard-like decoys in the shared corpus |
| `RegexQueryTest.cpp` string/index query cases | direct Prefix/Postfix/Inner/Match/Regex result semantics are covered; plan parsing, raw fallback, offsets, and segment execution remain consumer coverage |
| `test_string_expr.cpp::RegexMatch`, `RegexMatchPartialMatchSemantics` | Regex literal substring, start/end/both anchors, class, dot/newline, empty, case sensitivity and `(?i)` |
| `test_string_expr.cpp::RegexMatchClickHouseEdgeCases` | dot-star/lazy, alternation including empty branch, optional/repeated/nested groups, Unicode codepoints, escaped metacharacters, word boundaries, long literal, invalid backreference |
| `test_string_expr.cpp::ExtractLiteralsFromRegex`, `ExtractLiteralsFromRegexDirect` | final regex results map to the manual regex table; literal extraction and phase-one candidate shape are explicitly deferred |

### Master LIKE table entries

The contract cases retain each ordinary semantic row from
`PatternMatchConsistency.AllMatchersMustAgree`; entries sharing the same rule
may use the same dataset but remain separate named query cases.

| Source group | Patterns retained |
|---|---|
| Basic | `hello`, `hello%`, `%world`, `%llo%`, `h_llo`, `h%o`, `%`, `test%`, `%ing`, `%est%` |
| Overlap | `%aa%aa%`, `%aa%aa%aa%`, `%ab%ba%`, `%ab%ab%`, `%ab%ab%ab%`, `a%aa`, `aa%a`, `%a%a%`, `%a%a%a%`, `a%a` |
| UTF-8 | `caf_`, `%你%`, `a_b` over `aéb`, `%😀%`, `你%`, `%好` |
| Escapes/literals | `100\\%`, `%\\%`, `\\%%`, `file\\_name%`, `%\\\\%`, `\\%percent\\%`, `\\_underscore\\_`, regex metacharacters interpreted literally, and three trailing-backslash rejection forms |
| Edge | `_`, `__`, `___`, `%%`, `_%`, `%_`, empty pattern |
| Special bytes | `hello%`, `%world`, `hello%world`, `a%b`, `a__b`, `%` over space, tab, LF, and CRLF data |
| Embedded NUL | `%`, `a%b`, `a%`, `%b`, `a%c`, `hello%`, `%hello`, `a_b`, `_hello`, `hello_`, `a__b`, `_`, `__`, `ab`, and length-bearing literal-NUL patterns `a\0b`, `a\0%`, `%\0b`, `\0%`, `%\0`, `%\0%`, `a\0_` |

### Master direct literal-operation entries

- Prefix: `app`, `ban`, `cat`, `dog`, `hello`, `test`, `world`, empty,
  Unicode, literal metacharacters, rare `QOP`, and absent `QUOKKA`.
- Postfix: `e`, `world`, `peace`, `hello`, `test`, `world`, empty,
  Unicode, literal `%a`, rare `ZEBRA`, and absent `QUOKKA`.
- Inner: `pp`, `an`, `world`, `ello`, `hello`, `test`, `world`, empty,
  Unicode, literal `_`, repeated in-row `ab`, rare `ZEBRA`/`QOP`, and absent
  `QUOKKA`.

### Master regex entries

- Partial versus exact: `abc`, `^hello`, `world$`, `^exact$`.
- Character behavior: `[0-9]+`, `a.c`, dot over newline, `(?-s)` dot without
  newline, case-sensitive `Hello`, and `(?i)hello`.
- Empty/general: empty, `.*`, `.*?`, alternation including `(abc|)`.
- Structure: `abc(de)?fg`, `(ab){2,3}c`, a named numeric group, nested groups,
  and a rejected backreference.
- Literals/Unicode: `file\\.txt`, escaped `()[]{}`, word-boundary `error`, Yen,
  emoji, tab/CR, and a 200-byte literal.

## 已覆盖的重构前额外缺口

- Null rows whose stored payload would match the pattern.
- All-null input for empty and non-empty patterns.
- Every PatternOp across every declared pattern-capable backend configuration,
  with route and result expectations kept distinct.
- Literal `%`, `_`, and backslash under Prefix/Postfix/Inner, not only LIKE.
- Unicode literals and LIKE `_` code-point behavior across backend families.
- Empty string versus null under Match, literal operations, and regex.
- Long strings/patterns and duplicate postings across backend families.
- Invalid regex behavior for direct-query-capable configurations.
- Nullable and non-nullable backend profiles. Nullable pattern corpora expand
  only to nullable profiles; an all-valid portable corpus exercises every
  nullable/non-nullable memory/mmap profile for all five PatternOps.
- Hybrid VARCHAR readers participate in ordinary result cases. Low-cardinality
  inputs select Bitmap and high-cardinality inputs select Inverted under the
  shared deterministic Hybrid profile. Focused Postfix/Inner/Regex cases assert
  that the Bitmap delegate accepts and the Inverted delegate declines, while
  both paths still execute and check exact results.
- A 1,000-distinct-value `PatternHighCardinality` prefix case forces Bitmap's
  Roaring layout so both Bitmap mmap profiles actually take the file-backed
  open path rather than silently opening a small heap bitset.
- Portable byte-edge behavior across every capable backend uses valid UTF-8,
  including embedded U+0000. The separate arbitrary-byte differential corpus
  is FM-only because the pattern contract does not define invalid-UTF-8 VARCHAR
  ingestion and pinned master exercises those bytes only in FM's byte engine.

## 明确延后的特殊覆盖

- JSON-flat/composite path projection: path/domain behavior belongs to its
  composite reader tests rather than a flat scalar VARCHAR contract case.
- Ngram candidate extraction/refinement and regex literal extraction:
  candidate supersets and phase-two verification are separate contracts.
- Exact return strings from `extract_fixed_prefix_from_pattern`, non-string
  matcher overloads, and RE2-versus-Boost comparison machinery are helper
  implementation tests rather than `PatternMatchReader` operations. Their
  observable LIKE results and malformed-pattern errors are covered here.
- `RegexQueryUtilTest.cpp::InvalidUTF8ConsistencyTest.*`: malformed UTF-8 is
  outside the VARCHAR input contract stated by the source test itself. The
  FM-only byte differential corpus remains separately scoped and does not
  invent portable LIKE semantics for invalid VARCHAR bytes.
- Legacy FM general-LIKE candidate and phase-two recheck behavior, specifically
  `MatchOracleEqualsBruteForce`, `MatchGuardDeclinesUnselectiveAndStillExact`,
  `MatchGuardAcceptsRareHitOnLongRows`, and
  `MatchGuardAcceptsRareFragmentOnShortRows`. Current `FmIndexReader` rejects
  `Match` with `Unsupported`; treating the legacy candidate bitmap as an exact
  `PatternMatchReader` result would violate the contract.
- Hybrid selector policy and family-choice permutations beyond the shared
  deterministic low/high profiles are separate creation mechanism tests.
  Ordinary Hybrid VARCHAR readers are included in the result matrix.
- Executor fallback, expression lowering, batching, offsets, and multi-chunk
  behavior, including the FM executor tests, `RegexQueryTest.cpp`,
  `ExprArithMiscTest.cpp`, `ExprJsonIndexTest.cpp`, and
  `LikeConjunctExprTest.cpp`: consumer tests, not direct reader contract
  behavior.
- Skip-index statistics and ngram `CanHandleLiteral`/candidate filtering:
  pruning and candidate decisions use different interfaces and exactness
  guarantees from this exact row-domain reader contract.
- Corrupt/missing artifact entries, staging-directory lifetime, zero-copy
  ownership, and byte accounting: loader/storage lifecycle tests.
- Missing-binlog-row/default-value synthesis: ingestion/legacy adapter behavior
  occurs before this builder-reader contract.
- Benchmarks and unbounded fuzzing. Deterministic byte data and independent
  literal oracles remain covered as ordinary contract cases.

## 已知生产问题

See `22-pattern-match-issues.md`: Marisa truncates embedded NUL bytes at
build and lookup despite the length-aware string-view contract. Malformed RE2
patterns are also classified as `UnexpectedError` by the current shared matcher
constructor. Tests retain length-aware offsets and pin the current precise
invalid-regex rejection behavior without endorsing its error category.

## 静态用例计数

The file registers 207 declarative descriptors. Null-bearing ordinary cases
expand to the 12 nullable string profiles; all-valid ordinary cases expand to
all 24 string profiles. Focused FM, Bitmap, and Hybrid routing/layout cases
select every nullable/non-nullable memory/mmap profile in the named family that
is eligible for their dataset. The resulting source-derived parameter count is
2,372.

| Operation | Descriptors | Expanded parameters |
|---|---:|---:|
| PrefixMatch | 24 | 280 |
| PostfixMatch | 19 | 204 |
| InnerMatch | 29 | 264 |
| Match | 89 | 1,080 |
| RegexMatch | 46 | 544 |
| **Total** | **207** | **2,372** |

| Backend family | Profiles exercised | Expanded parameters |
|---|---:|---:|
| Bitmap | nullable/non-nullable, memory/mmap | 388 |
| Sort | nullable/non-nullable, memory/mmap | 384 |
| Inverted | nullable/non-nullable, memory/mmap | 384 |
| Hybrid | nullable/non-nullable, memory/mmap | 396 |
| Marisa | nullable/non-nullable, memory/mmap | 384 |
| FM | nullable/non-nullable, memory/mmap | 436 |
| **Total** | **24 named profiles** | **2,372** |

These registration counts were inferred from source and were confirmed by the
authorized full runs' 2,372-test Pattern suite count. After the shared Inverted
setup correction, the second run passed 2,322 parameters; the remaining 50 are
runtime-confirmed Marisa embedded-NUL result mismatches. They do not change the
intended coverage matrix or its contract-correct expectations.
