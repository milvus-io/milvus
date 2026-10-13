# FMIndex candidates for regex filtering

Status: implementation ported to the Segcore index contracts; build and execution validation pending.
Issue: [#53862](https://github.com/milvus-io/milvus/issues/53862).
Related LIKE work: [#52683](https://github.com/milvus-io/milvus/issues/52683).
Builds on the [FMIndex scalar index design](20260708-fm_index_scalar_index.md).

## Problem and scope

Regex filtering scans row bytes even when the pattern contains selective
literal conditions. Choosing the longest literal during extraction loses
information needed for index-specific planning: `foo.*bar` needs both literals,
and the shorter literal in `.*COMMON_LONG.*RARE` can be much rarer.
Alternation must also preserve branch coverage, including mandatory suffixes
such as `(ERROR|WARN).*timeout`.

Scope: existing `RegexMatch` on sealed VARCHAR with raw field data available.
Reuse the FM index format and canonical RE2 matcher. General LIKE semantics,
other field types and a new regex engine are out of scope.

## Extraction contract

Compile the canonical RE2 matcher before extracting any conditions. Preparation
returns a necessary Boolean condition with TRUE, literal, AND and OR nodes.
It is a candidate superset, never a final regex result. TRUE means no pruning.

`RequiredLiteral()` and `RequiredIndexCondition()` consume the public
`RE2::RequiredLiterals()` API from the pinned package:
`re2/20230301-milvus-regex1@milvus/dev#836c4df4da0690eb2122225b136b04b2`.
Conan applies this requirement to transitive consumers such as Arrow and
gRPC. The CMake target is `re2::re2`; the public header is `re2/re2.h`.
See the [package API contract](https://github.com/milvus-io/conanfiles/blob/864e3194c2c07aeed3f243cb83f2f8c1c5e69f86/recipes/re2/regex-v1/README.md).

The API reads the canonical matcher's existing AST. It returns an original-byte
mandatory scan substring and a necessary TRUE/LITERAL/AND/OR condition. Milvus
converts the returned condition into its internal representation. Both getters
share one lazy, thread-safe analysis per matcher. Matchers used only for recheck
do not perform extraction.

Bounds:

- Index atoms contain 2 to 64 original, case-sensitive bytes, including
  embedded NULs. A multi-byte rune qualifies. Fragments may end within a UTF-8
  sequence because lookups operate on bytes. Long runs retain bounded
  endpoints; truncation does not make those endpoints adjacent.
- Scan substrings contain at most 4096 bytes and may be one byte long.
- The condition has at most 127 nodes. AND may omit requirements to satisfy
  this budget; OR never drops a viable branch.
- The API visits at most 4096 AST nodes and reserves child visits before
  allocating child summaries. Traversal exhaustion yields TRUE and an empty
  scan literal for the whole regexp. This is separate from output-tree limits.
- Milvus declines extraction for patterns above 4096 bytes, RE2 programs above
  4096 instructions, or patterns that match empty input.

Optional, folded, non-singleton character classes and unknown operators
contribute no mandatory bytes. Singleton classes and escapes are decoded by
RE2. Zero-width assertions preserve byte adjacency and are evaluated during
RE2 recheck.

Dropping an OR branch is forbidden: `a|timeout` has a TRUE index condition,
because the short branch cannot supply a two-byte atom. TRUE in an AND can be
omitted. RE2 may factor shared prefixes or suffixes, so equivalent patterns can
produce different condition trees.

| Pattern | Example necessary condition |
| --- | --- |
| `foo.*bar` | `foo AND bar` |
| `.*COMMON_LONG.*RARE` | `COMMON_LONG AND RARE` |
| `(ERROR\|WARN).*timeout` | `(ERROR OR WARN) AND timeout` |
| `foo.*bar\|baz.*qux` | `(foo AND bar) OR (baz AND qux)` |
| `a\|timeout`, `(?i:foo)\|bar`, `foo\|`, `a*` | TRUE |
| `\Bfoo\|bar` | `foo OR bar` |
| `\x41\141\.` | decoded `Aa.` |

Raw scanning uses the API's mandatory substring as its Volnitsky prefilter.

## Planning and execution

`PrepareIndexCondition()` caches one immutable condition per thread, keyed by
full pattern bytes under fixed RE2 options. Copies are independent. Replacement
is published only after successful compilation; invalid patterns still fail.
Counts, row IDs and validity are never cached.

Each routing or candidate request counts every distinct retained atom once on
the **current segment's index** (at most 126 searches of at most 64 bytes).
Recursively select the least costly child of each AND; keep every OR child.
The cost of an OR is the sum of its children's occurrence counts. For example,
`(foo AND bar) OR (baz AND qux)` can select `bar OR baz` on one segment and
`foo OR qux` on another. Selection minimizes estimated locate work, not the
number of final candidate rows; it does not yet exploit intersections of
individually common atoms.

TRUE declines routing. Zero estimated occurrences accepts immediately;
otherwise routing uses this guard:
`selected_occurrences * sa_sample_rate < cost_ratio * TotalTokens`.
Occurrence counts upper-bound row counts and the sum prices every selected
branch, even overlapping branches. The guard does not price preparation,
bitmap operations or raw reads; performance measurements must guide tuning.

Exact contains lookups produce per-literal bitmaps, combined by intersection
and union. The generic evaluator can execute all retained conditions; production
currently executes the Count-selected condition. `PatternMatch(RegexMatch)`
returns only candidates and excludes NULL rows. A direct TRUE call returns all
non-null rows. The expression executor intersects its input bitmap, fetches
sealed-column strings and rechecks with the canonical RE2 matcher. Validity is
returned separately; NOT is applied to the rechecked result, never the candidate
superset. Offset-input evaluation retains the raw path.

Implementation: [analysis and selection](../../../internal/core/src/common/RegexLiteral.cpp),
[condition contract](../../../internal/core/src/common/RegexQuery.h),
[FM reader routing](../../../internal/core/src/index/scalar/fmindex/FmIndexReader.cpp),
[expression recheck](../../../internal/core/src/exec/expression/UnaryExpr.cpp).
`FmIndexReader` implements `IIndexReaderBase`, `IPatternMatchReader` and
`INullReader` in row coordinates; `PatternMatchIsExact` is false for both
`Match` and `RegexMatch`. It exposes neither predicates nor value lookup.
`FmIndexBuilder` creates `FmIndexArtifact`, and `FmIndexLoader` constructs the
reader over shared `FmIndexStorage`. `ExecFMPatternCandidates` consumes the
pattern/null contracts and performs the canonical recheck on retained sealed
field data. Test backend `PatternPolicy(RegexMatch)` is `CandidatesAndRun`.
The reader receives its cost ratio at construction (default 0.001); the current
loader uses that default, so live `SegcoreConfig` updates are not reflected in
an existing reader.

## Verification

Differential tests compare canonical RE2 matches with both the complete
condition tree and the Count-selected condition. They exercise nested
alternatives, short and case-folded branches, embedded NULs, Unicode, external
word boundaries, extraction budgets, cache replacement and concurrent
preparation. They also build two FM indexes with opposite `foo`/`bar`
selectivity and verify that Count chooses the literal that is rare on the
current index. Assertions check that every regex match passes the extracted
condition, allowing RE2 to factor the AST into equivalent trees.

Reader tests live in `index/scalar/fmindex/FmIndexReaderTest.cpp` and run in
`index_tests`. Native tests in `exec/expression/FmIndexExpressionTest.cpp` cover
sealed-column reads, batches, multiple chunks, NULL handling, input bitmap
intersection, NOT, offset fallback and invalid syntax. `RegexRequiredPrefixTest.cpp`
is collected into `all_tests` by the source test glob in `unittest/CMakeLists.txt`.
The reduced-recheck experiment asserts fewer sealed rows are read and verified
for dense selective workloads, alongside parity after every sample. Dependency
integration checks require the pinned RE2 package across Milvus, Arrow and gRPC.

### Native performance experiments

`FmIndexExpressionTest.RegexSealedColumnExperiment` runs in the ordinary C++ test suite with
1,000 rows, one warmup and three measured samples (median, microseconds),
rotating mode order. It records preparation, Count, candidate lookup, bitset
work, sealed-column reads, RE2 recheck and total time in gtest XML. Parity with
canonical RE2 is checked after every timed sample.

The stage experiment compares a raw literal/RE2 pipeline, full AND/OR
bitset execution and Count-selected execution. Each mode fetches selected
strings through the sealed chunk reader. A separate mixed-pattern sweep measures
physical-expression compilation, routing, reads and recheck on indexed and
unindexed segments, with expression-result caching disabled.

Workloads include independent common/rare literals, a shorter rare literal,
alternatives with a mandatory suffix, multiple OR branches, frequent and long
literals, short/folded OR branches and fallback. Dense and 1% input bitmaps,
cold/warm preparation and mixed patterns are covered. Candidate lookup time
includes bitmap population; the bitset stage covers Boolean combinations and
input intersection. Raw-read timing includes offset collection and pinning;
recheck also includes writing result bits.

`DISABLED_RegexEndToEndBenchmark` compares production raw scan and indexed
execution with cold/warm preparation across the same workloads. Expression
results are not cached, and build/load time is outside query timing.

```sh
internal/core/output/unittest/all_tests \
  --gtest_filter='RegexRequiredPrefix.*:FmIndexExpressionTest.*Regex*:FmIndex/FmIndexPatternExpressionTest.*' \
  --gtest_output=xml:fm_regex_ci.xml

internal/core/output/unittest/index_tests \
  --gtest_filter='*FmIndexReaderTest*Regex*:*PatternMatch*' \
  --gtest_output=xml:fm_regex_reader.xml

internal/core/output/unittest/all_tests --gtest_also_run_disabled_tests \
  --gtest_filter='FmIndexExpressionTest.DISABLED_RegexEndToEndBenchmark' \
  --gtest_output=xml:fm_regex_benchmark.xml
```
