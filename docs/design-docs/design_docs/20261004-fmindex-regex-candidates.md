# FMIndex candidates for regex filtering

Status: draft.
Issue: [#53862](https://github.com/milvus-io/milvus/issues/53862).
Related LIKE work: [#52683](https://github.com/milvus-io/milvus/issues/52683).
Builds on the [FMIndex scalar index design](20260708-fm_index_scalar_index.md).

## Problem and scope

Regex filtering scans row bytes even when a pattern contains a selective
mandatory substring. For example, `.*timeout` needs only rows containing
`timeout`. Use the existing FMIndex to find those rows, then apply the canonical
RE2 matcher. Results must equal a full regex scan, including NULL handling.

Scope: existing `RegexMatch` on sealed VARCHAR with raw field data available.
Reuse expression hooks and index format. General LIKE changes, other field
types, a new regex engine and acceleration of every RE2 pattern are out of scope.

## Design

1. Compile the canonical matcher before routing, so invalid patterns fail even
   when the candidate set is empty.
2. Extract mandatory byte substrings. Count each with FMIndex and choose the
   least frequent one. Accept zero occurrences immediately; otherwise use the
   existing guard:
   `occurrences * sa_sample_rate < fmindexCostRatio * TotalTokens`.
3. Retrieve candidate rows, intersect the input bitmap, fetch raw values and
   recheck with RE2. Return validity separately so NOT cannot match NULL.

`PatternMatch(RegexMatch)` returns a **candidate superset**, never final results.
Candidates use `ExecFMPatternCandidates`, bypassing the generic scalar-index
final-answer path. Missing requirements or a declined guard select raw scanning;
a direct candidate call without requirements returns all non-null rows.
Offset-input evaluation retains the raw path.

### Safe, bounded extraction

`RequiredIndexLiterals()` combines the RE2 matched-substring prefix with a
structurally derived interior literal. It returns at most three requirements,
each at most 64 bytes: the prefix and the first/last fragments of the interior
literal. Remove duplicates and subsumed fragments. Every requirement is
mandatory, so choosing any one is sound; these are AND terms, never OR branches.
Lookup is unanchored because a match can start anywhere within a row.

The structural analyzer tracks mandatory prefix, suffix and interior bytes,
plus whether a subexpression matches exactly one fixed string. Concatenation
joins only adjacent requirements; repetition uses its minimum count. Optional
bodies lose requirements, while variable repetition and truncation clear
exactness. Case-folded scopes contribute no fixed bytes. RE2 decodes escapes;
quote boundaries remain intact. Unsupported syntax falls back to a safe RE2
prefix or scanning.

Extraction stops for patterns above 4096 bytes/instructions or matching empty
input. Structural analysis caps nesting at 64 groups and literals at 4096
bytes. Raw scanning uses that longer literal with the existing Volnitsky
prefilter; FM lookups use 64-byte fragments to bound backward-search work.
A selective middle of a long literal may be missed, sacrificing pruning only.

RE2 range bounds are unsuitable for external word-boundary context:
`\Bfoo|bar` can yield prefix `bar`, yet matches `afoo`. Prefix extraction therefore
declines patterns containing `\b` or `\B`; structural analysis may still prove
an interior requirement for simpler forms such as `\Bfoo`.

| Pattern | Requirements / route |
| --- | --- |
| `ERROR.*timeout` | `ERROR`, `timeout`; choose the rarer fragment |
| `.*needle` | `needle` |
| `foo\|foobar` | `foo`, from RE2 bounds |
| `\x41\141\.` | decoded bytes `Aa.` |
| `foo\|bar`, `foo\|`, `a*`, empty pattern | scan |
| `\Bfoo` / `\Bfoo\|bar` | `foo` / scan |

### Preparation reuse

`PrepareIndexLiterals()` keeps one entry per thread, keyed by full pattern bytes
under the matcher's fixed RE2 options. Retain keys up to 4096 bytes and return
copies of the requirements. Construct replacement entries before publication;
invalid patterns still throw. Index statistics, row IDs and validity are never
cached, so the same pattern can safely query different indexes.

This removes repeated analysis between routing and candidate generation on the
same thread. Thread migration or another pattern causes a harmless miss.
The expression retains its own canonical matcher. The cost guard still estimates
locate work only; compilation, analysis and raw-column reads can outweigh savings.

Implementation: [literal analysis](../../../internal/core/src/common/RegexLiteral.cpp),
[FMIndex routing](../../../internal/core/src/index/FMIndex.cpp),
[expression recheck](../../../internal/core/src/exec/expression/UnaryExpr.cpp).

## Alternatives

[Google Code Search](https://github.com/google/codesearch) and
[Rust regex](https://github.com/rust-lang/regex) use bounded literal summaries
and conservative weakening. This design adopts those principles with existing
FM substring lookup. Boolean OR extraction is deferred: every alternative must
be covered, and the current requirement vector cannot represent that contract.

[RE2 FilteredRE2](https://github.com/google/re2/blob/2023-03-01/re2/filtered_re2.h)
is not a drop-in extractor: its atoms are lowercased, incompatible with the
existing case-sensitive byte index.
[PostgreSQL pg_trgm](https://github.com/postgres/postgres/tree/master/contrib/pg_trgm)
uses a lossy NFA-derived trigram filter and recheck; adopting its pipeline would
require different regex internals and index machinery.

## Validation

Ten standalone correctness tests passed, covering 6,162 patterns against 1,589
rows: alternation, escapes, empty matches, boundaries, Unicode/NUL/invalid UTF-8,
groups, quantifiers and truncation. Cached and uncached requirements agree;
intersecting all extracted requirements retains every canonical match in this
corpus. Cache tests cover replacement, independent copies, invalid-pattern
retry, oversize bypass and concurrent threads. Standalone ASan/UBSan passed;
cache fixtures also passed TSan. Prebuilt dependencies were not sanitized.

Native fixtures cover nullable/NOT, bitmap/batch/multi-chunk evaluation, offset
fallback, invalid syntax and different-index selectivity. **These fixtures and
native end-to-end performance remain unverified.** Local component runs used
RE2 2025-11-05 with an error-header adapter, not Milvus's pinned RE2 20230301.

### Component measurements

Fixture: 20,000 rows, each containing 500 `x` bytes plus `COMMON`; 20 rows append
`RARE123END`, another 20 prepend `RAREwrong`. Apple M1 Pro, Clang 21, C++20 `-O2`,
FM sample rate 8, cost ratio 0.001. Three processes; one warmup and nine samples
of ten queries per mode, rotating order. Values below are medians of process
medians, in microseconds per query. Build/load time is excluded.

All modes use the same rows and verify parity against full RE2. Raw scan uses
Volnitsky + RE2. Prefix FM uses only the RE2 prefix with the same raw fallback.
Cold preparation alternates equivalent patterns to miss on every query; warm
preparation reuses one key. Cache priming and parity checks are outside timing.

| Pattern | FM candidates | Raw scan | Prefix FM | Current FM, cold / warm |
| --- | ---: | ---: | ---: | ---: |
| `.*RARE` | 40 | 3578.9 | 3544.4 | 85.9 / 80.2 |
| `x.*RARE123END` | 20 | 1455.0 | 1482.7 | 88.0 / 78.5 |
| `.*x{500}COMMONRARE123END` | 20 | 1863.1 | 1911.5 | 1918.6 / 1865.1 |
| `COMMON.*` | scan | 3233.9 | 3272.8 | 3278.9 / 3243.5 |
| `.*` | scan | 1404.2 | 1416.1 | 1464.6 / 1403.0 |
| `ABSENT.*` | 0 | 2223.9 | 76.7 | 61.0 / 47.1 |

Interior literals avoid scanning row bytes when selective. The raw prefilter
already reduces RE2 calls to 40 and 20 for the first two patterns; FM mainly
avoids those per-row substring searches. Long-pattern and fallback cases show
little benefit or regressions. Small timing differences remain noisy.
These component measurements exclude segment dispatch and sealed-column reads;
they do not establish production end-to-end speedups.

### Reproduction

After building the native unit-test target, run the commands below. Native
benchmarks require expression-result caching to be disabled.

```sh
internal/core/output/unittest/all_tests \
  --gtest_filter='RegexRequiredPrefix.*:FMIndex.*Regex*:FMIndex/FMIndexPatternExecutorTest.*'

internal/core/output/unittest/all_tests --gtest_also_run_disabled_tests \
  --gtest_filter='RegexRequiredPrefix.DISABLED_*Benchmark:FMIndex.DISABLED_RegexEndToEndBenchmark' \
  --gtest_output=xml:fm_regex_benchmark.xml
```
