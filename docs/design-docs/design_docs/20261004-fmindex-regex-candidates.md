# FMIndex candidates for regex filtering

- Status: draft implementation; maintainer approval is pending.
- Issue: https://github.com/milvus-io/milvus/issues/53862
- Related general LIKE work: https://github.com/milvus-io/milvus/issues/52683
- Base: [FMIndex scalar index design](20260708-fm_index_scalar_index.md).
- Scope: sealed VARCHAR, existing `RegexMatch`, canonical RE2 semantics.

## Candidate contract

`PartialRegexMatcher::RequiredPrefix()` returns zero or one mandatory byte
literal, at most 64 bytes long. It uses the common prefix of RE2's public
[`PossibleMatchRange` bounds](https://github.com/google/re2/blob/2023-03-01/re2/re2.h#L483-L498).
This is a prefix of the **matched substring**, not necessarily of the row;
FMIndex therefore uses unanchored substring lookup. Partial UTF-8 code points
in the common byte prefix are safe requirements for the byte index.

Extraction declines patterns containing `\b` or `\B`. RE2 computes the bounds
for an anchored search starting at the beginning of text, which can exclude
branches accepted with external word-boundary context. For example,
`\Bfoo|bar` can yield the prefix `bar` but partially matches `afoo`.
Conservatively declining escaped or quoted spellings only loses pruning.
Patterns exceeding 4096 bytes or 4096 compiled instructions also decline.
A canonical match on empty input proves no nonempty literal is mandatory;
these nullable patterns skip range construction entirely.
A missing requirement means scan, never an empty result.

| Pattern | Requirement / behavior |
| --- | --- |
| `ERROR.*timeout` | `ERROR`; canonical recheck required |
| `foo\|foobar` | `foo` |
| `\x41\141\.` | decoded bytes `Aa.` |
| `foo\|bar`, `foo\|`, `a*`, empty pattern | scan |
| `(?i)foo`, `[a-z]+`, `.*needle` | scan |
| Any pattern containing `\b` or `\B` | scan |

FMIndex does not extract arbitrary interior literals or OR trees. Alternation
and empty-match correctness may be satisfied through fallback; this change
does not promise acceleration for every supported RE2 pattern.

## Execution and fallback

1. The expression constructs the canonical matcher before routing. Invalid
   patterns still fail when candidates are empty or every row is masked.
2. `StringLiteralForCostGuard` passes the pattern through `ShouldUseOp`.
   `PatternCandidateGuardAccepts` counts required literals; zero occurrences
   accepts immediately. Otherwise it retains the existing
   `occurrences * sa_sample_rate < fmindexCostRatio * TotalTokens` guard.
3. `FMIndex::PatternMatch` returns exact results for Prefix/Postfix/InnerMatch,
   but **only candidate supersets** for Match/RegexMatch. A direct candidate
   call without a requirement returns all non-null rows.
4. `CanUseFMPatternCandidates` and `ExecFMPatternCandidates` handle both LIKE
   and regex on sealed VARCHAR. They intersect the input bitmap, fetch raw
   candidate offsets and apply the cached canonical matcher. Candidate bits
   must never go through the generic scalar-index final-answer path.
5. Batch cursors, multi-chunk reads and the independent validity bitmap remain
   in use. NOT must not turn NULL into a match. Offset-input evaluation uses
   the raw path; other scalar indexes keep their existing routing.

The guard estimates locate work only; it does not price regex compilation,
analysis or candidate-column reads. Guard and candidate calls still compile
and analyze independently. Sharing query analysis would require a separate
interface change and native end-to-end evidence. No index format, storage
path, planner, proto, configuration or error-code mapping changes are needed.

## Raw-scan analysis responsibility

`RegexQuery.h` owns canonical matching and the public requirement methods;
`RegexQuery.cpp` retains LIKE translation utilities. `RegexLiteral.cpp` owns
the bounded structural analysis behind `RequiredLiteral()`. FMIndex uses the
prefix method; raw scanning uses the potentially stronger interior literal.
The existing NGRAM extractor is not a correctness gate: it can misinterpret
escape payloads such as the digits of `\x41`. Its own index path is unchanged.

Each subexpression summary has a mandatory prefix, suffix and interior literal,
plus `exact`, which is true only when every match is the stored string.
Concatenation may join only adjacent suffix/prefix requirements. Repetition
composes the minimum count; zero-minimum bodies lose requirements, exact-zero
bodies are empty, and variable counts clear exactness. Case-folded scopes
contribute no fixed bytes. Truncation also clears exactness. Sequence buffers
are reused, and repeated summaries use bounded exponentiation instead of
expanding the matched text. No state is shared between matcher instances.

The analyzer follows the pinned [RE2 syntax contract](https://github.com/google/re2/blob/2023-03-01/doc/syntax.txt)
for groups, quantifiers, flags, classes, quoted runes and complete escapes.
RE2 decodes consuming escapes. Quote boundaries are preserved: globally
removing `\Q\E` could turn `\0\Q\E12` into a different octal escape.
Unsupported syntax invalidates the entire tentative summary and falls back
to `RequiredPrefix()`. Limits are 4096 pattern bytes/instructions, 64 nested
groups and 4096 literal bytes. FM prefixes retain their separate 64-byte cap.

`EnsureRegexScanCache` constructs the literal and Volnitsky table only when
raw scanning occurs. FM candidate rechecks use the canonical matcher directly.
Both sequential and offset/bitmap raw consumers reuse the scan cache.

## Related implementations and adoption assessment (2026-10-05)

These are algorithm references, not additional regex engines or implemented
Milvus routes. Source inspection used RE2's pinned `2023-03-01` tag and the
commit-pinned references below for the other projects.

| Project | How it filters candidates | Fit for this implementation |
| --- | --- | --- |
| [Google Code Search][codesearch] | Builds conservative AND/OR trigram queries from syntax-tree summaries of exact strings, prefixes and suffixes; bounds and simplifies the sets. | Adopt bounded Boolean requirements over FM byte literals. FMIndex already supports arbitrary substrings, so a new trigram index is unnecessary. |
| [Rust regex][rust-literals] | Extracts bounded prefix/suffix literal sequences from HIR; concatenation uses cross products, alternation uses unions, and truncated literals become inexact. [Prefilters][rust-prefilters] select memchr/memmem, Teddy or Aho-Corasick and reject empty needles. | Adopt the budget/exactness discipline and strategy selection. Directly using its parser for RE2 input would introduce a separate syntax/Unicode compatibility obligation; SIMD multi-pattern scanning is useful only if multiple needles justify it. |
| [RE2 FilteredRE2][re2-filtered] | Derives an internal AND/OR atom tree, lets the caller find atoms, then rechecks possible regex matches. | Uses the existing engine, but is not a drop-in byte-literal extractor: returned atoms are lowercased, require compatible case-insensitive lookup, and the tree is private. It primarily filters multiple regexes against a text, rather than selecting rows for one regex. |
| [PostgreSQL pg_trgm][pg-trgm] | Transforms the canonical regex NFA into a lossy trigram graph, bounds states/arcs/trigrams, simplifies it conservatively, and rechecks retrieved rows. | Borrow conservative weakening and explicit work budgets. Its NFA accessors, character colors and trigram index are PostgreSQL-specific; porting the full pipeline exceeds this FMIndex feature. |

The existing summary analyzer is conceptually close to Code Search's bounded
exact/prefix/suffix analysis. Its principal gap is the single requirement:
alternation with different literals still falls back. Keeping a richer result
bounded is more important than supporting every pattern.

Recommended order for a follow-up:

1. **Evaluate existing interior requirements for FM candidates.** Reuse
   `RequiredLiteral()` as a candidate requirement alongside `RequiredPrefix()`;
   compare bounded analysis cost and occurrence counts before choosing a route.
   Keep canonical recheck and existing fallback. Share immutable per-query
   analysis between the guard and candidate generation if approved interfaces
   allow it, rather than recompiling the regex independently at each step.
2. **Add a bounded Boolean requirement representation only when needed.**
   `ERROR.*timeout` permits `ERROR AND timeout`; `(ERROR|WARN).*timeout`
   permits `(ERROR OR WARN) AND timeout`. For AND, using just a selective
   mandatory child is sound. OR must cover every branch, using candidate
   union; it cannot pick only the rarest branch. `foo|` requires all rows.
   Budget exhaustion must weaken the filter, never truncate away alternatives.
   The current `CandidateLiterals` vector means all literals are required;
   returning OR alternatives through that interface would be incorrect.
3. **Use the available FM primitives and retain a cost guard.** Count or
   `CountBatch` can compare selectivity before locate; counts are occurrences,
   not distinct rows. Charge analysis, bitmap combinations and raw reads as
   well as locate in a measured cost model. Aho-Corasick/Teddy target scanning
   many needles in row bytes, not FM-index lookup; defer that extra machinery
   until workloads establish a benefit. RE2 remains the final matcher.

An isolated feasibility probe used the current analyzer and native FM library
on the existing 20,000-row fixture. It changed only the probe's requirement
selection, not the production wrapper or executor:

| Pattern | Current prefix route / candidates | Interior-literal probe / candidates |
| --- | --- | --- |
| `x.*RARE123END` | scan / 20,000 | FM `RARE123END` / 20 |
| `.*RARE` | scan / 20,000 | FM `RARE` / 40 |
| `RARE\|OTHER` | scan / 20,000 | scan / 20,000 |
| `RARE\|` | scan / 20,000 | scan / 20,000 |

All canonical matches survived the probe's filtering. This demonstrates
candidate reduction for these fixtures, not latency improvements or native
integration. The same RE2 2025-11-05 environment also reproduced the
FilteredRE2 incompatibility: `ERROR.*timeout` matches `ERROR timeout`, but
its atoms include lowercase `error`, absent from the original bytes. Sending
that atom directly to the existing case-sensitive index would be unsound.
The pinned RE2 source documents the same lowercase contract.

[codesearch]: https://github.com/google/codesearch/blob/74a12a911a79b901d1158c48d011b2da1b090fc9/index/regexp.go
[rust-literals]: https://github.com/rust-lang/regex/blob/72d650cb0a880a01ab6dc2137c0888e8f89740f7/regex-syntax/src/hir/literal.rs
[rust-prefilters]: https://github.com/rust-lang/regex/blob/72d650cb0a880a01ab6dc2137c0888e8f89740f7/regex-automata/src/util/prefilter/mod.rs
[re2-filtered]: https://github.com/google/re2/blob/2023-03-01/re2/filtered_re2.h
[pg-trgm]: https://github.com/postgres/postgres/blob/b83b7ddf7a64dce41624b5c0dabefd74dfe10ec0/contrib/pg_trgm/trgm_regexp.c

## Validation and usage

The focused tests cover alternation, escapes, empty matches, external word
boundaries, Unicode/NUL/invalid UTF-8, nested groups, quantifiers, truncation and
raw-scan selectivity. Native fixtures additionally exercise nullable/NOT,
bitmap/batch/multi-chunk execution, offset fallback and invalid syntax.

After building the normal native unit-test target:

```sh
internal/core/output/unittest/all_tests \
  --gtest_filter='RegexRequiredPrefix.*:FMIndex.*:FMIndex/FMIndexPatternExecutorTest.*'
```

Performance fixtures are opt-in and verify result parity rather than asserting
a speedup. The component and raw-scan fixtures use 20,000 rows of 500 `x` bytes
plus `COMMON`, 20 true `RARE123END` rows and 20 `RAREwrong` false positives.
The analysis fixture isolates extraction from compilation and row scanning.
The native end-to-end fixture includes expression compilation and column reads
on equivalent indexed/unindexed sealed segments. Build/load time is excluded.

```sh
internal/core/output/unittest/all_tests --gtest_also_run_disabled_tests \
  --gtest_filter='RegexRequiredPrefix.DISABLED_*Benchmark:FMIndex.DISABLED_RegexEndToEndBenchmark' \
  --gtest_output=xml:fm_regex_benchmark.xml
```

Disable expression caches for the end-to-end run. Record CPU, build flags, RE2
version, cost ratio, candidate counts and route selection; include declined
and regressing workloads. Component fallback is a direct RE2 scan, not the
production Volnitsky executor. The fixed old-needle raw baseline omits legacy
extraction and is not a full pre-change executor baseline.

### Local evidence (2026-10-05)

Seven enabled correctness tests and all three opt-in benchmarks passed. The
soundness corpus covers 6,162 patterns against 1,589 rows. The seven current
correctness test bodies also passed ASan/UBSan in a standalone assertion
harness; prebuilt RE2/libsais and the native Milvus binary were not sanitized.
Candidate counts, routing choices and all 14 raw-scan exact-check counts were
unchanged by the latest optimization.

Environment: Apple M1 Pro / arm64, Apple Clang 21, C++20 `-O2`, RE2
2025-11-05_2 and an error-header adapter. Before/after refers to the uncommitted
implementation immediately before the optimization, not repository HEAD.
Three serial process pairs alternated order. Each process warmed up once and
measured nine samples; tables report the median of the three process medians.

| Extraction only, warmed matcher | Before (µs) | After (µs) |
| --- | ---: | ---: |
| 3000-byte literal run | 971.940 | 188.631 |
| Repeated literal | 7.559 | 2.504 |
| Empty-alternative prefix | 2.406 | 0.024 |
| Escaped literals | 11.646 | 12.073 |

Extraction excludes matcher compilation, first-use DFA setup and row scanning.
Buffer reuse and bounded repetition reduce analysis work; the empty-input
probe avoids unnecessary range analysis. These are not query speedup claims.

| FM component workload | Candidates / 20,000 | Before (µs) | After (µs) |
| --- | ---: | ---: | ---: |
| Selective | 40 | 68.375 | 68.625 |
| Unselective, scan fallback | 20,000 | 1021.000 | 1067.583 |
| No prefix, scan fallback | 20,000 | 23605.500 | 23804.583 |
| Empty alternative, scan fallback | 20,000 | 1750.667 | 1324.959 |
| Zero hits | 0 | 59.125 | 57.083 |

Selective candidates remove 99.8% of exact checks. Raw-scan query medians vary
from -1.17% to +4.67% across the 14 fixtures; component unselective fallback
increases 4.56% in this sample. These measurements do not establish a stable
whole-query speedup or rule out small regressions. Keep fallback costs in
future comparisons. Full local XML, logs and source snapshots are retained in
`/tmp/milvus-r01-optimization-20261005`.

**Still required:** reconfigure/build native Milvus with pinned RE2 20230301,
run the wrapper/executor correctness fixtures and the native end-to-end
benchmark, and obtain maintainer agreement on the target branch and design.
The existing CMake source glob includes `RegexLiteral.cpp` after reconfiguration.
No native integration, end-to-end performance or design approval is implied.
