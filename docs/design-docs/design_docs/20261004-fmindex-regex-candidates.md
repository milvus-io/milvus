# FMIndex candidates for regex filtering

- Status: draft implementation; maintainer approval is pending.
- Issue: https://github.com/milvus-io/milvus/issues/53862
- Related general LIKE work: https://github.com/milvus-io/milvus/issues/52683
- Base: [FMIndex scalar index design](20260708-fm_index_scalar_index.md).
- Scope: sealed VARCHAR, existing `RegexMatch`, canonical RE2 semantics.

## Candidate contract

`PartialRegexMatcher::RequiredIndexLiterals()` returns at most three mandatory
byte substrings, each at most 64 bytes long. They are **AND requirements**:
every match must contain every returned substring. FMIndex counts them and
uses just the least frequent requirement to generate candidate rows. Canonical
RE2 recheck is always required; ordering and other regex constraints are not
answered by substring membership.

The requirements combine two existing analyses:

- `RequiredPrefix()` uses the common prefix of RE2's public
  [`PossibleMatchRange` bounds](https://github.com/google/re2/blob/2023-03-01/re2/re2.h#L483-L498).
  This is a prefix of the matched substring, not necessarily of the row.
- The bounded structural analyzer behind `RequiredLiteral()` finds a mandatory
  interior literal. For index lookup, use its first and last 64 bytes (only one
  fragment for shorter literals). Substrings of a mandatory literal remain
  mandatory. Do not concatenate these fragments or infer exactness from them.

Equal or subsumed requirements are omitted. Keeping independent prefix and
interior requirements avoids replacing a rare prefix with a common interior.
Capping index fragments bounds backward-search work on long repetitive literals;
raw scanning retains its separate, up-to-4096-byte literal. Only the ends of a
long interior literal are considered, so a selective middle may be missed.
This loses performance opportunities, never matches. Partial UTF-8 code points
are safe for the byte index.

The range-based prefix declines patterns containing `\b` or `\B`: RE2 computes
bounds for an anchored search starting at the beginning of text, which can
exclude branches accepted with external word-boundary context. For example,
`\Bfoo|bar` can yield the prefix `bar` but partially matches `afoo`.
The structural analyzer can still prove `foo` for `\Bfoo`, but declines the
alternation. Escaped or quoted spellings may conservatively disable the prefix.
Patterns exceeding 4096 bytes or compiled instructions decline both analyses.
Empty-match patterns supply no mandatory bytes. A missing requirement means
scan, never an empty result; unsupported structural syntax retains a safe
range-based prefix if one exists.

| Pattern | Requirements / behavior |
| --- | --- |
| `ERROR.*timeout` | `timeout`, `ERROR`; count and choose the rarer one |
| `.*needle` | `needle` |
| `foo\|foobar` | `foo` via RE2 bounds |
| `\x41\141\.` | decoded bytes `Aa.` |
| `foo\|bar`, `foo\|`, `a*`, empty pattern | scan |
| `(?i)foo`, `[a-z]+` | scan |
| `\Bfoo` / `\Bfoo\|bar` | `foo` / scan |

No OR tree or new regex engine is introduced. Alternation and empty-match
correctness may be satisfied through fallback; this does not promise
acceleration for every supported RE2 pattern.

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
the bounded structural analysis shared by `RequiredLiteral()` and
`RequiredIndexLiterals()`. FMIndex combines interior requirements with the RE2
prefix; raw scanning keeps its existing interior literal and Volnitsky search.
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
groups and 4096 literal bytes. FM lookup fragments have a separate 64-byte cap.

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

Adoption and remaining work:

1. **Implemented: bounded interior requirements for FM candidates.**
   `RequiredIndexLiterals()` reuses the existing structural analyzer alongside
   the canonical prefix. The guard and candidate generation share this one
   extraction entry point, retain the existing occurrence-count selection,
   and always recheck. This follows the bounded-summary and conservative
   weakening principles above. Sharing immutable per-query analysis across
   these calls remains a separate interface change requiring native evidence.
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

An isolated RE2 2025-11-05 probe reproduced the FilteredRE2 incompatibility:
`ERROR.*timeout` matches `ERROR timeout`, but its atoms include lowercase
`error`, absent from the original bytes. Sending that atom directly to the
existing case-sensitive index would be unsound. The pinned RE2 source
documents the same lowercase contract.

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
plus `COMMON`, 20 true `RARE123END` rows and 20 `RAREwrong` false positives
for `RARE\d+END`. In the component and native fixtures, `RAREwrong` is prepended
so `RARE.*COMMON` exercises a rare prefix with a common interior requirement.
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
and regressing workloads. The component benchmark compares four modes in one
binary: full RE2 reference, raw Volnitsky + RE2, prior prefix-only FM policy,
and the new bounded-literal FM policy. Both FM policies use the same raw
prefilter on fallback. Timing includes separate matcher compilation and
analysis/counting for the guard and accepted candidate generation, mirroring
the existing wrapper calls. Parity checks are outside the timed region.
It uses the actual FM library but in-memory row vectors and `vector<bool>`;
segment dispatch, bitmap implementations and sealed-column reads need the
native benchmark. The fixed old-needle raw baseline omits legacy extraction
and is not a full pre-change executor baseline.

### Local evidence (2026-10-05)

Eight enabled correctness tests and all three opt-in component/raw/analysis
benchmarks passed. The soundness corpus covers 6,162 patterns against 1,589
rows using canonical RE2 and the actual FM library. Even intersecting all
returned requirements retained every canonical match in that corpus; choosing
just one is therefore covered. Long literals also exercise the byte cap and
independent prefix retention. The eight enabled test bodies passed ASan/UBSan
in a standalone assertion harness; prebuilt RE2/libsais and the native Milvus
binary were not sanitized.

Environment: Apple M1 Pro / arm64, Apple Clang 21, C++20 `-O2`, RE2
2025-11-05_2 and an error-header adapter. FM sample rate is 8 and cost ratio is
0.001. Three serial processes each warmed up once and measured nine samples
per mode with rotating order. The table reports the median of three process
medians. The prefix policy models repository HEAD before this change in the
same binary, with the same raw fallback and equivalent repeated compilation;
it is not a historical native-binary comparison.

| Pattern | New FM candidates | Raw prefilter (µs) | Prefix FM (µs) | New FM (µs) | Change vs prefix |
| --- | ---: | ---: | ---: | ---: | ---: |
| `RARE\d+END` | 40 | 3546.6 | 107.9 | 90.8 | -15.8% |
| `COMMON.*` | scan | 3297.2 | 3257.0 | 3259.2 | +0.1% |
| `.*RARE` | 40 | 3591.7 | 3671.7 | 117.9 | -96.8% |
| `x.*RARE123END` | 20 | 1654.3 | 1581.9 | 134.6 | -91.5% |
| `.*x{500}COMMONRARE123END` | 20 | 2027.2 | 2216.8 | 2132.5 | -3.8% |
| `RARE.*COMMON` | 40 | 3230.9 | 133.2 | 119.0 | -10.6% |
| `.*` | scan | 1340.5 | 1311.0 | 1427.4 | +8.9% |
| `RARE\|` | scan | 1486.2 | 1545.2 | 1422.7 | -7.9% |
| `ABSENT.*` | 0 | 2299.9 | 91.6 | 80.3 | -12.3% |

`.*RARE` and `x.*RARE123END` reduce FM candidates from a 20,000-row scan to
40 and 20 rows (99.8% and 99.9% pruning). The existing raw Volnitsky prefilter
already performs only 40 and 20 canonical checks respectively: the new benefit
is avoiding substring scans over all row bytes, not reducing those exact-check
counts further. `RARE.*COMMON` retains the rare prefix's 40 candidates instead
of selecting the common interior. Zero-hit lookup performs no canonical checks.

Regressions remain visible. The no-literal fallback is 8.9% slower in this
sample. An earlier measurement round showed +4.3% for the long repeated
pattern, while the final round shows -3.8%; its final cost is still 5.2% above
the raw prefilter alone. Small differences are noisy and do not establish
stable improvements. Before limiting FM fragments to 64 bytes, this long
pattern regressed about 15% against the prefix policy. The cap bounds lookup
work, but does not price repeated regex compilation, candidate reads or all
analysis costs. Do not claim a general or production end-to-end speedup.

The unchanged raw-scan benchmark's 14 syntax fixtures and the analysis-only
benchmark were also rerun in three processes. Earlier extraction optimizations
(buffer reuse, bounded repetition, and empty-input range avoidance) are already
in HEAD; their historical comparisons remain under
`/tmp/milvus-r01-optimization-20261005`. Current XML, sanitizer log, binary,
source snapshots and measurement summary are under
`/tmp/milvus-r01-interior-20261005`.

### Integration audit and remaining gate

Source tracing checked both `CandidateLiterals` consumers: no requirement
makes the guard decline and direct candidate calls return all non-null rows;
a zero count accepts an empty candidate set; otherwise the rarest requirement
feeds locate. No OR alternatives enter this AND-only interface. Unsupported,
optional and empty-match extraction paths are exercised in component tests.

The existing expression constructor validates regex before routing. The
FM-specific branch in `ExecRangeVisitorImpl` sends candidates through
`ExecFMPatternCandidates`, intersects input bits, fetches raw offsets and
rechecks canonically. Validity is returned independently; offset input retains
the raw path. Native fixtures now cover newly accepted interior patterns,
nullable/NOT parity, a rare prefix with common interior, and ordering false
positives. These wrapper/executor fixtures were updated but **not run locally**.

**Still required:** reconfigure/build native Milvus with pinned RE2 20230301,
run the wrapper/executor correctness fixtures and the native end-to-end
benchmark, and obtain maintainer agreement on the target branch and design.
The local native `all_tests` binary is absent. The existing CMake source glob
includes `RegexLiteral.cpp` after reconfiguration. No native integration,
production performance or design approval is implied. Sharing per-query
analysis and calibrating total routing cost remain follow-ups requiring that
environment; Boolean OR extraction is also deferred.
