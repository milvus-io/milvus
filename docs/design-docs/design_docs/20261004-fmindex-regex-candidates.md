# FMIndex candidates for regex filtering

Status: draft.
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

`RequiredIndexCondition()` uses the RE2 prefilter tree for the index contract.
RE2's parsed structure preserves independent concatenation requirements and
alternative branch conditions. Optional and case-folded bodies contribute TRUE.
The existing bounded analyzer supplies original byte spellings and a necessary
structural condition used to verify the mapped prefilter. It also supplies the
fallback when the adapter cannot safely reconstruct a condition. A fixed suffix
remains pending until a gap, so a long run does not exhaust the tree budget with
overlapping windows.

The implementation uses RE2's public `FilteredRE2` entry point, which invokes the
pinned version's parsed `Regexp` walker and `Prefilter` internally. `FilteredRE2`
returns lowercased atoms by design, so the adapter maps each atom back to exact
byte spellings from the canonical literal summary before FMIndex lookup.
Finding a spelling somewhere in the pattern is not sufficient: lowercased atoms
merge occurrences across branches, including folded and character-class forms.
A bounded implication check must prove that the original-byte condition implies
the mapped condition. If it cannot, the adapter returns the original condition.
For example, `(?i:foo).*bar|foo.*baz` retains `bar OR (foo AND baz)` and must not
require lowercase `foo` for the first branch.

Unmapped atoms, such as a long RE2 atom exceeding the retained 64-byte endpoints,
also return the original condition, preserving independent literals such as
`RARE` and the bounded endpoints. This keeps the Conan package boundary on RE2's
installed public headers while retaining safe AND/OR relationships. RE2 validates
the full pattern and decodes consuming escapes. A bounded monotone-probe pass
over `AllPotentials` reconstructs minimal satisfying atom sets; if its budget is
exceeded, the original structural condition is retained as well.

Bounds:

- “2gram” means **two bytes**, not two Unicode characters. Atoms are original,
  case-sensitive bytes, including embedded NULs; a multi-byte rune qualifies.
- Each atom is at most 64 bytes. Longer fixed runs contribute their first and
  last fragments. Truncated fragments are never joined as if adjacent.
- A condition has at most 127 nodes, hence at most 126 literal occurrences.
  AND may omit an over-budget child; an over-budget OR becomes TRUE in full.
- The adapter allows at most 8,128 `AllPotentials` probes and 16,129 recursive
  implication checks. Exhausting either budget retains the original condition.
- Patterns and RE2 programs above 4096 bytes/instructions, or matching empty
  input, produce TRUE. Parser nesting is capped at 64. Repeats use bounded
  summaries and exponentiation rather than expanded strings. Together these
  bound traversal, intermediate strings, final tree size and Count work.

Dropping an OR branch is forbidden: `a|timeout` becomes TRUE because `a` is too
short, not `timeout`. TRUE in an AND can be omitted. Unsupported consuming
atoms break adjacency; zero-width assertions do not. External word boundaries
are retained structurally but disable the RE2 range-prefix fallback because
anchored range analysis can omit matches using preceding row bytes.

| Pattern | Necessary condition |
| --- | --- |
| `foo.*bar` | `foo AND bar` |
| `.*COMMON_LONG.*RARE` | `COMMON_LONG AND RARE` |
| `(ERROR\|WARN).*timeout` | `(ERROR OR WARN) AND timeout` |
| `foo.*bar\|baz.*qux` | `(foo AND bar) OR (baz AND qux)` |
| `a\|timeout`, `(?i:foo)\|bar`, `foo\|`, `a*` | TRUE |
| `\Bfoo\|bar` | `foo OR bar`; never the unsafe anchored prefix `bar` |
| `\x41\141\.` | decoded `Aa.` |

Raw scanning retains the existing `RequiredLiteral()`/Volnitsky prefilter,
including its conservative alternation fallback. Its 4096-byte literal budget
is separate from the index's 64-byte atoms.

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
otherwise retain the existing guard:
`selected_occurrences * sa_sample_rate < fmindexCostRatio * TotalTokens`.
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
[FMIndex routing](../../../internal/core/src/index/FMIndex.cpp),
[expression recheck](../../../internal/core/src/exec/expression/UnaryExpr.cpp).

## Verification

The standalone differential harness builds against the **pinned RE2
2023-03-01 source** and compares canonical RE2 matches with both the complete
condition tree and the Count-selected condition. It exercises nested
alternatives, short and case-folded branches, embedded NULs, Unicode, external
word boundaries, extraction budgets, cache replacement and concurrent
preparation. It also builds two FM indexes with opposite `foo`/`bar`
selectivity and verifies that Count chooses the literal that is rare on the
current index. Sanitizer runs rebuild the harness and its RE2, GoogleTest and
libsais dependencies with ASan and UBSan.

The source-to-consumer review follows literal construction and adjacency,
optional/repeat weakening, OR absorption, budget exhaustion, prefix fallback,
cache publication, current-index Count, exact contains, NULL exclusion, input
intersection, raw recheck and final NOT. Native tests exercise the same
conditions through sealed-column reads, batches, multiple chunks, offset
fallback and invalid syntax.

### Native performance experiments

`FMIndex.RegexSealedColumnExperiment` runs in the ordinary C++ test suite with
1,000 rows, one warmup and three measured samples (median, microseconds),
rotating mode order. It records preparation, Count, candidate lookup, bitset
work, sealed-column reads, RE2 recheck and total time in gtest XML. Parity with
canonical RE2 is checked after every timed sample.

The stage experiment compares a raw literal/RE2 pipeline, the former
longest-literal/prefix candidate policy, full AND/OR bitset execution and
Count-selected execution. Each mode fetches selected strings through the sealed
chunk reader and records preparation, Count, lookup, bitset, raw-read, recheck
and total time. A separate mixed-pattern sweep measures physical-expression
compilation, routing, reads and recheck on indexed and unindexed segments, with
expression-result caching explicitly disabled.

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
  --gtest_filter='RegexRequiredPrefix.*:FMIndex.*Regex*:FMIndex/FMIndexPatternExecutorTest.*' \
  --gtest_output=xml:fm_regex_ci.xml

internal/core/output/unittest/all_tests --gtest_also_run_disabled_tests \
  --gtest_filter='FMIndex.DISABLED_RegexEndToEndBenchmark' \
  --gtest_output=xml:fm_regex_benchmark.xml
```
