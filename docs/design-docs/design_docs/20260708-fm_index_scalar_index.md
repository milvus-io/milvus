# MEP: FM-Index Scalar Index — Accelerating the Full String-Predicate Family

- **Created:** 2026-07-08
- **Updated:** 2026-07-14 (v2 — scope expanded from `InnerMatch`-only to prefix /
  suffix / equality / general `LIKE` / regex; grounded in the completed core
  implementation and its benchmarks)
- **Updated:** 2026-07-17 (v3 — this document describes the full FM-index design,
  but the **shipped PR delivers only `VARCHAR` + the three anchored `LIKE` forms**
  (prefix/infix/suffix). Equality (`==`/`!=`/`IN`/`NOT IN`), general `LIKE`,
  regex, occurrence-count API, and `JSON`/`TEXT`/`ARRAY` are deliberate
  follow-ups — declined at routing or rejected at index creation in this
  release. See Goals for the delivered-vs-deferred split.)
- **Author(s):** @xiaofanluan
- **Status:** Draft
- **Component:** C++ Core (index, exec/expression), QueryNode, Go index-param-check,
  Storage V3 (LOB)
- **Core implementation:** [`xiaofan-luan/fm-index-lite`](https://github.com/xiaofan-luan/fm-index-lite)
  (self-contained C++17, complete, benchmarked — this is PR 1 of the original plan)
- **Related Issues:** TBD
- **Released:** TBD

## Summary

Introduce a new scalar index type, **`FMINDEX`**, for `VARCHAR` columns. This
document describes the full FM-index design; **this PR delivers the first slice
— VARCHAR columns and the three anchored `LIKE` forms (prefix/infix/suffix)**,
with the rest of the string-predicate family (equality, general `LIKE`, regex,
occurrence counting) and other data types (`TEXT`/`JSON`/`ARRAY`) called out
below as deliberate follow-ups. The structure itself can serve the whole family:

| predicate | today (no index / NGRAM) | with `FMINDEX` |
|---|---|---|
| `LIKE 'P%'` (prefix) | brute scan, or TRIE (prefix-only index) | **exact bitmap, no recheck** |
| `LIKE '%P'` (suffix) | **brute scan** (`INVERTED` declines `PostfixMatch`) | **exact bitmap, no recheck** |
| `LIKE '%P%'` (infix) | brute scan, or NGRAM candidates **+ recheck** | **exact bitmap, no recheck** |
| `== "P"` / `IN` | brute scan / TRIE / INVERTED | **not accelerated in this PR** — declined so it falls back to the scan / an equality index (`INVERTED`); FMINDEX is a substring index, not an equality index |
| `LIKE 'a%b_c%'` (general) | brute RE2 scan, or NGRAM candidates + recheck | factor-pruned candidates + recheck (factors of **any** length, exactly matched) |
| regex (`RegexMatch`) | **brute RE2 scan** (`INVERTED` declines) | required-literal pruning → candidates + recheck |
| substring occurrence **count** | not available | exact, `O(|P|)` per pattern, batched ~1 M patterns/s |
| fuzzy substring (edit ≤ k) | not available (only token-level `text_match_fuzzy`) | future: exact, index-native |

An FM-index is a compressed full-text self-index (BWT + wavelet matrix + sampled
suffix array). *Backward search* answers "does substring `P` occur, in which
rows, at which offsets, how many times" in `O(|P|)` rank operations with **no
false positives**. Because every anchored variant (prefix = occurrence at a
row's start, suffix = occurrence at a row's end, equality = both) falls out of
the same structure, one index serves the whole family above.

The core data structure is **complete** as a standalone library
(`fm-index-lite`, Apache-2.0-clean, translated from Lance/Infini-gram Mini
designs with libsais). Measured on 1 GiB of documents: build 44.6 s, index
1.01× the text on disk (this specific long-text corpus at `sa_sample_rate=32`;
the footprint is row-length / alphabet / sample-rate dependent and is much higher
for short rows — see **Capacity** below), `Count` 137 K qps single / ~1 M qps
batched,
`MatchingDocs` (`LIKE '%x%'`) 51 K qps, zero-copy mmap load. This MEP specifies
how it plugs into Milvus.

## Changes from v1 of this document

v1 scoped `FMINDEX` to `InnerMatch` + occurrence counting and listed prefix /
suffix / equality / regex as non-goals. Two things changed:

1. The core library shipped with more capability than v1 assumed: anchored
   prefix/suffix queries (`LocatePrefixDocs` / `LocateSuffixDocs`), batched
   count/locate with memory-level parallelism (`CountBatch`,
   `LocateDocsBatch`), edit-distance search (`FuzzyMatchingDocs`), text
   recovery (`Extract`), longest-match scoring, ASCII case-folding, and
   zero-copy mmap (`LoadView`).
2. A survey of the Milvus expression layer showed `PostfixMatch`, `InnerMatch`
   and `RegexMatch` **run as brute-force scans whenever the field's index is
   `INVERTED`** (`InvertedIndexTantivy::ShouldUseOp` returns `false` for those
   ops — `internal/core/src/index/InvertedIndexTantivy.h:290-306`), and NGRAM
   accelerates them only as candidates that must be re-verified row by row.

So v2 promotes the full predicate family to goals. The per-segment data model,
licensing analysis, and count/decontamination motivation of v1 are unchanged
and carried over.

## Motivation

### Workloads

- **Operator/log filtering** — `LIKE '%error-9F2%'`, `LIKE 'req-2026%'`,
  suffix filters on file extensions, regex over semi-structured log lines.
  Exact, complete results expected; today these scan.
- **Training-data decontamination** — "does this benchmark 13-gram occur in my
  corpus, how many times, in which rows" at bulk throughput. The FM-index is
  the purpose-built structure for this (Infini-gram Mini indexed 83 TB).
- **Sequence matching** — reads against a reference over `{A,C,G,T}` (the
  historical home of the FM-index: BWA, Bowtie).
- **Exact phrase filtering for CJK / analyzer-hostile text** — byte-exact
  `%短语%` needs no tokenizer and cannot be distorted by one, complementing
  BM25 `phrase_match`.

### Why FM-index, given NGRAM / INVERTED / TRIE already exist

| | `TRIE` | `INVERTED` (tantivy) | `NGRAM` (tantivy) | **`FMINDEX`** |
|---|---|---|---|---|
| prefix | exact | exact (`PrefixMatch`) | candidates + recheck | **exact** |
| suffix | — | — (declines, brute scan) | candidates + recheck | **exact** |
| infix | — | — (declines, brute scan) | candidates + recheck | **exact** |
| general `LIKE` | — | regex scan over terms | candidates + recheck, literal must fit gram bounds | candidates + recheck, **literals of any length, matched exactly** |
| regex | — | — (declines, brute scan) | candidates + recheck, all literals must fit gram bounds | candidates + recheck, **any-length literals, matched exactly** |
| occurrence count | — | — | — | **exact, O(\|P\|)** |
| false positives before recheck | n/a | n/a | yes (gram intersection) | **none** |
| index size | small | large (posting lists) | large | **~1–2× text for long rows; higher for short rows (see Capacity)** |

Two structural advantages over NGRAM's gram-intersection:

1. **No false positives on the anchored/infix family** — the SA interval *is*
   the answer, so the per-row verification pass (chunk reads + RE2) disappears
   for `PrefixMatch`/`PostfixMatch`/`InnerMatch`/`Equal`, which are the
   overwhelming majority of real `LIKE` traffic (the Go planner lowers every
   leading/trailing-`%`-only pattern to exactly these ops —
   `internal/parser/planparserv2/pattern_match.go`).
2. **Literal factors of any length** — NGRAM can only use literals of length
   ≥ `min_gram` and prunes with fixed-size gram postings (a long literal
   becomes an intersection of many gram lists). The FM-index matches each
   factor **exactly** in `O(len)` regardless of length, so both very short
   discriminative literals and very long ones prune at full strength.

And one capability that no existing index has at all: the **exact occurrence
count** in `O(|P|)` — the Infini-gram primitive that Lance computes and
discards — which both unlocks decontamination workloads and doubles as a free,
exact selectivity estimate inside the executor (see Cost Model).

### The trade-off and why it fits Milvus

An FM-index trades locate latency for size: counting is `O(|P|)`, but
*enumerating* matches costs `O(occurrences × sample_rate)` LF steps of random
access. Milvus builds scalar indexes **per sealed segment** and fans out, so
each index stays small (hundreds of MB of text) and locate cost is bounded;
a count-first guard (below) catches the degenerate low-selectivity patterns.
Immutability maps onto the segment lifecycle: inserts create new segments,
deletes mask at query time, compaction rebuilds.

### Capacity (on-disk footprint and resident memory)

The headline "~1× text" holds ONLY for long rows, a small alphabet, and a high
sample rate. Two sizes matter and they scale differently — keep them separate.

**On-disk (serialized) blob** — what is uploaded and CRC'd. From the serializer,
the blob is exactly: header + wavelet words + sampled bitvector + sampled-SA
values + document-boundary array, i.e.

    blob ≈ wavelet_words                   (BWT wavelet over content + 1 separator/row; ~1–1.3× text)
         + sampled_bitvector               (marks sampled SA positions; ~text/8)
         + (text / sa_sample_rate) × 4     (sampled suffix array; 4 B/sample, 8 B if corpus ≥ 4 GiB)
         + 8 × rows                        (per-row uint64 document-boundary array)

The rank directories are **NOT serialized** — they are rebuilt at load — so
`fm_block_bytes` does **not** change the on-disk blob. The per-row 8-byte
boundary and the sampled-SA term dominate when rows are short. Measured
serialized-blob ÷ text, by average row length:

| avg row length | `fm_sa_sample_rate=8` (default) | `fm_sa_sample_rate=32` |
|---|---|---|
| 1 byte    | 10.76× | 10.01× |
| 30 bytes  | 1.69×  | 1.30×  |
| 1000 bytes| 1.39×  | 1.01×  |

(Full-byte random data at rate 8 ≈ 1.89×.)

**Resident memory after an mmap load.** The blob is viewed zero-copy from the
mapping (≈0 heap); what is actually on the heap is (a) the rebuilt wavelet **rank
directories** — this is the ONLY term `fm_block_bytes` shrinks (≈ wavelet_words ÷
(fm_block_bytes/8)), NOT the blob — and (b) the null bitmap (1 bit/row, nullable
fields only). Also, FMINDEX has no raw data of its own (`HasRawData()=false`), so
the raw `VARCHAR` column is still loaded for the scan / recheck fallback — budget
for **text + blob**, not blob alone.

Short-`VARCHAR` workloads should either raise `fm_sa_sample_rate` or prefer
`INVERTED`/`TRIE`; narrowing the boundary encoding and revisiting the default
sample rate are tracked follow-ups.

## Goals

**Delivered in this PR (v1):**

- New index type `FMINDEX` on `VARCHAR` columns only.
- **Exact, no-recheck acceleration** of the three anchored `LIKE` forms:
  `PrefixMatch` (`LIKE 'P%'`), `PostfixMatch` (`LIKE '%P'`) and `InnerMatch`
  (`LIKE '%P%'`) — all lowered from `LIKE` by the existing planner, no parser
  or proto changes.
- Cost-guarded execution: the index declines patterns where a scan is cheaper,
  using its own O(|P|) count (`queryNode.fmindexCostRatio`, default 0.001).
- Standard scalar-index lifecycle: per-sealed-segment build, V3 single-file
  serialization, mmap (zero-copy view), caching-layer pinning.
- Growing segments and any op the index declines fall back to the existing
  brute-force scan, so results are always complete and exact.

**Deliberately deferred to follow-up PRs (NOT in this release):**

- **Equality (`Equal`/`NotEqual`, `IN`/`NOT IN`).** The library *can* answer
  these exactly (anchored prefix ∩ length filter), but a set of exact values is
  served better by the raw-data scan or an equality-oriented index (`INVERTED`),
  so `ShouldUseOp` declines the whole equality family and the executor routes it
  away from FMINDEX. The `In`/`NotIn` overrides remain implemented (required by
  the `ScalarIndex` interface) and correct, just not routed to.
- **`TEXT`, `JSON` string paths, `ARRAY<VARCHAR>`, struct sub-fields** — other
  data types. Rejected at `create_index` for now (VARCHAR-only checker).
- **General `Match` (`LIKE` with interior `%`/`_`) and `RegexMatch`** two-phase
  (candidate + recheck) acceleration — reuses the NGRAM machinery; designed
  here but not wired in this PR (both stay on the brute-force path).
- **Occurrence enumeration / count API** (`(pk, offset)` lists, batched
  scoring) surfaced as a user-facing decontamination primitive.

## Non-Goals

- **Lexicographic range** (`>`, `<`, `BETWEEN` on strings) — needs forward
  navigation (ψ/select) the structure does not carry; stays with
  `INVERTED`/`STL_SORT`/`TRIE`. `ShouldUseOp` returns `false` for range ops.
- **Growing segments** — the FM-index is build-once (global suffix order);
  growing segments keep the existing brute-force path, same as other scalar
  indexes.
- **Relevance scoring / BM25** — `FMINDEX` filters; it does not rank.
- **Case-insensitive matching in v1** — the library supports build-time ASCII
  folding, but Milvus has no case-insensitive operator; exposing it would make
  indexed and brute-force results diverge. Reserved (see Future Work).
- **The end-to-end decontamination workflow** (chunking, n-gram extraction,
  thresholds) — an application on top of the count API, tracked separately.

## Core Library Capability Inventory

`fm-index-lite` (`milvus::index::fmindex::FMIndex`, C++17, deps: vendored
libsais only). Everything below is implemented, fuzz-tested against brute-force
oracles, byte-identical to sdsl-lite on 120 K queries, ThreadSanitizer-clean
for concurrent reads, and load-fuzz-hardened (8 K-iteration corrupted-blob fuzz
under ASan):

| API | answers | used by (Milvus op) |
|---|---|---|
| `Build(docs, sa_sample_rate=32, case_insensitive=false)` | build over row values; an out-of-byte-alphabet separator symbol is injected per row | segment index build |
| `Count(P)` / `CountBatch({P_i})` | exact occurrence count, `O(\|P\|)`; batch overlaps cache misses (~7× single) | count API, cost guard, factor ordering |
| `MatchingDocs(P)` | sorted unique row ids containing `P` | `InnerMatch` |
| `LocatePrefixDocs(P)` / `LocateSuffixDocs(P)` | rows that **begin** / **end** with `P` (anchored) | `PrefixMatch` / `PostfixMatch`, `Equal` |
| `LocateDocs(P)` / `LocateDocsBatch` | sorted `(row, offset)` of every occurrence | enumeration API, ordered-factor verification |
| `FuzzyMatchingDocs(P, k)` | rows containing a substring within edit distance k | future fuzzy-contains |
| `Extract(row, offset, len)` | recover original bytes (self-index) | future highlight/snippet |
| `LongestMatch(Q)` | longest substring of `Q` present in the corpus | future contamination scoring |
| `NextTokenCounts(P)` | follow-byte distribution (∞-gram LM) | future |
| `Serialize` / `SerializeToFile` / `Deserialize` / `LoadView(base, size)` | flat blob, 8-byte-aligned sections, **zero-copy mmap view**; rank directories rebuilt on load (RAM-only) | V3 entries + mmap |

Measured, 1 GiB text in 1 M documents, Apple Silicon, single-threaded build:
build **44.6 s** (peak RSS ≈ 9× corpus), disk **1.01× corpus**, `Count`
**7.3 µs**, `CountBatch` **1.0 µs/pattern**, `MatchingDocs` **19.7 µs**,
`FuzzyMatchingDocs(k=1)` 0.46 ms, `Extract(64 B)` 63 µs. Warm-mmap query
throughput equals in-RAM. Versus sdsl-lite (reference FM-index) on text:
build 1.6×, count 1.7×, batched count 5.7×, locate 1.6–3× faster at equal size.
The current compact-build memory pass was additionally measured on one long
printable-byte row: rate 8 peaks at **9.68× corpus** for 64 MiB and **9.66×** for
256 MiB, including the caller's resident source bytes (down from 14.32×), with
64 MiB build time improving from 3.25–3.30 s to 3.07–3.12 s.

## Query Semantics

### Operator mapping

The Go planner already lowers `LIKE` into distinct ops
(`internal/parser/planparserv2/pattern_match.go`: leading/trailing-`%`-only →
`PrefixMatch`/`PostfixMatch`/`InnerMatch`/`Equal`; anything with `_` or an
interior `%` → `Match`; regex stays `RegexMatch`). `FMINDEX` needs **no parser,
proto, or planner changes**:

| plan `OpType` | FM primitive | routed to FMINDEX in this PR? | result | recheck |
|---|---|---|---|---|
| `PrefixMatch` | `LocatePrefixDocs(P)` | **yes** | exact | none |
| `PostfixMatch` | `LocateSuffixDocs(P)` | **yes** | exact | none |
| `InnerMatch` | `MatchingDocs(P)` | **yes** | exact | none |
| `Equal` / `In` / `NotIn` | *(library can do `LocatePrefixDocs(P)` ∩ length filter, but…)* | **no** — `ShouldUseOp` declines the equality family; routed to scan / `INVERTED` | exact via fallback | n/a |
| `Match` | literal-factor decomposition → per-factor anchored/infix bitmaps, AND in ascending-count order | **no** (follow-up) — brute-force path | — | — |
| `RegexMatch` | `extract_literals_from_regex` → per-literal `MatchingDocs`, AND | **no** (follow-up) — brute-force path | — | — |
| `Range` (lexicographic) | — | **no** — `ShouldUseOp` = false → existing paths | — | n/a |

Exactness contract: for the three anchored pattern rows the bitmap returned by
`PatternMatch()` is final — byte-identical to the brute-force scan by
construction (the planner's lowering is already defined as byte-equivalent to
the regex path, and FM matching is byte-exact). Every declined/deferred op falls
back to the existing brute-force scan (or another index), so it is exact by
definition — FMINDEX only ever *adds* an accelerated path, it never changes a
result.

- **Normalization: none.** Byte-exact, case-sensitive — identical to the
  brute-force semantics of these operators today. (NGRAM's case-folding makes
  it *candidates-only* by nature; FMINDEX's exactness requires byte parity.)
- **NULL rows** are indexed as empty documents and can never match (patterns
  are non-empty); the executor's valid-data mask applies as usual.
- **Empty literals** (`LIKE '%'` / `LIKE '%%'`, lowered to an anchored op with
  an empty pattern): the index ACCELERATES, answering directly with all non-null
  rows via `IsNotNull()` (every string trivially contains the empty string);
  `ShouldUseOp` returns true for an empty pattern. Null rows stay excluded, so
  `NULL LIKE '%'` is not a match.

### General `LIKE` (`Match`): literal-factor decomposition

Tokenize the pattern with the same escape model as
`planparserv2.scanLikePattern` / `translate_pattern_match_to_regex`
(`\` escapes the next byte; unescaped `%`/`_` are wildcards) — the codebase
already ships this as `split_by_wildcard` (used by
`NgramInvertedIndex::CanHandleLiteral`); FMINDEX reuses it. The maximal runs
of literal bytes between wildcards are the **factors**. Anchoring: no leading
wildcard → first factor is prefix-anchored; no trailing wildcard → last factor
suffix-anchored; `_` counts as a wildcard for factor-splitting (its
exactly-one-char meaning is enforced by phase-2 verification, which also
handles UTF-8 character-vs-byte width).

```
LIKE 'req-2026%GET%_500'
  factors:  "req-2026" (prefix-anchored)   "GET" (infix)   "500" (suffix-anchored)
  phase 1:  LocatePrefixDocs("req-2026") ∧ MatchingDocs("GET") ∧ LocateSuffixDocs("500")
  phase 2:  RE2/LikePatternMatcher on surviving rows only
```

Phase-1 evaluation order: `CountBatch` all factors first (one batched backward
search, ~1 µs/factor), then AND bitmaps in ascending-count order with
short-circuit — the most selective factor prunes first, and factors whose
count already exceeds the guard threshold are skipped as non-pruning rather
than enumerated. A pattern whose factors are all unusable (e.g. `LIKE '%_%'`)
declines to the scan path.

*Deferred enhancement (measure first):* for `%`-only patterns (no `_`) the
factor **order** constraint can be verified inside the index from
`LocateDocsBatch` offsets (greedy non-overlapping subsequence check per row),
eliminating phase 2 entirely. Ship recheck-based first; add if phase-2 reads
dominate.

### Regex (`RegexMatch`): required-literal extraction

Milvus already ships `extract_literals_from_regex` (used by NGRAM's
`CanHandleLiteral`, `NgramInvertedIndex.cpp:813-817`) — PR 3 reuses it: phase 1
evaluates each extracted literal as `MatchingDocs(literal)` (batched via
`CountBatch` first for the guard/ordering) and ANDs the bitmaps. Phase 2 runs
the existing RE2 matcher (`internal/core/src/common/RegexQuery.h`) over
surviving rows. Two FMINDEX-specific improvements over the NGRAM version:
literals of **any length** are usable (NGRAM declines the whole query if any
literal is shorter than `min_gram`), and each literal is matched exactly
rather than via gram intersection.

*Upgrade path:* RE2's `re2::Prefilter` / `FilteredRE2` (the Google-Code-Search
analyzer, already a Milvus dependency) produces a full boolean AND/OR tree
over required atoms — strictly stronger pruning for alternation-heavy
patterns. Adopt when workloads show `extract_literals_from_regex` leaving
pruning power on the table.

Decline (scan-path fallback) when: extraction yields no required literal
(`.*`, alternations with an empty branch, pure char-classes), or the pattern
sets case-insensitive flags (`(?i)`) — literal matching would need folded
search the index doesn't do in v1. This mirrors how ripgrep and code-search
degrade, and is always correct: declining just means today's behavior.

### Count / enumeration API (kept from v1, now Phase 4)

`Count`/`CountBatch`/`LocateDocs` surface as the decontamination and analytics
primitive: exact per-segment counts summed at the proxy, `(pk, offset)`
enumeration unioned. The API shape (expression function `substr_count(field,
"P")` vs. dedicated RPC) remains the main open question; the recommended
default result granularity is PK-deduplicated document membership, with raw
per-occurrence counts opt-in. Batched scoring of thousands of n-grams rides
`CountBatch` (~1 M patterns/s per segment).

## Cost Model and Adaptive Execution (wired in this PR)

Backward search makes the index **self-costing**: a substring count is exact
and costs `O(|P|)` (~7 µs at 1 GiB) *before* any enumeration is attempted, via
the library's `Count`/`CountPrefixDocs`/`CountSuffixDocs` (no suffix-array
locate). The count-first guard lives INSIDE the single routing gate
`ShouldUseOp(op, pattern)`: the executor's
`PhyUnaryRangeFilterExpr::DetermineExecPath` passes the concrete literal for
the anchored pattern ops, and FMINDEX's override runs the count and declines
when enumeration would lose to a scan (the expr downgrades to the RawData
path — both paths are exact, the gate only picks the cheaper executor). An
empty pattern means "no literal information" and is judged on the op alone:

- Enumeration cost ≈ `occ × sa_sample_rate` LF steps (random access).
- Scan cost ≈ segment text bytes (sequential) — proportional to the **total
  tokens (bytes)**, *not* to the row count.
- Guard: accelerate iff `occ × sa_sample_rate < active_tokens ×
  queryNode.fmindexCostRatio` (config, **default 0.001**). Normalizing by
  tokens, not rows, makes the threshold **invariant to row length** (below).
  Degenerate patterns (`'%a%'`) fail immediately and scan, avoiding the cliff.

Here `active_tokens` is the FM-index text length (Σ non-null row byte lengths),
known for free. The form and default were set empirically (a throwaway
FM-index benchmark, `sa_sample_rate = 8`), sweeping two axes:

- **Selectivity** (30-byte rows, 1 M): a brute scan is ~constant (~5 ms — it
  touches every byte) while FM enumeration grows with the match count. The
  `occ / row` crossover is ~1.2 % for `InnerMatch`, ~0.5–0.6 % for the anchored
  ops (anchored locate is ~2× costlier per hit).
- **Row length** (10 KB rows, 30 K): a scan now traverses ~300 MB while FM
  enumeration for the *same match count* is unchanged, so at 1 % match FM beats
  a scan **~900×** (67 µs vs. 62 ms) and the `occ / row` crossover jumps past
  100 %. So the crossover as a fraction of *rows* scales ~linearly with row
  length — but as `occ / total_tokens` it is **stable** (~0.0003 at both 30-byte
  and 10 KB rows). Hence the token normalization: `fmindexCostRatio` is the
  (row-length- and sample-rate-independent) sequential-scan-per-token vs.
  random-LF-step cost ratio — measured ~0.002 on short/repetitive rows and
  ~0.0008 on a cache-adversarial corpus (1 GiB of per-row random text, where
  the BWT has no runs and every LF step misses cache while the scan's memchr
  fast-path streams at ~7 GB/s), hence the conservative 0.001 default: the
  guard must never make the index SLOWER than the scan it replaces. A fixed
  `occ / rows` ratio would
  wrongly decline FM on long-row (`TEXT`/long `VARCHAR`) columns.

Pattern length is second-order: the count is `O(|P|)` and near-free (**~0.1 µs**,
selectivity-independent) via the library's `Count`/`CountPrefixDocs`/
`CountSuffixDocs` (no locate); a 50-char vs. 5-char query at equal selectivity
differs only ~1.6× on the FM side. This all generalizes to a future token-mode
index (`total_tokens` becomes the token count, not the byte count; the formula
is unchanged).

The guard's O(|P|) count and the subsequent enumeration each run their own
backward search — the count's suffix interval is NOT currently threaded into the
locate, so the backward search is repeated once. At O(|P|) (microseconds, no
suffix-array locate) that repeat is negligible next to the enumeration's locate
cost; threading the interval through to skip it is a possible future
optimization, not something this PR does.

Because the count is exact and near-free, it is also useful *outside* FMINDEX
execution: conjunction reordering (evaluate the most selective `LIKE` first —
the `PhyLikeConjunctExpr` machinery already caches phase-1 results across
batches) and, later, planner-level selectivity estimates. v1 uses it only for
factor ordering and the guard; the planner hook is Future Work.

## Data Model

Unchanged from v1 in substance; the separator mechanism now reflects the
implementation.

- **One row = one document.** Matches never cross rows. `VARCHAR` capped by
  `proxy.maxVarCharLength` (65535), `TEXT` by `proxy.maxTextLength` (2 MiB,
  LOB-backed). Larger logical documents are split by the ingestion pipeline
  (overlap ≥ max-query-length − 1 if cross-chunk matches must be preserved —
  an ETL concern, not the index's).
- **Per-segment concatenation with one separator per row.** The library injects
  a separator symbol after every row value, so no match can span rows *by
  construction* — `Count` included, with no boundary post-filtering. The
  doc-start array maps positions → `(row, offset)`. The separator is a synthetic
  symbol **outside the byte alphabet** (see the next point), not `\0`.
- **`\0` is a first-class content byte** (decided 2026-07-14; it is legal in
  values — proto3 strings and Milvus's own validation, `utf8.ValidString` at
  `internal/proxy/util.go:2376`, both accept U+0000, and data may predate the
  index). The library's v2 redesign (fm-index-lite `DESIGN-v2.md`) moves the
  document separator **outside the byte alphabet** (dense symbols: sentinel,
  separator, then content bytes — the textbook generalized-suffix-array
  scheme), so `\0` in row values indexes normally *and* `\0` in patterns is
  queryable; a one-row build failure or a decline rule for NUL patterns no
  longer exists. Library cost (accepted): the SA build materializes an integer
  symbol text over the already-vendored `libsais_int` / `libsais64_long`
  (int-alphabet SA). The compact path now samples before repurposing the SA for
  BWT symbols and releases build scratch explicitly, so the int32 staging does
  not overlap a separate BWT allocation; query-path depth and index size are
  unchanged. **Implemented and verified** in fm-index-lite (out-of-alphabet
  separator, format v8, differential/oracle tests and ASan/UBSan green). Milvus
  vendors the v2 library.
- **Cross-segment aggregation** is union (membership/enumeration) and sum
  (counts); complete and non-overlapping because Milvus rows are atomic —
  a row lives in exactly one segment (flush and compaction move whole rows).
- **Deletes** mask at query time via the segment delete bitmap; **compaction**
  rebuilds the index.

## Architecture and Integration

All integration points verified against master (branch state of 2026-07-14).
`FMINDEX` inherits `ScalarIndex<std::string>` directly (not
`InvertedIndexTantivy` — no tantivy involvement).

### C++ core (`internal/core/src/index/`)

- **Vendor the library**: `fm-index-lite/src/index/fmindex/*` →
  `internal/core/src/index/fmindex/` (namespace `milvus::index::fmindex` is
  already Milvus-shaped); `third_party/libsais` (Apache-2.0) vendored; NOTICE
  attribution (Lance, libsais, qwt reference) carried over.
- **`FMIndexWrapper : ScalarIndex<std::string>`** (new `FMIndex.h/.cpp`):
  - `Build(n, values, valid_data)` / `BuildWithFieldData(field_datas)` —
    collect row `string_view`s (NULL → empty), single `fmindex::Build`.
  - `SupportPatternMatch() = true`; `PatternMatch(pattern, op)` switching per
    the mapping table — the existing `UnaryIndexFuncForMatch`
    (`exec/expression/UnaryExpr.h:631-686`) then routes
    `PrefixMatch`/`PostfixMatch`/`InnerMatch`/`Match` automatically; the
    `RegexMatch` leg (`UnaryExpr.h:713-738`) likewise.
  - `ShouldUseOp(op, pattern)`: `true` only for the three anchored pattern ops
    (`PrefixMatch`/`PostfixMatch`/`InnerMatch`), gated by the count-first cost
    guard when a literal is supplied; `false` (fall back to the scan / another
    index) for **everything else, including the `Equal`/`NotEqual`/`IN`/`NOT IN`
    equality family**, `Match`/`RegexMatch`, and `Range`. The default is `false`
    so any unhandled op safely downgrades rather than routing into a method that
    throws.
  - `In`/`NotIn` are required `ScalarIndex` overrides, but the equality family is
    declined by `ShouldUseOp`, so they are never routed to; they
    `ThrowInfo(Unsupported)` (like `Range` and `Reverse_Lookup`) so an accidental
    future route fails loudly rather than returning wrong rows.
  - The count-first guard is folded into `ShouldUseOp(op, pattern)` above — there
    is **no** separate `CanAccelerate` method; a future two-phase executor
    (PR 3) would consult it the way NGRAM consults `CanHandleLiteral`.
  - `HasRawData() = false`; `Reverse_Lookup` unsupported (phase-2 verification
    reads raw data through the segment, as NGRAM's does; `Extract` could serve
    this later — Future Work).
  - (PR 3, **not this PR**) two-phase entry points mirroring
    `NgramInvertedIndex::ExecutePhase1/2` for `Match`/`RegexMatch` — not
    implemented here; general `LIKE` / regex currently fall back to the scan.
- **Registration**: `ScalarIndexType::FMINDEX` (`ScalarIndex.h:39-49` +
  To/FromString), `FMINDEX_INDEX_TYPE = "FMINDEX"` (`Meta.h`), param key
  `fm_sa_sample_rate` (`Meta.h`), `std::optional<FMIndexParams>` in
  `CreateIndexInfo` (`IndexInfo.h:30-45`), dispatch in
  `IndexFactory::CreatePrimitiveScalarIndex<std::string>`
  (`IndexFactory.cpp:768-777`, beside the `ngram_params` branch),
  `IsMmapSupported()` list (`ScalarIndex.h:188-196`).

### Expression layer (`internal/core/src/exec/expression/`)

- Exact ops: **zero changes** — `SupportPatternMatch()`/`PatternMatch()`
  routing is already in place for any `ScalarIndex`.
- `Match`/`RegexMatch` two-phase: generalize the NGRAM-specific plumbing
  (`PhyLikeConjunctExpr`, `ExecuteNgramPhase1/2` hooks in `UnaryExpr.h:1089-1099`,
  the `PinWrapper<NgramInvertedIndex*>` pin at `UnaryExpr.h:1177`) to a
  candidate-generating-index interface both NGRAM and FMINDEX implement. The
  phase-1-cached / phase-2-per-batch structure and its conjunction
  optimization (`LikeConjunctExpr.cpp:61-101`) are reused as-is.

### Go layer (`internal/util/indexparamcheck/`)

- `IndexFMINDEX = "FMINDEX"` (`index_type.go`), added to `IsScalarMmapIndex`.
- `fm_index_checker.go` modeled on `ngram_index_checker.go`: field type ∈
  {`VARCHAR`} v1 (+`TEXT` in the LOB PR — note NGRAM's checker rejects TEXT
  today, so FMINDEX is the first substring index there); optional
  `fm_sa_sample_rate` ∈ [4, 256], default 8 (locate-latency-tuned; benchmarks
  showed 8 gives ~7-10× faster locate than 32 at ~30% larger index).
- Register in `conf_adapter_mgr.go` (`registerIndexChecker`, line 61 area).

### Serialization, mmap, caching

- **V3 single-file format**: implement `WriteEntries(IndexEntryWriter*)` /
  `LoadEntries(IndexEntryReader&, config)` (the `UploadUnified`/`LoadUnified`
  path, `ScalarIndex.h:242-259`) writing the library's flat blob
  (`SerializeToFile` streams; header + 8-byte-aligned payload sections).
- **mmap**: the blob is designed for `LoadView(base, size)` zero-copy — quad
  wavelet words, sampled bitmaps, sampled-SA values AND doc boundaries are all
  viewed in place (the samples through a narrow/wide-aware accessor, so the
  compact 4-byte on-disk form is served directly); the ISA table (Extract's
  anchor) is built lazily on first `Extract`, which no Milvus query op calls.
  What a load DOES rebuild in RAM is the rank directories, ~the size of the
  wavelet section itself — so mmap-resident memory is bounded by (and in
  practice well under) the blob size, which is what the load-resource
  estimator reports. Load validation is hardened (structural checks over the
  views, allocation-free; corrupted-payload integrity remains the storage
  checksum's job, as for all indexes). *Open item (a future format bump, pre-GA
  only; the current on-disk format is v8):*
  serialize the rank directories too and view them, making LoadView fully
  zero-copy and dropping the O(m) directory-build pass at load — requires a
  clamp-on-read (or verify-on-load) design so a corrupted directory stays
  memory-safe.
- **Caching layer**: standard pinning (`PinWrapper`) as with NGRAM; concurrent
  reads are lock-free by construction (all query methods `const`, TSan-verified).

### Build path

Standard DataCoord → IndexNode task flow; build is single-threaded per segment
and parallel across segments. For compact builds below 2 GiB, long-row peak
process RSS is ~9.7× the segment text at sample rate 8, including the caller's
source data (~8.7× build overhead); a 512 MB text column therefore needs about
5 GB. To keep concurrent builds within that envelope, DataCoord classifies
FMINDEX as a **heavy index task** for slot accounting: it uses the existing
configurable `dataCoord.slot.indexTaskSlotUsage` curve, not the lightweight
`scalarIndexTaskSlotUsage` curve. With the defaults, FMINDEX consumes 1 slot at
<=10 MiB, 4 slots at (10, 100] MiB, 16 slots at (100, 512] MiB, and at >512 MiB
`floor(field_bytes / 512 MiB) * 64` slots (so 1 GiB consumes 128). A worker unit
with 2 CPU / 8 GiB advertises 16 slots, so a (100, 512] MiB FMINDEX build drains
one such unit; that is sufficient for the measured ~5 GiB peak of a 512 MiB
column.
This reuses the vector-index concurrency mechanism without changing the worker
protocol. The slot scheduler is still concurrency control, not hard memory
admission: if one task is larger than every node's advertised capacity, the
existing best-effort path assigns it to the node with the most slots and drains
that node's remaining slots. A single oversized build can therefore still OOM
on an undersized node; a strict memory-capacity admission gate remains a
follow-up. `TEXT` values above the 64 KiB inline threshold are read through the
Storage V3 LOB reader (`20260407-text_lob_storage.md`) in the TEXT PR,
C++-native, no per-row CGO.

## What Else the Structure Accelerates

Answering "还有什么能加速的" explicitly. In this MEP's phases:

1. **Equality / `IN` / `NOT IN`** — the structure *can* answer these
   (anchored prefix ∩ length), but this PR **declines** them in `ShouldUseOp`
   (equality is served better by the scan or an equality index), so they fall
   back rather than route to FMINDEX. Listed as a library capability, not a
   shipped route.
2. **Multi-`LIKE` conjunctions** — `CountBatch` scores every literal of every
   conjunct in one MLP-batched pass; the most selective conjunct prunes first
   (plugs into `PhyLikeConjunctExpr`'s existing cross-batch caching).
3. **Exact substring counting at bulk throughput** — the decontamination /
   corpus-analytics primitive (Phase 4 API).
4. **Free exact selectivity** — every accelerated query gets an exact match
   count in µs, reusable by the executor for ordering and by a future
   cost-based planner hook.

Future work enabled by capabilities already in the library:

5. **Fuzzy substring filter** — `FuzzyMatchingDocs(P, k)`: index-native
   edit-distance-bounded *substring* search (typos in names/domains/codes),
   complementing token-level `text_match_fuzzy` (which needs an analyzer and
   matches whole tokens). Needs a small expr surface, e.g.
   `inner_match_fuzzy(field, "P", k)`, k ≤ 2.
   *Alignment with `text_match_fuzzy`* (analyzed, decided 2026-07-14):
   - **Edit model:** the library's backtracking search is plain Levenshtein;
     `text_match_fuzzy` is Damerau (adjacent transposition = 1). Alignment
     requires a library change — an opt-in `transpositions` flag adding one
     deterministic two-step branch (consume `P[i-1]` then `P[i]`, cost 1) to
     the DFS; memoization key unchanged, cost increase small (unlike
     substitution, it does not branch over the alphabet). The `k' = 2k`
     workaround is rejected (backtracking explodes at k' = 4). Lands with the
     fuzzy expr phase, not the vendoring PR (no caller before then).
   - **Token semantics:** for non-stemming analyzers (tokenize + lowercase),
     a `case_insensitive`-built FMINDEX with transpositions yields a
     *superset* of `text_match_fuzzy` matches → exact alignment via
     token-boundary recheck. Stemming/normalizing analyzers break the
     superset property — no alignment path; `text_match_fuzzy` stays.
   - **Byte vs. char:** tantivy counts edits in Unicode chars, FM in bytes —
     one CJK char substitution = 3 byte-edits, beyond a k ≤ 2 byte budget.
     Non-ASCII fuzzy alignment is out of scope (CJK fuzzy belongs to
     analyzer + `text_match_fuzzy`).
   - **Growing segments:** `text_match_fuzzy` has a growing index; FMINDEX is
     sealed-only — a structural coverage gap, documented, not chased.
   Position: ship as a distinct byte-level operator, not a replacement.
6. **Match highlighting / snippets without raw-data reads** — `Extract`
   recovers ±N bytes around each `(row, offset)` hit from the index itself
   (self-index property); useful when raw text lives in object-storage LOBs.
7. **Contamination scoring** — `LongestMatch` (longest span of a query present
   in the corpus) catches paraphrased/truncated overlap that exact n-grams
   miss; `NextTokenCounts` turns a segment into an ∞-gram LM.
8. **JSON string paths** — NGRAM already supports JSON via `json_cast_type`;
   FMINDEX can follow the same `CreateJsonIndex` route for string-valued paths.
9. **Case-insensitive mode** — build-time ASCII folding; a folded index is a
   strict superset generator for case-sensitive queries (candidates + recheck)
   and becomes exact the day an `ILIKE`-style operator exists.
10. **Token-level mode for token-id decontamination** — an FMINDEX over
    `ARRAY<INT>` fields (token sequences), the Infini-gram-parity form of
    n-gram scoring: symbols are token ids instead of bytes, sequences ~4×
    shorter, 13-token patterns take fewer backward steps than their ~50-byte
    equivalents. This is a *different workload*, not a replacement encoding:
    the byte index is the semantic requirement for `LIKE`/regex (arbitrary
    substrings cross token boundaries), and byte-exact matching is already
    char-correct for UTF-8/CJK (self-synchronizing encoding — a valid pattern
    cannot mis-align mid-character). Library-side the internals (QuadVector,
    WaveletMatrix4, rank9, dense remap) are already symbol-generic; the work
    is the `FMIndex` top layer (`libsais_int` SA, variable symbol table,
    `uint32_t*` API). **Library side designed** (fm-index-lite `DESIGN-v2.md`
    §5, token mode); the Milvus `ARRAY<INT>` integration remains future work
    pending a concrete decontamination consumer.
11. **Document-listing structure** (Sadakane) for occurrence-heavy patterns,
    and an **r-index** (run-length BWT) for heavily duplicated corpora —
    research-grade follow-ups if workloads demand them.

## Index Params and Config

| param | default | range | meaning |
|---|---|---|---|
| `fm_sa_sample_rate` | 8 | [4, 256] | SA sampling: space vs. locate latency. No effect on `Count`. Default 8 is locate-tuned (~7-10× faster locate than 32, ~30% larger index); raise to 64+ for count-only workloads |
| `mmap.enabled` | collection/field setting | — | zero-copy view load |
| `queryNode.fmindexCostRatio` (config, **wired**: paramtable -> SegcoreSetFMIndexCostRatio -> SegcoreConfig, read by `FMIndex::ShouldUseOp`) | 0.001 | (0, 1] | guard: brute-scan instead of enumerate when `occ × sa_sample_rate ≥ active_tokens × this`. Normalized by tokens (bytes), **not rows**, so it is row-length invariant. Measured crossover: ~0.002 (repetitive/short rows) down to ~0.0008 (random text, cache-adversarial for LF walks) — default 0.001 takes the conservative end |

Reserved for future: `case_insensitive` (see above).

## Phasing / PR Breakdown

1. **PR 1 — core data structure.** v1 ✅ complete as `fm-index-lite` (all
   primitives in the capability table, oracle/fuzz/TSan/ASan-load tests,
   benchmarks vs sdsl-lite and Lance). Vendoring waits on the library's **v2
   symbol-domain redesign** (fm-index-lite `DESIGN-v2.md`): out-of-alphabet
   separator (full `\0` support), token-level mode, and the Milvus-alignment
   APIs (streaming serialization sink, interval reuse, doc-id visitor,
   `EqualsDocs`, `MatchingDocsBatch`). Minimum bar for Milvus PR 2 is v2
   step 2 (symbol-domain byte mode); token mode and alignment APIs are
   additive.
2. **PR 2 — index wiring + exact ops (VARCHAR).** Vendor library; registration
   plumbing (C++ enum/Meta/IndexInfo/IndexFactory, Go checker); V3
   serialization + mmap; `PatternMatch` for
   `PrefixMatch`/`PostfixMatch`/`InnerMatch`; count-first guard. End-to-end:
   `LIKE 'P%'`, `LIKE '%P'`, `LIKE '%P%'` accelerated exactly, no recheck. The
   equality family (`==` / `IN` / `NOT IN`) is **declined** (falls back to the
   scan / an equality index), not accelerated in this PR.
3. **PR 3 — general `LIKE` + regex (two-phase).** Factor decomposition
   (`split_by_wildcard`) and required-literal extraction
   (`extract_literals_from_regex`), both reused; generalize the NGRAM
   two-phase interface; conjunction integration; decline rules.
4. **PR 4 — count / enumeration API.** Query surface (open question below),
   proxy cross-segment aggregation, PK-dedup default, `CountBatch` bulk path.
5. **PR 5 — TEXT / LOB.** Checker accepts TEXT; builder reads via LOB reader.
6. **Future.** Fuzzy-contains expr; highlight/Extract; JSON paths;
   case-insensitive; ordered-factor exact verification for `%`-only patterns;
   planner selectivity hook; doc-listing / r-index.

## Test Plan

- **Per-op differential fuzz (the core invariant):** for randomized corpora
  (varied alphabets, UTF-8/CJK, empty/NULL rows, rows containing `\0`) and
  randomized patterns, every accelerated op's final bitmap must be
  byte-identical to the pure brute-force executor's: anchored ops directly;
  `Match`/`RegexMatch` after phase 2. Includes escape-model edge cases
  (`\%`, `\\`, trailing `\`), `_` with multi-byte UTF-8 characters, patterns
  at row start/end, patterns longer than any row.
- **Factor/literal extraction units:** LIKE tokenization parity with
  `scanLikePattern`; regex decline cases (`.*`, `(?i)`, empty alternations);
  anchoring correctness.
- **Guard behavior:** degenerate patterns (`'%a%'`) take the scan path; the
  crossover threshold benchmarked, not assumed.
- **Cross-segment:** same data in 1 vs N segments yields identical
  union/sum; flush/compaction exercised; delete-bitmap masking.
- **Serialization/mmap:** V3 round-trip; mmap-view vs in-RAM result parity and
  throughput; corrupted-blob load fuzz (library ASan suite re-run in-tree).
- **Count API (PR 4):** counts vs brute-force; cross-segment sums; PK-dedup.
- **TEXT/LOB (PR 5):** values straddling the 64 KiB inline threshold;
  multi-MiB documents.
- **Domain smoke tests:** genome slice + reads; GSM8K-style n-gram
  decontamination sample.
- **Regression:** NGRAM behavior unchanged (including after the two-phase
  generalization); one-scalar-index-per-field constraint; AUTOINDEX unaffected.

## Compatibility and Rollout

Purely additive: new index type, no proto/parser changes, no change to NGRAM,
INVERTED, TRIE, or any existing routing (only a `false`→FMINDEX-handled
transition for ops that were brute-forced). PR 2 ships user-visible value on
its own (the exact `LIKE` family); each later PR is independently shippable.
Index params are validated at creation when present (`fm_sa_sample_rate` and
`fm_block_bytes` ranges, VARCHAR-only data type, duplicate keys rejected).
Unknown/extra param keys are **not** rejected — the base `scalarIndexChecker`
accepts them silently (same as every scalar index), so they are ignored.

**Scalar index engine version bump (4 → 5).** FMINDEX registers as scalar index
engine version 5 (`common.CurrentScalarIndexEngineVersion`), gated so creation
is refused until every **QueryNode** reports `>= MinScalarIndexVersionForFMINDEX`
(`ResolveScalarIndexVersion` aggregates QueryNode sessions only), so an old
QueryNode never loads an FMINDEX segment it cannot parse. NOTE: the build workers
(DataNode/IndexNode) are NOT part of this gate and must be upgraded no later than
the QueryNodes — the bundled upgrade script orders IndexNode/DataNode first;
gating the build workers on scalar capability is a follow-up. This is a
**capability-only bump** — the on-disk format of existing scalar indexes is
unchanged (exactly as the v3 → v4 JSON-path bump was). One consequence to be
aware of, inherited from the shared upgrade path and NOT specific to FMINDEX:
`compactionTrigger.ShouldRebuildSegmentIndex` compares **every** scalar index's
stored version against the current engine version, so with
`dataCoord.autoUpgradeSegmentIndex=true` all pre-v5 scalar indexes (INVERTED,
BITMAP, …) become "too old" and are rebuilt once via compaction — the same
one-time cost every prior scalar engine bump incurred. The flag defaults to
**off**, so self-managed clusters see no change; **Cloud environments that
enable it should expect (and schedule for) this one-time scalar-index rebuild**
when they roll to the version carrying this bump. No format migration is needed
otherwise.

## Open Questions

1. **Count/enumeration API surface (PR 4)** — expression function
   (`substr_count(field, "P")`) vs. dedicated RPC vs. query-output annotation.
   Recommendation: expression functions, following the `text_match_fuzzy`
   5-layer precedent (proto op + grammar + executor + index + checker).
2. **Two-phase generalization shape (PR 3)** — introduce a
   `PatternCandidateIndex` interface both NGRAM and FMINDEX implement
   (recommended), or keep parallel FMINDEX-specific expr paths.
3. **Guard default (PR-2)** — the guard normalizes by **tokens, not rows**
   (`occ × sa_sample_rate < active_tokens × fmindexCostRatio`), default
   `fmindexCostRatio = 0.001`, from FM-index benchmark sweeps (crossover
   measured ~0.002 on repetitive/short rows, ~0.0008 on 1 GiB per-row random
   text — the conservative end wins: never slower than the scan).
   Rationale: the `occ / rows` crossover scales with row length (~1.2 % at
   30-byte rows, >100 % at 10 KB rows where FM beats a scan ~900× even at 1 %
   match), while `occ / total_tokens` is stable (~0.0003) — a fixed row-ratio
   would wrongly decline FM on long-row `TEXT`/`VARCHAR`. **This PR ships the
   guard**: the count-first check lives in `FMIndex::ShouldUseOp(op, pattern)`
   and `queryNode.fmindexCostRatio` is wired paramtable → CGO → `SegcoreConfig`.
4. **FMINDEX + NGRAM coexistence on one field** — keep one-scalar-index-per-
   field in v1 (users choose). After PR 3 the only capability NGRAM retains
   that FMINDEX lacks is JSON-path indexing (FMINDEX JSON is Future Work);
   revisit routing only if a real workload needs both on one field.

## References

- Ferragina & Manzini, *Opportunistic Data Structures with Applications* (FM-index), JACM 2005.
- Infini-gram Mini (FM-index at 83 TB, decontamination): arXiv:2506.12229.
- Infini-gram (suffix array, 5T tokens): arXiv:2401.17377.
- Ceregini, Kurpicz, Venturini, *Quad Wavelet Matrix*, DCC 2024 (query-path structure).
- Vasimuddin et al., bwa-mem2, IPDPS 2019 (batched/MLP backward search).
- R. Cox, *Regular Expression Matching with a Trigram Index* (Google Code
  Search) — the prefilter-atoms approach, here with exact factors instead of
  trigrams; RE2 `Prefilter`/`FilteredRE2`.
- Lance FM-index: `lance_index::scalar::fmindex` (Apache-2.0; shape reference).
- `fm-index-lite`: github.com/xiaofan-luan/fm-index-lite — `DESIGN.md`
  (structure internals), `BENCHMARK.md` (all numbers cited here).
- Milvus TEXT LOB storage: `20260407-text_lob_storage.md`; fuzzy text match:
  `20260702-text_match_fuzzy.md`.
