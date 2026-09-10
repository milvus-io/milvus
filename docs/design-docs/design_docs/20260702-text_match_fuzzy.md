# Fuzzy Text Match Filter (`text_match_fuzzy`)

**Author:** thc1006
**Date:** 2026-07-02
**Issue:** https://github.com/milvus-io/milvus/issues/50920
**Status:** Implementation Complete

---

## Background

Milvus supports `text_match` (per-token boolean match) and `phrase_match` (ordered match with slop) over analyzer-backed `VARCHAR` fields, served by the tantivy inverted index. Neither tolerates typos: a query token must match an indexed token exactly.

Text that users type or that comes from the wild — search boxes, logs, product names — frequently contains misspellings. A user who stored `"allergy"` and searches for `"alergy"` gets nothing back. This document describes `text_match_fuzzy`, an edit-distance filter that mirrors the `text_match` path end-to-end and tolerates a bounded number of typos.

```
text_match_fuzzy(text, "alergy", max_edit_distance=1)   # matches rows containing "allergy"
```

---

## Semantics

- **Per-token OR over analyzed tokens.** The query string is run through the field's analyzer; each resulting token becomes one fuzzy term query and a row matches if **any** token matches (boolean OR). This is the same shape as `text_match`.
- **`max_edit_distance` ∈ [0, 2].** `K` bounds the number of single-character edits (insert / delete / substitute, plus transpositions — see below). tantivy's fuzzy automaton hard-caps the distance at 2; a value outside `[0, 2]` is rejected at parse time and re-checked in the executor. `K = 0` degenerates to an exact term match.
- **Transpositions cost 1 (Damerau-Levenshtein).** Swapping two adjacent characters counts as a single edit, matching the Elasticsearch fuzzy default (`transpositions=true`).
- **Filter-only, no scoring.** Like `text_match` / `phrase_match`, the operator produces a bitset, not a relevance score. Fuzzy scoring / ranking is out of scope here and tracked in #50921.

---

## Differences from Elasticsearch fuzziness

The two points above (distance bound, transpositions) align with Elasticsearch fuzzy queries. This first version intentionally does **not** expose:

- `fuzziness=AUTO` (length-dependent distance) — only an explicit integer `K` is accepted.
- `prefix_length` (leading characters that must match exactly) — effectively `0`.
- `max_expansions` (cap on the number of expanded terms) — unbounded. A distance-2 query over a very large term dictionary can expand to many terms, so keep `K` small on high-cardinality fields.

None of these need a wire-format change to add later; they would be extra options, the same way `min_should_match` extends `text_match`.

---

## What changed (5 layers, mirroring `text_match`)

### 1. Protobuf (`pkg/proto/plan.proto`)

New op type `TextMatchFuzzy = 17`. The edit distance rides in the existing `UnaryRangeExpr.extra_values[0]` — the same slot `phrase_match` uses for slop and `text_match` for `min_should_match` — so no new field is introduced. `plan.pb.go` is regenerated, not hand-edited.

### 2. ANTLR grammar + Go parser (`internal/parser/planparserv2/`)

New grammar rule `text_match_fuzzy(Identifier, expr, max_edit_distance = IntegerConstant)`. The `max_edit_distance` option name is a **soft keyword** — the grammar accepts a plain `Identifier` in that slot and `VisitTextMatchFuzzy` validates it (case-insensitively) equals `max_edit_distance` — so it is *not* reserved and a scalar field literally named `max_edit_distance` remains usable elsewhere in a filter. `VisitTextMatchFuzzy` also validates `K ∈ [0, 2]` and stores it in `extra_values`. The shared prologue of the three text visitors (`text_match` / `text_match_fuzzy` / `phrase_match`) is extracted into `parseTextMatchOperand`.

### 3. C++ executor (`internal/core/src/exec/expression/`)

`ExecTextMatch` dispatches `TextMatchFuzzy` to `FuzzyMatchQuery`, reading and re-validating `K` from `extra_values`. A single `IsTextIndexOpType()` predicate replaces the hand-expanded op lists at the three routing sites (`ExecRangeVisitorImpl`, `DetermineExecPath`, `SupportOffsetInput`).

### 4. C++ index (`internal/core/src/index/TextMatchIndex.*`)

`FuzzyMatchQuery` mirrors `MatchQuery`; the growing-segment refresh (`Commit`/`Reload`) and bitset allocation shared by all three text-index queries live in one `PrepareBitset()` helper.

### 5. Rust / tantivy binding (`internal/core/thirdparty/tantivy/...`)

`fuzzy_match_query` tokenizes the query and ORs one `FuzzyTermQuery` (distance `K`, `transposition_cost_one = true`) per token via `BooleanQuery::union`, reusing the shared `tokenize_terms` helper. It is exposed over FFI as `tantivy_fuzzy_match_query`. As `K = 0` is exactly a term match, it short-circuits to the cheaper `match_query` (multiterms) path instead of building a Levenshtein automaton per token.

---

## Testing

- **Rust unit test** (`test_fuzzy_match_query`): distance 0/1/2 boundaries in both directions, multi-token OR, and out-of-range rejection (a wrap value that would otherwise truncate through `as u8`).
- **C++ gtest** (`TextMatch.GrowingNaive` / `TextMatch.SealedNaive`): a typo at distance 1 matches the same rows as the exact term on both growing and sealed segments, `not text_match_fuzzy(...)`, and executor rejection of an out-of-range or missing `max_edit_distance`.
- **Go parser test** (`TestExpr_TextMatchFuzzy`) and an L0 Python e2e case.

---

## Related

- #50921 — fuzzy scoring / ranking (out of scope for this filter-only operator).
