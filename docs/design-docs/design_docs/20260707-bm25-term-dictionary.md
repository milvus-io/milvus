# BM25 Term Dictionary for Lexical Features

**Author:** thc1006
**Date:** 2026-07-07
**Issue:** https://github.com/milvus-io/milvus/issues/50922
**Related:** #50921 (fuzzy BM25, first consumer) · #50920 (`text_match_fuzzy`, filter-only, merged) · epic #50926
**Status:** Draft, for discussion

---

## 1. Background

Milvus BM25 does not go through Tantivy; it runs on the knowhere **sparse-vector** index. At ingest the analyzer tokenizes text and each term is hashed via `HashString2LessUint32`, and **the term string is discarded**. Corpus statistics keep only `map[uint32]int32` (hashed-id to document-frequency). At query time the IDF oracle tokenizes and hashes the query the same way.

Verified against master (2026-07-07):

- The term string is dropped at `internal/util/function/bm25_function.go:195-196`, inside `(*BM25FunctionRunner).run`: `hash := typeutil.HashString2LessUint32(token); embeddingMap[hash] += 1`. The `embeddingMap` is already per-document deduplicated (one entry per unique token), so what feeds the stats is document-frequency, not collection-frequency.
- The tokenizer is the Tantivy analyzer reached over CGO (`internal/util/analyzer`), so tokens originate on the Rust side.
- Stats hold no strings: `internal/storage/stats.go:301-305`; `AppendBytes` does `rowsWithToken[index] += 1` once per row, so df counts documents, not occurrences.
- Query IDF is built once per shard at the delegator, before segment fan-out: `internal/querynodev2/delegator/delegator.go:389` (`prepareSearchFunction`) calls `buildBM25IDF` (`delegator_data.go:1265`), which rewrites the request's `PlaceholderGroup` in place (`delegator_data.go:1351`). No reverse (term to id) map exists.
- BM25 stats are flushed as a per-segment artifact via a dedicated channel: `internal/flushcommon/syncmgr/growing_source.go` (`BM25FieldIDs`, `BM25StatsLogIDs`, `CommittedBM25Stats()`).
- Compaction rebuilds BM25 stats from the surviving output rows, not by summing child stats: `internal/datanode/compactor/segment_writer.go:366,394` call `stats.AppendBytes(...)` per output row.
- `SegmentInfo` (`pkg/proto/data_coord.proto:384`) carries `bm25statslogs = 27`; its fields run up to `commit_timestamp = 36`, so the next free tag is **37**.

There's no vocabulary, so the engine can't enumerate corpus terms near a query term. That gap blocks fuzzy BM25 (#50921), scored prefix, and suggester.

## 2. Goals / Non-goals

**Goals (v1)**
- Persist a per-segment **term dictionary** (an FST over the analyzed term strings) so the corpus vocabulary can be enumerated and fuzzy-matched.
- Expose a query-time **expand** step and land **fuzzy BM25 end-to-end**: expand a query token to corpus terms, hash them, and score through the **existing** sparse IDF path so relevance stays on today's BM25.

**Non-goals (explicit deferrals, not oversights)**
- **Scored prefix and suggester.** They reuse the same expand API but come after fuzzy. Deferring prefix also avoids the analyzer-pipeline question in section 3.6.
- **LOB-backed TEXT.** `stringInputsFromRecord` (`record_materializer.go:587-588`) already rejects `*array.Binary` with "cannot materialize bm25 from text binary values without lob decoding". v1 targets **VARCHAR** (and inline TEXT); LOB TEXT is a follow-up.
- **Growing-segment dictionary.** v1 is **sealed-first**: a growing, un-flushed segment has no dictionary yet, so fuzzy won't reach its most recent rows even though exact BM25 does (section 3.6). Documented recall gap, closed at flush.
- **Multi-analyzer BM25** (`multi_analyzer_bm25_function.go`): one dictionary assumes one analyzer per field. v1 **rejects enabling the dictionary on a multi-analyzer BM25 field at creation** rather than build a wrong one.
- Highlight (today's `GetHighlight` re-analyzes stored text; needs no dictionary).

## 3. Design

### 3.1 Route B, with df kept out of the FST
Persist the vocabulary and expand-then-score on the sparse path. The important refinement: the FST stores only the **term set** (the vocabulary), and the authoritative `term to df` stays in the existing `BM25Stats` / IDF oracle. That split is what makes the rest simple:

- The vocabulary is **delete-invariant**: soft-deleting a document never changes which strings a term maps to. So cross-segment merge is a **union**, and never needs document-frequency arithmetic.
- Document-frequency for scoring already lives in `BM25Stats`, which compaction already rebuilds correctly per surviving output row. The dictionary never re-derives df, so it can't disagree with scoring.
- The FST may carry a `u64` value as an approximate df **only** as a candidate-ranking hint (section 3.6); it is never read for scoring.

### 3.2 FST on the Rust side
The dictionary is an FST built and queried on the **Rust side** in the tantivy-binding, reusing the crates Tantivy already pulls in: `tantivy-fst` for the FST and `levenshtein_automata` for the fuzzy DFA (the same ones `FuzzyTermQuery` uses). The binding will declare and pin `fst` / `levenshtein_automata` in its own `Cargo.toml` even though they arrive transitively today. The Go side only orchestrates persistence, load, and the expand call. Rationale: the querynode is already CGO-heavy, BM25 tokens come from the Rust analyzer, and keeping one FST implementation avoids a new Go dependency.

New FFI (returns data, not a bitset):
- `build(sorted terms, optional df hints) -> serialized FST bytes` (the `fst` builder requires keys inserted in sorted order).
- `union(child FST bytes...) -> merged FST bytes` (vocabulary union for compaction).
- `expand_fuzzy(term, max_edits, prefix_len, max) -> [(term, df_hint)]` (build a `levenshtein_automata` DFA, wrap it in a small `fst::Automaton` adapter as tantivy does internally, intersect with the FST).
- `enumerate -> stream of terms` (debugging, merge).

The FFI contract (opaque handle ownership, result-buffer layout with explicit term lengths rather than NUL termination, no panic across the boundary, and caching the `LevenshteinAutomatonBuilder` for distances 0 to 2 so it isn't rebuilt per token) is defined in PR1.

### 3.3 Where the vocabulary is captured
Capture happens at the **growing to sealed flush**, from the segment's retained input text, reusing the existing BM25-stats flush channel (`growing_source.go`): the flush already produces per-segment BM25 stats, and it has the input VARCHAR in hand, so it tokenizes that field with the field analyzer and builds the segment's vocabulary FST alongside the stats, emitting a `TermDictLogID` next to `BM25StatsLogID`.

This is flush-time re-tokenization, so there is a parity window: the flush analyzer must match the ingest analyzer that produced the sparse hashes, or a term's stored hash would not match its stats entry. Two mitigations: the flush happens shortly after ingest in the same binary, and the artifact is stamped with an analyzer fingerprint (section 3.7) so a mismatch is detectable rather than silent. Capturing the terms in the same ingest `run()` that builds the sparse vector would remove the window entirely, but the term strings are dropped at the proxy and reaching the segment with them means a new field on the write path; that heavier option is left as a follow-up if the fingerprint ever trips.

### 3.4 Persist and load
- Proto: add `map<int64, FieldBinlog> term_dict_logs = 37;` to `message SegmentInfo` (`data_coord.proto`): per-field keying like `textStatsLogs = 26`, storing a `FieldBinlog` like `bm25statslogs = 27`. Regenerate with `make generated-proto-without-cpp`.
- Binlog: a new `BinlogType` next to `BM25Binlog` in `internal/storage/binlog_writer.go`.
- **The artifact must follow the exact same path `bm25statslogs` already travels**, not only `SegmentInfo`. That path is the real work: `SaveBinlogPathsRequest`, `CompactionSegment` / compaction result, `SegmentLoadInfo`, DataCoord meta and querycoord distribution, GC of dropped segments, and the Storage V2 vs V3 manifest source-of-truth. The design principle is "mirror `bm25statslogs` everywhere it flows"; enumerating and testing each site is where an artifact silently becomes an orphan if missed. Import/copy-segment, snapshot/restore, and CDC/replication follow the same rule and are verified per PR, not solved in prose here.

### 3.5 Compaction merge (union, not df arithmetic)
Because the FST holds only the vocabulary, compaction **unions** the child dictionaries (a k-way ordered merge over the sorted term streams; on a shared term, keep the term and, if the df hint is stored, take the larger). It never re-reads the input text, so the LOB limitation in section 2 does not bite compaction, and it never sums df, so the delete/TTL/split correctness problem does not arise: authoritative df comes from `BM25Stats`, which compaction already rebuilds per surviving output row. This reconciles with "df adds across disjoint segments" (the union is the vocabulary analog; df correctness is inherited from the stats, including deletes and multi-output splits). A term that has no surviving documents after deletes stays in the union but is filtered at query time by its live df (section 3.6).

### 3.6 Query-time expand (global snapshot, expand once)
The query is rewritten once per shard at the delegator (section 1), so expansion must happen there too, not per segment. On each target-version change the delegator builds a **global vocabulary snapshot** by unioning the active segments' FSTs (mirroring how the IDF oracle already aggregates per-segment stats into a global IDF). A search then:

1. normalizes the query token with the field analyzer (fuzzy uses the full analyzer);
2. calls `expand_fuzzy` once against the snapshot;
3. ranks candidates by a stable order `(edit_distance ASC, live df DESC, term bytes ASC)` and keeps `max_expansions`, **always keeping the exact term**; the FST df hint only pre-filters, the live df from the IDF oracle decides;
4. **deduplicates by string, then by hash** (`HashString2LessUint32` can collide; two distinct expanded terms mapping to one hash must contribute that hash once, not twice, so an expanded query doesn't double-weight a dimension);
5. drops candidates whose live df is below `min_doc_freq`;
6. hashes the survivors and feeds the **existing** single-rewrite IDF path unchanged.

Because df comes from the stats and dedup is by string then hash, the collision surface is no worse than a direct query today, and df stays document-frequency.

### 3.7 Opt-in, guard, artifact header
- **Opt-in per field**, off by default, flag on the BM25 function / field schema, **create-time only in v1** (enable-after-creation needs a backfill and is deferred).
- **Guard.** Cap `max_term_length` (skip over-long tokens) and a per-segment `max_vocab`. Selection under the cap is **deterministic** (keep the highest-df terms), so the retained set does not depend on insert or compaction order. Fuzzy candidate caps follow ES defaults: `max_expansions = 50`, `prefix_length = 0`, `min_doc_freq = 1`. The exact-IDF vocabulary is never df-pruned; the cutoff scopes only the fuzzy candidate set.
- **Artifact header** (so truncation and mismatch are observable, not silent): `magic`, `format_version`, `field_id`, `function_id`, `schema_version`, `analyzer_digest` (config plus resource versions), `complete` / `truncated` flag with `accepted` / `dropped` counts, `term_count`, `payload_length`, `payload_checksum`, then the FST payload. A truncated dictionary is surfaced to the query layer, not hidden in a datanode log line.

### 3.8 Sizing
Dictionary size scales with vocabulary (Heaps' law: sub-linear, saturating), not rows. A typical natural-language segment at 10^5 unique terms is ~1-2 MB (FST, ~10-20 B/term), the same order as the existing `map[uint32]int32` df structure, which already costs O(vocab). Identifier-like text (UUIDs, logs) is O(rows) and is the reason for per-field opt-in and the `max_vocab` guard.

### 3.9 Load-time lifecycle
The loaded FST follows the same lifecycle the IDF oracle already implements for sealed stats: load/unload with the segment, disk and memory accounting into admission, a per-(segment, field) handle owned by the querynode with locking so a concurrent unload can't free an FST mid-query, destroy on schema-field drop, and atomic swap on target-version change. A corrupted or version-mismatched artifact (detected via the header) is isolated to exact-only for that segment rather than failing the query.

## 4. Implementation plan (small, test-first PRs)

Each PR is one focused change, written test-first, with Boy-Scout cleanups bounded to the touched lines, a verification gate, and DCO sign-off. Dictionary first, fuzzy wired last.

1. **Rust term-dictionary in the tantivy-binding.** `build` / `union` / `expand_fuzzy` / `enumerate` on `tantivy-fst` + `levenshtein_automata`, the FFI, and the header. Rust tests: build then expand round-trip, fuzzy finds `allergy` for `alergy` at edit 1 (not edit 0), a union that keeps the term set, and a corrupted-header rejection. No Go wiring.
2. **Build and persist at flush (opt-in).** The per-field flag (rejecting multi-analyzer BM25), proto `term_dict_logs = 37`, the `BinlogType`, and the flush-time build reusing the BM25-stats channel. Tests: an enabled VARCHAR field flushes a loadable dictionary whose term set matches the corpus; a disabled field flushes none and leaves BM25 output byte-unchanged; df stays document-frequency for a doc repeating a token.
3. **Load on the querynode + the metadata path.** `SegmentLoadInfo`, distribution, GC, and the read handle. Tests: a loaded segment enumerates the corpus; a pre-feature segment reads as absent, not error; a dropped segment's artifact is GC'd.
4. **Compaction union.** Union child dictionaries; assert the union equals the term set of the inputs and that df for scoring still comes from the rebuilt stats after a compaction that deletes rows.
5. **Fuzzy end-to-end.** The delegator global snapshot, expand-once, ranking + string/hash dedup + `min_doc_freq`, feeding the existing IDF path (the #50921 consumer). Tests: a typo retrieves and scores the intended docs on today's BM25 path; an integration test drives insert, flush, compaction with a delete, then query.

## 5. Open questions

1. Capture parity (section 3.3): flush-time re-tokenization with a fingerprint (recommended) vs capturing terms in the ingest `run()` behind a new write-path field.
2. v1 scope confirmation: VARCHAR + fuzzy + sealed-first, with TEXT/LOB, growing, prefix, and suggester deferred.
3. Guard defaults: `max_term_length`, `max_vocab`, and the candidate caps.
4. Storage V2 vs V3 manifest: confirm the artifact registers in both the same way `bm25statslogs` does.

## 6. Testing / verification

Correctness-critical cases treated as mandatory: df counts once per document; compaction with deletes and with TTL expiry; a child split into multiple output segments; VARCHAR (and the multi-analyzer rejection); two expanded terms colliding to one hash, and an exact term colliding with a fuzzy variant; `max_vocab` truncation deterministic across input order; `max_expansions` ranking with the exact term always kept; growing-only vs sealed vs mixed; a pre-feature segment with no artifact; a corrupted/truncated/version-mismatched artifact isolated to exact-only; concurrent query with segment unload. Each stage asserts the loaded data (terms, and df from the stats), not merely that a writer was called. Run the relevant `make test-{datanode,querynode}` per PR and a full `make test-go` before the end-to-end PR, since proto and metadata changes ripple.
