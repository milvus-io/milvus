# Fuzzy BM25 Search with Segment FSTs

- **Feature DRI:** @aoiasd
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Status:** Draft

## 1. Summary

Fuzzy BM25 is a typo-tolerant form of BM25 full-text search. It expands each
analyzed query term to indexed terms within a configured edit distance, then
uses the expanded terms in the existing BM25 TF/IDF scoring path.

For query term `a[1..m]` and indexed term `b[1..n]`, the Levenshtein distance is
defined by:

```text
D(i, 0) = i
D(0, j) = j

D(i, j) = min(
    D(i - 1, j)     + 1,              // deletion
    D(i, j - 1)     + 1,              // insertion
    D(i - 1, j - 1) + [a[i] != b[j]]  // substitution or match
)
```

`D(m, n)` is the minimum number of edits needed to transform one term into the
other. The fuzzy automaton also treats an adjacent transposition as one edit,
matching the existing `text_match_fuzzy` behavior. For example, `milvuz`
matches `milvus` at distance 1.

Milvus BM25 currently hashes analyzed terms into `uint32` dimensions. This is
sufficient for scoring but loses the enumerable string vocabulary required for
fuzzy expansion. This design therefore adds a Segment FST for each required
text field. A fuzzy-enabled BM25 Function identifies which input field must
preserve analyzed terms for automaton-based enumeration.

The user continues to call the normal BM25 search API. Fuzzy BM25 is enabled
when `fuzzy_max_edit_distance` is greater than zero. Fuzzy expansion happens
before IDF construction; the final segment search and BM25 scoring path remain
unchanged.

Milvus already supports `text_match_fuzzy` through a Tantivy text index. The
initial fuzzy BM25 implementation uses a separate Segment FST because the two
paths have different ownership and query requirements. Long term, the Segment
FST should become part of one shared Segment Text Index containing both an FST
and postings.

## 2. Goals and non-goals

### Goals

- Support typo-tolerant BM25 search with one consistent query rewrite and IDF
  vector across all target segments.
- Provide an enumerable vocabulary for every fuzzy-enabled BM25 input field.
- Keep growing vocabulary local to the Delegator and sealed vocabulary with the
  Workers that own the segments.
- Make the Segment FST reusable by future term-based text features.
- Reduce sealed-segment expansion RPCs later with a Global BM25 FST.

### Non-goals

- Replacing the BM25 sparse index or scoring formula.
- Replacing the `text_match_fuzzy` API in the first implementation.
- Supporting fuzzy BM25 for External Collections in the first release.
- Supporting fuzzy-aware highlighting in the first release.
- Defining all future prefix, wildcard, or regexp query APIs.

## 3. Core concepts

### 3.1 Fuzzy

Fuzzy is a query-term expansion operation:

```text
query term + fuzzy options + term vocabulary
  -> indexed candidate terms
```

It is not a new scoring model:

- `text_match_fuzzy` uses expanded terms for Boolean posting lookup.
- fuzzy BM25 hashes expanded terms and uses the existing BM25 scoring path.

### 3.2 Segment FST

A Segment FST is the enumerable vocabulary for one segment and one field. It
contains analyzed term bytes but no postings.

Runtime ownership follows segment ownership:

```text
growing segment on Delegator
  -> committed FST fragments + mutable Trie delta

sealed segment on Worker
  -> consolidated FST, or fragments before consolidation
```

Only sealed segments require a Worker expansion interface. Growing segments are
already local to the Delegator.

The Segment FST is the artifact delivered by this design. The long-term shared
artifact is:

```text
Segment Text Index
  |- Segment FST   term enumeration and automaton navigation
  `- postings      term-to-document lookup
```

### 3.3 Global BM25 FST

A Global BM25 FST is a field-level union of stable sealed-segment vocabularies,
loaded with the shard Delegator. It reduces Worker RPC fan-out during fuzzy
BM25 query rewriting.

It contains no postings and does not replace Segment FSTs. Its coverage is
explicitly bound to sealed-segment FST generations.

## 4. User interface

### 4.1 Function capability

The BM25 Function uses the optional parameter:

```text
function.type = BM25
function.params = {"enable_fuzzy": "true"}
```

`enable_fuzzy` belongs to the BM25 Function because fuzzy expansion is a BM25
query capability. The Function binds that capability to its input text field,
whose analyzed terms must be preserved in Segment FSTs.

The physical artifact remains field-scoped. If multiple fuzzy-enabled BM25
Functions use the same input field, Milvus builds and stores only one Segment
FST for that field. This also preserves the path toward a shared field-scoped
Segment Text Index.

The Function must have the normal BM25 shape. Its input field must be `VARCHAR`
or `TEXT` and set `enable_analyzer = true`. Omission or `false` keeps fuzzy BM25
disabled and does not require Segment FST maintenance.

The first implementation may reject online changes to this Function parameter
because existing segments do not yet have complete Segment FSTs. A follow-up PR
should support `false -> true` by building and publishing complete FSTs for
existing segments before exposing the capability. The disable path must also
define cleanup and in-flight-query behavior.

Long term, `enable_match` or a Match Index definition should own construction
of the shared Segment Text Index, making `enable_fuzzy` unnecessary.

### 4.2 Search options

Fuzzy BM25 uses the normal BM25 search API and adds search parameters:

- `fuzzy_max_edit_distance`: maximum edit distance in `[0, 2]`. A value greater
  than zero enables fuzzy BM25. Zero or omission uses exact BM25.
- `fuzzy_max_expansions`: maximum number of expanded terms after the global
  rewrite. It defaults to `50`.
- `fuzzy_prefix_length`: number of initial characters that must match exactly.
  The prefix is not fuzzified. It defaults to `0`.

```text
# exact BM25
search_params = {"anns_field": "sparse_bm25"}

# fuzzy BM25
search_params = {
    "anns_field": "sparse_bm25",
    "fuzzy_max_edit_distance": 1,
    "fuzzy_max_expansions": 50,
    "fuzzy_prefix_length": 1,
}
```

A positive edit distance is accepted only when the BM25 Function producing the
searched `anns_field` has `enable_fuzzy = true`. `fuzzy_max_expansions` and
`fuzzy_prefix_length` do not independently enable fuzzy BM25.

## 5. Segment FST lifecycle

### 5.1 Write and sync

The existing BM25 materializer analyzes the input text before writing the
Insert message. For a fuzzy-enabled field, the same analyzer execution produces
both:

```text
analyzed rows
  |- hashed per-row TF for BM25
  `- message-level deduplicated term bytes for the Segment FST
```

The term sidecar travels with the durable Insert message so both the query-side
growing dictionary and the flush pipeline observe the same analyzed terms and
checkpoint.

Each sync writes an immutable sorted FST fragment. A separate term-enum file is
unnecessary because an FST is losslessly enumerable.

FST fragments are needed before sort compaction. A flushed-but-unsorted segment
can still be restored as growing and become readable before sort compaction
finishes. Its committed vocabulary must therefore be recoverable without
reanalyzing all raw text. The fragment is recovery and compaction input; it is
not intended to remain the steady-state sealed representation.

### 5.2 Compaction and import

Sort compaction merges input FSTs into one consolidated Segment FST. Other
compaction and import paths must generate or preserve an FST that covers the
same readable data as the output segment.

Deletes and TTL may leave stale terms in the FST. This is safe because terms
with zero document frequency in the pinned target are removed before the
expansion limit is applied. A future compaction may rebuild from live rows to
reclaim stale vocabulary.

### 5.3 Load and recovery

- A growing segment loads committed FST fragments into the Delegator and
  resumes later terms in a mutable Trie.
- A sealed segment loads its Segment FST on the Worker.
- A sealed segment without a consolidated FST may temporarily traverse its FST
  fragments as one logical vocabulary.
- A segment is not exact-fuzzy-readable unless its Segment FST covers all
  readable data.

Persistent FSTs use mmap by default. A configuration may choose resident-memory
loading when lower lookup latency is worth the memory cost.

## 6. Exact fuzzy BM25 query flow

The Delegator performs fuzzy expansion before the current BM25 IDF step:

```text
normal BM25 search request
  -> analyze source terms without hashing
  -> pin readable growing and sealed targets
  -> expand growing terms locally
  -> expand sealed terms through batched Worker RPCs
  -> merge and rank candidates
  -> hash candidates and build target-bound IDF vector
  -> run the normal segment search
```

### 6.1 Expansion

The Delegator expands growing targets against its local dictionaries. It groups
sealed targets by Worker and sends one batched expansion request per Worker.
The Worker interface serves sealed segments only.

Each source term applies the configured edit distance and exact prefix length.
Candidates from all sources are unioned before the global expansion limit is
applied.

### 6.2 Global rewrite and IDF

The Delegator rewrites candidates in this order:

1. Merge and deduplicate candidates from all target vocabularies.
2. Look up document frequency over the pinned target.
3. Remove candidates whose target document frequency is zero.
4. Apply `fuzzy_max_expansions` globally.
5. Hash the retained terms and build the BM25 query TF/IDF vector.

The same final vector is sent to every target segment. A segment naturally
ignores dimensions it does not contain. Fuzzy behavior therefore changes only
lexical preparation, not segment-level BM25 search or score computation.

### 6.3 Consistency and partial results

Term expansion, document-frequency lookup, IDF construction, and final search
must use the same readable target and FST generations.

Worker expansion failures follow the existing Search partial-result policy. If
partial results are accepted, the successfully expanded segments become the
reduced target used by both IDF construction and final search. Partial term
expansion must never be combined with full-target BM25 statistics.

All rows of a multi-analyzer field contribute terms to one field vocabulary.
Multi-analyzer selects one analyzer per row; it does not create separate FSTs.

## 7. Global BM25 FST

### 7.1 Purpose and publication

Exact Phase 1 search may contact every Worker that owns a sealed target. A
Global BM25 FST reduces that fan-out by materializing a union of stable sealed
Segment FSTs close to the Delegator.

A published generation contains:

- one Global BM25 FST per shard and field;
- a generation identifier;
- a coverage manifest that identifies the exact sealed Segment FST generations
  included in the union.

A timestamp frontier alone is not sufficient coverage proof because compaction
or rebuild can replace a segment generation without changing its data time
range. Publication is atomic, and an in-flight query keeps its pinned global
generation.

### 7.2 Exact mode

```text
growing target                 -> local Delegator Segment FST
covered sealed target          -> Global BM25 FST
uncovered sealed target        -> Worker Segment FST RPC
```

If all sealed targets are covered, exact expansion needs no Worker RPC. If no
compatible Global BM25 FST is available, search falls back to the Phase 1 path.

### 7.3 Global-only mode

A future query option may skip uncovered sealed-segment RPCs:

```text
fuzzy_expansion_mode = exact | global_only
```

`global_only` still expands growing terms locally, but uses only the pinned
Global BM25 FST for sealed data. It may miss terms from uncovered sealed
segments and can change recall and ranking. The response and metrics should
expose global generation, coverage, and lag.

## 8. Delivery plan

### Phase 1: Segment FST and exact fuzzy BM25

- Add the BM25 Function capability and search options.
- Persist analyzed terms and build Segment FSTs across sync, compaction, import,
  load, and recovery.
- Add sealed Worker expansion and Delegator-side global rewrite/IDF.

### Phase 2: Global BM25 FST

- Build and publish covered sealed vocabularies.
- Use the Global BM25 FST for exact expansion and Worker RPCs for the uncovered
  delta.

### Phase 3: operational improvements

- Add `global_only` mode.
- Support online alteration of `enable_fuzzy` with backfill and cleanup.
- Converge fuzzy BM25 and Tantivy text match on a shared Segment Text Index.

## 9. Correctness requirements

1. BM25 TF and preserved terms come from the same analyzer execution.
2. A readable segment has an FST covering all readable data.
3. Growing vocabulary is served locally; sealed vocabulary is served by either
   its Worker or a compatible Global BM25 FST.
4. Expansion, DF/IDF, and final search use one pinned target.
5. Every target segment receives the same final BM25 query vector.
6. DF-zero terms are removed before the global expansion limit.
7. Accepted partial expansion and final search use the same reduced target.

## 10. Compatibility, testing, and observability

Existing BM25 Functions default to fuzzy disabled. Until online backfill is
available, only Functions created with `enable_fuzzy = true` require complete
Segment FSTs for their input fields.
Mixed-version clusters must prevent fuzzy-enabled collections from running on
nodes that do not understand the term sidecar, FST metadata, or expansion RPC.

The main validation areas are:

- analyzer-term preservation and message-level deduplication;
- recovery before and after sort compaction;
- FST preservation through compaction and import;
- growing/sealed candidate union and target-bound IDF;
- partial-result target consistency;
- exact-prefix and edit-distance behavior;
- Global BM25 FST coverage and fallback.

Key metrics include Segment FST size/load mode, expansion candidates before and
after filtering, Worker RPC fan-out and latency, and Global BM25 FST coverage.

## 11. Related documents and code

- `docs/design-docs/design_docs/20260702-text_match_fuzzy.md`
- `internal/streamingnode/server/wal/interceptors/shard/function_materializer.go`
- `internal/flushcommon/`
- `internal/datanode/compactor/`
- `internal/querynodev2/delegator/`
- `internal/querynodev2/segments/`
- `internal/core/src/index/TextMatchIndex.*`
