# Fuzzy BM25 Search with Segment FSTs

- **Feature DRI:** @aoiasd
- **Primary Approver:** @congqixia
- **Independent Approver:** @zhengbuqian
- **Design Review:** TBD

## 1. Summary

Fuzzy BM25 is typo-tolerant BM25 full-text search. It expands each analyzed
query term to indexed terms within a configured edit distance, then sends the
expanded terms through the existing BM25 TF/IDF scoring path.

For terms `a[1..m]` and `b[1..n]`, Levenshtein distance is defined by:

```text
D(i, 0) = i
D(0, j) = j

D(i, j) = min(
    D(i - 1, j)     + 1,              // deletion
    D(i, j - 1)     + 1,              // insertion
    D(i - 1, j - 1) + [a[i] != b[j]]  // substitution or match
)
```

`D(m, n)` is the minimum number of edits required to transform one term into
the other. Milvus uses the optimal-string-alignment variant of
Damerau-Levenshtein distance: when `i, j >= 2`, `a[i] = b[j-1]`, and
`a[i-1] = b[j]`, it also considers `D(i-2, j-2) + 1`, so one adjacent
transposition costs one edit. For example, both `milvuz` (substitution) and
`miluvs` (transposition) match `milvus` at distance 1. This matches the existing
`text_match_fuzzy` behavior.

BM25 currently hashes analyzed terms into `uint32` sparse dimensions. Hashing
is sufficient for scoring but loses the enumerable string vocabulary required
for fuzzy expansion. This design adds a Segment FST for each fuzzy-enabled BM25
input field. QueryNode uses it to intersect the field vocabulary with a
Levenshtein automaton before the Delegator builds the normal BM25 query vector.
Document frequency and average document length always come from the existing
global BM25 statistics.

Users still call the normal BM25 search API. A BM25 Function opts its input
field into vocabulary maintenance with `enable_fuzzy = true`; a search uses
fuzzy expansion when `fuzziness > 0`.

Milvus already implements `text_match_fuzzy` through the Tantivy-based Text Log
V1 (`text_log`). This proposal introduces Text Log V2 (`text_log_v2`) for the
Segment FST. The two paths initially remain separate. The long-term goal is a
Segment Text Index in Text Log V2 that shares term enumeration and postings
between fuzzy BM25, fuzzy match, and future term-based text features.

## 2. Scope

### Goals

- Build a complete enumerable vocabulary for each fuzzy-enabled BM25 field.
- Expand growing and sealed vocabularies into one query rewrite, then use
  global BM25 statistics and the existing segment search path.
- Make Segment FSTs follow the normal segment flush, compaction, import, load,
  recovery, copy, and garbage-collection lifecycle.
- Keep growing state with the Delegator and sealed state with the Worker that
  owns the segment.
- Add a Global BM25 FST later to reduce sealed-segment expansion RPCs.

### Non-goals

- Replacing the BM25 sparse index or scoring formula.
- Replacing Text Log V1 or changing `text_match_fuzzy` in the first release.
- Supporting External Collections, online `enable_fuzzy` alteration, or
  fuzzy-aware highlighting in the first release.
- Defining prefix, wildcard, or regexp query APIs.

## 3. Architecture

### 3.1 Core concepts

**Fuzzy** is vocabulary expansion, not a scoring model:

```text
query term + fuzzy options + term vocabulary -> indexed candidate terms
```

`text_match_fuzzy` uses candidates for Boolean posting lookup. Fuzzy BM25
hashes them and uses the existing BM25 scoring path.

**Segment FST** is the enumerable analyzed-term vocabulary for one segment and
one field. It contains no postings. A logical Segment FST can temporarily have
several immutable fragments before compaction consolidates them.

```text
growing segment on Delegator -> one mutable segcore Trie per field
sealed segment on Worker      -> one FST, or several committed fragments
```

For growing recovery, QueryNode streams every committed FST fragment into the
segment's Trie and releases the FST readers. WAL replay adds later terms to the
same Trie. Updates and traversal are synchronized in C++. Sealed segments keep
immutable FST readers for their loaded lifetime and expose expansion only
through the Worker interface.

**Global BM25 FST** is a future shard-and-field-level union of stable sealed
Segment FST generations, loaded beside the Delegator. It is a vocabulary
routing optimization, not a replacement for Segment FSTs or global BM25
statistics.

### 3.2 Component ownership

| Component | Responsibility |
|---|---|
| Proxy | Validate Function capability and fuzzy search options. |
| StreamingNode | Materialize BM25 output and analyzed-term sidecar into the Insert message. |
| Flush/DataNode writers | Build Text Log V2 FST fragments or complete replacement FSTs. |
| DataCoord/storage metadata | Publish, copy, expose, and garbage-collect Text Log V2 with segment data. |
| QueryNode Worker | Load and expand sealed Segment FSTs. |
| QueryNode Delegator | Maintain growing Tries, batch Worker expansion, build global-stat IDF, and run search. |

## 4. User contract

### 4.1 Function capability

The optional BM25 Function parameter is:

```text
function.type = BM25
function.params = {"enable_fuzzy": "true"}
```

`enable_fuzzy` belongs to the Function because it enables a BM25 query
capability. The Function identifies the input text field whose terms must be
preserved. The input must be `VARCHAR` or `TEXT` with
`enable_analyzer = true`. Omission or `false` keeps exact BM25 behavior and
does not build Segment FSTs.

The artifact remains field-scoped. If multiple fuzzy-enabled BM25 Functions
share one input field, Milvus builds one Segment FST for that field. A future
`enable_match` capability or Match Index should own the shared Text Log V2
construction after the text paths converge.

The first release rejects online changes to `enable_fuzzy`: existing segments
would not have complete FST coverage. Future `false -> true` support must build
all visible Segment FSTs before publishing fuzzy readiness; exact BM25 remains
available during backfill.

### 4.2 Search options

| Parameter | Meaning | Default |
|---|---|---:|
| `fuzziness` | Maximum edit distance in `[0, 2]`; zero or omission means exact BM25. | `0` |
| `fuzzy_max_expansions` | Candidates per analyzed source term and vocabulary component, in `[1, 1024]`. | `50` |
| `fuzzy_prefix_length` | Initial Unicode code points that must match exactly. | `0` |

```text
search_params = {
    "anns_field": "sparse_bm25",
    "fuzziness": 1,
    "fuzzy_max_expansions": 50,
    "fuzzy_prefix_length": 1,
}
```

The exact term, when present, counts toward `fuzzy_max_expansions`. Each
component chooses candidates by smaller edit distance and then term-byte order.
The Delegator unions component results without applying another expansion
limit.

A positive `fuzziness` is valid only when the BM25 Function producing
`anns_field` has `enable_fuzzy = true`. Invalid or conflicting user options are
input errors rejected at Proxy. The other two options do not independently
enable fuzzy search.

## 5. Segment FST lifecycle

### 5.1 WAL materialization and sync

For a fuzzy-enabled field, one analyzer execution normally produces both the
hashed per-row BM25 TF and message-level deduplicated term bytes. The term
sidecar is part of the Insert payload, not a separate WAL message, so it shares
the Insert's VChannel, TimeTick, transaction, lock, append ACK, replay, and CDC
replication semantics. Duplicate delivery is idempotent because consumers use
byte-wise set union.

Each sync writes an immutable sorted FST fragment. A separate term-enum file is
unnecessary because an FST is losslessly enumerable. Fragments are required
before sort compaction: a flushed unsorted segment can be restored as growing
and queried before consolidation, so its committed vocabulary must already be
recoverable.

Text Log V2 uses the ordinary segment-log lifecycle:

- StorageV1/V2 stores FSTs under `text_log_v2/...` and publishes `FieldBinlog`
  metadata.
- StorageV3 publishes `text_log_v2.<fieldID>` entries in the LOON manifest; the
  manifest version is the atomic artifact generation and source of truth.

Every generation records a `coverage_timestamp`, the maximum system timestamp
from the same row set as its vocabulary. It is a completeness fence, not the
FST creation time.

### 5.2 Rebuild, copy, and deletion

Compaction and stats-sort analyze their final output rows and write one
replacement FST. Vocabulary and coverage come from the same final-row scan, so
deletes, TTL, and row filtering are reflected after rebuild. Between rebuilds,
stale terms are allowed; with a finite expansion limit they can occasionally
occupy a candidate slot and reduce recall. A StorageV3 rebuild projects both
the fuzzy input field and system timestamp from the final LOON row set rather
than inferring coverage from optional summaries.

Row-producing import follows the same final-output rule. Segment copy, backup,
and restore preserve the referenced Text Log V2 generation. Collection rename
does not rewrite ID-based artifacts. Partition/collection deletion and orphan
cleanup remove Text Log V2 through the same metadata and object-storage GC
paths as other segment logs.

### 5.3 Load and recovery

For every fuzzy-enabled field, QueryNode requires a supported FST format,
non-zero coverage metadata, and:

```text
max(Text Log V2 coverage_timestamp) >= max(readable data timestamp)
```

Missing, corrupt, unknown-format, or stale artifacts are data-integrity
failures; QueryNode does not publish the incomplete segment. A growing segment
imports all valid FST fragments into one Trie before WAL replay. A sealed
segment keeps the readers and queries each fragment until consolidation.

| QueryNode setting | Default | Dynamic | Effect |
|---|---:|---:|---|
| `queryNode.mmap.textLogV2` | `true` | no | mmap sealed FSTs; `false` loads them into memory. |
| `queryNode.textLogV2GrowingTrieExpansionFactor` | `32.0` | yes | Estimate growing-Trie heap for load admission. |

The mmap setting controls only temporary reads during growing recovery; the
resulting Trie is heap-resident. Changing mmap mode requires a QueryNode
restart. The expansion factor affects admission estimates, not results.
FST readers have no independent invalidation channel: segment load, replacement,
and release own their lifecycle, while the generation fence detects stale
in-flight requests.

## 6. Exact fuzzy BM25 query

```text
normal BM25 request
  -> analyze source terms without hashing
  -> pin readable growing and sealed targets
  -> expand each growing segment against its local Trie
  -> batch sealed segment IDs by Worker and expand their FSTs
  -> union and deduplicate candidate terms
  -> hash candidates and build IDF from global BM25 statistics
  -> send one query vector to the successful target segments
  -> run normal segment BM25 search and reduce
```

Prefix traversal occurs before the edit-distance automaton, so an edit or
transposition cannot cross the exact-prefix boundary. Multi-analyzer chooses
one analyzer per row, but every row contributes to the same field vocabulary.

`fuzzy_max_expansions` applies once to each growing Trie and each immutable FST
fragment. Consolidation changes the physical components and can therefore
change which bounded candidates survive; results are deterministic for a
pinned artifact generation.

Expansion and final search use the same target snapshot and per-segment FST
generation. A generation change is a transient system failure that restarts
the complete two-phase operation rather than reusing stale candidates. Every
target receives the same final vector and naturally ignores dimensions it does
not contain.

Worker failures follow the existing Search partial-result policy. When partial
results are accepted, successfully expanded segments become the final-search
target. Candidate terms can be missing, but DF/IDF and average document length
still come from global BM25 statistics; they are never recomputed from the
reduced target.

`fuzzy_max_expansions` is not an aggregate admission limit. Before Phase 1 is
generally enabled, QueryNode must also bound total term/component work, response
bytes, RPC concurrency, and in-flight native traversals. Static request excess
is an input-size error; runtime saturation is a retriable resource error. The
system rejects excess work rather than globally clipping candidates and
changing query semantics.

Context cancellation stops orchestration and remote RPCs. Cooperative
cancellation within one native FST/Trie traversal is follow-up work; aggregate
admission bounds the exposure in the initial release.

## 7. Global BM25 FST

A future builder publishes one Global BM25 FST per shard and field with a
generation ID and a manifest of the exact sealed Segment FST generations it
covers. A timestamp frontier alone is insufficient because compaction can
replace an artifact without changing its data time range. Publication is
atomic, and in-flight queries pin a generation.

`exact` mode consults every target vocabulary:

```text
growing target        -> local Trie
covered sealed target -> Global BM25 FST
uncovered target      -> Worker Segment FST RPC
```

Here `exact` means no target vocabulary is deliberately omitted, not that a
bounded result is bit-identical before and after FST consolidation. If no
compatible Global FST exists, the query uses only Segment FST RPCs.

A future `global_only` mode skips RPCs for uncovered sealed segments while
still expanding growing terms locally. It is explicitly approximate and can
change recall and ranking; responses and metrics must expose global generation,
coverage, and lag.

## 8. Delivery and final state

The current implementation stack targets the complete Phase 1 acceptance
boundary. Phases 2 and 3 are intentionally separate follow-ups.

### Phase 1: serviceable fuzzy BM25

- Add Function/search validation and the Insert term sidecar.
- Build and carry Segment FSTs through every segment lifecycle path.
- Load sealed FSTs, rebuild growing Tries, batch Worker expansion, and use
  global-stat IDF for final BM25 search.
- Add aggregate expansion admission without changing per-component semantics.

### Phase 2: Global BM25 FST

- Build and atomically publish covered sealed vocabularies.
- Use the global generation for covered targets and Worker RPCs for the delta.

### Phase 3: evolution

- Add `global_only`, online `enable_fuzzy` backfill/cleanup, fuzzy highlighting,
  and cooperative native cancellation.
- If WAL message-size limits require it, chunk or separately reference term
  sidecars without changing their Insert checkpoint semantics.
- Move Text Log V1 features into the shared Text Log V2 Segment Text Index and
  retire V1 only after feature and migration parity.

## 9. Correctness, security, and availability invariants

1. WAL BM25 TF and preserved terms come from one schema version and analyzer;
   rebuild paths analyze the same final rows used to establish coverage.
2. Every readable fuzzy-enabled segment has complete FST coverage.
3. Insert rows and their term sidecar share one WAL payload and checkpoint;
   retry or replay cannot change the vocabulary.
4. Expansion and search use one pinned target and matching FST generations.
5. All targets use one candidate union and global BM25 weighting corpus.
6. Partial expansion and final search use the same reduced target.
7. Segment FST readers follow segment load/release; growing recovery releases
   temporary readers after importing one Trie.
8. The sidecar and FST contain unhashed analyzed terms. They inherit existing
   WAL/object-storage encryption and authorization, and term contents must not
   appear in logs or metric labels.

## 10. Compatibility and rollout

Existing BM25 Functions default to fuzzy disabled, and Text Log V1 remains
valid. The user-facing flag and search options reuse `FunctionSchema.Params`
and the existing search-parameter map. Internal wire changes add the Insert
sidecar and QueryNode expansion messages. The sidecar must first be released in
`milvus-proto` and pinned by Milvus. A rolling upgrade may proceed while fuzzy
remains disabled, but
`enable_fuzzy` can be used only after every producer, consumer, DataNode, and
QueryNode understands the sidecar, Text Log V2, and expansion RPC. Mixed-version
fuzzy service is unsupported.

The first release rejects External Collections and online changes to
`enable_fuzzy`. Rolling back after fuzzy-enabled writes begin is unsupported
for affected collections: an old writer could append rows without terms and
break coverage. Rollback requires stopping their writes and fuzzy traffic, or a
future backfill/disable workflow.

The Feature DRI and Primary Approver retain follow-up maintenance ownership
unless it is explicitly transferred.

## 11. Verification and observability

Verification covers:

- schema/search validation, exact `fuzziness = 0`, edit distance, transposition,
  Unicode prefix, deterministic limit ordering, and multi-analyzer selection;
- local end-to-end misspelling search on both growing and sealed segments;
- flush fragments, pre-sort growing recovery, consolidation, compaction,
  stats-sort, import, StorageV3 manifest restart, copy/restore, and drop/GC;
- duplicate WAL delivery, transaction replay, CDC payload preservation, and
  concurrent Trie update/search;
- corrupt/missing/stale artifacts, generation retry, Worker partial failure,
  aggregate admission, and cancellation;
- Global FST publication, coverage fallback, and `global_only` accuracy loss.

Operational signals include FST bytes and load mode, growing-Trie heap/rebuild
latency, expansion input and candidate counts, Worker fan-out/latency,
admission rejection, partial-result ratio, generation retry, and Global FST
coverage/lag. Troubleshooting logs identify collection, field, segment,
generation, load mode, and failure class without logging terms.

## 12. Decisions and alternatives

- Delegator-loading all sealed FSTs duplicates Worker-owned lifecycle and
  resources; batched Worker expansion keeps artifacts with their owner.
- A separate term-enum file duplicates an already enumerable FST.
- Building only at sort compaction cannot recover a flushed unsorted segment.
- Keeping recovery FSTs beside a mutable Trie makes growing lookup and limits
  depend on fragment count; importing one Trie gives one growing dictionary.
- Segment-local IDF makes scores depend on placement and availability; global
  BM25 statistics remain authoritative.

## 13. Related documents and code

- Feature issue: TBD
- Design PR: <https://github.com/milvus-io/milvus/pull/51952>
- `docs/design-docs/design_docs/20260702-text_match_fuzzy.md`
- `docs/design-docs/design_docs/20260226-manifest-format.md`
- `internal/streamingnode/server/wal/interceptors/shard/function_materializer.go`
- `internal/flushcommon/`
- `internal/datanode/compactor/`
- `internal/querynodev2/delegator/`
- `internal/querynodev2/segments/`
- `internal/core/src/textindex/`
