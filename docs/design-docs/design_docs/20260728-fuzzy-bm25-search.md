# Text Term Index Foundation and Fuzzy BM25 Search

**Author:** zhicheng
**Date:** 2026-07-28
**Status:** Draft

---

## 1. Summary

This document proposes a three-phase design for fuzzy BM25 search in Milvus.
The first phase establishes a segment-level Text Term Index foundation: terms
produced by the analyzer are preserved through WAL and immutable FST fragments
across flush, compaction, load, and recovery, and QueryNode Workers expose a
common term-expansion interface.
Fuzzy BM25 then expands query tokens on the Workers before the Delegator builds
the BM25 IDF vector.

The second phase adds a Global FST on the Streaming Query Node (SN) as an exact
acceleration layer. The Global FST supplies the vocabulary for covered
segments, while the Delegator requests only the uncovered segment dictionaries
from Workers. The third phase allows users to explicitly skip the uncovered
segment RPC and trade freshness for latency.

The initial field-level switch, `enable_fuzzy_bm25`, is intentionally
transitional. The long-term direction is for `enable_match`, or a dedicated
Match Index definition, to build one complete Milvus Text Term Index containing
the enumerable FST and postings. Exact text match, fuzzy text match, fuzzy
BM25, prefix, wildcard, and other term-based text features should share that
index instead of maintaining feature-specific files and execution paths.

---

## 2. Background

### 2.1 Current BM25 search path

For a BM25 function, the WAL write path materializes the sparse BM25 output
before appending an Insert message. The analyzer emits tokens, each token is
hashed to a `uint32`, and the per-row token frequencies are stored in a sparse
vector.

On the query path, the shard Delegator currently performs the following work
before sending the search request to QueryNode Workers:

```text
raw query text
  -> BM25 function runner
  -> analyzer
  -> hash tokens and build query TF sparse vector
  -> IDFOracle.BuildIDF
  -> BM25 query sparse vector
  -> SearchSegments on Workers
```

`IDFOracle` is owned by the Delegator because BM25 IDF must be computed from a
consistent shard-level view rather than independently by every segment.

Fuzzy BM25 cannot use this path unchanged. Fuzzy expansion requires the
original token strings and the searchable term vocabulary. Expansion must
therefore happen before hashing and before `BuildIDF`.

### 2.2 Current fuzzy text match path

Milvus already implements the filter operator `text_match_fuzzy`. It is a
segment-local operation over the existing Tantivy `TextMatchIndex`:

```text
query text
  -> segment TextMatchIndex
  -> Tantivy analyzer
  -> Levenshtein automaton x Tantivy term dictionary FST
  -> segment postings
  -> result bitset
```

This path is appropriate for a filter because term expansion and posting lookup
are both local to the segment. It does not need to expose the expanded terms to
the Delegator.

Fuzzy BM25 is different. The expanded terms must be combined across all target
segments before the Delegator constructs one common BM25 query vector. If each
segment independently built and scored a different vector, scores from
different segments would not be comparable.

The existing `TextMatchIndex` proves that an FST is useful for more than fuzzy
BM25. Segment-level FST traversal is also a natural building block for fuzzy
match, prefix, wildcard, regexp/automaton intersection, term enumeration, and
term-to-posting lookup. This is why the durable segment dictionary should stay
with the Worker that owns the segment. A Global FST is an additional unioned
vocabulary for cross-segment query rewriting; it does not replace segment-level
text indexes.

### 2.3 Flushed-but-unsorted segments

With sort compaction enabled, DataCoord marks a newly flushed segment
`is_invisible=true`. Recovery information reports that segment as unflushed, so
a collection load before sort compaction completes restores it through
`LoadGrowing` as a `SegmentTypeGrowing` segment from its binlogs or manifest and
then resumes WAL consumption from the channel seek position.

This behavior is important for the term dictionary design:

- a sync or flush cannot assume that the segment immediately becomes a sealed
  Worker segment;
- already-synced terms must be recoverable into the growing dictionary;
- the sort-compaction output is the first point at which a single persistent
  FST can be guaranteed under the default workflow.

---

## 3. Goals

1. Support fuzzy term expansion for BM25 while preserving globally consistent
   IDF and comparable scores across segments.
2. Keep segment dictionaries on QueryNode Workers, where they can serve future
   segment-local text features as well as fuzzy BM25.
3. Preserve analyzed terms across WAL, sync, recovery, and compaction without
   re-analyzing all raw text during every recovery.
4. Use FST itself as both the durable enumerable vocabulary and the query
   artifact, avoiding a duplicate term-enum file.
5. Provide exact fuzzy BM25 in Phase 1, exact Global FST acceleration in Phase
   2, and an explicit approximate low-RPC mode in Phase 3.
6. Define artifacts and interfaces that can evolve into one shared Milvus Text
   Term Index rather than a permanent fuzzy-BM25-only index.

## 4. Non-goals

- Replacing the existing BM25 sparse inverted index or BM25 scoring formula.
- Replacing the current `text_match_fuzzy` API in the first phase.
- Defining all future prefix, wildcard, regexp, or fuzzy-match scoring APIs.
- Making approximate expansion the default behavior.
- Supporting an online transition from `enable_fuzzy_bm25=false` to `true`
  without an explicit backfill procedure in the first release.

---

## 5. Terminology

### 5.1 FST fragment

An immutable FST containing the sorted, deduplicated analyzer output terms
newly covered by one sync for one segment, field, and analyzer identity. An FST
is losslessly enumerable in lexical order, so it serves both as the durable
vocabulary and as the query artifact. Compaction, migration, and Global FST
generation iterate input FSTs directly; no parallel term-enum file is stored.

### 5.2 Segment term dictionary

The runtime interface used to enumerate terms that satisfy a query automaton.
Its implementation depends on the segment state:

```text
SegmentTermDictionary
  |- MutableTrieDictionary       unsynced growing delta
  |- CompositeFSTDictionary      synced FST fragments
  `- PersistentFSTDictionary     compacted sealed segment
```

### 5.3 Text Term Index

The long-term, complete segment text-index artifact:

```text
Text Term Index
  |- FST                         enumerable term navigation
  `- postings                    term-to-row/doc location
```

Phase 1 builds only the term-dictionary subset when `enable_fuzzy_bm25=true`.
The target architecture is for fields controlled by `enable_match` or a Match
Index definition to build the complete artifact and share it across text
features.

### 5.4 Dictionary identity and generation

A dictionary is identified by at least:

```text
(collection, vchannel, segment, field, analyzer identity, dictionary generation)
```

The analyzer identity is a stable digest of analyzer configuration, referenced
resources, multi-analyzer selection, and the tokenizer implementation version.
A segment ID alone is not sufficient because compaction, schema change, or
index rebuild can publish a new dictionary for the same logical data frontier.

### 5.5 Lexical snapshot

A query-scoped snapshot binding the following state:

- the readable segment distribution;
- the segment dictionary generations;
- the BM25 statistics used for DF, IDF, and average document length;
- the Global FST generation and its coverage, if used.

Expansion, IDF construction, and segment search must all use the same lexical
snapshot.

---

## 6. Three-phase delivery

### Phase 1: Segment Text Term Index foundation and exact fuzzy BM25

- Add the temporary field switch `enable_fuzzy_bm25`.
- Preserve message-level deduplicated terms in Insert WAL messages.
- Maintain a mutable Trie for growing segments.
- Write one small immutable FST fragment per sync.
- Build one consolidated FST during sort compaction.
- Recover growing dictionaries from existing FST fragments and then WAL.
- Load persistent FSTs on Workers and traverse multiple fragments directly for
  unsorted or transitional segments.
- Add a batched Worker RPC for fuzzy term expansion.
- Merge terms and build a target-bound IDF vector on the Delegator.

### Phase 2: Global FST exact acceleration

- Compact segment FSTs into a Global FST.
- Load the Global FST on the SN with the Delegator.
- Treat the Global FST as a compacted base vocabulary and segment dictionaries
  as an uncovered delta.
- Use the Global FST for covered segments and RPC only for uncovered segments.
- Preserve exact results; if every target segment is covered, expansion needs
  no Worker RPC.

### Phase 3: User-controlled approximate expansion

- Add an explicit query mode that uses only the Global FST.
- Skip uncovered-segment expansion RPC when requested by the user.
- Expose coverage and dictionary lag so the accuracy/latency trade-off is
  observable.

---

## 7. Field-level feature gate

Phase 1 adds `enable_fuzzy_bm25` to the BM25 input string field.

```text
enable_analyzer = true
enable_fuzzy_bm25 = true
BM25 function input = this field
```

Validation rules:

1. The field must be `VARCHAR` or `TEXT`.
2. `enable_analyzer` must be `true`.
3. The field must be an input of a BM25 function.
4. The property defaults to `false`.
5. The first release treats the property as creation-time immutable. Enabling
   it on existing data requires an explicit backfill/rebuild design.
6. For a multi-analyzer BM25 field, message term batches and dictionaries are
   separated by analyzer identity. A term analyzed with analyzer A must never
   be inserted into or queried through analyzer B's dictionary.

`enable_fuzzy_bm25` is not intended to become the permanent owner of text
index construction. It exists so fuzzy BM25 can be delivered before the
complete Text Term Index is shared with `text_match_fuzzy`.

The expected future rule is:

```text
enable_match or Match Index enabled
  -> build FST + postings once
  -> text_match, text_match_fuzzy, fuzzy BM25, prefix, wildcard share it
```

---

## 8. Phase 1 architecture

### 8.1 End-to-end overview

```text
                               WRITE PATH

Proxy -> StreamingNode WAL materialization
          |- analyzer -> hashes + TF -> BM25 sparse output
          `- analyzer terms -> message-level unique term sidecar
                                  |
                                  v
                         WAL Insert message
                                  |
                    +-------------+-------------+
                    |                           |
                    v                           v
          query growing segment          Flush Manager
          Mutable Trie update             per-sync term set
                                                |
                                                v
                                        sync FST fragment
                                                |
                                                v
                                         sort compaction
                                            one FST


                               SEARCH PATH

Delegator waits for tSafe and pins lexical snapshot
          |
          v
analyze query without hashing
          |
          v
ExpandTerms RPC, batched by Worker
          |
          v
merge candidates -> target DF filter -> global rewrite limit
          |
          v
hash candidates -> query TF -> target-bound IDF vector
          |
          v
SearchSegments with the same vector for every target segment
```

### 8.2 Analyze once during WAL materialization

The StreamingNode shard interceptor already materializes BM25 function output
before WAL append. For `enable_fuzzy_bm25` fields, the materializer must expose
both results from the same analyzer pass:

```text
input text rows
  -> analyzer
      |- hashed per-row TF for the BM25 sparse output field
      `- original term bytes for the message term sidecar
```

Re-running the analyzer only to collect terms is undesirable because it doubles
CPU cost and risks divergence if an analyzer or external resource is not fully
deterministic.

The message term sidecar is stored in the Insert message body, not in the light
WAL header or message properties. Its logical shape is:

```text
TextTermBatch {
    input_field_id
    analyzer_identity
    repeated bytes sorted_unique_terms
}
```

The actual wire change can either extend `msgpb.InsertRequest` or introduce a
new versioned Insert body. It must follow the streaming message code-generation
and rolling-upgrade rules.

Terms are deduplicated within one message for each `(field, analyzer identity)`.
The current Insert message is assigned to one partition/segment. If batch
inserts later span multiple segment assignments, the sidecar must become
assignment-scoped; applying the union of the whole message to every assigned
segment would make the segment dictionaries incorrect.

Term bytes are the exact bytes used by the text index. No Unicode
normalization, lowercasing, or stemming is performed after the analyzer.

### 8.3 Growing segment dictionary

The QueryNode Worker maintains one `MutableTrieDictionary` per enabled growing
segment, field, and analyzer identity.

When an Insert message becomes visible to the growing segment, its message term
batch is inserted into the Trie. Trie insertion is idempotent, which makes
replaying a term that is already present in a synced FST fragment safe.

The write-side Flush Manager also consumes the same message term batch and
updates its per-segment, per-sync term set. These two consumers have different
responsibilities:

- the query-side Trie serves current growing searches;
- the Flush Manager term set produces a durable FST fragment at sync.

They must advance under the same message/checkpoint boundary as the row data.

### 8.4 Sync writes FST fragments

Every sync sorts and deduplicates the terms newly covered by that sync and
writes one immutable FST fragment for each enabled `(field, analyzer identity)`.
The in-memory term set is cleared only after the sync manifest is committed.

A separate term-enum file is unnecessary. FST construction from an already
sorted per-sync term set is inexpensive, and an FST can be traversed in lexical
order with low overhead whenever compaction or recovery needs to enumerate all
terms. Persisting both the sorted terms and an FST would duplicate the same
vocabulary and add another artifact whose checkpoint and lifecycle must remain
consistent.

Multiple small FST fragments are valid dictionaries. A Worker can traverse
them directly and deduplicate the result, while sort compaction later reduces
them to one FST for the stable sealed-segment path.

A sync atomically publishes:

- insert data files;
- BM25 statistics;
- zero or one FST fragment per enabled `(field, analyzer identity)`;
- the data checkpoint and FST coverage checkpoint;
- the manifest or V2 metadata that references the complete set.

The core invariant is:

```text
data_checkpoint == fst_coverage_checkpoint
```

An empty-term sync still advances FST coverage in the manifest even though it
does not create a physical FST file. Otherwise recovery cannot distinguish
“this data range contained no new terms” from “term persistence was lost.”

Each FST fragment, together with its manifest entry, records at least:

- field ID and analyzer identity;
- covered WAL/checkpoint range;
- term count;
- encoding version;
- checksum.

The manifest publication is the commit point. A fragment uploaded but not
referenced by the committed manifest is garbage, not readable state.

### 8.5 Sort compaction builds the persistent dictionary

Sort compaction opens iterators over all input FST fragments, performs a
streaming k-way merge, deduplicates terms, and produces exactly:

```text
one immutable FST file
```

The FST describes one dictionary generation and is published with the
compaction output segment. Later mix, merge, schema-bump, or other segment
compactions preserve the same one-FST property for their output.

The consolidated FST remains fully enumerable and is therefore sufficient as
both compaction input and query artifact. Rebuilding it only requires iterating
the previous FST generation; no raw term list is required.

Compaction may apply deletes or TTL while merging data. A simple union of input
input FSTs can therefore retain terms that no longer occur in a live row. This
does not create false-positive BM25 matches because those terms have target DF
zero and no postings in the BM25 sparse index. Phase 1 filters target-DF-zero
terms before applying candidate limits. A future compaction can optionally
rebuild the enum from live rows to reclaim stale vocabulary.

For a future complete Text Term Index, sort compaction should build the FST and
postings together. The existing Tantivy `TextMatchIndex` already builds this
kind of full segment-local structure for `enable_match` fields; the long-term
implementation should expose or evolve that artifact rather than permanently
building a second FST for fuzzy BM25.

### 8.6 Loading and recovery

The dictionary must be ready before the segment becomes readable for exact
fuzzy BM25.

| Segment state | Dictionary load behavior |
|---|---|
| Growing, including flushed-but-invisible | Load or mmap all committed FST fragments as an immutable base, initialize an empty mutable Trie for the later WAL delta, load BM25 stats, then resume WAL term updates. |
| Sorted sealed | Load or mmap the one consolidated persistent FST directly. |
| Visible sealed without a consolidated FST | Load its committed FST fragments as a composite dictionary. A Worker may optionally merge and cache one local FST when fragment count crosses a threshold. |
| Missing required FST coverage | The segment is not exact-fuzzy-readable. Fail exact fuzzy BM25 instead of silently omitting its vocabulary. |

For a growing recovery, ordering is:

```text
load segment data through committed checkpoint
  -> load BM25 stats through the same checkpoint
  -> load FST fragments through the same checkpoint as immutable base
  -> initialize Mutable Trie for later WAL terms
  -> publish segment as readable
  -> replay later WAL messages from seek position
```

Term insertion is idempotent, so overlap at a recovery boundary is safe. Query
expansion unions the immutable FST base and mutable Trie delta. A gap between
the data and FST coverage checkpoint is not safe and must prevent readability.

The visible-sealed-without-consolidated-FST path is required when sort
compaction is disabled or delayed. Direct fragment traversal is the correctness
path; a locally merged FST is only a cache and must not be uploaded from the
query path. Durable artifact publication remains a DataNode/compaction
responsibility.

### 8.7 Common Worker dictionary interface

Upper layers do not branch on mutable Trie, a composite of FST fragments, or a
single compacted FST. The Worker segment exposes one logical interface:

```text
ExpandTerms(
    field,
    analyzer_identity,
    source_terms,
    fuzzy_options,
    expected_dictionary_generation
) -> candidate terms
```

The implementation intersects the fuzzy automaton with the segment dictionary.
The initial edit-distance and transposition semantics should align with
`text_match_fuzzy` unless the public fuzzy BM25 API explicitly defines a
different contract.

The result preserves enough information for deterministic global rewriting:

- query index (`nq` index);
- source-token index;
- candidate term bytes;
- edit distance;
- optional future boost;
- served segment and dictionary generation information.

### 8.8 Batched expansion RPC

The Delegator groups the pinned target segments by Worker and sends one
`ExpandTerms` RPC per Worker rather than one RPC per segment.

```text
ExpandTermsRequest {
    collection_id
    vchannel
    lexical_snapshot_version
    field_id
    analyzer_identity
    fuzzy_options
    repeated source_terms_by_query
    repeated SegmentDictionaryRef segments
}
```

The Worker may deduplicate the union of candidates across its requested
segments before returning them, but it must validate that every requested
segment and dictionary generation was served. A response that silently skips a
segment is invalid.

Local Workers use the same interface without serialization. Remote Workers use
a new QueryNode RPC and a corresponding method on the existing `cluster.Worker`
abstraction.

### 8.9 Query analysis and candidate merge

After waiting for tSafe and pinning the readable distribution, the Delegator
analyzes the raw query with `with_hash=false`. It retains:

- the source term bytes;
- per-query source term frequency;
- source-token ordering when needed by the fuzzy rewrite policy;
- analyzer identity.

The Delegator merges and deduplicates Worker responses by query and source
token. The same candidate returned by many segments appears only once in the
final query vocabulary.

Before hashing, the Delegator performs the global rewrite in this order:

1. merge candidates from all required dictionary sources;
2. look up DF against the pinned target segments;
3. remove candidates whose target DF is zero;
4. apply the global `max_expansions` policy;
5. compute candidate query TF and optional fuzzy boost;
6. hash candidate terms and combine candidates that collide on the same BM25
   hash;
7. build the IDF vector from the same pinned target statistics.

Filtering DF-zero candidates before `max_expansions` is necessary because stale
terms from deletes, TTL, or a broad Global FST must not consume the expansion
budget and suppress live terms.

An initial deterministic rewrite can order candidates by:

```text
(edit distance ascending, target DF descending, term bytes ascending)
```

The exact fuzzy boost and query-TF aggregation policy is part of the public
scoring contract and must be finalized before implementation. A simple initial
policy is constant boost with the expanded candidate inheriting its source
term's query frequency; contributions are summed when multiple source terms
rewrite to the same candidate.

### 8.10 Target-bound IDF

The current `IDFOracle.BuildIDF(fieldID, tf)` reads the Oracle's current
aggregated stats. Its API does not prove that those stats are the same segment
target pinned by the search.

Fuzzy BM25 needs a stronger API because candidate filtering, candidate limits,
IDF, and search routing all depend on the same target. The implementation
should introduce either:

```text
BuildIDFAt(lexical_snapshot, field, query_tf)
LookupDFAt(lexical_snapshot, field, term_hashes)
```

or a single immutable lexical-snapshot object that provides both operations.

This is especially important for partition-scoped search and Global FST use. A
Global FST can contain terms from segments outside the requested partitions.
Only DF computed over the actual pinned target can remove those terms before
they consume `max_expansions`.

### 8.11 Search dispatch

After the IDF vector is constructed, the Delegator replaces the raw text
placeholder with the final BM25 sparse vector and sends the same vector to all
target segments.

```text
expanded global query vector Q
  -> Worker A: segment 1, segment 2
  -> Worker B: segment 3
  -> local growing segment 4
```

A segment naturally ignores dimensions for terms it does not contain. Using
one unioned vector preserves score comparability and avoids a separate vector
per segment.

The fuzzy expansion step belongs after `PinReadableSegments` and before the
existing search subtask organization and `SearchSegments` calls.

### 8.12 Failure semantics

Exact mode is fail-closed:

- a required Worker expansion failure fails the query or restarts the complete
  expansion on a newly pinned lexical snapshot;
- a missing dictionary generation is not treated as an empty vocabulary;
- analyzer identity mismatch fails the query;
- a distribution change after pinning does not alter the in-flight query;
- partial expansion results must not be combined with full-target IDF stats.

If existing partial-search behavior selects a reduced readable target, fuzzy
expansion, DF/IDF, and `SearchSegments` must all use that same reduced target.
An expansion RPC failure after the target is pinned must not independently drop
another segment and continue.

---

## 9. Phase 2: Global FST for exact acceleration

### 9.1 Motivation

Phase 1 is exact but adds a pre-search RPC to every Worker that owns a target
segment. Large collections may contain enough segments that expansion latency
and RPC fan-out become significant.

A Global FST reduces this cost by materializing a union of stable segment
vocabularies close to the Delegator on the SN.

### 9.2 Global FST is a base, not a replacement

The Global FST represents a compacted base vocabulary:

```text
exact target vocabulary
    = Global FST vocabulary for covered segments
      UNION
      segment dictionaries for uncovered segments
```

It does not contain postings and cannot execute segment-local filters. Workers
continue to load their segment dictionaries and future complete Text Term
Indexes.

### 9.3 Building and publishing

A background global-dictionary compaction job opens iterators over consolidated
segment FSTs and performs a streaming k-way union. It publishes:

- one Global FST per `(collection/vchannel, field, analyzer identity)`;
- a generation ID and format version;
- a coverage manifest;
- checksum, term count, and build timestamp.

The Global FST is loaded or mmapped on the SN that hosts the shard Delegator.
Generation publication is atomic: a Delegator sees either the old complete
generation or the new complete generation, never a partially uploaded set.

### 9.4 Coverage manifest

Using only `segment.end_ts < global_checkpoint` is not sufficient to prove
coverage. A segment can be rebuilt by compaction, use a new analyzer generation,
or be published after the checkpoint while containing older timestamps.

The coverage manifest must bind at least:

```text
vchannel
field_id
analyzer_identity
global_dictionary_generation
WAL/checkpoint frontier
covered segment dictionary generations
```

A segment is covered only when its exact dictionary generation is listed, or
when coverage can be safely inherited from fully covered compaction inputs
under an explicit coordinator rule.

The checkpoint remains useful as a fast eligibility hint, but segment end
timestamp alone is not the correctness proof.

### 9.5 Exact search with Global FST

The Delegator pins the Global FST generation with the lexical snapshot and
partitions the target:

```text
covered target segments     -> Global FST
uncovered target segments   -> batched Worker ExpandTerms RPC
```

The query flow becomes:

```text
analyze source terms
  -> expand once against pinned Global FST
  -> expand uncovered segments on Workers
  -> union candidates
  -> target DF filter
  -> global rewrite and IDF
  -> SearchSegments
```

If all target segment dictionary generations are covered, exact expansion uses
zero Worker RPCs. If no compatible Global FST is loaded, exact mode falls back
to the Phase 1 all-Worker path.

Terms for deleted or no-longer-targeted segments can remain in the Global FST.
The target-DF-zero filter removes them before candidate limiting. Therefore the
Global FST can be maintained as a monotonic vocabulary union initially, which
simplifies publication and compaction.

### 9.6 Global and segment generation changes

An in-flight query keeps its pinned Global FST and distribution generations.
New segment loads, compactions, and Global FST publications affect later
queries only.

If a target transition introduces a segment generation not covered by the
pinned Global FST, exact mode includes that segment in the Worker RPC delta. A
newer Global FST must not be substituted midway through the query because its
coverage may describe a different target.

---

## 10. Phase 3: user-controlled approximate expansion

Phase 3 exposes an explicit query option:

```text
fuzzy_expansion_mode = exact | global_only
```

### 10.1 `exact`

- Default mode.
- Use the Global FST for covered segments.
- RPC to Workers for every uncovered target segment.
- Fall back to the Phase 1 all-Worker path if no compatible Global FST exists.
- Fail rather than silently lose vocabulary when a required RPC cannot be
  completed.

### 10.2 `global_only`

- Use only the pinned Global FST.
- Do not request uncovered segment dictionaries.
- Reject the query if no compatible Global FST exists.
- May miss terms introduced after the Global FST coverage frontier.
- Can change recall, the expansion set, and BM25 ranking.

This mode is useful when a large, mature collection has a sufficiently complete
Global FST and the user prefers predictable low RPC latency over the freshest
possible vocabulary.

The response or query metrics should expose the used Global FST generation,
coverage ratio, and lag so that approximate behavior is diagnosable.

---

## 11. Metadata and interface changes

The implementation is expected to require changes in the following areas.

### Schema

- Parse and validate `enable_fuzzy_bm25` on string fields.
- Include it in schema compatibility and immutable-property checks.
- Define its interaction with multi-analyzer fields.

### WAL message

- Add a versioned, field/analyzer-scoped term sidecar to Insert messages.
- Generate/update specialized message bindings through the existing codegen
  path.
- Preserve the sidecar through transaction assembly and replication.

### Segment metadata and manifest

- FST fragment entries with checkpoint coverage.
- Consolidated FST entries with dictionary generation.
- Analyzer identity and encoding version.
- Global FST coverage manifest and generation.

### QueryNode RPC

- Add batched `ExpandTerms` request/response messages.
- Add the method to the QueryNode service and `cluster.Worker` interface.
- Add local and remote Worker implementations.

### Delegator and IDF Oracle

- Analyze fuzzy BM25 queries before hashing.
- Pin a lexical snapshot.
- Merge Global/Worker candidates and perform a deterministic global rewrite.
- Add target-bound DF and IDF operations.

### Worker segment runtime

- Add the common `SegmentTermDictionary` interface.
- Maintain mutable Trie dictionaries for growing segments.
- Load/mmap persistent FST dictionaries for sorted sealed segments.
- Traverse FST fragments as a composite dictionary and optionally cache a
  locally merged FST when necessary.

---

## 12. Correctness invariants

1. **WAL materialization invariant**: BM25 sparse output and the message term
   batch come from the same analyzer execution and analyzer identity.
2. **Sync coverage invariant**: data and FST coverage checkpoints advance
   together, including empty-term syncs.
3. **Readable segment invariant**: an exact-fuzzy-readable segment has a
   dictionary covering all of its readable data.
4. **Generation invariant**: every expansion source is validated against the
   dictionary generation pinned by the query.
5. **Snapshot invariant**: segment distribution, term expansion, DF/IDF stats,
   and Global FST coverage describe one lexical snapshot.
6. **Global vector invariant**: every target segment receives the same final
   BM25 query vector.
7. **Exact-mode invariant**: every target segment's vocabulary is represented
   by either the pinned Global FST or a successful Worker expansion response.
8. **Rewrite invariant**: DF-zero terms are removed before `max_expansions`.
9. **Analyzer invariant**: dictionaries built by different analyzer identities
   are never unioned or queried together.

---

## 13. Resource management and observability

### 13.1 Memory and disk accounting

Account separately for:

- growing Trie memory;
- persistent FST mmap or resident memory;
- FST-fragment mmap/resident memory and optional local merged-FST cache;
- loaded Global FST memory/mmap;
- FST fragment object count and bytes.

Locally merged FST caches should use the existing segment lifecycle and resource
manager so release/reopen removes the correct dictionary generation.

### 13.2 Metrics

Recommended metrics include:

- terms emitted and unique terms per WAL message;
- FST fragment count and bytes per segment;
- growing Trie term count and memory;
- fragment/composite/consolidated FST load latency and memory;
- expansion candidate count before and after deduplication, DF filtering, and
  `max_expansions`;
- expansion RPC fan-out, latency, bytes, and failure count;
- percentage of target segments covered by the Global FST;
- queries requiring zero, partial, or full expansion RPC;
- Global FST generation age and checkpoint lag;
- `global_only` query count and observed uncovered-target ratio.

Tracing should make the following stages visible under the search trace:

```text
analyze -> pin lexical snapshot -> global expansion -> worker expansion
        -> candidate rewrite -> IDF build -> segment search
```

---

## 14. Compatibility and rollout

### 14.1 Existing collections

Existing fields default to `enable_fuzzy_bm25=false`. Phase 1 does not claim
that enabling the property online makes old segments immediately searchable.
An online enable flow needs a backfill job that creates complete FSTs before
publishing the new schema capability.

### 14.2 Rolling upgrade

Collections using `enable_fuzzy_bm25` must be scheduled only to nodes that
understand:

- the WAL term sidecar;
- the segment dictionary metadata;
- the `ExpandTerms` RPC;
- lexical snapshot generation checks.

Old consumers must not discard a required term sidecar and still advertise
the segment as exact-fuzzy-readable. Capability/version gating is therefore
required before the feature can be enabled in a mixed-version cluster.

### 14.3 Existing `enable_match` fields

The first implementation may build a lightweight fuzzy-BM25 dictionary beside
the current Tantivy text index, but this duplication must be treated as
transitional. The next consolidation step should make the Tantivy/Milvus Text
Term Index export FST enumeration and expansion interfaces required by the
Delegator path.

---

## 15. Testing plan

### Write and recovery

- Message-level deduplication, including empty terms and repeated terms.
- BM25 output and the message term batch use exactly the same analyzer identity.
- Multi-analyzer dictionaries remain isolated.
- Sync publishes matching data/FST-coverage checkpoints.
- Crash before and after manifest commit leaves either the old complete state
  or the new complete state.
- Growing recovery loads the immutable FST-fragment base and resumes the Trie
  WAL delta without gaps.
- Collection load between flush and sort restores the flushed-invisible segment
  as growing with a complete dictionary.

### Compaction and load

- K-way iteration over input FSTs produces one consolidated FST.
- Sort, mix, schema-bump, and merge compactions publish correct generations.
- Composite fragment traversal and consolidated FST traversal return identical
  expansions.
- Missing dictionary artifacts prevent exact-fuzzy readability.
- Deleted/TTL-only stale terms are removed by target DF before candidate limit.

### Search correctness

- Candidate union is independent of Worker/segment placement.
- All target segments receive byte-identical query vectors.
- Partition-scoped search filters Global FST terms using target-bound DF.
- Hash collisions are aggregated consistently with current BM25 semantics.
- Expansion failure cannot silently produce partial vocabulary in exact mode.
- Distribution or dictionary generation changes during a query do not mix
  snapshots.

### Global FST

- Covered plus uncovered-delta expansion equals all-segment expansion.
- Fully covered targets issue zero expansion RPCs in exact mode.
- End timestamp alone does not incorrectly mark a rebuilt segment covered.
- Global generation publication is atomic.
- `global_only` behavior is stable and reports coverage/lag.

### Performance

- WAL analyzer CPU and message-size overhead.
- Trie memory under high-cardinality text.
- FST build, load, and mmap latency.
- Expansion latency versus edit distance and vocabulary size.
- RPC fan-out reduction from Global FST coverage.
- Candidate explosion and `max_expansions` effectiveness.

---

## 16. Alternatives considered

### Load every segment FST on the Delegator

This avoids expansion RPCs but duplicates all Worker dictionaries on the SN,
increases memory and load traffic, and separates the dictionary from the
segment-local features that also need it. It does not scale as the common Text
Term Index grows to include postings.

### Build one global FST only

A global-only design cannot represent newly growing or recently compacted
segments until the next global build. Without a segment delta path, exact
search is impossible. It also does not serve segment-local posting lookup.

### Persist both term enums and FSTs

The FST is already a losslessly enumerable, lexically ordered representation
of the term set. Persisting a parallel term-enum file duplicates storage,
object count, checkpoint metadata, compaction inputs, and failure handling
without adding information. Direct FST iteration is sufficient for recovery,
segment compaction, and Global FST construction.

### Expand independently inside every BM25 segment search

Each segment would construct a different query vector and potentially use a
different expansion limit. Result scores would not share one query rewrite or
IDF context, making global ranking inconsistent.

### Re-analyze raw text during every load

This avoids dictionary files but substantially increases load/recovery cost,
requires raw input availability, and risks analyzer-version divergence. Durable
FST fragments make the analyzed vocabulary explicit and reproducible.

---

## 17. Open decisions

The architecture does not depend on the following details, but they must be
finalized before implementation:

1. Public fuzzy BM25 request syntax and the complete option set.
2. Exact fuzzy boost and query-TF aggregation semantics.
3. Scope and ordering semantics of `max_expansions` for multi-token and multi-NQ
   requests.
4. FST format, fragment metadata, and compression/checksum details.
5. Whether the first implementation extends `InsertRequest` or introduces a new
   versioned Insert body.
6. Coordinator and object-storage protocol for Global FST build/publication.
7. Online enable/backfill and disable semantics.
8. The milestone at which the lightweight fuzzy-BM25 FST is consolidated with
   the existing Tantivy `TextMatchIndex` files.

---

## 18. Related code and documents

- `docs/design-docs/design_docs/20260702-text_match_fuzzy.md`
- `internal/streamingnode/server/wal/interceptors/shard/function_materializer.go`
- `internal/flushcommon/writebuffer/write_buffer.go`
- `internal/flushcommon/syncmgr/`
- `internal/datanode/compactor/sort_compaction.go`
- `internal/datanode/compactor/compactor_common.go`
- `internal/querynodev2/delegator/delegator.go`
- `internal/querynodev2/delegator/delegator_data.go`
- `internal/querynodev2/delegator/idf_oracle.go`
- `internal/querynodev2/delegator/distribution.go`
- `internal/querynodev2/segments/segment_loader.go`
- `internal/core/src/index/TextMatchIndex.*`
- `internal/core/thirdparty/tantivy/tantivy-binding/src/index_reader_text.rs`
