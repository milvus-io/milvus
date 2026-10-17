# MEP: BM25 Term Dictionary

- **Created:** 2026-07-07
- **Author(s):** @thc1006
- **Status:** Under Review
- **Component:** StreamingNode, DataNode, QueryNode, Storage
- **Related Issues:** #50922, #50921, #50926
- **Related PRs:** #51098, #51952
- **Released:** N/A

## Summary

Milvus BM25 stores analyzed terms only as `uint32` sparse dimensions, so query
code cannot enumerate corpus terms. This MEP adds one complete per-segment term
dictionary for each fuzzy-enabled BM25 input field. Terms are captured beside
sparse TF in the same analyzer execution, persisted as row-aligned sidecars and
Rust FSTs, and carried through the normal segment lifecycle. Scoring DF and
average document length remain in `BM25Stats`.

#51952 proposes the fuzzy query consumer and overlaps this infrastructure. It is
still open. The two PRs must share one sidecar, artifact format, coverage rule,
and failure policy before either implementation lands. This MEP owns the shared
dictionary contract; #51952/#50921 owns user-visible fuzzy rewrite semantics
unless maintainers choose to combine the documents.

Before either design is approved, #51952 must reference this shared contract
and remove conflicting lifecycle rules, or the two documents must be combined.
Until then, neither document is an independently implementable specification.

## Motivation

The current path loses information needed for lexical expansion:

- `BM25FunctionRunner.run` hashes analyzer tokens and discards their strings.
- `BM25Stats` stores DF by hash, not by string. A positive bucket DF does not
  prove that a colliding string is live.
- `GrowingSourceSyncTask.Run` flushes only
  `[segment.FlushedRows(), targetOffset)` and may seal with zero new rows.
  Building at the final flush cannot cover the whole segment.
- `buildBM25IDF` analyzes a VarChar placeholder and rewrites it before target
  fan-out. Refeeding analyzed candidates through it would analyze twice.
- One final sparse rewrite does not require the delegator to own every FST;
  Workers can return candidates from their pinned segment artifacts.

These facts rule out flush-only re-tokenization, truncated child-FST union, and
per-segment `exact-only` fallback after a shared query has been expanded.

## Public Interfaces

### Function capability

A BM25 Function opts in through:

```text
function.params = {"enable_fuzzy": "true"}
```

The schema resolves an immutable binding:

```text
Function ID + output sparse field ID
    -> input VARCHAR field ID + schema version + analyzer/resource identity
```

The dictionary is keyed by the input field; `BM25Stats` and search are keyed by
the output field. Multiple enabled Functions may share a dictionary only when
their input and analyzer identity are identical and the output bindings remain
explicit.

v1 supports VARCHAR with one analyzer and create-time enablement only. It
rejects TEXT, including inline TEXT, and multi-analyzer BM25 rather than making
support depend on an inline/LOB threshold.

A future `false -> true` alteration must backfill every visible segment before
publishing fuzzy readiness. During backfill, `fuzziness > 0` is rejected while
exact BM25 remains available.

### Internal wire and storage surface

A fuzzy-enabled Insert carries a row-addressable analyzed-term sidecar beside
the existing sparse output. Segment storage contains:

1. durable row-aligned sidecar chunks used by compaction and repair; and
2. immutable FST fragments used by query-time enumeration.

The logical Insert sidecar is keyed by input field, schema version, and
analyzer/resource identity. It contains a flat `terms` byte-string array and
term-index `row_offsets`. The encoding invariants are:

- `len(row_offsets) == num_rows + 1`, the first offset is zero, offsets are
  monotonic, and the final offset equals `len(terms)`;
- each row's term slice is byte-sorted and deduplicated; repeated-token counts
  remain only in sparse TF; and
- null, empty, and tokenless input rows have an empty slice, preserving row
  positions.

Every producer and consumer validates these invariants and the binding before
using the payload. The implementation PR chooses unused protobuf tags on its
pinned `milvus-proto` and Milvus bases, then regenerates bindings. Exact message
and metadata names are settled once with #51952, not independently in both
documents.

Fuzzy search parameters, edit-distance semantics, candidate weighting, and the
unit of `max_expansions` belong to #51952. They are not duplicated here. The
consumer must state prefix units and transposition behavior consistently across
validation and native traversal; this MEP does not assign `min_doc_freq` an
Elasticsearch-derived default.

## Design Details

### Analyze once and preserve provenance

For each input row, one analyzer invocation returns:

- the existing sparse TF, including repeated-token counts; and
- normalized, unhashed term bytes with row membership.

The sidecar may dictionary-encode repeated bytes but cannot collapse row
boundaries. It must be possible to select terms for an arbitrary surviving
output-row set.

StreamingNode materializes this pair before appending the Insert to WAL. Both
values therefore share the Insert's VChannel, transaction, TimeTick, append
acknowledgement, replay, and CDC semantics. The analyzer digest is retained for
diagnostics, but a digest created during flush cannot prove the provenance of
already-persisted sparse TF.

QueryNode consumes the committed sidecar from each growing Insert to update the
field's Trie. Growing recovery first reconstructs that Trie from committed FST
fragments and then applies post-checkpoint WAL sidecars. It never analyzes the
input text again.

Today the materializer returns early when all Function output fields exist.
For an enabled binding, completeness must require the matching sidecar too. A
pre-materialized sparse output without its sidecar is an internal/mixed-version
protocol failure; StreamingNode must not synthesize a sidecar with a second
analyzer execution.

### Persist complete coverage

Each incremental sync writes a row-sidecar chunk and an FST fragment for the
same row range. FST fragments are needed before sort compaction because a
flushed unsorted segment can be restored as growing and queried before
consolidation.

A published generation records format version, segment and field identity,
schema/analyzer identity, source row ranges, a coverage fence, counts, payload
lengths, and checksums. Its normalized, non-overlapping ranges must cover
`[0, committedRows)` without a gap; a maximum offset or timestamp is not proof
of complete coverage.

Publication order is:

1. persist data, row sidecar, and FST fragment;
2. atomically publish one manifest/metadata generation referencing them;
3. acknowledge the checkpoint after that generation is durable.

Object-write success followed by metadata failure is retryable. Retry reuses or
recognizes the same range/generation. Replay deduplicates the parent row and its
sidecar together; FST set union is not a substitute for row-level idempotency.

A zero-row seal adds no fragment and carries forward the verified fragment set.
Recovery loads committed fragments, then replays WAL after the matching
checkpoint.

### Artifact and native ownership

The FST contains the complete normalized term set only. It has no scoring DF or
approximate DF hint. The Rust tantivy-binding owns build, enumeration, and fuzzy
intersection through direct pinned `fst` and `levenshtein_automata`
dependencies. Its CGO API has explicit buffer ownership and term lengths. Go
owns persistence and orchestration. Rust panics and borrowed buffers never cross
the C ABI; failures return a typed status. The existing Tantivy text-index FST
remains a separate artifact.

`complete` means every analyzer term for every covered inserted row is present
and row-range coverage has no gap. It is a no-false-negative property, not a
claim that the FST equals the live term set: soft-deleted terms may remain until
compaction.

A capped dictionary is never published as complete. `max_vocab` and a
dictionary-only `max_term_length` would destroy coverage. If a corpus-token
length limit is required, the capability contract declares it and the
materializer rejects an oversized token before WAL append; it cannot hash the
token into sparse TF while omitting it from the dictionary.

### Compaction and lifecycle

Compaction and statistics-sort rebuild one replacement FST from the durable
sidecars of final surviving output rows. They do not union child FSTs and do not
re-run a possibly changed analyzer. The existing compactor continues rebuilding
`BM25Stats` from output sparse rows. A required missing or corrupt sidecar aborts
output publication; compaction never converts incomplete input into a
`complete` artifact.

The replacement generation publishes with output data. Old generations remain
until query pins, recovery, and metadata references release them. Import, copy,
backup, restore, Storage V1/V2, Storage V3 manifests, and GC preserve or remove
the sidecar, FST generation, and coverage as one lifecycle unit.

The implementation audit includes `SaveBinlogPathsRequest`, compaction inputs
and results, `SegmentLoadInfo`, DataCoord catalog/snapshots, QueryCoord target
distribution, import/copy/restore, and object-store GC. Row-producing import
uses the same structured analyzer result; a binlog/copy import preserves a
matching sidecar/FST generation or rejects fuzzy enablement for that segment.

### Query snapshot and consumer constraints

A fuzzy request pins one operation snapshot containing:

```text
target/distribution version
selected growing and sealed segment IDs
per-target Trie/FST generation and coverage
Function/input/output/analyzer binding
BM25Stats generation
```

Expansion, candidate selection, IDF, dispatch, and search use that snapshot. A
target or artifact change either retains the old pin through search or restarts
the whole operation.

For a growing segment, applying a row and its sidecar publishes the row
visibility watermark with an immutable or reference-counted Trie generation.
A query pins a generation whose coverage reaches its read timestamp. Locking a
single mutable Trie only during traversal is insufficient if rows can become
visible to the same request afterward; vocabulary and searched rows would no
longer describe one snapshot. A row cannot become searchable before its terms
are present in the published generation.

v1 permits one analyzer/resource identity for all selected targets. A request
does not analyze once against one generation and search dictionaries produced
by another. Mixed identities make fuzzy readiness false until a compatible
rebuild completes.

The existing `idfOracle.BuildIDF` lock protects one call but does not bind its
mutable current stats to a target version. The fuzzy path needs a
reference-counted stats-generation handle acquired with the target snapshot and
passed to a prepared-TF IDF helper.

The consumer analyzes a query once and keeps its original exact sparse TF.
`cat cat` therefore retains exact TF 2. Exact dimensions do not depend on FST
membership and are not set-deduplicated. The consumer defines fuzzy-dimension
weighting and resolves string duplicates, hash aliases, exact-hash collisions,
and candidates reached from different source terms before serializing one
sparse vector.

Any bounded consumer must define whether its cap applies per FST fragment, per
segment, per source term, or to final sparse dimensions. A per-component cap is
approximate and can change after fragment consolidation. If the consumer claims
global exact top-k, it must enumerate all eligible candidates or use pruning
with a proved bound, apply eligibility and alias removal before the cap, and
keep the exact term independently.

Hashed DF is a bucket-level signal, not string-level liveness. Before
compaction, stale deleted terms can consume a finite expansion slot. v1 does
not promise top-k over the live string corpus.

For `fuzziness > 0`, every selected target must provide complete pinned
coverage. A missing, corrupt, stale, incomplete, or analyzer-incompatible
artifact marks the target fuzzy-unready without preventing its exact load. v1
then rejects fuzzy for the shard before expansion; it neither silently removes
that target nor sends it an `exact-only` query. A safe generation change is
retried; transient availability is a typed retriable system error; persisted
corruption is a typed data-integrity error. `fuzziness = 0` remains on the
existing exact path.

### Resource, security, and implementation conventions

Admission covers sidecar and FST builders, old and new generations during swap,
mmap/heap readers, growing Tries, query pins, traversal buffers, Worker replies,
and the final sparse query. Limits fail explicitly and never silently truncate
coverage or candidates. Existing WAL request-size validation includes the
sidecar; an oversized materialized Insert fails before append.

Term bytes are sensitive derived text. They use the same configured transport,
storage-encryption, and access controls as the parent Insert and segment. They
never appear in logs or metric labels.

Preliminary FST sizing used the repository's current `tantivy-fst` 0.5.0 line
and a streaming `MapBuilder<BufWriter<File>>` on Linux x86-64 with rustc 1.96.0.
The already-analyzed identifier corpus was the sorted sequence
`id-{counter:016x}-{splitmix64(counter):016x}`; no tokenizer was run. Peak RSS
was sampled from `/proc` every 10 ms.

| Unique terms | Serialized bytes | Observed builder peak RSS |
| ---: | ---: | ---: |
| 1,000,000 | 21,493,979 | 11,288 KiB |
| 10,000,000 | 214,931,057 | 11,476 KiB |

This is a high-cardinality identifier stress case, not a natural-language size
claim. Keys were generated in sorted order, so the measurement excludes sorting,
sidecar retention, loading, snapshot overlap, and query traversal. The complete
resource gate in the Test Plan still applies to the implementation.

Future Go changes use existing concrete types unless a second implementation
requires an interface, pass the available `context.Context`, use typed `merr`
errors, and log through `mlog`. Comments follow the repository review guide and
pre-2022 Kubernetes practice: document exported contracts and non-obvious
invariants or reasons, begin exported comments with the symbol name, and avoid
narrating visible code. A comment must remain shorter than the code it guards.

## Compatibility, Deprecation, and Migration Plan

Existing Functions default to fuzzy disabled and need no artifact. A
fuzzy-enabled segment with absent/corrupt coverage is distinct from a disabled
or pre-feature segment and cannot serve a fuzzy request.

Mixed-version fuzzy writes are unsupported. `enable_fuzzy` becomes usable only
after every producer, WAL consumer, DataNode, and QueryNode understands the
sidecar and artifact. Rollback after enabled writes begin requires stopping
those writes and fuzzy traffic.

Future online enablement uses a backfill generation. Exact search remains
available, fuzzy search remains unavailable, and readiness is published only
after all visible targets have complete coverage. Backfill cannot build only an
FST with the current analyzer and call it compatible: it must either verify the
rebuilt sparse TF row-by-row against stored TF or atomically rebuild sparse TF,
BM25 stats, sidecars, and FSTs from one analyzer execution. Disablement must
likewise define when sidecars and FSTs become collectible.

An analyzer configuration or resource-generation change follows the same
rule. v1 does not compact terms from different analyzer identities into an
artifact labeled with only the newest identity.

No existing protobuf tag is assumed. The implementation pins a base, allocates
an unused field, regenerates bindings, and verifies old readers ignore the new
optional field while enabled new readers distinguish optional absence from
required corruption.

## Test Plan

| Area | Required evidence |
| --- | --- |
| Capture | Sparse TF and sidecar come from one analyzer execution; row offsets, null/empty rows, binding, and byte ordering are validated; existing output without sidecar is rejected; repeated token TF is preserved. |
| WAL and recovery | Transaction, replay, CDC, duplicate delivery, two incremental syncs, zero-row seal, range-gap detection, and object-write-before-metadata-ack restart. |
| Compaction | Delete, TTL, and multi-output split rebuild from final row sidecars; capped child dictionaries are not used. |
| Query semantics | Filter-before-cap, exact term outside FST, repeated query TF, same/cross-source hash aliases, and no approximate prefilter claiming exact rank. |
| Snapshot | Target replacement or growing insert between expansion and search retains a generation through the read watermark or restarts; IDF uses the pinned stats generation. |
| Failure policy | Disabled absence is accepted; required missing/corrupt/stale/incompatible artifacts leave exact load available but reject fuzzy before expansion or target exclusion. |
| Lifecycle | Import, copy/restore, Storage V1/V2/V3 restart, pinned-generation GC, and rolling upgrade/rollback gates. |
| Resources | Reproducible 1M--10M-term measurements include sidecar/FST bytes, builders, load modes, swap peak, expansion work, and final query size. |

Go tests run with `-tags dynamic,test -gcflags="all=-N -l"`. Proto changes use
`make generated-proto-without-cpp`. Relevant DataNode and QueryNode suites run
per phase, and the final wire/metadata change runs `make test-go`.

## Delivery Plan

1. Native complete FST and versioned header.
2. Same-execution analyzer result and WAL sidecar.
3. Durable sidecar/FST fragments, coverage, and recovery.
4. Compaction, metadata, load, copy/restore, and GC lifecycle.
5. Joint #51952/#50921 consumer after the shared contract is approved.

Each change is test-first and independently reviewable. No phase claims a later
phase's end-to-end behavior.

## Rejected Alternatives

- **Re-tokenize at final flush:** the flush range is incremental and its analyzer
  does not prove ingest provenance.
- **Union capped child FSTs:** discarded strings cannot be recovered, and final
  row deletes/TTL are not represented.
- **Put DF in the FST:** scoring already uses hash-keyed `BM25Stats`; a string
  DF would introduce a second, conflicting statistic.
- **Require a delegator-global FST in v1:** one final rewrite does not determine
  artifact ownership. A global FST can be a later generation-covered cache.
- **Degrade one target to exact-only:** every target receives the shared
  expanded vector, so the claimed isolation is false.
- **Treat an analyzer digest as provenance:** a flush/query digest match says
  nothing about the analyzer that produced earlier sparse TF.

## References

- Issue #50922: shared term dictionary
- Issue #50921: fuzzy BM25 consumer
- PR #51952: overlapping fuzzy BM25 proposal
- `internal/util/function/bm25_function.go`
- `internal/streamingnode/server/wal/interceptors/shard/function_materializer.go`
- `internal/flushcommon/syncmgr/growing_source.go`
- `internal/querynodev2/delegator/delegator_data.go`
- `internal/querynodev2/delegator/idf_oracle.go`
- `internal/storage/stats.go`
- [Milvus contribution guide](../../../CONTRIBUTING.md)
- [Milvus code review guide](../../../CODE_REVIEW.md)
- [Kubernetes coding conventions (2020)](https://github.com/kubernetes/community/blob/427ccfbc7d423d8763ed756f3b8c888b7de3cf34/contributors/guide/coding-conventions.md)
