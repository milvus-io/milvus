# StreamingNode IDF Oracle Runtime

## Contract

A loaded vchannel owns one current BM25 aggregate, shared by all QueryViews and
all request DataVersions. Statistics are the newest DataView baseline successfully prepared locally for
a received QueryView, plus applied live growing events. "Newest" is local to SN;
it does not mean the coordinator's globally latest DataVersion. They are not historical
MVCC snapshots. Segment selection, MVCC visibility and Up serving leases remain
versioned independently. A query plan fixes its IDF vectors and average document
length for SN/QN execution; all BM25 subqueries in one plan read one aggregate
state.

DataView lifecycle protects referenced objects until the view is fully Dropped.
An old view's release acknowledgement must follow the required IDF transition,
or complete Oracle shutdown. This design relies on that protection and does not
implement a missing-object/GC full-rebuild fallback.

## Retained state

- One aggregate per loaded BM25 field: document frequencies, row and token counts.
- One mutable BM25Stats per contributing growing segment, with flush/seal metadata.
- Sealed resource descriptors (segment, partition, exact stats paths/manifest),
  without per-segment sealed statistics, memory cache or local disk cache.
- Current DataVersion, serialized preparation and at most one shared lazy materialization.
- Growing contribution membership (segment IDs only), without duplicate statistics.

There is no DataVersion-to-statistics map or duplicate growing contribution map.
Same-ID resource changes replace the old descriptor and contribution. Zero
frequency keys are removed; map capacity can be compacted after substantial
shrinkage.

## Initialization

QueryRuntime initialization resolves load metadata and prepares its modules from
the no-gap WALView snapshot. IDF loads sealed resources concurrently under one process-wide limit and merges
each completed result immediately, discarding parsed segment statistics afterward.
A worker holds its permit through both decoding and merging, preventing an
unbounded queue of decoded segment statistics. Growing
statistics come from persisted stats and snapshot inserts and remain in memory.
RecoveryStorage may capture WAL inserts before its asynchronous pack writer has
materialized BM25 outputs. Both the growing search segment and IDF recovery fill
missing function outputs on a private request copy, preserving existing outputs
and never mutating retained WAL bodies. They use the existing local runner
fallback, with managed runners when available.
Only loaded BM25 output fields contribute. Live events buffered during build are
applied before readiness. Partial initialization is never published.

The default eager mode materializes the aggregate during initialization. With
`queryView.idfOracle.lazyLoadSealedStats=true`, initialization prepares growing
stats only. The first BM25 query discovers sealed resources for the runtime's
recorded target DataVersion and materializes the shared aggregate. Concurrent
queries share one load. Canceling a query only cancels its wait; runtime close or
a newer target cancels the shared load. Waiters retry against a changed target.
The same applied-event barrier and final-commit checks protect first publication.
Changing the lazy setting affects subsequently created runtimes.

`queryView.idfOracle.sealedStatsLoadConcurrencyRatio` defaults to 4 times CPU cores
and dynamically resizes the shared limit. Both new configuration keys are version
3.1.0. Reads reuse `queryNode.idfOracle.readBufferSize` and the existing storage
open/read retry configuration.

## Refresh and handoff

A received QueryView prepares its explicit DataVersion before reporting Ready.
Preparation first passes the owner final-commit check and applied-event barrier,
then advances the single materialized aggregate. The old aggregate remains usable
while I/O proceeds. An unmaterialized lazy runtime only updates its target; it
performs no sealed I/O until the first BM25 query. Once materialized, all later
preparation is synchronous. There is no independent background advancement task
or per-version prepared-resource map.

Initialization and refresh use the exact-version resource RPC and validate its
response. Equal or older targets reuse the aggregate without a fetch. Coordinator
progress alone does not select a different version; seal notifications supply
handoff metadata only. Retryable preparation failures remain Preparing and retry
through NodeScheduler. Permanent failures invoke OnUnrecoverable. An in-progress
preparation returns the scheduler delay sentinel to another preparation instead
of blocking a scheduler worker behind it.

Compare resource descriptors, not only segment IDs. Read removed sealed resources
from their recorded old S3 locations into the negative delta; read added resources
into the positive delta. Unchanged resources require no stats reads. Growing
contributions leaving the aggregate are subtracted from their in-memory stats.
Reads occur outside the Oracle mutex and use bounded streaming buffers.

After initialization, the Oracle mutex also owns all growing-statistics access,
including segment membership, mutable segment statistics, flush/seal metadata,
and cleanup. Cleanup runs in the same critical section as aggregate publication
or target advancement, rather than using an independent store mutex after
releasing the Oracle lock. Live events, version preparation and lazy
materialization can run on different goroutines; each observes consistent
membership and statistics under this one lock.

One refresh executes at a time, keeping the sealed membership base stable. A WAL
event barrier is enqueued under the vchannel owner lock after object reads and
drained by the query dispatcher before commit. Commit reconciles
growing handoff, then applies the complete delta and publishes membership/version
under one lock. Readers never observe a partial subtraction. Ordinary unrelated
inserts update growing stats and the aggregate and do not invalidate S3 work.
A failed or cancelled read discards its unpublished delta and retries; the last
complete aggregate remains available.

A growing-to-sealed handoff waits for the segment's ordered flush observation and
final-commit sealed DataVersion. Unresolved flushes delay publication, including
when compaction has already replaced the segment in the target. No growing
contribution can be counted alongside its sealed replacement. Empty growing
segments retire on flush without waiting for a sealed DataVersion, which the
coordinator does not assign to empty segments. Growing BM25 stats
can then be freed even if old QueryViews retain the physical search segment.

The oldest QueryView watermark still controls GrowingRuntime resource GC. It no
longer controls IDF freshness. Before completing a view release, IDF must reach
the remaining view watermark; closing the last runtime instead cancels/drains
refresh work and releases all statistics. No scheduled worker waits for another
queued task on the same scheduler.

## Query behavior

BuildIDF/BuildIDFBatch accept a query context for lazy initialization and do not
select statistics by request DataVersion. Query text is tokenized
before reading the aggregate; a batch of field/token requests is evaluated under
one read lock without copying the global vocabulary. Phase 2 uses the resulting
plan parameters, without another Oracle read.

An empty latest aggregate cannot prove that an older view has no rows. It must
not set SearchOptimization.Skip. For an empty corpus, document-frequency lookup
uses zero counts and average document length uses the positive fallback 1. Missing
loaded fields are errors, not empty corpora. This keeps scoring finite without
incorrectly suppressing old-view candidates.

## Cost and validation

With global vocabulary U, growing statistics G and sealed metadata M, steady
memory is O(U + G + M), independent of concurrent DataVersion count. Refresh
adds positive/negative delta memory plus at most C decoded segment statistics and
C read buffers, where C is the process-wide concurrency limit. Large compactions
may approach corpus size. The read limit bounds task count, not bytes; a very large
single segment can still require substantial decoding memory.
Metadata comparison remains O(M); stats I/O follows changed resources. After first materialization, query IDF
work is O(query nonzeros) with no object-store requests. There is no local IDF
file cache. Eviction trades an additional old-stats S3 read for lower memory.

Validate flush/compaction and same-ID replacement, concurrent inserts, delayed
seal notification, failure/retry/cancellation atomicity, release ordering, multiple
old/new views using one aggregate, hybrid-query consistency, empty latest corpus
with nonempty old-view candidates, and stable memory/read counts across many
versions. Use real BM25 RPC validation as well as deterministic tests.

Key packages: wal/vchannel/idf, wal/vchannel/queryresource,
wal/adaptor/query_plan.go, datacoord/services_sn_query.go, storage/stats.go.

## Merge decisions for BM25 loading optimization

The disk-backed cache from 6f0e1991 is intentionally excluded. Its benefit is fewer
S3 reads when removing sealed contributions; its cost is disk occupancy proportional
to retained resources, local-file references/cleanup, and additional preparation
state. DataView lifetime already protects the source objects. This implementation
keeps the established no-cache resource contract and adopts lazy loading, bounded
parallel reads, immediate aggregation, retryable streaming reads, and chunked
decoding independently. It also preserves loaded partition/field scope, StorageV3
manifest recovery, batched hybrid-search scoring and atomic delta validation.
