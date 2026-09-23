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
- Current DataVersion, monotonic requested target and one scheduled refresh.

There is no DataVersion-to-statistics map or duplicate growing contribution map.
Same-ID resource changes replace the old descriptor and contribution. Zero
frequency keys are removed; map capacity can be compacted after substantial
shrinkage.

## Initialization

QueryRuntime initialization resolves load metadata and prepares its modules from
the no-gap WALView snapshot. IDF reads sealed statistics into the aggregate one
resource at a time, discarding parsed segment statistics after merging. Growing
statistics come from persisted stats and snapshot inserts and remain in memory.
RecoveryStorage may capture WAL inserts before its asynchronous pack writer has
materialized BM25 outputs. Both the growing search segment and IDF recovery fill
missing function outputs on a private request copy, preserving existing outputs
and never mutating retained WAL bodies. They use the existing local runner
fallback, with managed runners when available.
Only loaded BM25 output fields contribute. Live events buffered during build are
applied before readiness. Partial initialization is never published.

## Refresh and handoff

A received QueryView requests refresh to its explicit DataVersion. Pending
requests coalesce to the greatest requested version. Initialization and refresh
both use the existing exact-version resource RPC and validate that the response
matches the request. Equal or older targets reuse the aggregate without a fetch.
There is no latest-version RPC mode, periodic discovery, or additional config.
Coordinator-only progress (including compaction/import) is observed when the
corresponding QueryView arrives. Seal notifications record handoff metadata;
they do not request a different DataVersion. A failed refresh retries its known
target, while queries continue using the last complete local aggregate.

Compare resource descriptors, not only segment IDs. Read removed sealed resources
from their recorded old S3 locations into the negative delta; read added resources
into the positive delta. Unchanged resources require no stats reads. Growing
contributions leaving the aggregate are subtracted from their in-memory stats.
Reads occur outside the Oracle mutex and use bounded streaming buffers.

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

BuildIDF does not select statistics by request DataVersion. Query text is tokenized
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
adds temporary delta/decoding memory; large compactions may approach corpus size.
Metadata comparison remains O(M); stats I/O follows changed resources. Query IDF
work is O(query nonzeros) with no object-store requests. There is no local IDF
file cache. Eviction trades an additional old-stats S3 read for lower memory.

Validate flush/compaction and same-ID replacement, concurrent inserts, delayed
seal notification, failure/retry/cancellation atomicity, release ordering, multiple
old/new views using one aggregate, hybrid-query consistency, empty latest corpus
with nonempty old-view candidates, and stable memory/read counts across many
versions. Use real BM25 RPC validation as well as deterministic tests.

Key packages: wal/vchannel/idf, wal/vchannel/queryresource,
wal/adaptor/query_plan.go, datacoord/services_sn_query.go, storage/stats.go.
