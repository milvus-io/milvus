# QueryView partition statistics and global segment pruning

## Extraction boundary

This branch implements the StreamingNode statistics consumer, global optimizer,
query-client dispatch/reduction, and shared selection contracts. The DataCoord
statistics producer, DataView publication and Coordinator builder described
below remain parent-integration responsibilities. QueryNode must apply the
selection through the injected execution-provider interface; its resource
implementation is not included here. The parallel `segment_manifest_versions`
field is synchronized as an interface so selection preserves physical resource
identity when the parent consumes this extraction.

## Ownership and publication

DataViewOfPartition binds an immutable `partition_stats_version`. Zero means
there is no pruning-statistics resource. A nonzero version identifies the
existing JSON object at
`<root>/part_stats/<collectionID>/<partitionID>/<vchannel>/<version>`.
The path/reference is the version identity; existing DataCoord remapping may
write zero in the JSON Version field. DataView contains the reference, not the statistics body. Clustering compaction
finishes writing/remapping statistics before publishing its output membership
and statistics version. A statistics-only change increments CompactVersion.
Ordinary flushes inherit the partition reference; uncovered segments remain
query candidates. Older DataViews retain their own reference.

QueryView carries `ClusteredPartitionStats { partition_id, version }` for its
loaded partitions in its StreamingNode portion. Collection/vchannel identity
comes from the enclosing QueryViewMeta; this proto is an immutable object
selector, while the resource manager owns runtime reference counting. SN constructs object paths directly; partition statistics do not use
GetStreamingNodeQueryViewResources. That RPC remains available for BM25.

## SN resources

The resource key is `(vchannel, partitionID, partitionStatsVersion)`. VChannel
already identifies the collection; collection ID is needed for path construction
but is not an additional cache-key component. QueryViews register references
before scheduling preparation. Concurrent references share one immutable decoded
snapshot and one in-progress load. Resource I/O runs outside owner/state locks.

Loading every declared resource is a hard QueryView Ready prerequisite, including
UpRecovering. Retryable failures use the existing preparation scheduler; corrupt
resources cannot be reported as successful empty statistics. Missing declarations
and unsupported pruning predicates are different from failed resource loads.
This resource does not inherit BM25's lazy-load option or latest-aggregate model.

Release removes the View's resource references. The last reference cancels pending
I/O and releases memory; a late completion cannot resurrect an unreferenced entry.
Serving View handles protect planning through completion, so no additional
query-level statistics pin is needed. Old/new Views can coexist and share versions.
After all resources prepare successfully, the manager publishes one immutable
partition-to-snapshot map in that View's reference record before reporting Ready.
ReadRuntime borrows that map in O(1), without rebuilding it or locking individual
statistics resources. Empty statistics scopes use a nil map without allocation.
Publication is serialized with Release and checks the registration's identity,
so a late preparation cannot populate a released or replacement reference.
Repeated readiness attempts reuse the published map. Release removes the View's
map together with its resource references; existing Go readers keep immutable
snapshots alive until their references disappear. WAL handoff/shutdown invalidate
queries and can bypass normal lease draining, but they neither mutate nor manually
free these Go-owned maps/snapshots.
The ownership and publication invariants are:

1. Preparing and recovered UpRecovering Views acquire references before scheduling
   any load. Manager.mu serializes registration, shared-reference counts, cached
   map publication and reference removal. Repeated registration of the same key
   does not increment ownership again.
2. Each `(vchannel, partitionID, version)` owns one resource and one immutable
   decoded snapshot. The resource mutex serializes loading/publication; concurrent
   preparations yield while loading. A different version has a different resource.
3. A query cannot obtain a statistics-bearing ReadRuntime until its View's cached
   map has been published. Each resource's successful prepare establishes snapshot
   visibility before the map is assembled; Manager.mu publishes the complete map
   to readers. Neither pruning nor teardown mutates published maps/snapshots.
4. Phase 1 holds a call-scoped View lease through planning. Normal Drop waits for
   active leases before Release; the shared resource survives until its last View
   reference is removed. Phase 2 uses the recorded selections rather than rereading
   partition statistics.
5. Release detaches the View map and decrements each resource exactly once. The
   last release cancels I/O and removes the resource from the manager's cache.
   A detached loader can only retain its old resource; it cannot insert it back
   into the cache. Preparation completion checks both runtime and registration
   identity under Manager.mu before publishing any View map. A canceled load from
   a released/replaced registration completes its readiness task normally when
   the task context is still live; cancellation of a current registration or of
   the task itself remains an error. This avoids scheduler failure logs for
   ordinary resource retirement without hiding unexpected cancellation.
6. Cached maps and snapshots are Go-owned. After handoff/Close or final Release,
   readers already holding them retain their memory until those readers finish.
   This protects memory, while normal WAL/View errors govern whether an interrupted
   query may complete. Recovery reloads exact persisted View versions; it never
   substitutes the latest statistics.

Memory reclamation never deletes objects. TODO: integrate historical statistics
objects with unified DataView/object GC, including recovery and other SN owners.

## Planning and execution

Phase 1 acquires an exact-schema Up View, fixes MVCC and resolves partition scope.
GlobalOptimizer consumes that View's statistics and sealed membership. Function
query preprocessing (including BM25) remains in the same planning pipeline.
Scalar pruning applies conservative range predicates. Term predicates sort their
locally converted values before evaluating range exits, including internal PK
plans that bypass the expression rewriter. This does not reorder the caller's
IDs or mutate the protobuf plan. Vector pruning uses the
existing partition-wide centroid/ratio/TopK selection across QN assignments;
it is approximate candidate selection, not an exact exclusion proof.

Only statistics matching candidate SegmentIDs and the queried FieldID/type are
eligible. Statistics for replaced segments do not influence vector budgets.
Uncovered or semantically unsupported candidates remain selected. Query/retrieve
never uses vector-distance pruning. A different searched vector field must not
use the clustering field's centroids. Growing candidates retain existing SN
scope/visibility handling and are not filtered using sealed partition statistics.

QueryPlan records per-worker segment selection with four explicit modes:
`All`, `Include`, `Exclude`, and `None`. `All` and `None` carry no IDs;
partial selections use the smaller nonempty inclusion or exclusion list, with
packed IDs. `All` is the default enum value, so an absent singular selection
also means all View members in scope. Repeated hybrid entries are explicit so
an `All` or `None` slot cannot shift subsequent subqueries. Entirely empty QN
workers are omitted; this does not remove SN growing work. QN intersects the
selection with its exact View and partition scope before acquiring physical
handles, then keeps the existing exact-registration Transform MVCC wait.
Pruning does not mutate View membership or release prepared segment resources.

Hybrid subqueries have separate selections, encoded relative to each node's
membership within that subrequest's partition scope. A fully selected scope
therefore needs no segment IDs. Empty local scopes and skipped slots are explicit
`None` entries and preserve positional identity. Each subquery executes its
own selection after partition filtering. The worker union remains encoded against
full node membership: it must preserve the original candidate set even if a
retained requery carries a broader partition scope. Retained requery uses the original View/MVCC/workers
and the union of original search selections, without rerunning vector pruning.
Retries discard the failed attempt's plan selections and partial results together.

Phase 1 returns diagnostic facts in
`GetQueryPlanResponse.execution_summary.global_optimizer.flags`, separately from
QueryPlan execution controls. `ExecutionSummary` contains optional subprocess
summaries. `GlobalOptimizerExecutionSummary` owns its uint32 bitmap independently
of feature-usage counters. Its nested `BitOffset` enum defines offsets (not masks):

- `PrunedByPartitionFiltering = 0`: requested partition scope excludes sealed candidates.
- `PrunedByScalarFiltering = 1`: scalar statistics exclude candidates.
- `PrunedByVectorFiltering = 2`: vector statistics exclude candidates.

Set bits with `uint32(1) << offset`. Record actual exclusions, not enabled settings,
loaded statistics, or an already-empty View. Partition filtering is independent
of the statistics-pruning switch and availability. Hybrid subqueries contribute
the union of their diagnostic flags. QueryClient retains only the successful
attempt's summary, including when no workers remain; failed attempts contribute
nothing. Partition diagnostics are collected in the existing worker-selection
partition pass, using one request membership set shared by QNs. Hybrid worker
selection uses the union of active subquery scopes; detecting an exclusion by
any subquery uses their intersection, with unrestricted scopes as the identity
and skipped subqueries ignored. This avoids rescanning View partitions per
subquery or repeatedly searching request ID slices. It does not iterate segment
IDs. Statistics pruning returns its scalar/vector flags with the exclusion set,
using its already-resolved clustering field without another Schema scan.
Proxy-side reduction maps scalar/vector flags to existing SegmentPrune
usage. Partition filtering and unknown bits do not map to that usage counter.

Empty summaries are omitted. For the current flags, the uint32 varint and tag
occupy 2 bytes; with both nested message envelopes the complete addition to the
Phase 1 response is 6 bytes. No summary adds 0 bytes; a full 32-bit word adds at
most 10 bytes including envelopes. Enum declarations occupy no per-message wire
bytes. Extend the bitmap with another uint32 field and its own offsets when
needed, without renumbering existing offsets or preallocating unused words.
Summaries are not forwarded to Phase 2 workers and never control execution.

Every successfully executed shard
plan with no collected results contributes an empty result and its MVCC snapshot,
including plans with no workers; this behavior does not depend on pruning.
Aggregation uses `PartialAggregationResult` at QN and shard reduction boundaries.
A fieldless or typed zero-row payload decodes to Empty, the merge identity;
a scalar NULL or a real zero remains a value. On the existing RPC, global Empty
is represented by one identity-state row: count=0 and NULL for min/max/sum.
GROUP BY Empty has zero rows in every column. Global zero-row RPC payloads
cannot be emitted because released 3.0 reducers read row zero. Wire validity is
projected with the shared validity helper at the QN and shard response boundaries,
after ORDER BY and limit have finished rebuilding columns, so both released and
current consumers observe the NULL state without version-dependent merge rules.
Intermediate encoding and finalization create independent field envelopes while
sharing read-only scalar payloads; output renaming and protocol projection cannot
mutate retained partials and do not deep-copy entire value arrays.
Execution metadata (including View/MVCC, feature bits and cost) remains independent
of the payload. Proxy alone calls `Finalize` to obtain `FinalAggregationResult`
before output remapping. Global empty input returns count=0 and NULL for
aggregates without candidates; avg remains a sum/count composition and returns
NULL when count is zero or sum is NULL, while other rows remain computable. These states
prevent empty shards from changing positive min or negative max, and preserve
NULL when all contributions are NULL. AllRetrieveCount is never an emptiness
criterion. There is no new protobuf field or production Proxy routing switch.
StatisticsOnView remains physical row counting and does not apply pruning.

## Validation

Cover statistics-only DataView publication, flush inheritance, compaction and
old/new View coexistence; shared load/refcount/retry/release/recovery; direct
object-path resolution; scalar predicates and conservative fallback; partition
wide vector selection, multiple vectors, different fields and missing stats;
per-worker and per-subquery dispatch, all-empty plans and retained requery;
actual FeatureSegmentPrune accounting. Run the clustering/pruning integration
case with the latest binary, bounded to ten minutes per IT case.
