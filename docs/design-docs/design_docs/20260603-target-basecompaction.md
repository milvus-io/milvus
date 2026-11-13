# MEP: Compaction target base slice

- **Created:** 2026-06-03
- **Author(s):** @XuanYang-cn
- **Status:** In Progress
- **Component:** Coordinator
- **Related Issues:** #49991, #50057
- **Released:** N/A

## Dedicated Target API

The dedicated API gives a compaction request a durable resource ID while it
waits for data or execution capacity. A Target can create several physical
tasks over its lifetime. Each task has its own identity and is recovered by
the compaction task scheduler. The original Base design is recorded below.

### Create, observe, and stop

`CreateCompactionTarget`, `DescribeCompactionTarget`, `ListCompactionTargets`,
and `DropCompactionTarget` are available over gRPC and under
`/v2/vectordb/compaction_targets` over HTTP. Create resolves the collection
through RootCoord and authorizes `PrivilegeCompaction` on that resource.
DataCoord revalidates the stable collection ID and metadata revision before
committing the Target. Describe and List resolve current collection names
from the stored ID. Cluster administrators can inspect history for a deleted
collection and list cluster-wide Standing Targets with `includeSystem`.

Create fixes a timestamp `T` and returns the Target ID and its initial matched
row count. An uncertain persistence result is confirmed by reading the same
candidate ID. If that read cannot establish the outcome, Create reports an
unknown outcome. Proxy forwarding and SDK Create calls dispatch at most one
application request. The operator can use List to inspect recent Targets
before deciding whether to submit a new request.

An active Target can report a fresh observation through Describe. A successful
observation can report zero remaining rows while a terminal write is still
pending. List returns persisted definitions and lifecycle fields. The first
successful terminal write fixes the stop reason and timestamp. Repeating Drop
preserves this result while the record exists.

### Manual REWRITE

The current scope is healthy, Flushed data in the collection outside L0 and
L2, excluding imports that have not committed. A segment's effective data
start must be strictly before `T`. This uses the import commit timestamp,
segment statistics, and start position. Missing timestamp facts are included
conservatively. If the caller supplies segment IDs, the Target stores those
lineage roots and follows the current descendants recorded by compaction.

REWRITE matches in-scope segments whose rewrite timestamp is zero or earlier
than `T`. A full Mix, Sort, Clustering, or Schema replacement publishes its
task's admission timestamp. Same-ID schema updates retain the existing
timestamp. Snapshot protection, occupancy, and index readiness can defer work
while the segment remains part of the observed match set.

Before saving satisfaction, DataCoord rechecks the collection snapshot and
relevant in-flight inputs while serializing new task admission. It also takes
shared locks on the collection's DDL resources. DropCollection holds the
exclusive collection lock through its acknowledgment, so early channel
cleanup cannot turn pending work into a false `satisfied` outcome. The control
channel callback persists the collection tombstone, then ends active Targets
for that stable ID with `scope_dropped`. Startup recovery checks retained
active Targets against collection metadata. The configurable
`dataCoord.compaction.target.retentionSeconds` window defaults to seven days.
After the Target record is collected, Describe and Drop return TargetNotFound.

### Manual OPTIMIZE and Standing SIZE

Create takes a positive byte size for OPTIMIZE. It computes one actual size
using the existing Force Merge sizing rules and persists both values. Each
physical group uses that size, including groups that first flush while the
Target is active. A group is satisfied when it has no segments above the 5%
tolerance and at most one small tail. DataCoord rechecks published segment
sizes and related in-flight work before recording satisfaction.

An active OPTIMIZE excludes new SIZE packages with overlapping inputs and
overlapping Clustering work. The final check uses current segment facts under
the collection admission lock. A collection has at most one active OPTIMIZE.
Its active slot and Target record are written atomically, so a delayed write
from a previous coordinator cannot admit a second Target.

Standing SIZE keeps one cluster-wide ID. Describe, List, and reconciliation
derive its settings from the current configuration. Reconciliation uses
TwoTier with the existing normal and disk size profiles. Eligible segments
with an independent rewrite requirement can share a size package. Any
remaining independent rewrites can run individually.

### Standing SORT and task admission

Standing SORT keeps one cluster-wide ID and persists its enabled condition.
It checks ordinary flushed data for the published sorted state and can plan
Sort work while global auto compaction is disabled. Import owns its sorting
stage. When a segment needs both SORT and REWRITE, a sufficiently recent
published Sort replacement can satisfy the REWRITE condition.

Reconciliation plans each complete input group, then selects full plans
across collections, groups, and compatible work. In both startup modes, the
Inspector uses one token bucket for new physical tasks from legacy compaction,
TBC, Import, and Restore. A failed pre-queue submission returns its token.
Recovery and requeue of previously accepted tasks use the original admission.
`dataCoord.compaction.rateLimitTokens` and `dataCoord.compaction.rateLimitInterval`
configure the shared bucket. The legacy `target.maxEventsPerReconcile` setting
is deprecated.

### Compatibility and verification

`dataCoord.compaction.enableTargetBasedCompaction` is a default-off startup
setting. With it enabled, ordinary manual compaction and target-size requests
use the dedicated Target API. Legacy L0 and Clustering still create physical
tasks, and their existing task-state RPCs remain available. In legacy mode,
ordinary manual compaction uses its task path. Stored Target history becomes
accessible again when the setting is re-enabled.

Verification exercises the four operations through Go SDK and REST, including
the published replacement's lineage, snapshot protection and resumption, and
collection deletion while a Target is active. Coordinator tests cover
ambiguous writes, stale collection identity, late segment publication, and
contention among task producers.

## Original Base Slice

## Summary

This slice adds the first target-based compaction path behind the default-off
`dataCoord.compaction.enableTargetBasedCompaction` guard.

When the guard is enabled, a plain manual compaction request records one finite
`REWRITE` target and returns the target id in the existing `compactionID`
response field. DataCoord then drains that target through the v2
`CompactionTriggerManager` loop and `compactionTargetReconciler` until no
in-domain segment still matches the rewrite predicate. A request for an
external collection remains unsupported. A request received while the
collection has a pending snapshot or an unloaded protected snapshot RefIndex
returns `ErrCompactionBlocked` without allocating or persisting a target.

Guard disabled behavior remains the existing manual compaction flow.

## Motivation

The Base path needs a durable request-level model that survives restart, while
operators also need a bounded amount of physical work from each reconciliation
tick. The event bound must not freeze semantic convergence or keep an already
satisfied target active.

## Public Interfaces

This slice adds the refreshable
`dataCoord.compaction.target.maxEventsPerReconcile` configuration. It defaults
to `100` and limits the total number of compaction views returned by one Target
Reconciler call. A non-positive or malformed value is invalid and falls back to
the default.

No API, proto, SDK, metric, or persisted-record shape changes are introduced by
the event limit.

## Design Details

### Record

The guarded manual path persists a `CompactionTarget` with:

- `intent = INTENT_REWRITE`
- `tail_limit = 0`
- `state = TARGET_STATE_ACTIVE`
- `activatedAtTS = expectedTS`
- top-level `collectionID` copied from the request collection
- optional request segment ids encoded in `properties["segment_ids"]`

`activatedAtTS` and `expectedTS` use one allocator timestamp. The API schema does
not change; the target id rides the existing `compactionID` field. Partition and
channel are not stored on the target; guarded requests carrying either filter
are rejected instead of silently widening their scope. The Reconciler derives
the compaction group label from live segment facts.

### Select and Execute

The v2 trigger manager registers a guarded target reconciler. Reconciliation is
target-first: each active target selects one complete semantic match set, asks
the target whether that set is satisfied, and only then filters the matches for
physical execution.

The Base `REWRITE` target produces MixCompaction views and emits them through
the existing `TriggerTypeSingle` dispatch path. This slice does not add a
target-specific trigger type or change the Target interface; later target
intents can define their dispatch mapping when they are introduced.

Each reconciliation reads the event limit once. The limit bounds the total
number of compaction views returned across all active targets. Reaching the
limit suppresses only additional physical work: the reconciler still evaluates
every target's complete match set and inactivates satisfied targets. Target
ordering and priority remain unchanged in this slice.

The Base manual `REWRITE` universe contains healthy `Flushed` segments that are
not importing, are neither L0 nor L2, and are not precisely protected by a
loaded snapshot RefIndex. Known external collections are outside the universe,
including for a cluster-wide target. Within that universe the finite rewrite
predicate is:

```text
ScopeIn(target, segment)
&& segment.create_ts < target.expectedTS
&& segment.data_ts <= target.expectedTS
```

Legacy segments with `create_ts = 0` match the create timestamp gate. New data
with `data_ts > expectedTS` does not match, so ongoing ingest does not keep a
finite target open forever.

`isCompacting`, invisibility, and unsortedness are temporary execution blockers.
They do not alter the match set or satisfaction result. When index-based
compaction is enabled, execution uses the exact existing manual-path call:

```text
FilterInIndexedSegments(ctx, handler, meta, true, candidates...)
```

This preserves the current enabled/disabled, ready/unready, and no-index
collection behavior. The existing `isNormalManualCompactionCandidate` helper is
unchanged and remains the final Segment execution predicate.

A collection-wide snapshot block is also an execution gate, not a semantic
Segment filter. If the block appears after target creation, matching work stays
pending and resumes after the block clears. For a cluster-wide target, only the
blocked collection pauses. Compaction admission and completion both revalidate
the collection block and precise per-Segment snapshot protection to close races
after planning.

### Complete and Recover

For this base slice, a target is satisfied when no in-domain Segment matches the
same predicate. A temporary execution blocker never makes a target satisfied.
The reconciler marks satisfied records `TARGET_STATE_INACTIVE` after the
target-first semantic sweep.

No progress cursor is stored. Restart recovery comes from the durable target
record plus durable segment `create_ts` values, then the v2 reconciler re-derives
matches from current meta.

### Create Timestamp

Compaction task `create_ts` is minted from the allocator timestamp source, not
the local wall clock. Replacement segments inherit the producing task
`create_ts`.

## Compatibility, Deprecation, and Migration Plan

The Target path remains behind the default-off
`dataCoord.compaction.enableTargetBasedCompaction` guard. When the path is
enabled, the new event limit defaults to `100`; a dynamic update applies to the
next reconciliation call. Older binaries ignore the new key. The change adds no
persisted state and requires no migration or rollback cleanup.

## Out Of Scope

- Target-aware status, drop, and retention.
- Standing target reconciliation.
- `OPTIMIZE`, `SIZE`, `SORT`, and `BACKFILL` target intents.
- Tail tolerance with `tail_limit >= 1`.
- Retiring the v1 compaction trigger path.
- Persisted or structured execution-blocker causes, including derived invisible
  publication causes.
- Durable target source metadata and explicit satisfaction-cause logs.
- Changing the existing retirement-update error contract.
- Defining target priority, fairness, or a time-based dispatch rate limiter.

## Test Plan

- Guard disabled manual compaction keeps current behavior.
- Guard enabled manual compaction records an active rewrite target and returns
  immediately.
- Guard enabled manual compaction rejects partition and channel filters before
  allocating or persisting a target.
- Predicate coverage includes legacy `create_ts = 0`, newer `data_ts`, and
  self-exclusion after freshening.
- Candidate-stage coverage proves non-Flushed, importing, L0/L2, and precisely
  snapshot-protected Segments are outside the manual match domain, while
  compacting, invisible, unsorted, and index-rejected matches keep the target
  active without emitting work.
- Target-first coverage proves satisfaction uses the complete match set before
  execution filtering and marks the target `TARGET_STATE_INACTIVE` only when no
  match remains.
- Event-limit coverage proves one reconciliation emits no more than the dynamic
  configured maximum while continuing satisfaction checks, and invalid limits
  fall back to `100`.
- Snapshot coverage proves pre-existing blocks reject the manual request,
  post-creation blocks pause and resume work, cluster-wide targets continue on
  unblocked collections, and task admission rejects stale planned work.
- External-collection coverage proves global targets do not match or emit work
  for external collections.
- Index coverage proves parity for index filtering enabled and disabled,
  finished and unready indexes, and collections with no index.
- Reload coverage verifies active records resume from persisted meta.
- Build, DataCoord tests, metastore tests, static checks, and generated proto
  hygiene pass.

## Rejected Alternatives

- Caching the limit in the reconciler or registering a dedicated watcher was
  rejected because reading the refreshable parameter once per reconciliation
  is sufficient.
- Stopping the Target loop after the budget is exhausted was rejected because
  it would skip satisfaction checks for later Targets.
- Defining deterministic ordering, fairness, or priority was deferred to the
  later scheduling slice.

## References

- [Target-based compaction parent issue #49991](https://github.com/milvus-io/milvus/issues/49991)
- [Compaction target foundation issue #50057](https://github.com/milvus-io/milvus/issues/50057)
