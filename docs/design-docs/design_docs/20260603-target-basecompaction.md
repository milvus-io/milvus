# MEP: Compaction target base slice

- **Created:** 2026-06-03
- **Author(s):** @XuanYang-cn
- **Status:** In Progress
- **Component:** Coordinator
- **Related Issues:** #49991, #50057
- **Released:** N/A

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
