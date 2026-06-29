# MEP: Compaction target base slice

Current state: In Progress

ISSUE:

- [[Enhancement]: Support reason based compaction trigger #49991](https://github.com/milvus-io/milvus/issues/49991)
- [[Sub-task] Add compaction target foundation #50057](https://github.com/milvus-io/milvus/issues/50057)

Keywords: DataCoord, Compaction, Target

Released: N/A

## Summary

This slice adds the first target-based compaction path behind the default-off
`dataCoord.compaction.enableTargetBasedCompaction` guard.

When the guard is enabled, a plain manual compaction request records one finite
`REWRITE` target and returns the target id in the existing `compactionID`
response field. DataCoord then drains that target through the v2
`CompactionTriggerManager` loop and `compactionTargetReconciler` until no
in-scope segment still matches the rewrite predicate.

Guard disabled behavior remains the existing manual compaction flow.

## Design

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
channel are not stored on the target; the Reconciler derives the compaction group
label from live segment facts.

### Select and Execute

The v2 trigger manager registers a guarded target reconciler. On each tick the
reconciler reloads current segment facts from DataCoord meta and evaluates the
shared finite rewrite predicate:

```text
ScopeIn(target, segment)
&& segment.create_ts < target.expectedTS
&& segment.data_ts < target.expectedTS
```

Legacy segments with `create_ts = 0` match the create timestamp gate. New data
with `data_ts >= expectedTS` does not match, so ongoing ingest does not keep a
finite target open forever.

The reconciler uses `isNormalManualCompactionCandidate` only as an enqueue gate. A
matched segment that is transiently ineligible remains matched and can be
retried on a later tick. A matched segment freshened by another compaction
inherits a newer `create_ts` and self-excludes.

### Complete and Recover

For this base slice, a target is satisfied when no in-scope segment matches the
same predicate. The reconciler marks satisfied records `TARGET_STATE_INACTIVE`.

No progress cursor is stored. Restart recovery comes from the durable target
record plus durable segment `create_ts` values, then the v2 reconciler re-derives
matches from current meta.

### Create Timestamp

Compaction task `create_ts` is minted from the allocator timestamp source, not
the local wall clock. Replacement segments inherit the producing task
`create_ts`.

## Out Of Scope

- Target-aware status, drop, and retention.
- Standing target reconciliation.
- `OPTIMIZE`, `SIZE`, `SORT`, and `BACKFILL` target intents.
- Tail tolerance with `tail_limit >= 1`.
- Retiring the v1 compaction trigger path.

## Test Plan

- Guard disabled manual compaction keeps current behavior.
- Guard enabled manual compaction records an active rewrite target and returns
  immediately.
- Predicate coverage includes legacy `create_ts = 0`, newer `data_ts`, and
  self-exclusion after freshening.
- Target reconciler enqueues only currently eligible manual candidates and marks
  the target `TARGET_STATE_INACTIVE` when no match remains.
- Reload coverage verifies active records resume from persisted meta.
- Build, DataCoord tests, metastore tests, static checks, and generated proto
  hygiene pass.
