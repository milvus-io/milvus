# MEP: Compaction reason record base slice

Current state: In Progress

ISSUE:

- [[Enhancement]: Support reason based compaction trigger #49991](https://github.com/milvus-io/milvus/issues/49991)
- [[Sub-task] Add compaction reason record foundation #50057](https://github.com/milvus-io/milvus/issues/50057)

Keywords: DataCoord, Compaction, Reason Record

Released: N/A

## Summary

This slice adds the first reason-driven compaction path behind the default-off
`dataCoord.compaction.reasonRecord.enabled` guard.

When the guard is enabled, a plain manual compaction request records one finite
`REWRITE` reason and returns the reason id in the existing `compactionID`
response field. DataCoord then drains that record through the v2 compaction
trigger loop until no in-scope segment still matches the rewrite predicate.

Guard disabled behavior remains the existing manual compaction flow.

## Design

### Record

The guarded manual path persists a `CompactionReasonRecord` with:

- `reason_type = REASON_INTENT_REWRITE`
- `tail_limit = 0`
- `state = REASON_STATE_ACTIVE`
- `createdAtTS = expectedTS`
- scope copied from the request collection, partition, channel, and segment ids

`createdAtTS` and `expectedTS` use one allocator timestamp. The API schema does
not change; the reason id rides the existing `compactionID` field.

### Select and Execute

The v2 trigger manager registers a guarded reason policy. On each tick the
policy reloads current segment facts from DataCoord meta and evaluates the
shared finite rewrite predicate:

```text
ScopeIn(reason, segment)
&& segment.create_ts < reason.expectedTS
&& segment.data_ts < reason.expectedTS
```

Legacy segments with `create_ts = 0` match the create timestamp gate. New data
with `data_ts >= expectedTS` does not match, so ongoing ingest does not keep a
finite reason open forever.

The policy uses `isNormalManualCompactionCandidate` only as an enqueue gate. A
matched segment that is transiently ineligible remains matched and can be
retried on a later tick. A matched segment freshened by another compaction
inherits a newer `create_ts` and self-excludes.

### Complete and Recover

For this base slice, a reason is satisfied when no in-scope segment matches the
same predicate. The policy marks satisfied records `DONE`.

No progress cursor is stored. Restart recovery comes from the durable reason
record plus durable segment `create_ts` values, then the v2 policy re-derives
matches from current meta.

### Create Timestamp

Compaction task `create_ts` is minted from the allocator timestamp source, not
the local wall clock. Replacement segments inherit the producing task
`create_ts`.

## Out Of Scope

- Reason-aware status, drop, and retention.
- Standing reason reconciliation.
- `OPTIMIZE`, `SIZE`, `SORT`, and `BACKFILL` reason types.
- Tail tolerance with `tail_limit >= 1`.
- Retiring the v1 compaction trigger path.

## Test Plan

- Guard disabled manual compaction keeps current behavior.
- Guard enabled manual compaction records an active rewrite reason and returns
  immediately.
- Predicate coverage includes legacy `create_ts = 0`, newer `data_ts`, and
  self-exclusion after freshening.
- Reason policy enqueues only currently eligible manual candidates and marks
  the reason `DONE` when no match remains.
- Reload coverage verifies active records resume from persisted meta.
- Build, DataCoord tests, metastore tests, static checks, and generated proto
  hygiene pass.
