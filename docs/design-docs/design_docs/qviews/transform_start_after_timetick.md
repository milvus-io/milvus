# TODO: Transform Start-After TimeTick

Status: blocked on a StreamingNode shard-lifecycle refactor. The current
DataView branch does not add a DataView or SegmentInfo
`transform_start_after_timetick` field, does not carry the frontier in
`OnL0Compact`, and does not use it for TransformLog GC.

This document records the target protocol so the future StreamingNode PR can
introduce the capability without making Coordinator infer the latest Growing
Segment state.

## Required semantics

For a Segment `s`, `C(s) = T` will mean that every Transform event on the same
VChannel with `timetick <= T` has either:

1. been applied to the exact Segment contents referenced by SegmentInfo; or
2. been evaluated and proven irrelevant to that Segment.

QueryNode may consume TransformLog entries strictly after `T`. The value is a
continuous-prefix proof, not the timestamp of the last observed Delete. It
must not be inferred from Manifest version, maximum deltalog timestamp, or an
L0 scheduling position.

The future SegmentInfo field is expected to be equivalent to:

```proto
// TODO: Add after StreamingNode implements the shard Flush barrier.
// Every Transform through this TimeTick has been applied to this Segment or
// proven irrelevant. Zero means unknown coverage.
uint64 transform_start_after_timetick = <new-field-number>;
```

For a normal Streaming Flush Segment, its initial value will be the exclusive
Insert start boundary:

```text
C(flushSegment) = flushSegment.start_position.Timestamp
```

The start-position contract must guarantee that every stored row has an Insert
timestamp strictly greater than the boundary.

DataView will publish only one frontier per shard. For frontier `F`, every
Segment in that shard's DataView must satisfy:

```text
C(segment) >= F
```

Across DataViews, `F` must never decrease. Each effective L0 TransformVersion
must strictly increase it.

## Why the capability is deferred

Coordinator observes asynchronously projected SegmentMeta. It cannot prove
that it has already seen every Growing Segment on the shard, so it cannot
safely decide that an L0 completion may advance the shard frontier.

The current L0 path is also partition-oriented. Publishing a partition task's
position as a shard frontier can miss an older Growing Segment in another
partition. For this reason, the current branch retains TransformVersion only
for Manifest-version updates and omits the frontier entirely.

StreamingNode owns the shard WAL order, Growing Segment lifecycle, and
Transform input. The safety barrier therefore belongs on StreamingNode.

## Future shard-level barrier

Assume the current DataView shard frontier is `F`. To publish a higher
frontier `T`, StreamingNode must execute:

```text
1. Establish a shard WAL barrier T, where T > F.
2. Rotate every current Growing Segment on the shard.
3. Route post-barrier Inserts into new Growing Segments.
4. Flush every rotated Segment.
5. Wait for all corresponding OnFlush events to commit to DataView.
6. Materialize the complete shard's L0 prefix through T.
7. Persist Manifest/deltalog changes and Segment coverage proofs.
8. Publish one coalesced OnL0Compact event with shard frontier T.
```

The design intentionally does not select Growing Segments by partition or
primary key. Rotation is shard-wide. Once rotation completes, new writes may
continue while the old Segments Flush asynchronously.

Step 5 is the publication fence: L0 frontier `T` must not become visible before
every pre-barrier Growing Segment has entered DataView. Coordinator consumes
the acknowledged event order and does not scan SegmentMeta to determine
whether its view is current.

The certified L0 materialization evaluates every loadable Segment in the
shard. A Segment with no matching Delete still receives a coverage proof to
`T`, because the Transform prefix was evaluated and proven irrelevant.

Raw TransformLog-to-L0 staging may happen earlier. It does not advance Segment
coverage, DataView frontier, or TransformLog GC until the shard protocol above
completes.

## Target invariants

After every successful L0 publication:

```text
all DataView Segment coverage >= F
all current Growing Segment start boundaries > F
```

An ordinary later Flush therefore joins with:

```text
C(newSegment) = newSegment.start_position.Timestamp >= F
```

and cannot force the DataView frontier backward.

The resulting DataVersion behavior is:

| Event | Frontier | DataVersion |
|---|---|---|
| Barrier-induced Flush | remains `F` | StreamingVersion increases |
| Completed shard L0 materialization | `F -> T`, `T > F` | TransformVersion increases |
| Ordinary post-barrier Flush | remains `F` | StreamingVersion increases |

Partial Segment L0 work may commit a newer Manifest in SegmentMeta before the
whole shard reaches `T`. It must not publish a TransformVersion with an
unchanged frontier merely to expose that Manifest. The future producer will
coalesce the revisions and publish them with the next strictly higher shard
frontier. An older DataView stays correct by retaining its old Manifest and
consuming the corresponding TransformLog suffix.

## Target Segment update rules

| Operation | Future coverage rule |
|---|---|
| Legacy or unknown Segment | `0` |
| Streaming Flush | `start_position.Timestamp` |
| L0, Delete matched | `max(oldC, T)` |
| L0, no Delete matched | `max(oldC, T)` after an explicit zero-match proof |
| Segment not evaluated by L0 | keep `oldC`; frontier `T` cannot be published |
| Sort Compaction | inherit input coverage |
| Multi-input Compaction | minimum input coverage |
| Split | every output inherits source coverage |
| Exact Manifest copy | inherit source coverage |
| Index-only or metadata-only update | keep `oldC` |

For the same Segment ID, coverage may only increase. A replacement or imported
Segment may join DataView only when its coverage is at least the current shard
frontier.

## Failure and recovery requirements

The future StreamingNode implementation needs an idempotent state machine:

```text
Open
  -> BarrierEstablished(T)
  -> GrowingSegmentsRotated
  -> FlushDataDurable
  -> FlushDataViewCommitted
  -> L0Materialized
  -> TransformDataViewCommitted(T)
  -> Open
```

- Failure before the Flush DataView commit prevents L0 frontier publication.
- Failure after Flush commit leaves a valid DataView at the old frontier.
- Failure after SegmentMeta/Manifest update but before DataView publication
  leaves the new materialization unpublished and safe to retry.
- Only the final DataView commit makes `T` eligible for TransformLog GC.

Every transition must be recoverable from WAL or durable task state. Replayed
Flush and L0 events must be idempotent.

## Follow-up implementation checklist

The StreamingNode refactor PR must provide all of the following before the TODO
can be removed:

- a shard-wide Growing Segment rotation barrier;
- durable and replayable barrier progress;
- acknowledged ordering from all barrier Flush commits to L0 publication;
- complete-shard L0 evaluation, including zero-match proofs;
- SegmentInfo coverage persisted with the corresponding Manifest/deltalogs;
- a shard frontier carried by DataView and QueryView;
- DataViewManager validation that the frontier never regresses;
- QueryViewRef-based TransformLog retention and GC integration; and
- recovery tests covering failures at every barrier phase.

Until those prerequisites exist, `transform_start_after_timetick` remains a
design TODO and must not be approximated with Coordinator-observed SegmentMeta
or a partition L0 task position.
