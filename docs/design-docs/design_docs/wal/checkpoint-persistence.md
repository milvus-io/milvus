# Checkpoint And Snapshot Persistence

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

This document defines the single global checkpoint and the catalog publication
protocol. Message lifetime is defined in
[WAL Message Ack Design](message_ack.md).

## 1. Global Checkpoint Model

```go
type WALCheckpoint struct {
    MessageID message.MessageID
    TimeTick  uint64
    Magic     int64  // recovery format marker (hint)
    Term      int64  // owning publisher term (fencing metadata)
}
```

The checkpoint is the largest published continuous prefix. Internally, the
Tracker also has a completed point that may be newer than the published point:

```text
candidate = Tracker.CheckpointThrough(WALSummary.LastAcked())
published checkpoint <= candidate <= observed WAL tail
```

Only the published point survives a crash and only it may be used for replay or
truncation.

## 2. Component Snapshot Model

Every independently persisted recovery component includes:

```text
checkpoint_time_tick
Payload
Tombstone or cleanup state, when applicable
```

`checkpoint_time_tick` means that the snapshot contains the complete durable
effect of every message relevant to that component through the value. A
component may additionally keep later completed work in memory, but it cannot
publish a component checkpoint with a gap.

The following state is represented as component snapshots:

- VChannel collection, partition, schema, and lifecycle state;
- Segment assignment, object references, row statistics, and lifecycle state;
- the VChannel [L0Materializer](l0_materializer.md) frontier, using the existing
  `VChannelMeta.transform_materialized_time_tick` field;
- salvage and cleanup metadata that must precede checkpoint publication.

The PChannel replication and AlterWAL control state is logically a component
snapshot, but is physically embedded in `WALCheckpoint` rather than stored under
a separate catalog key. Its latest recoverable state may be ahead of the global
replay position, just like a Segment snapshot (see [§7](#7-pchannel-control-state)).

Transform records themselves are not a component snapshot: their durability is
owned by the pchannel-scoped WALSummary (chunk + manifest on object storage,
term-scoped object keys). The summary copies records without retaining message handles. RecoveryStorage
selects a complete tracked WAL position at or below the logical
`WALSummary.LastAcked()` TimeTick before publishing, so the global checkpoint cannot pass the recoverable summary prefix. See
[WALSummary Design](summary.md).

The persisted component fields use one uniform name:

| Snapshot | Component checkpoint field |
|---|---|
| `VChannelMeta` | `checkpoint_time_tick` (+ `transform_materialized_time_tick` for the L0 frontier) |
| `SegmentAssignmentMeta` | `checkpoint_time_tick` |

There is no `applied_through_time_tick`, `data_checkpoint_time_tick`, persisted
`sync_up_time_tick`, or persisted last-Delete frontier. Metadata and Segment components use the
same replay rule:

```text
message.TimeTick <= component.checkpoint_time_tick -> no-op
message.TimeTick >  component.checkpoint_time_tick -> apply complete effect
```

L0Materializer's materialized cursor is independent of VChannel metadata's
`checkpoint_time_tick`. Its requested window is runtime-only and reconstructed
through replay. After either a full or base-only VChannel snapshot is durable,
its captured materialized cursor may be reported to Summary for retention;
in-memory completion alone does not release stored Delete history. Explicit
Flush/lifecycle requests retain their WAL handles until L0 completion and dirty
M installation, so unfinished requests remain replayable without a persisted
request field. The publisher must save M before publishing past the request.

## 3. Why Component Checkpoints Are Required

Suppose M1 affects Segment A and is blocked, while M2 affects Segment B and
finishes:

```text
global completed prefix: before M1
Segment A checkpoint: before M1
Segment B checkpoint: M2
```

A persist batch may publish Segment B's snapshot while the global checkpoint
remains before M1. After a crash, replay starts before M1:

- Segment A applies M1;
- Segment B skips M2 because its snapshot already covers it.

Without Segment B's checkpoint, the system would either apply M2 twice or need
a global versioned snapshot history. The component checkpoint is the smaller
mechanism; it does not create another recovery cursor.

## 4. Freeze And Publish Protocol

The publisher executes:

```text
candidate = Tracker.CheckpointThrough(WALSummary.LastAcked())
freeze candidate
  -> consume stable dirty component snapshots
  -> save all component deltas
  -> save candidate as the global checkpoint last
  -> MarkPersisted on the exact consumed snapshots
  -> truncate WAL through candidate.MessageID
```

An asynchronous component must perform operations in this order:

```text
required object/lifecycle work succeeds
  -> install resulting recovery metadata
  -> advance the continuous component checkpoint when possible
  -> mark component dirty
  -> release retained message handle
```

This ordering guarantees that every message included in `candidate` has a dirty
snapshot available when the publisher freezes the batch.

## 5. Catalog Transaction Boundary

When all operations fit in one etcd transaction, component deltas and the
checkpoint are committed atomically.

When the operation count exceeds the transaction limit, catalog may write
component deltas in chunks, but it must write the checkpoint only after all
component chunks succeed.

Crash behavior:

| Crash point | Recovery behavior |
|---|---|
| Before object write | Old checkpoint replays the message. |
| After object write, before component snapshot | Replay may leave an orphan object, but does not skip data. |
| After component snapshot, before checkpoint | Replay starts old; component checkpoint makes covered work a no-op. |
| After checkpoint commit | All required component state is already visible. |
| During chunked catalog fallback | Published component checkpoints make partial progress replay-safe. |

The protocol provides logical exactly-once recovery state, not physical
exactly-once object creation. Orphan object collection belongs to GC/Defrag.

### 5.1 Checkpoint publication is fenced by term

Recovery claims the consume checkpoint with the assignment term before probing
summary storage. Every later checkpoint carries that term. Catalog publication
rejects an older publisher, uses a value CAS on an existing checkpoint, and a
version-zero CAS for first creation. Read-back verification detects a lost CAS.
The checkpoint remains the final commit marker after component writes.

This fences checkpoint publication. Component writes made by the chunked
transaction fallback before its final CAS are not individually fenced; complete
cross-owner component fencing remains separate work.

## 6. Dirty Snapshot Stability

`ConsumeDirtySnapshots` returns immutable clones plus exact `MarkPersisted`
callbacks. A component may continue observing later messages after consumption.
Calling `MarkPersisted` must advance only to the checkpoint captured in that
snapshot and must not clear newer dirty state.

A snapshot may be ahead of the frozen global candidate. This is safe because
its `checkpoint_time_tick` will suppress duplicate component effects during
replay.

## 7. PChannel Control State

Replication configuration, replication progress, and AlterWAL state are
embedded directly in `WALCheckpoint` (fields `replicate_config`,
`replicate_checkpoint`, `alter_wal_state`). They are **stored atomically with the
checkpoint**, but need not represent the same logical TimeTick. Control keeps
its latest state; it does not retain historical versions to match the global
replay position. A control-only change rewrites the checkpoint (the dirty check
compares the control fields). There is no separate catalog key for control.

```proto
message WALCheckpoint {
    common.MessageID message_id = 1;
    uint64 time_tick = 2;
    int64 recovery_magic = 3;
    common.ReplicateConfiguration replicate_config = 4;
    common.ReplicateCheckpoint replicate_checkpoint = 5;
    AlterWALState alter_wal_state = 6;
    int64 term = 7;
    uint64 control_checkpoint_time_tick = 8;
}
```

For example, an unfinished Insert can hold the global checkpoint at 100 while
Control already contains configuration B from 120. Persisting `100 + B` is
valid: restart restores B, replays the missing data effects, and must converge
to the latest Control through the recovery barrier. The write-path replication
manager is initialized from the snapshot after bounded replay finishes.

The required property is replay idempotency of the complete Control state and
its side effects, not equality with the global checkpoint's TimeTick. Segment
snapshots establish this using their own applied frontier. A Control-local
applied frontier is likewise component metadata, not a second global scanner
or truncation checkpoint. The latest state and its frontier are stored together
in `WALCheckpoint.control_checkpoint_time_tick`, allowing old control effects
to be skipped while data components still replay.
Already-covered AlterWAL events may still advance their persisted stage, after
the required data checkpoint has been published.

`ApplyControl`, proto conversion and cloning preserve the Control frontier.
Recovery initializes its applied boundary to the maximum of the saved Control
frontier and the global TimeTick: the global prefix is already covered even if
the last control event was older. Older checkpoints without the new field use
the global position as before; their missing historical Control frontier cannot
be inferred. A Control-only frontier advance beyond the global floor makes the
checkpoint dirty even when the control payload is unchanged.

The regression test covers a failure that occurred when this frontier was lost:

1. At local TT 100, this node is secondary with source progress 500.
2. At 110, topology changes while retaining the same source; progress stays 500.
3. At 120, force-promote makes it primary and captures salvage progress 500.
4. Persist latest Control with global position 100, then restart and replay.
5. Replaying 110 from the saved primary state creates secondary progress 0;
   replaying 120 captures salvage progress 0. Configuration B is correct, but
   the pending salvage write differs and can overwrite the saved boundary.

With the saved Control frontier at 120, replay skips both old control effects
and leaves the saved salvage boundary intact. A crash before metadata publication
instead restores the old secondary state and replays the transitions, producing
the same salvage boundary 500. New control events after 120 still apply.

This is not exactly-once external API execution. An API may succeed just before
a crash that loses the local metadata update, in which case recovery invokes it
again. Such APIs must tolerate retries or deduplicate a stable operation ID.
Control's applied frontier may be published only after its required effects are
recoverable. Derived salvage metadata is saved before or with the checkpoint;
marking an effect covered before its required work succeeds would lose that work.

## 8. Close

Correctness does not require a final persist during `Close`. The last published
checkpoint is always a valid recovery start and the remaining tail is replayed
on restart.

An optional bounded best-effort publish may reduce the next recovery time, but
it must not:

- fabricate completion;
- release unfinished handles;
- wait for work whose persistence condition is not yet satisfied;
- loop until mutable state appears stable.

## 9. Removed Design Elements

The implementation removes, rather than deprecates:

- Meta checkpoint and Data checkpoint fields;
- `GetDataCheckpoint` and branch-local aliases;
- checkpoint migration code added only for this feature branch;
- Meta/Data fields in dirty-snapshot coordination;
- any persist path that selects one of two recovery cursors.
