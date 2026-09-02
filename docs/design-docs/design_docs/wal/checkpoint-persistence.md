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
published checkpoint <= completed point <= observed WAL tail
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
- the VChannel transform materialization frontier
  (`VChannelMeta.transform_materialized_time_tick`);
- salvage and cleanup metadata that must precede checkpoint publication.

The PChannel replication and AlterWAL control state is **not** a component
snapshot: it is embedded in `WALCheckpoint` itself and advances atomically with
it (see [§7](#7-pchannel-control-state)).

Transform records themselves are not a component snapshot: their durability is
owned by the pchannel-scoped WALSummary (chunk + manifest on object storage,
term-scoped object keys). The summary releases message handles only after a
chunk and its manifest are durable, so the global checkpoint can never pass a
transform record the summary has not persisted. See
[WALSummary Design](summary.md).

The persisted component fields use one uniform name:

| Snapshot | Component checkpoint field |
|---|---|
| `VChannelMeta` | `checkpoint_time_tick` (+ `transform_materialized_time_tick` for the L0 frontier) |
| `SegmentAssignmentMeta` | `checkpoint_time_tick` |

There is no `applied_through_time_tick`, `data_checkpoint_time_tick`, persisted
`sync_up_time_tick`, or persisted last-Delete frontier. Component code uses the
same replay rule:

```text
message.TimeTick <= component.checkpoint_time_tick -> no-op
message.TimeTick >  component.checkpoint_time_tick -> apply complete effect
```

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
candidate = Tracker.CompletedPoint()
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

### 5.1 Checkpoint advancement is NOT fenced (TODO)

The consume checkpoint is the **commit point** of the snapshot: it is staged
with `CommitSave` so it is the last write to become visible, after every other
part of the snapshot has landed. It is currently a plain last-write commit
marker, **not** a compare-and-swap.

TODO(#52542 follow-up): concurrent publishers are not fenced. A superseded
publisher of an older term that survived a takeover can still overwrite both
the consume checkpoint and the component state of the current publisher, and
WAL truncation then follows the last writer's checkpoint. The intended design —
fence the whole commit on the recorded term (`WALCheckpoint.term`) via a
guarded commit: an atomic create-if-absent for the first write, an atomically
checked value equality on the serialized checkpoint afterwards (etcd cannot
compare proto fields), followed by a read-back verification because the etcd
txn reports success even when the guard fails — is deliberately not implemented
in this PR: it requires backend compare-and-swap support that TiKV does not
provide yet (`CompareVersionAndSwap` is a stub returning
`ErrServiceUnimplemented`).

Risk: two concurrent RecoveryStorage instances writing the same pchannel can
interleave component state and checkpoint from different terms. Do not rely on
takeover racing without landing the fenced commit.

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
`replicate_checkpoint`, `alter_wal_state`). They advance **atomically with the
checkpoint**: the checkpoint is the single source of truth for the control
state after a crash, and a control-only change rewrites the checkpoint (the
dirty check compares the control fields). There is no separate catalog key for
the control state.

```proto
message WALCheckpoint {
    common.MessageID message_id = 1;
    uint64 time_tick = 2;
    int64 recovery_magic = 3;
    common.ReplicateConfiguration replicate_config = 4;
    common.ReplicateCheckpoint replicate_checkpoint = 5;
    AlterWALState alter_wal_state = 6;
    int64 term = 7;
}
```

This keeps a single metadata point: the durable control state has no
independent `checkpoint_time_tick` — its frontier is the checkpoint position.
The in-memory control state is still tracked separately for deduplication and
stage transitions (AlterWAL FLUSHING → ADVANCE_CHECKPOINT), and recovery
decodes it from the checkpoint, then replays control messages after it.

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
