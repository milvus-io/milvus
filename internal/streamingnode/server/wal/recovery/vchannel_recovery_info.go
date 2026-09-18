package recovery

import (
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// newVChannelRecoveryInfoFromCreateCollectionMessage creates a new vchannel recovery info from a create collection message.
func newVChannelRecoveryInfoFromVChannelMeta(meta []*streamingpb.VChannelMeta) map[string]*vchannelRecoveryInfo {
	infos := make(map[string]*vchannelRecoveryInfo, len(meta))
	for _, m := range meta {
		infos[m.Vchannel] = &vchannelRecoveryInfo{
			meta:  m,
			dirty: false, // recover from persisted info, so it is not dirty.
			// Reloaded, not observed: the seal record's own tick is not in the
			// meta, so the gate is reseeded with the same function the flusher
			// reseeds its close gate with (see SplitFenceGate).
			splitFenceGate: SplitFenceGate(m),
		}
	}
	return infos
}

// SplitFenceGate is the fence gate of a SPLITTED source vchannel as reseeded
// from its persisted meta, 0 for any other vchannel: the tick the flusher's
// close gate of the source's data sync service is reseeded to on restart, and
// so the tick the source's flusher checkpoint must reach before the source
// counts as drained (see vchannelRecoveryInfo.DrainedPastFence).
//
// The gate is the own tick of the seal record the data sync service consumed,
// which the meta does not name directly: split_time_tick is T_switch, which
// can be earlier than the first fence record in the WAL (a re-drive after a
// failed first append), while checkpoint_time_tick was set to that record's
// own tick when the recovery storage observed it and only moves forward
// afterwards. The larger of the two is therefore never before the seal
// record; and a later gate only keeps a draining service open a little
// longer, since time ticks keep advancing its checkpoint.
//
// The flusher reseeds its close gate with this same function
// (fencedTicksFromSnapshot), but not necessarily from the same meta: on a
// restart the recovery storage reseeds from the catalog meta before the
// replay, while the flusher reseeds from the post-replay snapshot, and a
// replayed source-side DDL record can bump CheckpointTimeTick in between. So
// the flusher's gate may be later than, never earlier than, the recovery
// storage's. That direction is the safe one: the recovery storage never waits
// for the source (DrainedPastFence) longer than the flusher keeps its service
// open, and the flusher's later gate is again only a slightly longer drain.
func SplitFenceGate(meta *streamingpb.VChannelMeta) uint64 {
	if meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		return 0
	}
	return max(meta.GetSplitTimeTick(), meta.GetCheckpointTimeTick())
}

// newVChannelRecoveryInfoFromCreateCollectionMessage creates a new vchannel recovery info from a create collection message.
func newVChannelRecoveryInfoFromCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) *vchannelRecoveryInfo {
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(msg.MustBody())
	return newVChannelRecoveryInfo(msg.VChannel(), msg.Header().CollectionId, msg.Header().PartitionIds, schema, msg.TimeTick())
}

// newVChannelRecoveryInfoFromSplitShardMessage creates a new vchannel
// recovery info from a shard split target vchannel's genesis replica. The
// genesis carries the same CreateCollection shape, so the same schema parser
// seeds the vchannel meta.
//
// It also records the genesis position (split_genesis_checkpoint): the
// position the flusher spawns the target's data sync service from when it
// consumes this replica. DataCoord has no position for the target until the
// SplitShard ack callback seeds one, and the flusher recovering the target
// after a restart in between needs this one instead.
func newVChannelRecoveryInfoFromSplitShardMessage(msg message.ImmutableSplitShardMessageV2) *vchannelRecoveryInfo {
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(msg.MustBody().GetGenesis())
	info := newVChannelRecoveryInfo(msg.VChannel(), msg.Header().CollectionId, msg.Header().PartitionIds, schema, msg.TimeTick())
	info.meta.SplitGenesisCheckpoint = &streamingpb.WALCheckpoint{
		MessageId: message.MustMarshalMessageID(msg.LastConfirmedMessageID()),
		TimeTick:  msg.TimeTick(),
	}
	return info
}

// newVChannelRecoveryInfo builds the seed recovery info of a vchannel genesis.
func newVChannelRecoveryInfo(vchannel string, collectionID int64, partitionIDs []int64, schema *schemapb.CollectionSchema, timetick uint64) *vchannelRecoveryInfo {
	partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(partitionIDs))
	for _, partitionId := range partitionIDs {
		partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{
			PartitionId: partitionId,
		})
	}
	return &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: vchannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: collectionID,
				Partitions:   partitions,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             schema,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: timetick,
					},
				},
			},
			CheckpointTimeTick: timetick,
		},
		// a new incoming vchannel genesis is always dirty until it is persisted.
		dirty: true,
	}
}

// vchannelRecoveryInfo is the recovery info for a vchannel.
type vchannelRecoveryInfo struct {
	meta              *streamingpb.VChannelMeta
	flusherCheckpoint *WALCheckpoint // update from the flusher.
	dirty             bool           // whether the vchannel recovery info is dirty.
	// splitFenceGate is the fence gate of a SPLITTED source: the tick its
	// flusher checkpoint must reach before the source counts as drained. It
	// mirrors the flusher's close gate of the source's data sync service
	// (flusherComponents.fenced): the own tick of the first fence record this
	// storage observed flipping the vchannel to SPLITTED, or SplitFenceGate of
	// the meta when the vchannel was reloaded already SPLITTED. In memory
	// only; 0 for a vchannel that is not SPLITTED (see fenceGate).
	splitFenceGate uint64
}

// IsActive returns true if the vchannel is active.
func (info *vchannelRecoveryInfo) IsActive() bool {
	return info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
}

// fenceGate returns the fence gate of a SPLITTED source (see splitFenceGate),
// 0 for any other vchannel. A SPLITTED info that never went through
// ObserveSplitShard or the reload takes the gate its meta reseeds.
func (info *vchannelRecoveryInfo) fenceGate() uint64 {
	if info.splitFenceGate != 0 {
		return info.splitFenceGate
	}
	return SplitFenceGate(info.meta)
}

// DrainedPastFence reports whether this vchannel is a SPLITTED source whose
// flusher checkpoint has reached its fence gate.
//
// That is the condition the flusher closes the source's data sync service on
// (flusherComponents.closeDrainedFencedSources), and it means two things at
// once: every row the source will ever hold is persisted -- the checkpoint is
// the earliest unsynced position, and no DML lands after the fence -- and the
// service has consumed the seal record, so every segment of the source is
// sealed rather than left growing. Nothing will ever advance the checkpoint
// again once the service is closed, which is why a drained source must not be
// waited on by anything that needs the flusher to progress (the AlterWAL
// FLUSHING wait) and is what makes a retired source collectable.
//
// A checkpoint past T_switch but before the gate is NOT drained: on a
// re-driven fence the seal record is later than T_switch, and until the
// service consumes it the source's segments are still growing.
func (info *vchannelRecoveryInfo) DrainedPastFence() bool {
	if info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED || info.flusherCheckpoint == nil {
		return false
	}
	gate := info.fenceGate()
	return gate != 0 && info.flusherCheckpoint.TimeTick >= gate
}

// SplitTimeTickIfSplitted returns the split time tick (T_switch) and true when
// the vchannel is a SPLITTED shard split source that the recovery storage
// still holds, retired or not: until it is collected its frozen flusher
// checkpoint keeps pinning WAL truncation.
func (info *vchannelRecoveryInfo) SplitTimeTickIfSplitted() (uint64, bool) {
	if info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		return 0, false
	}
	return info.meta.SplitTimeTick, true
}

// IsPartitionActive returns true if the partition is active.
func (info *vchannelRecoveryInfo) IsPartitionActive(partitionId int64) bool {
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == partitionId {
			return true
		}
	}
	return false
}

// GetFlushCheckpoint returns the flush checkpoint of the vchannel recovery info.
// return nil if the flush checkpoint is not set.
func (info *vchannelRecoveryInfo) GetFlushCheckpoint() *WALCheckpoint {
	return info.flusherCheckpoint
}

// GetSchema returns the schema of the vchannel at the given timetick.
// return nil if the schema is not found.
func (info *vchannelRecoveryInfo) GetSchema(timetick uint64) (int, *schemapb.CollectionSchema) {
	if timetick == 0 {
		// timetick 0 means the latest schema.
		timetick = math.MaxUint64
	}

	for i := len(info.meta.CollectionInfo.Schemas) - 1; i >= 0; i-- {
		schema := info.meta.CollectionInfo.Schemas[i]
		if schema.CheckpointTimeTick <= timetick {
			return i, schema.Schema
		}
	}
	return -1, nil
}

// UpdateFlushCheckpoint updates the flush checkpoint of the vchannel recovery info.
func (info *vchannelRecoveryInfo) UpdateFlushCheckpoint(checkpoint *WALCheckpoint) error {
	// Because current L0 may be consuming the data before the flush checkpoint,
	// so we introduce a tolerance duration to delay the drop operation of the schema that is not used anymore.
	tolerance := paramtable.Get().StreamingCfg.WALRecoverySchemaExpirationTolerance.GetAsDurationByParse()
	if info.flusherCheckpoint == nil || info.flusherCheckpoint.MessageID.LTE(checkpoint.MessageID) {
		info.flusherCheckpoint = checkpoint
		timetick := tsoutil.AddPhysicalDurationOnTs(info.flusherCheckpoint.TimeTick, -tolerance)
		idx, _ := info.GetSchema(timetick)
		for i := 0; i < idx; i++ {
			// drop the schema that is not used anymore.
			// the future GetSchema operation will use the timetick greater than the flusher checkpoint.
			// Those schema is too old, and will not be used anymore, can be dropped.
			if info.meta.CollectionInfo.Schemas[i].State == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL {
				info.meta.CollectionInfo.Schemas[i].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
				info.dirty = true
			}
		}
		return nil
	}
	return status.NewInner("update illegal checkpoint of flusher, current: %s, target: %s", info.flusherCheckpoint.MessageID.String(), checkpoint.MessageID.String())
}

// ObserveSchemaChange is called when a schema change message is observed.
func (info *vchannelRecoveryInfo) ObserveSchemaChange(msg message.ImmutableSchemaChangeMessageV2) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}

	info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
		Schema:             msg.MustBody().Schema,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		CheckpointTimeTick: msg.TimeTick(),
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
}

// ObservePutCollection is called when a put collection message is observed.
func (info *vchannelRecoveryInfo) ObserveAlterCollection(msg message.ImmutableAlterCollectionMessageV2) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	if messageutil.IsSchemaChange(msg.Header()) {
		info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
			Schema:             msg.MustBody().Updates.Schema,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
			CheckpointTimeTick: msg.TimeTick(),
		})
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
}

// ObserveDropCollection is called when a drop collection message is observed.
func (info *vchannelRecoveryInfo) ObserveDropCollection(msg message.ImmutableDropCollectionMessageV1) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	if info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		// make it idempotent, only the first drop collection message can be observed.
		return
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	// A genuine DropCollection reaching a vchannel that a shard split had
	// already retired must still be a real drop: clear Retired so
	// dropAllVirtualChannel does not mistake this DROPPED meta for a split
	// source being locally collected and skip notifying DataCoord about it.
	info.meta.Retired = false
	info.dirty = true
}

// ObserveRetire is called when a vchannel is retired: the routing commit
// that finishes a shard split delists this vchannel, so it no longer serves
// or receives traffic.
//
// Unlike the old ObserveDropVChannel this never moves the vchannel to
// DROPPED — the source of a shard split must not, because DROPPED is what
// dropAllVirtualChannel scans the persisted snapshot for to call DataCoord's
// DropVirtualChannel, and that RPC destroys DataCoord's own tombstone for a
// channel a client can still be depending on if this streamingnode crashes
// before the split is fully drained. So retiring only sets a flag on an
// already-SPLITTED vchannel: it stays SPLITTED, and
// ConsumeDirtyAndGetSnapshot is what later decides the meta is safe to
// remove from the catalog, once the flusher checkpoint proves nothing is
// left to replay past the fence — without ever touching DataCoord.
//
// Only a SPLITTED (already-fenced) vchannel is retirable. A vchannel that
// has not been fenced yet takes no action (retiring only ever follows
// fencing in the real flow), and neither does one that is already retired
// or otherwise gone — which is what makes a replayed retire harmless.
func (info *vchannelRecoveryInfo) ObserveRetire() {
	if info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		return
	}
	if info.meta.Retired {
		// make it idempotent, only the first retire can be observed.
		return
	}
	info.meta.Retired = true
	info.dirty = true
}

// ObserveSplitShard is called when a split shard message is observed.
// The vchannel is fenced by shard split: it never accepts new DML again,
// and the state must survive restarts so the fence keeps holding after
// recovery.
//
// SplitTimeTick is T_switch as the source handler reported it on the record
// (splitSwitchTimeTickOf), not necessarily the record's own tick. The handler
// fences the vchannel in memory before it appends, at the first attempt's tick,
// and never rolls that back. If that first append failed without persisting,
// the first fence record in the WAL is a re-drive with a later tick that
// reports the first attempt's tick -- the one DataCoord records, and the one a
// tombstone rebuilt from this meta after a restart must report again.
// CheckpointTimeTick keeps the record's own tick.
//
// An already-SPLITTED vchannel is left exactly as it is: a same-task re-fence
// seals the same data, and must not move SplitTimeTick: the retire collection
// waits for the flusher checkpoint to pass SplitTimeTick, and once the source's
// data sync service has closed past the first fence nothing would ever advance
// that checkpoint to a later tick -- the row would never be collected and would
// pin truncation.
func (info *vchannelRecoveryInfo) ObserveSplitShard(msg message.ImmutableSplitShardMessageV2) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	if info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		return
	}
	if info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		// make it idempotent, a dropped vchannel never goes back to normal
		// or splitted.
		return
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	// The record's own tick is the seal record's tick the flusher gates the
	// source's data sync service on (flusherComponents.RecordFence sees the
	// same record); a same-task re-fence returns above and moves neither.
	info.splitFenceGate = msg.TimeTick()
	// SplitTimeTick is T_switch and stays fixed at the fence, unlike
	// CheckpointTimeTick which keeps advancing; it is read back on an
	// already-fenced re-fence so the split coordinator recovers T_switch.
	info.meta.SplitTimeTick = splitSwitchTimeTickOf(msg)
	// The task that placed the fence, so a re-fence can tell the same task's
	// retry from a concurrent task landing on a source it does not own.
	info.meta.SplitTaskId = msg.Header().GetSplitTaskId()
	info.dirty = true
}

// splitSwitchTimeTickOf returns T_switch for a source fence record: the tick
// the source handler stamped on it as SplitShardExtraResponse (the task's first
// fence, which the shard manager recorded before the append), or the record's
// own tick for a record that carries none or a zero one.
func splitSwitchTimeTickOf(msg message.ImmutableSplitShardMessageV2) uint64 {
	if extra := message.AppendExtraOf(msg); extra != nil {
		resp := &message.SplitShardExtraResponse{}
		if err := extra.UnmarshalTo(resp); err == nil && resp.GetSplitTimeTick() != 0 {
			return resp.GetSplitTimeTick()
		}
	}
	return msg.TimeTick()
}

// ObserveDropPartition is called when a drop partition message is observed.
func (info *vchannelRecoveryInfo) ObserveDropPartition(msg message.ImmutableDropPartitionMessageV1) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	for i, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			// make it idempotent, only the first drop partition message can be observed.
			info.meta.CollectionInfo.Partitions = append(info.meta.CollectionInfo.Partitions[:i], info.meta.CollectionInfo.Partitions[i+1:]...)
			info.meta.CheckpointTimeTick = msg.TimeTick()
			info.dirty = true
			return
		}
	}
}

// ObserveCreatePartition is called when a create partition message is observed.
func (info *vchannelRecoveryInfo) ObserveCreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage.
		return
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			// make it idempotent, only the first create partition message can be observed.
			return
		}
	}
	info.meta.CollectionInfo.Partitions = append(info.meta.CollectionInfo.Partitions, &streamingpb.PartitionInfoOfVChannel{
		PartitionId: msg.Header().PartitionId,
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
}

// ConsumeDirtyAndGetSnapshot returns the snapshot of the vchannel recovery info.
// It returns nil if the vchannel recovery info is not dirty and there is
// nothing else that must still reach the catalog (see below).
//
// ShouldBeRemoved is computed from the live state regardless of dirty, same
// as before: a DROPPED vchannel is always removable. A SPLITTED vchannel
// that the adoption replica has retired becomes removable too, once its own
// flusher checkpoint has drained past its fence gate (DrainedPastFence): the
// condition the flusher closed its data sync service on, which proves there
// is nothing left of the source to replay and nothing left of it growing.
// Its own checkpoint, not the pchannel-wide minimum: another split's frozen
// source, or a vchannel with no checkpoint yet, says nothing about this one
// (see hasCollectableRetiredVChannelLocked).
//
// The catalog's own removal logic keys off State==DROPPED (it has no separate
// "retired and drained" concept), so the moment a retired SPLITTED vchannel
// becomes removable, the returned snapshot is rewritten to State=DROPPED —
// with Retired left true — so this one last write is what actually deletes
// the row from the catalog; the live meta in info itself is untouched (it is
// about to be forgotten by the caller anyway once ShouldBeRemoved is true).
// This has to happen unconditionally, not only when dirty: Retired is
// typically set (and persisted, clearing dirty) well before the flusher
// checkpoint independently catches up to the fence, so the
// round that finally satisfies the removal condition is very often one where
// dirty is already false — and without emitting a snapshot on that round too,
// the catalog would never see the DROPPED write and the meta would linger in
// etcd forever, reloaded on every restart. dropAllVirtualChannel is what
// reads Retired back off a DROPPED snapshot to tell this apart from a genuine
// drop and skip calling DataCoord for it.
func (info *vchannelRecoveryInfo) ConsumeDirtyAndGetSnapshot() (dirtySnapshot *streamingpb.VChannelMeta, ShouldBeRemoved bool) {
	retiredAndDrained := info.meta.Retired && info.DrainedPastFence()
	shouldBeRemoved := info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED || retiredAndDrained

	if !info.dirty {
		if retiredAndDrained {
			snapshot := proto.Clone(info.meta).(*streamingpb.VChannelMeta)
			snapshot.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
			return snapshot, true
		}
		return nil, shouldBeRemoved
	}
	// create the snapshot of the vchannel recovery info first.
	snapshot := proto.Clone(info.meta).(*streamingpb.VChannelMeta)
	if retiredAndDrained {
		snapshot.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	}

	// consume the dirty part of the vchannel recovery info.
	for i := len(info.meta.CollectionInfo.Schemas) - 1; i >= 0; i-- {
		// the schema is always dropped by timetick order,
		// so we find the max index of the schema that is dropped,
		// and drop all schema before it.
		// the last schema is always normal, so it's safe to drop the schema by range.
		if info.meta.CollectionInfo.Schemas[i].State == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED {
			info.meta.CollectionInfo.Schemas = info.meta.CollectionInfo.Schemas[i+1:]
			break
		}
	}
	info.dirty = false
	return snapshot, shouldBeRemoved
}
