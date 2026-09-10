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
		}
	}
	return infos
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
func newVChannelRecoveryInfoFromSplitShardMessage(msg message.ImmutableSplitShardMessageV2) *vchannelRecoveryInfo {
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(msg.MustBody().GetGenesis())
	return newVChannelRecoveryInfo(msg.VChannel(), msg.Header().CollectionId, msg.Header().PartitionIds, schema, msg.TimeTick())
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
}

// IsActive returns true if the vchannel is active.
func (info *vchannelRecoveryInfo) IsActive() bool {
	return info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
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
func (info *vchannelRecoveryInfo) ObserveRetire(timetick uint64) {
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
// An already-SPLITTED vchannel is not re-fenced from scratch: T_switch (the
// split task's latest fence record) is raised to the new message's tick
// when that tick is larger, mirroring the shard manager's own T_switch
// bookkeeping — a re-fence after a crash, or a retry that lands a later
// tick than the one already recorded, must keep T_switch current. An older
// tick (a stale retry arriving after a newer fence already advanced things)
// changes nothing, which is covered by the checkpoint guard below.
func (info *vchannelRecoveryInfo) ObserveSplitShard(msg message.ImmutableSplitShardMessageV2) {
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	if info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED {
		if msg.TimeTick() > info.meta.SplitTimeTick {
			info.meta.SplitTimeTick = msg.TimeTick()
			info.meta.CheckpointTimeTick = msg.TimeTick()
			info.dirty = true
		}
		return
	}
	if info.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		// make it idempotent, a dropped vchannel never goes back to normal
		// or splitted.
		return
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	// SplitTimeTick is T_switch and stays fixed at the fence, unlike
	// CheckpointTimeTick which keeps advancing; it is read back on an
	// already-fenced re-fence so the split coordinator recovers T_switch.
	info.meta.SplitTimeTick = msg.TimeTick()
	// The task that placed the fence, so a re-fence can tell the same task's
	// retry from a concurrent task landing on a source it does not own.
	info.meta.SplitTaskId = msg.Header().GetSplitTaskId()
	info.dirty = true
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
// It returns nil if the vchannel recovery info is not dirty.
//
// ShouldBeRemoved is computed from the live state regardless of dirty, same
// as before: a DROPPED vchannel is always removable. A SPLITTED vchannel
// that the adoption replica has retired becomes removable too, once
// flusherCheckpointTimeTick — the tick up to which the flusher has actually
// drained this pchannel — has passed the fence (SplitTimeTick), because that
// is what proves there is nothing left of the source to replay.
func (info *vchannelRecoveryInfo) ConsumeDirtyAndGetSnapshot(flusherCheckpointTimeTick uint64) (dirtySnapshot *streamingpb.VChannelMeta, ShouldBeRemoved bool) {
	shouldBeRemoved := info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED ||
		(info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED &&
			info.meta.Retired && flusherCheckpointTimeTick >= info.meta.SplitTimeTick)
	if !info.dirty {
		return nil, shouldBeRemoved
	}
	// create the snapshot of the vchannel recovery info first.
	snapshot := proto.Clone(info.meta).(*streamingpb.VChannelMeta)

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
