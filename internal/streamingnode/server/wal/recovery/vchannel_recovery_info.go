package recovery

import (
	"math"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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
	partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(msg.Header().PartitionIds))
	for _, partitionId := range msg.Header().PartitionIds {
		partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{
			PartitionId: partitionId,
		})
	}
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(msg.MustBody().Schema, schema); err != nil {
		panic("failed to unmarshal collection schema, err: " + err.Error())
	}
	return &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: msg.VChannel(),
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: msg.Header().CollectionId,
				Partitions:   partitions,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             schema,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: msg.TimeTick(),
					},
				},
			},
			CheckpointTimeTick: msg.TimeTick(),
		},
		// a new incoming create collection request is always dirty until it is persisted.
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
	if info.flusherCheckpoint == nil || info.flusherCheckpoint.MessageID.LTE(checkpoint.MessageID) {
		info.flusherCheckpoint = checkpoint
		idx, _ := info.GetSchema(info.flusherCheckpoint.TimeTick)
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
	return errors.Errorf("update illegal checkpoint of flusher, current: %s, target: %s", info.flusherCheckpoint.MessageID.String(), checkpoint.MessageID.String())
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
func (info *vchannelRecoveryInfo) ConsumeDirtyAndGetSnapshot() (dirtySnapshot *streamingpb.VChannelMeta, ShouldBeRemoved bool) {
	if !info.dirty {
		return nil, info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
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
	return snapshot, info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
}
