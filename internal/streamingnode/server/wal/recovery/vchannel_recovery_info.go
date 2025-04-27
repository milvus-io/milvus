package recovery

import (
	"google.golang.org/protobuf/proto"

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
	return &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: msg.VChannel(),
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: msg.Header().CollectionId,
				Partitions:   partitions,
			},
			CheckpointTimeTick: msg.TimeTick(),
		},
		// a new incoming create collection request is always dirty until it is persisted.
		dirty: true,
	}
}

// vchannelRecoveryInfo is the recovery info for a vchannel.
type vchannelRecoveryInfo struct {
	meta  *streamingpb.VChannelMeta
	dirty bool // whether the vchannel recovery info is dirty.
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
	info.dirty = false
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta), info.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
}
