package vchannel

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Retired sources retain their metadata as a replay filter and a durable DML
// fence. Query runtimes and their target-stream subscriptions are released.
func (info *VChannelView) IsSplitRetired() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED
}

func (info *VChannelView) ObserveRetireSplitShard(msg message.ImmutableRetireSplitShardMessageV2) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	header := msg.Header()
	if !msg.IsUnreplicable() || msg.ReplicateHeader() != nil ||
		header.GetCollectionId() != info.meta.GetCollectionInfo().GetCollectionId() ||
		header.GetSplitTaskId() == 0 || header.GetSplitTaskId() != info.meta.GetSplitLineage().GetSplitTaskId() ||
		msg.VChannel() != info.meta.GetVchannel() ||
		info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL ||
		!info.shouldObserveLocked(msg.TimeTick()) || info.closingLocked(0) {
		return false
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

func (m *VChannelRecoveryModule) releaseSplitQueryRetentionLocked() {
	if m.summaryManager != nil {
		m.summaryManager.SetQueryRetention(m.vchannel, math.MaxUint64)
	}
}
