package channel

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

// replicateConfigHelper is a helper to manage the replicate configuration.
type replicateConfigHelper struct {
	*replicateutil.ConfigHelper
	ackedPendings *types.AckedResult
	dirty         bool
}

// newReplicateConfigHelperFromMessage creates a new replicate config helper from message.
func newReplicateConfigHelperFromMessage(replicateConfig message.ImmutableAlterReplicateConfigMessageV2) *replicateConfigHelper {
	return newReplicateConfigHelper(&streamingpb.ReplicateConfigurationMeta{
		ReplicateConfiguration: nil,
		AckedResult:            types.NewAckedPendings(replicateConfig.BroadcastHeader().VChannels).AckedResult,
	})
}

// newReplicateConfigHelper creates a new replicate config helper from proto.
func newReplicateConfigHelper(replicateConfig *streamingpb.ReplicateConfigurationMeta) *replicateConfigHelper {
	if replicateConfig == nil {
		return nil
	}
	return &replicateConfigHelper{
		ConfigHelper:  replicateutil.MustNewConfigHelper(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), replicateConfig.GetReplicateConfiguration()),
		ackedPendings: types.NewAckedPendingsFromProto(replicateConfig.GetAckedResult()),
		dirty:         false,
	}
}

// StartUpdating starts the updating process.
// return true if the replicate configuration is changed, false otherwise.
func (rc *replicateConfigHelper) StartUpdating(config *commonpb.ReplicateConfiguration, pchannels []string) bool {
	if rc.ConfigHelper != nil && proto.Equal(config, rc.GetReplicateConfiguration()) {
		return false
	}
	if rc.ackedPendings == nil {
		rc.ackedPendings = types.NewAckedPendings(pchannels)
	}
	return true
}

// Apply applies the replicate configuration to the wal.
func (rc *replicateConfigHelper) Apply(config *commonpb.ReplicateConfiguration, cp []types.AckedCheckpoint) {
	if rc.ackedPendings == nil {
		panic("ackedPendings is nil when applying replicate configuration")
	}
	for _, cp := range cp {
		if rc.ackedPendings.Ack(cp) {
			rc.dirty = true
		}
	}
}

// ConsumeIfDirty consumes the dirty part of the replicate configuration.
func (rc *replicateConfigHelper) ConsumeIfDirty(incoming *commonpb.ReplicateConfiguration) (config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta, dirty bool) {
	if !rc.dirty {
		return nil, nil, false
	}
	rc.dirty = false

	if !rc.ackedPendings.IsAllAcked() {
		// not all the channels are acked, return the current replicate configuration and acked result.
		var cfg *commonpb.ReplicateConfiguration
		if rc.ConfigHelper != nil {
			cfg = rc.ConfigHelper.GetReplicateConfiguration()
		}
		return &streamingpb.ReplicateConfigurationMeta{
			ReplicateConfiguration: cfg,
			AckedResult:            rc.ackedPendings.AckedResult,
		}, nil, true
	}

	// all the channels are acked, return the new replicate configuration and acked result.
	newConfig := replicateutil.MustNewConfigHelper(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), incoming)
	newIncomingCDCTasks := rc.getNewIncomingTask(newConfig)
	rc.ConfigHelper = newConfig
	rc.ackedPendings = nil
	return &streamingpb.ReplicateConfigurationMeta{
		ReplicateConfiguration: incoming,
		AckedResult:            nil,
	}, newIncomingCDCTasks, true
}

// getNewIncomingTask gets the new incoming task from replicatingTasks.
func (cm *replicateConfigHelper) getNewIncomingTask(newConfig *replicateutil.ConfigHelper) []*streamingpb.ReplicatePChannelMeta {
	incoming := newConfig.GetCurrentCluster()
	var current *replicateutil.MilvusCluster
	if cm.ConfigHelper != nil {
		current = cm.ConfigHelper.GetCurrentCluster()
	}
	incomingReplicatingTasks := make([]*streamingpb.ReplicatePChannelMeta, 0, len(incoming.TargetClusters()))
	for _, targetCluster := range incoming.TargetClusters() {
		if current != nil && current.TargetCluster(targetCluster.GetClusterId()) != nil {
			// target already exists, skip it.
			continue
		}
		for _, pchannel := range targetCluster.GetPchannels() {
			incomingReplicatingTasks = append(incomingReplicatingTasks, &streamingpb.ReplicatePChannelMeta{
				SourceChannelName: targetCluster.MustGetSourceChannel(pchannel),
				TargetChannelName: pchannel,
				TargetCluster:     targetCluster.MilvusCluster,
			})
		}
	}
	return incomingReplicatingTasks
}
