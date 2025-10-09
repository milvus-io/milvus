package util

import (
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

func IsRoleChangedByAlterReplicateConfigMessage(msg message.ImmutableMessage, replicateInfo *streamingpb.ReplicatePChannelMeta) (roleChanged bool) {
	prcMsg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
	replicateConfig := prcMsg.Header().ReplicateConfiguration
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	currentCluster := replicateutil.MustNewConfigHelper(currentClusterID, replicateConfig).GetCurrentCluster()
	_, err := currentCluster.GetTargetChannel(replicateInfo.GetSourceChannelName(),
		replicateInfo.GetTargetCluster().GetClusterId())
	if err != nil {
		// Cannot find the target channel, it means that the `current->target` topology edge is removed,
		// so the cluster role is changed.
		log.Info("cluster role changed by alter replicate config message",
			replicateutil.ConfigLogFields(replicateConfig)...)
		return true
	}
	log.Info("target channel found, cluster role not changed",
		replicateutil.ConfigLogFields(replicateConfig)...)
	return false
}
