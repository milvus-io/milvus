package replicateutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type ConfigHelper interface {
	// Cluster related
	GetCluster(clusterID string) (*milvuspb.MilvusCluster, bool)
	GetSourceCluster() *milvuspb.MilvusCluster
	GetTargetClusters() []*milvuspb.MilvusCluster

	// PChannel related
	GetSourceChannel(targetChannelName string) string
	GetTargetChannel(sourceChannelName string) string
}

// var _ ConfigHelper = &configHelper{}

// type configHelper struct {
// 	sourceCluster  *milvuspb.MilvusCluster
// 	targetClusters []*milvuspb.MilvusCluster
// 	clusterMap     map[string]*milvuspb.MilvusCluster

// 	sourceChannelMap map[string]map[string]string // sourceChannel -> (targetClusterID -> targetChannel)
// 	targetChannelMap map[string]string            // targetChannel -> sourceChannel
// }

// func NewConfigHelper(config *milvuspb.ReplicateConfiguration) ConfigHelper {
// 	helper := &configHelper{}
// 	helper.build(config)
// 	return helper
// }

// func (h *configHelper) build(config *milvuspb.ReplicateConfiguration) {
// 	h.clusterMap = make(map[string]*milvuspb.MilvusCluster, len(config.GetClusters()))
// 	for _, cluster := range config.GetClusters() {
// 		h.clusterMap[cluster.GetClusterId()] = cluster
// 	}

// 	h.targetClusters = make([]*milvuspb.MilvusCluster, 0, len(config.GetClusters()))
// 	for _, topology := range config.GetCrossClusterTopology() {
// 		if h.sourceCluster == nil {
// 			sourceClusterID := topology.GetSourceClusterId()
// 			h.sourceCluster = h.clusterMap[sourceClusterID]
// 		}
// 		targetClusterID := topology.GetTargetClusterId()
// 		h.targetClusters = append(h.targetClusters, h.clusterMap[targetClusterID])
// 	}

// 	h.sourceChannelMap = make(map[string]string)
// 	h.targetChannelMap = make(map[string]string)
// 	for _, cluster := range h.targetClusters {
// 		sourcePChannels := h.sourceCluster.GetPchannels()
// 		for _, pchannel := range cluster.GetPchannels() {
// 		}
// 	}
// }
