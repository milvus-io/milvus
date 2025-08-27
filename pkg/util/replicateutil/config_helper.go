package replicateutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type ConfigHelper interface {
	// Cluster related
	GetCluster(clusterID string) *milvuspb.MilvusCluster
	GetSourceCluster() *milvuspb.MilvusCluster
	GetTargetClusters() []*milvuspb.MilvusCluster

	// PChannel related
	GetSourceChannel(targetChannelName string) string
	GetTargetChannel(sourceChannelName string, targetClusterID string) string
}

var _ ConfigHelper = &configHelper{}

type (
	targetToSourceChannelMap = map[string]string // targetChannel -> sourceChannel
	sourceToTargetChannelMap = map[string]string // sourceChannel -> targetChannel
)

type configHelper struct {
	sourceCluster  *milvuspb.MilvusCluster
	targetClusters []*milvuspb.MilvusCluster
	clusterMap     map[string]*milvuspb.MilvusCluster

	sourceToTargetChannelMap map[string]sourceToTargetChannelMap // targetClusterID -> sourceToTargetChannelMap
	targetToSourceChannelMap targetToSourceChannelMap
}

func NewConfigHelper(config *milvuspb.ReplicateConfiguration) ConfigHelper {
	helper := &configHelper{}
	helper.build(config)
	return helper
}

func (h *configHelper) build(config *milvuspb.ReplicateConfiguration) {
	h.clusterMap = make(map[string]*milvuspb.MilvusCluster, len(config.GetClusters()))
	for _, cluster := range config.GetClusters() {
		h.clusterMap[cluster.GetClusterId()] = cluster
	}

	h.targetClusters = make([]*milvuspb.MilvusCluster, 0, len(config.GetClusters()))
	for _, topology := range config.GetCrossClusterTopology() {
		if h.sourceCluster == nil {
			sourceClusterID := topology.GetSourceClusterId()
			h.sourceCluster = h.clusterMap[sourceClusterID]
		}
		targetClusterID := topology.GetTargetClusterId()
		h.targetClusters = append(h.targetClusters, h.clusterMap[targetClusterID])
	}

	sourcePChannels := h.sourceCluster.GetPchannels()
	h.sourceToTargetChannelMap = make(map[string]sourceToTargetChannelMap)
	for _, targetCluster := range h.targetClusters {
		sourceToTarget := make(sourceToTargetChannelMap)
		for i, tc := range targetCluster.GetPchannels() {
			sourceToTarget[sourcePChannels[i]] = tc
		}
		h.sourceToTargetChannelMap[targetCluster.GetClusterId()] = sourceToTarget
	}

	h.targetToSourceChannelMap = make(targetToSourceChannelMap)
	for _, targetCluster := range h.targetClusters {
		for i, tc := range targetCluster.GetPchannels() {
			h.targetToSourceChannelMap[tc] = h.sourceCluster.GetPchannels()[i]
		}
	}
}

func (h *configHelper) GetCluster(clusterID string) *milvuspb.MilvusCluster {
	cluster, ok := h.clusterMap[clusterID]
	if !ok {
		panic(fmt.Sprintf("cluster %s not found", clusterID))
	}
	return cluster
}

func (h *configHelper) GetSourceCluster() *milvuspb.MilvusCluster {
	return h.sourceCluster
}

func (h *configHelper) GetTargetClusters() []*milvuspb.MilvusCluster {
	return h.targetClusters
}

func (h *configHelper) GetSourceChannel(targetChannelName string) string {
	sourceChannel, ok := h.targetToSourceChannelMap[targetChannelName]
	if !ok {
		panic(fmt.Sprintf("source channel not found for target channel %s", targetChannelName))
	}
	return sourceChannel
}

func (h *configHelper) GetTargetChannel(sourceChannelName string, targetClusterID string) string {
	sourceToTarget, ok := h.sourceToTargetChannelMap[targetClusterID]
	if !ok {
		panic(fmt.Sprintf("target cluster %s not found", targetClusterID))
	}
	targetChannel, ok := sourceToTarget[sourceChannelName]
	if !ok {
		panic(fmt.Sprintf("target channel not found for source channel %s, target cluster %s", sourceChannelName, targetClusterID))
	}
	return targetChannel
}
