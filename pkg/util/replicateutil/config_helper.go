// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replicateutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type (
	targetToSourceChannelMap = map[string]string // targetChannel -> sourceChannel
	sourceToTargetChannelMap = map[string]string // sourceChannel -> targetChannel
)

type ConfigHelper struct {
	sourceCluster  *commonpb.MilvusCluster
	targetClusters []*commonpb.MilvusCluster
	clusterMap     map[string]*commonpb.MilvusCluster

	sourceToTargetChannelMap map[string]sourceToTargetChannelMap // targetClusterID -> sourceToTargetChannelMap
	targetToSourceChannelMap targetToSourceChannelMap

	config *commonpb.ReplicateConfiguration
}

func NewConfigHelper(config *commonpb.ReplicateConfiguration) *ConfigHelper {
	if config == nil {
		return nil
	}
	helper := &ConfigHelper{
		config: config,
	}
	helper.build(config)
	return helper
}

func (h *ConfigHelper) build(config *commonpb.ReplicateConfiguration) {
	h.clusterMap = make(map[string]*commonpb.MilvusCluster, len(config.GetClusters()))
	for _, cluster := range config.GetClusters() {
		h.clusterMap[cluster.GetClusterId()] = cluster
	}

	h.targetClusters = make([]*commonpb.MilvusCluster, 0, len(config.GetClusters()))
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

func (h *ConfigHelper) GetCluster(clusterID string) *commonpb.MilvusCluster {
	cluster, ok := h.clusterMap[clusterID]
	if !ok {
		panic(fmt.Sprintf("cluster %s not found", clusterID))
	}
	return cluster
}

func (h *ConfigHelper) GetSourceCluster() *commonpb.MilvusCluster {
	return h.sourceCluster
}

func (h *ConfigHelper) GetTargetClusters() []*commonpb.MilvusCluster {
	return h.targetClusters
}

func (h *ConfigHelper) GetSourceChannel(targetChannelName string) string {
	sourceChannel, ok := h.targetToSourceChannelMap[targetChannelName]
	if !ok {
		panic(fmt.Sprintf("source channel not found for target channel %s", targetChannelName))
	}
	return sourceChannel
}

func (h *ConfigHelper) GetTargetChannel(sourceChannelName string, targetClusterID string) string {
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

func (h *ConfigHelper) GetFullReplicateConfiguration() *commonpb.ReplicateConfiguration {
	return h.config
}
