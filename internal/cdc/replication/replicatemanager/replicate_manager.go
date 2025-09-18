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

package replicatemanager

import (
	"context"
	"strings"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context

	// replicators is a map of replicate pchannel name to ChannelReplicator.
	replicators         map[string]Replicator
	replicatorPChannels map[string]*streamingpb.ReplicatePChannelMeta
}

func NewReplicateManager() *replicateManager {
	return &replicateManager{
		ctx:                 context.Background(),
		replicators:         make(map[string]Replicator),
		replicatorPChannels: make(map[string]*streamingpb.ReplicatePChannelMeta),
	}
}

func (r *replicateManager) CreateReplicator(replicateInfo *streamingpb.ReplicatePChannelMeta) {
	logger := log.With(
		zap.String("sourceChannel", replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", replicateInfo.GetTargetChannelName()),
	)
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	if !strings.Contains(replicateInfo.GetSourceChannelName(), currentClusterID) {
		// current cluster is not source cluster, skip create replicator
		return
	}
	replicatorKey := streamingcoord.BuildReplicatePChannelMetaKey(replicateInfo)
	_, ok := r.replicators[replicatorKey]
	if ok {
		logger.Debug("replicator already exists, skip create replicator")
		return
	}
	replicator := NewChannelReplicator(replicateInfo)
	replicator.StartReplicate()
	r.replicators[replicatorKey] = replicator
	r.replicatorPChannels[replicatorKey] = replicateInfo
	logger.Info("created replicator for replicate pchannel")
}

func (r *replicateManager) RemoveOutOfTargetReplicators(targetReplicatePChannels []*streamingpb.ReplicatePChannelMeta) {
	targets := lo.KeyBy(targetReplicatePChannels, streamingcoord.BuildReplicatePChannelMetaKey)
	for replicatorKey, replicator := range r.replicators {
		if pchannelMeta, ok := targets[replicatorKey]; !ok {
			replicator.StopReplicate()
			delete(r.replicators, replicatorKey)
			delete(r.replicatorPChannels, replicatorKey)
			log.Info("removed replicator due to out of target",
				zap.String("sourceChannel", pchannelMeta.GetSourceChannelName()),
				zap.String("targetChannel", pchannelMeta.GetTargetChannelName()),
			)
		}
	}
}

func (r *replicateManager) Close() {
	for _, replicator := range r.replicators {
		replicator.StopReplicate()
	}
}
