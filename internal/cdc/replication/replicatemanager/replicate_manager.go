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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context

	// replicators is a map of replicate pchannel name to ChannelReplicator.
	replicators map[string]Replicator
}

func NewReplicateManager() *replicateManager {
	return &replicateManager{
		ctx:         context.Background(),
		replicators: make(map[string]Replicator),
	}
}

func (r *replicateManager) CreateReplicator(replicateInfo *streamingpb.ReplicatePChannelMeta) {
	_, ok := r.replicators[replicateInfo.GetSourceChannelName()]
	if ok {
		return
	}
	replicator := NewChannelReplicator(replicateInfo)
	replicator.StartReplicate()
	r.replicators[replicateInfo.GetSourceChannelName()] = replicator
	log.Ctx(r.ctx).Info("created replicator for replicate pchannel",
		zap.String("sourceChannel", replicateInfo.GetSourceChannelName()),
		zap.String("targetChannel", replicateInfo.GetTargetChannelName()),
	)
}
