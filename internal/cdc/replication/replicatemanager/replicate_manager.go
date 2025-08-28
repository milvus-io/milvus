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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/samber/lo"
)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context

	// clusterReplicators is a map of target clusterID to ClusterReplicator.
	clusterReplicators map[string]ClusterReplicator
}

func NewReplicateManager() *replicateManager {
	return &replicateManager{
		ctx:                context.Background(),
		clusterReplicators: make(map[string]ClusterReplicator),
	}
}

func (r *replicateManager) UpdateReplications(config *commonpb.ReplicateConfiguration) {
	configHelper := replicateutil.NewConfigHelper(config)
	targetClusters := lo.KeyBy(configHelper.GetTargetClusters(), func(cluster *commonpb.MilvusCluster) string {
		return cluster.GetClusterId()
	})

	// Add and start new cluster replicators that in targetClusters but not in clusterReplicators.
	for _, targetCluster := range targetClusters {
		if _, ok := r.clusterReplicators[targetCluster.GetClusterId()]; ok {
			continue
		}
		clusterReplicator := NewClusterReplicator(targetCluster, config)
		clusterReplicator.StartReplicateCluster()
		r.clusterReplicators[targetCluster.GetClusterId()] = clusterReplicator
	}
	// Stop and remove cluster replicators that are not in the candidate clusters.
	for clusterID, clusterReplicator := range r.clusterReplicators {
		if _, ok := targetClusters[clusterID]; !ok {
			clusterReplicator.StopReplicateCluster()
			delete(r.clusterReplicators, clusterID)
		}
	}
}

func (r *replicateManager) Close() {
	for _, clusterReplicator := range r.clusterReplicators {
		clusterReplicator.StopReplicateCluster()
	}
	r.clusterReplicators = make(map[string]ClusterReplicator)
}
