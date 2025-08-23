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
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ClusterReplicator interface {
	// StartReplicateCluster starts the replication for the target cluster.
	StartReplicateCluster()

	// StopReplicateCluster stops the replication for the target cluster
	// and wait for the replication to exit.
	StopReplicateCluster()
}

var _ ClusterReplicator = (*clusterReplicator)(nil)

// clusterReplicator is the implementation of ClusterReplicator.
type clusterReplicator struct {
	targetCluster *milvuspb.MilvusCluster
	// channelReplicators is a map of target channel name to target ChannelReplicator.
	channelReplicators *typeutil.ConcurrentMap[string, ChannelReplicator]
	configuration      *milvuspb.ReplicateConfiguration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewClusterReplicator creates a new ClusterReplicator.
func NewClusterReplicator(targetCluster *milvuspb.MilvusCluster, configuration *milvuspb.ReplicateConfiguration) ClusterReplicator {
	ctx, cancel := context.WithCancel(context.Background())
	return &clusterReplicator{
		ctx:                ctx,
		cancel:             cancel,
		targetCluster:      targetCluster,
		channelReplicators: typeutil.NewConcurrentMap[string, ChannelReplicator](),
		configuration:      configuration,
	}
}

func (r *clusterReplicator) StartReplicateCluster() {
	sourceCluster := replicateutil.MustGetMilvusCluster(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), r.configuration)
	for _, targetChannelName := range r.targetCluster.GetPchannels() {
		sourceChannelName := replicateutil.MustGetSourceChannelName(sourceCluster.GetClusterId(), targetChannelName, r.configuration)
		channelReplicator := NewChannelReplicator(sourceChannelName, targetChannelName, r.targetCluster)
		channelReplicator.StartReplicateChannel()
		r.channelReplicators.Insert(targetChannelName, channelReplicator)
	}
	r.wg.Add(1)
	go r.updateMetricsLoop()
}

func (r *clusterReplicator) StopReplicateCluster() {
	r.channelReplicators.Range(func(key string, value ChannelReplicator) bool {
		value.StopReplicateChannel()
		return true
	})
	r.cancel()
	r.wg.Wait()
}

func (r *clusterReplicator) updateMetricsLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.updateMetrics()
		}
	}
}

func (r *clusterReplicator) updateMetrics() {
	// TODO: sheep, update metrics
}
