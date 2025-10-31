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
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context
	mu  sync.Mutex

	// replicators is a map of replicate pchannel name to ChannelReplicator.
	replicators        map[string]Replicator
	replicatorChannels map[string]*meta.ReplicateChannel
}

func NewReplicateManager() *replicateManager {
	return &replicateManager{
		ctx:                context.Background(),
		replicators:        make(map[string]Replicator),
		replicatorChannels: make(map[string]*meta.ReplicateChannel),
	}
}

func buildReplicatorKey(metaKey string, modRevision int64) string {
	return fmt.Sprintf("%s/_v%d", metaKey, modRevision)
}

func (r *replicateManager) CreateReplicator(channel *meta.ReplicateChannel) {
	r.mu.Lock()
	defer r.mu.Unlock()

	logger := log.With(zap.String("key", channel.Key), zap.Int64("modRevision", channel.ModRevision))
	repKey := buildReplicatorKey(channel.Key, channel.ModRevision)
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	if !strings.Contains(channel.Value.GetSourceChannelName(), currentClusterID) {
		return
	}
	_, ok := r.replicators[repKey]
	if ok {
		logger.Debug("replicator already exists, skip create replicator")
		return
	}
	replicator := NewChannelReplicator(channel)
	replicator.StartReplication()
	r.replicators[repKey] = replicator
	r.replicatorChannels[repKey] = channel
	logger.Info("created replicator for replicate pchannel")
}

func (r *replicateManager) RemoveReplicator(key string, modRevision int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeReplicatorInternal(key, modRevision)
}

func (r *replicateManager) removeReplicatorInternal(key string, modRevision int64) {
	logger := log.With(zap.String("key", key), zap.Int64("modRevision", modRevision))
	repKey := buildReplicatorKey(key, modRevision)
	replicator, ok := r.replicators[repKey]
	if !ok {
		logger.Info("replicator not found, skip remove")
		return
	}
	replicator.StopReplication()
	delete(r.replicators, repKey)
	delete(r.replicatorChannels, repKey)
	logger.Info("removed replicator for replicate pchannel")
}

func (r *replicateManager) RemoveOutdatedReplicators(aliveChannels []*meta.ReplicateChannel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	alivesMap := make(map[string]struct{})
	for _, channel := range aliveChannels {
		repKey := buildReplicatorKey(channel.Key, channel.ModRevision)
		alivesMap[repKey] = struct{}{}
	}
	for repKey := range r.replicators {
		if _, ok := alivesMap[repKey]; !ok {
			channel := r.replicatorChannels[repKey]
			r.removeReplicatorInternal(channel.Key, channel.ModRevision)
		}
	}
}

func (r *replicateManager) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, replicator := range r.replicators {
		replicator.StopReplication()
	}
	r.replicators = make(map[string]Replicator)
	r.replicatorChannels = make(map[string]*meta.ReplicateChannel)
}
