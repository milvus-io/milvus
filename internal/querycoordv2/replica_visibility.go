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

package querycoordv2

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type replicaVisibilityBatch struct {
	replicas map[int64]map[int64]struct{}
}

// replicaVisibilityManager gates newly spawned cluster-level load-config replicas
// from Proxy shard-leader discovery until the whole batch is ready.
type replicaVisibilityManager struct {
	mu sync.RWMutex

	nextBatchID int64
	activeBatch int64
	batches     map[int64]*replicaVisibilityBatch

	invisibleReplicas map[int64]map[int64]int64 // collectionID -> replicaID -> batchID
}

func newReplicaVisibilityManager() *replicaVisibilityManager {
	return &replicaVisibilityManager{
		batches:           make(map[int64]*replicaVisibilityBatch),
		invisibleReplicas: make(map[int64]map[int64]int64),
	}
}

func (m *replicaVisibilityManager) BeginBatch() int64 {
	if m == nil {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextBatchID++
	batchID := m.nextBatchID
	m.activeBatch = batchID
	m.batches[batchID] = &replicaVisibilityBatch{
		replicas: make(map[int64]map[int64]struct{}),
	}
	return batchID
}

func (m *replicaVisibilityManager) EndBatch(batchID int64) {
	if m == nil || batchID == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeBatch == batchID {
		m.activeBatch = 0
	}
	if batch, ok := m.batches[batchID]; ok && len(batch.replicas) == 0 {
		delete(m.batches, batchID)
	}
}

func (m *replicaVisibilityManager) CancelBatch(batchID int64) {
	if m == nil || batchID == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeBatchLocked(batchID)
	if m.activeBatch == batchID {
		m.activeBatch = 0
	}
}

func (m *replicaVisibilityManager) AddInvisibleReplicas(collectionID int64, replicas ...*meta.Replica) {
	if m == nil || len(replicas) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	batchID := m.activeBatch
	if batchID == 0 {
		return
	}
	batch, ok := m.batches[batchID]
	if !ok {
		batch = &replicaVisibilityBatch{
			replicas: make(map[int64]map[int64]struct{}),
		}
		m.batches[batchID] = batch
	}

	if batch.replicas[collectionID] == nil {
		batch.replicas[collectionID] = make(map[int64]struct{})
	}
	if m.invisibleReplicas[collectionID] == nil {
		m.invisibleReplicas[collectionID] = make(map[int64]int64)
	}
	for _, replica := range replicas {
		if replica == nil {
			continue
		}
		replicaID := replica.GetID()
		batch.replicas[collectionID][replicaID] = struct{}{}
		m.invisibleReplicas[collectionID][replicaID] = batchID
	}
}

func (m *replicaVisibilityManager) IsVisible(replica *meta.Replica) bool {
	if m == nil || replica == nil {
		return true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	replicas := m.invisibleReplicas[replica.GetCollectionID()]
	_, invisible := replicas[replica.GetID()]
	return !invisible
}

func (m *replicaVisibilityManager) HasPending() bool {
	if m == nil {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.batches) > 0
}

func (m *replicaVisibilityManager) PromoteReady(
	ctx context.Context,
	metaMgr *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	distMgr *meta.DistributionManager,
	nodeMgr *session.NodeManager,
) []int64 {
	if m == nil {
		return nil
	}

	batches := m.snapshotBatches()
	if len(batches) == 0 {
		return nil
	}

	readyBatches := make([]int64, 0)
	readyCollections := typeutil.NewUniqueSet()
	for batchID, batch := range batches {
		if m.isBatchReady(ctx, metaMgr, targetMgr, distMgr, nodeMgr, batch) {
			readyBatches = append(readyBatches, batchID)
			for collectionID := range batch.replicas {
				readyCollections.Insert(collectionID)
			}
		}
	}
	if len(readyBatches) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, batchID := range readyBatches {
		m.removeBatchLocked(batchID)
	}
	collections := readyCollections.Collect()
	log.Ctx(ctx).Info("load config replica visibility promoted",
		zap.Int64s("collections", collections),
		zap.Int64s("batches", readyBatches))
	return collections
}

func (m *replicaVisibilityManager) snapshotBatches() map[int64]*replicaVisibilityBatch {
	m.mu.RLock()
	defer m.mu.RUnlock()

	batches := make(map[int64]*replicaVisibilityBatch, len(m.batches))
	for batchID, batch := range m.batches {
		if len(batch.replicas) == 0 {
			continue
		}
		copied := &replicaVisibilityBatch{
			replicas: make(map[int64]map[int64]struct{}, len(batch.replicas)),
		}
		for collectionID, replicas := range batch.replicas {
			copied.replicas[collectionID] = make(map[int64]struct{}, len(replicas))
			for replicaID := range replicas {
				copied.replicas[collectionID][replicaID] = struct{}{}
			}
		}
		batches[batchID] = copied
	}
	return batches
}

func (m *replicaVisibilityManager) isBatchReady(
	ctx context.Context,
	metaMgr *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	distMgr *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	batch *replicaVisibilityBatch,
) bool {
	for collectionID, replicas := range batch.replicas {
		channels := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
		if len(channels) == 0 {
			return false
		}
		for replicaID := range replicas {
			replica := metaMgr.Get(ctx, replicaID)
			if replica == nil {
				continue
			}
			for channelName := range channels {
				leader := distMgr.ChannelDistManager.GetShardLeader(channelName, replica)
				if leader == nil || !leader.IsServiceable() || nodeMgr.Get(leader.Node) == nil {
					return false
				}
			}
		}
	}
	return true
}

func (m *replicaVisibilityManager) removeBatchLocked(batchID int64) {
	batch, ok := m.batches[batchID]
	if !ok {
		return
	}
	for collectionID, replicas := range batch.replicas {
		for replicaID := range replicas {
			if m.invisibleReplicas[collectionID][replicaID] == batchID {
				delete(m.invisibleReplicas[collectionID], replicaID)
			}
		}
		if len(m.invisibleReplicas[collectionID]) == 0 {
			delete(m.invisibleReplicas, collectionID)
		}
	}
	delete(m.batches, batchID)
}

func (s *Server) tryPromoteReadyReplicaVisibility(ctx context.Context) {
	if s.replicaVisibilityManager == nil || !s.replicaVisibilityManager.HasPending() {
		return
	}
	collections := s.replicaVisibilityManager.PromoteReady(ctx, s.meta, s.targetMgr, s.dist, s.nodeMgr)
	if len(collections) == 0 {
		return
	}
	if s.proxyClientManager == nil {
		return
	}
	if err := s.proxyClientManager.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: collections,
	}); err != nil {
		log.Ctx(ctx).Warn("failed to invalidate proxy shard leader cache after promoting replica visibility",
			zap.Int64s("collections", collections),
			zap.Error(err))
	}
}
