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

package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type CollectionShardLeaderCache = map[string]*querypb.ShardLeadersList

// LeaderCacheObserver is to invalidate shard leader cache when leader location changes
type LeaderCacheObserver struct {
	wg      sync.WaitGroup
	dist    *meta.DistributionManager
	meta    *meta.Meta
	target  *meta.TargetManager
	nodeMgr *session.NodeManager

	proxyManager proxyutil.ProxyClientManagerInterface

	stopOnce sync.Once

	closeCh chan struct{}

	shardLeaderCaches typeutil.ConcurrentMap[int64, CollectionShardLeaderCache]
}

func (o *LeaderCacheObserver) Start(ctx context.Context) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.schedule(ctx)
	}()
}

func (o *LeaderCacheObserver) Stop() {
	o.stopOnce.Do(func() {
		close(o.closeCh)
		o.wg.Wait()
	})
}

func (o *LeaderCacheObserver) schedule(ctx context.Context) {
	ticker := time.NewTicker(paramtable.Get().QueryCoordCfg.ShardLeaderCacheInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("stop leader cache observer due to context done")
			return
		case <-o.closeCh:
			log.Info("stop leader cache observer")
			return

		case <-ticker.C:
			o.cleanReleasedCollection()
			o.observe(ctx)
		}
	}
}

func (o *LeaderCacheObserver) observe(ctx context.Context) {
	collectionIDs := o.meta.CollectionManager.GetAll()
	changedCollections := make([]int64, 0)
	newLeaderDist := make(map[int64]CollectionShardLeaderCache, 0)
	for _, cid := range collectionIDs {
		if o.readyToObserve(cid) {
			newLeaders, changed := o.checkShardLeaderListChanged(cid)
			if changed {
				changedCollections = append(changedCollections, cid)
				newLeaderDist[cid] = newLeaders
			}
		}
	}

	if len(changedCollections) > 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Second))
		defer cancel()
		err := o.proxyManager.InvalidateShardLeaderCache(ctx, &proxypb.InvalidateShardLeaderCacheRequest{
			CollectionIDs: changedCollections,
		})
		if err != nil {
			log.Warn("failed to invalidate proxy's shard leader cache", zap.Error(err))
			return
		}

		// update shardLeaderCaches when invalidate shard leader success
		for cid, newLeaders := range newLeaderDist {
			o.shardLeaderCaches.Insert(cid, newLeaders)
		}
	}
}

func (o *LeaderCacheObserver) cleanReleasedCollection() {
	collectionSet := typeutil.NewUniqueSet(o.meta.CollectionManager.GetAll()...)
	o.shardLeaderCaches.Range(func(key int64, value CollectionShardLeaderCache) bool {
		if !collectionSet.Contain(key) {
			o.shardLeaderCaches.Remove(key)
		}
		return true
	})
}

func (o *LeaderCacheObserver) checkShardLeaderListChanged(collectionID int64) (map[string]*querypb.ShardLeadersList, bool) {
	newLeaders, err := utils.GetReadableShardLeaders(o.meta, o.dist, o.target, o.nodeMgr, collectionID)
	if err != nil {
		log.Warn("failed to get readable shard leader list", zap.Int64("collectionID", collectionID), zap.Error(err))
		return map[string]*querypb.ShardLeadersList{}, true
	}

	oldLeaders, ok := o.shardLeaderCaches.Get(collectionID)
	if !ok {
		return newLeaders, true
	}

	if len(oldLeaders) != len(newLeaders) {
		return newLeaders, true
	}

	shardLeaderEquals := func(oldLeader, newLeader *querypb.ShardLeadersList) bool {
		if len(oldLeader.NodeIds) != len(newLeader.NodeIds) {
			return false
		}

		oldNodesSet := typeutil.NewUniqueSet(oldLeader.NodeIds...)
		for _, nodeID := range newLeader.NodeIds {
			if !oldNodesSet.Contain(nodeID) {
				return false
			}
		}

		return true
	}
	// check whether shard leader list changed
	for ch, newLeader := range newLeaders {
		oldLeader := oldLeaders[ch]
		if !shardLeaderEquals(oldLeader, newLeader) {
			return newLeaders, true
		}
	}

	return nil, false
}

func (o *LeaderCacheObserver) readyToObserve(collectionID int64) bool {
	metaExist := (o.meta.GetCollection(collectionID) != nil)
	targetExist := o.target.IsNextTargetExist(collectionID) || o.target.IsCurrentTargetExist(collectionID)
	return metaExist && targetExist
}

func NewLeaderCacheObserver(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr *meta.TargetManager,
	nodeMgr *session.NodeManager,
	proxyManager proxyutil.ProxyClientManagerInterface,
) *LeaderCacheObserver {
	return &LeaderCacheObserver{
		dist:         dist,
		meta:         meta,
		target:       targetMgr,
		nodeMgr:      nodeMgr,
		proxyManager: proxyManager,
		closeCh:      make(chan struct{}),
	}
}
