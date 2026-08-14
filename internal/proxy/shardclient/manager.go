// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ShardClientMgr interface {
	GetShard(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]NodeInfo, error)
	GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error)
	InvalidateShardLeaderCache(collections []int64)
	ListShardLocation() map[int64]NodeInfo
	RemoveDatabase(database string)

	GetClient(ctx context.Context, nodeInfo NodeInfo) (types.QueryNodeClient, error)
	SetClientCreatorFunc(creator queryNodeCreatorFunc)

	Start()
	Close()
}

type shardClientMgrImpl struct {
	clients       *typeutil.ConcurrentMap[UniqueID, *shardClient]
	clientCreator queryNodeCreatorFunc
	closeCh       chan struct{}

	purgeInterval   time.Duration
	expiredDuration time.Duration

	mixCoord types.MixCoordClient

	leaderMut sync.RWMutex
	// collLeader keys shard leaders by the cluster-unique collection id, so name/alias/database
	// resolution (done upstream against the meta cache) can never serve one collection's shard
	// leaders under another's name after an alias repoint or a cross-db rename. Eviction is by
	// collection id only -- the cache deliberately does not depend on the mutable
	// collection->database mapping. See issue #51533.
	//
	// The resource group is part of the key because it is part of the question: a request
	// scoped to one resource group and an unscoped one ask GetShardLeaders different things and
	// get different answers, so caching them under the same key would serve one scope's leaders
	// to another. An unscoped lookup keys on the empty string and therefore keeps a cache of
	// exactly the shape it had when the key was the bare collection id.
	collLeader map[shardLeaderCacheKey]*shardLeaders
}

// shardLeaderCacheKey identifies one cached shard-leader answer: the collection
// it is about, and the resource group the answer was restricted to ("" for the
// collection-wide answer).
type shardLeaderCacheKey struct {
	collectionID  int64
	resourceGroup string
}

const (
	defaultPurgeInterval   = 600 * time.Second
	defaultExpiredDuration = 60 * time.Minute
)

// SessionOpt provides a way to set params in ShardClientMgr
type shardClientMgrOpt func(s ShardClientMgr)

func withShardClientCreator(creator queryNodeCreatorFunc) shardClientMgrOpt {
	return func(s ShardClientMgr) { s.SetClientCreatorFunc(creator) }
}

func DefaultQueryNodeClientCreator(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	return registry.GetInMemoryResolver().ResolveQueryNode(ctx, addr, nodeID)
}

// NewShardClientMgr creates a new shardClientMgr
func NewShardClientMgr(mixCoord types.MixCoordClient, options ...shardClientMgrOpt) *shardClientMgrImpl {
	s := &shardClientMgrImpl{
		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
		clientCreator:   DefaultQueryNodeClientCreator,
		closeCh:         make(chan struct{}),
		purgeInterval:   defaultPurgeInterval,
		expiredDuration: defaultExpiredDuration,

		collLeader: make(map[shardLeaderCacheKey]*shardLeaders),
		mixCoord:   mixCoord,
	}
	for _, opt := range options {
		opt(s)
	}

	return s
}

func (c *shardClientMgrImpl) SetClientCreatorFunc(creator queryNodeCreatorFunc) {
	c.clientCreator = creator
}

func (m *shardClientMgrImpl) GetShard(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]NodeInfo, error) {
	method := "GetShard"
	key := shardLeaderCacheKey{collectionID: collectionID, resourceGroup: routingResourceGroup(ctx)}
	// check cache first
	cacheShardLeaders := m.getCachedShardLeaders(key, method)
	if cacheShardLeaders == nil || !withCache {
		// refresh shard leader cache
		newShardLeaders, err := m.updateShardLocationCache(ctx, database, collectionName, key)
		if err != nil {
			return nil, err
		}
		cacheShardLeaders = newShardLeaders
	}

	return cacheShardLeaders.Get(channel), nil
}

func (m *shardClientMgrImpl) GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error) {
	method := "GetShardLeaderList"
	key := shardLeaderCacheKey{collectionID: collectionID, resourceGroup: routingResourceGroup(ctx)}
	// check cache first
	cacheShardLeaders := m.getCachedShardLeaders(key, method)
	if cacheShardLeaders == nil || !withCache {
		// refresh shard leader cache
		newShardLeaders, err := m.updateShardLocationCache(ctx, database, collectionName, key)
		if err != nil {
			return nil, err
		}
		cacheShardLeaders = newShardLeaders
	}

	return cacheShardLeaders.GetShardLeaderList(), nil
}

func (m *shardClientMgrImpl) getCachedShardLeaders(key shardLeaderCacheKey, caller string) *shardLeaders {
	m.leaderMut.RLock()
	cacheShardLeaders := m.collLeader[key]
	m.leaderMut.RUnlock()

	if cacheShardLeaders != nil {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), caller, metrics.CacheHitLabel).Inc()
	} else {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), caller, metrics.CacheMissLabel).Inc()
	}

	return cacheShardLeaders
}

func (m *shardClientMgrImpl) updateShardLocationCache(ctx context.Context, database, collectionName string, key shardLeaderCacheKey) (*shardLeaders, error) {
	collectionID := key.collectionID
	log := mlog.With(
		mlog.String("db", database),
		mlog.FieldCollectionName(collectionName),
		mlog.FieldCollectionID(collectionID),
		mlog.String("resourceGroup", key.resourceGroup))

	method := "updateShardLocationCache"
	tr := timerecord.NewTimeRecorder(method)
	defer metrics.ProxyUpdateCacheLatency.WithLabelValues(paramtable.GetStringNodeID(), method).
		Observe(float64(tr.ElapseSpan().Milliseconds()))

	req := &querypb.GetShardLeadersRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetShardLeaders),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID:            collectionID,
		WithUnserviceableShards: true,
		// Empty unless the request was scoped at its entry, in which case the
		// coordinator answers only for the replicas in that resource group.
		// The filter cannot be applied here: the response flattens every
		// replica into one list per channel, so which replica - and therefore
		// which resource group - a leader belongs to is gone by the time it
		// arrives.
		ResourceGroup: key.resourceGroup,
	}
	resp, err := m.mixCoord.GetShardLeaders(ctx, req)
	if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		log.Error(ctx, "failed to get shard locations",
			mlog.FieldCollectionID(collectionID),
			mlog.Err(err))
		return nil, err
	}

	shards := parseShardLeaderList2QueryNode(resp.GetShards())

	// convert shards map to string for logging
	if mlog.LevelEnabled(mlog.DebugLevel) {
		shardStr := make([]string, 0, len(shards))
		for channel, nodes := range shards {
			nodeStrs := make([]string, 0, len(nodes))
			for _, node := range nodes {
				nodeStrs = append(nodeStrs, node.String())
			}
			shardStr = append(shardStr, fmt.Sprintf("%s:[%s]", channel, strings.Join(nodeStrs, ", ")))
		}
		log.Debug(ctx, "update shard leader cache", mlog.String("newShardLeaders", strings.Join(shardStr, ", ")))
	}

	newShardLeaders := &shardLeaders{
		collectionID: collectionID,
		shardLeaders: shards,
		idx:          atomic.NewInt64(0),
	}

	m.leaderMut.Lock()
	m.collLeader[key] = newShardLeaders
	m.leaderMut.Unlock()

	return newShardLeaders, nil
}

func parseShardLeaderList2QueryNode(shardsLeaders []*querypb.ShardLeadersList) map[string][]NodeInfo {
	shard2QueryNodes := make(map[string][]NodeInfo)

	for _, leaders := range shardsLeaders {
		qns := make([]NodeInfo, len(leaders.GetNodeIds()))

		for j := range qns {
			qns[j] = NodeInfo{leaders.GetNodeIds()[j], leaders.GetNodeAddrs()[j], leaders.GetServiceable()[j]}
		}

		shard2QueryNodes[leaders.GetChannelName()] = qns
	}

	return shard2QueryNodes
}

// used for Garbage collection shard client
func (m *shardClientMgrImpl) ListShardLocation() map[int64]NodeInfo {
	m.leaderMut.RLock()
	defer m.leaderMut.RUnlock()
	shardLeaderInfo := make(map[int64]NodeInfo)

	for _, shardLeaders := range m.collLeader {
		for _, nodeInfos := range shardLeaders.shardLeaders {
			for _, node := range nodeInfos {
				shardLeaderInfo[node.NodeID] = node
			}
		}
	}
	return shardLeaderInfo
}

// RemoveDatabase is a no-op for the shard cache. DropDatabase requires the database to be empty
// first (rootcoord rejects a non-empty drop), so every collection has already been dropped
// individually and evicted by id via InvalidateShardLeaderCache before this is called. The cache
// is keyed by the cluster-unique collection id and deliberately does not track database
// membership -- that mapping is mutable (cross-db rename), so making eviction depend on it would
// let a stale attribution drop a live collection or leak one that moved. Kept on the interface so
// the DropDatabase meta-cache invalidation path has a symmetric hook.
func (m *shardClientMgrImpl) RemoveDatabase(database string) {}

// InvalidateShardLeaderCache drops the cached shard leaders for the given collection ids.
// Called on shard-leader balance (querycoord), collection drop, and search/query retry.
//
// Every resource-group scope of the named collections goes, not just the collection-wide entry:
// a caller invalidating a collection is saying its leaders moved, and leaving one scope's copy
// behind would keep routing queries scoped to it at query nodes that no longer serve it. The
// scan is over the whole cache because the scopes of a collection are not enumerable from the
// id alone; the cache holds one entry per (loaded collection, scope in use), so it is small.
func (m *shardClientMgrImpl) InvalidateShardLeaderCache(collections []int64) {
	mlog.Info(context.TODO(), "Invalidate shard cache for collections", mlog.Int64s("collectionIDs", collections))
	invalidated := typeutil.NewSet(collections...)
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	for key := range m.collLeader {
		if invalidated.Contain(key.collectionID) {
			delete(m.collLeader, key)
		}
	}
}

func (c *shardClientMgrImpl) GetClient(ctx context.Context, info NodeInfo) (types.QueryNodeClient, error) {
	client, _ := c.clients.GetOrInsert(info.NodeID, newShardClient(info, c.clientCreator, c.expiredDuration))
	return client.getClient(ctx)
}

// PurgeClient purges client if it is not used for a long time
func (c *shardClientMgrImpl) PurgeClient() {
	ticker := time.NewTicker(c.purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			shardLocations := c.ListShardLocation()
			c.clients.Range(func(key UniqueID, value *shardClient) bool {
				if _, ok := shardLocations[key]; !ok {
					// if the client is not used for more than 1 hour, and it's not a delegator anymore, should remove it
					if value.isExpired() {
						closed := value.Close(false)
						if closed {
							c.clients.Remove(key)
							mlog.Info(context.TODO(), "remove idle node client", mlog.FieldNodeID(key))
						}
					}
				}
				return true
			})
		}
	}
}

func (c *shardClientMgrImpl) Start() {
	go c.PurgeClient()
}

// Close release clients
func (c *shardClientMgrImpl) Close() {
	close(c.closeCh)
	c.clients.Range(func(key UniqueID, value *shardClient) bool {
		value.Close(true)
		c.clients.Remove(key)
		return true
	})
}
