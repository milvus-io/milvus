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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ShardClientMgr interface {
	GetShard(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]NodeInfo, error)
	GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error)
	DeprecateShardCache(database, collectionName string)
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

	leaderMut  sync.RWMutex
	collLeader map[string]map[string]*shardLeaders // database -> collectionName -> collection_leaders
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

		collLeader: make(map[string]map[string]*shardLeaders),
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
	// check cache first
	cacheShardLeaders := m.getCachedShardLeaders(database, collectionName, method)
	if cacheShardLeaders == nil || !withCache {
		// refresh shard leader cache
		newShardLeaders, err := m.updateShardLocationCache(ctx, database, collectionName, collectionID)
		if err != nil {
			return nil, err
		}
		cacheShardLeaders = newShardLeaders
	}

	return cacheShardLeaders.Get(channel), nil
}

func (m *shardClientMgrImpl) GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error) {
	method := "GetShardLeaderList"
	// check cache first
	cacheShardLeaders := m.getCachedShardLeaders(database, collectionName, method)
	if cacheShardLeaders == nil || !withCache {
		// refresh shard leader cache
		newShardLeaders, err := m.updateShardLocationCache(ctx, database, collectionName, collectionID)
		if err != nil {
			return nil, err
		}
		cacheShardLeaders = newShardLeaders
	}

	return cacheShardLeaders.GetShardLeaderList(), nil
}

func (m *shardClientMgrImpl) getCachedShardLeaders(database, collectionName, caller string) *shardLeaders {
	m.leaderMut.RLock()
	var cacheShardLeaders *shardLeaders
	db, ok := m.collLeader[database]
	if !ok {
		cacheShardLeaders = nil
	} else {
		cacheShardLeaders = db[collectionName]
	}
	m.leaderMut.RUnlock()

	if cacheShardLeaders != nil {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), caller, metrics.CacheHitLabel).Inc()
	} else {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), caller, metrics.CacheMissLabel).Inc()
	}

	return cacheShardLeaders
}

func (m *shardClientMgrImpl) updateShardLocationCache(ctx context.Context, database, collectionName string, collectionID int64) (*shardLeaders, error) {
	log := log.Ctx(ctx).With(
		zap.String("db", database),
		zap.String("collectionName", collectionName),
		zap.Int64("collectionID", collectionID))

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
	}
	resp, err := m.mixCoord.GetShardLeaders(ctx, req)
	if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		log.Error("failed to get shard locations",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		return nil, err
	}

	shards := parseShardLeaderList2QueryNode(resp.GetShards())

	// convert shards map to string for logging
	if log.Logger.Level() == zap.DebugLevel {
		shardStr := make([]string, 0, len(shards))
		for channel, nodes := range shards {
			nodeStrs := make([]string, 0, len(nodes))
			for _, node := range nodes {
				nodeStrs = append(nodeStrs, node.String())
			}
			shardStr = append(shardStr, fmt.Sprintf("%s:[%s]", channel, strings.Join(nodeStrs, ", ")))
		}
		log.Debug("update shard leader cache", zap.String("newShardLeaders", strings.Join(shardStr, ", ")))
	}

	newShardLeaders := &shardLeaders{
		collectionID: collectionID,
		shardLeaders: shards,
		idx:          atomic.NewInt64(0),
	}

	m.leaderMut.Lock()
	if _, ok := m.collLeader[database]; !ok {
		m.collLeader[database] = make(map[string]*shardLeaders)
	}
	m.collLeader[database][collectionName] = newShardLeaders
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

	for _, dbInfo := range m.collLeader {
		for _, shardLeaders := range dbInfo {
			for _, nodeInfos := range shardLeaders.shardLeaders {
				for _, node := range nodeInfos {
					shardLeaderInfo[node.NodeID] = node
				}
			}
		}
	}
	return shardLeaderInfo
}

func (m *shardClientMgrImpl) RemoveDatabase(database string) {
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	delete(m.collLeader, database)
}

// DeprecateShardCache clear the shard leader cache of a collection
func (m *shardClientMgrImpl) DeprecateShardCache(database, collectionName string) {
	log.Info("deprecate shard cache for collection", zap.String("collectionName", collectionName))
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	dbInfo, ok := m.collLeader[database]
	if ok {
		delete(dbInfo, collectionName)
		if len(dbInfo) == 0 {
			delete(m.collLeader, database)
		}
	}
}

// InvalidateShardLeaderCache called when Shard leader balance happened
func (m *shardClientMgrImpl) InvalidateShardLeaderCache(collections []int64) {
	log.Info("Invalidate shard cache for collections", zap.Int64s("collectionIDs", collections))
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	collectionSet := typeutil.NewUniqueSet(collections...)
	for dbName, dbInfo := range m.collLeader {
		for collectionName, shardLeaders := range dbInfo {
			if collectionSet.Contain(shardLeaders.collectionID) {
				delete(dbInfo, collectionName)
			}
		}
		if len(dbInfo) == 0 {
			delete(m.collLeader, dbName)
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
							log.Info("remove idle node client", zap.Int64("nodeID", key))
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
