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
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	leaderMut    sync.RWMutex
	collLeader   map[string]shardLeadersByCollectionName // database -> collectionName -> collectionID -> collection_leaders
	sfShardCache conc.Singleflight[*shardLeaders]
	refreshSeq   uint64
	refreshes    map[shardCacheRefreshKey]*shardCacheRefreshToken

	shardCacheRefreshTimeout     time.Duration
	shardCacheVersionTTL         time.Duration
	maxShardCacheVersionsPerName int

	testHookAfterShardCacheDoChan       func()
	testHookListShardLocationReadLocked func()
	testHookBeforeShardLocationDelete   func()
}

type (
	shardLeadersByCollectionID   map[int64]*shardLeaders
	shardLeadersByCollectionName map[string]shardLeadersByCollectionID
	shardCacheRefreshKey         struct {
		database       string
		collectionName string
		collectionID   int64
		force          bool
	}
	shardCacheRefreshToken struct {
		id uint64
	}
	shardCacheVersionRef struct {
		database       string
		collectionName string
		collectionID   int64
		leaders        *shardLeaders
	}
)

const (
	defaultPurgeInterval   = 600 * time.Second
	defaultExpiredDuration = 60 * time.Minute

	defaultShardCacheRefreshTimeout = 10 * time.Second
	// Historical alias targets need a short grace period for in-flight requests,
	// but must not retain QueryNode locations beyond the client-purge cadence.
	defaultShardCacheVersionTTL = defaultPurgeInterval
	// Two entries cover the common old/new alias transition; two extra slots
	// absorb overlapping multi-target changes without unbounded growth.
	defaultMaxShardCacheVersionsPerName = 4
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

		collLeader: make(map[string]shardLeadersByCollectionName),
		refreshes:  make(map[shardCacheRefreshKey]*shardCacheRefreshToken),
		mixCoord:   mixCoord,

		shardCacheRefreshTimeout:     defaultShardCacheRefreshTimeout,
		shardCacheVersionTTL:         defaultShardCacheVersionTTL,
		maxShardCacheVersionsPerName: defaultMaxShardCacheVersionsPerName,
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
	shardLeaders, err := m.getShardLeaders(ctx, withCache, database, collectionName, collectionID, "GetShard")
	if err != nil {
		return nil, err
	}

	return shardLeaders.Get(channel), nil
}

func (m *shardClientMgrImpl) GetShardLeaderList(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error) {
	shardLeaders, err := m.getShardLeaders(ctx, withCache, database, collectionName, collectionID, "GetShardLeaderList")
	if err != nil {
		return nil, err
	}

	return shardLeaders.GetShardLeaderList(), nil
}

func (m *shardClientMgrImpl) loadCachedShardLeaders(database, collectionName string, collectionID int64) *shardLeaders {
	m.leaderMut.RLock()
	defer m.leaderMut.RUnlock()

	if db, ok := m.collLeader[database]; ok {
		if collection, ok := db[collectionName]; ok {
			return collection[collectionID]
		}
	}
	return nil
}

func (m *shardClientMgrImpl) getCachedShardLeaders(database, collectionName string, collectionID int64, caller string) *shardLeaders {
	cacheShardLeaders := m.loadCachedShardLeaders(database, collectionName, collectionID)

	if cacheShardLeaders != nil {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), caller, metrics.CacheHitLabel).Inc()
	} else {
		metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), caller, metrics.CacheMissLabel).Inc()
	}

	return cacheShardLeaders
}

func (m *shardClientMgrImpl) acquireShardCacheRefresh(key shardCacheRefreshKey) *shardCacheRefreshToken {
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()

	// The token is the refresh generation for this logical cache entry. Removing
	// it during invalidation makes later callers use a different singleflight key.
	if refresh := m.refreshes[key]; refresh != nil {
		return refresh
	}
	if m.refreshes == nil {
		m.refreshes = make(map[shardCacheRefreshKey]*shardCacheRefreshToken)
	}
	m.refreshSeq++
	refresh := &shardCacheRefreshToken{id: m.refreshSeq}
	m.refreshes[key] = refresh
	return refresh
}

func (m *shardClientMgrImpl) finishShardCacheRefresh(key shardCacheRefreshKey, refresh *shardCacheRefreshToken) {
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()

	if m.refreshes[key] == refresh {
		delete(m.refreshes, key)
	}
}

func (m *shardClientMgrImpl) invalidateShardCacheRefreshesLocked(match func(shardCacheRefreshKey) bool) {
	for key := range m.refreshes {
		if match(key) {
			delete(m.refreshes, key)
		}
	}
}

func (m *shardClientMgrImpl) getShardLeaders(
	ctx context.Context,
	withCache bool,
	database string,
	collectionName string,
	collectionID int64,
	caller string,
) (*shardLeaders, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cacheShardLeaders := m.getCachedShardLeaders(database, collectionName, collectionID, caller)
	if cacheShardLeaders != nil && withCache {
		cacheShardLeaders.touch(time.Now())
		return cacheShardLeaders, nil
	}

	// Keep independently addressable entries for every collection ID that may
	// still have requests in flight under the same name/alias. The singleflight
	// key includes the ID so old and new alias targets neither overwrite nor
	// join each other's refresh. Forced refreshes use a separate key to preserve
	// their semantics while still coalescing concurrent forced refreshes.
	refreshKey := shardCacheRefreshKey{
		database:       database,
		collectionName: collectionName,
		collectionID:   collectionID,
		force:          !withCache,
	}
	refresh := m.acquireShardCacheRefresh(refreshKey)
	key := fmt.Sprintf("%s/%s/%d/%t/%d", database, collectionName, collectionID, refreshKey.force, refresh.id)
	resultCh := m.sfShardCache.DoChan(key, func() (*shardLeaders, error) {
		defer m.finishShardCacheRefresh(refreshKey, refresh)
		if withCache {
			if cached := m.loadCachedShardLeaders(database, collectionName, collectionID); cached != nil {
				cached.touch(time.Now())
				return cached, nil
			}
		}

		// A shared refresh must not inherit the first caller's cancellation or
		// deadline: other waiters may still need the result. Keep request-scoped
		// values while imposing a hard upper bound on the detached Coord RPC.
		refreshTimeout := m.shardCacheRefreshTimeout
		if refreshTimeout <= 0 {
			refreshTimeout = defaultShardCacheRefreshTimeout
		}
		refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refreshTimeout)
		defer cancel()
		return m.updateShardLocationCache(refreshCtx, database, collectionName, collectionID, refreshKey, refresh)
	})
	if m.testHookAfterShardCacheDoChan != nil {
		m.testHookAfterShardCacheDoChan()
	}

	select {
	case result := <-resultCh:
		leaders, err, _ := conc.UnwrapSingleflightResult[*shardLeaders](result)
		return leaders, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *shardClientMgrImpl) updateShardLocationCache(
	ctx context.Context,
	database,
	collectionName string,
	collectionID int64,
	refreshKey shardCacheRefreshKey,
	refresh *shardCacheRefreshToken,
) (*shardLeaders, error) {
	log := mlog.With(
		mlog.String("db", database),
		mlog.FieldCollectionName(collectionName),
		mlog.FieldCollectionID(collectionID))

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
	newShardLeaders.touch(time.Now())

	m.cacheShardLeaders(database, collectionName, collectionID, refreshKey, refresh, newShardLeaders)

	return newShardLeaders, nil
}

func (m *shardClientMgrImpl) cacheShardLeaders(
	database,
	collectionName string,
	collectionID int64,
	refreshKey shardCacheRefreshKey,
	refresh *shardCacheRefreshToken,
	leaders *shardLeaders,
) {
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	// Check the token under the same lock used by invalidation and cache deletion,
	// so an older RPC cannot write back after an invalidation has completed.
	if m.refreshes[refreshKey] != refresh {
		return
	}

	if _, ok := m.collLeader[database]; !ok {
		m.collLeader[database] = make(shardLeadersByCollectionName)
	}
	if _, ok := m.collLeader[database][collectionName]; !ok {
		m.collLeader[database][collectionName] = make(shardLeadersByCollectionID)
	}
	versions := m.collLeader[database][collectionName]
	versions[collectionID] = leaders
	m.pruneExpiredShardVersionsLocked(versions, time.Now())
	m.pruneExcessShardVersionsLocked(versions, collectionID)
}

func (m *shardClientMgrImpl) pruneExpiredShardVersionsLocked(versions shardLeadersByCollectionID, now time.Time) {
	ttl := m.shardCacheVersionTTL
	if ttl <= 0 {
		ttl = defaultShardCacheVersionTTL
	}
	cutoff := now.Add(-ttl)
	for collectionID, leaders := range versions {
		if leaders.idleBefore(cutoff) {
			delete(versions, collectionID)
		}
	}
}

func (m *shardClientMgrImpl) pruneExcessShardVersionsLocked(versions shardLeadersByCollectionID, protectedID int64) {
	limit := m.maxShardCacheVersionsPerName
	if limit <= 0 {
		limit = defaultMaxShardCacheVersionsPerName
	}
	for len(versions) > limit {
		var oldestID int64
		oldestAccess := int64(^uint64(0) >> 1)
		found := false
		for collectionID, leaders := range versions {
			if collectionID == protectedID {
				continue
			}
			lastAccess := leaders.lastAccessUnixNano.Load()
			if lastAccess < oldestAccess {
				oldestID = collectionID
				oldestAccess = lastAccess
				found = true
			}
		}
		if !found {
			return
		}
		delete(versions, oldestID)
	}
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
	shardLeaderInfo := make(map[int64]NodeInfo)
	now := time.Now()
	ttl := m.shardCacheVersionTTL
	if ttl <= 0 {
		ttl = defaultShardCacheVersionTTL
	}
	cutoff := now.Add(-ttl)
	expired := make([]shardCacheVersionRef, 0)

	m.leaderMut.RLock()
	if m.testHookListShardLocationReadLocked != nil {
		m.testHookListShardLocationReadLocked()
	}
	for database, dbInfo := range m.collLeader {
		for collectionName, collectionInfo := range dbInfo {
			for collectionID, leaders := range collectionInfo {
				if leaders.idleBefore(cutoff) {
					expired = append(expired, shardCacheVersionRef{
						database:       database,
						collectionName: collectionName,
						collectionID:   collectionID,
						leaders:        leaders,
					})
					continue
				}
				for _, nodeInfos := range leaders.shardLeaders {
					for _, node := range nodeInfos {
						shardLeaderInfo[node.NodeID] = node
					}
				}
			}
		}
	}
	m.leaderMut.RUnlock()

	if len(expired) == 0 {
		return shardLeaderInfo
	}
	if m.testHookBeforeShardLocationDelete != nil {
		m.testHookBeforeShardLocationDelete()
	}

	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	// A cache hit may touch or replace a candidate after the read scan. Recheck
	// both pointer identity and last access before deleting it under the write lock.
	for _, candidate := range expired {
		dbInfo := m.collLeader[candidate.database]
		collectionInfo := dbInfo[candidate.collectionName]
		leaders := collectionInfo[candidate.collectionID]
		if leaders == nil {
			continue
		}
		if leaders == candidate.leaders && leaders.idleBefore(cutoff) {
			delete(collectionInfo, candidate.collectionID)
			if len(collectionInfo) == 0 {
				delete(dbInfo, candidate.collectionName)
			}
			if len(dbInfo) == 0 {
				delete(m.collLeader, candidate.database)
			}
			continue
		}
		for _, nodeInfos := range leaders.shardLeaders {
			for _, node := range nodeInfos {
				shardLeaderInfo[node.NodeID] = node
			}
		}
	}
	return shardLeaderInfo
}

func (m *shardClientMgrImpl) RemoveDatabase(database string) {
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	m.invalidateShardCacheRefreshesLocked(func(key shardCacheRefreshKey) bool {
		return key.database == database
	})
	delete(m.collLeader, database)
}

// DeprecateShardCache clear the shard leader cache of a collection
func (m *shardClientMgrImpl) DeprecateShardCache(database, collectionName string) {
	mlog.Info(context.TODO(), "deprecate shard cache for collection", mlog.FieldCollectionName(collectionName))
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	m.invalidateShardCacheRefreshesLocked(func(key shardCacheRefreshKey) bool {
		return key.database == database && key.collectionName == collectionName
	})
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
	mlog.Info(context.TODO(), "Invalidate shard cache for collections", mlog.Int64s("collectionIDs", collections))
	m.leaderMut.Lock()
	defer m.leaderMut.Unlock()
	collectionSet := typeutil.NewUniqueSet(collections...)
	m.invalidateShardCacheRefreshesLocked(func(key shardCacheRefreshKey) bool {
		return collectionSet.Contain(key.collectionID)
	})
	for dbName, dbInfo := range m.collLeader {
		for collectionName, collectionInfo := range dbInfo {
			for collectionID := range collectionInfo {
				if collectionSet.Contain(collectionID) {
					delete(collectionInfo, collectionID)
				}
			}
			if len(collectionInfo) == 0 {
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
