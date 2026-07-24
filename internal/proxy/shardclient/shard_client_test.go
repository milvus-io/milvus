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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func testShardLeaderCache(entries map[string]*shardLeaders) shardLeadersByCollectionName {
	cache := make(shardLeadersByCollectionName, len(entries))
	for name, leaders := range entries {
		cache[name] = shardLeadersByCollectionID{leaders.collectionID: leaders}
	}
	return cache
}

func shardLeaderResponse(channel string, collectionID int64, address string) *querypb.GetShardLeadersResponse {
	return &querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{{
			ChannelName: channel,
			NodeIds:     []int64{collectionID},
			NodeAddrs:   []string{address},
			Serviceable: []bool{true},
		}},
	}
}

func blockingShardLeaderRefresh(
	t *testing.T,
	channel string,
	collectionID int64,
) (types.MixCoordClient, <-chan struct{}, func(), *atomic.Int32) {
	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	var releaseOnce sync.Once
	coordCalls := atomic.NewInt32(0)
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			coordCalls.Inc()
			close(rpcStarted)
			select {
			case <-releaseRPC:
				return shardLeaderResponse(channel, collectionID, "new:19530"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Once()

	release := func() {
		releaseOnce.Do(func() { close(releaseRPC) })
	}
	return mixcoord, rpcStarted, release, coordCalls
}

func TestShardClientMgr(t *testing.T) {
	ctx := context.Background()
	nodeInfo := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil)
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mixcoord := mocks.NewMockMixCoordClient(t)

	mgr := NewShardClientMgr(mixcoord)
	mgr.SetClientCreatorFunc(creator)
	_, err := mgr.GetClient(ctx, nodeInfo)
	assert.Nil(t, err)

	mgr.Close()
	assert.Equal(t, mgr.clients.Len(), 0)
}

func TestGetShardRefreshesAliasWhenCachedCollectionIDChanges(t *testing.T) {
	const (
		database        = "test_db"
		alias           = "test_alias"
		channel         = "test_channel"
		oldCollectionID = int64(100)
		newCollectionID = int64(200)
	)

	oldNode := NodeInfo{NodeID: 1, Address: "old:19530", Serviceable: true}
	newNode := NodeInfo{NodeID: 2, Address: "new:19530", Serviceable: true}

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.MatchedBy(func(req *querypb.GetShardLeadersRequest) bool {
		return req.GetCollectionID() == newCollectionID
	})).Return(&querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: channel,
				NodeIds:     []int64{newNode.NodeID},
				NodeAddrs:   []string{newNode.Address},
				Serviceable: []bool{newNode.Serviceable},
			},
		},
	}, nil).Once()

	mgr := NewShardClientMgr(mixcoord)
	mgr.collLeader[database] = testShardLeaderCache(map[string]*shardLeaders{
		alias: {
			idx:          atomic.NewInt64(0),
			collectionID: oldCollectionID,
			shardLeaders: map[string][]NodeInfo{
				channel: {oldNode},
			},
		},
	})

	nodes, err := mgr.GetShard(context.Background(), true, database, alias, newCollectionID, channel)
	assert.NoError(t, err)
	assert.Equal(t, []NodeInfo{newNode}, nodes)
}

func TestGetShardLeaderListRefreshesAliasWhenCachedCollectionIDChanges(t *testing.T) {
	const (
		database        = "test_db"
		alias           = "test_alias"
		oldChannel      = "old_channel"
		newChannel      = "new_channel"
		oldCollectionID = int64(100)
		newCollectionID = int64(200)
	)

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.MatchedBy(func(req *querypb.GetShardLeadersRequest) bool {
		return req.GetCollectionID() == newCollectionID
	})).Return(&querypb.GetShardLeadersResponse{
		Status: merr.Success(),
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: newChannel,
				NodeIds:     []int64{2},
				NodeAddrs:   []string{"new:19530"},
				Serviceable: []bool{true},
			},
		},
	}, nil).Once()

	mgr := NewShardClientMgr(mixcoord)
	mgr.collLeader[database] = testShardLeaderCache(map[string]*shardLeaders{
		alias: {
			idx:          atomic.NewInt64(0),
			collectionID: oldCollectionID,
			shardLeaders: map[string][]NodeInfo{
				oldChannel: {{NodeID: 1, Address: "old:19530", Serviceable: true}},
			},
		},
	})

	channels, err := mgr.GetShardLeaderList(context.Background(), database, alias, newCollectionID, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{newChannel}, channels)
}

func TestShardCacheDoesNotPingPongBetweenAliasCollectionIDs(t *testing.T) {
	const (
		database        = "test_db"
		alias           = "test_alias"
		channel         = "test_channel"
		oldCollectionID = int64(100)
		newCollectionID = int64(200)
	)

	var coordCalls atomic.Int32
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			coordCalls.Inc()
			collectionID := req.GetCollectionID()
			return &querypb.GetShardLeadersResponse{
				Status: merr.Success(),
				Shards: []*querypb.ShardLeadersList{
					{
						ChannelName: channel,
						NodeIds:     []int64{collectionID},
						NodeAddrs:   []string{fmt.Sprintf("node-%d:19530", collectionID)},
						Serviceable: []bool{true},
					},
				},
			}, nil
		}).Maybe()

	mgr := NewShardClientMgr(mixcoord)
	for i := 0; i < 10; i++ {
		for _, collectionID := range []int64{oldCollectionID, newCollectionID} {
			nodes, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
			assert.NoError(t, err)
			assert.Equal(t, []NodeInfo{{
				NodeID:      collectionID,
				Address:     fmt.Sprintf("node-%d:19530", collectionID),
				Serviceable: true,
			}}, nodes)
		}
	}

	assert.Equal(t, int32(2), coordCalls.Load(), "each collection ID should be fetched at most once")
}

func TestShardCacheCoalescesConcurrentRefreshPerCollectionID(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		channel      = "test_channel"
		collectionID = int64(200)
		workers      = 32
	)

	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	var startOnce sync.Once
	var coordCalls atomic.Int32
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.MatchedBy(func(req *querypb.GetShardLeadersRequest) bool {
		return req.GetCollectionID() == collectionID
	})).RunAndReturn(func(_ context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
		coordCalls.Inc()
		startOnce.Do(func() { close(rpcStarted) })
		<-releaseRPC
		return &querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: channel,
					NodeIds:     []int64{collectionID},
					NodeAddrs:   []string{"new:19530"},
					Serviceable: []bool{true},
				},
			},
		}, nil
	}).Once()

	mgr := NewShardClientMgr(mixcoord)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			nodes, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
			assert.NoError(t, err)
			if assert.Len(t, nodes, 1) {
				assert.Equal(t, collectionID, nodes[0].NodeID)
			}
		}()
	}

	close(start)
	<-rpcStarted
	close(releaseRPC)
	wg.Wait()
	assert.Equal(t, int32(1), coordCalls.Load())
}

func TestShardCacheLeaderCancellationDoesNotCancelSharedRefresh(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixcoord, rpcStarted, releaseRPC, coordCalls := blockingShardLeaderRefresh(t, channel, collectionID)

	mgr := NewShardClientMgr(mixcoord)
	joined := make(chan struct{}, 2)
	mgr.testHookAfterShardCacheDoChan = func() {
		joined <- struct{}{}
	}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(leaderCtx, true, database, alias, collectionID, channel)
		leaderDone <- err
	}()
	<-rpcStarted
	<-joined

	waiterDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
		waiterDone <- err
	}()
	<-joined

	started := time.Now()
	cancelLeader()
	leaderErr := <-leaderDone
	assert.ErrorIs(t, leaderErr, context.Canceled)
	assert.Less(t, time.Since(started), 200*time.Millisecond)

	select {
	case err := <-waiterDone:
		t.Fatalf("shared refresh ended after leader cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseRPC()
	assert.NoError(t, <-waiterDone)
	assert.Equal(t, int32(1), coordCalls.Load())
}

func TestShardCacheWaiterHonorsOwnDeadline(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixcoord, rpcStarted, releaseRPC, coordCalls := blockingShardLeaderRefresh(t, channel, collectionID)

	mgr := NewShardClientMgr(mixcoord)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
		leaderDone <- err
	}()
	<-rpcStarted

	go func() {
		time.Sleep(300 * time.Millisecond)
		releaseRPC()
	}()
	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWaiter()
	started := time.Now()
	_, waiterErr := mgr.GetShard(waiterCtx, true, database, alias, collectionID, channel)
	assert.ErrorIs(t, waiterErr, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 200*time.Millisecond)

	releaseRPC()
	assert.NoError(t, <-leaderDone)
	assert.Equal(t, int32(1), coordCalls.Load())
}

func TestShardCacheSharedRefreshHasTimeout(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()

	mgr := NewShardClientMgr(mixcoord)
	mgr.shardCacheRefreshTimeout = 50 * time.Millisecond
	started := time.Now()
	_, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestShardCacheInvalidationFencesInFlightRefresh(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		channel      = "test_channel"
		collectionID = int64(200)
	)

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFirst) })

	coordCalls := atomic.NewInt32(0)
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			switch coordCalls.Inc() {
			case 1:
				close(firstStarted)
				<-releaseFirst
				return shardLeaderResponse(channel, collectionID, "old:19530"), nil
			case 2:
				close(secondStarted)
				return shardLeaderResponse(channel, collectionID, "new:19530"), nil
			default:
				return nil, fmt.Errorf("unexpected extra GetShardLeaders call")
			}
		}).Twice()

	mgr := NewShardClientMgr(mixcoord)
	firstDone := make(chan []NodeInfo, 1)
	go func() {
		nodes, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
		assert.NoError(t, err)
		firstDone <- nodes
	}()
	<-firstStarted

	mgr.InvalidateShardLeaderCache([]int64{collectionID})
	secondDone := make(chan []NodeInfo, 1)
	go func() {
		nodes, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
		assert.NoError(t, err)
		secondDone <- nodes
	}()

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("post-invalidation request joined the pre-invalidation refresh")
	}
	secondNodes := <-secondDone
	if assert.Len(t, secondNodes, 1) {
		assert.Equal(t, "new:19530", secondNodes[0].Address)
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	firstNodes := <-firstDone
	if assert.Len(t, firstNodes, 1) {
		assert.Equal(t, "old:19530", firstNodes[0].Address)
	}

	cached := mgr.loadCachedShardLeaders(database, alias, collectionID)
	if assert.NotNil(t, cached) {
		nodes := cached.Get(channel)
		if assert.Len(t, nodes, 1) {
			assert.Equal(t, "new:19530", nodes[0].Address, "invalidated refresh must not overwrite the new cache entry")
		}
	}
	assert.Equal(t, int32(2), coordCalls.Load())
	mgr.leaderMut.RLock()
	assert.Empty(t, mgr.refreshes)
	mgr.leaderMut.RUnlock()
}

func TestDeprecateShardCacheFencesInFlightRefresh(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixcoord, rpcStarted, releaseRPC, coordCalls := blockingShardLeaderRefresh(t, channel, collectionID)
	mgr := NewShardClientMgr(mixcoord)
	requestDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
		requestDone <- err
	}()
	<-rpcStarted

	mgr.DeprecateShardCache(database, alias)
	releaseRPC()
	assert.NoError(t, <-requestDone)
	assert.Nil(t, mgr.loadCachedShardLeaders(database, alias, collectionID))
	assert.Equal(t, int32(1), coordCalls.Load())
	mgr.leaderMut.RLock()
	assert.Empty(t, mgr.refreshes)
	mgr.leaderMut.RUnlock()
}

func TestShardCacheBoundsHistoricalVersionsPerName(t *testing.T) {
	const (
		database = "test_db"
		alias    = "test_alias"
		channel  = "test_channel"
	)

	var coordCalls atomic.Int32
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			coordCalls.Inc()
			collectionID := req.GetCollectionID()
			return shardLeaderResponse(channel, collectionID, fmt.Sprintf("node-%d:19530", collectionID)), nil
		}).Maybe()

	mgr := NewShardClientMgr(mixcoord)
	totalVersions := defaultMaxShardCacheVersionsPerName + 2
	for collectionID := int64(1); collectionID <= int64(totalVersions); collectionID++ {
		_, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
		assert.NoError(t, err)
	}

	mgr.leaderMut.RLock()
	versions := mgr.collLeader[database][alias]
	assert.Len(t, versions, defaultMaxShardCacheVersionsPerName)
	assert.NotContains(t, versions, int64(1))
	assert.NotContains(t, versions, int64(2))
	assert.Contains(t, versions, int64(totalVersions))
	mgr.leaderMut.RUnlock()

	locations := mgr.ListShardLocation()
	assert.NotContains(t, locations, int64(1))
	assert.NotContains(t, locations, int64(2))
	assert.Contains(t, locations, int64(totalVersions))
	assert.Equal(t, int32(totalVersions), coordCalls.Load())
}

func TestListShardLocationDropsIdleHistoricalVersions(t *testing.T) {
	const (
		database = "test_db"
		alias    = "test_alias"
	)

	oldLeaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: 100,
		shardLeaders: map[string][]NodeInfo{"old-channel": {{NodeID: 1, Address: "old:19530", Serviceable: true}}},
	}
	newLeaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: 200,
		shardLeaders: map[string][]NodeInfo{"new-channel": {{NodeID: 2, Address: "new:19530", Serviceable: true}}},
	}
	now := time.Now()
	oldLeaders.touch(now.Add(-2 * time.Minute))
	newLeaders.touch(now)

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.shardCacheVersionTTL = time.Minute
	mgr.collLeader[database] = shardLeadersByCollectionName{
		alias: {100: oldLeaders, 200: newLeaders},
	}

	locations := mgr.ListShardLocation()
	assert.NotContains(t, locations, int64(1), "idle historical leaders must not keep an old QueryNode client alive")
	assert.Contains(t, locations, int64(2))

	mgr.leaderMut.RLock()
	versions := mgr.collLeader[database][alias]
	assert.NotContains(t, versions, int64(100))
	assert.Contains(t, versions, int64(200))
	mgr.leaderMut.RUnlock()
}

func TestListShardLocationAllowsConcurrentCacheHits(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		collectionID = int64(200)
	)
	leaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: collectionID,
		shardLeaders: map[string][]NodeInfo{"channel": {{NodeID: 1, Address: "node:19530", Serviceable: true}}},
	}
	leaders.touch(time.Now())

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.collLeader[database] = shardLeadersByCollectionName{alias: {collectionID: leaders}}
	readLocked := make(chan struct{})
	releaseScan := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseScan) })
	mgr.testHookListShardLocationReadLocked = func() {
		close(readLocked)
		<-releaseScan
	}

	listDone := make(chan struct{})
	go func() {
		defer close(listDone)
		mgr.ListShardLocation()
	}()
	<-readLocked

	hitDone := make(chan *shardLeaders, 1)
	go func() {
		hitDone <- mgr.loadCachedShardLeaders(database, alias, collectionID)
	}()
	select {
	case cached := <-hitDone:
		assert.Same(t, leaders, cached)
	case <-time.After(time.Second):
		t.Fatal("cache hit was blocked by ListShardLocation scan")
	}

	releaseOnce.Do(func() { close(releaseScan) })
	<-listDone
}

func TestListShardLocationRechecksExpiredCandidateBeforeDelete(t *testing.T) {
	const (
		database     = "test_db"
		alias        = "test_alias"
		collectionID = int64(200)
	)
	leaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: collectionID,
		shardLeaders: map[string][]NodeInfo{"channel": {{NodeID: 1, Address: "node:19530", Serviceable: true}}},
	}
	leaders.touch(time.Now().Add(-2 * time.Minute))

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.shardCacheVersionTTL = time.Minute
	mgr.collLeader[database] = shardLeadersByCollectionName{alias: {collectionID: leaders}}
	beforeDelete := make(chan struct{})
	resumeDelete := make(chan struct{})
	var resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resumeDelete) })
	mgr.testHookBeforeShardLocationDelete = func() {
		close(beforeDelete)
		<-resumeDelete
	}

	locationsDone := make(chan map[int64]NodeInfo, 1)
	go func() {
		locationsDone <- mgr.ListShardLocation()
	}()
	<-beforeDelete
	leaders.touch(time.Now())
	resumeOnce.Do(func() { close(resumeDelete) })

	locations := <-locationsDone
	assert.Contains(t, locations, int64(1))
	assert.Same(t, leaders, mgr.loadCachedShardLeaders(database, alias, collectionID))
}

func TestShardClient(t *testing.T) {
	nodeInfo := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}
	shardClient := newShardClient(nodeInfo, creator, 3*time.Second)
	assert.Equal(t, len(shardClient.clients), 0)
	assert.Equal(t, false, shardClient.initialized.Load())
	assert.Equal(t, false, shardClient.isClosed)

	ctx := context.Background()
	_, err := shardClient.getClient(ctx)
	assert.Nil(t, err)
	assert.Equal(t, len(shardClient.clients), paramtable.Get().ProxyCfg.QueryNodePoolingSize.GetAsInt())

	// test close
	closed := shardClient.Close(false)
	assert.False(t, closed)
	closed = shardClient.Close(true)
	assert.True(t, closed)
}

func TestPurgeClient(t *testing.T) {
	node := NodeInfo{
		NodeID: 1,
	}

	returnEmptyResult := atomic.NewBool(false)

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	s := &shardClientMgrImpl{
		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
		clientCreator:   creator,
		closeCh:         make(chan struct{}),
		purgeInterval:   1 * time.Second,
		expiredDuration: 3 * time.Second,

		collLeader: map[string]shardLeadersByCollectionName{
			"default": testShardLeaderCache(map[string]*shardLeaders{
				"test": {
					idx:          atomic.NewInt64(0),
					collectionID: 1,
					shardLeaders: map[string][]NodeInfo{
						"0": {node},
					},
				},
			}),
		},
	}

	go s.PurgeClient()
	defer s.Close()
	_, err := s.GetClient(context.Background(), node)
	assert.Nil(t, err)
	qnClient, ok := s.clients.Get(1)
	assert.True(t, ok)
	assert.True(t, qnClient.lastActiveTs.Load() > 0)

	time.Sleep(2 * time.Second)
	// expected client should not been purged before expiredDuration
	assert.Equal(t, s.clients.Len(), 1)
	assert.True(t, time.Now().UnixNano()-qnClient.lastActiveTs.Load() >= 2*time.Second.Nanoseconds())

	_, err = s.GetClient(context.Background(), node)
	assert.Nil(t, err)
	time.Sleep(2 * time.Second)
	// GetClient should refresh lastActiveTs, expected client should not be purged
	assert.Equal(t, s.clients.Len(), 1)
	assert.True(t, time.Now().UnixNano()-qnClient.lastActiveTs.Load() < 3*time.Second.Nanoseconds())

	time.Sleep(2 * time.Second)
	// client reach the expiredDuration, expected client should not be purged
	assert.Equal(t, s.clients.Len(), 1)
	assert.True(t, time.Now().UnixNano()-qnClient.lastActiveTs.Load() > 3*time.Second.Nanoseconds())

	s.DeprecateShardCache("default", "test")
	returnEmptyResult.Store(true)
	time.Sleep(2 * time.Second)
	// remove client from shard location, expected client should be purged
	assert.Eventually(t, func() bool {
		return s.clients.Len() == 0
	}, 10*time.Second, 1*time.Second)
}

func TestDeprecateShardCache(t *testing.T) {
	node := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mixcoord := mocks.NewMockMixCoordClient(t)
	mgr := NewShardClientMgr(mixcoord)
	mgr.SetClientCreatorFunc(creator)

	t.Run("Clear with no collection info", func(t *testing.T) {
		mgr.DeprecateShardCache("default", "collection_not_exist")
		// Should not panic or error
	})

	t.Run("Clear valid collection empty cache", func(t *testing.T) {
		// Add a collection to cache first
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"default": testShardLeaderCache(map[string]*shardLeaders{
				"test_collection": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.DeprecateShardCache("default", "test_collection")

		// Verify cache is cleared
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["test_collection"]
		assert.False(t, exists)
	})

	t.Run("Clear one collection, keep others", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"default": testShardLeaderCache(map[string]*shardLeaders{
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 101,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.DeprecateShardCache("default", "collection1")

		// Verify collection1 is cleared but collection2 remains
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.False(t, exists)
		_, exists = mgr.collLeader["default"]["collection2"]
		assert.True(t, exists)
	})

	t.Run("Clear last collection in database removes database", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"test_db": testShardLeaderCache(map[string]*shardLeaders{
				"last_collection": {
					idx:          atomic.NewInt64(0),
					collectionID: 200,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.DeprecateShardCache("test_db", "last_collection")

		// Verify database is also removed
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["test_db"]
		assert.False(t, exists)
	})

	mgr.Close()
}

func TestInvalidateShardLeaderCache(t *testing.T) {
	node := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mixcoord := mocks.NewMockMixCoordClient(t)
	mgr := NewShardClientMgr(mixcoord)
	mgr.SetClientCreatorFunc(creator)

	t.Run("Invalidate one ID keeps another ID under the same alias", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"default": {
				"test_alias": {
					100: {
						idx:          atomic.NewInt64(0),
						collectionID: 100,
						shardLeaders: map[string][]NodeInfo{"old-channel": {node}},
					},
					200: {
						idx:          atomic.NewInt64(0),
						collectionID: 200,
						shardLeaders: map[string][]NodeInfo{"new-channel": {node}},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100})

		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		versions := mgr.collLeader["default"]["test_alias"]
		assert.NotContains(t, versions, int64(100))
		assert.Contains(t, versions, int64(200))
	})

	t.Run("Invalidate single collection", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"default": testShardLeaderCache(map[string]*shardLeaders{
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 101,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100})

		// Verify collection with ID 100 is removed, but 101 remains
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.False(t, exists)
		_, exists = mgr.collLeader["default"]["collection2"]
		assert.True(t, exists)
	})

	t.Run("Invalidate multiple collections", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"default": testShardLeaderCache(map[string]*shardLeaders{
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 101,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
				"collection3": {
					idx:          atomic.NewInt64(0),
					collectionID: 102,
					shardLeaders: map[string][]NodeInfo{
						"channel-3": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100, 102})

		// Verify collections 100 and 102 are removed, but 101 remains
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.False(t, exists)
		_, exists = mgr.collLeader["default"]["collection2"]
		assert.True(t, exists)
		_, exists = mgr.collLeader["default"]["collection3"]
		assert.False(t, exists)
	})

	t.Run("Invalidate non-existent collection", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"default": testShardLeaderCache(map[string]*shardLeaders{
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{999})

		// Verify collection1 still exists
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.True(t, exists)
	})

	t.Run("Invalidate all collections in database removes database", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"test_db": testShardLeaderCache(map[string]*shardLeaders{
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 200,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 201,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{200, 201})

		// Verify database is removed
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["test_db"]
		assert.False(t, exists)
	})

	t.Run("Invalidate across multiple databases", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]shardLeadersByCollectionName{
			"db1": testShardLeaderCache(map[string]*shardLeaders{
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			}),
			"db2": testShardLeaderCache(map[string]*shardLeaders{
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 100, // Same collection ID in different database
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			}),
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100})

		// Verify collection is removed from both databases
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["db1"]
		assert.False(t, exists) // db1 should be removed
		_, exists = mgr.collLeader["db2"]
		assert.False(t, exists) // db2 should be removed
	})

	mgr.Close()
}

func TestShuffleShardLeaders(t *testing.T) {
	t.Run("Shuffle with multiple nodes", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
				{NodeID: 2, Address: "localhost:9001", Serviceable: true},
				{NodeID: 3, Address: "localhost:9002", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(5),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		// Verify result has same channel
		assert.Len(t, result, 1)
		assert.Contains(t, result, "channel-1")

		// Verify all nodes are present
		assert.Len(t, result["channel-1"], 3)

		// Verify the first node is based on idx rotation (idx=6, 6%3=0, so nodeID 1 should be first)
		assert.Equal(t, int64(1), result["channel-1"][0].NodeID)

		// Verify all nodes are still present (shuffled)
		nodeIDs := make(map[int64]bool)
		for _, node := range result["channel-1"] {
			nodeIDs[node.NodeID] = true
		}
		assert.True(t, nodeIDs[1])
		assert.True(t, nodeIDs[2])
		assert.True(t, nodeIDs[3])
	})

	t.Run("Shuffle rotates first replica based on idx", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
				{NodeID: 2, Address: "localhost:9001", Serviceable: true},
				{NodeID: 3, Address: "localhost:9002", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(5),
			collectionID: 100,
			shardLeaders: shards,
		}

		// First read, idx will be 6 (5+1), 6%3=0, so first replica should be leaders[0] which is nodeID 1
		reader := sl.GetReader()
		result := reader.Shuffle()
		assert.Equal(t, int64(1), result["channel-1"][0].NodeID)

		// Second read, idx will be 7 (6+1), 7%3=1, so first replica should be leaders[1] which is nodeID 2
		reader = sl.GetReader()
		result = reader.Shuffle()
		assert.Equal(t, int64(2), result["channel-1"][0].NodeID)

		// Third read, idx will be 8 (7+1), 8%3=2, so first replica should be leaders[2] which is nodeID 3
		reader = sl.GetReader()
		result = reader.Shuffle()
		assert.Equal(t, int64(3), result["channel-1"][0].NodeID)
	})

	t.Run("Shuffle with single node", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(0),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		assert.Len(t, result["channel-1"], 1)
		assert.Equal(t, int64(1), result["channel-1"][0].NodeID)
	})

	t.Run("Shuffle with multiple channels", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
				{NodeID: 2, Address: "localhost:9001", Serviceable: true},
			},
			"channel-2": {
				{NodeID: 3, Address: "localhost:9002", Serviceable: true},
				{NodeID: 4, Address: "localhost:9003", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(0),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		// Verify both channels are present
		assert.Len(t, result, 2)
		assert.Contains(t, result, "channel-1")
		assert.Contains(t, result, "channel-2")

		// Verify each channel has correct number of nodes
		assert.Len(t, result["channel-1"], 2)
		assert.Len(t, result["channel-2"], 2)
	})

	t.Run("Shuffle with empty leaders", func(t *testing.T) {
		shards := map[string][]NodeInfo{}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(0),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		assert.Len(t, result, 0)
	})
}

// func BenchmarkShardClientMgr(b *testing.B) {
// 	node := nodeInfo{
// 		nodeID: 1,
// 	}
// 	cache := NewMockCache(b)
// 	cache.EXPECT().ListShardLocation().Return(map[int64]nodeInfo{
// 		1: node,
// 	}).Maybe()
// 	globalMetaCache = cache
// 	qn := mocks.NewMockQueryNodeClient(b)
// 	qn.EXPECT().Close().Return(nil).Maybe()

// 	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
// 		return qn, nil
// 	}
// 	s := &shardClientMgrImpl{
// 		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
// 		clientCreator:   creator,
// 		closeCh:         make(chan struct{}),
// 		purgeInterval:   1 * time.Second,
// 		expiredDuration: 10 * time.Second,
// 	}
// 	go s.PurgeClient()
// 	defer s.Close()

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			_, err := s.GetClient(context.Background(), node)
// 			assert.Nil(b, err)
// 		}
// 	})
// }
