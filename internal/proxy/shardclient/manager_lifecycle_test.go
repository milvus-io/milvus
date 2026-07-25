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
)

func blockingShardLeaderRefresh(
	t *testing.T,
	channel string,
	collectionID int64,
) (types.MixCoordClient, <-chan struct{}, func(), *atomic.Int32) {
	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	var releaseOnce sync.Once
	coordCalls := atomic.NewInt32(0)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			coordCalls.Inc()
			close(rpcStarted)
			select {
			case <-releaseRPC:
				return singleShardResp(channel, collectionID, "new:19530"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Once()

	release := func() {
		releaseOnce.Do(func() { close(releaseRPC) })
	}
	return mixCoord, rpcStarted, release, coordCalls
}

func TestShardCacheDoesNotPingPongBetweenAliasCollectionIDs(t *testing.T) {
	const (
		database        = "test_db"
		alias           = "test_alias"
		channel         = "test_channel"
		oldCollectionID = int64(100)
		newCollectionID = int64(200)
	)

	coordCalls := atomic.NewInt32(0)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			coordCalls.Inc()
			collectionID := req.GetCollectionID()
			return singleShardResp(channel, collectionID, fmt.Sprintf("node-%d:19530", collectionID)), nil
		}).Maybe()

	mgr := NewShardClientMgr(mixCoord)
	for i := 0; i < 10; i++ {
		for _, collectionID := range []int64{oldCollectionID, newCollectionID} {
			nodes, err := mgr.GetShard(context.Background(), true, database, alias, collectionID, channel)
			assert.NoError(t, err)
			if assert.Len(t, nodes, 1) {
				assert.Equal(t, collectionID, nodes[0].NodeID)
			}
		}
	}

	assert.Equal(t, int32(2), coordCalls.Load())
}

func TestShardCacheRefreshKeyIgnoresMutableDatabaseAndName(t *testing.T) {
	const (
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixCoord, rpcStarted, releaseRPC, coordCalls := blockingShardLeaderRefresh(t, channel, collectionID)
	mgr := NewShardClientMgr(mixCoord)
	joined := make(chan struct{}, 2)
	mgr.testHookAfterShardCacheDoChan = func() { joined <- struct{}{} }

	firstDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db_before", "name_before", collectionID, channel)
		firstDone <- err
	}()
	<-rpcStarted
	<-joined

	secondDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db_after", "name_after", collectionID, channel)
		secondDone <- err
	}()
	<-joined
	releaseRPC()

	assert.NoError(t, <-firstDone)
	assert.NoError(t, <-secondDone)
	assert.Equal(t, int32(1), coordCalls.Load(), "database/name changes must not split a collection-ID refresh")
}

func TestShardCacheCoalescesConcurrentRefreshPerCollectionID(t *testing.T) {
	const (
		channel      = "test_channel"
		collectionID = int64(200)
		workers      = 32
	)

	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	coordCalls := atomic.NewInt32(0)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			coordCalls.Inc()
			close(rpcStarted)
			<-releaseRPC
			return singleShardResp(channel, collectionID, "new:19530"), nil
		}).Once()

	mgr := NewShardClientMgr(mixCoord)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			nodes, err := mgr.GetShard(context.Background(), true, "db", "alias", collectionID, channel)
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

func TestShardCacheNewerForcedRefreshWinsWriteRace(t *testing.T) {
	const (
		channel      = "test_channel"
		collectionID = int64(200)
	)

	normalStarted := make(chan struct{})
	forcedStarted := make(chan struct{})
	releaseNormal := make(chan struct{})
	releaseForced := make(chan struct{})
	var releaseNormalOnce sync.Once
	var releaseForcedOnce sync.Once
	defer releaseNormalOnce.Do(func() { close(releaseNormal) })
	defer releaseForcedOnce.Do(func() { close(releaseForced) })

	coordCalls := atomic.NewInt32(0)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			switch coordCalls.Inc() {
			case 1:
				close(normalStarted)
				<-releaseNormal
				return singleShardResp(channel, collectionID, "old:19530"), nil
			case 2:
				close(forcedStarted)
				<-releaseForced
				return singleShardResp(channel, collectionID, "new:19530"), nil
			default:
				return nil, fmt.Errorf("unexpected extra GetShardLeaders call")
			}
		}).Twice()

	type shardResult struct {
		nodes []NodeInfo
		err   error
	}
	mgr := NewShardClientMgr(mixCoord)
	normalDone := make(chan shardResult, 1)
	go func() {
		nodes, err := mgr.GetShard(context.Background(), true, "db", "alias", collectionID, channel)
		normalDone <- shardResult{nodes: nodes, err: err}
	}()
	<-normalStarted

	forcedDone := make(chan shardResult, 1)
	go func() {
		nodes, err := mgr.GetShard(context.Background(), false, "db", "alias", collectionID, channel)
		forcedDone <- shardResult{nodes: nodes, err: err}
	}()
	<-forcedStarted

	releaseForcedOnce.Do(func() { close(releaseForced) })
	forcedResult := <-forcedDone
	assert.NoError(t, forcedResult.err)
	if assert.Len(t, forcedResult.nodes, 1) {
		assert.Equal(t, "new:19530", forcedResult.nodes[0].Address)
	}

	releaseNormalOnce.Do(func() { close(releaseNormal) })
	normalResult := <-normalDone
	assert.NoError(t, normalResult.err)
	if assert.Len(t, normalResult.nodes, 1) {
		assert.Equal(t, "old:19530", normalResult.nodes[0].Address)
	}

	cached := mgr.loadCachedShardLeaders(collectionID)
	if assert.NotNil(t, cached) {
		assert.Equal(t, "new:19530", cached.Get(channel)[0].Address)
	}
	assert.Equal(t, int32(2), coordCalls.Load())
	mgr.leaderMut.RLock()
	assert.Empty(t, mgr.refreshes)
	assert.Empty(t, mgr.refreshWriteSeq)
	mgr.leaderMut.RUnlock()
}

func TestShardCacheLeaderCancellationDoesNotCancelSharedRefresh(t *testing.T) {
	const (
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixCoord, rpcStarted, releaseRPC, coordCalls := blockingShardLeaderRefresh(t, channel, collectionID)
	mgr := NewShardClientMgr(mixCoord)
	joined := make(chan struct{}, 2)
	mgr.testHookAfterShardCacheDoChan = func() { joined <- struct{}{} }

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(leaderCtx, true, "db", "alias", collectionID, channel)
		leaderDone <- err
	}()
	<-rpcStarted
	<-joined

	waiterDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db", "alias", collectionID, channel)
		waiterDone <- err
	}()
	<-joined

	started := time.Now()
	cancelLeader()
	assert.ErrorIs(t, <-leaderDone, context.Canceled)
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
		channel      = "test_channel"
		collectionID = int64(200)
	)

	mixCoord, rpcStarted, releaseRPC, coordCalls := blockingShardLeaderRefresh(t, channel, collectionID)
	mgr := NewShardClientMgr(mixCoord)
	leaderDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db", "alias", collectionID, channel)
		leaderDone <- err
	}()
	<-rpcStarted

	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWaiter()
	started := time.Now()
	_, waiterErr := mgr.GetShard(waiterCtx, true, "db", "alias", collectionID, channel)
	assert.ErrorIs(t, waiterErr, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 200*time.Millisecond)

	releaseRPC()
	assert.NoError(t, <-leaderDone)
	assert.Equal(t, int32(1), coordCalls.Load())
}

func TestShardCacheSharedRefreshHasTimeout(t *testing.T) {
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()

	mgr := NewShardClientMgr(mixCoord)
	mgr.shardCacheRefreshTimeout = 50 * time.Millisecond
	started := time.Now()
	_, err := mgr.GetShard(context.Background(), true, "db", "alias", 200, "channel")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestShardCacheInvalidationFencesInFlightRefresh(t *testing.T) {
	const (
		channel      = "test_channel"
		collectionID = int64(200)
	)

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFirst) })

	coordCalls := atomic.NewInt32(0)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *querypb.GetShardLeadersRequest, _ ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			switch coordCalls.Inc() {
			case 1:
				close(firstStarted)
				<-releaseFirst
				return singleShardResp(channel, collectionID, "old:19530"), nil
			case 2:
				close(secondStarted)
				return singleShardResp(channel, collectionID, "new:19530"), nil
			default:
				return nil, fmt.Errorf("unexpected extra GetShardLeaders call")
			}
		}).Twice()

	mgr := NewShardClientMgr(mixCoord)
	firstDone := make(chan []NodeInfo, 1)
	go func() {
		nodes, err := mgr.GetShard(context.Background(), true, "db", "alias", collectionID, channel)
		assert.NoError(t, err)
		firstDone <- nodes
	}()
	<-firstStarted

	mgr.InvalidateShardLeaderCache([]int64{collectionID})
	secondDone := make(chan []NodeInfo, 1)
	go func() {
		nodes, err := mgr.GetShard(context.Background(), true, "db", "alias", collectionID, channel)
		assert.NoError(t, err)
		secondDone <- nodes
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("post-invalidation request joined the pre-invalidation refresh")
	}
	assert.Equal(t, "new:19530", (<-secondDone)[0].Address)

	releaseOnce.Do(func() { close(releaseFirst) })
	assert.Equal(t, "old:19530", (<-firstDone)[0].Address)
	cached := mgr.loadCachedShardLeaders(collectionID)
	assert.NotNil(t, cached)
	assert.Equal(t, "new:19530", cached.Get(channel)[0].Address)
	assert.Equal(t, int32(2), coordCalls.Load())
	mgr.leaderMut.RLock()
	assert.Empty(t, mgr.refreshes)
	mgr.leaderMut.RUnlock()
}

func TestInvalidateShardLeaderCacheRevokesRefreshTokensByID(t *testing.T) {
	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.collLeader.Insert(100, &shardLeaders{})
	mgr.acquireShardCacheRefresh(shardCacheRefreshKey{collectionID: 100})
	mgr.acquireShardCacheRefresh(shardCacheRefreshKey{collectionID: 100, force: true})
	mgr.acquireShardCacheRefresh(shardCacheRefreshKey{collectionID: 200})

	mgr.InvalidateShardLeaderCache([]int64{100})

	mgr.leaderMut.RLock()
	defer mgr.leaderMut.RUnlock()
	assert.False(t, mgr.collLeader.Contain(100))
	assert.NotContains(t, mgr.refreshes, shardCacheRefreshKey{collectionID: 100})
	assert.NotContains(t, mgr.refreshes, shardCacheRefreshKey{collectionID: 100, force: true})
	assert.Contains(t, mgr.refreshes, shardCacheRefreshKey{collectionID: 200})
	assert.NotContains(t, mgr.refreshWriteSeq, int64(100))
	assert.Contains(t, mgr.refreshWriteSeq, int64(200))
}

func TestListShardLocationDropsIdleEntries(t *testing.T) {
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
	oldLeaders.touch(time.Now().Add(-2 * time.Minute))
	newLeaders.touch(time.Now())

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.shardCacheTTL = time.Minute
	mgr.collLeader.Insert(100, oldLeaders)
	mgr.collLeader.Insert(200, newLeaders)

	locations := mgr.ListShardLocation()
	assert.NotContains(t, locations, int64(1))
	assert.Contains(t, locations, int64(2))
	assert.False(t, mgr.collLeader.Contain(100))
	assert.True(t, mgr.collLeader.Contain(200))
}

func TestListShardLocationAllowsConcurrentCacheHits(t *testing.T) {
	const collectionID = int64(200)
	leaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: collectionID,
		shardLeaders: map[string][]NodeInfo{"channel": {{NodeID: 1, Address: "node:19530", Serviceable: true}}},
	}
	leaders.touch(time.Now())

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.collLeader.Insert(collectionID, leaders)
	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseScan) })
	mgr.testHookListShardLocationScan = func() {
		close(scanStarted)
		<-releaseScan
	}

	listDone := make(chan struct{})
	go func() {
		defer close(listDone)
		mgr.ListShardLocation()
	}()
	<-scanStarted

	hitDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db", "name", collectionID, "channel")
		hitDone <- err
	}()
	select {
	case err := <-hitDone:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cache hit was blocked by ListShardLocation scan")
	}

	releaseOnce.Do(func() { close(releaseScan) })
	<-listDone
}

func TestListShardLocationScanDoesNotBlockInvalidation(t *testing.T) {
	leaders100 := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: 100,
		shardLeaders: map[string][]NodeInfo{"channel-100": {{NodeID: 1, Address: "node-1:19530", Serviceable: true}}},
	}
	leaders200 := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: 200,
		shardLeaders: map[string][]NodeInfo{"channel-200": {{NodeID: 2, Address: "node-2:19530", Serviceable: true}}},
	}
	leaders100.touch(time.Now())
	leaders200.touch(time.Now())

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.collLeader.Insert(100, leaders100)
	mgr.collLeader.Insert(200, leaders200)
	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseScan) })
	mgr.testHookListShardLocationScan = func() {
		close(scanStarted)
		<-releaseScan
	}

	listDone := make(chan struct{})
	go func() {
		defer close(listDone)
		mgr.ListShardLocation()
	}()
	<-scanStarted

	invalidationDone := make(chan struct{})
	go func() {
		defer close(invalidationDone)
		mgr.InvalidateShardLeaderCache([]int64{100})
	}()
	select {
	case <-invalidationDone:
	case <-time.After(time.Second):
		t.Fatal("shard cache scan blocked an unrelated invalidation")
	}

	hitDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db", "name", 200, "channel-200")
		hitDone <- err
	}()
	select {
	case err := <-hitDone:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cache hit was blocked while the shard cache scan was paused")
	}

	releaseOnce.Do(func() { close(releaseScan) })
	<-listDone
}

func TestShardCacheHitTouchesBeforeIdleDelete(t *testing.T) {
	const collectionID = int64(200)
	leaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: collectionID,
		shardLeaders: map[string][]NodeInfo{"channel": {{NodeID: 1, Address: "node:19530", Serviceable: true}}},
	}
	leaders.touch(time.Now().Add(-2 * time.Minute))

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.shardCacheTTL = time.Minute
	mgr.collLeader.Insert(collectionID, leaders)
	cacheLoaded := make(chan struct{})
	releaseHit := make(chan struct{})
	var releaseHitOnce sync.Once
	defer releaseHitOnce.Do(func() { close(releaseHit) })
	mgr.testHookAfterShardCacheLoad = func() {
		close(cacheLoaded)
		<-releaseHit
	}

	hitDone := make(chan error, 1)
	go func() {
		_, err := mgr.GetShard(context.Background(), true, "db", "name", collectionID, "channel")
		hitDone <- err
	}()
	<-cacheLoaded

	beforeDelete := make(chan struct{})
	mgr.testHookBeforeShardLocationDelete = func() { close(beforeDelete) }
	locationsDone := make(chan map[int64]NodeInfo, 1)
	go func() { locationsDone <- mgr.ListShardLocation() }()
	<-beforeDelete

	releaseHitOnce.Do(func() { close(releaseHit) })
	assert.NoError(t, <-hitDone)
	locations := <-locationsDone
	assert.Contains(t, locations, int64(1))
	assert.Same(t, leaders, mgr.loadCachedShardLeaders(collectionID))
}

func TestListShardLocationRechecksExpiredCandidateBeforeDelete(t *testing.T) {
	const collectionID = int64(200)
	leaders := &shardLeaders{
		idx:          atomic.NewInt64(0),
		collectionID: collectionID,
		shardLeaders: map[string][]NodeInfo{"channel": {{NodeID: 1, Address: "node:19530", Serviceable: true}}},
	}
	leaders.touch(time.Now().Add(-2 * time.Minute))

	mgr := NewShardClientMgr(mocks.NewMockMixCoordClient(t))
	mgr.shardCacheTTL = time.Minute
	mgr.collLeader.Insert(collectionID, leaders)
	beforeDelete := make(chan struct{})
	resumeDelete := make(chan struct{})
	var resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resumeDelete) })
	mgr.testHookBeforeShardLocationDelete = func() {
		close(beforeDelete)
		<-resumeDelete
	}

	locationsDone := make(chan map[int64]NodeInfo, 1)
	go func() { locationsDone <- mgr.ListShardLocation() }()
	<-beforeDelete

	_, err := mgr.GetShard(context.Background(), true, "db", "name", collectionID, "channel")
	assert.NoError(t, err)
	resumeOnce.Do(func() { close(resumeDelete) })

	locations := <-locationsDone
	assert.Contains(t, locations, int64(1))
	assert.Same(t, leaders, mgr.loadCachedShardLeaders(collectionID))
}
