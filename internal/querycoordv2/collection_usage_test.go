// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const usageTestVChannel = "by-dev-rootcoord-dml_100v0"

func enableAutoReleaseForTest(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoReleaseIdleTTLSeconds.Key, "10"))
	require.NoError(t, params.Save(params.ProxyCfg.EnableAutoLoad.Key, "true"))
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoReleaseEnabled.Key, "true"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryCoordCfg.AutoReleaseIdleTTLSeconds.Key))
		require.NoError(t, params.Reset(params.QueryCoordCfg.AutoReleaseEnabled.Key))
		require.NoError(t, params.Reset(params.ProxyCfg.EnableAutoLoad.Key))
	})
}

func newUsageTestManager(t *testing.T) (*Server, *collectionUsageManager, *metastoremocks.QueryCoordCatalog, *int) {
	t.Helper()
	s, catalog, _ := newReadinessTestServer(t, testPersistedQueryView(100, qviews.ShardID{
		ReplicaID: 1000,
		VChannel:  usageTestVChannel,
	}))
	releaseCount := 0
	manager := s.collectionUsage
	manager.release = func(context.Context, int64) error {
		releaseCount++
		return nil
	}
	return s, manager, catalog, &releaseCount
}

func activityTime(manager *collectionUsageManager, collectionID int64) (time.Time, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	lastActiveAt, exists := manager.lastActiveAt[collectionID]
	return lastActiveAt, exists
}

func setActivityTime(manager *collectionUsageManager, collectionID int64, at time.Time) {
	manager.mu.Lock()
	manager.lastActiveAt[collectionID] = at
	manager.mu.Unlock()
}

func TestCollectionUsageRefreshesOnEveryDQLAttempt(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, _, releaseCount := newUsageTestManager(t)
	manager.scan(context.Background())
	_, exists := activityTime(manager, 100)
	require.True(t, exists)

	// No request ID or Proxy identity is needed. Repeated attempts and requests
	// from different Proxies all refresh the same collection timestamp.
	req := readinessRequest(usageTestVChannel)
	for i := 0; i < 3; i++ {
		before := time.Now()
		status, err := s.EnsureCollectionReady(context.Background(), req)
		require.NoError(t, merr.CheckRPCCall(status, err))
		lastActiveAt, _ := activityTime(manager, 100)
		require.False(t, lastActiveAt.Before(before))
		manager.scan(context.Background())
		require.Zero(t, *releaseCount)
	}

	// There is no end notification. A query which is still running after TTL
	// does not prevent release and must use the existing DQL retry path.
	setActivityTime(manager, 100, time.Now().Add(-9*time.Second))
	manager.scan(context.Background())
	require.Zero(t, *releaseCount)
	setActivityTime(manager, 100, time.Now().Add(-10*time.Second))
	manager.scan(context.Background())
	require.Equal(t, 1, *releaseCount)
}

func TestCollectionUsageHotEnableAndScanInterval(t *testing.T) {
	enableAutoReleaseForTest(t)
	params := paramtable.Get()
	enabledKey := params.QueryCoordCfg.AutoReleaseEnabled.Key
	intervalKey := params.QueryCoordCfg.AutoReleaseCheckInterval.Key
	require.NoError(t, params.Save(enabledKey, "false"))
	require.NoError(t, params.Save(intervalKey, "60"))
	t.Cleanup(func() { require.NoError(t, params.Reset(intervalKey)) })
	dispatcher := paramtable.GetBaseTable().Manager().Dispatcher
	enabledWatchers := len(dispatcher.Get(enabledKey))
	autoLoadKey := params.ProxyCfg.EnableAutoLoad.Key
	autoLoadWatchers := len(dispatcher.Get(autoLoadKey))
	intervalWatchers := len(dispatcher.Get(intervalKey))
	_, manager, _, _ := newUsageTestManager(t)
	manager.scan(context.Background())
	_, exists := activityTime(manager, 100)
	require.False(t, exists, "startup disabled must not initialize activity")
	manager.start(context.Background())

	// Save follows the same cache-invalidation and event-dispatch order as
	// the file and etcd sources. Warm the cache before changing each value.
	require.False(t, params.QueryCoordCfg.AutoReleaseEnabled.GetAsBool())
	require.NoError(t, params.Save(enabledKey, "true"))
	require.Equal(t, time.Minute, params.QueryCoordCfg.AutoReleaseCheckInterval.GetAsDuration(time.Second))
	require.NoError(t, params.Save(intervalKey, "1"))
	require.Eventually(t, func() bool {
		_, exists := activityTime(manager, 100)
		return exists
	}, 5*time.Second, 10*time.Millisecond, "the new interval must replace the 60-second startup timer")

	require.NoError(t, params.Save(intervalKey, "60"))
	require.NoError(t, params.Save(enabledKey, "false"))
	require.NoError(t, params.Save(enabledKey, "true"))
	require.Never(t, func() bool {
		_, exists := activityTime(manager, 100)
		return exists
	}, 1500*time.Millisecond, 10*time.Millisecond, "increasing the interval must also reset the timer")
	manager.close()
	require.Len(t, dispatcher.Get(enabledKey), enabledWatchers)
	require.Len(t, dispatcher.Get(autoLoadKey), autoLoadWatchers)
	require.Len(t, dispatcher.Get(intervalKey), intervalWatchers)
}

func TestCollectionUsageFeatureGatesClearActivityAndRestartTTL(t *testing.T) {
	params := paramtable.Get()
	for _, gate := range []struct {
		name string
		key  string
	}{
		{name: "auto release", key: params.QueryCoordCfg.AutoReleaseEnabled.Key},
		{name: "auto load", key: params.ProxyCfg.EnableAutoLoad.Key},
	} {
		t.Run(gate.name, func(t *testing.T) {
			enableAutoReleaseForTest(t)
			require.NoError(t, params.Save(gate.key, "false"))
			s, manager, _, releaseCount := newUsageTestManager(t)
			manager.scan(context.Background())
			_, exists := activityTime(manager, 100)
			require.False(t, exists)

			require.NoError(t, params.Save(gate.key, "true"))
			manager.scan(context.Background())
			_, exists = activityTime(manager, 100)
			require.True(t, exists)
			setActivityTime(manager, 100, time.Now().Add(-10*time.Second))

			require.NoError(t, params.Save(gate.key, "false"))
			_, exists = activityTime(manager, 100)
			require.False(t, exists)
			status, err := s.EnsureCollectionReady(context.Background(), readinessRequest(usageTestVChannel))
			require.NoError(t, merr.CheckRPCCall(status, err))
			_, exists = activityTime(manager, 100)
			require.False(t, exists, "disabled DQL must not recreate activity")
			manager.scan(context.Background())
			require.Zero(t, *releaseCount)

			require.NoError(t, params.Save(gate.key, "true"))
			manager.scan(context.Background())
			lastActiveAt, exists := activityTime(manager, 100)
			require.True(t, exists)
			require.WithinDuration(t, time.Now(), lastActiveAt, time.Second)
			require.Zero(t, *releaseCount)
		})
	}
}

func TestCollectionUsageHotTTLAppliesToScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, _, releaseCount := newUsageTestManager(t)
	manager.scan(context.Background())
	initial := time.Now().Add(-11 * time.Second)
	setActivityTime(manager, 100, initial)
	params := paramtable.Get()
	key := params.QueryCoordCfg.AutoReleaseIdleTTLSeconds.Key
	require.NoError(t, params.Save(key, "20"))
	manager.scan(context.Background())
	require.Zero(t, *releaseCount, "extending TTL must postpone release")
	require.NoError(t, params.Save(key, "5"))
	manager.scan(context.Background())
	require.Equal(t, 1, *releaseCount, "shortening TTL must use the existing timestamp")
	lastActiveAt, _ := activityTime(manager, 100)
	require.Equal(t, initial, lastActiveAt, "changing TTL must not reset activity")
}

func TestCollectionUsageEnsureRefreshesOnEntryAndReady(t *testing.T) {
	for _, scenario := range []string{"ready", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			ready := scenario == "ready"
			enableAutoReleaseForTest(t)
			s, _, syncer := newReadinessTestServer(t, testPersistedQueryView(100, qviews.ShardID{
				ReplicaID: 1000, VChannel: usageTestVChannel,
			}))
			manager := s.collectionUsage
			manager.initializeLastActiveAt(100)
			initial := time.Now().Add(-20 * time.Second)
			setActivityTime(manager, 100, initial)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			second := "by-dev-rootcoord-dml_100v1"
			result := make(chan error, 1)
			go func() {
				result <- s.ensureCollectionReady(ctx, readinessRequest(usageTestVChannel, second))
			}()
			assertReadinessBlocked(t, result)
			lastActiveAt, _ := activityTime(manager, 100)
			require.True(t, lastActiveAt.After(initial), "every Ensure attempt refreshes activity before waiting")
			initial = lastActiveAt
			beforeReady := time.Now()
			if ready {
				shardID := qviews.ShardID{ReplicaID: 1000, VChannel: second}
				builder := qviews.NewQueryViewAtCoordBuilder(1000, &viewpb.DataViewOfCollection{
					CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1},
					Shards: []*viewpb.DataViewOfShard{{Vchannel: second}},
				}, second).SetLoadInfoVersion(s.qviewsRuntime.loadConfigStore.GetConfigVersion(100))
				require.NoError(t, s.qviewsRuntime.shardViewRegistry.Ensure(shardID).AddPreparing(ctx, builder))
				preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
				readyView := preparing.View.IntoProto()
				readyView.Meta.State = viewpb.QueryViewState_QueryViewStateReady
				preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(readyView))
				up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
				up.OnSyncResponse(up.View)
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if ready {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("readiness wait did not finish")
			}
			lastActiveAt, _ = activityTime(manager, 100)
			if ready {
				require.False(t, lastActiveAt.Before(beforeReady), "refresh must use the ready time, not the wait start")
			} else {
				require.Equal(t, initial, lastActiveAt, "canceled wait must not refresh activity")
			}
		})
	}
}

func TestCollectionUsageInitializesFromRecoveredLoadConfig(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, _, _ := newReadinessTestServer(t)
	manager := s.collectionUsage
	req := readinessRequest(usageTestVChannel)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	status, err := s.EnsureCollectionReady(ctx, req)
	require.NoError(t, err)
	require.Equal(t, merr.TimeoutCode, status.GetCode())
	before := time.Now()
	manager.scan(context.Background())
	lastActiveAt, exists := activityTime(manager, 100)
	require.True(t, exists, "a load config starts the idle timer without a readiness RPC")
	require.False(t, lastActiveAt.Before(before))
}

func TestCollectionUsageCleansReleasedCollectionAndRestartsTTL(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, catalog, releaseCount := newUsageTestManager(t)
	manager.scan(context.Background())
	config := s.qviewsRuntime.loadConfigStore.Get(100).Config.Clone()
	setActivityTime(manager, 100, time.Now().Add(-20*time.Second))
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(context.Background(), 100))
	_, exists := activityTime(manager, 100)
	require.False(t, exists)

	// A late query touch cannot recreate a released collection's activity.
	manager.touch(100)
	_, exists = activityTime(manager, 100)
	require.False(t, exists)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(context.Background(), config))
	before := time.Now()
	manager.scan(context.Background())
	lastActiveAt, exists := activityTime(manager, 100)
	require.True(t, exists)
	require.False(t, lastActiveAt.Before(before))
	require.Zero(t, *releaseCount)
}

func TestCollectionUsageReleaseConcurrency(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, _, _ := newUsageTestManager(t)
	params := paramtable.Get()
	concurrencyKey := params.QueryCoordCfg.AutoReleaseConcurrency.Key
	require.NoError(t, params.Save(concurrencyKey, "2"))
	t.Cleanup(func() { require.NoError(t, params.Reset(concurrencyKey)) })

	const collections = 5
	started := make(chan struct{}, collections)
	unblock := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(unblock) }) }
	t.Cleanup(releaseAll)
	var active atomic.Int32
	var maxActive atomic.Int32
	var calls atomic.Int32
	manager.release = func(context.Context, int64) error {
		calls.Add(1)
		running := active.Add(1)
		for current := maxActive.Load(); running > current; current = maxActive.Load() {
			if maxActive.CompareAndSwap(current, running) {
				break
			}
		}
		started <- struct{}{}
		<-unblock
		active.Add(-1)
		return nil
	}

	done := make(chan struct{})
	go func() {
		manager.releaseCollections(context.Background(), []int64{1, 2, 3, 4, 5})
		close(done)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("release did not reach configured concurrency")
		}
	}
	select {
	case <-started:
		t.Fatal("release exceeded configured concurrency")
	case <-time.After(100 * time.Millisecond):
	}

	releaseAll()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent releases did not finish")
	}
	require.EqualValues(t, 2, maxActive.Load())
	require.EqualValues(t, collections, calls.Load())
}

func TestCollectionUsageConcurrentTouchAndScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, _, _ := newUsageTestManager(t)
	manager.scan(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				manager.touch(100)
				manager.scan(context.Background())
			}
		}()
	}
	params := paramtable.Get()
	key := params.QueryCoordCfg.AutoReleaseEnabled.Key
	for i := 0; i < 100; i++ {
		require.NoError(t, params.Save(key, "false"))
		require.NoError(t, params.Save(key, "true"))
	}
	wg.Wait()
	manager.scan(context.Background())
	_, exists := activityTime(manager, 100)
	require.True(t, exists)
}
