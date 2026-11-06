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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
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

// scanAndWait scans and waits until the releases the scan queued have finished.
func scanAndWait(t *testing.T, manager *collectionUsageManager) {
	t.Helper()
	manager.scan(context.Background())
	waitReleasesDone(t, manager)
}

func waitReleasesDone(t *testing.T, manager *collectionUsageManager) {
	t.Helper()
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return len(manager.releasing) == 0
	}, 5*time.Second, time.Millisecond)
}

// useGlobalTTLForTest makes every collection use the global idle TTL.
func useGlobalTTLForTest(t *testing.T, manager *collectionUsageManager) {
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	manager.broker = broker
}

// queueIdleReleases queues collections whose activity predates the idle TTL
// enableAutoReleaseForTest sets.
func queueIdleReleases(manager *collectionUsageManager, collectionIDs ...int64) {
	for _, collectionID := range collectionIDs {
		setActivityTime(manager, collectionID, time.Now().Add(-20*time.Second))
	}
	manager.queueReleases(context.Background(), collectionIDs)
}

func setReleaseConcurrencyForTest(t *testing.T, concurrency string) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoReleaseConcurrency.Key, concurrency))
	t.Cleanup(func() { require.NoError(t, params.Reset(params.QueryCoordCfg.AutoReleaseConcurrency.Key)) })
}

func TestCollectionUsageRefreshesOnEveryDQLAttempt(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, _, releaseCount := newUsageTestManager(t)
	scanAndWait(t, manager)
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
		scanAndWait(t, manager)
		require.Zero(t, *releaseCount)
	}

	// There is no end notification. A query which is still running after TTL
	// does not prevent release and must use the existing DQL retry path.
	setActivityTime(manager, 100, time.Now().Add(-9*time.Second))
	scanAndWait(t, manager)
	require.Zero(t, *releaseCount)
	setActivityTime(manager, 100, time.Now().Add(-10*time.Second))
	scanAndWait(t, manager)
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
	scanAndWait(t, manager)
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
			scanAndWait(t, manager)
			_, exists := activityTime(manager, 100)
			require.False(t, exists)

			require.NoError(t, params.Save(gate.key, "true"))
			scanAndWait(t, manager)
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
			scanAndWait(t, manager)
			require.Zero(t, *releaseCount)

			require.NoError(t, params.Save(gate.key, "true"))
			scanAndWait(t, manager)
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
	scanAndWait(t, manager)
	initial := time.Now().Add(-11 * time.Second)
	setActivityTime(manager, 100, initial)
	params := paramtable.Get()
	key := params.QueryCoordCfg.AutoReleaseIdleTTLSeconds.Key
	require.NoError(t, params.Save(key, "20"))
	scanAndWait(t, manager)
	require.Zero(t, *releaseCount, "extending TTL must postpone release")
	require.NoError(t, params.Save(key, "5"))
	scanAndWait(t, manager)
	require.Equal(t, 1, *releaseCount, "shortening TTL must use the existing timestamp")
	lastActiveAt, _ := activityTime(manager, 100)
	require.Equal(t, initial, lastActiveAt, "changing TTL must not reset activity")
}

func TestCollectionUsageUsesDifferentCollectionTTLs(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, _, _ := newUsageTestManager(t)
	for _, id := range []int64{200, 300, 400} {
		cfg := s.qviewsRuntime.loadConfigStore.Get(100).Config.Clone()
		cfg.CollectionID = id
		cfg.Replicas[0].ReplicaID = id * 10
		require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(context.Background(), cfg))
	}
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id int64) ([]*commonpb.KeyValuePair, error) {
		values := map[int64]string{100: "20", 200: "5", 300: "-1"}
		if value, ok := values[id]; ok {
			return []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: value}}, nil
		}
		return nil, nil
	})
	manager.broker = broker
	scanAndWait(t, manager)
	for _, id := range []int64{100, 200, 300, 400} {
		setActivityTime(manager, id, time.Now().Add(-11*time.Second))
	}
	released := make(chan int64, 4)
	manager.release = func(_ context.Context, id int64) error {
		released <- id
		return nil
	}
	scanAndWait(t, manager)
	require.Len(t, released, 2)
	require.ElementsMatch(t, []int64{200, 400}, []int64{<-released, <-released})
}

func TestCollectionUsageDynamicCollectionTTL(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, _, releaseCount := newUsageTestManager(t)
	properties := []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: "20"}}
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).RunAndReturn(func(context.Context, int64) ([]*commonpb.KeyValuePair, error) {
		return properties, nil
	})
	manager.broker = broker
	scanAndWait(t, manager)
	initial := time.Now().Add(-6 * time.Second)
	setActivityTime(manager, 100, initial)
	configVersion := s.qviewsRuntime.loadConfigStore.GetConfigVersion(100)
	for _, value := range []string{"30", "-1", "5", ""} {
		if value == "" {
			properties = nil
		} else {
			properties = []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: value}}
		}
		before := *releaseCount
		scanAndWait(t, manager)
		if value == "5" {
			require.Equal(t, before+1, *releaseCount)
		} else {
			require.Equal(t, before, *releaseCount)
		}
		lastActiveAt, _ := activityTime(manager, 100)
		require.Equal(t, initial, lastActiveAt, "changing or deleting a property must not reset activity")
		require.Equal(t, configVersion, s.qviewsRuntime.loadConfigStore.GetConfigVersion(100))
	}
	properties = []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: "-1"}}
	manager.touch(100)
	lastActiveAt, _ := activityTime(manager, 100)
	require.True(t, lastActiveAt.After(initial), "DQL must still refresh activity while auto release is disabled for this collection")
	properties = []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: "5"}}
	scanAndWait(t, manager)
	require.Equal(t, 1, *releaseCount, "enabling auto release must respect the latest DQL activity")
}

func TestCollectionUsageInvalidTTLFallsBackToGlobalTTL(t *testing.T) {
	for _, value := range []string{"invalid", "0"} {
		t.Run(value, func(t *testing.T) {
			enableAutoReleaseForTest(t)
			_, manager, _, releaseCount := newUsageTestManager(t)
			scanAndWait(t, manager)
			broker := meta.NewMockBroker(t)
			broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).Return(
				[]*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: value}}, nil,
			).Times(3) // two scans and the check before the release
			manager.broker = broker

			setActivityTime(manager, 100, time.Now().Add(-5*time.Second))
			scanAndWait(t, manager)
			require.Zero(t, *releaseCount, "invalid TTL must respect the global idle TTL")

			initial := time.Now().Add(-11 * time.Second)
			setActivityTime(manager, 100, initial)
			scanAndWait(t, manager)
			require.Equal(t, 1, *releaseCount, "invalid TTL must allow release after the global idle TTL")
			lastActiveAt, _ := activityTime(manager, 100)
			require.Equal(t, initial, lastActiveAt, "falling back to the global TTL must not reset activity")
		})
	}
}

func TestCollectionUsageSkipsUnavailableMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "metadata unavailable", err: merr.ErrServiceUnavailable},
		{name: "collection dropped", err: merr.ErrCollectionNotFound},
		{name: "canceled", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enableAutoReleaseForTest(t)
			_, manager, _, releaseCount := newUsageTestManager(t)
			scanAndWait(t, manager)
			initial := time.Now().Add(-11 * time.Second)
			setActivityTime(manager, 100, initial)
			broker := meta.NewMockBroker(t)
			broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).Return(nil, tc.err).Once()
			manager.broker = broker
			scanAndWait(t, manager)
			require.Zero(t, *releaseCount, "a failed lookup must not fall back to the global TTL and release")
			lastActiveAt, _ := activityTime(manager, 100)
			require.Equal(t, initial, lastActiveAt)
		})
	}
}

func TestCollectionUsageReadsLoadedPropertiesOnEveryScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, _, releaseCount := newUsageTestManager(t)
	// Activity alone must not make an unloaded collection a scan target.
	setActivityTime(manager, 200, time.Now().Add(-time.Hour))
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).Return(
		[]*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: "-1"}}, nil,
	).Twice()
	manager.broker = broker
	scanAndWait(t, manager)
	scanAndWait(t, manager)
	require.Zero(t, *releaseCount)
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
	scanAndWait(t, manager)
	lastActiveAt, exists := activityTime(manager, 100)
	require.True(t, exists, "a load config starts the idle timer without a readiness RPC")
	require.False(t, lastActiveAt.Before(before))
}

func TestCollectionUsageCleansReleasedCollectionAndRestartsTTL(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, _, releaseCount := newUsageTestManager(t)
	scanAndWait(t, manager)
	config := s.qviewsRuntime.loadConfigStore.Get(100).Config.Clone()
	setActivityTime(manager, 100, time.Now().Add(-20*time.Second))
	s.qviewsRuntime.loadConfigStore.StopPlanning(100)
	require.NoError(t, s.qviewsRuntime.loadConfigStore.Remove(context.Background(), 100))
	_, exists := activityTime(manager, 100)
	require.False(t, exists)

	// A late query touch cannot recreate a released collection's activity.
	manager.touch(100)
	_, exists = activityTime(manager, 100)
	require.False(t, exists)

	require.NoError(t, s.qviewsRuntime.loadConfigStore.Put(context.Background(), config))
	before := time.Now()
	scanAndWait(t, manager)
	lastActiveAt, exists := activityTime(manager, 100)
	require.True(t, exists)
	require.False(t, lastActiveAt.Before(before))
	require.Zero(t, *releaseCount)
}

func TestCollectionUsageReleaseConcurrency(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, _, _ := newUsageTestManager(t)
	useGlobalTTLForTest(t, manager)
	setReleaseConcurrencyForTest(t, "2")

	const collections = 5
	// Room for duplicate releases, so a regression fails instead of blocking.
	started := make(chan struct{}, 2*collections)
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

	queueIdleReleases(manager, 1, 2, 3, 4, 5)
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
	// A scan while every slot is busy queues nothing twice.
	queueIdleReleases(manager, 1, 2, 3, 4, 5)

	releaseAll()
	waitReleasesDone(t, manager)
	require.EqualValues(t, collections, calls.Load())
	require.EqualValues(t, 2, maxActive.Load())
}

func TestCollectionUsageSlowReleaseHoldsOnlyItsSlot(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, _, _ := newUsageTestManager(t)
	useGlobalTTLForTest(t, manager)
	setReleaseConcurrencyForTest(t, "2")

	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	var released atomic.Int32
	manager.release = func(_ context.Context, collectionID int64) error {
		if collectionID == 1 {
			<-stuck
		}
		released.Add(1)
		return nil
	}
	queueIdleReleases(manager, 1)
	// Collections found by a later scan release through the remaining slot.
	queueIdleReleases(manager, 2, 3)
	require.Eventually(t, func() bool { return released.Load() == 2 }, time.Second, time.Millisecond)
}

func TestCollectionUsageQueuedReleaseChecksIdlenessAgain(t *testing.T) {
	for name, change := range map[string]func(*collectionUsageManager, *atomic.Bool){
		"queried while queued":      func(m *collectionUsageManager, _ *atomic.Bool) { m.touch(2) },
		"TTL disabled while queued": func(_ *collectionUsageManager, disabled *atomic.Bool) { disabled.Store(true) },
	} {
		t.Run(name, func(t *testing.T) {
			enableAutoReleaseForTest(t)
			_, manager, _, _ := newUsageTestManager(t)
			setReleaseConcurrencyForTest(t, "1")
			var disabled atomic.Bool
			broker := meta.NewMockBroker(t)
			broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, collectionID int64) ([]*commonpb.KeyValuePair, error) {
				if collectionID == 2 && disabled.Load() {
					return []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseIdleTTLKey, Value: "-1"}}, nil
				}
				return nil, nil
			}).Maybe()
			manager.broker = broker

			first := make(chan struct{})
			released := make(chan int64, 2)
			manager.release = func(_ context.Context, collectionID int64) error {
				if collectionID == 1 {
					<-first
				}
				released <- collectionID
				return nil
			}
			queueIdleReleases(manager, 1, 2)
			change(manager, &disabled)
			close(first)
			waitReleasesDone(t, manager)
			require.Len(t, released, 1, "collection 2 stays loaded")
			require.EqualValues(t, 1, <-released)
		})
	}
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
