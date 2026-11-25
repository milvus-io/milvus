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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	walbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

const usageTestVChannel = "by-dev-rootcoord-dml_100v0"

func enableAutoReleaseForTest(t *testing.T, scanRoles ...replicateutil.Role) {
	t.Helper()
	b := mock_balancer.NewMockBalancer(t)
	if len(scanRoles) == 0 {
		b.EXPECT().ReplicateRole().Return(replicateutil.RolePrimary).Maybe()
	}
	for _, role := range scanRoles {
		b.EXPECT().ReplicateRole().Return(role).Once()
	}
	getBalancer := mockey.Mock(walbalance.GetWithContext).Return(b, nil).Build()
	t.Cleanup(func() { getBalancer.UnPatch() })

	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoReleaseTTISeconds.Key, "10"))
	require.NoError(t, params.Save(params.ProxyCfg.EnableAutoLoad.Key, "true"))
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoReleaseEnabled.Key, "true"))
	t.Cleanup(func() {
		require.NoError(t, params.Reset(params.QueryCoordCfg.AutoReleaseTTISeconds.Key))
		require.NoError(t, params.Reset(params.QueryCoordCfg.AutoReleaseEnabled.Key))
		require.NoError(t, params.Reset(params.ProxyCfg.EnableAutoLoad.Key))
	})
}

func newUsageTestManager(t *testing.T) (*Server, *collectionUsageManager, *mockey.Mocker) {
	t.Helper()
	view := testPersistedQueryView(100, qviews.ShardID{
		ReplicaID: 1000,
		VChannel:  usageTestVChannel,
	})
	s, _, _ := newReadinessTestServer(t, view)
	store := s.qviewsRuntime.loadConfigStore
	cfg := store.GetConfig(100).Clone()
	cfg.AutoLoad = true
	require.NoError(t, store.Put(t.Context(), cfg))
	s.meta = newViewMeta(&metacache.CollectionInfo{ID: 100, VChannelNames: []string{usageTestVChannel}})
	manager, releaseMock := setupUsageTestManager(t, s, view)
	return s, manager, releaseMock
}

type usageDataViews []*viewpb.QueryViewOfShard

func (views usageDataViews) RegisterDataViewListener(listener api.DataViewListener) func() {
	collections := make(map[int64][]*api.ShardDataView)
	for _, view := range views {
		meta := view.GetMeta()
		collections[meta.GetCollectionId()] = append(collections[meta.GetCollectionId()], &api.ShardDataView{VChannel: meta.GetVchannel()})
	}
	for id, shards := range collections {
		listener(id, &api.CollectionDataView{Shards: shards})
	}
	return func() {}
}

func setupUsageTestManager(t *testing.T, s *Server, views ...*viewpb.QueryViewOfShard) (*collectionUsageManager, *mockey.Mocker) {
	t.Helper()
	runtime := s.qviewsRuntime
	runtime.viewFeedback = viewfeedback.New(runtime.loadConfigStore, usageDataViews(views))
	t.Cleanup(runtime.viewFeedback.Close)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.broker = broker
	manager := newCollectionUsageManager(s)
	releaseMock := mockey.Mock((*collectionUsageManager).autoReleaseCollection).Return(nil).Build()
	t.Cleanup(func() {
		manager.close()
		releaseMock.UnPatch()
	})
	return manager, releaseMock
}

func activityUnixNanos(manager *collectionUsageManager, collectionID int64) (int64, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	lastActiveUnixNanos, exists := manager.lastActiveUnixNanos[collectionID]
	return lastActiveUnixNanos, exists
}

func setActivityTime(manager *collectionUsageManager, collectionID int64, at time.Time) {
	manager.mu.Lock()
	manager.lastActiveUnixNanos[collectionID] = at.UnixNano()
	manager.mu.Unlock()
}

// startReleaseBatch returns a channel closed when all releases finish.
func startReleaseBatch(manager *collectionUsageManager, collectionIDs ...int64) <-chan struct{} {
	candidates := make([]releaseCandidate, 0, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		candidates = append(candidates, releaseCandidate{collectionID: collectionID, reason: releaseReasonTTI})
	}
	completed := make(chan struct{})
	go func() {
		manager.releaseCollections(context.Background(), candidates)
		close(completed)
	}()
	return completed
}

func waitForReleaseBatch(t *testing.T, completed <-chan struct{}) {
	t.Helper()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("release batch did not finish")
	}
}

func setUsageParameter(t *testing.T, item *paramtable.ParamItem, value string) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(item.Key, value))
	t.Cleanup(func() { require.NoError(t, params.Reset(item.Key)) })
}

func newLRUUsageManager(t *testing.T, count int, scanRoles ...replicateutil.Role) (*collectionUsageManager, chan int64, *mockey.Mocker, *mockey.Mocker) {
	t.Helper()
	enableAutoReleaseForTest(t, scanRoles...)
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseTTISeconds, "3600")
	ctx := t.Context()
	store, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	views := make([]*viewpb.QueryViewOfShard, 0, count)
	collections := make([]*metacache.CollectionInfo, 0, count)
	for i := 0; i < count; i++ {
		id := int64(100 + i)
		channel := fmt.Sprintf("by-dev-rootcoord-dml_%dv0", id)
		replicaID := id * 10
		require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{
			CollectionID: id,
			AutoLoad:     true,
			Replicas:     []*loadmgr.ReplicaAssignment{{ReplicaID: replicaID, ResourceGroup: "pool"}},
		}))
		views = append(views, testPersistedQueryView(id, qviews.ShardID{ReplicaID: replicaID, VChannel: channel}))
		collections = append(collections, &metacache.CollectionInfo{ID: id, VChannelNames: []string{channel}})
	}
	s, _ := newReadinessTestServerWithStore(t, store, views...)
	s.meta = newViewMeta(collections...)
	memoryMock := mockey.Mock((*collectionUsageManager).collectMemoryUsageByRG).
		Return(map[string]collectionMemoryUsage{"pool": {used: 90, total: 100}}).Build()
	t.Cleanup(func() { memoryMock.UnPatch() })
	manager, releaseMock := setupUsageTestManager(t, s, views...)
	for i := 0; i < count; i++ {
		setActivityTime(manager, int64(100+i), time.Now().Add(-time.Duration(count-i)*time.Second))
	}
	released := make(chan int64, count*2)
	releaseMock.To(func(_ *collectionUsageManager, _ context.Context, id int64, _ collectionReleaseReason) error {
		manager.s.qviewsRuntime.loadConfigStore.StopPlanning(id)
		released <- id
		return nil
	})
	return manager, released, releaseMock, memoryMock
}

func TestCollectionUsageSkipsSecondaryScanAndResumesOnPromotion(t *testing.T) {
	manager, released, _, memoryMock := newLRUUsageManager(t, 2, replicateutil.RoleSecondary, replicateutil.RolePrimary)
	setActivityTime(manager, 100, time.Now().Add(-2*time.Hour))
	broker := meta.NewMockBroker(t)
	manager.s.broker = broker

	manager.scan(t.Context())
	require.Zero(t, memoryMock.Times())
	require.Empty(t, released)

	// Collection 100 has expired. Collection 101 qualifies only under memory pressure.
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).Return(nil, nil).Twice()
	manager.scan(t.Context())
	require.Equal(t, 1, memoryMock.Times())
	require.Len(t, released, 2)
	require.ElementsMatch(t, []int64{100, 101}, []int64{<-released, <-released})
}

func TestCollectionUsageSkipsScanWhenRoleUnavailable(t *testing.T) {
	getBalancer := mockey.Mock(walbalance.GetWithContext).Return(nil, context.DeadlineExceeded).Build()
	defer getBalancer.UnPatch()
	manager := &collectionUsageManager{enabled: true}
	manager.scan(t.Context())
	require.Equal(t, 1, getBalancer.Times())
}

func reportCollectionAccess(manager *collectionUsageManager, replicaID int64, channel string, at time.Time) {
	id := qviews.ShardID{ReplicaID: replicaID, VChannel: channel}
	manager.s.qviewsRuntime.viewFeedback.OnViewFeedback(qviews.ViewFeedbackWindow{
		StartUnixNanos: at.Add(-time.Second).UnixNano(),
		EndUnixNanos:   at.Add(time.Second).UnixNano(),
		Shards: map[qviews.ShardID]qviews.ViewFeedback{
			id: {ShardID: id, RequestCount: 1, LastAccessUnixNanos: at.UnixNano()},
		},
	})
}

func TestCollectionUsageExpiresOnFirstScanWithOldFeedback(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, releaseMock := newUsageTestManager(t)
	now := time.Now()
	reportCollectionAccess(manager, 1000, usageTestVChannel, now.Add(-20*time.Second))
	manager.scan(t.Context())
	lastActiveUnixNanos, exists := activityUnixNanos(manager, 100)
	require.True(t, exists)
	require.Equal(t, now.Add(-20*time.Second).UnixNano(), lastActiveUnixNanos)
	require.Equal(t, 1, releaseMock.Times(), "expired feedback makes the collection eligible on its first scan")
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
	_, manager, _ := newUsageTestManager(t)
	manager.start(context.Background())
	manager.scan(context.Background())
	_, exists := activityUnixNanos(manager, 100)
	require.False(t, exists, "startup disabled must not initialize activity")

	// Save follows the same cache-invalidation and event-dispatch order as
	// the file and etcd sources. Warm the cache before changing each value.
	require.False(t, params.QueryCoordCfg.AutoReleaseEnabled.GetAsBool())
	require.NoError(t, params.Save(enabledKey, "true"))
	require.Equal(t, time.Minute, params.QueryCoordCfg.AutoReleaseCheckInterval.GetAsDuration(time.Second))
	require.NoError(t, params.Save(intervalKey, "1"))
	require.Eventually(t, func() bool {
		_, exists := activityUnixNanos(manager, 100)
		return exists
	}, 5*time.Second, 10*time.Millisecond, "the new interval must replace the 60-second startup timer")

	require.NoError(t, params.Save(intervalKey, "60"))
	require.NoError(t, params.Save(enabledKey, "false"))
	require.NoError(t, params.Save(enabledKey, "true"))
	require.Never(t, func() bool {
		_, exists := activityUnixNanos(manager, 100)
		return exists
	}, 1500*time.Millisecond, 10*time.Millisecond, "increasing the interval must also reset the timer")
	manager.close()
	require.Len(t, dispatcher.Get(enabledKey), enabledWatchers)
	require.Len(t, dispatcher.Get(autoLoadKey), autoLoadWatchers)
	require.Len(t, dispatcher.Get(intervalKey), intervalWatchers)
}

func TestCollectionUsageFeatureGatesClearActivityAndRestartTTI(t *testing.T) {
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
			_, manager, releaseMock := newUsageTestManager(t)
			manager.scan(context.Background())
			_, exists := activityUnixNanos(manager, 100)
			require.False(t, exists)

			require.NoError(t, params.Save(gate.key, "true"))
			manager.scan(context.Background())
			_, exists = activityUnixNanos(manager, 100)
			require.True(t, exists)
			setActivityTime(manager, 100, time.Now().Add(-10*time.Second))

			require.NoError(t, params.Save(gate.key, "false"))
			_, exists = activityUnixNanos(manager, 100)
			require.False(t, exists)
			manager.scan(context.Background())
			require.Zero(t, releaseMock.Times())

			require.NoError(t, params.Save(gate.key, "true"))
			manager.scan(context.Background())
			lastActiveUnixNanos, exists := activityUnixNanos(manager, 100)
			require.True(t, exists)
			require.GreaterOrEqual(t, lastActiveUnixNanos, time.Now().Add(-time.Second).UnixNano())
			require.LessOrEqual(t, lastActiveUnixNanos, time.Now().UnixNano())
			require.Zero(t, releaseMock.Times())
		})
	}
}

func TestCollectionUsageHotTTIAppliesToScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, releaseMock := newUsageTestManager(t)
	manager.scan(context.Background())
	initial := time.Now().Add(-11 * time.Second)
	setActivityTime(manager, 100, initial)
	params := paramtable.Get()
	key := params.QueryCoordCfg.AutoReleaseTTISeconds.Key
	require.NoError(t, params.Save(key, "20"))
	manager.scan(context.Background())
	require.Zero(t, releaseMock.Times(), "extending TTI must postpone release")
	require.NoError(t, params.Save(key, "5"))
	manager.scan(context.Background())
	require.Equal(t, 1, releaseMock.Times(), "shortening TTI must use the existing timestamp")
	lastActiveUnixNanos, _ := activityUnixNanos(manager, 100)
	require.Equal(t, initial.UnixNano(), lastActiveUnixNanos, "changing TTI must not reset activity")
}

func TestCollectionUsageUsesDifferentCollectionTTIs(t *testing.T) {
	manager, released, _, memoryMock := newLRUUsageManager(t, 4)
	memoryMock.Return(nil)
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseTTISeconds, "10")
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id int64) ([]*commonpb.KeyValuePair, error) {
		values := map[int64]string{100: "20", 101: "5", 102: "-1"}
		if value, ok := values[id]; ok {
			return []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: value}}, nil
		}
		return nil, nil
	})
	manager.s.broker = broker
	for id := int64(100); id <= 103; id++ {
		setActivityTime(manager, id, time.Now().Add(-11*time.Second))
	}
	manager.scan(context.Background())
	require.Len(t, released, 2)
	require.ElementsMatch(t, []int64{101, 103}, []int64{<-released, <-released})
}

func TestCollectionUsageDynamicCollectionTTI(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, releaseMock := newUsageTestManager(t)
	properties := []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: "20"}}
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).RunAndReturn(func(context.Context, int64) ([]*commonpb.KeyValuePair, error) {
		return properties, nil
	})
	manager.s.broker = broker
	manager.scan(context.Background())
	initial := time.Now().Add(-6 * time.Second)
	setActivityTime(manager, 100, initial)
	configVersion := s.qviewsRuntime.loadConfigStore.GetConfigVersion(100)
	for _, value := range []string{"30", "-1", "5", ""} {
		if value == "" {
			properties = nil
		} else {
			properties = []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: value}}
		}
		before := releaseMock.Times()
		manager.scan(context.Background())
		if value == "5" {
			require.Equal(t, before+1, releaseMock.Times())
		} else {
			require.Equal(t, before, releaseMock.Times())
		}
		lastActiveUnixNanos, _ := activityUnixNanos(manager, 100)
		require.Equal(t, initial.UnixNano(), lastActiveUnixNanos, "changing or deleting a property must not reset activity")
		require.Equal(t, configVersion, s.qviewsRuntime.loadConfigStore.GetConfigVersion(100))
	}
	properties = []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: "-1"}}
	reportCollectionAccess(manager, 1000, usageTestVChannel, time.Now())
	manager.scan(t.Context())
	lastActiveUnixNanos, _ := activityUnixNanos(manager, 100)
	require.Greater(t, lastActiveUnixNanos, initial.UnixNano(), "SN feedback must still refresh activity while auto release is disabled for this collection")
	properties = []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: "5"}}
	manager.scan(context.Background())
	require.Equal(t, 1, releaseMock.Times(), "enabling auto release must respect the latest reported activity")
}

func TestCollectionUsageInvalidTTIFallsBackToGlobalTTI(t *testing.T) {
	for _, value := range []string{"invalid", "0"} {
		t.Run(value, func(t *testing.T) {
			enableAutoReleaseForTest(t)
			_, manager, releaseMock := newUsageTestManager(t)
			manager.scan(context.Background())
			broker := meta.NewMockBroker(t)
			broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).Return(
				[]*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: value}}, nil,
			).Times(2) // one lookup per scan
			manager.s.broker = broker

			setActivityTime(manager, 100, time.Now().Add(-5*time.Second))
			manager.scan(context.Background())
			require.Zero(t, releaseMock.Times(), "invalid TTI must respect the global idle TTI")

			initial := time.Now().Add(-11 * time.Second)
			setActivityTime(manager, 100, initial)
			manager.scan(context.Background())
			require.Equal(t, 1, releaseMock.Times(), "invalid TTI must allow release after the global idle TTI")
			lastActiveUnixNanos, _ := activityUnixNanos(manager, 100)
			require.Equal(t, initial.UnixNano(), lastActiveUnixNanos, "falling back to the global TTI must not reset activity")
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
			_, manager, releaseMock := newUsageTestManager(t)
			manager.scan(context.Background())
			initial := time.Now().Add(-11 * time.Second)
			setActivityTime(manager, 100, initial)
			broker := meta.NewMockBroker(t)
			broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).Return(nil, tc.err).Once()
			manager.s.broker = broker
			manager.scan(context.Background())
			require.Zero(t, releaseMock.Times(), "a failed lookup must not fall back to the global TTI and release")
			lastActiveUnixNanos, _ := activityUnixNanos(manager, 100)
			require.Equal(t, initial.UnixNano(), lastActiveUnixNanos)
		})
	}
}

func TestCollectionUsageReadsLoadedPropertiesOnEveryScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, releaseMock := newUsageTestManager(t)
	// Activity alone must not make an unloaded collection a scan target.
	setActivityTime(manager, 200, time.Now().Add(-time.Hour))
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).Return(
		[]*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: "-1"}}, nil,
	).Twice()
	manager.s.broker = broker
	manager.scan(context.Background())
	manager.scan(context.Background())
	require.Zero(t, releaseMock.Times())
}

func TestCollectionUsageStartsTTIOnFirstReadyScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	view := testPersistedQueryView(100, qviews.ShardID{
		ReplicaID: 1000, VChannel: usageTestVChannel,
	})
	s, _, syncer := newReadinessTestServer(t, view)
	runtime := s.qviewsRuntime
	cfg := runtime.loadConfigStore.GetConfig(100).Clone()
	cfg.AutoLoad = true
	require.NoError(t, runtime.loadConfigStore.Put(t.Context(), cfg))
	second := "by-dev-rootcoord-dml_100v1"
	s.meta = newViewMeta(&metacache.CollectionInfo{ID: 100, VChannelNames: []string{usageTestVChannel, second}})
	manager, releaseMock := setupUsageTestManager(t, s, view)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.ensureCollectionReady(ctx, readinessRequest(usageTestVChannel, second)) }()
	assertReadinessBlocked(t, result)
	manager.scan(ctx)
	_, exists := activityUnixNanos(manager, 100)
	require.False(t, exists, "scanning a loading collection must not start the timer")

	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: second}
	builder := qviews.NewQueryViewAtCoordBuilder(1000, &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1},
		Shards: []*viewpb.DataViewOfShard{{Vchannel: second}},
	}, second).SetLoadInfoVersion(runtime.loadConfigStore.GetConfigVersion(100))
	require.NoError(t, runtime.shardViewRegistry.Ensure(shardID).AddPreparing(ctx, builder))
	preparing := receiveReadinessSync(t, syncer, qviews.QueryViewStatePreparing)
	readyView := preparing.View.IntoProto()
	readyView.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	preparing.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(readyView))
	up := receiveReadinessSync(t, syncer, qviews.QueryViewStateUp)
	up.OnSyncResponse(up.View)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("readiness wait did not finish")
	}
	_, exists = activityUnixNanos(manager, 100)
	require.False(t, exists, "Ensure success must not record an access")

	beforeScan := time.Now()
	manager.scan(ctx)
	lastActiveUnixNanos, exists := activityUnixNanos(manager, 100)
	require.True(t, exists)
	require.GreaterOrEqual(t, lastActiveUnixNanos, beforeScan.UnixNano())
	require.Zero(t, releaseMock.Times())

	reportedAt := beforeScan.Add(-20 * time.Second)
	reportCollectionAccess(manager, 1000, usageTestVChannel, reportedAt)
	manager.scan(ctx)
	current, _ := activityUnixNanos(manager, 100)
	require.Equal(t, lastActiveUnixNanos, current, "an older SQN report must not move the timer backward")
	require.Zero(t, releaseMock.Times())

	reportedAt = time.Now()
	reportCollectionAccess(manager, 1000, usageTestVChannel, reportedAt)
	manager.scan(ctx)
	lastActiveUnixNanos, _ = activityUnixNanos(manager, 100)
	require.Equal(t, reportedAt.UnixNano(), lastActiveUnixNanos, "a newer SQN report advances the timer")
	require.Zero(t, releaseMock.Times())
}

func TestCollectionUsageClearsActivityWhenReleaseOverlapsScan(t *testing.T) {
	enableAutoReleaseForTest(t)
	s, manager, releaseMock := newUsageTestManager(t)
	store := s.qviewsRuntime.loadConfigStore
	config := store.GetConfig(100).Clone()
	setActivityTime(manager, 100, time.Now().Add(-20*time.Second))
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, int64(100)).RunAndReturn(func(context.Context, int64) ([]*commonpb.KeyValuePair, error) {
		// The scan has read the load config and readiness when release clears them.
		store.StopPlanning(100)
		_, exists := activityUnixNanos(manager, 100)
		require.False(t, exists, "release clears the existing activity record")
		return nil, nil
	}).Once()
	s.broker = broker

	manager.scan(t.Context())
	_, exists := activityUnixNanos(manager, 100)
	require.False(t, exists, "the scan must not leave activity for a released collection")
	require.NoError(t, store.Remove(t.Context(), 100))

	require.NoError(t, store.Put(t.Context(), config))
	manager.scan(t.Context())
	_, exists = activityUnixNanos(manager, 100)
	require.False(t, exists, "a fresh load must become ready before starting a new timer")
	require.Zero(t, releaseMock.Times())
}

func TestCollectionUsageLRUDefaultBatchSelectsOldest64(t *testing.T) {
	manager, released, _, _ := newLRUUsageManager(t, 70)
	reportCollectionAccess(manager, 1000, usageTestVChannel, time.Now())
	manager.scan(context.Background())
	require.Len(t, released, 64)
	require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(100), "recent SN feedback protects the oldest baseline")
	for id := int64(101); id < 165; id++ {
		require.False(t, manager.s.qviewsRuntime.loadConfigStore.Contains(id))
	}
	for id := int64(165); id < 170; id++ {
		require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(id))
	}
}

func TestCollectionUsagePicksLatestReplicaFeedback(t *testing.T) {
	manager, _, _, _ := newLRUUsageManager(t, 2)
	runtime := manager.s.qviewsRuntime
	store := runtime.loadConfigStore
	cfg := store.GetConfig(100).Clone()
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 999, ResourceGroup: "another-pool"})
	require.NoError(t, store.Put(t.Context(), cfg))
	// Release selection compares collection-level activity across all replicas,
	// including a recent query served outside the pressured pool.
	now := time.Now()
	reportCollectionAccess(manager, 1000, usageTestVChannel, now.Add(-time.Second))
	reportCollectionAccess(manager, 999, usageTestVChannel, now)
	candidate, _, ok := manager.releaseCandidate(t.Context(), store.GetConfig(100), runtime.viewFeedback.GetPlanningSnapshot())
	require.True(t, ok)
	require.Equal(t, now.UnixNano(), candidate.lastActiveUnixNanos)
}

func TestCollectionUsageSkipsManualAndLoadingCollections(t *testing.T) {
	manager, released, _, _ := newLRUUsageManager(t, 4)
	store := manager.s.qviewsRuntime.loadConfigStore
	for _, id := range []int64{100, 103} {
		cfg := store.GetConfig(id).Clone()
		cfg.AutoLoad = false
		require.NoError(t, store.Put(t.Context(), cfg))
	}
	manager.s.meta = newViewMeta(
		&metacache.CollectionInfo{ID: 100, VChannelNames: []string{usageTestVChannel}},
		&metacache.CollectionInfo{ID: 101, VChannelNames: []string{"by-dev-rootcoord-dml_101v0", "by-dev-rootcoord-dml_101v1"}},
		&metacache.CollectionInfo{ID: 102, VChannelNames: []string{"by-dev-rootcoord-dml_102v0"}},
		&metacache.CollectionInfo{ID: 103, VChannelNames: []string{"by-dev-rootcoord-dml_103v0"}},
	)
	setActivityTime(manager, 100, time.Now().Add(-2*time.Hour))
	setActivityTime(manager, 101, time.Now().Add(-2*time.Hour))
	delete(manager.lastActiveUnixNanos, 102)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, int64(102)).Return(nil, nil).Once()
	manager.s.broker = broker

	manager.scan(t.Context())

	require.Len(t, released, 1)
	require.EqualValues(t, 102, <-released, "LRU can select a collection on its first ready scan without feedback")
	require.True(t, store.Contains(100))
	require.True(t, store.Contains(101))
	require.True(t, store.Contains(103))
}

func TestCollectionUsageLRUHysteresisAndDynamicParameters(t *testing.T) {
	manager, released, _, memoryMock := newLRUUsageManager(t, 8)
	params := &paramtable.Get().QueryCoordCfg
	setUsageParameter(t, &params.AutoReleaseLRUMaxEvictions, "2")
	usage := uint64(84)
	memoryMock.To(func(*collectionUsageManager, context.Context) map[string]collectionMemoryUsage {
		return map[string]collectionMemoryUsage{"pool": {used: usage, total: 100}}
	})
	manager.scan(context.Background())
	require.Empty(t, released)
	usage = 85
	manager.scan(context.Background())
	require.Len(t, released, 2)
	usage = 84
	manager.scan(context.Background())
	require.Len(t, released, 4, "pressure persists below high until low is reached")
	usage = 80
	manager.scan(context.Background())
	usage = 84
	manager.scan(context.Background())
	require.Len(t, released, 4, "pressure does not restart below high")
	setUsageParameter(t, &params.AutoReleaseLRUHighWatermark, "84")
	manager.scan(context.Background())
	require.Len(t, released, 6, "lowering high applies to the next scan")
	setUsageParameter(t, &params.AutoReleaseLRUHighWatermark, "95")
	setUsageParameter(t, &params.AutoReleaseLRULowWatermark, "87")
	manager.scan(context.Background())
	require.Len(t, released, 6, "raising low ends existing pressure")
	setUsageParameter(t, &params.AutoReleaseLRUMaxEvictions, "1")
	usage = 95
	manager.scan(context.Background())
	require.Len(t, released, 7, "a new batch limit applies without restarting")
}

func TestCollectionUsageTTIDoesNotConsumeLRUQuota(t *testing.T) {
	manager, released, _, _ := newLRUUsageManager(t, 3)
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseLRUMaxEvictions, "1")
	setActivityTime(manager, 102, time.Now().Add(-2*time.Hour))

	manager.scan(context.Background())
	require.Len(t, released, 2)
	require.ElementsMatch(t, []int64{102, 100}, []int64{<-released, <-released}, "expired TTI plus the oldest LRU candidate")
	require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(101))
}

func TestCollectionUsageScanFinishesSelectedBatch(t *testing.T) {
	manager, released, releaseMock, memoryMock := newLRUUsageManager(t, 5)
	params := &paramtable.Get().QueryCoordCfg
	setUsageParameter(t, &params.AutoReleaseConcurrency, "1")
	setUsageParameter(t, &params.AutoReleaseLRUMaxEvictions, "2")
	// 100 and 101 expire by TTI. LRU picks 102 and 103, leaving 104 for the next scan.
	setActivityTime(manager, 100, time.Now().Add(-2*time.Hour))
	setActivityTime(manager, 101, time.Now().Add(-2*time.Hour))

	var firstReleaseStarted atomic.Bool
	memoryMock.To(func(*collectionUsageManager, context.Context) map[string]collectionMemoryUsage {
		used := uint64(90)
		if firstReleaseStarted.Load() {
			used = 80
		}
		return map[string]collectionMemoryUsage{"pool": {used: used, total: 100}}
	})
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id int64) ([]*commonpb.KeyValuePair, error) {
		if firstReleaseStarted.Load() && id >= 100 && id <= 103 {
			return []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: "-1"}}, nil
		}
		return nil, nil
	})
	manager.s.broker = broker
	releaseMock.To(func(_ *collectionUsageManager, _ context.Context, id int64, _ collectionReleaseReason) error {
		firstReleaseStarted.Store(true)
		manager.s.qviewsRuntime.loadConfigStore.StopPlanning(id)
		released <- id
		return nil
	})

	manager.scan(context.Background())
	require.Len(t, released, 4)
	require.ElementsMatch(t, []int64{100, 101, 102, 103}, []int64{<-released, <-released, <-released, <-released})
	manager.scan(context.Background())
	require.Empty(t, released, "the next scan observes the low watermark")
	require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(104))
}

func TestCollectionUsageLRUExemptionsAndPoolIsolation(t *testing.T) {
	manager, released, _, memoryMock := newLRUUsageManager(t, 4)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetCollectionProperties(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, id int64) ([]*commonpb.KeyValuePair, error) {
		if id == 100 {
			return []*commonpb.KeyValuePair{{Key: common.CollectionAutoReleaseTTIKey, Value: "-1"}}, nil
		}
		return nil, nil
	}).Maybe()
	manager.s.broker = broker
	cfg := manager.s.qviewsRuntime.loadConfigStore.Get(101).Config.Clone()
	cfg.Replicas[0].ResourceGroup = "other"
	require.NoError(t, manager.s.qviewsRuntime.loadConfigStore.Put(context.Background(), cfg))
	memoryMock.To(func(*collectionUsageManager, context.Context) map[string]collectionMemoryUsage {
		return map[string]collectionMemoryUsage{"pool": {used: 90, total: 100}, "other": {used: 50, total: 100}}
	})
	manager.scan(context.Background())
	require.Len(t, released, 2)
	require.ElementsMatch(t, []int64{102, 103}, []int64{<-released, <-released})
	require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(100))
	require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(101))
}

func TestCollectionUsageLRUMissingMetricsAndInvalidWatermarksPreserveTTI(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "missing metrics", false: "invalid watermark pair"}[missing], func(t *testing.T) {
			manager, released, _, memoryMock := newLRUUsageManager(t, 3)
			if missing {
				memoryMock.To(func(*collectionUsageManager, context.Context) map[string]collectionMemoryUsage { return nil })
			} else {
				setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseLRULowWatermark, "95")
			}
			setActivityTime(manager, 102, time.Now().Add(-2*time.Hour))
			manager.scan(context.Background())
			require.Len(t, released, 1)
			require.EqualValues(t, 102, <-released)
		})
	}
}

func TestCollectionUsageChecksLoadOriginAfterAcquiringReleaseLock(t *testing.T) {
	manager, _, releaseMock, _ := newLRUUsageManager(t, 1)
	releaseMock.UnPatch()
	store := manager.s.qviewsRuntime.loadConfigStore
	manual := store.GetConfig(100).Clone()
	manual.AutoLoad = false

	b := mock_broadcaster.NewMockBroadcastAPI(t)
	b.EXPECT().Close().Once()
	lock := mockey.Mock((*Server).startBroadcastWithCollectionIDLock).To(
		func(_ *Server, ctx context.Context, _ int64) (broadcaster.BroadcastAPI, error) {
			// A manual load finishes before the release acquires the lock.
			require.NoError(t, store.Put(ctx, manual))
			return b, nil
		},
	).Build()
	defer lock.UnPatch()

	logs := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info"})
	require.NoError(t, manager.autoReleaseCollection(t.Context(), 100, releaseReasonTTI))
	require.False(t, store.GetConfig(100).AutoLoad)
	require.NotContains(t, logs.String(), "event=collection_auto_released")
}

func TestCollectionUsageReleaseLogs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		used   uint64
		idle   time.Duration
		reason string
	}{
		{"TTI expired", 50, 2 * time.Hour, "tti"},
		{"TTI expired under pressure", 90, 2 * time.Hour, "tti"},
		{"LRU under pressure", 90, time.Minute, "memory_pressure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, _, releaseMock, memoryMock := newLRUUsageManager(t, 1)
			releaseMock.UnPatch()
			manager.s.meta = newViewMeta(&metacache.CollectionInfo{
				ID: 100, DatabaseName: "tenant_db", Schema: &schemapb.CollectionSchema{DbName: "tenant_db", Name: "embeddings"},
				VChannelNames: []string{usageTestVChannel},
			})
			broadcaster := mock_broadcaster.NewMockBroadcastAPI(t)
			broadcaster.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnavailable).Once()
			broadcaster.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(nil, nil).Once()
			broadcaster.EXPECT().Close().Twice()
			lock := mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(broadcaster, nil).Build()
			defer lock.UnPatch()
			memoryMock.Return(map[string]collectionMemoryUsage{"pool": {used: tc.used, total: 100}})
			setActivityTime(manager, 100, time.Now().Add(-tc.idle))
			logs := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info"})
			manager.scan(context.Background())
			failed := logs.String()
			require.Contains(t, failed, "releaseReason="+tc.reason)
			require.NotContains(t, failed, "event=collection_auto_released")

			manager.scan(context.Background())
			success := strings.TrimPrefix(logs.String(), failed)
			require.Contains(t, success, "event=collection_auto_released")
			require.Contains(t, success, "collectionID=100")
			require.Contains(t, success, "dbName=tenant_db")
			require.Contains(t, success, "collectionName=embeddings")
			require.Contains(t, success, "releaseReason="+tc.reason)
		})
	}
}

func TestCollectionUsageLRUFailedReleaseCanBeSelectedAgain(t *testing.T) {
	manager, _, releaseMock, _ := newLRUUsageManager(t, 1)
	var calls atomic.Int32
	releaseMock.To(func(_ *collectionUsageManager, _ context.Context, id int64, _ collectionReleaseReason) error {
		if calls.Add(1) == 1 {
			return merr.ErrServiceUnavailable
		}
		manager.s.qviewsRuntime.loadConfigStore.StopPlanning(id)
		return nil
	})
	manager.scan(context.Background())
	require.True(t, manager.s.qviewsRuntime.loadConfigStore.Contains(100))
	manager.scan(context.Background())
	require.False(t, manager.s.qviewsRuntime.loadConfigStore.Contains(100))
	require.EqualValues(t, 2, calls.Load())
}

func TestAutoReleaseMemoryUsageUsesRawWeightedWorkerMemory(t *testing.T) {
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.NodeMemoryMetrics)
	require.NoError(t, err)
	for _, failure := range []string{"", "rpc", "unsupported metric", "malformed", "missing capacity", "missing usage"} {
		t.Run("reading/"+failure, func(t *testing.T) {
			cluster := session.NewMockCluster(t)
			nodes := session.NewNodeManager()
			catalog := metastoremocks.NewQueryCoordCatalog(t)
			catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
			manager := meta.NewMeta(nil, catalog, nodes, metacache.NewMetaStore(nil))
			const healthyRG = "healthy"
			for id := int64(1); id <= 7; id++ {
				labels := map[string]string{}
				switch id {
				case 4:
					labels[sessionutil.LabelStreamingNodeEmbeddedQueryNode] = "1"
				case 6:
					labels[sessionutil.LegacyLabelStreamingNodeEmbeddedQueryNode] = "1"
				case 7:
					labels[sessionutil.LabelResourceGroup] = healthyRG
				}
				nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id, Labels: labels}))
				if id != 5 { // Node 5 is suspended outside any resource group.
					manager.HandleNodeUp(context.Background(), id)
				}
			}
			nodes.Stopping(3)
			for _, id := range []int64{1, 2, 7} {
				used, total := uint64(90), uint64(100)
				if id == 2 {
					used, total = 450, 900
				}
				if id == 7 {
					used, total = 30, 200
				}
				if id == 2 && failure == "rpc" {
					cluster.EXPECT().GetMetrics(mock.Anything, id, req).Return(nil, merr.ErrServiceUnavailable).Once()
					continue
				}
				if id == 2 && failure == "unsupported metric" {
					cluster.EXPECT().GetMetrics(mock.Anything, id, req).Return(&milvuspb.GetMetricsResponse{Status: merr.Status(merr.ErrParameterInvalid)}, nil).Once()
					continue
				}
				if id == 2 && failure == "missing capacity" {
					total = 0
				}
				if id == 2 && failure == "missing usage" {
					used = 0
				}
				payload, err := metricsinfo.MarshalComponentInfos(metricsinfo.NodeMemory{Memory: total, MemoryUsage: used})
				require.NoError(t, err)
				if id == 2 && failure == "malformed" {
					payload = "not-json"
				}
				cluster.EXPECT().GetMetrics(mock.Anything, id, req).Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: payload}, nil).Once()
			}
			s := &Server{nodeMgr: nodes, meta: manager, cluster: cluster}
			usage := (&collectionUsageManager{s: s}).collectMemoryUsageByRG(context.Background())
			want := map[string]collectionMemoryUsage{healthyRG: {used: 30, total: 200}}
			if failure == "" {
				want[meta.DefaultResourceGroupName] = collectionMemoryUsage{used: 540, total: 1000}
			}
			require.Equal(t, want, usage)
		})
	}
}

func TestAutoReleaseMemoryUsageQueuedNodeGetsFullTimeout(t *testing.T) {
	const nodeCount = maxConcurrentQueryNodeMetrics + 1
	nodes := session.NewNodeManager()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	manager := meta.NewMeta(nil, catalog, nodes, metacache.NewMetaStore(nil))
	for id := int64(1); id <= nodeCount; id++ {
		nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id}))
		manager.HandleNodeUp(context.Background(), id)
	}
	cluster := session.NewMockCluster(t)
	payload, err := metricsinfo.MarshalComponentInfos(metricsinfo.NodeMemory{Memory: 100, MemoryUsage: 90})
	require.NoError(t, err)
	resp := &milvuspb.GetMetricsResponse{Status: merr.Success(), Response: payload}
	// Each RPC fits its own timeout, but the queued node finishes after the first batch's deadline.
	cluster.EXPECT().GetMetrics(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ int64, _ *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
			select {
			case <-time.After(3 * queryNodeMetricsTimeout / 4):
				return resp, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Times(nodeCount)

	s := &Server{nodeMgr: nodes, meta: manager, cluster: cluster}
	synctest.Test(t, func(t *testing.T) {
		usage := (&collectionUsageManager{s: s}).collectMemoryUsageByRG(t.Context())
		require.Equal(t, collectionMemoryUsage{used: 90 * nodeCount, total: 100 * nodeCount}, usage[meta.DefaultResourceGroupName])
	})
}

func TestCollectionUsageReleaseConcurrency(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, releaseMock := newUsageTestManager(t)
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseConcurrency, "2")

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
	releaseMock.To(func(*collectionUsageManager, context.Context, int64, collectionReleaseReason) error {
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
	})

	completed := startReleaseBatch(manager, 1, 2, 3, 4, 5)
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
	waitForReleaseBatch(t, completed)

	require.EqualValues(t, collections, calls.Load())
	require.EqualValues(t, 2, maxActive.Load())
}

func TestCollectionUsageReleaseWaitsForBatch(t *testing.T) {
	enableAutoReleaseForTest(t)
	_, manager, releaseMock := newUsageTestManager(t)
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseConcurrency, "2")
	stuck := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(stuck) }) }
	t.Cleanup(unblock)
	var released atomic.Int32
	releaseMock.To(func(_ *collectionUsageManager, _ context.Context, collectionID int64, _ collectionReleaseReason) error {
		if collectionID == 1 {
			<-stuck
		}
		released.Add(1)
		return nil
	})
	completed := startReleaseBatch(manager, 1, 2, 3)
	require.Eventually(t, func() bool { return released.Load() == 2 }, time.Second, time.Millisecond)
	select {
	case <-completed:
		t.Fatal("release returned before the entire batch finished")
	default:
	}
	unblock()
	waitForReleaseBatch(t, completed)
	require.EqualValues(t, 3, released.Load())
}

func TestCollectionUsageReleaseConcurrencyUpdatesNextBatch(t *testing.T) {
	enableAutoReleaseForTest(t)
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseConcurrency, "1")
	_, manager, releaseMock := newUsageTestManager(t)
	gates := make(map[int64]chan struct{}, 6)
	var gateClosers [6]sync.Once
	for id := int64(1); id <= 6; id++ {
		gates[id] = make(chan struct{})
	}
	unblock := func(id int64) { gateClosers[id-1].Do(func() { close(gates[id]) }) }
	t.Cleanup(func() {
		for id := int64(1); id <= 6; id++ {
			unblock(id)
		}
	})
	started := make(chan int64, 6)
	releaseMock.To(func(_ *collectionUsageManager, _ context.Context, id int64, _ collectionReleaseReason) error {
		started <- id
		<-gates[id]
		return nil
	})
	next := func() int64 {
		t.Helper()
		select {
		case id := <-started:
			return id
		case <-time.After(time.Second):
			t.Fatal("release task did not start")
			return 0
		}
	}
	completed := startReleaseBatch(manager, 1, 2, 3)
	require.EqualValues(t, 1, next())
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoReleaseConcurrency.Key, "3"))
	select {
	case <-started:
		t.Fatal("concurrency update changed the running batch's limit")
	case <-time.After(50 * time.Millisecond):
	}
	for id := int64(1); id <= 2; id++ {
		unblock(id)
		require.EqualValues(t, id+1, next())
	}
	unblock(3)
	waitForReleaseBatch(t, completed)

	completed = startReleaseBatch(manager, 4, 5, 6)
	require.ElementsMatch(t, []int64{4, 5, 6}, []int64{next(), next(), next()})
	for id := int64(4); id <= 6; id++ {
		unblock(id)
	}
	waitForReleaseBatch(t, completed)
}

func TestCollectionUsageCloseCancelsReleasesAndUnblocksScan(t *testing.T) {
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseConcurrency, "1")
	manager, _, releaseMock, _ := newLRUUsageManager(t, 5)
	started := make(chan int64, 5)
	releaseMock.To(func(_ *collectionUsageManager, ctx context.Context, id int64, _ collectionReleaseReason) error {
		started <- id
		<-ctx.Done()
		return ctx.Err()
	})
	setUsageParameter(t, &paramtable.Get().QueryCoordCfg.AutoReleaseCheckInterval, "1")
	manager.start(context.Background())
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("release task did not start")
	}
	closed := make(chan struct{})
	go func() {
		manager.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not unblock the scan waiting for the release batch")
	}
	require.Empty(t, started, "pending collections must not start releasing during shutdown")
}
