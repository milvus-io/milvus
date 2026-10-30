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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNewQViewsRuntimeRecoversLoadConfigAndQueryViews(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{
		{DbID: 1, CollectionID: 100},
	}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, []int64{100}).Return(map[int64][]*querypb.PartitionLoadInfo{
		100: {{CollectionID: 100, PartitionID: 10}},
	}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{
		{ID: 1000, CollectionID: 100, ResourceGroup: "rg1"},
	}, nil).Once()

	detached := 0
	data := api.PrepareCollectionDataView(&api.CollectionDataView{CollectionID: 100, DataVersion: qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, Shards: []*api.ShardDataView{{VChannel: "v0", GrowingTransformStartAfterTimetick: 10, CompactedTransformStartAfterTimetick: 10}}})
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*fakeRuntimeDataViewProvider).RegisterDataViewListener).To(func(_ *fakeRuntimeDataViewProvider, listener api.DataViewListener) func() {
			listener(100, data)
			return func() { detached++ }
		}).Build(),
		mockey.Mock((*fakeRuntimeQueryNodeClient).RegisterQueryNodeListener).To(func(_ *fakeRuntimeQueryNodeClient, _ context.Context, listener func(int64, *qnmanager.NodeInfo)) (func(), error) {
			listener(10, &qnmanager.NodeInfo{ServerID: 10, ServerLabels: map[string]string{sessionutil.LabelResourceGroup: "rg1"}})
			return func() { detached++ }, nil
		}).Build(),
		mockey.Mock((*fakeRuntimeResourceGroupManager).RegisterResourceGroupListener).To(func(_ *fakeRuntimeResourceGroupManager, listener func(string, []int64)) func() {
			listener("rg1", []int64{10})
			return func() { detached++ }
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: "v0"}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{testPersistedQueryView(100, shardID)},
		},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     newRuntimeDataViewProvider(t),
	})
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotNil(t, runtime.loadManager)
	require.NotNil(t, runtime.balancer)

	configs := runtime.loadConfigStore.Snapshot().ConfigsMap()
	require.Contains(t, configs, int64(100))
	assert.Equal(t, []int64{10}, configs[100].PartitionIDs)
	require.NotNil(t, runtime.shardViewRegistry.Get(shardID))
	assert.Contains(t, runtime.shardViewRegistry.Snapshot().StatsMap(), shardID)
	require.NoError(t, runtime.balanceCache.WaitForReady(ctx))
	require.Same(t, data, runtime.balanceCache.GetCollection(100).DataView())
	require.Equal(t, "rg1", runtime.balanceCache.GetNode(10).Info().ResourceGroup)
	runtime.stop()
	runtime.stop()
	require.Equal(t, 3, detached)
}

func TestQViewsRuntimeLoadManagerTriggersBalancer(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog:    catalog,
		queryViewCatalog:     &fakeQueryViewCatalog{},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     newRuntimeDataViewProvider(t),
		balancerFactory: func(*cache.Cache) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, runtime.loadManager.UpdateLoadConfig(ctx, testAlterLoadConfigResult()))

	assert.Empty(t, runtime.shardViewRegistry.ShardIDs())
	assert.Equal(t, []balancer.TriggerScope{{DirtyCollections: []int64{100}}}, fakeBalancer.triggers)
}

func testPersistedQueryView(collectionID int64, shardID qviews.ShardID) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId:    collectionID,
			ReplicaId:       shardID.ReplicaID,
			Vchannel:        shardID.VChannel,
			State:           viewpb.QueryViewState_QueryViewStateUp,
			LoadInfoVersion: 1,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				QueryVersion: 1,
			},
		},
	}
}

func testAlterLoadConfigResult() message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := funcutil.GetControlChannel("test")
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			DbId:         1,
			CollectionId: 100,
			PartitionIds: []int64{10},
			Replicas: []*messagespb.LoadReplicaConfig{
				{ReplicaId: 1000, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
			},
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{controlChannel: {}},
	}
}

type fakeQueryViewCatalog struct {
	views []*viewpb.QueryViewOfShard
	saves [][]*viewpb.QueryViewOfShard
}

func (c *fakeQueryViewCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return c.views, nil
}

func (c *fakeQueryViewCatalog) SaveQueryViews(_ context.Context, views []*viewpb.QueryViewOfShard) error {
	c.saves = append(c.saves, views)
	return nil
}

type fakeRuntimeViewSyncClient struct{}

func (c *fakeRuntimeViewSyncClient) RegisterNodeChangedNotifier(func()) {}
func (c *fakeRuntimeViewSyncClient) IsNodeAlive(context.Context, qviews.WorkNode) bool {
	return true
}

func (c *fakeRuntimeViewSyncClient) OpenSyncStream(context.Context, qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return nil, merr.WrapErrServiceUnavailableMsg("test stream is not connected")
}
func (c *fakeRuntimeViewSyncClient) Close() {}

type fakeRuntimeQueryNodeClient struct{}

func (c *fakeRuntimeQueryNodeClient) RegisterNodeChangedNotifier(func()) {}
func (c *fakeRuntimeQueryNodeClient) GetAllQueryNodes(context.Context) (map[int64]*qnmanager.NodeInfo, error) {
	return nil, nil
}

type fakeRuntimeResourceGroupManager struct{}

func (m *fakeRuntimeResourceGroupManager) ListResourceGroups(context.Context) []string {
	return nil
}

func (m *fakeRuntimeResourceGroupManager) GetNodes(context.Context, string) ([]int64, error) {
	return nil, nil
}

type fakeRuntimeDataViewProvider struct{}

func (p *fakeRuntimeDataViewProvider) RegisterDataViewListener(api.DataViewListener) func() {
	return func() {}
}

func (p *fakeRuntimeDataViewProvider) Get(context.Context, int64, *viewpb.DataVersion) (qviews.DataViewRef, error) {
	panic("patch using mockey")
}

// Immutable reference fixture; persistence and GC ownership are tested with the
// real DataView manager in coordview/dataview_integration_test.go.
type runtimeDataViewRef struct{ view *viewpb.DataViewOfCollection }

func (r *runtimeDataViewRef) DataView() *viewpb.DataViewOfCollection { return r.view }
func (r *runtimeDataViewRef) Version() *viewpb.DataVersion           { return r.view.DataVersion }
func (r *runtimeDataViewRef) Stats(int64) (qviews.SegmentStats, bool) {
	return qviews.SegmentStats{}, false
}
func (r *runtimeDataViewRef) Deref() {}

func newRuntimeDataViewProvider(t *testing.T) *fakeRuntimeDataViewProvider {
	t.Helper()
	patch := mockey.Mock((*fakeRuntimeDataViewProvider).Get).To(func(_ *fakeRuntimeDataViewProvider, _ context.Context, id int64, version *viewpb.DataVersion) (qviews.DataViewRef, error) {
		return &runtimeDataViewRef{view: &viewpb.DataViewOfCollection{CollectionId: id, DataVersion: qviews.FromProtoDataVersion(version).IntoProto()}}, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return &fakeRuntimeDataViewProvider{}
}

type fakeRuntimeBalancer struct {
	started  bool
	stopped  bool
	triggers []balancer.TriggerScope
}

func (b *fakeRuntimeBalancer) Reconcile(context.Context) error { return nil }

func (b *fakeRuntimeBalancer) Start(context.Context) {
	b.started = true
}

func (b *fakeRuntimeBalancer) Stop() {
	b.stopped = true
}

func (b *fakeRuntimeBalancer) Trigger(scopes ...balancer.TriggerScope) {
	b.triggers = append(b.triggers, scopes...)
}

var (
	_ syncer.ViewSyncClient           = (*fakeRuntimeViewSyncClient)(nil)
	_ balancer.DataViewProvider       = (*fakeRuntimeDataViewProvider)(nil)
	_ loadmgr.DirtyCollectionNotifier = func(int64) {}
)

func (c *fakeRuntimeQueryNodeClient) RegisterQueryNodeListener(context.Context, func(int64, *qnmanager.NodeInfo)) (func(), error) {
	return func() {}, nil
}

func (m *fakeRuntimeResourceGroupManager) RegisterResourceGroupListener(func(string, []int64)) func() {
	return func() {}
}
