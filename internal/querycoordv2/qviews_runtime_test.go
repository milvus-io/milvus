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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
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

	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: "v0"}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{testPersistedQueryView(100, shardID)},
		},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
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
}

func TestNewQViewsRuntimeUsesDefaultRowBalanceConfig(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	var config *balancer.BalanceConfig
	_, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog:    catalog,
		queryViewCatalog:     &fakeQueryViewCatalog{},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(builder *balancer.SnapshotBuilder) qviewsBalancer {
			config = builder.Build(ctx).Config
			return &fakeRuntimeBalancer{}
		},
	})
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Positive(t, config.StickinessWeight)
	assert.Positive(t, config.NodeLoadWeight)
	assert.Positive(t, config.FanoutWeight)
	assert.Positive(t, config.StickyRowsScale)
	assert.Positive(t, config.TargetRowsPerShardNode)
}

type fakeRuntimeDataViewReferences struct {
	recovered []qviews.DataVersion
}

func (r *fakeRuntimeDataViewReferences) PinDataView(context.Context, int64, qviews.DataVersion) error {
	return nil
}

func (r *fakeRuntimeDataViewReferences) RecoverDataViewReference(_ context.Context, _ int64, version qviews.DataVersion) (bool, error) {
	r.recovered = append(r.recovered, version)
	return true, nil
}

func (r *fakeRuntimeDataViewReferences) UnpinDataView(int64, qviews.DataVersion) {}

func TestQViewsRuntimePassesReferenceManagerToRegistry(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()
	refs := &fakeRuntimeDataViewReferences{}

	_, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{views: []*viewpb.QueryViewOfShard{
			testPersistedQueryView(100, qviews.ShardID{ReplicaID: 1000, VChannel: "v0"}),
		}},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		dataViewReferences:   refs,
	})
	require.NoError(t, err)
	require.Equal(t, []qviews.DataVersion{{StreamingVersion: 1, CompactVersion: 1}}, refs.recovered)
}

func TestQViewsRuntimeLoadManagerEnsuresShardsAndTriggersBalancer(t *testing.T) {
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
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, runtime.loadManager.UpdateLoadConfig(ctx, testAlterLoadConfigResult()))

	assert.NotNil(t, runtime.shardViewRegistry.Get(qviews.ShardID{ReplicaID: 1000, VChannel: "v0"}))
	assert.NotNil(t, runtime.shardViewRegistry.Get(qviews.ShardID{ReplicaID: 1000, VChannel: "v1"}))
	assert.Equal(t, []balancer.TriggerScope{{DirtyCollections: []int64{100}}}, fakeBalancer.triggers)
}

func testPersistedQueryView(collectionID int64, shardID qviews.ShardID) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: collectionID,
			ReplicaId:    shardID.ReplicaID,
			Vchannel:     shardID.VChannel,
			State:        viewpb.QueryViewState_QueryViewStateUp,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				QueryVersion: 1,
			},
		},
	}
}

func testAlterLoadConfigResult() message.BroadcastResultAlterLoadConfigMessageV2 {
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
		WithBroadcast([]string{"v0", "v1", "by-dev-rootcoord-dml" + funcutil.ControlChannelSuffix}).
		MustBuildBroadcast()
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{
			"v0": {},
			"v1": {},
			"by-dev-rootcoord-dml" + funcutil.ControlChannelSuffix: {},
		},
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
	return nil, nil
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

func (p *fakeRuntimeDataViewProvider) DataViewSnapshot(context.Context) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

type fakeRuntimeBalancer struct {
	started  bool
	stopped  bool
	triggers []balancer.TriggerScope
}

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
