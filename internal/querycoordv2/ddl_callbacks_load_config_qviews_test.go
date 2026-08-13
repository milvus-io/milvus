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
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadCollectionBroadcastsLoadConfigToControlChannel(t *testing.T) {
	ctx := context.Background()
	server, catalog, broker := newLoadConfigQViewsServer(t)
	collectionID := int64(100)
	vchannels := []string{"v0", "v1"}
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(testDescribeCollection(collectionID, vchannels), nil).Twice()
	broker.EXPECT().GetPartitions(mock.Anything, collectionID).
		Return([]int64{10, 20}, nil).Once()
	broker.EXPECT().GetCollectionLoadInfo(mock.Anything, collectionID).
		Return([]string{meta.DefaultResourceGroupName}, int64(1), nil).Once()

	var captured message.BroadcastMutableMessage
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) {
		captured = msg
	})

	require.NoError(t, server.broadcastAlterLoadConfigCollectionV2ForLoadCollection(ctx, &querypb.LoadCollectionRequest{
		CollectionID: collectionID,
	}))
	require.NotNil(t, captured)
	assert.Equal(t, []string{streaming.WAL().ControlChannel()}, captured.BroadcastHeader().VChannels)
	assert.False(t, captured.BroadcastHeader().AckSyncUp)
	assert.Empty(t, server.meta.GetAll(ctx))
	catalog.AssertNotCalled(t, "SaveCollection", mock.Anything, mock.Anything)
}

func TestLoadPartitionsBroadcastsLoadConfigToControlChannel(t *testing.T) {
	ctx := context.Background()
	server, _, broker := newLoadConfigQViewsServer(t)
	collectionID := int64(100)
	vchannels := []string{"v0", "v1"}
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(testDescribeCollection(collectionID, vchannels), nil).Twice()
	broker.EXPECT().GetCollectionLoadInfo(mock.Anything, collectionID).
		Return([]string{meta.DefaultResourceGroupName}, int64(1), nil).Once()

	var captured message.BroadcastMutableMessage
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) {
		captured = msg
	})

	require.NoError(t, server.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(ctx, &querypb.LoadPartitionsRequest{
		CollectionID: collectionID,
		PartitionIDs: []int64{10},
	}))
	require.NotNil(t, captured)
	assert.Equal(t, []string{streaming.WAL().ControlChannel()}, captured.BroadcastHeader().VChannels)
	assert.False(t, captured.BroadcastHeader().AckSyncUp)
}

func TestReleaseCollectionUsesQViewsLoadConfigAsLoadedSource(t *testing.T) {
	ctx := context.Background()
	server, catalog, broker := newLoadConfigQViewsServer(t)
	collectionID := int64(100)
	vchannels := []string{"v0", "v1"}
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, server.qviewsRuntime.loadConfigStore.Put(ctx, &loadmgr.LoadConfig{
		DbID:         1,
		CollectionID: collectionID,
		PartitionIDs: []int64{10, 20},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 1000, ResourceGroup: meta.DefaultResourceGroupName, Priority: commonpb.LoadPriority_HIGH},
		},
	}))
	require.False(t, server.meta.Exist(ctx, collectionID))

	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(testDescribeCollection(collectionID, vchannels), nil).Twice()
	var captured message.BroadcastMutableMessage
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) {
		captured = msg
	})

	require.NoError(t, server.broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
		CollectionID: collectionID,
	}))
	require.NotNil(t, captured)
	assert.Equal(t, []string{streaming.WAL().ControlChannel()}, captured.BroadcastHeader().VChannels)
	assert.False(t, captured.BroadcastHeader().AckSyncUp)
}

func TestReleasePartitionsDropLoadConfigBroadcastsToControlChannel(t *testing.T) {
	ctx := context.Background()
	server, catalog, broker := newLoadConfigQViewsServer(t)
	collectionID := int64(100)
	vchannels := []string{"v0", "v1"}
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, server.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			Status:       querypb.LoadStatus_Loaded,
			LoadType:     querypb.LoadType_LoadPartition,
			LoadFields:   []int64{100},
			FieldIndexID: map[int64]int64{100: 200},
		},
	}, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  10,
			Status:       querypb.LoadStatus_Loaded,
		},
	}))

	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(testDescribeCollection(collectionID, vchannels), nil).Twice()
	var captured message.BroadcastMutableMessage
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) {
		captured = msg
	})

	collectionReleased, err := server.broadcastAlterLoadConfigCollectionV2ForReleasePartitions(ctx, &querypb.ReleasePartitionsRequest{
		CollectionID: collectionID,
		PartitionIDs: []int64{10},
	})
	require.NoError(t, err)
	require.True(t, collectionReleased)
	require.NotNil(t, captured)
	assert.Equal(t, []string{streaming.WAL().ControlChannel()}, captured.BroadcastHeader().VChannels)
	assert.False(t, captured.BroadcastHeader().AckSyncUp)
}

func newLoadConfigQViewsServer(t *testing.T) (*Server, *metastoremocks.QueryCoordCatalog, *meta.MockBroker) {
	t.Helper()
	paramtable.Init()
	initStreamingSystem()

	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()
	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(context.Background(), qviewsRuntimeDependencies{
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

	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(func() (int64, error) { return 1000, nil }, catalog, nodeMgr)
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:  1,
		Address: "localhost",
	}))
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Once()
	m.HandleNodeUp(context.Background(), 1)

	broker := meta.NewMockBroker(t)
	return &Server{
		meta:          m,
		broker:        broker,
		qviewsRuntime: runtime,
	}, catalog, broker
}

func registerCaptureBroadcast(t *testing.T, capture func(message.BroadcastMutableMessage)) {
	t.Helper()
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			capture(msg)
			return &types.BroadcastAppendResult{}, nil
		},
	).Once()
	bapi.EXPECT().Close().Return().Once()
	mb := mock_broadcaster.NewMockBroadcaster(t)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).Return(bapi, nil).Once()
	mb.EXPECT().Close().Return().Maybe()
	broadcast.ResetBroadcaster()
	broadcast.Register(mb)
	t.Cleanup(broadcast.ResetBroadcaster)
}

func testDescribeCollection(collectionID int64, vchannels []string) *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		DbName:              "default",
		DbId:                1,
		CollectionID:        collectionID,
		CollectionName:      "test_collection",
		VirtualChannelNames: append([]string{}, vchannels...),
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100},
			},
		},
	}
}
