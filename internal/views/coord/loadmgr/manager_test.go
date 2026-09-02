package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func newEmptyLoadConfigStore(t *testing.T, catalog *mocks.QueryCoordCatalog) *LoadConfigStore {
	t.Helper()
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()
	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	return store
}

func sampleAlterLoadConfigHeader() *messagespb.AlterLoadConfigMessageHeader {
	return &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     1,
		CollectionId:             100,
		PartitionIds:             []int64{10},
		LoadFields:               []*messagespb.LoadFieldConfig{{FieldId: 200, IndexId: 300}},
		UserSpecifiedReplicaMode: true,
		Replicas: []*messagespb.LoadReplicaConfig{
			{ReplicaId: 1000, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaId: 1001, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
		},
	}
}

func sampleAlterLoadConfigResult() message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := funcutil.GetControlChannel("test")
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(sampleAlterLoadConfigHeader()).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{controlChannel: {}},
	}
}

func TestCollectionLoadManager_UpdateLoadConfig(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	store := newEmptyLoadConfigStore(t, catalog)
	var notified []int64
	manager := NewCollectionLoadManager(
		store,
		func(collectionID int64) { notified = append(notified, collectionID) },
	)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigResult()))

	cfg := store.Snapshot().ConfigsMap()[100]
	require.NotNil(t, cfg)
	require.Len(t, cfg.Replicas, 2)
	assert.Equal(t, int64(1000), cfg.Replicas[0].ReplicaID)
	assert.Equal(t, "rg1", cfg.Replicas[0].ResourceGroup)
	assert.Equal(t, []int64{100}, notified)
}

func TestCollectionLoadManager_ReleaseCollectionKeepsRegistryForReconcile(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	store := newEmptyLoadConfigStore(t, catalog)
	var notified []int64
	manager := NewCollectionLoadManager(
		store,
		func(collectionID int64) { notified = append(notified, collectionID) },
	)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigResult()))

	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, manager.ReleaseCollection(context.Background(), &messagespb.DropLoadConfigMessageHeader{
		CollectionId: 100,
	}))

	assert.NotContains(t, store.Snapshot().ConfigsMap(), int64(100))
	assert.Equal(t, []int64{100, 100}, notified)
}

func TestCollectionLoadManager_DiscoverableShardAssignments(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	store := newEmptyLoadConfigStore(t, catalog)
	var assignmentUpdates int
	manager := NewCollectionLoadManager(store, nil)
	manager.SetShardAssignmentNotifier(func() { assignmentUpdates++ })

	shardID := qviews.ShardID{
		ReplicaID: 1000,
		VChannel:  "by-dev-rootcoord-dml_0_100v2",
	}
	manager.ObserveShardUp(shardID)
	manager.ObserveShardUp(shardID)

	assignments := manager.ShardAssignmentsByPChannel()
	require.Len(t, assignments, 1)
	assert.Equal(t, []types.ShardAssignmentEntry{
		{CollectionID: 100, ShardIndex: 2, ReplicaID: 1000},
	}, assignments["by-dev-rootcoord-dml_0"])
	assert.Equal(t, 1, assignmentUpdates)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, store.Put(context.Background(), &LoadConfig{CollectionID: 100}))
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, manager.ReleaseCollection(context.Background(), &messagespb.DropLoadConfigMessageHeader{
		CollectionId: 100,
	}))
	assert.Empty(t, manager.ShardAssignmentsByPChannel())
	assert.Equal(t, 2, assignmentUpdates)
}
