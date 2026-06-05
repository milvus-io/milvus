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
)

type fakeReplicaAllocator struct {
	nodes map[int64][]int64
}

func (f *fakeReplicaAllocator) AssignNodes(ctx context.Context, cfg *LoadConfig) (*LoadConfig, error) {
	next := cfg.Clone()
	for _, replica := range next.Replicas {
		replica.Nodes = append([]int64{}, f.nodes[replica.ReplicaID]...)
	}
	return next, nil
}

type fakeShardProvider struct {
	vchannels []string
}

func (f *fakeShardProvider) VChannels(ctx context.Context, collectionID int64) ([]string, error) {
	return append([]string{}, f.vchannels...), nil
}

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

func TestCollectionLoadManager_UpdateLoadConfig(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	store := newEmptyLoadConfigStore(t, catalog)
	var ensured []qviews.ShardID
	var notified []int64
	manager := NewCollectionLoadManager(
		store,
		func(shardID qviews.ShardID) { ensured = append(ensured, shardID) },
		&fakeReplicaAllocator{nodes: map[int64][]int64{
			1000: {1, 2},
			1001: {3, 4},
		}},
		&fakeShardProvider{vchannels: []string{"v0", "v1"}},
		func(collectionID int64) { notified = append(notified, collectionID) },
	)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigHeader()))

	cfg := store.Snapshot().ConfigsMap()[100]
	require.NotNil(t, cfg)
	assert.ElementsMatch(t, []int64{1, 2}, cfg.Replicas[0].Nodes)
	assert.ElementsMatch(t, []qviews.ShardID{
		{ReplicaID: 1000, VChannel: "v0"},
		{ReplicaID: 1000, VChannel: "v1"},
		{ReplicaID: 1001, VChannel: "v0"},
		{ReplicaID: 1001, VChannel: "v1"},
	}, ensured)
	assert.Equal(t, []int64{100}, notified)
}

func TestCollectionLoadManager_ReleaseCollectionKeepsRegistryForReconcile(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	store := newEmptyLoadConfigStore(t, catalog)
	var ensured []qviews.ShardID
	var notified []int64
	manager := NewCollectionLoadManager(
		store,
		func(shardID qviews.ShardID) { ensured = append(ensured, shardID) },
		&fakeReplicaAllocator{nodes: map[int64][]int64{1000: {1}, 1001: {2}}},
		&fakeShardProvider{vchannels: []string{"v0"}},
		func(collectionID int64) { notified = append(notified, collectionID) },
	)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, manager.UpdateLoadConfig(context.Background(), sampleAlterLoadConfigHeader()))

	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, manager.ReleaseCollection(context.Background(), &messagespb.DropLoadConfigMessageHeader{
		CollectionId: 100,
	}))

	assert.NotContains(t, store.Snapshot().ConfigsMap(), int64(100))
	assert.NotEmpty(t, ensured, "release keeps actual shard lifecycle to Balancer reconciliation")
	assert.Equal(t, []int64{100, 100}, notified)
}
