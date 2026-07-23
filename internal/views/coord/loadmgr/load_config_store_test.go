package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func newTestStore(t *testing.T) (*LoadConfigStore, *mocks.QueryCoordCatalog) {
	t.Helper()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	return store, catalog
}

func sampleConfig() *LoadConfig {
	return &LoadConfig{
		DbID:         1,
		CollectionID: 100,
		PartitionIDs: []int64{10, 20},
		LoadFields: []*messagespb.LoadFieldConfig{
			{FieldId: 200, IndexId: 300},
		},
		Replicas: []*ReplicaAssignment{
			{ReplicaID: 1000, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
			{ReplicaID: 1001, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
		},
	}
}

// expectFullSave sets expectations for a Put that writes the whole config.
// Uses .Times(n) so tests can chain multiple Puts in a single catalog setup.
func expectFullSave(catalog *mocks.QueryCoordCatalog, times int) {
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Times(times)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Times(times)
}

func TestPut_NewCollection(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()
	before := store.Snapshot()

	expectFullSave(catalog, 1)

	require.NoError(t, store.Put(context.Background(), cfg))
	after := store.Snapshot()
	require.NotSame(t, before, after)

	got := after.ConfigsMap()[cfg.CollectionID]
	require.NotNil(t, got)
	assert.Equal(t, cfg.CollectionID, got.CollectionID)
	assert.ElementsMatch(t, cfg.PartitionIDs, got.PartitionIDs)
	assert.Len(t, got.Replicas, 2)
}

func TestPut_PersistsReplicaNumberFromReplicaAssignments(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()

	var saved querypb.CollectionLoadInfo
	catalog.EXPECT().
		SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, collection *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) {
			saved = *collection
		}).
		Return(nil).
		Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	require.NoError(t, store.Put(context.Background(), cfg))
	assert.Equal(t, int32(len(cfg.Replicas)), saved.GetReplicaNumber())
}

func TestLoadConfigPersistedStatusIsLoaded(t *testing.T) {
	cfg := sampleConfig()

	collection := cfg.toCollectionLoadInfoProto()
	assert.Equal(t, querypb.LoadStatus_Loaded, collection.GetStatus())

	partitions := cfg.toPartitionLoadInfoProtos()
	require.Len(t, partitions, len(cfg.PartitionIDs))
	for _, partition := range partitions {
		assert.Equal(t, querypb.LoadStatus_Loaded, partition.GetStatus())
	}
}

func TestPut_UpdateExistingWritesFullConfig(t *testing.T) {
	store, catalog := newTestStore(t)

	// First Put writes full config.
	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), sampleConfig()))

	// Second Put with the same config still writes everything (no dedup).
	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), sampleConfig()))

	catalog.AssertExpectations(t)
}

func TestPut_PartitionRemovedDeletesOrphans(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig() // partitions {10, 20}

	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), cfg))

	// Remove partition 10, add partition 30.
	next := cfg.Clone()
	next.PartitionIDs = []int64{20, 30}

	catalog.EXPECT().ReleasePartition(mock.Anything, int64(100), int64(10)).
		Return(nil).Once()
	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), next))

	got := store.Snapshot().ConfigsMap()[cfg.CollectionID]
	assert.ElementsMatch(t, []int64{20, 30}, got.PartitionIDs)
}

func TestPut_ReplicaRemovedDeletesOrphans(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()

	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), cfg))

	// Drop replica 1001, change replica 1000, add replica 1002.
	next := cfg.Clone()
	next.Replicas = []*ReplicaAssignment{
		{ReplicaID: 1000, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
		{ReplicaID: 1002, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
	}

	catalog.EXPECT().ReleaseReplica(mock.Anything, int64(100), int64(1001)).
		Return(nil).Once()
	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), next))

	snapshot := store.Snapshot()
	assert.NotContains(t, snapshot.ReplicaToConfigMap(), int64(1001))
	assert.Contains(t, snapshot.ReplicaToConfigMap(), int64(1000))
	assert.Contains(t, snapshot.ReplicaToConfigMap(), int64(1002))
}

func TestPut_EmptyReplicasSkipsSaveReplica(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()
	cfg.Replicas = nil

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	// No SaveReplica expected when Replicas is empty.
	require.NoError(t, store.Put(context.Background(), cfg))
}

func TestRemove(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()

	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), cfg))

	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()

	require.NoError(t, store.Remove(context.Background(), 100))

	snapshot := store.Snapshot()
	assert.NotContains(t, snapshot.ConfigsMap(), int64(100))
	assert.NotContains(t, snapshot.ReplicaToConfigMap(), int64(1000))
	assert.NotContains(t, snapshot.ReplicaToConfigMap(), int64(1001))
}

func TestRemove_NonExistent(t *testing.T) {
	store, _ := newTestStore(t)
	require.NoError(t, store.Remove(context.Background(), 999))
}

func TestSnapshot_ReturnsAllConfigs(t *testing.T) {
	store, catalog := newTestStore(t)
	expectFullSave(catalog, 2)

	cfg1 := sampleConfig()
	cfg2 := sampleConfig()
	cfg2.CollectionID = 101
	cfg2.Replicas = []*ReplicaAssignment{
		{ReplicaID: 2000, ResourceGroup: "rg2"},
	}

	require.NoError(t, store.Put(context.Background(), cfg1))
	require.NoError(t, store.Put(context.Background(), cfg2))

	assert.Len(t, store.Snapshot().ConfigsMap(), 2)
}

func TestSnapshot_LazilyRefreshesResidentSnapshot(t *testing.T) {
	store, catalog := newTestStore(t)

	s1 := store.Snapshot()
	s2 := store.Snapshot()
	require.Same(t, s1, s2)

	expectFullSave(catalog, 1)
	require.NoError(t, store.Put(context.Background(), sampleConfig()))
	require.Same(t, s1, store.snapshot, "Put should only advance version and keep the cached snapshot stale")

	s3 := store.Snapshot()
	require.NotSame(t, s1, s3)
	require.Same(t, s3, store.Snapshot())
	assert.Equal(t, uint64(2), s3.Version())
	assert.Contains(t, s3.ConfigsMap(), int64(100))
}

func TestSnapshot_CoalescesMultipleMutations(t *testing.T) {
	store, catalog := newTestStore(t)
	s1 := store.Snapshot()

	expectFullSave(catalog, 2)
	require.NoError(t, store.Put(context.Background(), sampleConfig()))

	next := sampleConfig()
	next.CollectionID = 101
	next.Replicas = []*ReplicaAssignment{
		{ReplicaID: 2000, ResourceGroup: "rg2"},
	}
	require.NoError(t, store.Put(context.Background(), next))

	require.Same(t, s1, store.snapshot)
	s2 := store.Snapshot()
	require.NotSame(t, s1, s2)
	assert.Equal(t, uint64(3), s2.Version())
	assert.Contains(t, s2.ConfigsMap(), int64(100))
	assert.Contains(t, s2.ConfigsMap(), int64(101))
	require.Same(t, s2, store.Snapshot())
}

func TestSnapshot_TracksLoadInfoVersionPerCollection(t *testing.T) {
	store, catalog := newTestStore(t)

	expectFullSave(catalog, 2)
	cfg1 := sampleConfig()
	cfg2 := sampleConfig()
	cfg2.CollectionID = 101
	cfg2.Replicas = []*ReplicaAssignment{
		{ReplicaID: 2000, ResourceGroup: "rg2"},
	}

	require.NoError(t, store.Put(context.Background(), cfg1))
	first := store.Snapshot()
	assert.Equal(t, uint64(2), first.ConfigVersion(cfg1.CollectionID))

	require.NoError(t, store.Put(context.Background(), cfg2))
	second := store.Snapshot()
	assert.Equal(t, uint64(2), second.ConfigVersion(cfg1.CollectionID))
	assert.Equal(t, uint64(3), second.ConfigVersion(cfg2.CollectionID))
}

func TestRecoverLoadConfigStore_EmptyState(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	assert.Empty(t, store.Snapshot().ConfigsMap())
}

func TestRecoverLoadConfigStore_WithPersistedData(t *testing.T) {
	catalog := mocks.NewQueryCoordCatalog(t)

	collections := []*querypb.CollectionLoadInfo{
		{
			CollectionID:             100,
			DbID:                     1,
			LoadFields:               []int64{200},
			FieldIndexID:             map[int64]int64{200: 300},
			UserSpecifiedReplicaMode: true,
		},
	}
	partitions := map[int64][]*querypb.PartitionLoadInfo{
		100: {
			{CollectionID: 100, PartitionID: 10},
			{CollectionID: 100, PartitionID: 20},
		},
	}
	replicas := []*querypb.Replica{
		{ID: 1000, CollectionID: 100, ResourceGroup: "rg1"},
		{ID: 1001, CollectionID: 100, ResourceGroup: "rg1"},
	}

	catalog.EXPECT().GetCollections(mock.Anything).Return(collections, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(partitions, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(replicas, nil).Once()

	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)

	snapshot := store.Snapshot()
	cfg := snapshot.ConfigsMap()[100]
	require.NotNil(t, cfg)
	assert.Equal(t, int64(1), cfg.DbID)
	assert.True(t, cfg.UserSpecifiedReplicaMode)
	assert.ElementsMatch(t, []int64{10, 20}, cfg.PartitionIDs)
	require.Len(t, cfg.LoadFields, 1)
	assert.Equal(t, int64(200), cfg.LoadFields[0].FieldId)
	assert.Equal(t, int64(300), cfg.LoadFields[0].IndexId)
	require.Len(t, cfg.Replicas, 2)

	assert.NotNil(t, snapshot.ReplicaToConfigMap()[1000])
	assert.NotNil(t, snapshot.ReplicaToConfigMap()[1001])
}

func TestFromAlterLoadConfigMessage(t *testing.T) {
	msg := &messagespb.AlterLoadConfigMessageHeader{
		DbId:                     1,
		CollectionId:             100,
		PartitionIds:             []int64{10, 20},
		LoadFields:               []*messagespb.LoadFieldConfig{{FieldId: 200, IndexId: 300}},
		UserSpecifiedReplicaMode: true,
		Replicas: []*messagespb.LoadReplicaConfig{
			{ReplicaId: 1000, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
		},
	}

	cfg := FromAlterLoadConfigMessage(msg)
	assert.Equal(t, int64(1), cfg.DbID)
	assert.Equal(t, int64(100), cfg.CollectionID)
	assert.ElementsMatch(t, []int64{10, 20}, cfg.PartitionIDs)
	assert.True(t, cfg.UserSpecifiedReplicaMode)
	require.Len(t, cfg.Replicas, 1)
	assert.Equal(t, int64(1000), cfg.Replicas[0].ReplicaID)
	assert.Equal(t, "rg1", cfg.Replicas[0].ResourceGroup)
}
