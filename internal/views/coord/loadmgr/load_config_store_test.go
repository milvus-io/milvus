package loadmgr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func newTestStore(t *testing.T) (*LoadConfigStore, *versionedTestHooks) {
	t.Helper()
	hooks := new(versionedTestHooks)
	deps := versionedTestDependencies(t, hooks)
	deps.Schema = func(context.Context, int64) (*schemapb.CollectionSchema, error) {
		return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 200}}}, nil
	}
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	return store, hooks
}

func sampleConfig() *LoadConfig {
	return &LoadConfig{
		DbID: 1, CollectionID: 100, PartitionIDs: []int64{10, 20},
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 200}},
		LoadFields:     []*viewpb.LoadFieldConfig{{FieldId: 200}},
		Replicas:       []*ReplicaAssignment{{ReplicaID: 1000, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH}},
	}
}

func TestLoadConfigImmutablePublicationAndRecovery(t *testing.T) {
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := sampleConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	entry := store.Get(100)
	require.Equal(t, entry.Config.Version, entry.ConfigVersion)
	require.NotZero(t, entry.ConfigVersion)
	cfg.PartitionIDs[0] = 999
	cfg.Replicas[0].ResourceGroup = "mutation"
	require.Equal(t, []int64{10, 20}, entry.Config.PartitionIDs)
	require.Equal(t, "rg1", entry.Config.Replicas[0].ResourceGroup)
	recovered, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.Equal(t, entry, recovered.Get(100))
	record := entry.Config.toCollectionLoadConfigProto()
	require.Len(t, record.Replicas, len(cfg.Replicas))
	require.Equal(t, commonpb.LoadPriority_HIGH, record.Replicas[0].Priority)
}

func TestLoadConfigSnapshotOwnershipAndWithdrawal(t *testing.T) {
	store, _ := newTestStore(t)
	cfg := sampleConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.Snapshot()
	require.Same(t, first, store.Snapshot())
	cfg.PartitionIDs = []int64{20}
	require.NoError(t, store.Put(t.Context(), cfg))
	require.True(t, store.Contains(100))
	require.Same(t, first, store.snapshot, "hot getters do not rebuild the snapshot")
	second := store.Snapshot()
	require.NotSame(t, first, second)
	require.Equal(t, []int64{10, 20}, first.ConfigsMap()[100].PartitionIDs)
	require.Equal(t, []int64{20}, second.ConfigsMap()[100].PartitionIDs)
	require.Same(t, second.ConfigsMap()[100], second.ReplicaToConfigMap()[1000])
	store.StopPlanning(100)
	require.False(t, store.Contains(100))
	require.Empty(t, store.Snapshot().ConfigsMap())
	require.NoError(t, store.Remove(t.Context(), 100))
	require.NoError(t, store.Remove(t.Context(), 100))
	require.Equal(t, []int64{20}, second.ConfigsMap()[100].PartitionIDs)
}

func TestLoadConfigPersistenceDoesNotBlockReadsOrOtherCollections(t *testing.T) {
	store, hooks := newTestStore(t)
	blocked, proceed := make(chan struct{}), make(chan struct{})
	hooks.beforeSave = func(info *viewpb.CollectionLoadConfig) error {
		if info.CollectionID == 100 {
			close(blocked)
			<-proceed
		}
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var firstError error
	go func() { defer wg.Done(); firstError = store.Put(t.Context(), sampleConfig()) }()
	<-blocked
	require.Nil(t, store.GetConfig(100))
	require.Empty(t, store.Snapshot().ConfigsMap())
	cfg := sampleConfig()
	cfg.CollectionID = 200
	require.NoError(t, store.Put(t.Context(), cfg))
	close(proceed)
	wg.Wait()
	require.NoError(t, firstError)
}

func TestLoadConfigSerializesSameCollectionAndConfigVersion(t *testing.T) {
	store, hooks := newTestStore(t)
	var inside atomic.Int32
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error {
		require.EqualValues(t, 1, inside.Add(1))
		inside.Add(-1)
		return nil
	}
	var wg sync.WaitGroup
	for i := int64(0); i < 10; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			cfg := sampleConfig()
			cfg.PartitionIDs = []int64{id}
			require.NoError(t, store.Put(t.Context(), cfg))
			entry := store.Get(100)
			require.Equal(t, entry.Config.Version, entry.ConfigVersion)
		}(i)
	}
	wg.Wait()
}

func TestFromAlterLoadConfigMessage(t *testing.T) {
	header := &messagespb.AlterLoadConfigMessageHeader{
		DbId: 1, CollectionId: 100,
		PartitionIds: []int64{10}, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 200, IndexId: 300}},
		RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 200}}, UserSpecifiedReplicaMode: true,
		Replicas: []*messagespb.LoadReplicaConfig{{ReplicaId: 1000, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_LOW}},
	}
	cfg := FromAlterLoadConfigMessage(header)
	header.PartitionIds[0] = 20
	header.LoadFields[0].FieldId = 999
	require.Equal(t, []int64{10}, cfg.PartitionIDs)
	require.EqualValues(t, 200, cfg.LoadFields[0].FieldId)
	require.Equal(t, []int64{300}, cfg.LoadFields[0].IndexIds)
	cfg.LoadFields[0].IndexIds[0] = 301
	require.EqualValues(t, 300, header.LoadFields[0].IndexId, "conversion must not retain message-owned storage")
	require.Equal(t, commonpb.LoadPriority_LOW, cfg.Replicas[0].Priority)
}
