package loadmgr

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newTestStore(t *testing.T) (*LoadConfigStore, *versionedTestHooks) {
	t.Helper()
	hooks := new(versionedTestHooks)
	deps := versionedTestDependencies(t, hooks)

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

func TestAutoLoadIntentPersistsAcrossPublicationAndRecovery(t *testing.T) {
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	manager := NewCollectionLoadManager(store, nil, nil)
	header := &messagespb.AlterLoadConfigMessageHeader{
		CollectionId: 100, AutoLoad: true, PartitionIds: []int64{10},
		Replicas: []*messagespb.LoadReplicaConfig{{ReplicaId: 1000, ResourceGroupName: "rg1"}},
	}
	require.NoError(t, manager.ApplyLoadConfig(t.Context(), header))
	initial := store.GetConfig(100)
	require.True(t, initial.AutoLoad)
	require.True(t, initial.Clone().AutoLoad)
	require.True(t, initial.toCollectionLoadConfigProto().GetAutoLoad())
	require.NoError(t, manager.AddPartition(t.Context(), 100, 20))
	require.True(t, store.GetConfig(100).AutoLoad, "derived load revisions preserve their source")

	recovered, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.True(t, recovered.GetConfig(100).AutoLoad)
	require.Equal(t, []int64{10, 20}, recovered.GetConfig(100).PartitionIDs)

	// A manual load replaces the intent, without mutating a published snapshot.
	header.AutoLoad = false
	require.NoError(t, manager.ApplyLoadConfig(t.Context(), header))
	require.False(t, store.GetConfig(100).AutoLoad)
	require.True(t, initial.AutoLoad)
	version := store.GetConfigVersion(100)
	require.NoError(t, manager.ApplyLoadConfig(t.Context(), header))
	require.Equal(t, version, store.GetConfigVersion(100), "broadcast replay is idempotent")
	recovered, err = RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.False(t, recovered.GetConfig(100).AutoLoad)

	store.StopPlanning(100)
	require.NoError(t, store.Remove(t.Context(), 100))
	recovered, err = RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.Nil(t, recovered.GetConfig(100), "release removes the marker with the load intent")
}

func TestAutoLoadSourceUpdatePreservesRequirementsIdentity(t *testing.T) {
	hooks := new(versionedTestHooks)
	deps := versionedTestDependencies(t, hooks)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := sampleConfig()
	cfg.AutoLoad = true
	require.NoError(t, store.Put(t.Context(), cfg))
	original := store.GetConfig(100)
	snapshot := store.Snapshot()
	before, err := store.GetLoadInfo(t.Context(), 100, original.Version)
	require.NoError(t, err)
	release, err := store.AcquireLoadInfo(t.Context(), 100, original.Version, original.MinimumDataVersion)
	require.NoError(t, err)
	defer release()
	store.EnableGC()
	var published []*LoadConfig
	stop := store.RegisterLoadConfigListener(func(_ int64, cfg *LoadConfig, version uint64) {
		require.Equal(t, original.Version, version)
		published = append(published, cfg)
	})
	defer stop()
	cfg.AutoLoad = false
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error { return merr.ErrServiceUnavailable }
	require.ErrorIs(t, store.Put(t.Context(), cfg), merr.ErrServiceUnavailable)
	require.Same(t, original, store.GetConfig(100), "failed writes must not publish")
	require.Len(t, published, 1)
	hooks.beforeSave = nil
	require.NoError(t, store.Put(t.Context(), cfg))
	current := store.GetConfig(100)
	require.Equal(t, original.Version, current.Version)
	require.NotSame(t, original, current)
	require.False(t, current.AutoLoad)
	require.True(t, original.AutoLoad)
	require.True(t, snapshot.ConfigsMap()[100].AutoLoad, "old readers retain their snapshot")
	require.False(t, store.Snapshot().ConfigsMap()[100].AutoLoad)
	require.Len(t, published, 2)
	require.Same(t, current, published[1])
	require.Same(t, current, store.history[100][current.Version])
	after, err := store.GetLoadInfo(t.Context(), 100, original.Version)
	require.NoError(t, err)
	require.Equal(t, before, after, "QN/SN requirements do not change")
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	require.Equal(t, 1, store.references[loadInfoKey{100, original.Version}])
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Same(t, current, store.GetConfig(100), "callback replay is a no-op")
	require.Len(t, published, 2)
	recovered, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.Equal(t, current, recovered.GetConfig(100))
	stop()
	cfg.PartitionIDs = append(cfg.PartitionIDs, 30)
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Greater(t, store.GetConfigVersion(100), original.Version, "real requirements still advance identity")
	require.Len(t, store.history[100], 2)
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	require.Len(t, store.history[100], 2, "referenced requirements survive GC")
	release()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	require.Len(t, store.history[100], 1)
}
