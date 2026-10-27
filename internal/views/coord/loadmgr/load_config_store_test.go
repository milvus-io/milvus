package loadmgr

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// expectCatalogCalls patches only persistence boundaries and verifies their use.
func expectCatalogCalls(t *testing.T, builder *mockey.MockBuilder, calls int) {
	t.Helper()
	patch := builder.Build()
	t.Cleanup(func() { assert.Equal(t, calls, patch.Times()); patch.UnPatch() })
}

func newTestStore(t *testing.T) (*LoadConfigStore, catalogkv.Catalog) {
	t.Helper()
	catalog := catalogkv.NewCatalog(nil)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetCollections).Return(nil, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetReplicas).Return(nil, nil), 1)

	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	return store, catalog
}

func TestLoadConfigGetPreservesConfigVersionDuringPut(t *testing.T) {
	store, _ := newTestStore(t)
	const updates = 200
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil), updates+1)
	put := func(revision int64) error {
		return store.Put(context.Background(), &LoadConfig{
			CollectionID: 1,
			LoadFields:   []*messagespb.LoadFieldConfig{{FieldId: 100 + revision}},
		})
	}
	require.NoError(t, put(1))
	// Expose a write between individual getters even with a single test CPU.
	// Get must read the config and version together, rather than combine getters.
	var getConfig func(*LoadConfigStore, int64) *LoadConfig
	getter := mockey.Mock((*LoadConfigStore).GetConfig).Origin(&getConfig).To(func(s *LoadConfigStore, id int64) *LoadConfig {
		config := getConfig(s, id)
		runtime.Gosched()
		return config
	}).Build()
	defer getter.UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initialVersion := store.Get(1).ConfigVersion
	start := make(chan struct{})
	done := make(chan struct{})
	var writeErr error
	go func() {
		defer close(done)
		<-start
		for revision := int64(2); revision <= updates+1; revision++ {
			if writeErr = ctx.Err(); writeErr != nil {
				return
			}
			if writeErr = put(revision); writeErr != nil {
				return
			}
			runtime.Gosched()
		}
	}()
	close(start)
	defer func() { cancel(); <-done }()

reading:
	for {
		entry := store.Get(1)
		assert.Equal(t, loadInfoVersion(entry.Config), entry.ConfigVersion)
		select {
		case <-done:
			require.NoError(t, writeErr)
			break reading
		case <-ctx.Done():
			t.Fatal("concurrent config updates did not finish")
		default:
			runtime.Gosched()
		}
	}
	final := store.Get(1)
	require.NotEqual(t, initialVersion, final.ConfigVersion)
	require.EqualValues(t, 100+updates+1, final.Config.LoadFields[0].FieldId)
	require.Equal(t, loadInfoVersion(final.Config), final.ConfigVersion)

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseReplicas).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 1, collection)
		return nil
	}), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseCollection).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 1, collection)
		return nil
	}), 1)
	require.NoError(t, store.Remove(context.Background(), 1))
	entry := store.Get(1)
	assert.Nil(t, entry.Config)
	assert.Zero(t, entry.ConfigVersion)
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
// Call counts preserve the persistence assertions independently of the store.
func expectFullSave(t *testing.T, catalog catalogkv.Catalog, times int) {
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil), times)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil), times)
}

func TestPut_NewCollection(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()
	before := store.Snapshot()

	expectFullSave(t, catalog, 1)

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
	store, _ := newTestStore(t)
	cfg := sampleConfig()

	var savedReplicaNumber int32
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).To(func(_ context.Context, collection *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) error {
		savedReplicaNumber = collection.GetReplicaNumber()
		return nil
	}), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil), 1)

	require.NoError(t, store.Put(context.Background(), cfg))
	assert.Equal(t, int32(len(cfg.Replicas)), savedReplicaNumber)
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
	expectFullSave(t, catalog, 2)
	require.NoError(t, store.Put(context.Background(), sampleConfig()))

	// Second Put with the same config still writes everything (no dedup).
	require.NoError(t, store.Put(context.Background(), sampleConfig()))
}

func TestPut_PartitionRemovedDeletesOrphans(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig() // partitions {10, 20}

	expectFullSave(t, catalog, 2)
	require.NoError(t, store.Put(context.Background(), cfg))

	// Remove partition 10, add partition 30.
	next := cfg.Clone()
	next.PartitionIDs = []int64{20, 30}

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleasePartition).To(func(_ context.Context, collection int64, ids ...int64) error {
		require.EqualValues(t, 100, collection)
		require.Equal(t, []int64{10}, ids)
		return nil
	}), 1)
	require.NoError(t, store.Put(context.Background(), next))

	got := store.Snapshot().ConfigsMap()[cfg.CollectionID]
	assert.ElementsMatch(t, []int64{20, 30}, got.PartitionIDs)
}

func TestPut_ReplicaRemovedDeletesOrphans(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()

	expectFullSave(t, catalog, 2)
	require.NoError(t, store.Put(context.Background(), cfg))

	// Drop replica 1001, change replica 1000, add replica 1002.
	next := cfg.Clone()
	next.Replicas = []*ReplicaAssignment{
		{ReplicaID: 1000, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
		{ReplicaID: 1002, ResourceGroup: "rg1", Priority: commonpb.LoadPriority_HIGH},
	}

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseReplica).To(func(_ context.Context, collection int64, ids ...int64) error {
		require.EqualValues(t, 100, collection)
		require.Equal(t, []int64{1001}, ids)
		return nil
	}), 1)
	require.NoError(t, store.Put(context.Background(), next))

	snapshot := store.Snapshot()
	assert.NotContains(t, snapshot.ReplicaToConfigMap(), int64(1001))
	assert.Contains(t, snapshot.ReplicaToConfigMap(), int64(1000))
	assert.Contains(t, snapshot.ReplicaToConfigMap(), int64(1002))
}

func TestPut_EmptyReplicasSkipsSaveReplica(t *testing.T) {
	store, _ := newTestStore(t)
	cfg := sampleConfig()
	cfg.Replicas = nil

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil), 0)
	// No SaveReplica expected when Replicas is empty.
	require.NoError(t, store.Put(context.Background(), cfg))
}

func TestPut_DifferentCollectionsPersistConcurrently(t *testing.T) {
	store, _ := newTestStore(t)
	cfg1 := sampleConfig()
	cfg1.Replicas = nil
	cfg2 := sampleConfig()
	cfg2.CollectionID = 101
	cfg2.Replicas = nil

	entered := make(chan int64, 2)
	release := make(chan struct{})
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).To(func(_ context.Context, collection *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) error {
		entered <- collection.GetCollectionID()
		<-release
		return nil
	}), 2)
	errs := make(chan error, 2)
	go func() { errs <- store.Put(context.Background(), cfg1) }()
	go func() { errs <- store.Put(context.Background(), cfg2) }()

	seen := make(map[int64]struct{}, 2)
	timer := time.NewTimer(time.Second)
	for len(seen) < 2 {
		select {
		case collectionID := <-entered:
			seen[collectionID] = struct{}{}
		case <-timer.C:
			close(release)
			require.FailNow(t, "different collections did not persist concurrently")
		}
	}
	if !timer.Stop() {
		<-timer.C
	}
	close(release)

	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	assert.Contains(t, store.Snapshot().ConfigsMap(), cfg1.CollectionID)
	assert.Contains(t, store.Snapshot().ConfigsMap(), cfg2.CollectionID)
}

func TestPut_SameCollectionPersistsSerially(t *testing.T) {
	store, _ := newTestStore(t)
	cfg := sampleConfig()
	cfg.Replicas = nil
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).To(func(context.Context, *querypb.CollectionLoadInfo, ...*querypb.PartitionLoadInfo) error {
		switch calls.Add(1) {
		case 1:
			close(firstEntered)
			<-releaseFirst
		case 2:
			close(secondEntered)
		}
		return nil
	}), 2)
	errs := make(chan error, 2)
	go func() { errs <- store.Put(context.Background(), cfg) }()
	<-firstEntered

	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		errs <- store.Put(context.Background(), cfg.Clone())
	}()
	<-secondStarted

	select {
	case <-secondEntered:
		close(releaseFirst)
		require.FailNow(t, "same collection persisted concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)

	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "second put did not continue after the first completed")
	}
}

func TestSnapshot_DoesNotWaitForCollectionPersistence(t *testing.T) {
	store, _ := newTestStore(t)
	cfg := sampleConfig()
	cfg.Replicas = nil
	entered := make(chan struct{})
	release := make(chan struct{})
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.SaveCollection).To(func(context.Context, *querypb.CollectionLoadInfo, ...*querypb.PartitionLoadInfo) error {
		close(entered)
		<-release
		return nil
	}), 1)
	putDone := make(chan error, 1)
	go func() { putDone <- store.Put(context.Background(), cfg) }()
	<-entered

	snapshotDone := make(chan *LoadConfigSnapshot, 1)
	go func() { snapshotDone <- store.Snapshot() }()
	select {
	case snapshot := <-snapshotDone:
		assert.NotContains(t, snapshot.ConfigsMap(), int64(100))
	case <-time.After(time.Second):
		close(release)
		require.FailNow(t, "snapshot waited for collection persistence")
	}

	close(release)
	require.NoError(t, <-putDone)
	assert.Contains(t, store.Snapshot().ConfigsMap(), int64(100))
}

func TestRemove(t *testing.T) {
	store, catalog := newTestStore(t)
	cfg := sampleConfig()

	expectFullSave(t, catalog, 1)
	require.NoError(t, store.Put(context.Background(), cfg))

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseReplicas).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseCollection).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)

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

func TestContainsDoesNotRefreshSnapshot(t *testing.T) {
	store, catalog := newTestStore(t)
	resident := store.Snapshot()

	expectFullSave(t, catalog, 1)
	require.NoError(t, store.Put(context.Background(), sampleConfig()))
	require.Same(t, resident, store.snapshot)
	assert.True(t, store.Contains(100))
	assert.False(t, store.Contains(999))
	require.Same(t, resident, store.snapshot)

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseReplicas).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.ReleaseCollection).To(func(_ context.Context, collection int64) error {
		require.EqualValues(t, 100, collection)
		return nil
	}), 1)
	require.NoError(t, store.Remove(context.Background(), 100))
	assert.False(t, store.Contains(100))
	require.Same(t, resident, store.snapshot)
}

func TestSnapshot_ReturnsAllConfigs(t *testing.T) {
	store, catalog := newTestStore(t)
	expectFullSave(t, catalog, 2)

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

	expectFullSave(t, catalog, 1)
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

	expectFullSave(t, catalog, 2)
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

	expectFullSave(t, catalog, 2)
	cfg1 := sampleConfig()
	cfg2 := sampleConfig()
	cfg2.CollectionID = 101
	cfg2.Replicas = []*ReplicaAssignment{
		{ReplicaID: 2000, ResourceGroup: "rg2"},
	}

	require.NoError(t, store.Put(context.Background(), cfg1))
	first := store.Snapshot()
	assert.Equal(t, loadInfoVersion(cfg1), first.ConfigVersion(cfg1.CollectionID))

	require.NoError(t, store.Put(context.Background(), cfg2))
	second := store.Snapshot()
	assert.Equal(t, loadInfoVersion(cfg1), second.ConfigVersion(cfg1.CollectionID))
	assert.Equal(t, loadInfoVersion(cfg2), second.ConfigVersion(cfg2.CollectionID))
}

func TestRecoverLoadConfigStore_EmptyState(t *testing.T) {
	catalog := catalogkv.NewCatalog(nil)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetCollections).Return(nil, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetReplicas).Return(nil, nil), 1)

	store, err := RecoverLoadConfigStore(context.Background(), catalog)
	require.NoError(t, err)
	assert.Empty(t, store.Snapshot().ConfigsMap())
}

func TestRecoverLoadConfigStore_WithPersistedData(t *testing.T) {
	catalog := catalogkv.NewCatalog(nil)

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

	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetCollections).Return(collections, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetPartitions).Return(partitions, nil), 1)
	expectCatalogCalls(t, mockey.Mock(catalogkv.Catalog.GetReplicas).Return(replicas, nil), 1)

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
