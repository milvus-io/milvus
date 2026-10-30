package loadmgr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestCollectionLoadInfoMessageBoundary(t *testing.T) {
	deps := versionedTestDependencies(t)
	deps.Indexes = func(int64, ...int64) []*indexpb.IndexInfo {
		return []*indexpb.IndexInfo{{FieldID: 100, IndexID: 200}, {FieldID: 100, IndexID: 201}}
	}
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	header := &messagespb.AlterLoadConfigMessageHeader{
		CollectionId: 1, PartitionIds: []int64{10, 11},
		LoadFields:     []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 200}},
		RequiredFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
		Replicas:       []*messagespb.LoadReplicaConfig{{ReplicaId: 20, ResourceGroupName: "rg", Priority: commonpb.LoadPriority_LOW}},
	}
	before := proto.Clone(header)
	manager := NewCollectionLoadManager(store, nil, nil)
	require.NoError(t, manager.ApplyLoadConfig(t.Context(), header))
	config := store.GetConfig(1)
	require.Equal(t, []int64{200, 201}, config.RequiredFields[0].IndexIds,
		"the legacy single-index message must not truncate the QV index selection")
	require.Equal(t, config.RequiredFields, config.LoadFields)
	require.Equal(t, commonpb.LoadPriority_LOW, config.Replicas[0].Priority)
	require.True(t, proto.Equal(before, header), "publication must not mutate WAL intent")
	require.NoError(t, manager.ApplyLoadConfig(t.Context(), header))
	require.Equal(t, config.Version, store.GetConfigVersion(1), "callback replay retains the committed identity")
	header.Replicas[0].ResourceGroupName = "caller mutation"
	require.Equal(t, "rg", config.Replicas[0].ResourceGroup)
}

type versionedTestHooks struct {
	beforeSave           func(*viewpb.CollectionLoadConfig) error
	beforeRemoveVersions func(int64, []uint64) error
}

func versionedTestDependencies(t *testing.T, hooks ...*versionedTestHooks) LoadConfigStoreDependencies {
	t.Helper()
	catalog := catalogkv.NewCollectionLoadInfoCatalog(nil)
	var mu sync.Mutex
	records := make(map[loadInfoKey]*viewpb.CollectionLoadConfig)
	patches := []*mockey.Mocker{
		mockey.Mock(mockey.GetMethod(catalog, "List")).To(func(context.Context) ([]*viewpb.CollectionLoadConfig, error) {
			mu.Lock()
			defer mu.Unlock()
			var result []*viewpb.CollectionLoadConfig
			for _, info := range records {
				result = append(result, proto.Clone(info).(*viewpb.CollectionLoadConfig))
			}
			return result, nil
		}).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "Save")).To(func(_ context.Context, info *viewpb.CollectionLoadConfig) error {
			if len(hooks) != 0 && hooks[0].beforeSave != nil {
				if err := hooks[0].beforeSave(info); err != nil {
					return err
				}
			}
			mu.Lock()
			defer mu.Unlock()
			records[loadInfoKey{info.CollectionID, info.Version}] = proto.Clone(info).(*viewpb.CollectionLoadConfig)
			return nil
		}).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "RemoveVersions")).To(func(_ context.Context, id int64, versions []uint64) error {
			if len(hooks) != 0 && hooks[0].beforeRemoveVersions != nil {
				if err := hooks[0].beforeRemoveVersions(id, versions); err != nil {
					return err
				}
			}
			mu.Lock()
			defer mu.Unlock()
			for _, version := range versions {
				delete(records, loadInfoKey{id, version})
			}
			return nil
		}).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "RemoveCollection")).To(func(_ context.Context, id int64) error {
			mu.Lock()
			defer mu.Unlock()
			for key := range records {
				if key.collectionID == id {
					delete(records, key)
				}
			}
			return nil
		}).Build(),
	}
	t.Cleanup(func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	})
	var allocator atomic.Uint64
	return LoadConfigStoreDependencies{Catalog: catalog, AllocateVersion: func(context.Context) (uint64, error) { return allocator.Add(1), nil }}
}

func versionedTestConfig() *LoadConfig {
	return &LoadConfig{
		CollectionID: 1, PartitionIDs: []int64{10},
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 100}},
		LoadFields:     []*viewpb.LoadFieldConfig{{FieldId: 100}},
		Replicas:       []*ReplicaAssignment{{ReplicaID: 200, ResourceGroup: "rg", Priority: commonpb.LoadPriority_HIGH}},
	}
}

func TestCollectionLoadInfoVersions(t *testing.T) {
	deps := versionedTestDependencies(t)
	firstIndex := &indexpb.IndexInfo{CollectionID: 1, FieldID: 100, IndexID: 300, IndexName: "json.a"}
	secondIndex := &indexpb.IndexInfo{CollectionID: 1, FieldID: 100, IndexID: 301, IndexName: "json.b"}
	selected := []*indexpb.IndexInfo{firstIndex}
	definitions := map[int64]*indexpb.IndexInfo{300: firstIndex, 301: secondIndex}
	deps.Indexes = func(_ int64, ids ...int64) []*indexpb.IndexInfo {
		if len(ids) == 0 {
			return selected
		}
		var result []*indexpb.IndexInfo
		for _, id := range ids {
			if info := definitions[id]; info != nil {
				result = append(result, info)
			}
		}
		return result
	}
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := versionedTestConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.GetConfigVersion(1)
	release, err := store.AcquireLoadInfo(t.Context(), 1, first)
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Equal(t, first, store.GetConfigVersion(1), "callback replay must not allocate")

	selected = []*indexpb.IndexInfo{secondIndex, firstIndex, secondIndex}
	require.NoError(t, store.Put(t.Context(), cfg))
	second := store.GetConfigVersion(1)
	require.Greater(t, second, first)
	current, err := store.GetLoadInfo(t.Context(), 1, second)
	require.NoError(t, err)
	require.Equal(t, []int64{300, 301}, current.RequiredFields[0].IndexIds)
	require.Equal(t, []int64{300, 301}, current.LoadFields[0].IndexIds)
	require.Len(t, current.IndexInfoList, 2)
	current.IndexInfoList[0].IndexName = "caller mutation"
	current.RequiredFields[0].IndexIds[0] = 999
	old, err := store.GetLoadInfo(t.Context(), 1, first)
	require.NoError(t, err)
	require.Len(t, old.IndexInfoList, 1)
	require.Equal(t, "json.a", old.IndexInfoList[0].IndexName)
	selected = []*indexpb.IndexInfo{firstIndex, secondIndex}
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Equal(t, second, store.GetConfigVersion(1), "order and duplicates do not change identity")

	store.EnableGC()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	_, err = store.GetLoadInfo(t.Context(), 1, first)
	require.NoError(t, err)
	release()
	release()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	_, err = store.GetLoadInfo(t.Context(), 1, first)
	require.Error(t, err)

	selected = nil
	require.NoError(t, store.Put(t.Context(), cfg))
	dropped, err := store.GetLoadInfo(t.Context(), 1, 0)
	require.NoError(t, err)
	require.Empty(t, dropped.IndexInfoList)
	require.Empty(t, dropped.RequiredFields[0].IndexIds)
	old, err = store.GetLoadInfo(t.Context(), 1, second)
	require.NoError(t, err)
	require.Len(t, old.IndexInfoList, 2, "logical drop must not change historical selection")
	delete(definitions, 300)
	_, err = store.GetLoadInfo(t.Context(), 1, second)
	require.Error(t, err, "missing exact metadata cannot silently substitute current indexes")
}

func TestCollectionLoadInfoReleaseRecovery(t *testing.T) {
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := versionedTestConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	version := store.GetConfigVersion(1)
	release, err := store.AcquireLoadInfo(t.Context(), 1, version)
	require.NoError(t, err)
	store.StopPlanning(1)
	require.False(t, store.Contains(1))
	_, err = store.GetLoadInfo(t.Context(), 1, 0)
	require.Error(t, err)
	_, err = store.GetLoadInfo(t.Context(), 1, version)
	require.NoError(t, err)
	require.Error(t, store.Remove(t.Context(), 1), "cannot remove before final View reference")
	restarted, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.True(t, restarted.Contains(1), "recover persisted configuration before callback replay")
	restarted.StopPlanning(1) // durable DropLoadConfig callback retries after recovery
	require.False(t, restarted.Contains(1))
	afterCrash, err := restarted.AcquireLoadInfo(t.Context(), 1, version)
	require.NoError(t, err)
	require.NoError(t, restarted.CollectLoadInfo(t.Context()))
	afterCrash()
	require.NoError(t, restarted.Remove(t.Context(), 1))
	require.NoError(t, restarted.Remove(t.Context(), 1))
	release()
	reloaded, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.NoError(t, reloaded.Put(t.Context(), cfg))
	require.Greater(t, reloaded.GetConfigVersion(1), version)
}

func TestCollectionLoadInfoRecoveryBeforeGC(t *testing.T) {
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := versionedTestConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.GetConfigVersion(1)
	cfg.PartitionIDs = append(cfg.PartitionIDs, 11)
	require.NoError(t, store.Put(t.Context(), cfg))
	store, err = RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	release, err := store.AcquireLoadInfo(t.Context(), 1, first)
	require.NoError(t, err, "recovery has not reconstructed the View yet")
	store.EnableGC()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	old, err := store.GetLoadInfo(t.Context(), 1, first)
	require.NoError(t, err)
	require.Equal(t, []int64{10}, old.PartitionIDs)
	release()
	require.NoError(t, store.CollectLoadInfo(t.Context()))
	_, err = store.AcquireLoadInfo(t.Context(), 1, first)
	require.Error(t, err)
}

func TestCollectionLoadInfoAcquireSerializesWithGC(t *testing.T) {
	hooks := new(versionedTestHooks)
	deps := versionedTestDependencies(t, hooks)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := versionedTestConfig()
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.GetConfigVersion(1)
	cfg.PartitionIDs = append(cfg.PartitionIDs, 11)
	require.NoError(t, store.Put(t.Context(), cfg))
	entered, proceed := make(chan struct{}), make(chan struct{})
	hooks.beforeRemoveVersions = func(id int64, versions []uint64) error {
		close(entered)
		<-proceed
		return nil
	}
	store.EnableGC()
	collected := make(chan error, 1)
	go func() { collected <- store.CollectLoadInfo(t.Context()) }()
	<-entered
	acquired := make(chan error, 1)
	go func() {
		release, err := store.AcquireLoadInfo(t.Context(), 1, first)
		if release != nil {
			release()
		}
		acquired <- err
	}()
	// Even while catalog deletion is in flight, unrelated metadata reads do
	// not wait on the collection mutation guard.
	require.NotNil(t, store.GetConfig(1))
	close(proceed)
	require.NoError(t, <-collected)
	require.Error(t, <-acquired, "a new View must not acquire an already deleted version")
}
