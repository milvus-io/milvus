package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ api.DataViewPublisher = (dataview.Manager)(nil)

func TestBalancerConsumesRealDataViewManager(t *testing.T) {
	setBalanceParam(t, paramtable.Get(), paramtable.Get().QueryViewCfg.LogicalSizeSegmentBaseCost.Key, "100")
	ctx := t.Context()
	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*datacatalog.Catalog).ListAllDataViews).Return(nil, nil).Build(),
		mockey.Mock((*datacatalog.Catalog).SaveDataView).Return(nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetCollections).Return([]*querypb.CollectionLoadInfo{{CollectionID: 1}}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{
			1: {{CollectionID: 1, PartitionID: 100}},
		}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetReplicas).Return([]*querypb.Replica{{ID: 10, CollectionID: 1}}, nil).Build(),

		mockey.Mock((*stubSyncer).SyncViews).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	m, err := dataview.RecoverManager(ctx, datacatalog.NewCatalog(nil, "", ""),
		func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	version, err := m.OnBootstrapCollection(ctx, dataview.BootstrapCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{shardID.VChannel},
		Segments: []dataview.LoadableSegment{
			{SegmentID: 101, VChannel: shardID.VChannel, PartitionID: 100, RowNum: 42},
		},
	})
	require.NoError(t, err)
	catalog := queryview.NewQueryViewCatalog(nil, "coord")
	list := mockey.Mock(mockey.GetMethod(catalog, "ListQueryViews")).Return(nil, nil).Build()
	t.Cleanup(func() { list.UnPatch() })
	save := mockey.Mock(mockey.GetMethod(catalog, "SaveQueryViews")).Return(nil).Build()
	t.Cleanup(func() { save.UnPatch() })
	registry, err := coordview.RecoverShardViewRegistryWithDataViews(ctx, catalog, &stubSyncer{}, m)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	store, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	// Use real replaying source hooks; mock only the node source boundary.
	nodes := &testNodePublisher{}
	nodePatch := mockey.Mock((*testNodePublisher).RegisterNodeListener).To(func(_ *testNodePublisher, listener balancercache.NodeListener) func() {
		listener(1, &NodeInfo{NodeID: 1, Alive: true})
		return func() {}
	}).Build()
	t.Cleanup(func() { nodePatch.UnPatch() })
	cache := balancercache.NewFromSources(policyTestConfig(), store, m, registry, nodes)
	t.Cleanup(cache.Close)
	require.True(t, cache.Ready())
	controller := NewDefaultBalancer(cache, registry, nil, nil)
	controller.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	require.NoError(t, controller.Reconcile(ctx))
	stats := registry.Get(shardID).Stats()
	require.Equal(t, qviews.FromProtoDataVersion(version), stats.PreparingVersion.DataVersion)
	require.False(t, stats.Segments[101].LostStats)
	require.Equal(t, int64(142), stats.Segments[101].LogicalSize)
	require.Equal(t, int64(142), cache.GetNode(1).Info().PendingLogicalSize)
	require.Equal(t, int64(142), cache.GetNode(1).Info().TargetLogicalSize)
	require.Equal(t, int64(42), cache.GetCollection(1).DataView().TotalRows)
}

type testNodePublisher struct{}

// Patched with mockey: there is no hand-written node source behavior.
func (*testNodePublisher) RegisterNodeListener(balancercache.NodeListener) func() {
	panic("mock with mockey")
}

func TestRecoveredLoadIdentityAvoidsRebuildButDetectsRealChanges(t *testing.T) {
	deps := loadmgrtest.Dependencies(t)
	indexes := []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 200, IndexID: 300}}
	deps.Indexes = func(int64, ...int64) []*indexpb.IndexInfo { return indexes }
	before, err := loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{
		CollectionID: 1, PartitionIDs: []int64{1},
		LoadFields:     []*viewpb.LoadFieldConfig{{FieldId: 200}},
		RequiredFields: []*viewpb.LoadFieldConfig{{FieldId: 200}},
		Replicas:       []*loadmgr.ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "rg1"}},
	}
	require.NoError(t, before.Put(t.Context(), cfg))
	oldVersion := before.GetConfigVersion(1)
	after, err := loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	cache := balancercache.New(policyTestConfig())
	stop := after.RegisterLoadConfigListener(cache.PublishLoadConfig)
	defer stop()
	config := *cache.GetBalanceConfig()
	config.OptimizationEnabled = false
	cache.UpdateBalanceConfig(&config)
	id := cacheShard(1, 10)
	dv := qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}
	publishTestNodes(cache, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	segment := &SegmentDataView{SegmentID: 1000, PartitionID: 1, LogicalSize: 100}
	publishTestData(cache, 1, dv, map[int64]*SegmentDataView{1000: segment}, shardDataView(id.VChannel, 1, 1000))
	cache.PublishShard(id, withSegmentRows(testShardStats(ver(1, 1, 1), oldVersion, placement(1000, 1, 1, coordview.SegmentStateUp)), map[int64]int64{1000: 100}))
	policy := NewDefaultBalancePolicy()
	require.Empty(t, policy.Plan(cache, []qviews.ShardID{id}).Prepares, "unchanged recovery must not rebuild Up views")
	require.Contains(t, reusableResources(newPlanningContext(cache), id, segment, []int64{1}), int64(1))
	// A newly selected index changes only LoadInfoVersion, not Schema/DataVersion.
	indexes = append(indexes, &indexpb.IndexInfo{FieldID: 200, IndexID: 301})
	require.NoError(t, after.Put(t.Context(), cfg))
	plan := policy.Plan(cache, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id, "config change must be mandatory even with autoBalance disabled")
	require.NotEqual(t, oldVersion, plan.Prepares[id].Build().Meta.LoadInfoVersion)
	require.Empty(t, reusableResources(newPlanningContext(cache), id, segment, []int64{1}), "old resources must not alias the new load identity")
}
