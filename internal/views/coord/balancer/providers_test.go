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
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

var _ DataViewProvider = (dataview.Manager)(nil)

func TestBalancerConsumesRealDataViewManager(t *testing.T) {
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
		mockey.Mock((*fakeNodeProvider).Snapshot).Return(NewNodeSnapshot(1, map[int64]*NodeInfo{1: {NodeID: 1, Alive: true}})).Build(),
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
	registry, err := coordview.RecoverShardViewRegistry(ctx, catalog, &stubSyncer{}, m)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	store, err := loadmgr.RecoverLoadConfigStore(ctx, querycoord.NewCatalog(nil))
	require.NoError(t, err)
	builder := NewSnapshotBuilder(store, registry, &fakeNodeProvider{}, m, policyTestConfig())
	controller := NewDefaultBalancer(builder, registry, nil)
	controller.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	require.NoError(t, controller.Reconcile(ctx))
	stats := registry.Get(shardID).Stats()
	require.Equal(t, qviews.FromProtoDataVersion(version), stats.PreparingVersion.DataVersion)
	require.True(t, stats.Segments[101].HasRowNum)
	require.Equal(t, int64(42), stats.Segments[101].RowNum)
	snapshot := buildFullSnapshot(builder)
	require.Equal(t, int64(42), snapshot.Nodes[1].PendingRowCount)
}
