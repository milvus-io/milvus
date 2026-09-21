package balancer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Boundary methods are patched with mockey; real managers drive the lifecycle.
type stubSyncer struct{}

func (*stubSyncer) SyncViews(context.Context, syncer.SyncGroup) error { panic("mock with mockey") }
func (*stubSyncer) Close() error                                      { panic("mock with mockey") }

func emptyRegistry(t *testing.T, syncCallbacks ...func(context.Context, syncer.SyncGroup) error) *coordview.ShardViewRegistry {
	t.Helper()
	// Existing placement fixtures use collection 1 at (1,0) or (1,1).
	// Back their references with a real Manager; row estimates are published separately by each test.
	patch := mockey.Mock((*datacatalog.Catalog).ListAllDataViews).Return([]*viewpb.DataViewOfCollection{
		{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
		{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}},
	}, nil).Build()
	refs, err := dataview.RecoverManager(t.Context(), datacatalog.NewCatalog(nil, "", ""),
		func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	patch.UnPatch()
	require.NoError(t, err)
	catalog := queryview.NewQueryViewCatalog(nil, "coord")
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(mockey.GetMethod(catalog, "ListQueryViews")).Return(nil, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "SaveQueryViews")).Return(nil).Build(),
		mockey.Mock((*stubSyncer).SyncViews).To(func(_ *stubSyncer, ctx context.Context, group syncer.SyncGroup) error {
			if len(syncCallbacks) != 0 {
				return syncCallbacks[0](ctx, group)
			}
			return nil
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	reg, err := coordview.RecoverShardViewRegistry(t.Context(), catalog, &stubSyncer{}, refs)
	require.NoError(t, err)
	t.Cleanup(reg.Close)
	return reg
}

func addShardWithPreparingView(
	t *testing.T,
	reg *coordview.ShardViewRegistry,
	shardID qviews.ShardID,
	assignments map[int64]map[int64][]int64, // nodeID -> partitionID -> segIDs
) {
	t.Helper()
	mgr := reg.Ensure(shardID)
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 1,
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: shardID.VChannel}},
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
	}
	b := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, dataView, shardID.VChannel)
	b.SetAssignments(assignments)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))
}
