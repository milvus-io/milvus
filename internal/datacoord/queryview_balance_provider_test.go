package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
)

func TestQueryViewBalanceProviderUsesPublishedStats(t *testing.T) {
	mockey.PatchConvey("balance snapshots preserve published membership and stats", t, func() {
		mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(nil, nil).Build()
		mockey.Mock((*catalogkv.Catalog).SaveDataView).Return(nil).Build()
		mockey.Mock((*catalogkv.Catalog).DropDataView).Return(nil).Build()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		manager, err := dataview.RecoverManager(ctx, &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
		require.NoError(t, err)
		_, err = manager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"p0_1v0"}})
		require.NoError(t, err)
		firstVersion, err := manager.RecomputeNow(ctx, 1, func(context.Context, int64) ([]dataview.LoadableSegment, error) {
			return []dataview.LoadableSegment{{SegmentID: 10, PartitionID: 2, VChannel: "p0_1v0", ManifestVersion: 3, RowNum: 100}}, nil
		})
		require.NoError(t, err)
		provider := (&Server{dataViewManager: manager}).DataViewProvider()
		snapshot := provider.DataViewSnapshotForCollections(ctx, map[int64]struct{}{1: {}, 2: {}})
		shard, ok := snapshot.ShardView(1, "p0_1v0")
		require.True(t, ok)
		require.Equal(t, []int64{10}, shard.GetPartitions()[0].GetSegmentIds())
		require.Equal(t, []int64{3}, shard.GetPartitions()[0].GetSegmentManifestVersions())
		stats, ok := snapshot.SegmentInfo(10)
		require.True(t, ok)
		require.Equal(t, int64(100), stats.RowNum)
		_, err = manager.RecomputeNow(ctx, 1, func(context.Context, int64) ([]dataview.LoadableSegment, error) {
			return []dataview.LoadableSegment{{SegmentID: 11, PartitionID: 2, VChannel: "p0_1v0", RowNum: 200}}, nil
		})
		require.NoError(t, err)
		require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
		ref, err := manager.Get(ctx, 1, firstVersion)
		require.NoError(t, err)
		require.Nil(t, ref, "snapshot adapter must release its manager reference")
		require.Equal(t, []int64{10}, shard.GetPartitions()[0].GetSegmentIds())
		require.Equal(t, int64(100), stats.RowNum)
	})
}
