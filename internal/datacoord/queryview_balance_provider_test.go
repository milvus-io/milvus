package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewBalanceProviderUsesPublishedStats(t *testing.T) {
	mockey.PatchConvey("balance snapshots preserve published membership and stats", t, func() {
		mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(nil, nil).Build()
		mockey.Mock((*catalogkv.Catalog).SaveDataView).Return(nil).Build()
		mockey.Mock((*catalogkv.Catalog).DropDataView).Return(nil).Build()
		mockey.Mock((*catalogkv.Catalog).DropDataViews).Return(nil).Build()
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
		var published *api.CollectionDataView
		stop := provider.RegisterDataViewListener(func(id int64, view *api.CollectionDataView) {
			if id == 1 {
				published = view
			}
		})
		defer stop()
		require.NotNil(t, published)
		snapshot := published
		shard := snapshot.Shard("p0_1v0")
		require.NotNil(t, shard)
		require.Equal(t, int64(10), shard.Partitions[0].Segments[0].SegmentID)
		require.Equal(t, int64(3), shard.Partitions[0].Segments[0].ManifestVersion)
		stats, ok := snapshot.Segment(10)
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
		require.Equal(t, int64(10), shard.Partitions[0].Segments[0].SegmentID)
		require.Equal(t, int64(100), stats.RowNum)
	})
}

func TestRefreshDataViewPublishesBeforeReturning(t *testing.T) {
	mockey.PatchConvey("refresh fences committed segment mutations, not a queued recompute", t, func() {
		mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(nil, nil).Build()
		mockey.Mock((*catalogkv.Catalog).SaveDataView).Return(nil).Build()
		mockey.Mock((*catalogkv.Catalog).DropDataView).Return(nil).Build()
		mockey.Mock((*catalogkv.Catalog).DropDataViews).Return(nil).Build()
		manager, err := dataview.RecoverManager(t.Context(), &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
		require.NoError(t, err)
		_, err = manager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v1"}})
		require.NoError(t, err)
		var projectionErr error
		segments := []dataview.LoadableSegment{{SegmentID: 10, PartitionID: 2, VChannel: "v1", RowNum: 100}}
		mockey.Mock((*meta).loadableProjection).To(func(_ *meta, _ context.Context, _ int64) ([]dataview.LoadableSegment, error) {
			return segments, projectionErr
		}).Build()
		s := &Server{dataViewManager: manager, meta: &meta{}}
		s.UpdateStateCode(commonpb.StateCode_Healthy)
		version, err := s.RefreshDataView(t.Context(), 1)
		require.NoError(t, err)
		ref, err := manager.Get(t.Context(), 1, version)
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, []int64{10}, ref.DataView().Shards[0].Partitions[0].SegmentIds)
		ref.Deref()
		// Truncate's empty projection is a newer, immediately readable DataView.
		segments = nil
		next, err := s.RefreshDataView(t.Context(), 1)
		require.NoError(t, err)
		require.True(t, qviews.FromProtoDataVersion(next).GT(qviews.FromProtoDataVersion(version)))
		ref, err = manager.Get(t.Context(), 1, next)
		require.NoError(t, err)
		require.Empty(t, ref.DataView().Shards[0].Partitions)
		ref.Deref()
		projectionErr = merr.WrapErrServiceUnavailableMsg("projection unavailable")
		_, err = s.RefreshDataView(t.Context(), 1)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		_, err = manager.OnDropCollection(t.Context(), 1)
		require.NoError(t, err)
		_, err = s.RefreshDataView(t.Context(), 1)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	})
}
