package qnview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestStatisticsUsesPinnedReadStateAndPartitionScope(t *testing.T) {
	mockey.PatchConvey("count through the acquired ReadState, not the latest base", t, func() {
		mgr := newMockSegmentManager()
		handle := &mockSealedSegmentHandle{id: 1000, partitionID: 10}
		mgr.queryHandles = []SealedSegmentHandle{handle}
		read := &segments.LocalSegment{}
		mockey.Mock((*mockSealedSegmentHandle).Segment).Return(&viewSchemaSegment{read: read}).Build()
		mockey.Mock((*segments.LocalSegment).RowNum).To(func(s *segments.LocalSegment) int64 {
			require.Same(t, read, s)
			require.True(t, mgr.waitCalled)
			require.Zero(t, handle.releaseCount)
			return 17
		}).Build()
		h := NewQNQueryViewHandler(mgr)
		view := newPreparingQNView(1, 1)
		rc := &reportCollector{}
		h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
		acquired, _ := mgr.getAcquired(view.QueryViewKey())
		acquired.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
		server := viewquery.NewServer(h, nil)
		response, err := server.StatisticsOnView(t.Context(), &viewpb.StatisticsOnViewRequest{
			ShardId: view.ShardID().IntoProto(), Version: view.QueryViewKey().QueryViewVersion.IntoProto(),
			Mvcc: &viewpb.QueryPlanMVCC{TransformingTimetick: 99}, PartitionIds: []int64{10},
		})
		require.NoError(t, err)
		require.EqualValues(t, 17, response.GetRowCount())
		require.EqualValues(t, 99, mgr.waitTimetick)
		require.Len(t, mgr.queryView.GetPartitions(), 1)
		require.EqualValues(t, 10, mgr.queryView.GetPartitions()[0].GetPartitionId())
		require.Equal(t, 1, handle.releaseCount)

		ctx, cancel := context.WithCancel(t.Context())
		mgr.beforeQuery = cancel
		_, err = h.Statistics(ctx, &viewpb.StatisticsOnViewRequest{
			ShardId: view.ShardID().IntoProto(), Version: view.QueryViewKey().QueryViewVersion.IntoProto(),
			Mvcc: &viewpb.QueryPlanMVCC{},
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, handle.releaseCount)
		_, err = server.StatisticsOnView(t.Context(), &viewpb.StatisticsOnViewRequest{})
		require.Error(t, err)
		_, err = server.StatisticsOnView(t.Context(), &viewpb.StatisticsOnViewRequest{
			ShardId: view.ShardID().IntoProto(), Version: &viewpb.QueryViewVersion{QueryVersion: 999, DataVersion: &viewpb.DataVersion{}}, Mvcc: &viewpb.QueryPlanMVCC{},
		})
		require.True(t, viewerror.AsViewError(err).IsRetryable())
		// The server interceptor projects the logic error before the client decodes it.
		wireErr := viewerror.NewGRPCStatusFromError(err).Err()
		require.True(t, viewerror.AsViewError(viewerror.ConvertViewError("StatisticsOnView", wireErr)).IsRetryable())
	})
}
