package viewquery_test

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestStatisticsRPCDelegatesAndPreservesErrors(t *testing.T) {
	mockey.PatchConvey("statistics bypasses the scheduler and preserves the View error contract", t, func() {
		req := &viewpb.StatisticsOnViewRequest{
			ShardId:      &viewpb.ShardID{Vchannel: "p_1v0", ReplicaId: 3},
			Version:      &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 8}, QueryVersion: 9},
			Mvcc:         &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90},
			PartitionIds: []int64{10},
		}
		var providerErr error
		provider := mockey.Mock((*snview.SNQueryViewHandler).Statistics).To(func(_ *snview.SNQueryViewHandler, _ context.Context, got *viewpb.StatisticsOnViewRequest) (int64, error) {
			require.Same(t, req, got)
			return 7, providerErr
		}).Build()
		server := viewquery.NewServer(&snview.SNQueryViewHandler{}, nil)
		result, err := server.StatisticsOnView(t.Context(), req)
		require.NoError(t, err)
		require.EqualValues(t, 7, result.GetRowCount())
		providerErr = viewerror.NewViewInvalidated("retired")
		_, err = server.StatisticsOnView(t.Context(), req)
		require.Same(t, providerErr, err)
		providerErr = context.Canceled
		_, err = server.StatisticsOnView(t.Context(), req)
		require.ErrorIs(t, err, context.Canceled)
		calls := provider.Times()
		_, err = server.StatisticsOnView(t.Context(), nil)
		require.Error(t, err)
		require.Equal(t, calls, provider.Times())
	})
}
