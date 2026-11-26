package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestQueryViewProviderForwardsSelection(t *testing.T) {
	handler := &snview.SNQueryViewHandler{}
	binding := mockey.Mock((*walAdaptorImpl).queryViewTaskHandler).Return(handler, nil).Build()
	defer binding.UnPatch()
	selection := &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Exclude, SegmentIds: []int64{10}}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 20, TransformingTimetick: 15}
	searchReq, queryReq := &internalpb.SearchRequest{}, &internalpb.RetrieveRequest{}
	searchTasks, queryTasks := snview.NewSNSearchSegmentTasks(nil), snview.NewSNQuerySegmentTasks(nil)
	search := mockey.Mock((*snview.SNQueryViewHandler).AcquireSearchSegmentTasks).To(func(h *snview.SNQueryViewHandler, _ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, actualMVCC *viewpb.QueryPlanMVCC, req *internalpb.SearchRequest, actualSelection *viewpb.SegmentSelection) (viewquery.SearchSegmentTasks, error) {
		require.Same(t, handler, h)
		require.Same(t, mvcc, actualMVCC)
		require.Same(t, searchReq, req)
		require.Same(t, selection, actualSelection)
		return searchTasks, nil
	}).Build()
	defer search.UnPatch()
	query := mockey.Mock((*snview.SNQueryViewHandler).AcquireQuerySegmentTasks).To(func(h *snview.SNQueryViewHandler, _ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, actualMVCC *viewpb.QueryPlanMVCC, req *internalpb.RetrieveRequest, actualSelection *viewpb.SegmentSelection) (viewquery.QuerySegmentTasks, error) {
		require.Same(t, handler, h)
		require.Same(t, mvcc, actualMVCC)
		require.Same(t, queryReq, req)
		require.Same(t, selection, actualSelection)
		return queryTasks, nil
	}).Build()
	defer query.UnPatch()
	w := &walAdaptorImpl{}
	gotSearch, err := w.AcquireSearchSegmentTasks(t.Context(), qviews.ShardID{}, qviews.QueryViewVersion{}, mvcc, searchReq, selection)
	require.NoError(t, err)
	require.Same(t, searchTasks, gotSearch)
	gotSearch.Release()
	gotQuery, err := w.AcquireQuerySegmentTasks(t.Context(), qviews.ShardID{}, qviews.QueryViewVersion{}, mvcc, queryReq, selection)
	require.NoError(t, err)
	require.Same(t, queryTasks, gotQuery)
	gotQuery.Release()
}

func TestQueryExecutionRequiresServingWriter(t *testing.T) {
	ctx, seal := context.WithCancel(t.Context())
	defer seal()
	writer := &walAdaptorImpl{
		writerCtx:        ctx,
		roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: t.Context()},
	}
	channel := mockey.Mock((*roWALAdaptorImpl).Channel).Return(types.PChannelInfo{Name: "p", Term: 7}).Build()
	defer channel.UnPatch()
	shard := qviews.ShardID{VChannel: "p_1v0"}
	_, err := writer.queryViewTaskHandler(shard)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
	writer.queryViewHandler = &snview.SNQueryViewHandler{}
	handler, err := writer.queryViewTaskHandler(shard)
	require.NoError(t, err)
	require.Same(t, writer.queryViewHandler, handler)
	_, err = writer.queryViewTaskHandler(qviews.ShardID{VChannel: "other_1v0"})
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())

	seal()
	_, err = writer.queryViewTaskHandler(shard)
	require.True(t, viewerror.AsViewError(err).IsOnShutdown())
}
