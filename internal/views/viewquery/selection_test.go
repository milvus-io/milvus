//go:build test && dynamic

package viewquery

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestServerForwardsRegularSelectionsAndReleasesEmptyTasks(t *testing.T) {
	selection := &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_None}
	searchTasks, queryTasks := &fakeSearchSegmentTasks{}, &fakeQuerySegmentTasks{}
	var searchSelection, querySelection *viewpb.SegmentSelection
	searchPatch := mockey.Mock((*fakeTaskProvider).AcquireSearchSegmentTasks).To(func(_ *fakeTaskProvider, _ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, _ *viewpb.QueryPlanMVCC, _ *internalpb.SearchRequest, selection *viewpb.SegmentSelection) (SearchSegmentTasks, error) {
		searchSelection = proto.Clone(selection).(*viewpb.SegmentSelection)
		return searchTasks, nil
	}).Build()
	defer searchPatch.UnPatch()
	queryPatch := mockey.Mock((*fakeTaskProvider).AcquireQuerySegmentTasks).To(func(_ *fakeTaskProvider, _ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, _ *viewpb.QueryPlanMVCC, _ *internalpb.RetrieveRequest, selection *viewpb.SegmentSelection) (QuerySegmentTasks, error) {
		querySelection = proto.Clone(selection).(*viewpb.SegmentSelection)
		return queryTasks, nil
	}).Build()
	defer queryPatch.UnPatch()
	scheduler := &fakeScheduler{}
	server := NewServer(&fakeTaskProvider{}, scheduler)
	searchReq := validSearchRequest()
	searchReq.SegmentSelection = selection
	searchReq.LegacyReq.Nq = 2
	response, err := server.SearchOnView(t.Context(), searchReq)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0}, response.LegacyResults.ResultData.Topks)
	queryReq := validQueryRequest()
	queryReq.SegmentSelection = selection
	_, err = server.QueryOnView(t.Context(), queryReq)
	require.NoError(t, err)
	require.True(t, proto.Equal(selection, searchSelection))
	require.True(t, proto.Equal(selection, querySelection))
	require.Equal(t, 1, searchTasks.releaseCount)
	require.Equal(t, 1, queryTasks.releaseCount)
	require.Nil(t, scheduler.searchTasks)
	require.Nil(t, scheduler.queryTasks)
}

func TestServerHybridSelectionPreservesSlots(t *testing.T) {
	for _, positional := range []bool{false, true} {
		name := "worker_fallback"
		if positional {
			name = "positional"
		}
		t.Run(name, func(t *testing.T) {
			req := advancedSearchOnViewRequest([]*internalpb.SubSearchRequest{
				{FieldId: 1, Nq: 2, Topk: 3},
				{FieldId: 2, Nq: 2, Topk: 4, Skip: true},
				{FieldId: 3, Nq: 2, Topk: 5},
			})
			req.SegmentSelection = &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Exclude, SegmentIds: []int64{99}}
			if positional {
				req.SubSelections = []*viewpb.SegmentSelection{{}, {Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{22}}, {Mode: viewpb.SegmentSelection_None}}
			}
			// Exercise the wire: zero-byte All messages must retain their slot.
			data, err := proto.Marshal(req)
			require.NoError(t, err)
			decoded := &viewpb.SearchOnViewRequest{}
			require.NoError(t, proto.Unmarshal(data, decoded))
			var mu sync.Mutex
			got := make(map[int64]*viewpb.SegmentSelection)
			tasks := make(map[int64]*fakeSearchSegmentTasks)
			patch := mockey.Mock((*fakeTaskProvider).AcquireSearchSegmentTasks).To(func(_ *fakeTaskProvider, _ context.Context, _ qviews.ShardID, _ qviews.QueryViewVersion, _ *viewpb.QueryPlanMVCC, request *internalpb.SearchRequest, selection *viewpb.SegmentSelection) (SearchSegmentTasks, error) {
				mu.Lock()
				defer mu.Unlock()
				got[request.GetFieldId()] = proto.Clone(selection).(*viewpb.SegmentSelection)
				task := &fakeSearchSegmentTasks{}
				tasks[request.GetFieldId()] = task
				return task, nil
			}).Build()
			defer patch.UnPatch()
			response, err := NewServer(&fakeTaskProvider{}, &fakeScheduler{}).SearchOnView(t.Context(), decoded)
			require.NoError(t, err)
			require.Len(t, response.LegacyResults.SubResults, 3)
			require.Len(t, got, 2, "skipped middle request must not shift later selections")
			for _, index := range []int{0, 2} {
				expected := req.SegmentSelection
				if positional {
					expected = req.SubSelections[index]
				}
				require.True(t, proto.Equal(expected, got[int64(index+1)]))
				require.Equal(t, 1, tasks[int64(index+1)].releaseCount)
			}
			for index, result := range response.LegacyResults.SubResults {
				require.EqualValues(t, index, result.ReqIndex)
				require.Equal(t, []int64{0, 0}, result.ResultData.Topks)
			}
		})
	}
}

func TestServerRejectsMismatchedHybridSelectionsBeforeAcquiring(t *testing.T) {
	req := advancedSearchOnViewRequest([]*internalpb.SubSearchRequest{{FieldId: 1}, {FieldId: 2}})
	req.SubSelections = []*viewpb.SegmentSelection{{}}
	provider := &fakeTaskProvider{}
	_, err := NewServer(provider, &fakeScheduler{}).SearchOnView(t.Context(), req)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.ErrorContains(t, err, "hybrid segment selections do not match subrequests")
	require.Nil(t, provider.searchReq)
}
