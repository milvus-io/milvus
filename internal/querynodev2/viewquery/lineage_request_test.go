//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var lineagePlanMVCC = &viewpb.QueryPlanMVCC{
	GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 135,
	LineageTransformingTimeticks: map[string]uint64{"t1": 110, "t2": 130},
}

// Sealed segments of a view with lineage are read at the common read point B,
// not at the transform frontier of the view's own stream.
func TestSealedRequestsOfLineageViewReadAtReadPoint(t *testing.T) {
	handle := fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "s"}}

	searchRunner := &fakeSearchTaskRunner{result: &internalpb.SearchResults{}}
	queryRunner := &fakeQueryTaskRunner{result: &internalpb.RetrieveResults{}}
	executor := NewDirectSegmentTaskExecutorForTest(100, searchRunner, queryRunner)

	_, err := executor.Search(context.Background(), []qnview.QNSearchSegmentTask{{Handle: handle, Request: &internalpb.SearchRequest{CollectionID: 10}, MVCC: lineagePlanMVCC}})
	require.NoError(t, err)
	require.EqualValues(t, 135, searchRunner.req.GetReq().GetMvccTimestamp())
	require.Equal(t, []string{"s"}, searchRunner.req.GetDmlChannels())

	// Query covers retrieve by expression, retrieve by ids (requery), count and iterators:
	// they all reach the QueryNode as a RetrieveRequest.
	_, err = executor.Query(context.Background(), []qnview.QNQuerySegmentTask{{Handle: handle, Request: &internalpb.RetrieveRequest{CollectionID: 10, IsCount: true}, MVCC: lineagePlanMVCC}})
	require.NoError(t, err)
	require.EqualValues(t, 135, queryRunner.req.GetReq().GetMvccTimestamp())
}

// Without a read point the built requests are byte for byte what they were
// before lineage existed: the transform frontier is the read timestamp.
func TestSealedRequestsWithoutLineageAreUnchanged(t *testing.T) {
	handles := []qnview.SealedSegmentHandle{
		fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "v1"}},
		fakeSealedSegmentHandle{id: 2, segment: fakeReadableSegment{id: 2, vchannel: "v1"}},
	}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	marshal := func(m proto.Message) []byte {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
		require.NoError(t, err)
		return data
	}

	search := &internalpb.SearchRequest{CollectionID: 10, Topk: 5, MvccTimestamp: 7, PartitionIDs: []int64{1}}
	wantSearch := &querypb.SearchRequest{
		Req:        &internalpb.SearchRequest{CollectionID: 10, Topk: 5, MvccTimestamp: 99, PartitionIDs: []int64{1}},
		SegmentIDs: []int64{1, 2}, DmlChannels: []string{"v1"}, Scope: querypb.DataScope_Historical,
	}
	require.Equal(t, marshal(wantSearch), marshal(buildQNSearchRequest(search, mvcc, handles)))
	require.EqualValues(t, 7, search.GetMvccTimestamp(), "the shared request is not mutated")

	retrieve := &internalpb.RetrieveRequest{CollectionID: 10, Limit: 5, MvccTimestamp: 7}
	wantRetrieve := &querypb.QueryRequest{
		Req:        &internalpb.RetrieveRequest{CollectionID: 10, Limit: 5, MvccTimestamp: 99},
		SegmentIDs: []int64{1, 2}, DmlChannels: []string{"v1"}, Scope: querypb.DataScope_Historical,
	}
	require.Equal(t, marshal(wantRetrieve), marshal(buildQNQueryRequest(retrieve, mvcc, handles)))
}
