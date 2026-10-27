//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDirectSegmentTaskExecutorSearchBuildsStreamingRequestFromHandles(t *testing.T) {
	runner := &fakeSearchTaskRunner{result: &internalpb.SearchResults{NumQueries: 10}}
	executor := NewDirectSegmentTaskExecutorForTest(100, runner, &fakeQueryTaskRunner{})
	legacyReq := &internalpb.SearchRequest{CollectionID: 10}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	tasks := []snview.SNSearchSegmentTask{
		{Handle: fakeGrowingSegmentHandle{id: 1, partitionID: 10}, Request: legacyReq, MVCC: mvcc, VChannel: "v1"},
		{Handle: fakeGrowingSegmentHandle{id: 2, partitionID: 10}, Request: legacyReq, MVCC: mvcc, VChannel: "v1"},
	}

	result, err := executor.Search(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, runner.result, result)
	require.NotNil(t, runner.req)
	assert.Equal(t, querypb.DataScope_Streaming, runner.req.GetScope())
	assert.Equal(t, []int64{1, 2}, runner.req.GetSegmentIDs())
	assert.Equal(t, []string{"v1"}, runner.req.GetDmlChannels())
	assert.Equal(t, uint64(101), runner.req.GetReq().GetMvccTimestamp())
	assert.Equal(t, int64(100), runner.serverID)
	require.Len(t, runner.segments, 2)
}

func TestDirectSegmentTaskExecutorQueryBuildsStreamingRequestFromHandles(t *testing.T) {
	runner := &fakeQueryTaskRunner{result: &internalpb.RetrieveResults{AllRetrieveCount: 10}}
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, runner)
	legacyReq := &internalpb.RetrieveRequest{CollectionID: 10}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	tasks := []snview.SNQuerySegmentTask{
		{Handle: fakeGrowingSegmentHandle{id: 1, partitionID: 10}, Request: legacyReq, MVCC: mvcc, VChannel: "v1"},
		{Handle: fakeGrowingSegmentHandle{id: 2, partitionID: 10}, Request: legacyReq, MVCC: mvcc, VChannel: "v1"},
	}

	result, err := executor.Query(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, runner.result, result)
	require.NotNil(t, runner.req)
	assert.Equal(t, querypb.DataScope_Streaming, runner.req.GetScope())
	assert.Equal(t, []int64{1, 2}, runner.req.GetSegmentIDs())
	assert.Equal(t, []string{"v1"}, runner.req.GetDmlChannels())
	assert.Equal(t, uint64(101), runner.req.GetReq().GetMvccTimestamp())
	assert.Equal(t, int64(100), runner.serverID)
	require.Len(t, runner.segments, 2)
}

type fakeGrowingSegmentHandle struct {
	id          int64
	partitionID int64
}

func (h fakeGrowingSegmentHandle) ID() int64 {
	return h.id
}

func (h fakeGrowingSegmentHandle) PartitionID() int64 {
	return h.partitionID
}

func (h fakeGrowingSegmentHandle) Collection() *segcore.CCollection {
	return nil
}

func (h fakeGrowingSegmentHandle) Segment() segcore.CSegment {
	return nil
}

func (h fakeGrowingSegmentHandle) Release() {}

type fakeSearchTaskRunner struct {
	collection *segcore.CCollection
	segments   []segcore.CSegment
	req        *querypb.SearchRequest
	serverID   int64
	result     *internalpb.SearchResults
	err        error
}

func (r *fakeSearchTaskRunner) Search(ctx context.Context, collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error) {
	r.collection = collection
	r.segments = append([]segcore.CSegment(nil), selected...)
	r.req = req
	r.serverID = serverID
	return r.result, r.err
}

type fakeQueryTaskRunner struct {
	collection *segcore.CCollection
	segments   []segcore.CSegment
	req        *querypb.QueryRequest
	serverID   int64
	result     *internalpb.RetrieveResults
	err        error
}

func (r *fakeQueryTaskRunner) Query(ctx context.Context, collection *segcore.CCollection, selected []segcore.CSegment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error) {
	r.collection = collection
	r.segments = append([]segcore.CSegment(nil), selected...)
	r.req = req
	r.serverID = serverID
	return r.result, r.err
}
