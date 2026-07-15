//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDirectSegmentTaskExecutorSearchBuildsHistoricalRequestFromHandles(t *testing.T) {
	runner := &fakeSearchTaskRunner{result: &internalpb.SearchResults{NumQueries: 10}}
	executor := NewDirectSegmentTaskExecutorForTest(100, runner, &fakeQueryTaskRunner{})
	legacyReq := &internalpb.SearchRequest{CollectionID: 10}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	tasks := []qnview.QNSearchSegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
		{Handle: fakeSealedSegmentHandle{id: 2, segment: fakeReadableSegment{id: 2, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
	}

	result, err := executor.Search(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, runner.result, result)
	require.NotNil(t, runner.req)
	assert.Equal(t, querypb.DataScope_Historical, runner.req.GetScope())
	assert.Equal(t, []int64{1, 2}, runner.req.GetSegmentIDs())
	assert.Equal(t, []string{"v1"}, runner.req.GetDmlChannels())
	assert.Equal(t, uint64(99), runner.req.GetReq().GetMvccTimestamp())
	assert.Equal(t, int64(100), runner.serverID)
	require.Len(t, runner.segments, 2)
}

func TestDirectSegmentTaskExecutorSearchRejectsUnreadableSegment(t *testing.T) {
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, &fakeQueryTaskRunner{})
	tasks := []qnview.QNSearchSegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeTransformOnlySegment{id: 1}}, Request: &internalpb.SearchRequest{}, MVCC: &viewpb.QueryPlanMVCC{}},
	}

	_, err := executor.Search(context.Background(), tasks)

	require.Error(t, err)
}

func TestDirectSegmentTaskExecutorQueryBuildsHistoricalRequestFromHandles(t *testing.T) {
	runner := &fakeQueryTaskRunner{result: &internalpb.RetrieveResults{AllRetrieveCount: 10}}
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, runner)
	legacyReq := &internalpb.RetrieveRequest{CollectionID: 10}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99}
	tasks := []qnview.QNQuerySegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeReadableSegment{id: 1, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
		{Handle: fakeSealedSegmentHandle{id: 2, segment: fakeReadableSegment{id: 2, vchannel: "v1"}}, Request: legacyReq, MVCC: mvcc},
	}

	result, err := executor.Query(context.Background(), tasks)

	require.NoError(t, err)
	assert.Same(t, runner.result, result)
	require.NotNil(t, runner.req)
	assert.Equal(t, querypb.DataScope_Historical, runner.req.GetScope())
	assert.Equal(t, []int64{1, 2}, runner.req.GetSegmentIDs())
	assert.Equal(t, []string{"v1"}, runner.req.GetDmlChannels())
	assert.Equal(t, uint64(99), runner.req.GetReq().GetMvccTimestamp())
	assert.Equal(t, int64(100), runner.serverID)
	require.Len(t, runner.segments, 2)
}

func TestDirectSegmentTaskExecutorQueryRejectsUnreadableSegment(t *testing.T) {
	executor := NewDirectSegmentTaskExecutorForTest(100, &fakeSearchTaskRunner{}, &fakeQueryTaskRunner{})
	tasks := []qnview.QNQuerySegmentTask{
		{Handle: fakeSealedSegmentHandle{id: 1, segment: fakeTransformOnlySegment{id: 1}}, Request: &internalpb.RetrieveRequest{}, MVCC: &viewpb.QueryPlanMVCC{}},
	}

	_, err := executor.Query(context.Background(), tasks)

	require.Error(t, err)
}

type fakeSearchTaskRunner struct {
	collection *segments.Collection
	segments   []segments.Segment
	req        *querypb.SearchRequest
	serverID   int64
	result     *internalpb.SearchResults
	err        error
}

func (r *fakeSearchTaskRunner) Search(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.SearchRequest, serverID int64) (*internalpb.SearchResults, error) {
	r.collection = collection
	r.segments = append([]segments.Segment(nil), selected...)
	r.req = req
	r.serverID = serverID
	return r.result, r.err
}

type fakeQueryTaskRunner struct {
	collection *segments.Collection
	segments   []segments.Segment
	req        *querypb.QueryRequest
	serverID   int64
	result     *internalpb.RetrieveResults
	err        error
}

func (r *fakeQueryTaskRunner) Query(ctx context.Context, collection *segments.Collection, selected []segments.Segment, req *querypb.QueryRequest, serverID int64) (*internalpb.RetrieveResults, error) {
	r.collection = collection
	r.segments = append([]segments.Segment(nil), selected...)
	r.req = req
	r.serverID = serverID
	return r.result, r.err
}

type fakeReadableSegment struct {
	id       int64
	vchannel string
}

func (s fakeReadableSegment) ID() int64 {
	return s.id
}

func (s fakeReadableSegment) VChannel() string {
	return s.vchannel
}

func (s fakeReadableSegment) PartitionID() int64 {
	return 0
}

func (s fakeReadableSegment) TransformStartAfterTimeTick() uint64 {
	return 0
}

func (s fakeReadableSegment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (s fakeReadableSegment) AppliedTransformTimeTick() uint64 {
	return 0
}

func (s fakeReadableSegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s fakeReadableSegment) Release(context.Context) error {
	return nil
}

func (s fakeReadableSegment) QuerySegment() segments.Segment {
	return nil
}

func (s fakeReadableSegment) Collection() *segments.Collection {
	return nil
}

type fakeTransformOnlySegment struct {
	id int64
}

func (s fakeTransformOnlySegment) ID() int64 {
	return s.id
}

func (s fakeTransformOnlySegment) VChannel() string {
	return "v1"
}

func (s fakeTransformOnlySegment) PartitionID() int64 {
	return 0
}

func (s fakeTransformOnlySegment) TransformStartAfterTimeTick() uint64 {
	return 0
}

func (s fakeTransformOnlySegment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (s fakeTransformOnlySegment) AppliedTransformTimeTick() uint64 {
	return 0
}

func (s fakeTransformOnlySegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s fakeTransformOnlySegment) Release(context.Context) error {
	return nil
}
