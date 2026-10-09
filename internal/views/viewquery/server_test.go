//go:build test && dynamic

package viewquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestServerSearchOnViewDelegatesAndReleasesTasks(t *testing.T) {
	provider := &fakeTaskProvider{
		searchTasks: &fakeSearchSegmentTasks{tasks: []SearchSegmentTask{struct{}{}}},
	}
	scheduler := &fakeScheduler{
		searchResult: &internalpb.SearchResults{},
	}
	server := NewServer(provider, scheduler)
	req := &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{CollectionID: 10},
		ShardId:   testShardID().IntoProto(),
		Version:   testVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
	}

	resp, err := server.SearchOnView(context.Background(), req)

	require.NoError(t, err)
	assert.Same(t, scheduler.searchResult, resp.GetLegacyResults())
	assert.Equal(t, testShardID(), provider.searchShardID)
	assert.Equal(t, testVersion(), provider.searchVersion)
	assert.Same(t, req.GetMvcc(), provider.searchMVCC)
	assert.Same(t, req.GetLegacyReq(), provider.searchReq)
	assert.Same(t, provider.searchTasks, scheduler.searchTasks)
	assert.Equal(t, 1, provider.searchTasks.releaseCount)
}

func TestServerSearchOnViewReturnsProviderErrorWithoutScheduling(t *testing.T) {
	provider := &fakeTaskProvider{
		searchErr: viewerror.NewViewNotFound("missing view"),
	}
	scheduler := &fakeScheduler{}
	server := NewServer(provider, scheduler)

	_, err := server.SearchOnView(context.Background(), validSearchRequest())

	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Nil(t, scheduler.searchTasks)
}

func TestServerSearchOnViewReleasesTasksOnSchedulerError(t *testing.T) {
	tasks := &fakeSearchSegmentTasks{tasks: []SearchSegmentTask{struct{}{}}}
	provider := &fakeTaskProvider{searchTasks: tasks}
	scheduler := &fakeScheduler{searchErr: viewerror.NewUnknownError("execution failed")}
	server := NewServer(provider, scheduler)

	_, err := server.SearchOnView(context.Background(), validSearchRequest())

	require.Error(t, err)
	assert.Equal(t, codes.Unknown, status.Code(err))
	assert.Equal(t, 1, tasks.releaseCount)
}

func TestServerQueryOnViewDelegatesAndReleasesTasks(t *testing.T) {
	provider := &fakeTaskProvider{
		queryTasks: &fakeQuerySegmentTasks{tasks: []QuerySegmentTask{struct{}{}}},
	}
	scheduler := &fakeScheduler{
		queryResult: &internalpb.RetrieveResults{},
	}
	server := NewServer(provider, scheduler)
	req := &viewpb.QueryOnViewRequest{
		LegacyReq: &internalpb.RetrieveRequest{CollectionID: 11},
		ShardId:   testShardID().IntoProto(),
		Version:   testVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
	}

	resp, err := server.QueryOnView(context.Background(), req)

	require.NoError(t, err)
	assert.Same(t, scheduler.queryResult, resp.GetLegacyResults())
	assert.Equal(t, testShardID(), provider.queryShardID)
	assert.Equal(t, testVersion(), provider.queryVersion)
	assert.Same(t, req.GetMvcc(), provider.queryMVCC)
	assert.Same(t, req.GetLegacyReq(), provider.queryReq)
	assert.Same(t, provider.queryTasks, scheduler.queryTasks)
	assert.Equal(t, 1, provider.queryTasks.releaseCount)
}

func TestServerSearchOnViewReturnsEmptyResultWithoutScheduling(t *testing.T) {
	tasks := &fakeSearchSegmentTasks{}
	provider := &fakeTaskProvider{searchTasks: tasks}
	scheduler := &fakeScheduler{searchResult: &internalpb.SearchResults{NumQueries: 99}}
	server := NewServer(provider, scheduler)
	req := validSearchRequest()
	req.LegacyReq.Nq = 2
	req.LegacyReq.Topk = 3

	resp, err := server.SearchOnView(context.Background(), req)

	require.NoError(t, err)
	assert.Nil(t, scheduler.searchTasks)
	assert.Equal(t, 1, tasks.releaseCount)
	result := resp.GetLegacyResults()
	require.NotNil(t, result)
	assert.Equal(t, int64(2), result.GetNumQueries())
	assert.Equal(t, int64(3), result.GetTopK())
	require.NotNil(t, result.GetResultData())
	assert.Equal(t, int64(2), result.GetResultData().GetNumQueries())
	assert.Equal(t, []int64{0, 0}, result.GetResultData().GetTopks())
}

func TestServerQueryOnViewReturnsEmptyResultWithoutScheduling(t *testing.T) {
	tasks := &fakeQuerySegmentTasks{}
	provider := &fakeTaskProvider{queryTasks: tasks}
	scheduler := &fakeScheduler{queryResult: &internalpb.RetrieveResults{AllRetrieveCount: 99}}
	server := NewServer(provider, scheduler)

	resp, err := server.QueryOnView(context.Background(), validQueryRequest())

	require.NoError(t, err)
	assert.Nil(t, scheduler.queryTasks)
	assert.Equal(t, 1, tasks.releaseCount)
	require.NotNil(t, resp.GetLegacyResults())
	assert.NotNil(t, resp.GetLegacyResults().GetStatus())
}

func TestServerRejectsInvalidRequestShape(t *testing.T) {
	server := NewServer(&fakeTaskProvider{}, &fakeScheduler{})

	_, err := server.SearchOnView(context.Background(), &viewpb.SearchOnViewRequest{})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServerRequeryOnViewIsUnimplemented(t *testing.T) {
	server := NewServer(&fakeTaskProvider{}, &fakeScheduler{})

	_, err := server.RequeryOnView(context.Background(), &viewpb.RequeryOnViewRequest{})

	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func validSearchRequest() *viewpb.SearchOnViewRequest {
	return &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{CollectionID: 10},
		ShardId:   testShardID().IntoProto(),
		Version:   testVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
	}
}

func validQueryRequest() *viewpb.QueryOnViewRequest {
	return &viewpb.QueryOnViewRequest{
		LegacyReq: &internalpb.RetrieveRequest{CollectionID: 10},
		ShardId:   testShardID().IntoProto(),
		Version:   testVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
	}
}

func testShardID() qviews.ShardID {
	return qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
}

func testVersion() qviews.QueryViewVersion {
	return qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: 2, CompactVersion: 3},
		QueryVersion: 4,
	}
}

type fakeTaskProvider struct {
	searchTasks   *fakeSearchSegmentTasks
	searchErr     error
	searchShardID qviews.ShardID
	searchVersion qviews.QueryViewVersion
	searchMVCC    *viewpb.QueryPlanMVCC
	searchReq     *internalpb.SearchRequest

	queryTasks   *fakeQuerySegmentTasks
	queryErr     error
	queryShardID qviews.ShardID
	queryVersion qviews.QueryViewVersion
	queryMVCC    *viewpb.QueryPlanMVCC
	queryReq     *internalpb.RetrieveRequest
}

func (p *fakeTaskProvider) AcquireSearchSegmentTasks(
	_ context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (SearchSegmentTasks, error) {
	p.searchShardID = shardID
	p.searchVersion = version
	p.searchMVCC = mvcc
	p.searchReq = req
	if p.searchErr != nil {
		return nil, p.searchErr
	}
	return p.searchTasks, nil
}

func (p *fakeTaskProvider) AcquireQuerySegmentTasks(
	_ context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (QuerySegmentTasks, error) {
	p.queryShardID = shardID
	p.queryVersion = version
	p.queryMVCC = mvcc
	p.queryReq = req
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	return p.queryTasks, nil
}

type fakeScheduler struct {
	searchTasks  SearchSegmentTasks
	searchResult *internalpb.SearchResults
	searchErr    error

	queryTasks  QuerySegmentTasks
	queryResult *internalpb.RetrieveResults
	queryErr    error
}

func (s *fakeScheduler) Search(_ context.Context, tasks SearchSegmentTasks) (*internalpb.SearchResults, error) {
	s.searchTasks = tasks
	return s.searchResult, s.searchErr
}

func (s *fakeScheduler) Query(_ context.Context, tasks QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	s.queryTasks = tasks
	return s.queryResult, s.queryErr
}

type fakeSearchSegmentTasks struct {
	tasks        []SearchSegmentTask
	releaseCount int
}

func (t *fakeSearchSegmentTasks) Tasks() []SearchSegmentTask {
	return t.tasks
}

func (t *fakeSearchSegmentTasks) Release() {
	t.releaseCount++
}

type fakeQuerySegmentTasks struct {
	tasks        []QuerySegmentTask
	releaseCount int
}

func (t *fakeQuerySegmentTasks) Tasks() []QuerySegmentTask {
	return t.tasks
}

func (t *fakeQuerySegmentTasks) Release() {
	t.releaseCount++
}
