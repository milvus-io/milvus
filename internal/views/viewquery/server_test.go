//go:build test && dynamic

package viewquery

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/searchprofile"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The response carries, in its Status, the node's whole search and the
// segment searches recorded through the request context.
func TestServerSearchOnViewReportsStages(t *testing.T) {
	provider := &fakeTaskProvider{
		searchTasks: &fakeSearchSegmentTasks{tasks: []SearchSegmentTask{struct{}{}}},
	}
	server := NewServer(provider, stageRecordingScheduler{})

	resp, err := server.SearchOnView(context.Background(), validSearchRequest())

	require.NoError(t, err)
	stages := searchprofile.Take(resp.GetLegacyResults().GetStatus())
	require.Len(t, stages, 2)
	assert.Equal(t, searchprofile.Stage{Name: searchprofile.QNSegmentSearch, SumUs: 5000, MaxUs: 5000, Count: 1, MaxID: 11}, stages[0])
	assert.Equal(t, searchprofile.QNExecute, stages[1].Name)
}

// A hybrid search reports the segment searches of every sub-search, which
// share the request's profile.
func TestServerAdvancedSearchOnViewReportsSubSearchStages(t *testing.T) {
	provider := &fakeTaskProvider{
		searchTasks: &fakeSearchSegmentTasks{tasks: []SearchSegmentTask{struct{}{}}},
	}
	server := NewServer(provider, stageRecordingScheduler{})
	req := validSearchRequest()
	req.LegacyReq.IsAdvanced = true
	req.LegacyReq.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 1}, {Nq: 1, Topk: 1}}

	resp, err := server.SearchOnView(context.Background(), req)

	require.NoError(t, err)
	require.True(t, resp.GetLegacyResults().GetIsAdvanced())
	counts := make(map[string]int64)
	for _, s := range searchprofile.Take(resp.GetLegacyResults().GetStatus()) {
		counts[s.Name] = s.Count
	}
	assert.Equal(t, map[string]int64{searchprofile.QNExecute: 1, searchprofile.QNSegmentSearch: 2}, counts)
}

// stageRecordingScheduler records one segment search into the request's
// profile, as the segment searches of a real node do, and returns a new
// result for each search.
type stageRecordingScheduler struct {
	Scheduler
}

func (stageRecordingScheduler) Search(ctx context.Context, _ SearchSegmentTasks) (*internalpb.SearchResults, error) {
	searchprofile.FromContext(ctx).Observe(searchprofile.QNSegmentSearch, 5*time.Millisecond, 11)
	return &internalpb.SearchResults{Status: merr.Success()}, nil
}

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
	assert.Same(t, provider.searchErr, err)
	assert.Nil(t, scheduler.searchTasks)
}

func TestServerSearchOnViewReleasesTasksOnSchedulerError(t *testing.T) {
	tasks := &fakeSearchSegmentTasks{tasks: []SearchSegmentTask{struct{}{}}}
	provider := &fakeTaskProvider{searchTasks: tasks}
	scheduler := &fakeScheduler{searchErr: viewerror.NewUnknownError("execution failed")}
	server := NewServer(provider, scheduler)

	_, err := server.SearchOnView(context.Background(), validSearchRequest())

	require.Error(t, err)
	assert.Same(t, scheduler.searchErr, err)
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
	assert.Equal(t, viewpb.ViewCode_VIEW_CODE_UNKNOWN, viewerror.AsViewError(err).Code)
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
	TaskProvider
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
	_ *viewpb.SegmentSelection,
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
	_ *viewpb.SegmentSelection,
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

func TestServerSearchPassesExecutionOptionsAndReleasesOwner(t *testing.T) {
	provider := &fakeTaskProvider{searchTasks: &fakeSearchSegmentTasks{tasks: []SearchSegmentTask{struct{}{}}}}
	scheduler := &fakeScheduler{searchResult: &internalpb.SearchResults{}}
	req := validSearchRequest()
	req.ExecutionOptions = &viewpb.SearchExecutionOptions{FilterOnly: true, EnableExprCache: true}
	_, err := NewServer(provider, scheduler).SearchOnView(t.Context(), req)
	require.NoError(t, err)
	require.Same(t, req.ExecutionOptions, SearchOptions(scheduler.searchTasks))
	require.Equal(t, 1, provider.searchTasks.releaseCount)
}

// Splitting rebuilds each Chunk's Status, so the node's stages ride on the
// first Chunk only, which every reducer reads.
func TestSearchOnViewStreamSendsStagesOnFirstChunk(t *testing.T) {
	for _, tc := range []struct {
		name       string
		chunkBytes int64
		chunks     int
	}{
		{name: "several chunks", chunkBytes: 20, chunks: 3},
		{name: "one chunk", chunkBytes: 1 << 20, chunks: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &streamTestSearchTasks{tasks: []SearchSegmentTask{struct{}{}}}
			server := NewServer(&streamTestProvider{searchTasks: tasks}, &streamTestScheduler{result: streamTestResult()})
			client, cleanup := startSearchStreamTestServer(t, server)
			defer cleanup()
			request := streamTestRequest()
			request.StreamChunkBytes = tc.chunkBytes
			stream, err := reduce.NewSearchGRPCReduceStream(context.Background(), client, request)
			require.NoError(t, err)

			var chunks []*internalpb.SearchResults
			for {
				chunk, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				chunks = append(chunks, chunk)
			}
			require.NoError(t, stream.Close())
			require.Len(t, chunks, tc.chunks)
			for _, chunk := range chunks[1:] {
				require.NotContains(t, chunk.GetStatus().GetExtraInfo(), searchprofile.ExtraInfoKey)
			}
			stages := searchprofile.Take(chunks[0].GetStatus())
			require.Len(t, stages, 1)
			require.Equal(t, searchprofile.QNExecute, stages[0].Name)
		})
	}
}
