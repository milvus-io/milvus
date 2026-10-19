package queryclient

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardSearchReturnsQueryPlanMVCCForRequery(t *testing.T) {
	setupTestReplicaRouting(t)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	mvcc := &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	}
	version := &viewpb.QueryViewVersion{
		QueryVersion: 10,
	}
	queryNode := qviews.NewQueryNode(11)
	planClient := &fakeQueryPlanClient{
		plan: &viewpb.QueryPlan{
			ShardId: shardID.IntoProto(),
			Version: version,
			Mvcc:    mvcc,
			Request: &viewpb.QueryPlan_LegacySearchRequest{
				LegacySearchRequest: &internalpb.SearchRequest{},
			},
			WorkNodes: []*viewpb.QueryPlanWorkNode{
				{
					Node: &viewpb.QueryPlanWorkNode_QueryNode{
						QueryNode: &viewpb.QueryWorkNode{NodeId: queryNode.ID},
					},
				},
			},
		},
	}
	selection := &viewpb.SegmentSelection{SegmentIds: []int64{100}, Mode: viewpb.SegmentSelection_Exclude}
	subSelections := []*viewpb.SegmentSelection{{SegmentIds: []int64{101}, Mode: viewpb.SegmentSelection_Include}, {Mode: viewpb.SegmentSelection_All}}
	planClient.executionSummary = &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering}}
	planClient.plan.WorkNodes[0].SegmentSelection = selection
	planClient.plan.WorkNodes[0].SubSelections = subSelections
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
	)

	shardPlan, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: shardID.VChannel,
		Req: &internalpb.SearchRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Nq:               1,
			Topk:             1,
			MetricType:       "IP",
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	require.True(t, proto.Equal(selection, queryService.searchReq.GetSegmentSelection()))
	require.Equal(t, subSelections, queryService.searchReq.GetSubSelections())
	require.Equal(t, planClient.plan.WorkNodes, shardPlan.Selections)
	require.True(t, proto.Equal(planClient.executionSummary, shardPlan.ExecutionSummary))
	require.True(t, proto.Equal(mvcc, shardPlan.Mvcc))
	require.True(t, proto.Equal(mvcc, queryService.searchReq.GetMvcc()))
	require.Equal(t, mvcc.GetTransformingTimetick(), queryService.searchReq.GetLegacyReq().GetMvccTimestamp())
}

func TestSessionSearchOnPrimaryLetsSNGenerateQueryPlanMVCC(t *testing.T) {
	setupTestReplicaRouting(t)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	planClient := &fakeQueryPlanClient{
		plan: newTestSearchQueryPlan(shardID, &viewpb.QueryPlanMVCC{
			GrowingTimetick:      100,
			TransformingTimetick: 90,
		}),
	}
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: shardID.VChannel,
		Req: &internalpb.SearchRequest{
			CollectionID:       100,
			ConsistencyLevel:   commonpb.ConsistencyLevel_Session,
			GuaranteeTimestamp: 999,
			Nq:                 1,
			Topk:               1,
			MetricType:         "IP",
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	require.Equal(t, commonpb.ConsistencyLevel_Strong, planClient.planReq.GetConsistencyLevel())
	require.Nil(t, planClient.planReq.GetQueryPlanMvcc())
	require.Equal(t, 0, planClient.mvccReqCount)
}

func TestStrongSearchAlwaysTargetsPrimary(t *testing.T) {
	setupTestReplicaRouting(t)
	vchannel := "by-dev-rootcoord-dml_0_100v0"
	primaryShardID := qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
	planClient := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(primaryShardID, &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	})}
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: vchannel,
		Req: &internalpb.SearchRequest{
			CollectionID:       100,
			ConsistencyLevel:   commonpb.ConsistencyLevel_Strong,
			GuaranteeTimestamp: 999,
			Nq:                 1,
			Topk:               1,
			MetricType:         "IP",
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	// The client always targets the primary replica: the plan request carries
	// the primary shard ID and strong consistency is satisfied by the primary's
	// WAL directly, without a cross-replica MVCC round trip.
	require.Equal(t, primaryShardID.IntoProto(), planClient.planReq.GetShardId())
	require.Equal(t, commonpb.ConsistencyLevel_Strong, planClient.planReq.GetConsistencyLevel())
	require.Nil(t, planClient.planReq.GetQueryPlanMvcc())
	require.Equal(t, 0, planClient.mvccReqCount)
}

func TestShardSearchStreamReturnsPerVChannelReduceStream(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	leftNode := qviews.NewQueryNode(11)
	rightNode := qviews.NewQueryNode(12)
	request := newTestSearchRequest(3)
	request.PlaceholderGroup = []byte("vectors")
	request.SerializedExprPlan = []byte("filter plan")
	plan := newTestSearchQueryPlan(shardID, &viewpb.QueryPlanMVCC{})
	plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request).(*internalpb.SearchRequest)}
	qviews.OmitUnchangedQueryPayloads(plan, &viewpb.GetQueryPlanRequest{
		Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{LegacySearchRequest: request},
	})
	plan.WorkNodes = []*viewpb.QueryPlanWorkNode{
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: leftNode.ID}}},
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: rightNode.ID}}},
	}

	leftStream := &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: newTestSearchChunk(3, []int64{1, 4}, []float32{0.9, 0.6})}}}
	rightStream := &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: newTestSearchChunk(3, []int64{2, 3}, []float32{0.8, 0.7})}}}
	queryService := &fakeViewQueryServiceClient{
		searchOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
			switch node.Key() {
			case leftNode.Key():
				return leftStream, nil
			case rightNode.Key():
				return rightStream, nil
			default:
				return nil, errors.New("unexpected work node")
			}
		},
	}
	planClient := &fakeQueryPlanClient{plan: plan}
	client := newShardViewQueryClient(1, planClient, queryService)

	stream, shardPlan, err := client.SearchStream(context.Background(), &ShardSearchRequest{
		VChannel: shardID.VChannel,
		Req:      request, RequiredFieldIDs: []int64{101}, BarrierMVCC: 123,
	}, 20)

	require.NoError(t, err)
	require.Equal(t, shardID, shardPlan.ShardID)
	require.Equal(t, []int64{101}, planClient.planReq.GetRequiredFieldIds())
	require.Equal(t, uint64(123), planClient.planReq.GetBarrierMvcc())
	require.Equal(t, int64(20), queryService.searchReq.GetStreamChunkBytes())
	require.Equal(t, request.GetPlaceholderGroup(), queryService.searchReq.GetLegacyReq().GetPlaceholderGroup())
	require.Equal(t, request.GetSerializedExprPlan(), queryService.searchReq.GetLegacyReq().GetSerializedExprPlan())
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []float32{0.9, 0.8}, chunk.GetResultData().GetScores())
	chunk, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3}, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []float32{0.7}, chunk.GetResultData().GetScores())
	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, leftStream.closeCount())
	require.Equal(t, 1, rightStream.closeCount())
}

func TestShardQueryStreamReturnsPerVChannelReduceStream(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	leftNode := qviews.NewQueryNode(11)
	rightNode := qviews.NewQueryNode(12)
	request := &internalpb.RetrieveRequest{CollectionID: 100, IsIterator: true, Limit: 3, SerializedExprPlan: []byte("filter plan")}
	plan := &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 1},
		Mvcc:    &viewpb.QueryPlanMVCC{},
		Request: &viewpb.QueryPlan_LegacyRetrieveRequest{
			LegacyRetrieveRequest: proto.Clone(request).(*internalpb.RetrieveRequest),
		},
		WorkNodes: []*viewpb.QueryPlanWorkNode{
			{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: leftNode.ID}}},
			{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: rightNode.ID}}},
		},
	}
	qviews.OmitUnchangedQueryPayloads(plan, &viewpb.GetQueryPlanRequest{
		Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: request},
	})
	leftStream := &fakeQueryStream{recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{1, 3})}}}
	rightStream := &fakeQueryStream{recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{2, 4})}}}
	queryService := &fakeViewQueryServiceClient{
		queryOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error) {
			switch node.Key() {
			case leftNode.Key():
				return leftStream, nil
			case rightNode.Key():
				return rightStream, nil
			default:
				return nil, errors.New("unexpected work node")
			}
		},
	}
	planClient := &fakeQueryPlanClient{plan: plan}
	client := newShardViewQueryClient(1, planClient, queryService)

	stream, shardPlan, err := client.QueryStream(context.Background(), &ShardQueryRequest{
		VChannel: shardID.VChannel,
		Req:      request, ReadProofAfter: 456, BarrierMVCC: 123,
	}, 32)

	require.NoError(t, err)
	require.Equal(t, shardID, shardPlan.ShardID)
	require.Equal(t, uint64(456), planClient.planReq.GetReadProofAfter())
	require.Equal(t, uint64(123), planClient.planReq.GetBarrierMvcc())
	require.Equal(t, int64(32), queryService.queryReq.GetStreamChunkBytes())
	require.Equal(t, request.GetSerializedExprPlan(), queryService.queryReq.GetLegacyReq().GetSerializedExprPlan())
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, chunk.GetIds().GetIntId().GetData())
	chunk, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3}, chunk.GetIds().GetIntId().GetData())
	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, leftStream.closeCount())
	require.Equal(t, 1, rightStream.closeCount())
}

func TestShardSearchStreamCloseReleasesChildStream(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	request := newTestSearchRequest(1)
	plan := newTestSearchQueryPlan(shardID, &viewpb.QueryPlanMVCC{})
	plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request).(*internalpb.SearchRequest)}
	childStream := &fakeSearchStream{}
	client := newShardViewQueryClient(
		1,
		&fakeQueryPlanClient{plan: plan},
		&fakeViewQueryServiceClient{
			searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				return childStream, nil
			},
		},
	)

	stream, _, err := client.SearchStream(context.Background(), &ShardSearchRequest{VChannel: shardID.VChannel, Req: request}, 32)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	require.Equal(t, 1, childStream.closeCount())
}

func TestShardSearchClosesOpenedStreamsOnSetupFailure(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	leftNode := qviews.NewQueryNode(11)
	rightNode := qviews.NewQueryNode(12)
	request := newTestSearchRequest(1)
	plan := newTestSearchQueryPlan(shardID, &viewpb.QueryPlanMVCC{})
	plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request).(*internalpb.SearchRequest)}
	plan.WorkNodes = []*viewpb.QueryPlanWorkNode{
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: leftNode.ID}}},
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: rightNode.ID}}},
	}

	openedStream := &fakeSearchStream{}
	queryService := &fakeViewQueryServiceClient{
		searchOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
			if node.Key() == leftNode.Key() {
				return openedStream, nil
			}
			return nil, errors.New("open failed")
		},
	}
	client := newTestShardClient(1, shardID, plan, queryService)

	_, _, err := client.SearchStream(context.Background(), &ShardSearchRequest{VChannel: shardID.VChannel, Req: request}, 32)

	require.ErrorContains(t, err, "open failed")
	require.Equal(t, 1, openedStream.closeCount())
}

func TestShardSearchUsesBatchPathForUnsupportedSearch(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	request := newTestSearchRequest(1)
	request.GroupByFieldId = 100
	plan := newTestSearchQueryPlan(shardID, &viewpb.QueryPlanMVCC{})
	plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request).(*internalpb.SearchRequest)}
	batchResult := newTestSearchChunk(1, []int64{10}, []float32{0.9})
	queryService := &fakeViewQueryServiceClient{
		searchResponse: &viewpb.SearchOnViewResponse{LegacyResults: batchResult},
		searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
			return nil, errors.New("stream path should not be used")
		},
	}
	reducer := &recordingSearchResultReducer{}
	client := newTestShardClient(1, shardID, plan, queryService)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: shardID.VChannel,
		Req:      request,
		Reducer:  reducer,
	})

	require.NoError(t, err)
	require.Len(t, reducer.results, 1)
	require.Same(t, batchResult, reducer.results[0])
	require.Equal(t, 1, queryService.searchCalls)
	require.Zero(t, queryService.searchStreamCalls)
}

type fakeQueryPlanClient struct {
	QueryPlanClient
	executionSummary      *viewpb.ExecutionSummary
	growingInlineResponse *viewpb.GrowingInlineResponse
	plan                  *viewpb.QueryPlan
	planReq               *viewpb.GetQueryPlanRequest
	mvccReq               *viewpb.GetMVCCTimestampRequest
	mvccShardID           qviews.ShardID
	mvccReqCount          int
}

func (f *fakeQueryPlanClient) GetQueryPlan(_ context.Context, _ qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	f.planReq = proto.Clone(req).(*viewpb.GetQueryPlanRequest)
	return &viewpb.GetQueryPlanResponse{Plan: f.plan, GrowingInlineResponse: f.growingInlineResponse, ExecutionSummary: f.executionSummary}, nil
}

func (f *fakeQueryPlanClient) GetMVCCTimestamp(_ context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	f.mvccReq = proto.Clone(req).(*viewpb.GetMVCCTimestampRequest)
	f.mvccShardID = shardID
	f.mvccReqCount++
	return &viewpb.GetMVCCTimestampResponse{Mvcc: f.plan.GetMvcc()}, nil
}

type fakeViewQueryServiceClient struct {
	ViewQueryServiceClient
	searchReq          *viewpb.SearchOnViewRequest
	queryReq           *viewpb.QueryOnViewRequest
	searchResponse     *viewpb.SearchOnViewResponse
	searchOnViewStream func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error)
	queryOnViewStream  func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error)
	searchCalls        int
	searchStreamCalls  int
	queryStreamCalls   int
}

func (f *fakeViewQueryServiceClient) SearchOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	f.searchReq = req
	f.searchCalls++
	if f.searchResponse != nil {
		return f.searchResponse, nil
	}
	return &viewpb.SearchOnViewResponse{}, nil

}

func (f *fakeViewQueryServiceClient) SearchOnViewStream(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
	f.searchReq = req
	f.searchStreamCalls++
	if f.searchOnViewStream != nil {
		return f.searchOnViewStream(ctx, node, req)
	}
	return &fakeSearchStream{}, nil
}

func (f *fakeViewQueryServiceClient) QueryOnView(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return &viewpb.QueryOnViewResponse{}, nil
}

func (f *fakeViewQueryServiceClient) QueryOnViewStream(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error) {
	f.queryReq = req
	f.queryStreamCalls++
	if f.queryOnViewStream != nil {
		return f.queryOnViewStream(ctx, node, req)
	}
	return &fakeQueryStream{}, nil
}

type fakeSearchResultReducer struct{}

func (fakeSearchResultReducer) Add(qviews.ShardID, *viewpb.SearchOnViewResponse) error {
	return nil
}

func (fakeSearchResultReducer) ResetShard(qviews.ShardID) {}

func (fakeSearchResultReducer) Finish() (*internalpb.SearchResults, error) {
	return &internalpb.SearchResults{}, nil
}

type recordingSearchResultReducer struct {
	results []*internalpb.SearchResults
}

func (r *recordingSearchResultReducer) Add(_ qviews.ShardID, response *viewpb.SearchOnViewResponse) error {
	r.results = append(r.results, response.GetLegacyResults())
	return nil
}

func (*recordingSearchResultReducer) ResetShard(qviews.ShardID) {}

func (*recordingSearchResultReducer) Finish() (*internalpb.SearchResults, error) {
	return &internalpb.SearchResults{}, nil
}

type fakeSearchStreamRecv struct {
	chunk *internalpb.SearchResults
	err   error
}

type fakeSearchStream struct {
	mu         sync.Mutex
	ctx        context.Context
	recv       []fakeSearchStreamRecv
	closeCalls int
}

func (s *fakeSearchStream) Recv() (*internalpb.SearchResults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		default:
		}
	}
	if len(s.recv) == 0 {
		return nil, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return next.chunk, next.err
}

func (s *fakeSearchStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return nil
}

func (*fakeSearchStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeSearchStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

type fakeQueryStreamRecv struct {
	chunk *internalpb.RetrieveResults
	err   error
}

type fakeQueryStream struct {
	mu         sync.Mutex
	ctx        context.Context
	recv       []fakeQueryStreamRecv
	closeCalls int
}

func (s *fakeQueryStream) Recv() (*internalpb.RetrieveResults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		default:
		}
	}
	if len(s.recv) == 0 {
		return nil, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return next.chunk, next.err
}

func (s *fakeQueryStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return nil
}

func (*fakeQueryStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeQueryStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}

func newTestShardClient(maxRetries int, shardID qviews.ShardID, plan *viewpb.QueryPlan, queryService ViewQueryServiceClient) *shardViewQueryClient {
	return newShardViewQueryClient(
		maxRetries,
		&fakeQueryPlanClient{plan: plan},
		queryService,
	)
}

func newTestSearchRequest(topK int) *internalpb.SearchRequest {
	return &internalpb.SearchRequest{
		CollectionID:     100,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		Nq:               1,
		Topk:             int64(topK),
		MetricType:       "IP",
		IsIterator:       true,
	}
}

func newTestQueryChunk(ids []int64) *internalpb.RetrieveResults {
	values := make([]int64, len(ids))
	for i, id := range ids {
		values[i] = id * 10
	}
	return &internalpb.RetrieveResults{
		Status: merr.Success(),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
					},
				},
			},
		},
	}
}

func newTestSearchChunk(topK int64, ids []int64, scores []float32) *internalpb.SearchResults {
	return &internalpb.SearchResults{
		Status:     merr.Success(),
		MetricType: "IP",
		NumQueries: 1,
		TopK:       topK,
		ResultData: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       topK,
			Topks:      []int64{int64(len(ids))},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
			},
			Scores: scores,
		},
	}
}

func newTestSearchQueryPlan(shardID qviews.ShardID, mvcc *viewpb.QueryPlanMVCC) *viewpb.QueryPlan {
	queryNode := qviews.NewQueryNode(11)
	return &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 10},
		Mvcc:    mvcc,
		Request: &viewpb.QueryPlan_LegacySearchRequest{
			LegacySearchRequest: &internalpb.SearchRequest{},
		},
		WorkNodes: []*viewpb.QueryPlanWorkNode{
			{
				Node: &viewpb.QueryPlanWorkNode_QueryNode{
					QueryNode: &viewpb.QueryWorkNode{NodeId: queryNode.ID},
				},
			},
		},
	}
}
