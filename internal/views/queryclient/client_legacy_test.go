package queryclient

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLegacyClientPlainSearchUsesBatchWhenStreamingDisabled(t *testing.T) {
	setupTestReplicaRouting(t)
	collectionID := int64(100)
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	shardB := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_1_100v1"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardA.VChannel: legacySearchPlan(shardA, queryNode),
			shardB.VChannel: legacySearchPlan(shardB, queryNode),
		}},
		&legacyServiceClient{
			searchResults: map[string]*internalpb.SearchResults{
				shardA.VChannel: {Status: merr.Success(), Base: &commonpb.MsgBase{SourceID: 101}},
				shardB.VChannel: {Status: merr.Success(), Base: &commonpb.MsgBase{SourceID: 102}},
			},
			searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				return nil, errors.New("stream path should not be used")
			},
		},
		&legacyResolver{
			vchannels: []string{shardA.VChannel, shardB.VChannel},
		},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Nq:               1,
			Topk:             1,
			MetricType:       "IP",
		},
	})
	require.NoError(t, err)

	require.Len(t, result.Results, 2)
	require.ElementsMatch(t, []int64{101, 102}, []int64{
		result.Results[0].GetBase().GetSourceID(),
		result.Results[1].GetBase().GetSourceID(),
	})
	require.Len(t, result.Plans, 2)
}

func TestLegacyClientSearchReducesIteratorVChannelStreams(t *testing.T) {
	collectionID := int64(100)
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	shardB := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_1_100v1"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1, EnableReduceStream: true, ReduceStreamChunkBytes: 20},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardA.VChannel: legacySearchPlan(shardA, queryNode),
			shardB.VChannel: legacySearchPlan(shardB, queryNode),
		}},
		&legacyServiceClient{searchResults: map[string]*internalpb.SearchResults{
			shardA.VChannel: newTestSearchChunk(3, []int64{1, 4}, []float32{0.9, 0.6}),
			shardB.VChannel: newTestSearchChunk(3, []int64{2, 3}, []float32{0.8, 0.7}),
		}},
		&legacyResolver{
			vchannels: []string{shardA.VChannel, shardB.VChannel},
		},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
		AllowStreaming: true,
		Req: &internalpb.SearchRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Nq:               1,
			Topk:             3,
			MetricType:       "IP",
			IsIterator:       true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Stream)
	chunk, err := result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []float32{0.9, 0.8}, chunk.GetResultData().GetScores())
	chunk, err = result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3}, chunk.GetResultData().GetIds().GetIntId().GetData())
	require.Equal(t, []float32{0.7}, chunk.GetResultData().GetScores())
	_, err = result.Stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, result.Stream.Close())
	require.Len(t, result.Plans, 2)
}

func TestLegacyClientPlainANNUsesStreamingWhenEnabled(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)
	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1, EnableReduceStream: true, ReduceStreamChunkBytes: 20},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacySearchPlan(shardID, queryNode),
		}},
		&legacyServiceClient{searchResults: map[string]*internalpb.SearchResults{
			shardID.VChannel: newTestSearchChunk(2, []int64{10, 20}, []float32{0.9, 0.8}),
		}},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{AllowStreaming: true, Req: &internalpb.SearchRequest{
		CollectionID:     100,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		Nq:               1,
		Topk:             2,
		MetricType:       "IP",
		SearchType:       internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER,
	}})
	require.NoError(t, err)
	require.NotNil(t, result.Stream)
	chunk, err := result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, chunk.GetResultData().GetIds().GetIntId().GetData())
	_, err = result.Stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, result.Stream.Close())
}

func TestLegacyClientIteratorSearchUsesBatchWhenStreamingDisabled(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)
	batchResult := newTestSearchChunk(1, []int64{10}, []float32{0.9})
	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacySearchPlan(shardID, queryNode),
		}},
		&legacyServiceClient{
			searchResults: map[string]*internalpb.SearchResults{shardID.VChannel: batchResult},
			searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				return nil, errors.New("stream path should not be used")
			},
		},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{AllowStreaming: true, Req: &internalpb.SearchRequest{
		CollectionID:     100,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		Nq:               1,
		Topk:             1,
		MetricType:       "IP",
		IsIterator:       true,
	}})
	require.NoError(t, err)
	require.Nil(t, result.Stream)
	require.Len(t, result.Results, 1)
	require.Same(t, batchResult, result.Results[0])
}

func TestLegacyClientSearchUsesBatchForUnsupportedIterator(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)
	batchResult := newTestSearchChunk(1, []int64{10}, []float32{0.9})
	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1, EnableReduceStream: true},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacySearchPlan(shardID, queryNode),
		}},
		&legacyServiceClient{
			searchResults: map[string]*internalpb.SearchResults{shardID.VChannel: batchResult},
			searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				return nil, errors.New("stream path should not be used")
			},
		},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{Req: &internalpb.SearchRequest{
		CollectionID:     100,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		Nq:               1,
		Topk:             1,
		MetricType:       "IP",
		IsIterator:       true,
		GroupByFieldId:   100,
	}})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.Same(t, batchResult, result.Results[0])
}

func TestLegacyClientSearchReturnsUnconsumedStream(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)
	childStream := &fakeSearchStream{recv: []fakeSearchStreamRecv{{err: errors.New("first receive failed")}}}
	openCount := 0

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 2, EnableReduceStream: true, ReduceStreamChunkBytes: 32},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacySearchPlan(shardID, queryNode),
		}},
		&legacyServiceClient{
			searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				openCount++
				return childStream, nil
			},
		},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{AllowStreaming: true, Req: &internalpb.SearchRequest{
		CollectionID:     100,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		Nq:               1,
		Topk:             1,
		MetricType:       "IP",
		IsIterator:       true,
	}})
	require.NoError(t, err)
	require.Equal(t, 1, openCount)
	require.Equal(t, 2, client.Legacy().MaxRetries())
	require.True(t, client.Legacy().ReduceStreamEnabled())
	require.NotNil(t, result.Stream)
	_, err = result.Stream.Recv()
	require.ErrorContains(t, err, "first receive failed")
	require.NoError(t, result.Stream.Close())
	require.Equal(t, 1, childStream.closeCount())
}

func TestLegacyClientQueryReturnsRawResults(t *testing.T) {
	setupTestReplicaRouting(t)
	collectionID := int64(100)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1, EnableReduceStream: true},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacyQueryPlan(shardID, queryNode),
		}},
		&legacyServiceClient{
			queryResults: map[string]*internalpb.RetrieveResults{
				shardID.VChannel: {
					Status: merr.Success(),
					Base:   &commonpb.MsgBase{SourceID: 201},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}},
					},
				},
			},
		},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Limit:            1,
		},
	})
	require.NoError(t, err)

	require.Nil(t, result.Stream)
	require.Len(t, result.Results, 1)
	require.Equal(t, int64(201), result.Results[0].GetBase().GetSourceID())
	require.Len(t, result.Plans, 1)
}

func TestLegacyClientQueryReducesIteratorVChannelStreams(t *testing.T) {
	collectionID := int64(100)
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	shardB := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_1_100v1"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1, EnableReduceStream: true, ReduceStreamChunkBytes: 32},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardA.VChannel: legacyQueryPlan(shardA, queryNode),
			shardB.VChannel: legacyQueryPlan(shardB, queryNode),
		}},
		&legacyServiceClient{queryResults: map[string]*internalpb.RetrieveResults{
			shardA.VChannel: newTestQueryChunk([]int64{1, 3}),
			shardB.VChannel: newTestQueryChunk([]int64{2, 4}),
		}},
		&legacyResolver{vchannels: []string{shardA.VChannel, shardB.VChannel}},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		AllowStreaming: true,
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			IsIterator:       true,
			Limit:            3,
		},
	})

	require.NoError(t, err)
	require.Empty(t, result.Results)
	require.Len(t, result.Plans, 2)
	first, err := result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, first.GetIds().GetIntId().GetData())
	second, err := result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3}, second.GetIds().GetIntId().GetData())
	chunk, err := result.Stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
}

func TestLegacyClientQueryReducesBoundedOrdinaryVChannelStreams(t *testing.T) {
	collectionID := int64(100)
	shardA := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	shardB := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_1_100v1"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1, EnableReduceStream: true, ReduceStreamChunkBytes: 32},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardA.VChannel: legacyQueryPlan(shardA, queryNode),
			shardB.VChannel: legacyQueryPlan(shardB, queryNode),
		}},
		&legacyServiceClient{queryResults: map[string]*internalpb.RetrieveResults{
			shardA.VChannel: newTestQueryChunk([]int64{1, 3}),
			shardB.VChannel: newTestQueryChunk([]int64{2, 4}),
		}},
		&legacyResolver{vchannels: []string{shardA.VChannel, shardB.VChannel}},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		AllowStreaming: true,
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Limit:            3,
		},
	})

	require.NoError(t, err)
	require.Empty(t, result.Results)
	require.Len(t, result.Plans, 2)
	first, err := result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, first.GetIds().GetIntId().GetData())
	second, err := result.Stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3}, second.GetIds().GetIntId().GetData())
}

func TestLegacyClientQueryKeepsUnlimitedOrdinaryQueryOnBatchPath(t *testing.T) {
	require.False(t, SupportsQueryStream(&internalpb.RetrieveRequest{}))
	require.True(t, SupportsQueryStream(&internalpb.RetrieveRequest{
		Limit:         1,
		OrderByFields: []*planpb.OrderByField{{FieldId: 101, Ascending: true}},
	}))
}

func TestLegacyClientQueryPreservesEmptyDownstreamSnapshot(t *testing.T) {
	setupTestReplicaRouting(t)
	collectionID := int64(100)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacyQueryPlan(shardID, queryNode),
		}},
		&legacyServiceClient{
			queryResults: map[string]*internalpb.RetrieveResults{
				shardID.VChannel: {Status: merr.Success(), CostAggregation: &internalpb.CostAggregation{}},
			},
		},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
	})
	require.NoError(t, err)

	require.Len(t, result.Results, 1)
	require.Empty(t, result.Results[0].GetIds())
	require.EqualValues(t, 100, result.Results[0].GetMvccTimestamp())
	require.Zero(t, result.Results[0].GetFeatureBits())
	require.Len(t, result.Plans, 1)
}

func TestLegacyClientQueryDoesNotDispatchWhenPlanHasNoWorkNodes(t *testing.T) {
	setupTestReplicaRouting(t)
	collectionID := int64(100)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	service := &legacyServiceClient{
		queryResults: map[string]*internalpb.RetrieveResults{
			shardID.VChannel: {Status: merr.Success(), Base: &commonpb.MsgBase{SourceID: 201}},
		},
	}

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacyQueryPlanWithoutWorkNodes(shardID),
		}},
		service,
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
	})
	require.NoError(t, err)

	require.Len(t, result.Results, 1)
	require.Empty(t, result.Results[0].GetIds())
	require.EqualValues(t, 100, result.Results[0].GetMvccTimestamp())
	require.Zero(t, result.Results[0].GetFeatureBits())
	require.Len(t, result.Plans, 1)
	require.Zero(t, service.queryCallCount)
}

func TestLegacyClientSearchReturnsStatusError(t *testing.T) {
	setupTestReplicaRouting(t)
	collectionID := int64(100)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	queryNode := qviews.NewQueryNode(11)

	client := NewLegacyViewQueryClient(
		ViewQueryClientConfig{MaxRetries: 1},
		&legacyPlanClient{plans: map[string]*viewpb.QueryPlan{
			shardID.VChannel: legacySearchPlan(shardID, queryNode),
		}},
		&legacyServiceClient{
			searchResults: map[string]*internalpb.SearchResults{
				shardID.VChannel: {Status: merr.Status(merr.WrapErrServiceInternalMsg("search failed"))},
			},
		},
		&legacyResolver{
			vchannels: []string{shardID.VChannel},
		},
	)

	_, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Nq:               1,
			Topk:             1,
			MetricType:       "IP",
		},
	})
	require.Error(t, err)
}

type legacyResolver struct {
	vchannels []string
}

func (r *legacyResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return r.vchannels, nil
}

type legacyPlanClient struct {
	QueryPlanClient
	plans map[string]*viewpb.QueryPlan
}

func (c *legacyPlanClient) GetQueryPlan(_ context.Context, shardID qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	return &viewpb.GetQueryPlanResponse{Plan: c.plans[shardID.VChannel]}, nil
}

func (c *legacyPlanClient) GetMVCCTimestamp(context.Context, qviews.ShardID, *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	return &viewpb.GetMVCCTimestampResponse{}, nil
}

type legacyServiceClient struct {
	ViewQueryServiceClient
	searchResults      map[string]*internalpb.SearchResults
	searchOnViewStream func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error)
	queryResults       map[string]*internalpb.RetrieveResults
	queryOnViewStream  func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error)
	queryCallCount     int
}

func (c *legacyServiceClient) SearchOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return &viewpb.SearchOnViewResponse{
		LegacyResults: c.searchResults[req.GetShardId().GetVchannel()],
	}, nil
}

func (c *legacyServiceClient) SearchOnViewStream(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
	if c.searchOnViewStream != nil {
		return c.searchOnViewStream(ctx, node, req)
	}
	return &fakeSearchStream{
		ctx:  ctx,
		recv: []fakeSearchStreamRecv{{chunk: c.searchResults[req.GetShardId().GetVchannel()]}},
	}, nil
}

func (c *legacyServiceClient) QueryOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	c.queryCallCount++
	return &viewpb.QueryOnViewResponse{
		LegacyResults: c.queryResults[req.GetShardId().GetVchannel()],
	}, nil
}

func (c *legacyServiceClient) QueryOnViewStream(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error) {
	if c.queryOnViewStream != nil {
		return c.queryOnViewStream(ctx, node, req)
	}
	return &fakeQueryStream{
		ctx:  ctx,
		recv: []fakeQueryStreamRecv{{chunk: c.queryResults[req.GetShardId().GetVchannel()]}},
	}, nil
}
func legacySearchPlan(shardID qviews.ShardID, node qviews.QueryNode) *viewpb.QueryPlan {
	return &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 1},
		Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90},
		Request: &viewpb.QueryPlan_LegacySearchRequest{
			LegacySearchRequest: &internalpb.SearchRequest{},
		},
		WorkNodes: []*viewpb.QueryPlanWorkNode{
			{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: node.ID}}},
		},
	}
}

func legacyQueryPlan(shardID qviews.ShardID, node qviews.QueryNode) *viewpb.QueryPlan {
	return &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 1},
		Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90},
		Request: &viewpb.QueryPlan_LegacyRetrieveRequest{
			LegacyRetrieveRequest: &internalpb.RetrieveRequest{},
		},
		WorkNodes: []*viewpb.QueryPlanWorkNode{
			{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: node.ID}}},
		},
	}
}

func legacyQueryPlanWithoutWorkNodes(shardID qviews.ShardID) *viewpb.QueryPlan {
	return &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 1},
		Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90},
		Request: &viewpb.QueryPlan_LegacyRetrieveRequest{
			LegacyRetrieveRequest: &internalpb.RetrieveRequest{},
		},
	}
}

func TestLegacyRequeryUsesRetainedViewWithoutReplanning(t *testing.T) {
	setupTestReplicaRouting(t)
	mockey.PatchConvey("requery preserves the exact parent plan, including failures", t, func() {
		node := qviews.NewQueryNode(9)
		plan := ShardPlan{ShardID: qviews.ShardID{VChannel: "p_1v0", ReplicaID: 2}, Version: &viewpb.QueryViewVersion{QueryVersion: 5}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}, WorkNodes: []qviews.WorkNode{node}}
		selection := &viewpb.SegmentSelection{SegmentIds: []int64{777}, Mode: viewpb.SegmentSelection_Include}
		plan.Selections = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 9}}, SegmentSelection: selection}}
		service := &fakeViewQueryServiceClient{}
		client := &legacyClient{shardClient: &shardViewQueryClient{queryServiceClient: service}}
		var failure error
		calls := 0
		mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, selected qviews.WorkNode, request *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			calls++
			require.Equal(t, node, selected)
			require.True(t, proto.Equal(selection, request.GetSegmentSelection()))
			require.True(t, proto.Equal(plan.Version, request.GetVersion()))
			require.True(t, proto.Equal(plan.Mvcc, request.GetMvcc()))
			require.EqualValues(t, 90, request.GetLegacyReq().GetMvccTimestamp())
			require.EqualValues(t, 1, request.GetLegacyReq().GetSchemaVersion())
			require.Equal(t, []int64{101, 102}, request.GetLegacyReq().GetOutputFieldsId())
			if failure != nil {
				return nil, failure
			}
			return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
		}).Build()
		// Requery chooses its output fields now; the retained plan carries no
		// field allowlist and does not require another Phase 1 request.
		request := &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{SchemaVersion: 1, OutputFieldsId: []int64{101, 102}}, Plans: []ShardPlan{plan}}
		result, err := client.Query(t.Context(), request)
		require.NoError(t, err)
		require.Len(t, result.Plans, 1)
		require.Equal(t, 1, calls)
		failure = viewerror.NewViewInvalidated("original view retired")
		_, err = client.Query(t.Context(), request)
		require.ErrorIs(t, err, merr.ErrQueryViewNotReady)
		require.Equal(t, 2, calls, "do not retry into another snapshot")
		failure = merr.WrapErrFieldNotFound(102, "not available during requery")
		_, err = client.Query(t.Context(), request)
		require.ErrorIs(t, err, failure)
		require.Equal(t, 3, calls, "field errors abort requery without replanning")
		require.Zero(t, request.Req.MvccTimestamp, "dispatch must not mutate the shared request")
	})
}
