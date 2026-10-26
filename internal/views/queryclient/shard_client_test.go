package queryclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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
	selection := &viewpb.SegmentSelection{SegmentIds: []int64{100}, Exclude: true}
	subSelections := []*viewpb.SegmentSelection{{SegmentIds: []int64{101}}, {Exclude: true}}
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
	searchReq *viewpb.SearchOnViewRequest
}

func (f *fakeViewQueryServiceClient) SearchOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	f.searchReq = req
	return &viewpb.SearchOnViewResponse{}, nil
}

func (f *fakeViewQueryServiceClient) QueryOnView(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return &viewpb.QueryOnViewResponse{}, nil
}

type fakeSearchResultReducer struct{}

func (fakeSearchResultReducer) Add(qviews.ShardID, *viewpb.SearchOnViewResponse) error {
	return nil
}

func (fakeSearchResultReducer) ResetShard(qviews.ShardID) {}

func (fakeSearchResultReducer) Finish() (*internalpb.SearchResults, error) {
	return &internalpb.SearchResults{}, nil
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
