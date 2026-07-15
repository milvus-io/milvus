package queryclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestShardSearchReturnsQueryPlanMVCCForRequery(t *testing.T) {
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
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       shardID.VChannel,
			PrimaryShardID: shardID,
			ShardIDs:       []qviews.ShardID{shardID},
		}},
		fixedReplicaPicker{shardID: shardID},
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

	require.True(t, proto.Equal(mvcc, shardPlan.Mvcc))
	require.True(t, proto.Equal(mvcc, queryService.searchReq.GetMvcc()))
	require.Equal(t, mvcc.GetTransformingTimetick(), queryService.searchReq.GetLegacyReq().GetMvccTimestamp())
}

func TestSessionSearchOnPrimaryLetsSNGenerateQueryPlanMVCC(t *testing.T) {
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
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       shardID.VChannel,
			PrimaryShardID: shardID,
			ShardIDs:       []qviews.ShardID{shardID},
		}},
		fixedReplicaPicker{shardID: shardID},
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

func TestSessionSearchOnSecondaryUsesPrimaryWALMVCC(t *testing.T) {
	vchannel := "by-dev-rootcoord-dml_0_100v0"
	primaryShardID := qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
	secondaryShardID := qviews.ShardID{ReplicaID: 2, VChannel: vchannel}
	mvcc := &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	}
	planClient := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(secondaryShardID, mvcc)}
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: primaryShardID,
			ShardIDs:       []qviews.ShardID{primaryShardID, secondaryShardID},
		}},
		fixedReplicaPicker{shardID: secondaryShardID},
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: vchannel,
		Req: &internalpb.SearchRequest{
			CollectionID:       100,
			ConsistencyLevel:   commonpb.ConsistencyLevel_Session,
			GuaranteeTimestamp: 999,
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	require.Equal(t, 1, planClient.mvccReqCount)
	require.Equal(t, primaryShardID, planClient.mvccShardID)
	require.Equal(t, vchannel, planClient.mvccReq.GetVchannel())
	require.True(t, proto.Equal(mvcc, planClient.planReq.GetQueryPlanMvcc()))
	require.Equal(t, commonpb.ConsistencyLevel(0), planClient.planReq.GetConsistencyLevel())
}

type fakeShardResolver struct {
	replicas *resolver.ShardReplicas
}

func (f *fakeShardResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{f.replicas.VChannel}, nil
}

func (f *fakeShardResolver) ResolveShard(context.Context, int64, string) (*resolver.ShardReplicas, error) {
	return f.replicas, nil
}

type fixedReplicaPicker struct {
	shardID qviews.ShardID
}

func (p fixedReplicaPicker) Pick(context.Context, ReplicaPickInfo) (ReplicaPickResult, error) {
	return ReplicaPickResult{ShardID: p.shardID}, nil
}

type fakeQueryPlanClient struct {
	plan         *viewpb.QueryPlan
	planReq      *viewpb.GetQueryPlanRequest
	mvccReq      *viewpb.GetMVCCTimestampRequest
	mvccShardID  qviews.ShardID
	mvccReqCount int
}

func (f *fakeQueryPlanClient) GetQueryPlan(_ context.Context, _ qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	f.planReq = proto.Clone(req).(*viewpb.GetQueryPlanRequest)
	return &viewpb.GetQueryPlanResponse{Plan: f.plan}, nil
}

func (f *fakeQueryPlanClient) GetMVCCTimestamp(_ context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	f.mvccReq = proto.Clone(req).(*viewpb.GetMVCCTimestampRequest)
	f.mvccShardID = shardID
	f.mvccReqCount++
	return &viewpb.GetMVCCTimestampResponse{Mvcc: f.plan.GetMvcc()}, nil
}

type fakeViewQueryServiceClient struct {
	searchReq *viewpb.SearchOnViewRequest
}

func (f *fakeViewQueryServiceClient) SearchOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	f.searchReq = req
	return &viewpb.SearchOnViewResponse{}, nil
}

func (f *fakeViewQueryServiceClient) QueryOnView(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return &viewpb.QueryOnViewResponse{}, nil
}

func (f *fakeViewQueryServiceClient) RequeryOnView(context.Context, qviews.WorkNode, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return &viewpb.RequeryOnViewResponse{}, nil
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
