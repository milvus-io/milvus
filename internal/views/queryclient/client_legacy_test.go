package queryclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLegacyClientSearchReturnsRawResults(t *testing.T) {
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
		},
		&legacyResolver{
			vchannels: []string{shardA.VChannel, shardB.VChannel},
			replicas: map[string]*resolver.ShardReplicas{
				shardA.VChannel: {VChannel: shardA.VChannel, PrimaryShardID: shardA, ShardIDs: []qviews.ShardID{shardA}},
				shardB.VChannel: {VChannel: shardB.VChannel, PrimaryShardID: shardB, ShardIDs: []qviews.ShardID{shardB}},
			},
		},
		firstReplicaPicker{},
	)

	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
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

type firstReplicaPicker struct{}

func (firstReplicaPicker) Pick(_ context.Context, info ReplicaPickInfo) (ReplicaPickResult, error) {
	return ReplicaPickResult{ShardID: info.ShardReplicas.ShardIDs[0]}, nil
}

func TestLegacyClientQueryReturnsRawResults(t *testing.T) {
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
			replicas: map[string]*resolver.ShardReplicas{
				shardID.VChannel: {VChannel: shardID.VChannel, PrimaryShardID: shardID, ShardIDs: []qviews.ShardID{shardID}},
			},
		},
		fixedReplicaPicker{shardID: shardID},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
	})
	require.NoError(t, err)

	require.Len(t, result.Results, 1)
	require.Equal(t, int64(201), result.Results[0].GetBase().GetSourceID())
	require.Len(t, result.Plans, 1)
}

func TestLegacyClientQuerySkipsEmptyDownstreamResults(t *testing.T) {
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
			replicas: map[string]*resolver.ShardReplicas{
				shardID.VChannel: {VChannel: shardID.VChannel, PrimaryShardID: shardID, ShardIDs: []qviews.ShardID{shardID}},
			},
		},
		fixedReplicaPicker{shardID: shardID},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
	})
	require.NoError(t, err)

	require.Empty(t, result.Results)
	require.Len(t, result.Plans, 1)
}

func TestLegacyClientQueryDoesNotDispatchWhenPlanHasNoWorkNodes(t *testing.T) {
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
			replicas: map[string]*resolver.ShardReplicas{
				shardID.VChannel: {VChannel: shardID.VChannel, PrimaryShardID: shardID, ShardIDs: []qviews.ShardID{shardID}},
			},
		},
		fixedReplicaPicker{shardID: shardID},
	)

	result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
	})
	require.NoError(t, err)

	require.Empty(t, result.Results)
	require.Len(t, result.Plans, 1)
	require.Zero(t, service.queryCallCount)
}

func TestLegacyClientSearchReturnsStatusError(t *testing.T) {
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
			replicas: map[string]*resolver.ShardReplicas{
				shardID.VChannel: {VChannel: shardID.VChannel, PrimaryShardID: shardID, ShardIDs: []qviews.ShardID{shardID}},
			},
		},
		fixedReplicaPicker{shardID: shardID},
	)

	_, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:     collectionID,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
	})
	require.Error(t, err)
}

type legacyResolver struct {
	vchannels []string
	replicas  map[string]*resolver.ShardReplicas
}

func (r *legacyResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return r.vchannels, nil
}

func (r *legacyResolver) ResolveShard(_ context.Context, _ int64, vchannel string) (*resolver.ShardReplicas, error) {
	return r.replicas[vchannel], nil
}

type legacyPlanClient struct {
	plans map[string]*viewpb.QueryPlan
}

func (c *legacyPlanClient) GetQueryPlan(_ context.Context, shardID qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	return &viewpb.GetQueryPlanResponse{Plan: c.plans[shardID.VChannel]}, nil
}

func (c *legacyPlanClient) GetMVCCTimestamp(context.Context, qviews.ShardID, *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	return &viewpb.GetMVCCTimestampResponse{}, nil
}

type legacyServiceClient struct {
	searchResults  map[string]*internalpb.SearchResults
	queryResults   map[string]*internalpb.RetrieveResults
	queryCallCount int
}

func (c *legacyServiceClient) SearchOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return &viewpb.SearchOnViewResponse{
		LegacyResults: c.searchResults[req.GetShardId().GetVchannel()],
	}, nil
}

func (c *legacyServiceClient) QueryOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	c.queryCallCount++
	return &viewpb.QueryOnViewResponse{
		LegacyResults: c.queryResults[req.GetShardId().GetVchannel()],
	}, nil
}

func (c *legacyServiceClient) RequeryOnView(context.Context, qviews.WorkNode, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return &viewpb.RequeryOnViewResponse{}, nil
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
