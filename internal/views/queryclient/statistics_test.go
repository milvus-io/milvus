package queryclient

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func statisticsResponse(version int64, rows int64, nodes ...int64) *viewpb.GetQueryPlanResponse {
	plan := &viewpb.QueryPlan{
		ShardId: &viewpb.ShardID{ReplicaId: 3, Vchannel: "p_1v0"},
		Version: &viewpb.QueryViewVersion{QueryVersion: version, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
		Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 99, TransformingTimetick: 99},
		Request: &viewpb.QueryPlan_StatisticsRequest{StatisticsRequest: &viewpb.StatisticsRequest{PartitionIds: []int64{10}}},
	}
	for _, id := range nodes {
		plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: id}}})
	}
	return &viewpb.GetQueryPlanResponse{Plan: plan, GrowingInlineResponse: &viewpb.GrowingInlineResponse{
		Response: &viewpb.GrowingInlineResponse_StatisticsResponse{StatisticsResponse: &viewpb.StatisticsOnViewResponse{RowCount: rows}},
	}}
}

func TestStatisticsRetriesEntireShardAndJoinsLateResults(t *testing.T) {
	setupTestReplicaRouting(t)
	mockey.PatchConvey("a retired View cannot contribute to its successor's count", t, func() {
		plans := 0
		lateFinished := false
		started := make(chan struct{})
		mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			require.EqualValues(t, 1, shard.ReplicaID)
			require.IsType(t, &viewpb.GetQueryPlanRequest_BarrierMvcc{}, req.GetMvcc())
			require.EqualValues(t, 99, req.GetBarrierMvcc())
			plans++
			if plans == 1 {
				return statisticsResponse(1, 5, 11, 12), nil
			}
			require.True(t, lateFinished, "old workers must finish before another SN sample")
			return statisticsResponse(2, 7, 11, 12), nil
		}).Build()
		mockey.Mock((*compositeViewQueryServiceClient).StatisticsOnView).To(func(_ *compositeViewQueryServiceClient, ctx context.Context, node qviews.WorkNode, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
			require.EqualValues(t, 3, req.GetShardId().GetReplicaId())
			require.Equal(t, []int64{10}, req.GetPartitionIds())
			require.EqualValues(t, 99, req.GetMvcc().GetTransformingTimetick())
			id := node.(qviews.QueryNode).ID
			if req.GetVersion().GetQueryVersion() == 1 {
				if id == 11 {
					close(started)
					<-ctx.Done()
					lateFinished = true
					return &viewpb.StatisticsOnViewResponse{RowCount: 100}, nil
				}
				<-started
				return nil, viewerror.NewViewInvalidated("handoff")
			}
			return &viewpb.StatisticsOnViewResponse{RowCount: (id - 9) * 10}, nil
		}).Build()
		client := newShardViewQueryClient(2, &fakeQueryPlanClient{}, &compositeViewQueryServiceClient{})
		rows, err := client.statistics(t.Context(), "p_1v0", 1, &viewpb.StatisticsRequest{}, 99)
		require.NoError(t, err)
		require.EqualValues(t, 57, rows)
		require.Equal(t, 2, plans)
	})
}

func TestStatisticsEmptyAndGrowingOnlyShards(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, count := range []int64{0, 42} {
		response := statisticsResponse(1, count)
		// Even zero rows must remain distinguishable from an omitted inline result.
		wire, err := proto.Marshal(response)
		require.NoError(t, err)
		decoded := &viewpb.GetQueryPlanResponse{}
		require.NoError(t, proto.Unmarshal(wire, decoded))
		client := newShardViewQueryClient(1, &fakeQueryPlanClient{plan: decoded.Plan, growingInlineResponse: decoded.GrowingInlineResponse}, nil)
		rows, err := client.statistics(t.Context(), "p_1v0", 1, &viewpb.StatisticsRequest{}, 99)
		require.NoError(t, err)
		require.Equal(t, count, rows)
	}
}

func TestStatisticsRejectsMissingSNResult(t *testing.T) {
	setupTestReplicaRouting(t)
	response := statisticsResponse(1, 0)
	response.GrowingInlineResponse = nil
	client := newShardViewQueryClient(1, &fakeQueryPlanClient{plan: response.Plan, growingInlineResponse: response.GrowingInlineResponse}, nil)
	_, err := client.statistics(t.Context(), "p_1v0", 1, &viewpb.StatisticsRequest{}, 99)
	require.ErrorContains(t, err, "misses growing result")
}

func TestStatisticsCollectionAggregatesAndPropagatesFailure(t *testing.T) {
	setupTestReplicaRouting(t)
	mockey.PatchConvey("collection statistics combine one result per shard and return no partial count", t, func() {
		var shardErr error
		mockey.Mock((*shardViewQueryClient).statistics).To(func(_ *shardViewQueryClient, _ context.Context, channel string, collectionID int64, req *viewpb.StatisticsRequest, barrierMVCC uint64) (int64, error) {
			require.EqualValues(t, 1, collectionID)
			require.EqualValues(t, 99, barrierMVCC)
			if channel == "b" {
				return 7, shardErr
			}
			return 5, nil
		}).Build()
		resolver := &legacyResolver{vchannels: []string{"a", "b"}}
		client := &legacyClient{shardClient: &shardViewQueryClient{}, shardResolver: resolver}
		req := &viewpb.StatisticsRequest{}
		rows, err := client.Statistics(t.Context(), 1, req, 99)
		require.NoError(t, err)
		require.EqualValues(t, 12, rows)
		shardErr = context.DeadlineExceeded
		rows, err = client.Statistics(t.Context(), 1, req, 99)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, rows)
		mockey.Mock((*legacyResolver).ResolveVChannels).Return(nil, context.Canceled).Build()
		_, err = client.Statistics(t.Context(), 1, req, 99)
		require.ErrorIs(t, err, context.Canceled)
	})
}
