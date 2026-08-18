package queryclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestValidateQueryPlan(t *testing.T) {
	shard := testShard("v0")
	valid := func() *viewpb.GetQueryPlanResponse {
		return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
			ShardId:   shard.IntoProto(),
			Version:   &viewpb.QueryViewVersion{QueryVersion: 1},
			WorkNodes: []*viewpb.QueryPlanWorkNode{queryWorkNode(1), streamingWorkNode("p0")},
		}}
	}
	plan, nodes, err := validateQueryPlan(valid(), shard)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, []qviews.WorkNode{qviews.NewQueryNode(1), qviews.StreamingNode{PChannel: "p0"}}, nodes)

	tests := []struct {
		name     string
		response *viewpb.GetQueryPlanResponse
	}{
		{"nil response", nil},
		{"nil plan", &viewpb.GetQueryPlanResponse{}},
		{"nil shard", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{Version: &viewpb.QueryViewVersion{}}}},
		{"wrong shard", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: testShard("v1").IntoProto(), Version: &viewpb.QueryViewVersion{}}}},
		{"nil version", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shard.IntoProto()}}},
		{"nil work node", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{}, WorkNodes: []*viewpb.QueryPlanWorkNode{nil}}}},
		{"unknown work node", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{}, WorkNodes: []*viewpb.QueryPlanWorkNode{{}}}}},
		{"invalid query node", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{}, WorkNodes: []*viewpb.QueryPlanWorkNode{queryWorkNode(0)}}}},
		{"invalid streaming node", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{}, WorkNodes: []*viewpb.QueryPlanWorkNode{streamingWorkNode("")}}}},
		{"duplicate work node", &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{}, WorkNodes: []*viewpb.QueryPlanWorkNode{queryWorkNode(1), queryWorkNode(1)}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := validateQueryPlan(test.response, shard)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestLegacyPlanRequestHelpers(t *testing.T) {
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
	searchPlan := &viewpb.QueryPlan{
		Mvcc: mvcc,
		Request: &viewpb.QueryPlan_LegacySearchRequest{
			LegacySearchRequest: &internalpb.SearchRequest{CollectionID: 1},
		},
	}
	searchRequest, err := legacySearchRequestForNode(searchPlan, qviews.StreamingNode{PChannel: "p0"})
	require.NoError(t, err)
	require.Equal(t, uint64(100), searchRequest.GetMvccTimestamp())
	require.NotSame(t, searchPlan.GetLegacySearchRequest(), searchRequest)
	searchRequest, err = legacySearchRequestForNode(searchPlan, qviews.NewQueryNode(1))
	require.NoError(t, err)
	require.Equal(t, uint64(90), searchRequest.GetMvccTimestamp())
	_, err = legacySearchRequestForNode(&viewpb.QueryPlan{}, qviews.NewQueryNode(1))
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	retrievePlan := &viewpb.QueryPlan{Request: &viewpb.QueryPlan_LegacyRetrieveRequest{
		LegacyRetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 1},
	}}
	retrieveRequest, err := legacyRetrieveRequestForNode(retrievePlan, qviews.NewQueryNode(1))
	require.NoError(t, err)
	require.Zero(t, retrieveRequest.GetMvccTimestamp())
	require.NotSame(t, retrievePlan.GetLegacyRetrieveRequest(), retrieveRequest)
	_, err = legacyRetrieveRequestForNode(&viewpb.QueryPlan{}, qviews.NewQueryNode(1))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestPrimaryPlanConsistencyLevel(t *testing.T) {
	require.Equal(t, commonpb.ConsistencyLevel_Strong,
		primaryPlanConsistencyLevel(commonpb.ConsistencyLevel_Session))
	for _, level := range []commonpb.ConsistencyLevel{
		commonpb.ConsistencyLevel_Strong,
		commonpb.ConsistencyLevel_Bounded,
		commonpb.ConsistencyLevel_Eventually,
	} {
		require.Equal(t, level, primaryPlanConsistencyLevel(level))
	}
}

func TestEmptySuccessfulQueryResponse(t *testing.T) {
	require.True(t, isEmptySuccessfulQueryResponse(&viewpb.QueryOnViewResponse{
		LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()},
	}))
	require.False(t, isEmptySuccessfulQueryResponse(nil))
	require.False(t, isEmptySuccessfulQueryResponse(&viewpb.QueryOnViewResponse{}))
	require.False(t, isEmptySuccessfulQueryResponse(&viewpb.QueryOnViewResponse{
		LegacyResults: &internalpb.RetrieveResults{Status: merr.Status(merr.WrapErrServiceInternalMsg("failed"))},
	}))
}

func TestFanOutWithNoWorkNodes(t *testing.T) {
	called := false
	err := fanOut(context.Background(), nil, &viewpb.QueryPlan{}, testShard("v0"),
		func(context.Context, qviews.WorkNode, *viewpb.QueryPlan, qviews.ShardID) error {
			called = true
			return nil
		})
	require.NoError(t, err)
	require.False(t, called)
}

func TestNormalizeBoundaryError(t *testing.T) {
	require.NoError(t, normalizeBoundaryError(nil, "op"))
	require.ErrorIs(t, normalizeBoundaryError(context.DeadlineExceeded, "op"), context.DeadlineExceeded)
	typed := merr.WrapErrCollectionNotLoaded(1)
	require.Equal(t, typed, normalizeBoundaryError(typed, "op"))
	viewErr := errors.New("plain")
	require.ErrorIs(t, normalizeBoundaryError(viewErr, "op"), merr.ErrServiceInternal)
}
