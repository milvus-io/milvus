package queryclient

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestValidateQueryPlan(t *testing.T) {
	shard := testShard("v0")
	valid := func() *viewpb.GetQueryPlanResponse {
		return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
			ShardId: shard.IntoProto(),
			Version: &viewpb.QueryViewVersion{QueryVersion: 1},
			Mvcc: &viewpb.QueryPlanMVCC{
				GrowingTimetick:      100,
				TransformingTimetick: 110,
			},
			WorkNodes: []*viewpb.QueryPlanWorkNode{queryWorkNode(1), streamingWorkNode("p0")},
		}}
	}
	plan, nodes, err := validateQueryPlan(valid(), shard)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, []qviews.WorkNode{qviews.NewQueryNode(1), qviews.StreamingNode{PChannel: "p0"}}, nodes)

	response := valid()
	response.Plan.WorkNodes = nil
	response.Plan.SkipExecution = true
	_, nodes, err = validateQueryPlan(response, shard)
	require.NoError(t, err)
	require.Empty(t, nodes)

	tests := []struct {
		name   string
		mutate func(*viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse
	}{
		{"nil response", func(*viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse { return nil }},
		{"nil plan", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan = nil
			return response
		}},
		{"nil shard", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.ShardId = nil
			return response
		}},
		{"wrong shard", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.ShardId = testShard("v1").IntoProto()
			return response
		}},
		{"nil version", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.Version = nil
			return response
		}},
		{"nil MVCC", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.Mvcc = nil
			return response
		}},
		{"empty implicit execution", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.WorkNodes = nil
			return response
		}},
		{"skip with work node", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.SkipExecution = true
			return response
		}},
		{"nil work node", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{nil}
			return response
		}},
		{"unknown work node", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{}}
			return response
		}},
		{"invalid query node", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{queryWorkNode(0)}
			return response
		}},
		{"invalid streaming node", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{streamingWorkNode("")}
			return response
		}},
		{"duplicate work node", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.WorkNodes = []*viewpb.QueryPlanWorkNode{queryWorkNode(1), queryWorkNode(1)}
			return response
		}},
		{"missing query node MVCC", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.Mvcc.TransformingTimetick = 0
			return response
		}},
		{"missing streaming node MVCC", func(response *viewpb.GetQueryPlanResponse) *viewpb.GetQueryPlanResponse {
			response.Plan.Mvcc.GrowingTimetick = 0
			return response
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := validateQueryPlan(test.mutate(valid()), shard)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestValidateLegacyPlanDomain(t *testing.T) {
	require.NoError(t, validateLegacySearchPlan(&viewpb.QueryPlan{
		PlanDelta: &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{}},
	}))
	require.NoError(t, validateLegacyRetrievePlan(&viewpb.QueryPlan{
		PlanDelta: &viewpb.QueryPlan_LegacyRetrievePlan{LegacyRetrievePlan: &viewpb.LegacyRetrievePlan{}},
	}))
	require.NoError(t, validateLegacySearchPlan(&viewpb.QueryPlan{
		Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{}},
	}))
	require.NoError(t, validateLegacyRetrievePlan(&viewpb.QueryPlan{
		Request: &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{}},
	}))

	for _, plan := range []*viewpb.QueryPlan{
		{},
		{
			Request:   &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{}},
			PlanDelta: &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{}},
		},
		{PlanDelta: &viewpb.QueryPlan_LegacyRetrievePlan{LegacyRetrievePlan: &viewpb.LegacyRetrievePlan{}}},
	} {
		require.ErrorIs(t, validateLegacySearchPlan(plan), merr.ErrServiceInternal)
	}
	for _, plan := range []*viewpb.QueryPlan{
		{},
		{
			Request:   &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{}},
			PlanDelta: &viewpb.QueryPlan_LegacyRetrievePlan{LegacyRetrievePlan: &viewpb.LegacyRetrievePlan{}},
		},
		{PlanDelta: &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{}}},
	} {
		require.ErrorIs(t, validateLegacyRetrievePlan(plan), merr.ErrServiceInternal)
	}
}

func TestLegacyPlanRequestHelpers(t *testing.T) {
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
	originalSearch := &internalpb.SearchRequest{
		CollectionID:       1,
		SerializedExprPlan: []byte{1},
		PlaceholderGroup:   []byte{2},
	}
	searchPlan := &viewpb.QueryPlan{
		Mvcc: mvcc,
		PlanDelta: &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{
			SerializedExprPlan: []byte{},
			PlaceholderGroup:   []byte{3},
		}},
	}
	searchRequest, err := legacySearchRequestForNode(originalSearch, searchPlan, qviews.StreamingNode{PChannel: "p0"})
	require.NoError(t, err)
	require.Equal(t, uint64(100), searchRequest.GetMvccTimestamp())
	require.Equal(t, int64(1), searchRequest.GetCollectionID())
	require.Empty(t, searchRequest.GetSerializedExprPlan())
	require.Equal(t, []byte{3}, searchRequest.GetPlaceholderGroup())
	require.Equal(t, []byte{1}, originalSearch.GetSerializedExprPlan())
	require.Equal(t, []byte{2}, originalSearch.GetPlaceholderGroup())

	searchRequest, err = legacySearchRequestForNode(originalSearch, searchPlan, qviews.NewQueryNode(1))
	require.NoError(t, err)
	require.Equal(t, uint64(90), searchRequest.GetMvccTimestamp())

	deprecatedSearch := &internalpb.SearchRequest{CollectionID: 2}
	searchRequest, err = legacySearchRequestForNode(originalSearch, &viewpb.QueryPlan{
		Mvcc:    mvcc,
		Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: deprecatedSearch},
	}, qviews.NewQueryNode(1))
	require.NoError(t, err)
	require.Equal(t, int64(2), searchRequest.GetCollectionID())
	require.NotSame(t, deprecatedSearch, searchRequest)

	originalRetrieve := &internalpb.RetrieveRequest{CollectionID: 1}
	retrievePlan := &viewpb.QueryPlan{
		Mvcc:      mvcc,
		PlanDelta: &viewpb.QueryPlan_LegacyRetrievePlan{LegacyRetrievePlan: &viewpb.LegacyRetrievePlan{}},
	}
	retrieveRequest, err := legacyRetrieveRequestForNode(originalRetrieve, retrievePlan, qviews.NewQueryNode(1))
	require.NoError(t, err)
	require.Equal(t, uint64(90), retrieveRequest.GetMvccTimestamp())
	require.Equal(t, int64(1), retrieveRequest.GetCollectionID())
	require.NotSame(t, originalRetrieve, retrieveRequest)

	_, err = legacySearchRequestForNode(originalSearch, &viewpb.QueryPlan{}, qviews.NewQueryNode(1))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = legacyRetrieveRequestForNode(originalRetrieve, &viewpb.QueryPlan{}, qviews.NewQueryNode(1))
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

func TestSelectFanOutError(t *testing.T) {
	retryable := viewerror.NewViewInvalidated("stale")
	nonRetryable := viewerror.NewUnknownError("broken")

	require.NoError(t, selectFanOutError([]error{nil, nil}))
	require.Equal(t, retryable, selectFanOutError([]error{context.Canceled, retryable}))
	require.Equal(t, nonRetryable, selectFanOutError([]error{retryable, nonRetryable}))
	require.Equal(t, nonRetryable, selectFanOutError([]error{nonRetryable, retryable}))
	require.ErrorIs(t, selectFanOutError([]error{context.Canceled}), context.Canceled)
}

func TestFanOutUsesStableNodeOrderForErrors(t *testing.T) {
	nodes := []qviews.WorkNode{qviews.NewQueryNode(1), qviews.NewQueryNode(2)}
	first := errors.New("first node")
	secondCompleted := make(chan struct{})
	err := fanOut(context.Background(), nodes, &viewpb.QueryPlan{}, testShard("v0"),
		func(_ context.Context, node qviews.WorkNode, _ *viewpb.QueryPlan, _ qviews.ShardID) error {
			if node.(qviews.QueryNode).ID == 1 {
				<-secondCompleted
				return first
			}
			close(secondCompleted)
			return errors.New("second node")
		})
	require.Equal(t, first, err)
}

func TestWaitBeforeRetryHonorsCancellation(t *testing.T) {
	client := &shardViewQueryClient{
		maxAttempts:         2,
		retryInitialBackoff: time.Hour,
		retryMaxBackoff:     time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.waitBeforeRetry(ctx, 0), context.Canceled)
}

func TestNormalizeBoundaryError(t *testing.T) {
	require.NoError(t, normalizeBoundaryError(nil, "op"))
	require.ErrorIs(t, normalizeBoundaryError(context.DeadlineExceeded, "op"), context.DeadlineExceeded)
	typed := merr.WrapErrCollectionNotLoaded(1)
	require.Equal(t, typed, normalizeBoundaryError(typed, "op"))
	require.ErrorIs(t, normalizeBoundaryError(errors.New("plain"), "op"), merr.ErrServiceInternal)
	require.ErrorIs(t, normalizeBoundaryError(viewerror.NewViewInvalidated("stale"), "op"), merr.ErrServiceUnavailable)
	require.ErrorIs(t, normalizeBoundaryError(viewerror.NewUnknownError("broken"), "op"), merr.ErrServiceInternal)

	unavailable := viewerror.ConvertViewError("test", status.Error(codes.Unavailable, "down"))
	require.ErrorIs(t, normalizeBoundaryError(unavailable, "op"), merr.ErrServiceUnavailable)
	deadline := viewerror.ConvertViewError("test", status.Error(codes.DeadlineExceeded, "slow"))
	require.ErrorIs(t, normalizeBoundaryError(deadline, "op"), merr.ErrServiceUnavailable)
}
