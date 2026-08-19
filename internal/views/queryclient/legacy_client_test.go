package queryclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLegacySearchAcrossPrimaryShards(t *testing.T) {
	mockey.PatchConvey("returns raw SN and QN results with per-node MVCC", t, func() {
		vchannels := []string{"v0", "v1"}
		var requestMu sync.Mutex
		planRequests := make(map[string]*viewpb.GetQueryPlanRequest)

		mockey.Mock((*mockShardResolver).ResolveVChannels).Return(vchannels, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).To(
			func(_ *mockShardResolver, _ context.Context, _ int64, vchannel string) (*resolver.ShardReplicas, error) {
				shard := testShard(vchannel)
				return &resolver.ShardReplicas{VChannel: vchannel, PrimaryShardID: shard}, nil
			}).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).To(
			func(_ *mockQueryPlanClient, _ context.Context, shard qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				requestMu.Lock()
				planRequests[shard.VChannel] = proto.Clone(request).(*viewpb.GetQueryPlanRequest)
				requestMu.Unlock()
				return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
					ShardId: shard.IntoProto(),
					Version: &viewpb.QueryViewVersion{QueryVersion: 10},
					Mvcc: &viewpb.QueryPlanMVCC{
						GrowingTimetick:      100,
						TransformingTimetick: 90,
					},
					WorkNodes: []*viewpb.QueryPlanWorkNode{
						streamingWorkNode("p-" + shard.VChannel),
						queryWorkNode(10),
					},
					PlanDelta: &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{}},
				}}, nil
			}).Build()
		mockey.Mock((*mockViewQueryServiceClient).SearchOnView).To(
			func(_ *mockViewQueryServiceClient, _ context.Context, node qviews.WorkNode, request *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				sourceID := int64(1)
				expectedMVCC := uint64(90)
				if node.NodeType() == qviews.NodeTypeStreamingNode {
					sourceID = 2
					expectedMVCC = 100
				}
				if actual := request.GetLegacyReq().GetMvccTimestamp(); actual != expectedMVCC {
					return nil, merr.WrapErrServiceInternalMsg(
						"unexpected node MVCC: expected=%d, actual=%d", expectedMVCC, actual)
				}
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
					Status: merr.Success(),
					Base:   &commonpb.MsgBase{SourceID: sourceID},
				}}, nil
			}).Build()

		client := NewLegacyViewQueryClient(
			ViewQueryClientConfig{MaxAttempts: 1},
			&mockQueryPlanClient{},
			&mockViewQueryServiceClient{},
			&mockShardResolver{},
			NewPrimaryReplicaPicker(),
		)
		result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{Req: &internalpb.SearchRequest{
			CollectionID:     100,
			PartitionIDs:     []int64{7},
			ConsistencyLevel: commonpb.ConsistencyLevel_Session,
		}})
		require.NoError(t, err)
		require.Len(t, result.Results, 4)
		require.Len(t, result.Plans, 2)
		require.Equal(t, testShard("v0"), result.Plans[0].ShardID)
		require.Equal(t, testShard("v1"), result.Plans[1].ShardID)
		for _, vchannel := range vchannels {
			request := planRequests[vchannel]
			require.Equal(t, commonpb.ConsistencyLevel_Strong, request.GetConsistencyLevel())
			require.Equal(t, []int64{7}, request.GetPartitionIds())
			require.NotNil(t, request.GetLegacySearchRequest())
		}
		for _, entry := range result.Results {
			require.Contains(t, vchannels, entry.ShardID.VChannel)
			require.NotNil(t, entry.Result)
		}
	})
}

func TestLegacyQueryRetainsEmptyResults(t *testing.T) {
	mockey.PatchConvey("retains an empty successful node response and its shard", t, func() {
		shard := testShard("v0")
		var planRequest *viewpb.GetQueryPlanRequest
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{shard.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).Return(
			&resolver.ShardReplicas{VChannel: shard.VChannel, PrimaryShardID: shard}, nil).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).To(
			func(_ *mockQueryPlanClient, _ context.Context, _ qviews.ShardID, request *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				planRequest = proto.Clone(request).(*viewpb.GetQueryPlanRequest)
				return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
					ShardId:   shard.IntoProto(),
					Version:   &viewpb.QueryViewVersion{QueryVersion: 1},
					Mvcc:      &viewpb.QueryPlanMVCC{TransformingTimetick: 80},
					WorkNodes: []*viewpb.QueryPlanWorkNode{queryWorkNode(10)},
					PlanDelta: &viewpb.QueryPlan_LegacyRetrievePlan{LegacyRetrievePlan: &viewpb.LegacyRetrievePlan{}},
				}}, nil
			}).Build()
		mockey.Mock((*mockViewQueryServiceClient).QueryOnView).To(
			func(_ *mockViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, request *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
				if actual := request.GetLegacyReq().GetMvccTimestamp(); actual != 80 {
					return nil, merr.WrapErrServiceInternalMsg("unexpected query node MVCC: %d", actual)
				}
				return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{
					Status: merr.Success(),
				}}, nil
			}).Build()

		client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxAttempts: 1}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, NewPrimaryReplicaPicker())
		result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		}})
		require.NoError(t, err)
		require.Len(t, result.Results, 1)
		require.Equal(t, shard, result.Results[0].ShardID)
		require.NotNil(t, result.Results[0].Result)
		require.Len(t, result.Plans, 1)
		require.Equal(t, commonpb.ConsistencyLevel_Bounded, planRequest.GetConsistencyLevel())
		require.NotNil(t, planRequest.GetLegacyRetrieveRequest())
	})
}

func TestLegacyQueryReturnsRawResult(t *testing.T) {
	mockey.PatchConvey("returns a non-empty retrieve result", t, func() {
		shard := testShard("v0")
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{shard.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).Return(
			&resolver.ShardReplicas{VChannel: shard.VChannel, PrimaryShardID: shard}, nil).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).Return(&viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
			ShardId:   shard.IntoProto(),
			Version:   &viewpb.QueryViewVersion{QueryVersion: 1},
			Mvcc:      &viewpb.QueryPlanMVCC{TransformingTimetick: 80},
			WorkNodes: []*viewpb.QueryPlanWorkNode{queryWorkNode(10)},
			PlanDelta: &viewpb.QueryPlan_LegacyRetrievePlan{LegacyRetrievePlan: &viewpb.LegacyRetrievePlan{}},
		}}, nil).Build()
		mockey.Mock((*mockViewQueryServiceClient).QueryOnView).Return(&viewpb.QueryOnViewResponse{
			LegacyResults: &internalpb.RetrieveResults{
				Status: merr.Success(),
				Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1}},
				}},
			},
		}, nil).Build()

		client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxAttempts: 1}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, NewPrimaryReplicaPicker())
		result, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
			Req: &internalpb.RetrieveRequest{CollectionID: 100},
		})
		require.NoError(t, err)
		require.Len(t, result.Results, 1)
		require.Equal(t, shard, result.Results[0].ShardID)
		require.Equal(t, []int64{1}, result.Results[0].Result.GetIds().GetIntId().GetData())
	})
}

func TestLegacySearchExplicitSkip(t *testing.T) {
	mockey.PatchConvey("accepts an explicit Phase 1 skip without dispatching Phase 2", t, func() {
		shard := testShard("v0")
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{shard.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).Return(
			&resolver.ShardReplicas{VChannel: shard.VChannel, PrimaryShardID: shard}, nil).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).Return(&viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
			ShardId:       shard.IntoProto(),
			Version:       &viewpb.QueryViewVersion{QueryVersion: 1},
			Mvcc:          &viewpb.QueryPlanMVCC{},
			SkipExecution: true,
			PlanDelta:     &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{}},
		}}, nil).Build()

		client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxAttempts: 1}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, NewPrimaryReplicaPicker())
		result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.NoError(t, err)
		require.Empty(t, result.Results)
		require.Len(t, result.Plans, 1)
		require.True(t, result.Plans[0].SkipExecution)
		require.Empty(t, result.Plans[0].WorkNodes)
	})
}

func TestLegacyClientInputAndBoundaryErrors(t *testing.T) {
	client := NewLegacyViewQueryClient(ViewQueryClientConfig{}, nil, nil, nil, nil)
	_, err := client.Legacy().Search(context.Background(), nil)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = client.Legacy().Query(context.Background(), &LegacyQueryRequest{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	mockey.PatchConvey("classifies an untyped resolver failure as a system error", t, func() {
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return(nil, errors.New("discovery failed")).Build()
		client := NewLegacyViewQueryClient(ViewQueryClientConfig{}, nil, nil, &mockShardResolver{}, nil)
		_, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Contains(t, err.Error(), "discovery failed")
	})

	mockey.PatchConvey("rejects an empty resolved vchannel set", t, func() {
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{}, nil).Build()
		client := NewLegacyViewQueryClient(ViewQueryClientConfig{}, nil, nil, &mockShardResolver{}, nil)
		_, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	})

	mockey.PatchConvey("preserves context cancellation", t, func() {
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return(nil, context.Canceled).Build()
		client := NewLegacyViewQueryClient(ViewQueryClientConfig{}, nil, nil, &mockShardResolver{}, nil)
		_, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
			Req: &internalpb.RetrieveRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, context.Canceled)
	})

	mockey.PatchConvey("preserves cancellation observed by the shard executor", t, func() {
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{"v0"}, nil).Build()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		client := NewLegacyViewQueryClient(ViewQueryClientConfig{}, nil, nil, &mockShardResolver{}, nil)
		_, err := client.Legacy().Search(ctx, &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, context.Canceled)
	})

	mockey.PatchConvey("classifies a Phase 1 transport failure at the public boundary", t, func() {
		shard := testShard("v0")
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{shard.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).Return(
			&resolver.ShardReplicas{VChannel: shard.VChannel, PrimaryShardID: shard}, nil).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).Return(nil, errors.New("transport failed")).Build()
		client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxAttempts: 1}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, nil)
		_, err := client.Legacy().Query(context.Background(), &LegacyQueryRequest{
			Req: &internalpb.RetrieveRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Contains(t, err.Error(), "transport failed")
	})
}

func TestLegacySearchRetriesAndResetsShardResults(t *testing.T) {
	mockey.PatchConvey("drops partial results from a failed Phase 2 attempt", t, func() {
		shard := testShard("v0")
		var planCalls atomic.Int32
		var resolveCalls atomic.Int32
		firstNodeCompleted := make(chan struct{})

		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{shard.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).To(
			func(*mockShardResolver, context.Context, int64, string) (*resolver.ShardReplicas, error) {
				resolveCalls.Add(1)
				return &resolver.ShardReplicas{VChannel: shard.VChannel, PrimaryShardID: shard}, nil
			}).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).To(
			func(_ *mockQueryPlanClient, _ context.Context, _ qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				attempt := planCalls.Add(1)
				nodes := []*viewpb.QueryPlanWorkNode{queryWorkNode(1)}
				if attempt == 1 {
					nodes = append(nodes, queryWorkNode(2))
				}
				return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{
					ShardId:   shard.IntoProto(),
					Version:   &viewpb.QueryViewVersion{QueryVersion: int64(attempt)},
					Mvcc:      &viewpb.QueryPlanMVCC{TransformingTimetick: 80},
					WorkNodes: nodes,
					PlanDelta: &viewpb.QueryPlan_LegacySearchPlan{LegacySearchPlan: &viewpb.LegacySearchPlan{}},
				}}, nil
			}).Build()
		mockey.Mock((*mockViewQueryServiceClient).SearchOnView).To(
			func(_ *mockViewQueryServiceClient, _ context.Context, node qviews.WorkNode, request *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				if request.GetVersion().GetQueryVersion() == 1 && node.(qviews.QueryNode).ID == 2 {
					<-firstNodeCompleted
					return nil, viewerror.NewViewInvalidated("stale view")
				}
				if request.GetVersion().GetQueryVersion() == 1 {
					close(firstNodeCompleted)
				}
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
					Status: merr.Success(),
					Base:   &commonpb.MsgBase{SourceID: request.GetVersion().GetQueryVersion()},
				}}, nil
			}).Build()

		client := NewLegacyViewQueryClient(ViewQueryClientConfig{
			MaxAttempts:         2,
			RetryInitialBackoff: time.Nanosecond,
			RetryMaxBackoff:     time.Nanosecond,
		}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, NewPrimaryReplicaPicker())
		result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.NoError(t, err)
		require.Equal(t, int32(2), planCalls.Load())
		require.Equal(t, int32(2), resolveCalls.Load())
		require.Len(t, result.Results, 1)
		require.Equal(t, shard, result.Results[0].ShardID)
		require.Equal(t, int64(2), result.Results[0].Result.GetBase().GetSourceID())
	})
}

func TestLegacySearchRetryExhaustion(t *testing.T) {
	mockey.PatchConvey("returns retriable typed merr after the configured attempts", t, func() {
		shard := testShard("v0")
		var calls atomic.Int32
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{shard.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).Return(
			&resolver.ShardReplicas{VChannel: shard.VChannel, PrimaryShardID: shard}, nil).Build()
		mockey.Mock((*mockQueryPlanClient).GetQueryPlan).To(
			func(*mockQueryPlanClient, context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				calls.Add(1)
				return nil, viewerror.NewNotPrimaryError("primary changed")
			}).Build()

		client := NewLegacyViewQueryClient(ViewQueryClientConfig{
			MaxAttempts:         2,
			RetryInitialBackoff: time.Nanosecond,
			RetryMaxBackoff:     time.Nanosecond,
		}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, NewPrimaryReplicaPicker())
		_, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.True(t, merr.IsRetryableErr(err))
		require.Equal(t, int32(2), calls.Load())
	})
}

func TestLegacySearchRejectsNonPrimaryPickerResult(t *testing.T) {
	mockey.PatchConvey("enforces Primary-only independently of picker implementation", t, func() {
		primary := testShard("v0")
		secondary := qviews.ShardID{ReplicaID: 2, VChannel: primary.VChannel}
		mockey.Mock((*mockShardResolver).ResolveVChannels).Return([]string{primary.VChannel}, nil).Build()
		mockey.Mock((*mockShardResolver).ResolveShard).Return(
			&resolver.ShardReplicas{VChannel: primary.VChannel, PrimaryShardID: primary}, nil).Build()
		mockey.Mock((*mockReplicaPicker).Pick).Return(secondary, nil).Build()

		client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxAttempts: 1}, &mockQueryPlanClient{},
			&mockViewQueryServiceClient{}, &mockShardResolver{}, &mockReplicaPicker{})
		_, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{
			Req: &internalpb.SearchRequest{CollectionID: 100},
		})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Contains(t, err.Error(), "rejected non-primary shard")
	})
}
