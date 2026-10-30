package handler

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryViewRPCWaitForReady(t *testing.T) {
	for _, method := range []string{"plan", "mvcc", "search", "query"} {
		mockey.PatchConvey(method, t, func() {
			term := int64(1)
			calls, waits := 0, 0
			mockey.Mock((*analyzerWatcher).Get).To(func(*analyzerWatcher, context.Context, string) *types.PChannelInfoAssigned {
				return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p0", Term: term, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: 100 + term}}
			}).Build()
			mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, _ context.Context, name string, previous *types.PChannelInfoAssigned) error {
				require.Equal(t, "p0", name)
				require.Equal(t, int64(1), previous.Channel.Term)
				waits++
				// Discovery can lag longer than the orchestrator's three attempts.
				if waits == 4 {
					term = 2
				}
				return nil
			}).Build()
			attempt := func(ctx context.Context) error {
				calls++
				node, ok := contextutil.GetPickServerID(ctx)
				require.True(t, ok)
				require.Equal(t, 100+term, node)
				channel, err := decodeOutgoingQueryViewPChannel(ctx)
				require.NoError(t, err)
				require.Equal(t, term, channel.Term)
				if term == 1 {
					if calls == 2 {
						return grpcstatus.Error(codes.Unavailable, "connection lost")
					}
					if calls == 3 {
						return viewerror.NewGRPCStatusFromViewError(viewerror.NewNotPrimaryError("moved")).Err()
					}
					return viewerror.NewGRPCStatusFromViewError(viewerror.NewOnShutdownError("wal is on shutdown")).Err()
				}
				return nil
			}
			shard := qviews.ShardID{ReplicaID: 1, VChannel: "p0_100v0"}
			plan := &fakeQueryPlanServiceClient{}
			query := &fakeViewQueryServiceClient{}
			client := newTestHandlerClient(plan, query, nil)
			client.watcher = &analyzerWatcher{}
			switch method {
			case "plan":
				req := &viewpb.GetQueryPlanRequest{ShardId: shard.IntoProto()}
				before := proto.Clone(req)
				mockey.Mock((*fakeQueryPlanServiceClient).GetQueryPlan).To(func(_ *fakeQueryPlanServiceClient, ctx context.Context, actual *viewpb.GetQueryPlanRequest, _ ...grpc.CallOption) (*viewpb.GetQueryPlanResponse, error) {
					require.True(t, proto.Equal(before, actual))
					return &viewpb.GetQueryPlanResponse{}, attempt(ctx)
				}).Build()
				_, err := client.QueryViewClient().GetQueryPlan(t.Context(), shard, req)
				require.NoError(t, err)
			case "mvcc":
				mockey.Mock((*fakeQueryPlanServiceClient).GetMVCCTimestamp).To(func(_ *fakeQueryPlanServiceClient, ctx context.Context, _ *viewpb.GetMVCCTimestampRequest, _ ...grpc.CallOption) (*viewpb.GetMVCCTimestampResponse, error) {
					return &viewpb.GetMVCCTimestampResponse{}, attempt(ctx)
				}).Build()
				_, err := client.QueryViewClient().GetMVCCTimestamp(t.Context(), shard, &viewpb.GetMVCCTimestampRequest{Vchannel: shard.VChannel})
				require.NoError(t, err)
			case "search":
				req := &viewpb.SearchOnViewRequest{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{QueryVersion: 7}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 200}}
				before := proto.Clone(req)
				mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, ctx context.Context, actual *viewpb.SearchOnViewRequest, _ ...grpc.CallOption) (*viewpb.SearchOnViewResponse, error) {
					require.Same(t, req, actual)
					require.True(t, proto.Equal(before, actual))
					return &viewpb.SearchOnViewResponse{}, attempt(ctx)
				}).Build()
				_, err := client.QueryViewClient().SearchOnView(t.Context(), types.PChannelInfo{Name: "p0", Term: 1}, req)
				require.NoError(t, err)
			case "query":
				req := &viewpb.QueryOnViewRequest{ShardId: shard.IntoProto(), Version: &viewpb.QueryViewVersion{QueryVersion: 7}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 200}}
				before := proto.Clone(req)
				mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).To(func(_ *fakeViewQueryServiceClient, ctx context.Context, actual *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
					require.Same(t, req, actual)
					require.True(t, proto.Equal(before, actual))
					return &viewpb.QueryOnViewResponse{}, attempt(ctx)
				}).Build()
				_, err := client.QueryViewClient().QueryOnView(t.Context(), types.PChannelInfo{Name: "p0", Term: 1}, req)
				require.NoError(t, err)
			}
			require.Equal(t, 5, calls)
			require.Equal(t, 4, waits)
		})
	}
}

func TestQueryViewRPCDoesNotWaitForViewOrRequestErrors(t *testing.T) {
	for _, failure := range []error{
		viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("retired")).Err(),
		viewerror.NewGRPCStatusFromViewError(viewerror.NewViewNotFound("missing")).Err(),
		viewerror.NewGRPCStatusFromError(merr.WrapErrParameterInvalidMsg("bad expression")).Err(),
		grpcstatus.Error(codes.Unimplemented, "older node"),
	} {
		mockey.PatchConvey(failure.Error(), t, func() {
			call := mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).Return(nil, failure).Build()
			wait := mockey.Mock(fakeAssignmentWatcher.Watch).Return(context.Canceled).Build()
			client := newTestHandlerClient(nil, &fakeViewQueryServiceClient{}, nil)
			_, err := client.QueryViewClient().QueryOnView(t.Context(), types.PChannelInfo{Name: "p0"}, &viewpb.QueryOnViewRequest{})
			require.Equal(t, grpcstatus.Code(failure), grpcstatus.Code(err))
			require.Equal(t, viewerror.AsViewError(viewerror.ConvertViewError("test", failure)).Code, viewerror.AsViewError(err).Code)
			require.Equal(t, 1, call.Times())
			require.Zero(t, wait.Times())
		})
	}
}

func TestQueryViewRPCWaitForReadyCancellation(t *testing.T) {
	for _, scenario := range []string{"deadline", "cancel", "already canceled", "closed client"} {
		mockey.PatchConvey(scenario, t, func() {
			client := newTestHandlerClient(&fakeQueryPlanServiceClient{}, nil, nil)
			client.watcher = &analyzerWatcher{}
			mockey.Mock((*analyzerWatcher).Get).Return(&types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p0", Term: 1}}).Build()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, ctx context.Context, _ string, _ *types.PChannelInfoAssigned) error {
				if scenario == "cancel" {
					cancel()
				}
				<-ctx.Done()
				return ctx.Err()
			}).Build()
			call := mockey.Mock((*fakeQueryPlanServiceClient).GetQueryPlan).Return(nil, viewerror.NewGRPCStatusFromViewError(viewerror.NewOnShutdownError("closed")).Err()).Build()
			if scenario == "already canceled" {
				cancel()
			}
			if scenario == "closed client" {
				client.lifetime.SetState(typeutil.LifetimeStateStopped)
			}
			_, err := client.QueryViewClient().GetQueryPlan(ctx, qviews.ShardID{VChannel: "p0_100v0"}, &viewpb.GetQueryPlanRequest{})
			switch scenario {
			case "closed client":
				require.ErrorIs(t, err, ErrClientClosed)
				require.Zero(t, call.Times())
			case "already canceled":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, call.Times())
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, call.Times())
			default:
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, 1, call.Times())
			}
			client.lifetime.SetState(typeutil.LifetimeStateStopped)
			client.lifetime.Wait()
		})
	}
}

func TestQueryViewRPCUsesWALWaitForReady(t *testing.T) {
	mockey.PatchConvey("Phase 1 and SN Phase 2 share WAL transport readiness", t, func() {
		paramtable.Init()
		mockey.Mock((*analyzerConfigResolver).Scheme).Return("queryview-ready-test").Build()
		conn, err := grpc.NewClient("passthrough:///", getDialOptions(&analyzerConfigResolver{})...)
		require.NoError(t, err)
		defer conn.Close()
		conn.Connect()
		require.Eventually(t, func() bool {
			return conn.GetMethodConfig(streamingpb.StreamingNodeHandlerService_GetReplicateCheckpoint_FullMethodName).RetryPolicy != nil
		}, time.Second, time.Millisecond)
		walConfig := conn.GetMethodConfig(streamingpb.StreamingNodeHandlerService_GetReplicateCheckpoint_FullMethodName)
		for _, method := range []string{viewpb.QueryPlanService_GetQueryPlan_FullMethodName, viewpb.QueryPlanService_GetMVCCTimestamp_FullMethodName, viewpb.ViewQueryService_SearchOnView_FullMethodName, viewpb.ViewQueryService_QueryOnView_FullMethodName} {
			cfg := conn.GetMethodConfig(method)
			require.NotNil(t, cfg.WaitForReady, method)
			require.True(t, *cfg.WaitForReady, method)
			require.Equal(t, walConfig.RetryPolicy, cfg.RetryPolicy, method)
		}
	})
}
