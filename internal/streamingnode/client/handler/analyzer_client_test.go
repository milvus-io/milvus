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

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type analyzerWatcher struct{ assignment.Watcher }

func (*analyzerWatcher) Get(context.Context, string) *types.PChannelInfoAssigned { panic("unpatched") }
func (*analyzerWatcher) Watch(context.Context, string, *types.PChannelInfoAssigned) error {
	panic("unpatched")
}

type analyzerService struct {
	lazygrpc.Service[streamingpb.StreamingNodeHandlerServiceClient]
}

func (*analyzerService) GetService(context.Context) (streamingpb.StreamingNodeHandlerServiceClient, error) {
	panic("unpatched")
}

type analyzerRPC struct {
	streamingpb.StreamingNodeHandlerServiceClient
}

func (*analyzerRPC) RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	panic("unpatched")
}

type analyzerTrigger struct {
	types.AssignmentRebalanceTrigger
}

func (*analyzerTrigger) ReportAssignmentError(context.Context, types.PChannelInfo, error) error {
	panic("unpatched")
}

func TestAnalyzerAssignmentRetry(t *testing.T) {
	for _, scenario := range []string{"migration", "invalid", "analyzer unavailable", "unavailable", "unimplemented"} {
		mockey.PatchConvey(scenario, t, func() {
			term := int64(1)
			mockey.Mock((*analyzerWatcher).Get).To(func(*analyzerWatcher, context.Context, string) *types.PChannelInfoAssigned {
				return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: term, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: 10 + term}}
			}).Build()
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			reports := mockey.Mock((*analyzerTrigger).ReportAssignmentError).To(func(*analyzerTrigger, context.Context, types.PChannelInfo, error) error { return nil }).Build()
			mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, ctx context.Context, _ string, previous *types.PChannelInfoAssigned) error {
				if scenario == "migration" {
					require.Equal(t, int64(1), previous.Channel.Term)
					// Discovery catches up after the failed attempt has been reported.
					term++
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}).Build()
			calls := 0
			mockey.Mock((*analyzerRPC).RunAnalyzer).To(func(_ *analyzerRPC, ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				node, ok := contextutil.GetPickServerID(ctx)
				require.True(t, ok)
				require.Equal(t, 10+term, node)
				require.Equal(t, term, req.GetFieldAnalyzer().GetPchannel().GetTerm())
				switch scenario {
				case "migration":
					if calls == 1 {
						return nil, status.NewChannelNotExist("moved")
					}
				case "invalid":
					return nil, status.NewInvalidArgument("bad names")
				case "analyzer unavailable":
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.ErrServiceUnavailable)}, nil
				case "unavailable":
					if calls == 1 {
						return nil, status.NewInner("not ready")
					}
				case "unimplemented":
					return nil, status.ConvertStreamingError("RunAnalyzer", grpcstatus.Error(codes.Unimplemented, "older SN"))
				}
				return &streamingpb.StreamingNodeRunAnalyzerResponse{}, nil
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, service: &analyzerService{}, rebalanceTrigger: &analyzerTrigger{}}
			req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{Vchannel: "p_1v0"}}}
			resp, err := client.RunAnalyzer(context.Background(), req)
			require.Nil(t, req.GetFieldAnalyzer().GetPchannel(), "retry must not mutate caller request")
			switch scenario {
			case "migration":
				require.NoError(t, err)
				require.Equal(t, 2, calls)
				require.Equal(t, 1, reports.Times())
			case "analyzer unavailable":
				require.NoError(t, err)
				require.Error(t, merr.Error(resp.GetStatus()))
				require.Equal(t, 1, calls)
				require.Zero(t, reports.Times())
			case "unavailable":
				require.NoError(t, err)
				require.Equal(t, 2, calls)
				require.Zero(t, reports.Times())
			default:
				require.Error(t, err)
				require.Zero(t, reports.Times())
				require.Equal(t, 1, calls)
				if scenario == "unimplemented" {
					require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))
				}
			}
		})
	}
}

func TestAnalyzerInlineUsesExistingPicker(t *testing.T) {
	mockey.PatchConvey("inline analyzer needs no channel assignment", t, func() {
		mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
		calls := 0
		mockey.Mock((*analyzerRPC).RunAnalyzer).To(func(_ *analyzerRPC, ctx context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
			_, targeted := contextutil.GetPickServerID(ctx)
			require.False(t, targeted, "the existing picker must choose a ready SN")
			require.Equal(t, "{}", req.GetInlineAnalyzer().GetAnalyzerParams())
			calls++
			if calls == 1 {
				return nil, status.NewOnShutdownError("closing")
			}
			return &streamingpb.StreamingNodeRunAnalyzerResponse{}, nil
		}).Build()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}}
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{AnalyzerParams: "{}"}}}
		_, err := owner.RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = owner.RunAnalyzer(context.Background(), req)
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
		require.Equal(t, 2, calls)
	})
}

func TestAnalyzerDiscoveryAdmission(t *testing.T) {
	for _, scenario := range []string{"missing source", "missing channel", "service unavailable", "assignment unavailable"} {
		mockey.PatchConvey(scenario, t, func() {
			service := mockey.Mock((*analyzerService).GetService).To(func(*analyzerService, context.Context) (streamingpb.StreamingNodeHandlerServiceClient, error) {
				if scenario == "service unavailable" {
					return nil, status.NewInner("discovery not ready")
				}
				return &analyzerRPC{}, nil
			}).Build()
			watcher := mockey.Mock((*analyzerWatcher).Get).Return(nil).Build()
			mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, ctx context.Context, _ string, _ *types.PChannelInfoAssigned) error {
				<-ctx.Done()
				return ctx.Err()
			}).Build()
			owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}, watcher: &analyzerWatcher{}}
			req := &streamingpb.StreamingNodeRunAnalyzerRequest{}
			if scenario == "service unavailable" {
				req.Source = &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{}}
			} else if scenario != "missing source" {
				field := &streamingpb.StreamingFieldAnalyzer{}
				if scenario != "missing channel" {
					field.Vchannel = "p_1v0"
				}
				req.Source = &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: field}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			_, err := owner.RunAnalyzer(ctx, req)
			require.Error(t, err)
			if scenario == "missing source" || scenario == "missing channel" {
				require.True(t, status.AsStreamingError(err).IsInvalidArgument())
				require.Zero(t, service.Times())
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if scenario == "assignment unavailable" {
				require.Positive(t, watcher.Times())
				require.Zero(t, service.Times())
			} else {
				require.Zero(t, watcher.Times())
			}
		})
	}
}

func TestAnalyzerInlineUnimplementedAndCancellation(t *testing.T) {
	for _, scenario := range []string{"unimplemented", "cancel"} {
		mockey.PatchConvey(scenario, t, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			call := mockey.Mock((*analyzerRPC).RunAnalyzer).To(func(_ *analyzerRPC, _ context.Context, _ *streamingpb.StreamingNodeRunAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				if scenario == "unimplemented" {
					return nil, status.ConvertStreamingError("RunAnalyzer", grpcstatus.Error(codes.Unimplemented, "older SN"))
				}
				cancel()
				return nil, status.NewOnShutdownError("closing")
			}).Build()
			owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}}
			req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{}}}
			_, err := owner.RunAnalyzer(ctx, req)
			if scenario == "unimplemented" {
				require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Equal(t, 1, call.Times())
			owner.lifetime.SetState(typeutil.LifetimeStateStopped)
			owner.lifetime.Wait()
		})
	}
}

type analyzerConfigResolver struct{ resolver.Builder }

func (*analyzerConfigResolver) Scheme() string { panic("unpatched") }

func TestAnalyzerUsesSharedGRPCRetryPolicy(t *testing.T) {
	mockey.PatchConvey("analyzer uses the handler service retry policy", t, func() {
		paramtable.Init()
		mockey.Mock((*analyzerConfigResolver).Scheme).Return("analyzer-config-test").Build()
		conn, err := grpc.NewClient("passthrough:///", getDialOptions(&analyzerConfigResolver{})...)
		require.NoError(t, err)
		defer conn.Close()
		conn.Connect()
		// Wait until gRPC applies the shared service config.
		require.Eventually(t, func() bool {
			return conn.GetMethodConfig(streamingpb.StreamingNodeHandlerService_GetReplicateCheckpoint_FullMethodName).RetryPolicy != nil
		}, time.Second, time.Millisecond)
		require.Equal(t, conn.GetMethodConfig(streamingpb.StreamingNodeHandlerService_GetReplicateCheckpoint_FullMethodName).RetryPolicy,
			conn.GetMethodConfig(streamingpb.StreamingNodeHandlerService_RunAnalyzer_FullMethodName).RetryPolicy)
	})
}
