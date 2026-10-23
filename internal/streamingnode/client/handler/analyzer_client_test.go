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

func TestAnalyzerAssignmentRetryPreservesExactReadSchema(t *testing.T) {
	for _, version := range []int32{0, 7} {
		mockey.PatchConvey("read schema presence and version survive owner replacement", t, func() {
			term := int64(1)
			mockey.Mock((*analyzerWatcher).Get).To(func(*analyzerWatcher, context.Context, string) *types.PChannelInfoAssigned {
				return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: term, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: 10 + term}}
			}).Build()
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			mockey.Mock((*analyzerTrigger).ReportAssignmentError).Return(nil).Build()
			mockey.Mock((*analyzerWatcher).Watch).To(func(*analyzerWatcher, context.Context, string, *types.PChannelInfoAssigned) error {
				term++
				return nil
			}).Build()
			calls := 0
			mockey.Mock((*analyzerRPC).RunAnalyzer).To(func(_ *analyzerRPC, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				require.NotNil(t, req.GetFieldAnalyzer().ReadSchemaVersion)
				require.Equal(t, version, req.GetFieldAnalyzer().GetReadSchemaVersion())
				require.Equal(t, term, req.GetFieldAnalyzer().GetPchannel().GetTerm())
				if calls == 1 {
					return nil, status.NewChannelNotExist("owner replaced")
				}
				return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success()}, nil
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, service: &analyzerService{}, rebalanceTrigger: &analyzerTrigger{}}
			req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{Vchannel: "p_1v0", ReadSchemaVersion: &version}}}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := client.RunAnalyzer(ctx, req)
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.Nil(t, req.GetFieldAnalyzer().GetPchannel())
			require.Equal(t, version, req.GetFieldAnalyzer().GetReadSchemaVersion())
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

func (*analyzerRPC) ValidateAnalyzer(context.Context, *streamingpb.StreamingNodeValidateAnalyzerRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
	panic("unpatched")
}

func TestValidateAnalyzerDiscovery(t *testing.T) {
	for _, scenario := range []string{"retry", "invalid", "terminal system error", "unimplemented", "cancel"} {
		mockey.PatchConvey(scenario, t, func() {
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			mockey.Mock((*analyzerRPC).ValidateAnalyzer).To(func(_ *analyzerRPC, ctx context.Context, _ *streamingpb.StreamingNodeValidateAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
				_, targeted := contextutil.GetPickServerID(ctx)
				require.False(t, targeted)
				calls++
				switch scenario {
				case "retry":
					if calls == 1 {
						return nil, status.NewInner("inventory not ready")
					}
				case "invalid":
					return nil, status.NewInvalidArgument("invalid tokenizer")
				case "terminal system error":
					return nil, status.NewUnrecoverableError("native internal failure")
				case "unimplemented":
					return nil, grpcstatus.Error(codes.Unimplemented, "older SN")
				case "cancel":
					cancel()
					return nil, status.NewInner("not ready")
				}
				return &streamingpb.StreamingNodeValidateAnalyzerResponse{ResourceIds: []int64{42}}, nil
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}, watcher: &analyzerWatcher{}}
			resp, err := client.ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{})
			switch scenario {
			case "retry":
				require.NoError(t, err)
				require.Equal(t, []int64{42}, resp.ResourceIds)
				require.Equal(t, 2, calls)
			case "invalid":
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Equal(t, 1, calls)
			case "terminal system error":
				require.True(t, status.AsStreamingError(err).IsUnrecoverable())
				require.False(t, status.AsStreamingError(err).IsInvalidArgument())
				require.Equal(t, 1, calls)
			case "unimplemented":
				require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))
				require.Equal(t, 1, calls)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			}
			client.lifetime.SetState(typeutil.LifetimeStateStopped)
			_, err = client.ValidateAnalyzer(context.Background(), &streamingpb.StreamingNodeValidateAnalyzerRequest{})
			require.True(t, status.AsStreamingError(err).IsOnShutdown())
		})
	}
}

func TestValidateAnalyzerBusinessFailuresDoNotRetry(t *testing.T) {
	for _, failure := range []error{
		merr.WrapErrParameterInvalidMsg("file resource never_added_resource not found"),
		merr.SegcoreError(int32(merr.CodePathNotExist), "dictionary file not found"),
		merr.SegcoreError(int32(merr.CodeFileReadFailed), "dictionary file access denied"),
		merr.ErrServiceUnavailable,
		merr.ErrServiceInternal,
		merr.SegcoreError(int32(merr.CodeInvalidParameter), "invalid analyzer"),
	} {
		mockey.PatchConvey(failure.Error(), t, func() {
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			call := mockey.Mock((*analyzerRPC).ValidateAnalyzer).Return(&streamingpb.StreamingNodeValidateAnalyzerResponse{Status: merr.Status(failure)}, nil).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			resp, err := client.ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{})
			require.Nil(t, resp)
			require.Error(t, err)
			require.Equal(t, 1, call.Times(), "even retriable business failures must not enter readiness backoff")
			if merr.Code(failure) == merr.Code(merr.ErrParameterInvalid) {
				require.EqualError(t, err, failure.Error(), "parameter errors must pass through without extra context")
			}
			require.Equal(t, merr.GetErrorType(failure), merr.GetErrorType(err))
			require.Equal(t, merr.IsRetryableErr(failure), merr.IsRetryableErr(err))
			if merr.GetErrorType(failure) == merr.InputError {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			} else {
				require.Equal(t, merr.Code(failure), merr.Code(err))
			}
			client.lifetime.SetState(typeutil.LifetimeStateStopped)
			client.lifetime.Wait()
		})
	}
}

func TestAnalyzerRequestsRespectCallerDeadline(t *testing.T) {
	for _, validate := range []bool{false, true} {
		mockey.PatchConvey("persistent preparation failure respects the caller deadline", t, func() {
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			calls := 0
			mockey.Mock((*analyzerRPC).RunAnalyzer).To(func(_ *analyzerRPC, _ context.Context, _ *streamingpb.StreamingNodeRunAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				return nil, status.NewUnknownError("resource download failed")
			}).Build()
			mockey.Mock((*analyzerRPC).ValidateAnalyzer).To(func(_ *analyzerRPC, _ context.Context, _ *streamingpb.StreamingNodeValidateAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
				calls++
				return nil, status.NewUnknownError("resource download failed")
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}}
			// Persistent readiness errors must stop at the caller deadline.
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if validate {
					_, err = client.ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{})
				} else {
					_, err = client.RunAnalyzer(ctx, &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{}}})
				}
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(5 * time.Second):
				cancel()
				<-done
				t.Fatal("analyzer request did not respect the caller deadline")
			}
			require.Positive(t, calls)
			client.lifetime.SetState(typeutil.LifetimeStateStopped)
			client.lifetime.Wait()
		})
	}
}

func TestAnalyzerRequestsPreserveCallerContext(t *testing.T) {
	for _, withDeadline := range []bool{false, true} {
		mockey.PatchConvey("analyzer requests preserve the caller context", t, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if withDeadline {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 30*time.Minute)
				defer deadlineCancel()
			}
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			mockey.Mock((*analyzerRPC).RunAnalyzer).To(func(_ *analyzerRPC, got context.Context, _ *streamingpb.StreamingNodeRunAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				require.Same(t, ctx, got)
				return &streamingpb.StreamingNodeRunAnalyzerResponse{}, nil
			}).Build()
			mockey.Mock((*analyzerRPC).ValidateAnalyzer).To(func(_ *analyzerRPC, got context.Context, _ *streamingpb.StreamingNodeValidateAnalyzerRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeValidateAnalyzerResponse, error) {
				require.Same(t, ctx, got)
				return &streamingpb.StreamingNodeValidateAnalyzerResponse{}, nil
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}}
			_, err := client.RunAnalyzer(ctx, &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{}}})
			require.NoError(t, err)
			_, err = client.ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{})
			require.NoError(t, err)
		})
	}
}
