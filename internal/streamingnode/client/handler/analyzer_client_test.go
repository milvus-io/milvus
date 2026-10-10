package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type analyzerWatcher struct{ assignment.Watcher }

func (*analyzerWatcher) Get(context.Context, string) *types.PChannelInfoAssigned { panic("unpatched") }

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
	for _, scenario := range []string{"migration", "invalid", "schema mismatch", "unavailable"} {
		mockey.PatchConvey(scenario, t, func() {
			term := int64(1)
			mockey.Mock((*analyzerWatcher).Get).To(func(*analyzerWatcher, context.Context, string) *types.PChannelInfoAssigned {
				return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: term, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: 10 + term}}
			}).Build()
			mockey.Mock((*analyzerService).GetService).Return(&analyzerRPC{}, nil).Build()
			reports := mockey.Mock((*analyzerTrigger).ReportAssignmentError).To(func(*analyzerTrigger, context.Context, types.PChannelInfo, error) error { term++; return nil }).Build()
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
				case "schema mismatch":
					return nil, status.NewSchemaVersionMismatch("changed")
				case "unavailable":
					return nil, status.NewInner("not ready")
				}
				return &streamingpb.StreamingNodeRunAnalyzerResponse{}, nil
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, service: &analyzerService{}, rebalanceTrigger: &analyzerTrigger{}}
			req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{Vchannel: "p_1v0"}}}
			client.analyzerClient = newAnalyzerClient(client)
			_, err := client.AnalyzerClient().RunAnalyzer(context.Background(), req)
			require.Nil(t, req.GetFieldAnalyzer().GetPchannel(), "retry must not mutate caller request")
			if scenario == "migration" {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
				require.Equal(t, 1, reports.Times())
			} else {
				require.Error(t, err)
				require.Zero(t, reports.Times())
				if scenario == "unavailable" {
					require.Equal(t, 3, calls)
				} else {
					require.Equal(t, 1, calls)
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
		owner.analyzerClient = newAnalyzerClient(owner)
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{AnalyzerParams: "{}"}}}
		_, err := owner.AnalyzerClient().RunAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = owner.AnalyzerClient().RunAnalyzer(context.Background(), req)
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
			owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: &analyzerService{}, watcher: &analyzerWatcher{}}
			req := &streamingpb.StreamingNodeRunAnalyzerRequest{}
			if scenario != "missing source" {
				field := &streamingpb.StreamingFieldAnalyzer{}
				if scenario != "missing channel" {
					field.Vchannel = "p_1v0"
				}
				req.Source = &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: field}
			}
			_, err := newAnalyzerClient(owner).RunAnalyzer(context.Background(), req)
			require.Error(t, err)
			if scenario == "missing source" || scenario == "missing channel" {
				require.True(t, status.AsStreamingError(err).IsInvalidArgument())
				require.Zero(t, service.Times())
			} else {
				require.Equal(t, 3, service.Times(), "discovery failures must exhaust a bounded budget")
			}
			if scenario == "assignment unavailable" {
				require.Equal(t, 3, watcher.Times())
			} else {
				require.Zero(t, watcher.Times())
			}
		})
	}
}
