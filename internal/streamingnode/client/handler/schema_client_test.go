package handler

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type schemaRPC struct {
	streamingpb.StreamingNodeHandlerServiceClient
}

func (*schemaRPC) GetCollectionSchema(context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest, ...grpc.CallOption) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	panic("unpatched")
}

func TestSchemaAssignmentRetry(t *testing.T) {
	for _, scenario := range []string{"migration", "future version", "retired version"} {
		mockey.PatchConvey(scenario, t, func() {
			term := int64(1)
			mockey.Mock((*analyzerWatcher).Get).To(func(*analyzerWatcher, context.Context, string) *types.PChannelInfoAssigned {
				return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: term, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: 10 + term}}
			}).Build()
			mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, ctx context.Context, _ string, _ *types.PChannelInfoAssigned) error {
				if scenario == "migration" {
					term++
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}).Build()
			reports := mockey.Mock((*analyzerTrigger).ReportAssignmentError).Return(nil).Build()
			mockey.Mock((*analyzerService).GetService).Return(&schemaRPC{}, nil).Build()
			calls := 0
			mockey.Mock((*schemaRPC).GetCollectionSchema).To(func(_ *schemaRPC, ctx context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
				calls++
				node, ok := contextutil.GetPickServerID(ctx)
				require.True(t, ok)
				require.Equal(t, 10+term, node)
				require.Equal(t, term, req.GetPchannel().GetTerm())
				require.IsType(t, &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{}, req.GetSelector())
				require.Zero(t, req.GetSchemaVersion())
				if calls == 1 {
					switch scenario {
					case "migration":
						return nil, status.NewChannelNotExist("moved")
					case "future version":
						return nil, status.NewInner("not observed yet")
					case "retired version":
						return nil, status.NewUnrecoverableError("retired schema")
					}
				}
				return &streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: &schemapb.CollectionSchema{}, EffectiveTimeTick: 10}, nil
			}).Build()
			client := &handlerClientImpl{lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, service: &analyzerService{}, rebalanceTrigger: &analyzerTrigger{}}
			req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: "p_1v0", Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := client.GetCollectionSchema(ctx, req)
			require.Nil(t, req.Pchannel, "assignment retries must not mutate caller request")
			if scenario == "retired version" {
				require.Error(t, err)
				require.Nil(t, resp)
				require.Equal(t, 1, calls)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 10, resp.EffectiveTimeTick)
				require.Equal(t, 2, calls)
			}
			if scenario == "migration" {
				require.Equal(t, 1, reports.Times())
			} else {
				require.Zero(t, reports.Times())
			}
		})
	}
}

func TestSchemaClientAdmission(t *testing.T) {
	client := &handlerClientImpl{lifetime: typeutil.NewLifetime()}
	_, err := client.GetCollectionSchema(context.Background(), &streamingpb.StreamingNodeGetCollectionSchemaRequest{})
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	client.lifetime.SetState(typeutil.LifetimeStateStopped)
	_, err = client.GetCollectionSchema(context.Background(), &streamingpb.StreamingNodeGetCollectionSchemaRequest{})
	require.True(t, status.AsStreamingError(err).IsOnShutdown())
}

func TestSchemaClientRejectsInvalidSelectorsAndCancellation(t *testing.T) {
	client := &handlerClientImpl{lifetime: typeutil.NewLifetime()}
	// Invalid identities must fail before discovery or its retry loop.
	for _, vchannel := range []string{"", "p", "p_bad", "p_0v0", "p_-1v0", "p_01v0", "p_1v", "p_1v0_extra", "p_9223372036854775808v0"} {
		_, err := client.GetCollectionSchema(context.Background(), &streamingpb.StreamingNodeGetCollectionSchemaRequest{
			Vchannel: vchannel,
			Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true},
		})
		require.True(t, status.AsStreamingError(err).IsInvalidArgument(), "%s: %v", vchannel, err)
	}
	req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: "p_1v0"}
	_, err := client.GetCollectionSchema(context.Background(), req)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{}
	_, err = client.GetCollectionSchema(context.Background(), req)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: -1}
	_, err = client.GetCollectionSchema(context.Background(), req)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.GetCollectionSchema(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
}
