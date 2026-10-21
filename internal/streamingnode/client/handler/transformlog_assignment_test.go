package handler

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type assignmentTestHandler struct{ wal.TransformLogEventHandler }

func (*assignmentTestHandler) Handle(wal.TransformLogStreamEvent) { panic("unpatched") }
func (*assignmentTestHandler) Close()                             { panic("unpatched") }

func TestTransformLogReportsRejectedOwnerBeforeRetry(t *testing.T) {
	for _, failure := range []*status.StreamingError{
		status.NewChannelNotExist("p"),
		status.NewUnmatchedChannelTerm("p", 1, 2),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			mockey.PatchConvey("real RPC owner validation and assignment retry", t, func() {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var term, reports, checks atomic.Int64
				term.Store(1)
				mockey.Mock((*analyzerWatcher).Get).To(func(*analyzerWatcher, context.Context, string) *types.PChannelInfoAssigned {
					return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: term.Load()}, Node: types.StreamingNodeInfo{ServerID: 10}}
				}).Build()
				mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, _ context.Context, _ string, previous *types.PChannelInfoAssigned) error {
					require.EqualValues(t, 1, previous.Channel.Term)
					require.EqualValues(t, 2, term.Load())
					return nil
				}).Build()
				mockey.Mock((*analyzerTrigger).ReportAssignmentError).To(func(_ *analyzerTrigger, reportCtx context.Context, channel types.PChannelInfo, err error) error {
					require.NoError(t, reportCtx.Err(), "failed RPC cancellation must not cancel the report")
					require.Equal(t, types.PChannelInfo{Name: "p", Term: 1}, channel)
					require.Equal(t, failure.Code, status.AsStreamingError(err).Code)
					reports.Add(1)
					term.Store(2) // Model discovery publishing the corrected assignment after reporting.
					return nil
				}).Build()
				mockey.Mock(registry.GetLocalAvailableWAL).Return(nil, registry.ErrNoStreamingNodeDeployed).Build()
				// Inject the production WAL-owner rejection at the server RPC
				// boundary; use the real wire conversion, client and retry helper.
				mockey.Mock((*streamingpb.UnimplementedStreamingNodeHandlerServiceServer).SubscribeTransform).To(func(_ *streamingpb.UnimplementedStreamingNodeHandlerServiceServer, rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
					request, err := contextutil.GetCreateTransformStream(rpc.Context())
					if err != nil {
						return err
					}
					checks.Add(1)
					if request.GetPchannel().GetTerm() == 1 {
						return failure
					}
					require.EqualValues(t, 2, request.GetPchannel().GetTerm())
					if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CreateStream{CreateStream: &streamingpb.CreateTransformStreamResponse{}}}); err != nil {
						return err
					}
					for {
						req, err := rpc.Recv()
						if err != nil {
							return err
						}
						if create := req.GetCreate(); create != nil {
							if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_Create{Create: &streamingpb.CreateTransformSubscriptionResponse{SubscriptionId: create.GetSubscriptionId()}}}); err != nil {
								return err
							}
							if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_SyncUp{SyncUp: &streamingpb.TransformSubscriptionSyncUp{SubscriptionId: create.GetSubscriptionId(), TimeTick: 20}}}); err != nil {
								return err
							}
						} else if close := req.GetCloseSubscription(); close != nil {
							if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseSubscription{CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{SubscriptionId: close.GetSubscriptionId()}}}); err != nil {
								return err
							}
						} else if req.GetCloseStream() != nil {
							return rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseStream{CloseStream: &streamingpb.CloseTransformStreamResponse{}}})
						}
					}
				}).Build()
				listener := bufconn.Listen(64 * 1024)
				server := grpc.NewServer(grpc.StreamInterceptor(interceptor.NewStreamingServiceStreamServerInterceptor()))
				streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
				go func() { _ = server.Serve(listener) }()
				defer listener.Close()
				defer server.Stop()
				conn, err := grpc.NewClient("passthrough:///transform-assignment",
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithStreamInterceptor(interceptor.NewStreamingServiceStreamClientInterceptor()),
					grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
				require.NoError(t, err)
				defer conn.Close()
				mockey.Mock((*analyzerService).GetService).Return(streamingpb.NewStreamingNodeHandlerServiceClient(conn), nil).Build()
				client := &handlerClientImpl{
					lifetime: typeutil.NewLifetime(), watcher: &analyzerWatcher{}, service: &analyzerService{},
					rebalanceTrigger: &analyzerTrigger{}, newTransformLogEventStream: remote.CreateEventStream,
				}
				stream, err := client.AcquireTransformLogStream(ctx, "p")
				require.NoError(t, err)
				defer stream.Close()
				require.EqualValues(t, 1, reports.Load(), "owner rejection must use the existing assignment report path")
				require.EqualValues(t, 2, checks.Load())
				events := make(chan wal.TransformLogStreamEvent, 4)
				mockey.Mock((*assignmentTestHandler).Handle).To(func(_ *assignmentTestHandler, event wal.TransformLogStreamEvent) {
					events <- event
				}).Build()
				mockey.Mock((*assignmentTestHandler).Close).Return().Build()
				sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: &assignmentTestHandler{}})
				require.NoError(t, err)
				select {
				case event := <-events:
					require.NotNil(t, event.SyncUp)
				case <-ctx.Done():
					t.Fatal("corrected assignment did not deliver subscription progress")
				}
				require.NoError(t, sub.Close())
				require.NoError(t, stream.Close())
				require.EqualValues(t, 1, reports.Load(), "normal close must not report an assignment error")
			})
		})
	}
}
