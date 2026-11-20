package handler

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_assignment"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	serviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Reject the old position, then fail the first live subscription to exercise
// retry after catch-up has already completed.
type walSwitchCatchUpServer struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	subscriptionRequests      chan *streamingpb.CreateVChannelConsumerRequest
	failFirstLiveSubscription atomic.Bool
}

func (s *walSwitchCatchUpServer) Consume(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
	if err := stream.Send(&streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_Create{
		Create: &streamingpb.CreateConsumerResponse{WalName: "woodpecker"},
	}}); err != nil {
		return err
	}
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	subscriptionRequest := req.GetCreateVchannelConsumer()
	select {
	case s.subscriptionRequests <- subscriptionRequest:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if _, all := subscriptionRequest.GetDeliverPolicy().GetPolicy().(*streamingpb.DeliverPolicy_All); !all {
		return status.NewWALNameMismatchError("woodpecker", "test")
	}
	if s.failFirstLiveSubscription.Swap(false) {
		return status.NewUnrecoverableError("injected failure before live subscription succeeds")
	}
	if err := stream.Send(&streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_CreateVchannel{
		CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{Response: &streamingpb.CreateVChannelConsumerResponse_ConsumerId{ConsumerId: 1}},
	}}); err != nil {
		return err
	}
	msg := message.NewTimeTickMessageBuilderV1().WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).WithAllVChannel().MustBuildMutable().WithTimeTick(101).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	if err := stream.Send(&streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_Consume{
		Consume: &streamingpb.ConsumeMessageReponse{Message: msg.IntoImmutableMessageProto()},
	}}); err != nil {
		return err
	}
	_, err = stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

type walSwitchCatchUpMessageHandler struct {
	messages chan message.ImmutableMessage
	closed   atomic.Int32
}

func (h *walSwitchCatchUpMessageHandler) Handle(p message.HandleParam) message.HandleResult {
	select {
	case h.messages <- p.Message:
		return message.HandleResult{MessageHandled: true}
	case <-p.Ctx.Done():
		return message.HandleResult{Error: p.Ctx.Err()}
	}
}

func (h *walSwitchCatchUpMessageHandler) Close() { h.closed.Add(1) }

func TestRemoteConsumerCatchesUpOldWALBeforeLiveRetry(t *testing.T) {
	paramtable.Init()
	previousRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() { paramtable.SetRole(previousRole) })
	for _, initialPolicy := range []options.DeliverPolicy{
		options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(1)),
		options.DeliverPolicyStartAfter(walimplstest.NewTestMessageID(1)),
	} {
		t.Run(initialPolicy.String(), func(t *testing.T) {
			server := &walSwitchCatchUpServer{subscriptionRequests: make(chan *streamingpb.CreateVChannelConsumerRequest, 3)}
			server.failFirstLiveSubscription.Store(true)
			listener := bufconn.Listen(1 << 20)
			grpcServer := grpc.NewServer(grpc.StreamInterceptor(serviceinterceptor.NewStreamingServiceStreamServerInterceptor()))
			streamingpb.RegisterStreamingNodeHandlerServiceServer(grpcServer, server)
			go func() { _ = grpcServer.Serve(listener) }()
			t.Cleanup(grpcServer.Stop)
			conn, err := grpc.NewClient("passthrough:///wal-switch-catchup", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
				grpc.WithStreamInterceptor(serviceinterceptor.NewStreamingServiceStreamClientInterceptor()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
			service.EXPECT().GetService(mock.Anything).Return(streamingpb.NewStreamingNodeHandlerServiceClient(conn), nil)
			assignment := &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1}, Node: types.StreamingNodeInfo{ServerID: 1}}
			watcher := mock_assignment.NewMockWatcher(t)
			watcher.EXPECT().Get(mock.Anything, "p").Return(assignment)
			watcher.EXPECT().Watch(mock.Anything, "p", assignment).Return(nil).Once()
			hc := &handlerClientImpl{lifetime: typeutil.NewLifetime(), service: service, watcher: watcher, newConsumer: consumer.CreateConsumer}
			// Stub the old-WAL replay here; reader tests verify actual message delivery.
			catchUpCalls := 0
			catchUpPatch := mockey.Mock(utility.CatchUpWALAfterSwitch).To(func(_ context.Context, catchUpOpts utility.WALSwitchCatchUpOptions) (uint64, error) {
				catchUpCalls++
				require.True(t, proto.Equal(initialPolicy, catchUpOpts.DeliverPolicy))
				return 100, nil
			}).Build()
			defer catchUpPatch.UnPatch()
			handler := &walSwitchCatchUpMessageHandler{messages: make(chan message.ImmutableMessage, 1)}
			filters := []options.DeliverFilter{options.DeliverFilterTimeTickGT(100), options.DeliverFilterMessageType(message.MessageTypeInsert)}
			opts := &ConsumerOptions{PChannel: "p", VChannel: "v1", DeliverPolicy: initialPolicy, DeliverFilters: filters, MessageHandler: handler, IgnorePauseConsumption: true}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c, err := hc.CreateConsumer(ctx, opts)
			require.NoError(t, err)
			defer c.Close()
			require.Eventually(t, func() bool { return len(server.subscriptionRequests) == 3 }, time.Second, time.Millisecond,
				"expected the original subscription, failed All subscription, and successful All retry")
			oldPositionSubscription, failedLiveSubscription, liveSubscription := <-server.subscriptionRequests, <-server.subscriptionRequests, <-server.subscriptionRequests
			require.True(t, proto.Equal(initialPolicy, oldPositionSubscription.DeliverPolicy))
			require.IsType(t, &streamingpb.DeliverPolicy_All{}, failedLiveSubscription.DeliverPolicy.GetPolicy())
			require.IsType(t, &streamingpb.DeliverPolicy_All{}, liveSubscription.DeliverPolicy.GetPolicy())
			require.Equal(t, 1, catchUpCalls)
			for _, request := range []*streamingpb.CreateVChannelConsumerRequest{oldPositionSubscription, failedLiveSubscription, liveSubscription} {
				require.Equal(t, "v1", request.Vchannel)
				require.True(t, request.IgnorePauseConsumption)
				if request == oldPositionSubscription {
					require.Len(t, request.DeliverFilters, len(filters))
				} else {
					require.Len(t, request.DeliverFilters, len(filters)+1)
					require.True(t, proto.Equal(options.DeliverFilterTimeTickGT(100), request.DeliverFilters[len(filters)]))
				}
				for i := range filters {
					require.True(t, proto.Equal(filters[i], request.DeliverFilters[i]))
				}
			}
			require.Same(t, initialPolicy, opts.DeliverPolicy, "fallback must not mutate the outer consumer's position")
			require.Zero(t, handler.closed.Load(), "failed creation must preserve the handler for retry")
			select {
			case msg := <-handler.messages:
				require.EqualValues(t, 101, msg.TimeTick())
			case <-ctx.Done():
				t.Fatal("fallback did not resume delivery")
			}
			require.NoError(t, ctx.Err())
		})
	}
}
