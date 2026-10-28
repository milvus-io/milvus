package handler

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

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

type consumerMismatchServer struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	requests chan *streamingpb.CreateVChannelConsumerRequest
}

func (s *consumerMismatchServer) Consume(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
	if err := stream.Send(&streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_Create{
		Create: &streamingpb.CreateConsumerResponse{WalName: "woodpecker"},
	}}); err != nil {
		return err
	}
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	create := req.GetCreateVchannelConsumer()
	select {
	case s.requests <- create:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if _, all := create.GetDeliverPolicy().GetPolicy().(*streamingpb.DeliverPolicy_All); !all {
		return status.NewWALNameMismatchError("woodpecker", "test")
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

type mismatchMessageHandler struct {
	messages chan message.ImmutableMessage
	closed   atomic.Int32
}

func (h *mismatchMessageHandler) Handle(p message.HandleParam) message.HandleResult {
	select {
	case h.messages <- p.Message:
		return message.HandleResult{MessageHandled: true}
	case <-p.Ctx.Done():
		return message.HandleResult{Error: p.Ctx.Err()}
	}
}

func (h *mismatchMessageHandler) Close() { h.closed.Add(1) }

func TestHandlerClientRemoteConsumerFallsBackAfterWALNameMismatch(t *testing.T) {
	paramtable.Init()
	previousRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() { paramtable.SetRole(previousRole) })
	for _, policy := range []options.DeliverPolicy{
		options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(1)),
		options.DeliverPolicyStartAfter(walimplstest.NewTestMessageID(1)),
	} {
		t.Run(policy.String(), func(t *testing.T) {
			server := &consumerMismatchServer{requests: make(chan *streamingpb.CreateVChannelConsumerRequest, 2)}
			listener := bufconn.Listen(1 << 20)
			grpcServer := grpc.NewServer(grpc.StreamInterceptor(serviceinterceptor.NewStreamingServiceStreamServerInterceptor()))
			streamingpb.RegisterStreamingNodeHandlerServiceServer(grpcServer, server)
			go func() { _ = grpcServer.Serve(listener) }()
			t.Cleanup(grpcServer.Stop)
			conn, err := grpc.NewClient("passthrough:///consumer-mismatch", grpc.WithTransportCredentials(insecure.NewCredentials()),
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
			handler := &mismatchMessageHandler{messages: make(chan message.ImmutableMessage, 1)}
			filters := []options.DeliverFilter{options.DeliverFilterTimeTickGT(100), options.DeliverFilterMessageType(message.MessageTypeInsert)}
			opts := &ConsumerOptions{PChannel: "p", VChannel: "v1", DeliverPolicy: policy, DeliverFilters: filters, MessageHandler: handler, IgnorePauseConsumption: true}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c, err := hc.CreateConsumer(ctx, opts)
			require.NoError(t, err)
			defer c.Close()
			require.Eventually(t, func() bool { return len(server.requests) == 2 }, time.Second, time.Millisecond,
				"expected the original subscription followed by the All fallback")
			first, second := <-server.requests, <-server.requests
			require.True(t, proto.Equal(policy, first.DeliverPolicy))
			require.IsType(t, &streamingpb.DeliverPolicy_All{}, second.DeliverPolicy.GetPolicy())
			for _, request := range []*streamingpb.CreateVChannelConsumerRequest{first, second} {
				require.Equal(t, "v1", request.Vchannel)
				require.True(t, request.IgnorePauseConsumption)
				require.Len(t, request.DeliverFilters, len(filters))
				for i := range filters {
					require.True(t, proto.Equal(filters[i], request.DeliverFilters[i]))
				}
			}
			require.Same(t, policy, opts.DeliverPolicy, "fallback must not mutate the outer consumer's position")
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
