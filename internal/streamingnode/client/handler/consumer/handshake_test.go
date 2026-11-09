package consumer

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

	serviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type handshakeHandler struct{ closed atomic.Int32 }

func (h *handshakeHandler) Handle(message.HandleParam) message.HandleResult {
	return message.HandleResult{MessageHandled: true}
}
func (h *handshakeHandler) Close() { h.closed.Add(1) }
func handshakeOptions(h message.Handler) *ConsumerOptions {
	return &ConsumerOptions{
		Assignment: &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1}},
		VChannel:   "v1", DeliverPolicy: options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(1)), MessageHandler: h,
	}
}

func firstHandshakeResponse() *streamingpb.ConsumeResponse {
	return &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_Create{Create: &streamingpb.CreateConsumerResponse{WalName: "woodpecker"}}}
}

func successfulHandshakeResponse() *streamingpb.ConsumeResponse {
	return &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_CreateVchannel{CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{Response: &streamingpb.CreateVChannelConsumerResponse_ConsumerId{ConsumerId: 1}}}}
}

func TestConsumerFailedHandshakeCancelsOnlyItsStream(t *testing.T) {
	expected := status.NewWALNameMismatchError("woodpecker", "test")
	for _, stage := range []string{"open", "first-recv", "first-response", "second-send", "second-recv", "embedded-error", "second-response", "empty-second-response", "nil-embedded-error"} {
		t.Run(stage, func(t *testing.T) {
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			h := &handshakeHandler{}
			client := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
			stream := mock_streamingpb.NewMockStreamingNodeHandlerService_ConsumeClient(t)
			var attempt context.Context
			client.EXPECT().Consume(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ ...grpc.CallOption) (streamingpb.StreamingNodeHandlerService_ConsumeClient, error) {
				attempt = ctx
				if stage == "open" {
					return nil, expected
				}
				return stream, nil
			}).Once()
			if stage != "open" {
				calls := 0
				stream.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ConsumeResponse, error) {
					calls++
					if calls == 1 {
						if stage == "first-recv" {
							return nil, expected
						}
						if stage == "first-response" {
							return &streamingpb.ConsumeResponse{}, nil
						}
						return firstHandshakeResponse(), nil
					}
					if stage == "second-recv" {
						return nil, expected
					}
					if stage == "embedded-error" {
						return &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_CreateVchannel{CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{Response: &streamingpb.CreateVChannelConsumerResponse_Error{Error: expected.AsPBError()}}}}, nil
					}
					if stage == "empty-second-response" {
						return &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_CreateVchannel{CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{}}}, nil
					}
					if stage == "nil-embedded-error" {
						return &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_CreateVchannel{CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{Response: &streamingpb.CreateVChannelConsumerResponse_Error{}}}}, nil
					}
					return &streamingpb.ConsumeResponse{}, nil
				}).Maybe()
				stream.EXPECT().Send(mock.Anything).RunAndReturn(func(*streamingpb.ConsumeRequest) error {
					if stage == "second-send" {
						return expected
					}
					return nil
				}).Maybe()
				stream.EXPECT().CloseSend().Return(nil).Maybe()
			}
			c, err := CreateConsumer(parent, handshakeOptions(h), client)
			require.Error(t, err)
			require.Nil(t, c)
			if stage == "embedded-error" || stage == "open" || stage == "first-recv" || stage == "second-send" || stage == "second-recv" {
				require.True(t, status.AsStreamingError(err).IsWALNameMismatch())
			} else {
				require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_REQUEST_SEQ, status.AsStreamingError(err).Code)
			}
			require.NoError(t, parent.Err())
			require.ErrorIs(t, attempt.Err(), context.Canceled)
			require.Zero(t, h.closed.Load(), "failed creation must preserve the upper handler")
		})
	}
}

type handshakeServer struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	consume func(streamingpb.StreamingNodeHandlerService_ConsumeServer) error
}

func (s *handshakeServer) Consume(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
	return s.consume(stream)
}

func handshakeClient(t *testing.T, serve func(streamingpb.StreamingNodeHandlerService_ConsumeServer) error) streamingpb.StreamingNodeHandlerServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.StreamInterceptor(serviceinterceptor.NewServiceStreamServerInterceptor()))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &handshakeServer{consume: serve})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///handshake", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithStreamInterceptor(serviceinterceptor.NewServiceStreamClientInterceptor()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return streamingpb.NewStreamingNodeHandlerServiceClient(conn)
}

func TestRemoteConsumerReturnsTypedMismatchDuringCreation(t *testing.T) {
	client := handshakeClient(t, func(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
		if err := stream.Send(firstHandshakeResponse()); err != nil {
			return err
		}
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if _, old := req.GetCreateVchannelConsumer().GetDeliverPolicy().GetPolicy().(*streamingpb.DeliverPolicy_StartFrom); old {
			return status.NewWALNameMismatchError("woodpecker", "test")
		}
		if err := stream.Send(successfulHandshakeResponse()); err != nil {
			return err
		}
		_, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()
	h := &handshakeHandler{}
	opts := handshakeOptions(h)
	c, err := CreateConsumer(ctx, opts, client)
	require.Nil(t, c)
	require.True(t, status.AsStreamingError(err).IsWALNameMismatch())
	require.NoError(t, ctx.Err())
	require.Zero(t, h.closed.Load())
	opts.DeliverPolicy = options.DeliverPolicyAll()
	c, err = CreateConsumer(ctx, opts, client)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.Eventually(t, func() bool { return h.closed.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRemoteConsumerWaitsForSecondHandshakeAndCancels(t *testing.T) {
	entered := make(chan struct{})
	client := handshakeClient(t, func(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
		if err := stream.Send(firstHandshakeResponse()); err != nil {
			return err
		}
		if _, err := stream.Recv(); err != nil {
			return err
		}
		close(entered)
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	h := &handshakeHandler{}
	go func() { _, err := CreateConsumer(ctx, handshakeOptions(h), client); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no second handshake")
	}
	select {
	case err := <-done:
		t.Fatalf("constructor returned before second ack: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("handshake did not cancel")
	}
	require.Zero(t, h.closed.Load())
}
