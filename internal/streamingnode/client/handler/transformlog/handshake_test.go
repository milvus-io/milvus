package transformlog

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func handshakeClient(t *testing.T, serve func(streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error) streamingpb.StreamingNodeHandlerServiceClient {
	t.Helper()
	patch := mockey.Mock((*streamingpb.UnimplementedStreamingNodeHandlerServiceServer).SubscribeTransform).To(func(_ *streamingpb.UnimplementedStreamingNodeHandlerServiceServer, rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
		return serve(rpc)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer(grpc.StreamInterceptor(interceptor.NewStreamingServiceStreamServerInterceptor()))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///transform-handshake",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(interceptor.NewStreamingServiceStreamClientInterceptor()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { server.Stop(); _ = conn.Close(); _ = listener.Close() })
	return streamingpb.NewStreamingNodeHandlerServiceClient(conn)
}

func TestEventStreamCreationWaitsForOwnerValidation(t *testing.T) {
	entered, allow := make(chan struct{}), make(chan struct{})
	client := handshakeClient(t, func(rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
		close(entered)
		select {
		case <-allow:
		case <-rpc.Context().Done():
			return rpc.Context().Err()
		}
		if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CreateStream{CreateStream: &streamingpb.CreateTransformStreamResponse{}}}); err != nil {
			return err
		}
		request, err := rpc.Recv()
		if err != nil {
			return err
		}
		require.NotNil(t, request.GetCloseStream())
		return rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseStream{CloseStream: &streamingpb.CloseTransformStreamResponse{}}})
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type result struct {
		stream *EventStream
		err    error
	}
	created := make(chan result, 1)
	go func() {
		s, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, client)
		created <- result{s, err}
	}()
	recvEvent(t, entered)
	require.Empty(t, created, "opening the RPC is not owner validation")
	close(allow)
	got := recvEvent(t, created)
	require.NoError(t, got.err)
	require.NoError(t, got.stream.Close())
	require.NoError(t, ctx.Err())
}

func TestEventStreamCreationErrors(t *testing.T) {
	for _, mode := range []string{"wrong_owner", "wrong_term", "invalid_ack", "cancel", "eof"} {
		t.Run(mode, func(t *testing.T) {
			entered, exited := make(chan struct{}), make(chan struct{})
			client := handshakeClient(t, func(rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
				defer close(exited)
				close(entered)
				switch mode {
				case "wrong_owner":
					return status.NewChannelNotExist("pchannel")
				case "wrong_term":
					return status.NewUnmatchedChannelTerm("pchannel", 1, 2)
				case "eof":
					return nil
				case "invalid_ack":
					if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseSubscription{CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{SubscriptionId: 9}}}); err != nil {
						return err
					}
				}
				<-rpc.Context().Done()
				return rpc.Context().Err()
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if mode == "cancel" {
				go func() { <-entered; cancel() }()
			}
			s, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, client)
			require.Nil(t, s)
			require.Error(t, err)
			switch mode {
			case "wrong_owner", "wrong_term":
				require.True(t, status.AsStreamingError(err).IsWrongStreamingNode())
			case "invalid_ack":
				require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INVALID_REQUEST_SEQ, status.AsStreamingError(err).Code)
			case "cancel":
				require.True(t, status.IsCanceled(err))
			case "eof":
				require.ErrorIs(t, err, io.EOF)
			}
			recvEvent(t, exited) // Failure must cancel the physical RPC, not leave a reader behind.
			if mode != "cancel" {
				require.NoError(t, ctx.Err())
			}
		})
	}
}
