package transformlog

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// Use the real gRPC client stream: a fake with an idempotent CloseSend hides
// the transport race between EventStream.Close and recvLoop's finish.
type closeTestServer struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	serve func(streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error
}

type closeTestHandler struct {
	*recordingHandler
}

func (h *closeTestHandler) Close() {
	h.recordingHandler.Close()
}

func (s *closeTestServer) SubscribeTransform(stream streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
	return s.serve(stream)
}

func newCloseTestStream(t *testing.T, ctx context.Context, serve func(streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error) *EventStream {
	t.Helper()
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer(grpc.StaticStreamWindowSize(64*1024), grpc.StaticConnWindowSize(64*1024))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &closeTestServer{serve: serve})
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///transform-close-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Stop()
		_ = conn.Close()
		_ = listener.Close()
	})
	stream, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, streamingpb.NewStreamingNodeHandlerServiceClient(conn))
	require.NoError(t, err)
	return stream
}

func TestEventStreamConcurrentClose(t *testing.T) {
	for _, ending := range []string{"response", "eof", "error"} {
		t.Run(ending, func(t *testing.T) {
			var handlerCloses atomic.Int32
			patch := mockey.Mock((*closeTestHandler).Close).To(func(*closeTestHandler) { handlerCloses.Add(1) }).Build()
			defer patch.UnPatch()
			var closeRequests atomic.Int32
			stream := newCloseTestStream(t, context.Background(), func(rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
				for {
					req, err := rpc.Recv()
					if err != nil {
						return err
					}
					if create := req.GetCreate(); create != nil {
						if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_Create{
							Create: &streamingpb.CreateTransformSubscriptionResponse{SubscriptionId: create.GetSubscriptionId()},
						}}); err != nil {
							return err
						}
					} else if req.GetCloseStream() != nil {
						closeRequests.Add(1)
						switch ending {
						case "response":
							return rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseStream{
								CloseStream: &streamingpb.CloseTransformStreamResponse{},
							}})
						case "error":
							return status.Error(codes.Unavailable, "transport closed by test server")
						default:
							return nil
						}
					}
				}
			})
			sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: &closeTestHandler{newRecordingHandler()}})
			require.NoError(t, err)
			results := make(chan error, 16)
			for range cap(results) {
				go func() { results <- stream.Close() }()
			}
			for range cap(results) {
				err := recvEvent(t, results)
				if ending == "error" {
					require.Equal(t, codes.Unavailable, status.Code(err))
				} else {
					require.NoError(t, err)
				}
			}
			_ = sub.Close() // Wait for the subscription's terminal callback too.
			require.EqualValues(t, 1, handlerCloses.Load())
			require.EqualValues(t, 1, closeRequests.Load())
			require.Error(t, stream.send(&streamingpb.TransformRequest{}))
		})
	}
}

func TestEventStreamFinishCancelsBlockedSend(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	stream := newCloseTestStream(t, parent, func(rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
		close(entered)
		<-rpc.Context().Done() // Deliberately do not read: exhaust HTTP/2 flow control.
		return rpc.Context().Err()
	})
	recvEvent(t, entered)
	sent := make(chan error, 1)
	go func() {
		for range 32 {
			if err := stream.send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{
				Create: &streamingpb.CreateTransformSubscriptionRequest{Vchannel: strings.Repeat("x", 1024*1024)},
			}}); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	requireNoEvent(t, sent)
	finished := make(chan struct{})
	go func() { stream.finish(io.ErrUnexpectedEOF); close(finished) }()
	recvEvent(t, finished)
	require.Error(t, recvEvent(t, sent))
	require.ErrorIs(t, stream.Close(), io.ErrUnexpectedEOF)
	require.NoError(t, parent.Err(), "closing one RPC must not cancel its owner")
}

func TestEventStreamCloseTimeout(t *testing.T) {
	stream := newCloseTestStream(t, context.Background(), func(rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
		// Receive the close request but never acknowledge or end the RPC.
		_, err := rpc.Recv()
		if err != nil {
			return err
		}
		<-rpc.Context().Done()
		return rpc.Context().Err()
	})
	result := make(chan error, 1)
	go func() { result <- stream.Close() }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("close did not cancel the unresponsive RPC")
	}
}
