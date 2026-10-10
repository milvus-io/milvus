package transformlog

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type joiningTransformStream struct{ wal.TransformLogStream }

func (*joiningTransformStream) Subscribe(context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	panic("mockey")
}
func (*joiningTransformStream) Close() error          { panic("mockey") }
func (*joiningTransformStream) Done() <-chan struct{} { panic("mockey") }

type joiningTransformSubscription struct{ wal.TransformLogSubscription }

func (*joiningTransformSubscription) Close() error { panic("mockey") }

// The provider implementation belongs to the SN workspace. Model its callback
// worker and cancel-and-join Close contract here, while executing the real RPC
// server and handler. An immediate-return Close fake cannot catch this deadlock.
func TestSubscribeServerCreateFailureUnblocksCallbacks(t *testing.T) {
	for _, pending := range []string{"entry", "sync_up", "cancellation"} {
		t.Run(pending, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			workerCtx, cancelWorker := context.WithCancel(ctx)
			defer cancelWorker()
			started, callbackStarted, workerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			handlers := make(chan wal.TransformLogEventHandler, 1)
			streamClosed := make(chan struct{})
			var closeOnce sync.Once
			patch := func(p *mockey.Mocker) { t.Cleanup(func() { p.UnPatch() }) }
			patch(mockey.Mock((*joiningTransformStream).Done).Return(streamClosed).Build())
			patch(mockey.Mock((*joiningTransformStream).Subscribe).To(func(_ *joiningTransformStream, _ context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				handlers <- opt.Handler
				go func() {
					defer close(workerDone)
					close(started)
					event := wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel}
					switch pending {
					case "entry":
						event.Entry = &streamingpb.TransformLogEntry{TimeTick: 11}
					case "sync_up":
						event.SyncUp = &wal.TransformLogSyncUp{TimeTick: 11}
					case "cancellation":
						<-workerCtx.Done()
						event.Err = workerCtx.Err()
					}
					close(callbackStarted)
					_ = opt.Handler.Handle(event)
					<-workerCtx.Done()
				}()
				return &joiningTransformSubscription{}, nil
			}).Build())
			patch(mockey.Mock((*joiningTransformSubscription).Close).To(func(*joiningTransformSubscription) error {
				cancelWorker()
				<-workerDone
				return nil
			}).Build())
			patch(mockey.Mock((*joiningTransformStream).Close).To(func(*joiningTransformStream) error {
				cancelWorker()
				<-workerDone
				closeOnce.Do(func() { close(streamClosed) })
				return nil
			}).Build())
			patch(mockey.Mock((*SubscribeServer).send).To(func(s *SubscribeServer, response *streamingpb.TransformResponse) error {
				if response.GetCreate() == nil {
					t.Error("failed creation must not forward subscription events")
					return io.ErrClosedPipe
				}
				wait := callbackStarted
				if pending == "cancellation" {
					wait = started
				}
				select {
				case <-wait:
					return io.ErrClosedPipe
				case <-s.ctx.Done():
					return s.ctx.Err()
				}
			}).Build())
			executed := make(chan error, 1)
			rpcReturned := make(chan struct{})
			patch(mockey.Mock(streamingpb.UnimplementedStreamingNodeHandlerServiceServer.SubscribeTransform).To(func(_ streamingpb.UnimplementedStreamingNodeHandlerServiceServer, rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
				defer close(rpcReturned)
				serverCtx, serverCancel := context.WithCancelCause(rpc.Context())
				defer serverCancel(nil)
				server := &SubscribeServer{ctx: serverCtx, cancel: serverCancel, stream: rpc, logStream: &joiningTransformStream{}, outgoing: make(chan response, 16), subs: make(map[int64]*serverSubscription)}
				err := server.Execute()
				executed <- err
				return err
			}).Build())
			listener := bufconn.Listen(64 * 1024)
			server := grpc.NewServer()
			streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
			go func() { _ = server.Serve(listener) }()
			defer listener.Close()
			defer server.Stop()
			conn, err := grpc.NewClient("passthrough:///transform-create-failure", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			require.NoError(t, err)
			defer conn.Close()
			rpc, err := streamingpb.NewStreamingNodeHandlerServiceClient(conn).SubscribeTransform(ctx)
			require.NoError(t, err)
			require.NoError(t, rpc.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{Create: &streamingpb.CreateTransformSubscriptionRequest{SubscriptionId: 1, Vchannel: "v1", StartAfterTimeTick: 10}}}))
			var handler wal.TransformLogEventHandler
			select {
			case handler = <-handlers:
			case <-ctx.Done():
				t.Fatal("subscription was not created")
			}
			defer func() {
				// Rescue a regressed implementation before removing runtime patches.
				handler.Close()
				cancelWorker()
				cancel()
				<-workerDone
				<-rpcReturned
			}()
			stopped := make(chan struct{})
			go func() { server.GracefulStop(); close(stopped) }()
			select {
			case err := <-executed:
				require.ErrorIs(t, err, io.ErrClosedPipe)
			case <-time.After(time.Second):
				t.Fatal("create failure blocked joining subscription callbacks")
			}
			select {
			case <-stopped:
			case <-ctx.Done():
				t.Fatal("gRPC graceful shutdown did not complete")
			}
			select {
			case <-streamClosed:
			default:
				t.Fatal("Execute did not close the acquired stream")
			}
			_, err = rpc.Recv()
			require.Error(t, err)
		})
	}
}
