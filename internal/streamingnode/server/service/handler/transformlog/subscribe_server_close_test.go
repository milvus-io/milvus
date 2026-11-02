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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// Exercise the real WALSummary worker and gRPC shutdown: a subscription fake
// whose Close returns immediately cannot detect the callback/join deadlock.
func TestSubscribeServerCreateFailureUnblocksCallbacks(t *testing.T) {
	for _, pending := range []string{"entry", "sync_up", "cancellation"} {
		t.Run(pending, func(t *testing.T) {
			readStarted := make(chan struct{})
			callbackStarted := make(chan struct{})
			var readOnce, callbackOnce sync.Once
			changed := make(chan struct{})
			readPatch := mockey.Mock((*walsummary.Manager).ReadTransform).To(func(_ *walsummary.Manager, ctx context.Context, _ string, _, _ uint64, _ walsummary.ReadLimits) (walsummary.TransformBatch, error) {
				readOnce.Do(func() { close(readStarted) })
				if pending == "cancellation" {
					<-ctx.Done()
					return walsummary.TransformBatch{}, ctx.Err()
				}
				batch := walsummary.TransformBatch{CoveredThrough: 11, ReadableThrough: 11, Changed: changed, TransformChanged: changed}
				if pending == "entry" {
					batch.Entries = []*streamingpb.TransformLogEntry{{TimeTick: 11}}
				}
				return batch, nil
			}).Build()
			defer readPatch.UnPatch()

			handlers := make(chan *serverEventHandler, 1)
			var originalHandle func(*serverEventHandler, wal.TransformLogStreamEvent)
			handlePatch := mockey.Mock((*serverEventHandler).Handle).To(func(h *serverEventHandler, event wal.TransformLogStreamEvent) {
				callbackOnce.Do(func() {
					handlers <- h
					close(callbackStarted)
				})
				originalHandle(h, event)
			}).Origin(&originalHandle).Build()
			defer handlePatch.UnPatch()

			sendPatch := mockey.Mock((*SubscribeServer).send).To(func(s *SubscribeServer, response *streamingpb.TransformResponse) error {
				if response.GetCreate() == nil {
					t.Error("failed creation must not send subscription events")
					return io.ErrClosedPipe
				}
				// Hold the create acknowledgement until the worker has started. In
				// the cancellation case, Close cancels the read without a callback.
				started := callbackStarted
				if pending == "cancellation" {
					started = readStarted
				}
				select {
				case <-started:
					return io.ErrClosedPipe
				case <-s.stream.Context().Done():
					return s.stream.Context().Err()
				}
			}).Build()
			defer sendPatch.UnPatch()

			manager := walsummary.NewManager(walsummary.ManagerConfig{})
			manager.ObserveQueryBarrier("v1", 11)
			logStream := walsummary.NewDispatchStream(manager)
			executed := make(chan error, 1)
			rpcReturned := make(chan struct{})
			servePatch := mockey.Mock(streamingpb.UnimplementedStreamingNodeHandlerServiceServer.SubscribeTransform).To(func(_ streamingpb.UnimplementedStreamingNodeHandlerServiceServer, rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
				defer close(rpcReturned)
				server := &SubscribeServer{stream: rpc, logStream: logStream, subs: make(map[int64]wal.TransformLogSubscription)}
				err := server.Execute()
				executed <- err
				return err
			}).Build()
			defer servePatch.UnPatch()

			listener := bufconn.Listen(64 * 1024)
			server := grpc.NewServer()
			streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
			go func() { _ = server.Serve(listener) }()
			defer listener.Close()
			defer server.Stop()
			conn, err := grpc.NewClient("passthrough:///transform-create-failure",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			rpc, err := streamingpb.NewStreamingNodeHandlerServiceClient(conn).SubscribeTransform(ctx)
			require.NoError(t, err)
			require.NoError(t, rpc.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{
				Create: &streamingpb.CreateTransformSubscriptionRequest{SubscriptionId: 1, Vchannel: "v1", StartAfterTimeTick: 10},
			}}))

			var handler *serverEventHandler
			if pending != "cancellation" {
				select {
				case handler = <-handlers:
				case <-ctx.Done():
					t.Fatal("worker did not enter Handle")
				}
			}
			defer func() {
				// Rescue the unfixed implementation after a failing assertion so
				// no goroutine can outlive the runtime patches.
				if handler != nil {
					handler.Close()
				}
				cancel()
				_ = logStream.Close()
				<-rpcReturned
			}()

			stopped := make(chan struct{})
			go func() { server.GracefulStop(); close(stopped) }()
			select {
			case err := <-executed:
				require.ErrorIs(t, err, io.ErrClosedPipe)
			case <-time.After(time.Second):
				t.Fatal("create failure deadlocked waiting for the subscription callback")
			}
			select {
			case <-stopped:
			case <-ctx.Done():
				t.Fatal("gRPC graceful shutdown did not complete")
			}
			_, err = rpc.Recv()
			require.Error(t, err)
			require.ErrorIs(t, logStream.Error(), context.Canceled, "Execute must also close the acquired stream")
		})
	}
}

func TestSubscribeServerDataSendFailureStopsWorker(t *testing.T) {
	for _, kind := range []string{"entry", "sync_up"} {
		t.Run(kind, func(t *testing.T) {
			changed := make(chan struct{})
			readPatch := mockey.Mock((*walsummary.Manager).ReadTransform).Return(walsummary.TransformBatch{
				Entries:        []*streamingpb.TransformLogEntry{{TimeTick: 10}},
				CoveredThrough: 10, ReadableThrough: 10, Changed: changed, TransformChanged: changed,
			}, nil).Build()
			defer readPatch.UnPatch()
			var original func(*SubscribeServer, *streamingpb.TransformResponse) error
			sendPatch := mockey.Mock((*SubscribeServer).send).Origin(&original).To(func(s *SubscribeServer, resp *streamingpb.TransformResponse) error {
				if (kind == "entry" && resp.GetMessageBatch() != nil) || (kind == "sync_up" && resp.GetSyncUp() != nil) {
					return io.ErrClosedPipe
				}
				return original(s, resp)
			}).Build()
			defer sendPatch.UnPatch()
			wire := newFakeSubscribeTransformServer(t.Context())
			defer close(wire.recvCh)
			manager := walsummary.NewManager(walsummary.ManagerConfig{})
			manager.ObserveQueryBarrier("v", 10)
			logStream := walsummary.NewDispatchStream(manager)
			server := &SubscribeServer{stream: wire, logStream: logStream, subs: make(map[int64]wal.TransformLogSubscription)}
			finished := make(chan error, 1)
			go func() { finished <- server.Execute() }()
			wire.recv(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{
				Create: &streamingpb.CreateTransformSubscriptionRequest{SubscriptionId: 1, Vchannel: "v"},
			}})
			require.NotNil(t, wire.sent(t).GetCreate())
			select {
			case err := <-finished:
				require.ErrorIs(t, err, io.ErrClosedPipe)
			case <-time.After(time.Second):
				t.Fatal("send failure did not release the worker and end Execute")
			}
			require.ErrorIs(t, logStream.Error(), context.Canceled)
		})
	}
}
