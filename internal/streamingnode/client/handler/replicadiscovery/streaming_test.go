package replicadiscovery

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

	serveradapter "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	domain "github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type endpoint struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	directory *domain.Directory
}

func (s *endpoint) SubscribeReplicas(rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
	stream := s.directory.AcquireStream(rpc.Context(), "p", nil)
	defer stream.Close()
	err := serveradapter.ServeStream(rpc, "p", stream)
	if err != nil {
		return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	}
	return nil
}

func remoteClientForTest(t *testing.T) (streamingpb.StreamingNodeHandlerServiceClient, *domain.Directory) {
	t.Helper()
	d := domain.NewDirectory()
	listener := bufconn.Listen(65536)
	server := grpc.NewServer()
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, &endpoint{directory: d})
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///replicas", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(); server.Stop(); listener.Close(); d.Close() })
	return streamingpb.NewStreamingNodeHandlerServiceClient(conn), d
}

func remoteForTest(t *testing.T) (*EventStream, *domain.Directory) {
	t.Helper()
	client, d := remoteClientForTest(t)
	stream, err := CreateEventStream(t.Context(), &EventStreamOptions{Assignment: &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1}}}, client)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })
	return stream, d
}

func TestReplicaWatchReceiveProgressDuringBlockedSend(t *testing.T) {
	stream, d := remoteForTest(t)
	entered, exited := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var original func(*EventStream, *streamingpb.ReplicaWatchRequest) error
	patch := mockey.Mock((*EventStream).send).Origin(&original).To(func(s *EventStream, req *streamingpb.ReplicaWatchRequest) error {
		err := original(s, req)
		if err == nil && req.GetCreate().GetVchannel() == "p_2v0" && once.CompareAndSwap(false, true) {
			// Hold the send lock after peer acceptance: responses may arrive before Send returns.
			s.sendMu.Lock()
			defer s.sendMu.Unlock()
			close(entered)
			defer close(exited)
			<-s.ctx.Done()
			return s.ctx.Err()
		}
		return err
	}).Build()
	defer patch.UnPatch()
	first, err := stream.Subscribe(t.Context(), "p_1v0", func(domain.Snapshot) {})
	require.NoError(t, err)
	received := make(chan struct{}, 1)
	finished := make(chan error, 1)
	go func() {
		_, err := stream.Subscribe(t.Context(), "p_2v0", func(domain.Snapshot) {
			select {
			case received <- struct{}{}:
			default:
			}
		})
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("send did not block")
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("receive waits for sender")
	}
	d.Set("p_1v0", 7, &qviews.QueryViewVersion{})
	require.Eventually(t, func() bool { return len(first.Snapshot().Replicas) == 1 }, time.Second, time.Millisecond)
	d.Close()
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("owner loss did not release Send")
	}
	<-exited
	require.Error(t, stream.Close())
	select {
	case <-stream.received:
	default:
		t.Fatal("Close did not join receive loop")
	}
}

func TestReplicaWatchPhysicalSubscriptionLifetime(t *testing.T) {
	stream, d := remoteForTest(t)
	_, err := stream.Subscribe(t.Context(), "", func(domain.Snapshot) {})
	require.Error(t, err)
	first, err := stream.Subscribe(t.Context(), "p_1v0", func(domain.Snapshot) {})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return first.Snapshot().Valid }, time.Second, time.Millisecond)
	require.NoError(t, first.Close())
	require.NoError(t, first.Close())
	second, err := stream.Subscribe(t.Context(), "p_1v0", func(domain.Snapshot) {})
	require.NoError(t, err)
	require.NotEqual(t, first.(*eventSubscription).ID(), second.(*eventSubscription).ID())
	d.Set("p_1v0", 9, &qviews.QueryViewVersion{})
	require.Eventually(t, func() bool { return len(second.Snapshot().Replicas) == 1 }, time.Second, time.Millisecond)
	require.False(t, first.Snapshot().Valid)
	require.NoError(t, stream.Close())
	require.NoError(t, stream.Close())
	require.False(t, second.Snapshot().Valid)
	_, err = stream.Subscribe(t.Context(), "p_1v0", func(domain.Snapshot) {})
	require.Error(t, err)
}

func TestReplicaWatchHandshakeAndSubscriptionFailures(t *testing.T) {
	for _, scenario := range []string{"invalid-handshake", "subscription-error", "stream-closed-before-ready", "cancel-subscribe"} {
		t.Run(scenario, func(t *testing.T) {
			mockey.PatchConvey(scenario, t, func() {
				entered := make(chan struct{})
				mockey.Mock((*endpoint).SubscribeReplicas).To(func(_ *endpoint, rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
					if scenario == "invalid-handshake" {
						return rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_CloseStream{CloseStream: &streamingpb.CloseReplicaWatchStreamResponse{}}})
					}
					if err := rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_CreateStream{CreateStream: &streamingpb.CreateReplicaWatchStreamResponse{}}}); err != nil {
						return err
					}
					req, err := rpc.Recv()
					if err != nil {
						return err
					}
					close(entered)
					id := req.GetCreate().GetSubscriptionId()
					switch scenario {
					case "subscription-error":
						return rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_SubscriptionError{SubscriptionError: &streamingpb.ReplicaWatchSubscriptionError{SubscriptionId: id, Vchannel: "p_1v0", Error: (*streamingpb.StreamingError)(status.NewOnShutdownError("owner closing"))}}})
					case "cancel-subscribe":
						req, err = rpc.Recv()
						if err != nil {
							return err
						}
						require.Equal(t, id, req.GetCloseSubscription().GetSubscriptionId())
					}
					return nil
				}).Build()
				client, _ := remoteClientForTest(t)
				stream, err := CreateEventStream(t.Context(), &EventStreamOptions{Assignment: &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1}}}, client)
				if scenario == "invalid-handshake" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				defer stream.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if scenario == "cancel-subscribe" {
					go func() { <-entered; cancel() }()
				}
				_, err = stream.Subscribe(ctx, "p_1v0", func(domain.Snapshot) {})
				require.Error(t, err)
				if scenario == "cancel-subscribe" {
					require.ErrorIs(t, err, context.Canceled)
				}
				if scenario == "subscription-error" {
					require.True(t, status.AsStreamingError(err).IsOnShutdown())
				}
			})
		})
	}
}
