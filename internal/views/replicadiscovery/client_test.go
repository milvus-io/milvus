package replicadiscovery

import (
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// A real gRPC test endpoint with replaceable WAL ownership, not a mocked stream.
type directoryServer struct {
	viewpb.UnimplementedViewQueryServiceServer
	mu        sync.Mutex
	directory *Directory
	active    atomic.Int64
}

func (s *directoryServer) WatchVChannelReplicas(stream viewpb.ViewQueryService_WatchVChannelReplicasServer) error {
	s.active.Add(1)
	defer s.active.Add(-1)
	s.mu.Lock()
	watch := s.directory.Watch()
	s.mu.Unlock()
	defer watch.Close()
	err := Serve(stream, "p", watch, nil)
	if err != nil {
		return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	}
	return nil
}

func newDirectoryTransport(t *testing.T) (*directoryServer, viewpb.ViewQueryServiceClient) {
	t.Helper()
	endpoint := &directoryServer{directory: NewDirectory()}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	viewpb.RegisterViewQueryServiceServer(server, endpoint)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.DialContext(t.Context(), "passthrough:///replicas", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		server.Stop()
		listener.Close()
		endpoint.mu.Lock()
		endpoint.directory.Close()
		endpoint.mu.Unlock()
	})
	return endpoint, viewpb.NewViewQueryServiceClient(conn)
}

func watcherForClient(t *testing.T, client viewpb.ViewQueryServiceClient) *watcher {
	t.Helper()
	w := newWatcher(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		stream, err := client.WatchVChannelReplicas(ctx)
		if err != nil {
			return nil, err
		}
		ready, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if !ready.GetReady() {
			return nil, status.NewInvalidRequestSeq("missing ready")
		}
		return NewRemoteStream(stream, func() {}), nil
	})
	t.Cleanup(w.Close)
	return w
}

func waitReplicaSnapshot(t *testing.T, sub *Subscription, want []int64) {
	t.Helper()
	require.Eventually(t, func() bool { s := sub.Snapshot(); return s.Valid && slices.Equal(s.Replicas, testReplicas(want...)) }, 5*time.Second, time.Millisecond)
}

func TestReplicaWatchReconnectAndSharedSubscriptions(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	endpoint.directory.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	w := watcherForClient(t, client)
	first, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	second, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	require.Same(t, first.state, second.state)
	waitReplicaSnapshot(t, first, []int64{1})
	first.Close()
	endpoint.directory.Set("p_1v0", 2, &qviews.QueryViewVersion{})
	waitReplicaSnapshot(t, second, []int64{1, 2})
	empty, err := w.subscribe("p_2v0", func(Snapshot) {})
	require.NoError(t, err)
	waitReplicaSnapshot(t, empty, nil)
	endpoint.mu.Lock()
	old := endpoint.directory
	replacement := NewDirectory()
	replacement.Set("p_1v0", 3, &qviews.QueryViewVersion{})
	endpoint.directory = replacement
	endpoint.mu.Unlock()
	old.Close()
	waitReplicaSnapshot(t, second, []int64{3})
	waitReplicaSnapshot(t, empty, nil)
	// Keep a sibling to exercise unsubscribe/re-subscribe on the same transport.
	for i := 0; i < 20; i++ {
		second.Close()
		second, err = w.subscribe("p_1v0", func(Snapshot) {})
		require.NoError(t, err)
		waitReplicaSnapshot(t, second, []int64{3})
	}
	second.Close()
	empty.Close()
	// No demand tears down the physical attempt; the logical Watch remains reusable.
	later, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	waitReplicaSnapshot(t, later, []int64{3})
	w.Close()
	snapshot := later.Snapshot()
	require.False(t, snapshot.Valid)
	select {
	case <-w.Done():
	default:
		t.Fatal("terminal state must wake consumers")
	}
	later.Close() // Closing after the parent must not close an already closed token.
	_, err = w.subscribe("p_1v0", func(Snapshot) {})
	require.Error(t, err)
}

func TestReplicaWatchCloseDuringAcquire(t *testing.T) {
	entered := make(chan struct{})
	w := newWatcher(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	sub, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	<-entered
	sub.Close()
	w.Close()
}

func TestReplicaWatchUnsubscribeWireOrdering(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := client.WatchVChannelReplicas(ctx)
	require.NoError(t, err)
	ready, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, ready.GetReady())
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0"}))
	snapshot, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, snapshot.GetSnapshot())
	endpoint.directory.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0", Unsubscribe: true}))
	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		if response.GetUnsubscribedVchannel() == "p_1v0" {
			break
		}
	}
	endpoint.directory.Set("p_1v0", 2, &qviews.QueryViewVersion{})
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0"}))
	snapshot, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, testReplicas(1, 2), replicasFromProto(snapshot.GetSnapshot().GetReplicas()))
}

func TestReplicaWatchRejectsWrongPChannelAndEOF(t *testing.T) {
	_, client := newDirectoryTransport(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := client.WatchVChannelReplicas(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "other_1v0"}))
	_, err = stream.Recv()
	require.Error(t, err)
	stream, err = client.WatchVChannelReplicas(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	w := watcherForClient(t, client)
	_, err = w.subscribe("other_1v0", func(Snapshot) {})
	require.Error(t, err)
}

func TestReplicaWatchInvalidatesSnapshotDuringReconnect(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	var attempts atomic.Int64
	reconnect := make(chan struct{})
	entered := make(chan struct{})
	w := newWatcher(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		if attempts.Add(1) > 1 {
			close(entered)
			select {
			case <-reconnect:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		stream, err := client.WatchVChannelReplicas(ctx)
		if err != nil {
			return nil, err
		}
		_, err = stream.Recv()
		if err != nil {
			return nil, err
		}
		return NewRemoteStream(stream, func() {}), nil
	})
	defer w.Close()
	sub, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	defer sub.Close()
	waitReplicaSnapshot(t, sub, nil)
	endpoint.mu.Lock()
	old := endpoint.directory
	endpoint.directory = NewDirectory()
	endpoint.directory.Set("p_1v0", 8, &qviews.QueryViewVersion{})
	endpoint.mu.Unlock()
	old.Close()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reconnect")
	}
	snapshot := sub.Snapshot()
	require.False(t, snapshot.Valid, "old empty snapshot is not authoritative after disconnect")
	close(reconnect)
	waitReplicaSnapshot(t, sub, []int64{8})
}

func TestReplicaWatchSlowReaderDoesNotBlockOwnerShutdown(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	for id := int64(1); id <= 100000; id++ {
		endpoint.directory.Set("p_1v0", id, &qviews.QueryViewVersion{})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := client.WatchVChannelReplicas(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0"}))
	require.Eventually(t, func() bool {
		endpoint.directory.mu.Lock()
		defer endpoint.directory.mu.Unlock()
		for watch := range endpoint.directory.listeners["p_1v0"] {
			if len(watch.pending) == 0 {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	// Do not receive the large snapshot: force stream-level flow control.
	endpoint.directory.Close()
	require.Eventually(t, func() bool { return endpoint.active.Load() == 0 }, time.Second, time.Millisecond)
}
