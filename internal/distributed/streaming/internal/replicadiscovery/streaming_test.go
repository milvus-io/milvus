package replicadiscovery

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/replicadiscovery"
	serveradapter "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	domain "github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// A real gRPC test endpoint with replaceable WAL ownership, not a mocked stream.
type directoryServer struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	mu        sync.Mutex
	directory *Directory
	active    atomic.Int64
}

func (s *directoryServer) SubscribeReplicas(stream streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
	s.active.Add(1)
	defer s.active.Add(-1)
	s.mu.Lock()
	watch := s.directory.AcquireStream(stream.Context(), "p", nil)
	s.mu.Unlock()
	defer watch.Close()
	err := serveradapter.ServeStream(stream, "p", watch)
	if err != nil {
		return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	}
	return nil
}

func newDirectoryTransport(t *testing.T, options ...grpc.ServerOption) (*directoryServer, streamingpb.StreamingNodeHandlerServiceClient) {
	t.Helper()
	endpoint := &directoryServer{directory: NewDirectory()}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(options...)
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, endpoint)
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
	return endpoint, streamingpb.NewStreamingNodeHandlerServiceClient(conn)
}

func resumableForClient(t *testing.T, client streamingpb.StreamingNodeHandlerServiceClient) *resumableStream {
	t.Helper()
	w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		return openRemote(ctx, client)
	})
	t.Cleanup(w.Close)
	return w
}

func waitReplicaSnapshot(t *testing.T, sub domain.Subscription, want []int64) {
	t.Helper()
	require.Eventually(t, func() bool { s := sub.Snapshot(); return s.Valid && slices.Equal(s.Replicas, testReplicas(want...)) }, 5*time.Second, time.Millisecond)
}

func TestReplicaWatchReconnectAndSharedSubscriptions(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	endpoint.directory.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	w := resumableForClient(t, client)
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
	w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
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
	stream, err := client.SubscribeReplicas(ctx)
	require.NoError(t, err)
	ready, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, ready.GetCreateStream() != nil)
	require.NoError(t, stream.Send(createRequest(1, "p_1v0")))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, created.GetCreate())
	snapshot, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, snapshot.GetSnapshot())
	endpoint.directory.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	require.NoError(t, stream.Send(closeRequest(1)))
	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		if response.GetCloseSubscription().GetSubscriptionId() == 1 {
			break
		}
	}
	endpoint.directory.Set("p_1v0", 2, &qviews.QueryViewVersion{})
	require.NoError(t, stream.Send(createRequest(2, "p_1v0")))
	created, err = stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 2, created.GetCreate().GetSubscriptionId())
	snapshot, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, testReplicas(1, 2), replicasFromProto(snapshot.GetSnapshot().GetSnapshot().GetReplicas()))
}

func TestReplicaWatchRejectsWrongPChannelAndEOF(t *testing.T) {
	_, client := newDirectoryTransport(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := client.SubscribeReplicas(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(createRequest(1, "other_1v0")))
	_, err = stream.Recv()
	require.Error(t, err)
	stream, err = client.SubscribeReplicas(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	w := resumableForClient(t, client)
	_, err = w.subscribe("other_1v0", func(Snapshot) {})
	require.Error(t, err)
}

func TestReplicaWatchInvalidatesSnapshotDuringReconnect(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	var attempts atomic.Int64
	reconnect := make(chan struct{})
	entered := make(chan struct{})
	w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		if attempts.Add(1) > 1 {
			close(entered)
			select {
			case <-reconnect:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return openRemote(ctx, client)
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

// snapshotBlockedStream lets Mockey inject flow control while retaining the real
// gRPC context, which is canceled only after the RPC handler returns.
type snapshotBlockedStream struct {
	grpc.ServerStream
}

func (s *snapshotBlockedStream) SendMsg(message any) error {
	return s.ServerStream.SendMsg(message)
}

func TestReplicaWatchSlowReaderDoesNotBlockOwnerShutdown(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	patch := mockey.Mock((*snapshotBlockedStream).SendMsg).To(func(s *snapshotBlockedStream, message any) error {
		if response, ok := message.(*streamingpb.ReplicaWatchResponse); ok && response.GetSnapshot() != nil {
			close(entered)
			<-s.Context().Done()
			defer close(exited)
			return s.Context().Err()
		}
		return s.ServerStream.SendMsg(message)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	endpoint, client := newDirectoryTransport(t, grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &snapshotBlockedStream{ServerStream: stream})
	}))
	endpoint.directory.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream, err := client.SubscribeReplicas(ctx)
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(createRequest(1, "p_1v0")))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot send did not start")
	}
	// The snapshot sender cannot progress until gRPC cancels its context.
	// Closing the directory must return the handler without joining that sender.
	endpoint.directory.Close()
	require.Eventually(t, func() bool { return endpoint.active.Load() == 0 }, time.Second, time.Millisecond)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("handler return did not release the blocked sender")
	}
}

func TestReplicaWatchRemoteBulkSubscribe(t *testing.T) {
	_, client := newDirectoryTransport(t)
	gate := make(chan struct{})
	w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return openRemote(ctx, client)
	})
	defer w.Close()
	var ready atomic.Int64
	const count = 5000
	for i := 0; i < count; i++ {
		_, err := w.subscribe(fmt.Sprintf("p_%s%dv0", strings.Repeat("1", 64), i), func(s Snapshot) {
			if s.Valid {
				ready.Add(1)
			}
		})
		require.NoError(t, err)
	}
	close(gate)
	require.Eventually(t, func() bool { return ready.Load() == count }, 10*time.Second, 10*time.Millisecond,
		"all remote subscriptions must receive initial snapshots while sends are still draining")
}

type Directory = domain.Directory

var (
	NewDirectory      = domain.NewDirectory
	replicasFromProto = domain.ReplicasFromProto
)

func openRemote(ctx context.Context, client streamingpb.StreamingNodeHandlerServiceClient) (Stream, error) {
	return remote.CreateEventStream(ctx, &remote.EventStreamOptions{Assignment: &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1}}}, client)
}

func createRequest(id int64, vc string) *streamingpb.ReplicaWatchRequest {
	return &streamingpb.ReplicaWatchRequest{Request: &streamingpb.ReplicaWatchRequest_Create{Create: &streamingpb.CreateReplicaWatchSubscriptionRequest{SubscriptionId: id, Vchannel: vc}}}
}

func closeRequest(id int64) *streamingpb.ReplicaWatchRequest {
	return &streamingpb.ReplicaWatchRequest{Request: &streamingpb.ReplicaWatchRequest_CloseSubscription{CloseSubscription: &streamingpb.CloseReplicaWatchSubscriptionRequest{SubscriptionId: id}}}
}

func testReplicas(ids ...int64) []Replica {
	rs := make([]Replica, len(ids))
	for i, id := range ids {
		rs[i].ReplicaID = id
	}
	return rs
}

func testUpVersion(up bool) *qviews.QueryViewVersion {
	if up {
		return &qviews.QueryViewVersion{}
	}
	return nil
}

func TestReplicaWatchLocalRemoteLocal(t *testing.T) {
	endpoint, client := newDirectoryTransport(t)
	local1, local2 := NewDirectory(), NewDirectory()
	defer local1.Close()
	defer local2.Close()
	local1.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	endpoint.directory.Set("p_1v0", 2, &qviews.QueryViewVersion{})
	local2.Set("p_1v0", 3, &qviews.QueryViewVersion{})
	var attempts atomic.Int64
	w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		switch attempts.Add(1) {
		case 1:
			return local1.AcquireStream(ctx, "p", nil), nil
		case 2:
			return openRemote(ctx, client)
		default:
			return local2.AcquireStream(ctx, "p", nil), nil
		}
	})
	defer w.Close()
	sub, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	waitReplicaSnapshot(t, sub, []int64{1})
	require.Zero(t, endpoint.active.Load())
	local1.Close()
	waitReplicaSnapshot(t, sub, []int64{2})
	require.EqualValues(t, 1, endpoint.active.Load())
	endpoint.directory.Close()
	waitReplicaSnapshot(t, sub, []int64{3})
	require.EqualValues(t, 3, attempts.Load())
}

func TestReplicaWatchLastCloseDuringAcquireThenResubscribe(t *testing.T) {
	d := NewDirectory()
	defer d.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int64
	w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
		if attempts.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			<-release
			return nil, ctx.Err()
		}
		return d.AcquireStream(ctx, "p", nil), nil
	})
	defer w.Close()
	old, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	<-entered
	old.Close()
	next, err := w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	close(release)
	waitReplicaSnapshot(t, next, nil)
	require.EqualValues(t, 2, attempts.Load())
	// An established stream is reusable immediately after the last subscriber leaves.
	next.Close()
	next, err = w.subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	waitReplicaSnapshot(t, next, nil)
	require.EqualValues(t, 2, attempts.Load())
}

func TestReplicaWatchIdleRetirementDoesNotBackoff(t *testing.T) {
	mockey.PatchConvey("idle retirement is not a failed attempt", t, func() {
		var reset func(*time.Timer, time.Duration) bool
		mockey.Mock((*time.Timer).Reset).Origin(&reset).To(func(timer *time.Timer, duration time.Duration) bool {
			if duration == idleStreamTimeout {
				duration = time.Millisecond
			}
			return reset(timer, duration)
		}).Build()
		var failures atomic.Int64
		mockey.Mock((*backoff.ExponentialBackOff).NextBackOff).To(func(*backoff.ExponentialBackOff) time.Duration {
			failures.Add(1)
			return time.Hour
		}).Build()
		d := NewDirectory()
		defer d.Close()
		streams := make(chan Stream, 2)
		w := newResumableStream(t.Context(), "p", func(ctx context.Context) (Stream, error) {
			stream := d.AcquireStream(ctx, "p", nil)
			streams <- stream
			return stream, nil
		})
		defer w.Close()
		sub, err := w.subscribe("p_1v0", func(Snapshot) {})
		require.NoError(t, err)
		waitReplicaSnapshot(t, sub, nil)
		first := <-streams
		require.NoError(t, sub.Close())
		select {
		case <-first.Done():
		case <-time.After(time.Second):
			t.Fatal("idle physical stream did not retire")
		}
		next, err := w.subscribe("p_1v0", func(Snapshot) {})
		require.NoError(t, err)
		waitReplicaSnapshot(t, next, nil)
		require.Zero(t, failures.Load(), "idle retirement must not consume failure backoff")
		require.NotSame(t, first, <-streams)
	})
}
