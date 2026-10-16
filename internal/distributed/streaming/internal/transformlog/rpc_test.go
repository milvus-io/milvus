package transformlog_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func rpcClient(t *testing.T, server streamingpb.StreamingNodeHandlerServiceServer) streamingpb.StreamingNodeHandlerServiceClient {
	t.Helper()
	listener := bufconn.Listen(64 * 1024)
	rpc := grpc.NewServer()
	streamingpb.RegisterStreamingNodeHandlerServiceServer(rpc, server)
	go func() { _ = rpc.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///transform-rpc",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(interceptor.NewStreamingServiceStreamClientInterceptor()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { rpc.Stop(); _ = conn.Close(); _ = listener.Close() })
	return streamingpb.NewStreamingNodeHandlerServiceClient(conn)
}

func rpcOptions(term int64) *remote.EventStreamOptions {
	return &remote.EventStreamOptions{Assignment: &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "p", Term: term},
		Node:    types.StreamingNodeInfo{ServerID: term},
	}}
}

func observeDelete(m *walsummary.Manager, tick uint64) {
	msg := message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			CollectionID: 1, PartitionID: 1,
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(tick)}}}},
			Timestamps:  []uint64{tick},
		}).MustBuildMutable().WithTimeTick(tick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(tick))).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick + 1)))
	m.ObserveMessage(context.Background(), msg)
}

// Only WAL lookup/ownership is injected. Metadata, service dispatch, protobuf,
// gRPC, Summary reads, event delivery and reconnect are the production path.
func summaryRPC(t *testing.T) (streamingpb.StreamingNodeHandlerServiceClient, *walsummary.Manager, <-chan wal.TransformLogStream) {
	t.Helper()
	manager := walsummary.NewManager(walsummary.ManagerConfig{})
	observeDelete(manager, 10)
	observeDelete(manager, 11)
	manager.ObserveQueryBarrier("p_1v0", 20)
	accesser := wal.NewTransformLogErrorAccesser(nil)
	acquired := make(chan wal.TransformLogStream, 8)
	acquire := mockey.Mock(mockey.GetMethod(accesser, "AcquireStream")).To(func(_ context.Context, channel string) (wal.TransformLogStream, error) {
		require.Equal(t, "p", channel)
		s := walsummary.NewStream(manager)
		acquired <- s
		return s, nil
	}).Build()
	t.Cleanup(func() { acquire.UnPatch() })
	getWAL := mockey.Mock((*mock_walmanager.MockManager).GetAvailableWAL).Return(&mock_wal.MockWAL{}, nil).Build()
	t.Cleanup(func() { getWAL.UnPatch() })
	getLog := mockey.Mock((*mock_wal.MockWAL).TransformLog).Return(accesser).Build()
	t.Cleanup(func() { getLog.UnPatch() })
	client := rpcClient(t, service.NewHandlerService(&mock_walmanager.MockManager{}))
	return client, manager, acquired
}

func TestTransformRPCResumesAcceptedEntryAndSyncUp(t *testing.T) {
	client, manager, acquired := summaryRPC(t)
	var attempts atomic.Int64
	stream := resumable.NewResumableStream(t.Context(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		return remote.CreateEventStream(ctx, rpcOptions(attempts.Add(1)), client)
	})
	defer stream.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	h := newRecordingHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", StartAfterTimeTick: 10, Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.EqualValues(t, 11, recvStreamEvent(t, h.events).Entry.GetTimeTick())
	require.EqualValues(t, 20, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	// Closing the owner sends cancellation before the RPC itself ends.
	first := <-acquired
	require.NoError(t, first.Close())
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("owner cancellation did not reconnect")
	}
	// No old Delete is redelivered after the accepted empty interval.
	require.EqualValues(t, 20, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	observeDelete(manager, 21)
	require.EqualValues(t, 21, recvStreamEvent(t, h.events).Entry.GetTimeTick())
	require.EqualValues(t, 21, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	manager.ObserveQueryBarrier("p_1v0", 30)
	require.EqualValues(t, 30, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	require.NoError(t, sub.Close())
	require.NoError(t, stream.Close())
	require.NoError(t, ctx.Err(), "RPC cancellation must not cancel the caller")

	// The non-resumable client must observe a successful close acknowledgement,
	// not the cancellation used internally to stop the Summary worker.
	direct, err := remote.CreateEventStream(ctx, rpcOptions(3), client)
	require.NoError(t, err)
	defer direct.Close()
	directHandler := newRecordingHandler()
	directSub, err := direct.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", StartAfterTimeTick: 30, Handler: directHandler})
	require.NoError(t, err)
	require.EqualValues(t, 30, recvStreamEvent(t, directHandler.events).SyncUp.TimeTick)
	require.NoError(t, directSub.Close())
	require.NoError(t, direct.Close())
}

func TestTransformRPCUnsupportedPeerIsTerminal(t *testing.T) {
	client := rpcClient(t, &streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
	var attempts atomic.Int32
	stream := resumable.NewResumableStream(t.Context(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		attempts.Add(1)
		return remote.CreateEventStream(ctx, rpcOptions(1), client)
	})
	defer stream.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newRecordingHandler()})
	require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))
	require.EqualValues(t, 1, attempts.Load())
	require.NoError(t, ctx.Err())
}

type recordingHandler struct {
	events chan wal.TransformLogStreamEvent
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{events: make(chan wal.TransformLogStreamEvent, 16)}
}

func (h *recordingHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.events <- event
	return nil
}
func (h *recordingHandler) Close() {}
func recvStreamEvent(t *testing.T, events <-chan wal.TransformLogStreamEvent) wal.TransformLogStreamEvent {
	t.Helper()
	select {
	case event := <-events:
		require.NoError(t, event.Err)
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for transform event")
		return wal.TransformLogStreamEvent{}
	}
}

func TestTransformRPCBoundedReplayAndInvalidOptions(t *testing.T) {
	client, _, _ := summaryRPC(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := remote.CreateEventStream(ctx, rpcOptions(1), client)
	require.NoError(t, err)
	defer stream.Close()
	h := newRecordingHandler()
	for _, opt := range []wal.TransformLogSubscriptionOption{
		{VChannel: "p_1v0"}, {Handler: h}, {VChannel: "p_1v0", StartAfterTimeTick: 20, EndTimeTick: 10, Handler: h},
	} {
		_, err := stream.Subscribe(ctx, opt)
		require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
	}
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", StartAfterTimeTick: 9, EndTimeTick: 11, Handler: h})
	require.NoError(t, err)
	require.Equal(t, "p_1v0", sub.VChannel())
	require.EqualValues(t, 10, recvStreamEvent(t, h.events).Entry.GetTimeTick())
	require.EqualValues(t, 11, recvStreamEvent(t, h.events).Entry.GetTimeTick())
	require.EqualValues(t, 11, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	require.NoError(t, sub.Close())
	require.NoError(t, stream.Close())
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	require.Error(t, err)
}

func TestTransformRPCRejectsMalformedRequests(t *testing.T) {
	client, _, _ := summaryRPC(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	missing, err := client.SubscribeTransform(ctx)
	require.NoError(t, err)
	_, err = missing.Recv()
	require.Error(t, err)
	ctx = contextutil.WithCreateTransformStream(ctx, &streamingpb.CreateTransformStreamRequest{Pchannel: types.NewProtoFromPChannelInfo(rpcOptions(1).Assignment.Channel)})
	raw, err := client.SubscribeTransform(ctx)
	require.NoError(t, err)
	// Bad subscription input is scoped to that subscription, preserving the RPC.
	require.NoError(t, raw.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{Create: &streamingpb.CreateTransformSubscriptionRequest{SubscriptionId: 1}}}))
	resp, err := raw.Recv()
	require.NoError(t, err)
	require.NotNil(t, resp.GetSubscriptionError())
	require.NoError(t, raw.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_CloseSubscription{CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: 99}}}))
	resp, err = raw.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 99, resp.GetCloseSubscription().GetSubscriptionId())
	require.NoError(t, raw.Send(&streamingpb.TransformRequest{}))
	_, err = raw.Recv()
	require.Error(t, err)
}

func TestResumableFactoryTerminalErrorAndClosedStream(t *testing.T) {
	stream := resumable.NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		return nil, streamingstatus.NewInvalidArgument("invalid channel")
	})
	h := newRecordingHandler()
	_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{Handler: h})
	require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
	<-stream.Done()
	_, err = stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	require.True(t, streamingstatus.AsStreamingError(err).IsInvalidArgument())
	require.Error(t, stream.Close())
}
