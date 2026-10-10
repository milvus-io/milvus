package transformlog_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// Local SN behavior is mocked at the shared WAL interfaces. The remote server,
// gRPC client, resumption and QueryNode consumer execute their production code.
type assignedWAL struct{ wal.WAL }

func (w *assignedWAL) TransformLog() wal.TransformLogAccesser { panic("mockey") }

type assignedManager struct{ walmanager.Manager }

func (m *assignedManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) { panic("mockey") }

type consumer struct {
	events chan wal.TransformLogStreamEvent
	done   chan struct{}
	once   sync.Once
	fail   error
}

func (c *consumer) Handle(wal.TransformLogStreamEvent) error { panic("mockey") }
func (c *consumer) Close()                                   { panic("mockey") }
func patch(t *testing.T, p *mockey.Mocker)                   { t.Helper(); t.Cleanup(func() { p.UnPatch() }) }
func newConsumer() *consumer {
	return &consumer{events: make(chan wal.TransformLogStreamEvent, 100), done: make(chan struct{})}
}

func next(t *testing.T, c *consumer) wal.TransformLogStreamEvent {
	t.Helper()
	select {
	case e := <-c.events:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("missing transform event")
		return wal.TransformLogStreamEvent{}
	}
}

func through(t *testing.T, c *consumer, tt uint64) []uint64 {
	t.Helper()
	var ticks []uint64
	for {
		e := next(t, c)
		require.NoError(t, e.Err)
		if e.Entry != nil {
			ticks = append(ticks, e.Entry.GetTimeTick())
		}
		if e.SyncUp != nil && e.SyncUp.TimeTick >= tt {
			return ticks
		}
	}
}

func observe(summary *contractSource, vc string, tt uint64) {
	msg := message.NewDeleteMessageBuilderV1().WithVChannel(vc).WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).WithBody(&msgpb.DeleteRequest{CollectionID: 1, PartitionID: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(tt)}}}}, Timestamps: []uint64{tt}}).MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tt))).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt + 1)))
	summary.publish(vc, messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{}))
}

func fixture(t *testing.T) (*contractSource, resumable.StreamFactory) {
	t.Helper()
	summary, factory, _ := grpcFixture(t)
	return summary, factory
}

func grpcFixture(t *testing.T) (*contractSource, resumable.StreamFactory, streamingpb.StreamingNodeHandlerServiceClient) {
	t.Helper()
	summary := newContractSource(t)
	manager := summary
	patch(t, mockey.Mock((*assignedWAL).TransformLog).Return(manager).Build())
	patch(t, mockey.Mock((*assignedManager).GetAvailableWAL).Return(&assignedWAL{}, nil).Build())
	patch(t, mockey.Mock((*consumer).Handle).To(func(c *consumer, e wal.TransformLogStreamEvent) error { c.events <- e; return c.fail }).Build())
	patch(t, mockey.Mock((*consumer).Close).To(func(c *consumer) { c.once.Do(func() { close(c.done) }) }).Build())
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.StreamInterceptor(interceptor.NewStreamingServiceStreamServerInterceptor()))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, service.NewHandlerService(&assignedManager{}))
	go server.Serve(listener)
	t.Cleanup(func() { server.GracefulStop(); listener.Close() })
	conn, err := grpc.NewClient("passthrough:///transform", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStreamInterceptor(interceptor.NewStreamingServiceStreamClientInterceptor()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
	factory := func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		return remote.CreateEventStream(ctx, &remote.EventStreamOptions{Assignment: &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1}, Node: types.StreamingNodeInfo{ServerID: 1}}}, client)
	}
	return summary, factory, client
}

func TestGRPCMultiplexCatchupLiveAndClose(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	observe(summary, "p_2v0", 20)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := factory(ctx, "p")
	require.NoError(t, err)
	defer stream.Close()
	h1, h2 := newConsumer(), newConsumer()
	sub1, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h1})
	require.NoError(t, err)
	sub2, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: h2, EndTimeTick: 30})
	require.NoError(t, err)
	require.NotEqual(t, sub1.ID(), sub2.ID())
	require.Equal(t, "p_2v0", sub2.VChannel())
	require.Equal(t, []uint64{10}, through(t, h1, 20))
	require.Equal(t, []uint64{20}, through(t, h2, 20))
	observe(summary, "p_2v0", 30)
	require.Empty(t, through(t, h1, 30))
	require.Equal(t, []uint64{30}, through(t, h2, 30))
	select {
	case <-h2.done:
	case <-ctx.Done():
		t.Fatal("bounded subscription not closed")
	}
	require.NoError(t, sub2.Close())
	require.NoError(t, sub1.Close())
}

func TestResumptionUsesAcceptedFrontier(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acquired := make(chan wal.TransformLogStream, 10)
	stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, p string) (wal.TransformLogStream, error) {
		s, e := factory(ctx, p)
		if e == nil {
			acquired <- s
		}
		return s, e
	})
	defer stream.Close()
	h := newConsumer()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h, EndTimeTick: 30})
	require.NoError(t, err)
	require.Equal(t, []uint64{10}, through(t, h, 10))
	first := <-acquired
	require.NoError(t, first.Close())
	observe(summary, "p_1v0", 20)
	observe(summary, "p_1v0", 30)
	require.Equal(t, []uint64{20, 30}, through(t, h, 30))
	select {
	case <-h.done:
	case <-ctx.Done():
		t.Fatal("logical subscription not completed")
	}
	require.NoError(t, sub.Close())
}

func TestGRPCSubscriptionErrorIdentity(t *testing.T) {
	summary, factory := fixture(t)
	summary.floor = 100
	for _, tc := range []struct {
		vc    string
		start uint64
		want  error
	}{{"missing", 100, wal.ErrTransformLogVChannelUnavailable}, {"p_1v0", 99, wal.ErrTransformLogStartPointTruncated}} {
		t.Run(tc.vc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := resumable.NewResumableStream(ctx, "p", factory)
			defer stream.Close()
			h := newConsumer()
			sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: tc.vc, StartAfterTimeTick: tc.start, Handler: h})
			if err != nil {
				require.ErrorIs(t, err, tc.want)
			} else {
				require.ErrorIs(t, next(t, h).Err, tc.want)
				require.ErrorIs(t, sub.Close(), tc.want)
			}
		})
	}
}

type streamManager struct{ wal.TransformLogStreamManager }

func (m *streamManager) AcquireStream(context.Context, string) (wal.TransformLogStream, error) {
	panic("mockey")
}

type segment struct{ qnview.TransformSegment }

func (s *segment) ID() int64                           { panic("mockey") }
func (s *segment) VChannel() string                    { panic("mockey") }
func (s *segment) TransformStartAfterTimeTick() uint64 { panic("mockey") }
func (s *segment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	panic("mockey")
}

func TestQueryNodeBufferConsumesCatchupAndLiveDeletes(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := resumable.NewResumableStream(ctx, "p", factory)
	defer stream.Close()
	patch(t, mockey.Mock((*streamManager).AcquireStream).Return(stream, nil).Build())
	patch(t, mockey.Mock((*segment).ID).Return(int64(1)).Build())
	patch(t, mockey.Mock((*segment).VChannel).Return("p_1v0").Build())
	patch(t, mockey.Mock((*segment).TransformStartAfterTimeTick).Return(uint64(0)).Build())
	applied := make(chan uint64, 10)
	patch(t, mockey.Mock((*segment).ApplyTransform).To(func(_ *segment, _ context.Context, e *streamingpb.TransformLogEntry) error {
		applied <- e.GetTimeTick()
		return nil
	}).Build())
	buffer := transformlogbuffer.New(t.Context(), &streamManager{})
	view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: "p_1v0"}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
	guard, err := buffer.Acquire(ctx, view)
	require.NoError(t, err)
	defer guard.Release()
	reg, err := buffer.RegisterSegment(ctx, &segment{})
	require.NoError(t, err)
	defer reg.Unregister()
	caughtUp := make(chan error, 1)
	reg.Catchup(ctx, func(err error) { caughtUp <- err })
	select {
	case err := <-caughtUp:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("catchup did not complete")
	}
	select {
	case tt := <-applied:
		require.Equal(t, uint64(10), tt)
	case <-ctx.Done():
		t.Fatal("catchup delete not applied")
	}
	observe(summary, "p_1v0", 20)
	require.NoError(t, guard.WaitTransformVisible(ctx, 20))
	select {
	case tt := <-applied:
		require.Equal(t, uint64(20), tt)
	case <-ctx.Done():
		t.Fatal("live delete not applied")
	}
}

func TestResumableFactoryRetryCancellationAndValidation(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, p string) (wal.TransformLogStream, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return factory(ctx, p)
	})
	defer stream.Close()
	_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{})
	require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
	h := newConsumer()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	require.NoError(t, err)
	require.Equal(t, "p_1v0", sub.VChannel())
	require.Equal(t, []uint64{10}, through(t, h, 10))
	cancel()
	select {
	case <-stream.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("parent cancellation leaked stream")
	}
	require.ErrorIs(t, stream.Error(), context.Canceled)
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newConsumer()})
	require.Error(t, err)
}

func TestSubscriptionCancellationWhileFactoryUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = fixture(t)
	entered := make(chan struct{}, 1)
	stream := resumable.NewResumableStream(ctx, "p", func(context.Context, string) (wal.TransformLogStream, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		return nil, context.DeadlineExceeded
	})
	defer stream.Close()
	request, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	result := make(chan error, 1)
	go func() {
		_, err := stream.Subscribe(request, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newConsumer()})
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("subscription did not start a connection attempt")
	}
	cancelRequest()
	require.ErrorIs(t, <-result, context.Canceled)
	requireLogicalStreamAlive(t, stream)
}

func TestRawSubscriptionProtocolValidation(t *testing.T) {
	_, _, client := grpcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Missing assignment metadata must fail before accepting a stream.
	missing, err := client.SubscribeTransform(ctx)
	require.NoError(t, err)
	_, err = missing.Recv()
	require.Error(t, err)
	ctx = contextutil.WithCreateTransformStream(ctx, &streamingpb.CreateTransformStreamRequest{Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 1}})
	stream, err := client.SubscribeTransform(ctx)
	require.NoError(t, err)
	_, err = stream.Header()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{Create: &streamingpb.CreateTransformSubscriptionRequest{SubscriptionId: 1, Vchannel: "p_1v0", StartAfterTimeTick: 10, EndTimeTick: 5}}}))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.ErrorIs(t, contextutil.TransformLogErrorFromProto(response.GetSubscriptionError()), wal.ErrTransformLogInvalidReadOption)
	require.NoError(t, stream.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_CloseSubscription{CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: 123}}}))
	response, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(123), response.GetCloseSubscription().GetSubscriptionId())
	require.NoError(t, stream.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_CloseStream{CloseStream: &streamingpb.CloseTransformStreamRequest{}}}))
	response, err = stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, response.GetCloseStream())
	unknown, err := client.SubscribeTransform(ctx)
	require.NoError(t, err)
	_, err = unknown.Header()
	require.NoError(t, err)
	require.NoError(t, unknown.Send(&streamingpb.TransformRequest{}))
	_, err = unknown.Recv()
	require.Error(t, err)
}

func TestRemoteValidationAndConsumerFailure(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := factory(ctx, "p")
	require.NoError(t, err)
	defer stream.Close()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{})
	require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
	h := newConsumer()
	// A failed callback must end this subscription without committing its cursor.
	h.fail = context.DeadlineExceeded
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	select {
	case <-h.done:
	case <-ctx.Done():
		t.Fatal("failed consumer not closed")
	}
	if sub != nil {
		require.ErrorIs(t, sub.Close(), context.DeadlineExceeded)
	}
	require.NoError(t, stream.Close())
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newConsumer()})
	require.Error(t, err)
}

func TestResumablePermanentFactoryFailure(t *testing.T) {
	_, _ = fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failure := status.NewUnrecoverableError("unsupported WAL")
	stream := resumable.NewResumableStream(ctx, "p", func(context.Context, string) (wal.TransformLogStream, error) { return nil, failure })
	defer stream.Close()
	_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newConsumer()})
	require.ErrorIs(t, err, failure)
	select {
	case <-stream.Done():
	case <-ctx.Done():
		t.Fatal("permanent failure retried")
	}
	require.ErrorIs(t, stream.Error(), failure)
}

func TestRemoteCanceledCreationAndSyncUpFailure(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := factory(canceled, "p")
	require.Error(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := factory(ctx, "p")
	require.NoError(t, err)
	defer stream.Close()
	h := newConsumer()
	h.fail = context.DeadlineExceeded
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", StartAfterTimeTick: 10, Handler: h})
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	select {
	case <-h.done:
	case <-ctx.Done():
		t.Fatal("syncup failure not closed")
	}
	if sub != nil {
		require.ErrorIs(t, sub.Close(), context.DeadlineExceeded)
	}
}

func TestResumptionAfterSubscriptionTransportFailure(t *testing.T) {
	summary, factory := fixture(t)
	observe(summary, "p_1v0", 10)
	var original func(*remote.EventStream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
	var calls int
	patch(t, mockey.Mock((*remote.EventStream).Subscribe).Origin(&original).To(func(s *remote.EventStream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return original(s, ctx, opt)
	}).Build())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := resumable.NewResumableStream(ctx, "p", factory)
	defer stream.Close()
	h := newConsumer()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, []uint64{10}, through(t, h, 10))
}

func TestLocalProviderRejectionIsTerminalAcrossGRPC(t *testing.T) {
	source, factory := fixture(t)
	source.acquireErr = status.NewUnrecoverableError("local provider is not installed")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := resumable.NewResumableStream(ctx, "p", factory)
	defer stream.Close()
	_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newConsumer()})
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	select {
	case <-stream.Done():
	case <-ctx.Done():
		t.Fatal("unsupported provider was retried")
	}
}

func TestEmptyBoundedInterval(t *testing.T) {
	source, factory := fixture(t)
	observe(source, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := resumable.NewResumableStream(ctx, "p", factory)
	defer stream.Close()
	h := newConsumer()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", StartAfterTimeTick: 10, EndTimeTick: 10, Handler: h})
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.Empty(t, through(t, h, 10))
	require.NoError(t, sub.Close())
}

func TestWALShutdownResumesWithoutSubscriptionFailure(t *testing.T) {
	source, factory := fixture(t)
	observe(source, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := resumable.NewResumableStream(ctx, "p", factory)
	defer stream.Close()
	h := newConsumer()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, []uint64{10}, through(t, h, 10))
	local := <-source.opened
	require.NoError(t, local.Close())
	observe(source, "p_1v0", 20)
	require.Equal(t, []uint64{20}, through(t, h, 20))
}

func TestBufferSharedStreamSurvivesViewCancellation(t *testing.T) {
	for _, secondChannel := range []string{"p_1v0", "p_2v0"} {
		t.Run(secondChannel, func(t *testing.T) {
			summary, factory := fixture(t)
			observe(summary, "p_1v0", 10)
			observe(summary, secondChannel, 11)
			var acquired wal.TransformLogStream
			var streamCtx context.Context
			patch(t, mockey.Mock((*streamManager).AcquireStream).To(func(_ *streamManager, ctx context.Context, p string) (wal.TransformLogStream, error) {
				streamCtx = ctx
				acquired = resumable.NewResumableStream(ctx, p, factory)
				return acquired, nil
			}).Build())
			buffer := transformlogbuffer.New(t.Context(), &streamManager{})
			view := func(channel string) *qviews.QueryViewAtQueryNode {
				return qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: channel}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			firstCtx, cancelFirst := context.WithCancel(ctx)
			defer cancelFirst()
			first, err := buffer.Acquire(firstCtx, view("p_1v0"))
			require.NoError(t, err)
			defer first.Release()
			second, err := buffer.Acquire(ctx, view(secondChannel))
			require.NoError(t, err)
			defer second.Release()
			require.NoError(t, second.WaitTransformVisible(ctx, 11))

			// Match detachViewLocked: cancel the view before releasing its guard.
			cancelFirst()
			first.Release()
			require.NoError(t, streamCtx.Err())
			observe(summary, secondChannel, 20)
			require.NoError(t, second.WaitTransformVisible(ctx, 20))
			second.Release()
			require.ErrorIs(t, streamCtx.Err(), context.Canceled)
			select {
			case <-acquired.Done():
			case <-ctx.Done():
				t.Fatal("last guard did not close shared stream")
			}
		})
	}
}
