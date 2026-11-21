package transformlog_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type observedAttempt struct {
	wal.TransformLogStream
	options chan<- wal.TransformLogSubscriptionOption
	failed  chan struct{}
	cause   error
}

func (s *observedAttempt) Done() <-chan struct{} {
	if s.failed != nil {
		return s.failed
	}
	return s.TransformLogStream.Done()
}

func (s *observedAttempt) Error() error {
	select {
	case <-s.failed:
		return s.cause
	default:
		return s.TransformLogStream.Error()
	}
}

func (s *observedAttempt) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	sub, err := s.TransformLogStream.Subscribe(ctx, opt)
	s.options <- opt
	return sub, err
}

func TestBufferSurvivesUnderlyingStreamCancellation(t *testing.T) {
	for _, cancelErr := range []error{context.Canceled, status.AsStreamingError(context.Canceled)} {
		t.Run(cancelErr.Error(), func(t *testing.T) {
			options := make(chan wal.TransformLogSubscriptionOption, 4)
			failed := make(chan struct{})
			var calls atomic.Int64
			stream := resumable.NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
				attempt := &observedAttempt{TransformLogStream: walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{})), options: options}
				if calls.Add(1) == 1 {
					attempt.failed, attempt.cause = failed, cancelErr
				}
				return attempt, nil
			})
			defer stream.Close()
			buffer := transformlogbuffer.New(sharedRPCStream{stream}, 1)
			defer buffer.Close()
			view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: "p_100v0"}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			guard, err := buffer.Acquire(ctx, view)
			require.NoError(t, err)
			defer guard.Release()
			first := <-options
			first.Handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 11}})
			first.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 12}})
			require.NoError(t, guard.WaitTransformVisible(ctx, 12))
			// Physical failure is exposed through Done/Error, never a business
			// event. The real buffer must survive the replacement attempt.
			close(failed)
			var second wal.TransformLogSubscriptionOption
			select {
			case second = <-options:
			case <-ctx.Done():
				t.Fatal("subscription error did not trigger reconnect")
			}
			require.Equal(t, uint64(12), second.StartAfterTimeTick)
			first.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionError: wal.ErrTransformLogStartPointTruncated})
			second.Handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 13}})
			second.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 20}})
			require.NoError(t, guard.WaitTransformVisible(ctx, 20))
			next, err := buffer.Acquire(ctx, view)
			require.NoError(t, err)
			require.NoError(t, next.WaitTransformVisible(ctx, 20))
			next.Release()
			// An explicitly removed logical subscription may be followed by a
			// new subscription on the same channel and physical attempt.
			retired, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_101v0", StartAfterTimeTick: 0, Handler: newRecordingHandler()})
			require.NoError(t, err)
			retiredAttempt := <-options
			require.NoError(t, retired.Close())
			activeHandler := newRecordingHandler()
			active, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_101v0", StartAfterTimeTick: 0, Handler: activeHandler})
			require.NoError(t, err)
			activeAttempt := <-options
			require.Zero(t, recvStreamEvent(t, activeHandler.events).SyncUp.TimeTick)
			retiredAttempt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionError: wal.ErrTransformLogStartPointTruncated})
			activeAttempt.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 22}})
			require.EqualValues(t, 22, recvStreamEvent(t, activeHandler.events).SyncUp.TimeTick)
			require.NoError(t, active.Close())
			require.Equal(t, int64(2), calls.Load())
			require.NoError(t, stream.Close())
			require.Equal(t, int64(2), calls.Load(), "explicit close must not reconnect")
		})
	}
}

func TestBufferRetriesCanceledPreparationWithoutClosingSharedStream(t *testing.T) {
	mockey.PatchConvey("a canceled preparation cannot destroy a stream retained by another View", t, func() {
		entered, resume := make(chan struct{}), make(chan struct{})
		var enteredOnce sync.Once
		stream := resumable.NewResumableStream(t.Context(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{})), nil
		})
		defer stream.Close()
		manager := wal.NewTransformLogErrorAccesser(wal.ErrTransformLogVChannelUnavailable)
		mockey.Mock(mockey.GetMethod(manager, "AcquireStream")).Return(stream, nil).Build()
		buffer := transformlogbuffer.New(manager, 1)
		view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: "p_100v0"}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
		first, err := buffer.Bind(view)
		require.NoError(t, err)
		next, err := buffer.Bind(view)
		require.NoError(t, err)
		defer next.Release()
		ctx, cancel := context.WithCancel(t.Context())
		failed := make(chan error, 1)
		go func() { _, err := first.Prepare(ctx); failed <- err }()
		<-entered
		cancel()
		require.ErrorIs(t, <-failed, context.Canceled)
		first.Release()
		select {
		case <-stream.Done():
			t.Fatal("pending View lost its owned stream when another preparation canceled")
		case <-time.After(30 * time.Millisecond):
		}
		close(resume)
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err = next.Prepare(ctx)
		require.NoError(t, err)
	})
}

// Connect the production Summary dispatcher, gRPC transport, resumable client
// and QN buffer. The physical owner can disappear without poisoning a retained
// QueryView, and the same buffer must then receive both replay and live Deletes.
func TestDispatchRPCBufferRecoversOwnerShutdown(t *testing.T) {
	client, summary, acquired := summaryRPCWithStream(t, func(m *walsummary.Manager) wal.TransformLogStream {
		return walsummary.NewDispatchStream(m)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var attempts atomic.Int64
	stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		return remote.CreateEventStream(ctx, rpcOptions(attempts.Add(1)), client)
	})
	defer stream.Close()
	manager := &vchannel.PChannelRecoveryManager{}
	patch := mockey.Mock(mockey.GetMethod(manager, "AcquireStream")).Return(stream, nil).Build()
	defer patch.UnPatch()
	buffer := transformlogbuffer.New(manager, 1)
	view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: "p_1v0"}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
	guard, err := buffer.Acquire(ctx, view)
	require.NoError(t, err)
	defer guard.Release()
	require.NoError(t, guard.WaitTransformVisible(ctx, 20))
	var first wal.TransformLogStream
	select {
	case first = <-acquired:
	case <-ctx.Done():
		t.Fatal("initial owner was not acquired")
	}
	require.NoError(t, first.Close())
	observeDelete(summary, 21)
	summary.ObserveQueryBarrier("p_1v0", 30)
	require.NoError(t, guard.WaitTransformVisible(ctx, 30))
	observeDelete(summary, 31)
	require.NoError(t, guard.WaitTransformVisible(ctx, 31))
	require.EqualValues(t, 2, attempts.Load())
	require.NoError(t, stream.Error())
}
