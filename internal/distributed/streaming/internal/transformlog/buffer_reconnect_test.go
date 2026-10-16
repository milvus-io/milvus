package transformlog_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBufferSurvivesSubscriptionCancellationBeforeDisconnect(t *testing.T) {
	for _, cancelErr := range []error{context.Canceled, status.AsStreamingError(context.Canceled)} {
		t.Run(cancelErr.Error(), func(t *testing.T) {
			options := make(chan wal.TransformLogSubscriptionOption, 4)
			var original func(*walsummary.Stream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
			patch := mockey.Mock((*walsummary.Stream).Subscribe).Origin(&original).To(func(s *walsummary.Stream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				sub, err := original(s, ctx, opt)
				options <- opt
				return sub, err
			}).Build()
			defer patch.UnPatch()
			var calls atomic.Int64
			stream := resumable.NewResumableStream(context.Background(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
				calls.Add(1)
				return walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{})), nil
			})
			defer stream.Close()
			manager := wal.NewTransformLogErrorAccesser(wal.ErrTransformLogVChannelUnavailable)
			acquire := mockey.Mock(mockey.GetMethod(manager, "AcquireStream")).Return(stream, nil).Build()
			defer acquire.UnPatch()
			buffer := transformlogbuffer.New(manager, 1)
			view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: "p_100v0"}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			guard, err := buffer.Acquire(ctx, view)
			require.NoError(t, err)
			defer guard.Release()
			first := <-options
			require.NoError(t, first.Handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 11}}))
			require.NoError(t, first.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 12}}))
			require.NoError(t, guard.WaitTransformVisible(ctx, 12))
			// The transport stays open: cancellation alone must initiate reconnection,
			// without reaching the real buffer's permanent failure path.
			require.NoError(t, first.Handler.Handle(wal.TransformLogStreamEvent{Err: cancelErr}))
			var second wal.TransformLogSubscriptionOption
			select {
			case second = <-options:
			case <-ctx.Done():
				t.Fatal("subscription error did not trigger reconnect")
			}
			require.Equal(t, uint64(12), second.StartAfterTimeTick)
			require.NoError(t, second.Handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 13}}))
			require.NoError(t, second.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 20}}))
			require.NoError(t, guard.WaitTransformVisible(ctx, 20))
			next, err := buffer.Acquire(ctx, view)
			require.NoError(t, err)
			require.NoError(t, next.WaitTransformVisible(ctx, 20))
			next.Release()
			require.Equal(t, int64(2), calls.Load())
			require.NoError(t, stream.Close())
			require.Equal(t, int64(2), calls.Load(), "explicit close must not reconnect")
		})
	}
}

func TestBufferRetriesCanceledPreparationWithoutClosingSharedStream(t *testing.T) {
	mockey.PatchConvey("a canceled preparation cannot destroy a stream retained by another View", t, func() {
		entered, resume := make(chan struct{}), make(chan struct{})
		stream := resumable.NewResumableStream(t.Context(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
			close(entered)
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
