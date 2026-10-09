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
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBufferResumesMigrationEventBeforeTransportEnds(t *testing.T) {
	for _, migration := range []error{
		context.Canceled, context.DeadlineExceeded,
		status.AsStreamingError(context.Canceled), status.AsStreamingError(context.DeadlineExceeded),
		status.NewOnShutdownError("owner closed"), status.NewChannelFenced("p"),
		status.NewChannelNotExist("p"), status.NewUnmatchedChannelTerm("p", 1, 2),
	} {
		t.Run(migration.Error(), func(t *testing.T) {
			source, factory := fixture(t)
			observe(source, "p_1v0", 10)
			observe(source, "p_2v0", 12)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var connections atomic.Int32
			stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, p string) (wal.TransformLogStream, error) {
				connections.Add(1)
				return factory(ctx, p)
			})
			defer stream.Close()
			patch(t, mockey.Mock((*streamManager).AcquireStream).Return(stream, nil).Build())
			patch(t, mockey.Mock((*segment).ID).Return(int64(1)).Build())
			patch(t, mockey.Mock((*segment).VChannel).Return("p_1v0").Build())
			patch(t, mockey.Mock((*segment).TransformStartAfterTimeTick).Return(uint64(0)).Build())
			applied := make(chan uint64, 10)
			patch(t, mockey.Mock((*segment).ApplyTransform).To(func(_ *segment, _ context.Context, entry *streamingpb.TransformLogEntry) error {
				applied <- entry.GetTimeTick()
				return nil
			}).Build())
			buffer := transformlogbuffer.New(&streamManager{}, 1)
			view := func(vc string) *qviews.QueryViewAtQueryNode {
				return qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: vc}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			}
			first, err := buffer.Acquire(ctx, view("p_1v0"))
			require.NoError(t, err)
			defer first.Release()
			second, err := buffer.Acquire(ctx, view("p_2v0"))
			require.NoError(t, err)
			defer second.Release()
			require.NoError(t, first.WaitTransformVisible(ctx, 12))
			require.NoError(t, second.WaitTransformVisible(ctx, 12))
			reg, err := buffer.RegisterSegment(ctx, &segment{})
			require.NoError(t, err)
			defer reg.Unregister()
			require.NoError(t, reg.WaitCatchup(ctx))
			require.Equal(t, uint64(10), receiveApplied(t, ctx, applied))

			// Emit only the subscription event. The SN stream is still alive,
			// so its server-side shutdown filter cannot convert this into EOF.
			local := <-source.opened
			require.NoError(t, local.ctx.Err())
			source.mu.Lock()
			for sub := range source.subscribers {
				if sub.opt.VChannel == "p_1v0" {
					sub.events <- wal.TransformLogStreamEvent{Err: migration}
				}
			}
			source.mu.Unlock()
			select {
			case <-source.opened:
			case <-ctx.Done():
				t.Fatal("migration event did not reconnect")
			}
			require.Eventually(t, func() bool {
				source.mu.Lock()
				defer source.mu.Unlock()
				if len(source.subscribers) != 2 {
					return false
				}
				for sub := range source.subscribers {
					if sub.ctx.Err() != nil || sub.opt.StartAfterTimeTick != 12 {
						return false
					}
				}
				return true
			}, time.Second, time.Millisecond, "both channels must resume from accepted SyncUp")
			observe(source, "p_1v0", 20)
			require.NoError(t, first.WaitTransformVisible(ctx, 20))
			require.NoError(t, second.WaitTransformVisible(ctx, 20))
			require.Equal(t, uint64(20), receiveApplied(t, ctx, applied))
			next, err := buffer.Acquire(ctx, view("p_1v0"))
			require.NoError(t, err, "migration must not set the buffer's permanent error")
			require.NoError(t, next.WaitTransformVisible(ctx, 20))
			next.Release()
			require.NoError(t, stream.Close())
			require.EqualValues(t, 2, connections.Load(), "explicit Close must not reconnect")
		})
	}
}

func receiveApplied(t *testing.T, ctx context.Context, applied <-chan uint64) uint64 {
	t.Helper()
	select {
	case tt := <-applied:
		return tt
	case <-ctx.Done():
		t.Fatal("missing applied Delete")
		return 0
	}
}

func TestMigrationRecoveryPreservesTerminalSubscriptions(t *testing.T) {
	for _, failure := range []error{
		status.NewUnknownError("corrupt retained chunk"), status.NewUnknownError("read failed: context canceled"),
		wal.ErrTransformLogStartPointTruncated, wal.ErrTransformLogVChannelUnavailable, wal.ErrTransformLogInvalidReadOption,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			source, factory := fixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := resumable.NewResumableStream(ctx, "p", factory)
			defer stream.Close()
			h := newConsumer()
			sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
			require.NoError(t, err)
			through(t, h, 0)
			source.mu.Lock()
			for sub := range source.subscribers {
				sub.events <- wal.TransformLogStreamEvent{Err: failure}
			}
			source.mu.Unlock()
			require.Equal(t, failure.Error(), next(t, h).Err.Error())
			select {
			case <-h.done:
			case <-ctx.Done():
				t.Fatal("semantic error did not close handler")
			}
			require.Error(t, sub.Close())
			select {
			case <-stream.Done():
			case <-ctx.Done():
				t.Fatal("terminal subscription was retried")
			}
			require.Len(t, source.opened, 1)
		})
	}
}

func TestMigrationRecoveryDoesNotRetryRejectedConsumer(t *testing.T) {
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded, status.NewUnknownError("rejected entry")} {
		t.Run(failure.Error(), func(t *testing.T) {
			source, factory := fixture(t)
			observe(source, "p_1v0", 10)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := resumable.NewResumableStream(ctx, "p", factory)
			defer stream.Close()
			h := newConsumer()
			h.fail = failure
			sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
			if err != nil {
				require.ErrorIs(t, err, failure)
			}
			select {
			case <-h.done:
			case <-ctx.Done():
				t.Fatal("rejected consumer was not closed")
			}
			if sub != nil {
				require.ErrorIs(t, sub.Close(), failure)
			}
			select {
			case <-stream.Done():
			case <-ctx.Done():
				t.Fatal("rejected consumer was retried")
			}
			require.Len(t, source.opened, 1)
		})
	}
}

func TestMigrationAttemptContextLifetime(t *testing.T) {
	source, factory := fixture(t)
	observe(source, "p_1v0", 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts := make(chan context.Context, 3)
	calls := 0
	stream := resumable.NewResumableStream(ctx, "p", func(attempt context.Context, p string) (wal.TransformLogStream, error) {
		attempts <- attempt
		calls++
		if calls == 1 {
			return nil, status.NewOnShutdownError("owner migrating")
		}
		return factory(attempt, p)
	})
	defer stream.Close()
	h := newConsumer()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, []uint64{10}, through(t, h, 10))
	require.ErrorIs(t, (<-attempts).Err(), context.Canceled)
	active := <-attempts
	require.NoError(t, active.Err())
	require.NoError(t, ctx.Err())
	require.NoError(t, stream.Close())
	require.ErrorIs(t, active.Err(), context.Canceled)
	require.NoError(t, ctx.Err(), "attempt cancellation must not cancel its parent")
}
