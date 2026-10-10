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
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func requireLogicalStreamAlive(t *testing.T, stream wal.TransformLogStream) {
	t.Helper()
	select {
	case <-stream.Done():
		t.Fatalf("logical stream ended: %v", stream.Error())
	default:
	}
}

func TestLogicalStreamCreatesPhysicalConnectionsOnDemand(t *testing.T) {
	source, factory := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var connections atomic.Int32
	stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, p string) (wal.TransformLogStream, error) {
		connections.Add(1)
		return factory(ctx, p)
	})
	defer stream.Close()
	require.Never(t, func() bool { return connections.Load() != 0 }, 50*time.Millisecond, time.Millisecond)
	for i := 1; i <= 3; i++ {
		h := newConsumer()
		sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: h})
		require.NoError(t, err)
		through(t, h, 0)
		local := <-source.opened
		require.NoError(t, sub.Close())
		select {
		case <-local.ctx.Done():
		case <-ctx.Done():
			t.Fatal("last unsubscribe did not release physical stream")
		}
		require.Never(t, func() bool { return connections.Load() != int32(i) }, 50*time.Millisecond, time.Millisecond)
		requireLogicalStreamAlive(t, stream)
	}
	require.NoError(t, stream.Close())
	select {
	case <-stream.Done():
	default:
		t.Fatal("owner close did not end idle logical stream")
	}
	_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: newConsumer()})
	require.Error(t, err)
}

func TestNewSubscriptionAfterCancelingPendingCreation(t *testing.T) {
	_, factory := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, stopped, proceed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var connections atomic.Int32
	stream := resumable.NewResumableStream(ctx, "p", func(attempt context.Context, p string) (wal.TransformLogStream, error) {
		if connections.Add(1) == 1 {
			close(entered)
			<-attempt.Done()
			close(stopped)
			select {
			case <-proceed:
			case <-ctx.Done():
			}
			return nil, attempt.Err()
		}
		return factory(attempt, p)
	})
	defer stream.Close()
	request, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	first := make(chan error, 1)
	go func() {
		_, err := stream.Subscribe(request, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: newConsumer()})
		first <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("missing connection attempt")
	}
	cancelRequest()
	require.ErrorIs(t, <-first, context.Canceled)
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("idle did not cancel pending physical creation")
	}
	requireLogicalStreamAlive(t, stream)
	// A new subscription can use the same logical stream after cancellation.
	h := newConsumer()
	second := make(chan error, 1)
	go func() {
		_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: h})
		second <- err
	}()
	close(proceed)
	require.NoError(t, <-second)
	through(t, h, 0)
	require.EqualValues(t, 2, connections.Load())
	requireLogicalStreamAlive(t, stream)
}

func TestBufferReplacesOnlyTerminatedLogicalStream(t *testing.T) {
	for _, failAcquire := range []bool{false, true} {
		t.Run(map[bool]string{false: "new stream", true: "new acquire fails"}[failAcquire], func(t *testing.T) {
			source, factory := fixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			failure := status.NewUnrecoverableError("old stream terminated")
			var streams []wal.TransformLogStream
			var parents []context.Context
			calls := 0
			patch(t, mockey.Mock((*streamManager).AcquireStream).To(func(_ *streamManager, parent context.Context, p string) (wal.TransformLogStream, error) {
				calls++
				if failAcquire && calls == 2 {
					return nil, failure
				}
				stream := resumable.NewResumableStream(parent, p, factory)
				streams = append(streams, stream)
				parents = append(parents, parent)
				return stream, nil
			}).Build())
			buffer := transformlogbuffer.New(t.Context(), &streamManager{})
			view := func(vc string) *qviews.QueryViewAtQueryNode {
				return qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: vc}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
			}
			old, err := buffer.Acquire(ctx, view("p_1v0"))
			require.NoError(t, err)
			defer old.Release()
			local := <-source.opened
			local.mu.Lock()
			local.err = failure
			local.cancel()
			local.mu.Unlock()
			select {
			case <-streams[0].Done():
			case <-ctx.Done():
				t.Fatal("logical stream did not terminate")
			}
			require.Error(t, old.WaitTransformVisible(ctx, 10))
			if failAcquire {
				_, err := buffer.Acquire(ctx, view("p_2v0"))
				require.ErrorIs(t, err, failure, "new Acquire must preserve its own failure")
			}
			current, err := buffer.Acquire(ctx, view("p_2v0"))
			require.NoError(t, err)
			defer current.Release()
			require.Len(t, streams, 2)
			// A new stream does not heal the old vchannel's terminal error.
			_, err = buffer.Acquire(ctx, view("p_1v0"))
			require.Error(t, err)
			old.Release()
			require.ErrorIs(t, parents[0].Err(), context.Canceled, "old release must close its own stream state")
			require.NoError(t, parents[1].Err(), "old release must not cancel the replacement")
			requireLogicalStreamAlive(t, streams[1])
			// Recreate the old vchannel on the new stream, then release its old
			// guard again: that guard must never detach the replacement.
			next, err := buffer.Acquire(ctx, view("p_1v0"))
			require.NoError(t, err)
			defer next.Release()
			old.Release()
			observe(source, "p_2v0", 20)
			require.NoError(t, current.WaitTransformVisible(ctx, 20))
			require.NoError(t, next.WaitTransformVisible(ctx, 20))
			require.Len(t, streams, 2, "old release must not evict the current cached stream")
			current.Release()
			requireLogicalStreamAlive(t, streams[1])
			next.Release()
			select {
			case <-streams[1].Done():
			case <-ctx.Done():
				t.Fatal("last current reference did not close its logical stream")
			}
		})
	}
}
