package transformlog_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func TestServerOwnerFailureEndsPhysicalStream(t *testing.T) {
	for _, failure := range []error{
		context.Canceled, context.DeadlineExceeded,
		status.NewOnShutdownError("owner closed"), status.NewChannelFenced("p"),
		status.NewChannelNotExist("p"), status.NewUnmatchedChannelTerm("p", 1, 2),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			source, factory := fixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Use the physical client directly: no resumeHandler can hide an
			// incorrectly forwarded SubscriptionError or cancel this RPC.
			stream, err := factory(ctx, "p")
			require.NoError(t, err)
			defer stream.Close()
			handlers := []*consumer{newConsumer(), newConsumer()}
			for i, vc := range []string{"p_1v0", "p_2v0"} {
				_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: vc, Handler: handlers[i]})
				require.NoError(t, err)
				through(t, handlers[i], 0)
			}
			local := <-source.opened
			require.NoError(t, local.ctx.Err())
			source.mu.Lock()
			for sub := range source.subscribers {
				if sub.opt.VChannel == "p_1v0" {
					sub.events <- wal.TransformLogStreamEvent{Err: failure}
				}
			}
			source.mu.Unlock()
			select {
			case <-stream.Done():
			case <-ctx.Done():
				t.Fatal("server did not end the PChannel RPC")
			}
			require.Equal(t, status.AsStreamingError(failure).Error(), status.AsStreamingError(stream.Error()).Error())
			for _, h := range handlers {
				require.Empty(t, h.events, "owner failure must not become a logical subscription error")
			}
			require.ErrorIs(t, local.ctx.Err(), context.Canceled)
		})
	}
}

func TestLogicalClosePreservesOtherSubscriptions(t *testing.T) {
	source, factory := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var connections atomic.Int32
	stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, p string) (wal.TransformLogStream, error) {
		connections.Add(1)
		return factory(ctx, p)
	})
	defer stream.Close()
	request, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	a, b := newConsumer(), newConsumer()
	first, err := stream.Subscribe(request, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: a})
	require.NoError(t, err)
	defer first.Close()
	second, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: b})
	require.NoError(t, err)
	defer second.Close()
	through(t, a, 0)
	through(t, b, 0)
	local := <-source.opened
	// Subscribe's context controls creation, not the returned subscription.
	cancelRequest()
	observe(source, "p_1v0", 10)
	require.Equal(t, []uint64{10}, through(t, a, 10))
	through(t, b, 10)
	require.NoError(t, first.Close())
	require.Empty(t, a.events)
	observe(source, "p_2v0", 20)
	require.Equal(t, []uint64{20}, through(t, b, 20))
	require.NoError(t, local.ctx.Err())
	require.EqualValues(t, 1, connections.Load())
	source.mu.Lock()
	remaining := len(source.subscribers)
	source.mu.Unlock()
	require.Equal(t, 1, remaining, "explicit close must release only its server reader")
}

func TestUnsubscribeWhilePChannelReconnects(t *testing.T) {
	source, factory := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reconnecting, proceed := make(chan struct{}), make(chan struct{})
	calls := 0
	stream := resumable.NewResumableStream(ctx, "p", func(ctx context.Context, p string) (wal.TransformLogStream, error) {
		calls++
		if calls == 2 {
			close(reconnecting)
			select {
			case <-proceed:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return factory(ctx, p)
	})
	defer stream.Close()
	a, b := newConsumer(), newConsumer()
	first, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: a})
	require.NoError(t, err)
	defer first.Close()
	second, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_2v0", Handler: b})
	require.NoError(t, err)
	defer second.Close()
	through(t, a, 0)
	through(t, b, 0)
	require.NoError(t, (<-source.opened).Close())
	select {
	case <-reconnecting:
	case <-ctx.Done():
		t.Fatal("PChannel did not reconnect")
	}
	require.NoError(t, first.Close())
	close(proceed)
	observe(source, "p_2v0", 20)
	require.Equal(t, []uint64{20}, through(t, b, 20))
	source.mu.Lock()
	channels := make([]string, 0, len(source.subscribers))
	for sub := range source.subscribers {
		channels = append(channels, sub.opt.VChannel)
	}
	source.mu.Unlock()
	require.Equal(t, []string{"p_2v0"}, channels, "closed logical subscription must not resurrect")
	require.Empty(t, a.events)
}

func TestTerminalPChannelFailureIsNotRetried(t *testing.T) {
	source, factory := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := resumable.NewResumableStream(ctx, "p", factory)
	defer stream.Close()
	handlers := []*consumer{newConsumer(), newConsumer()}
	for i, vc := range []string{"p_1v0", "p_2v0"} {
		_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: vc, Handler: handlers[i]})
		require.NoError(t, err)
		through(t, handlers[i], 0)
	}
	failure := status.NewUnrecoverableError("PChannel data is corrupt")
	local := <-source.opened
	local.mu.Lock()
	local.err = failure
	local.cancel()
	local.mu.Unlock()
	select {
	case <-stream.Done():
	case <-ctx.Done():
		t.Fatal("terminal PChannel error was retried")
	}
	require.Equal(t, failure.Error(), status.AsStreamingError(stream.Error()).Error())
	for _, h := range handlers {
		require.Equal(t, failure.Error(), status.AsStreamingError(next(t, h).Err).Error())
	}
	require.Empty(t, source.opened)
	_ = stream.Close()
}
