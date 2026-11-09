package transformlog

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

func TestResumableStreamIdleLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int32
		owners := make(chan *fakeTransformLogStream, 4)
		stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
			attempts.Add(1)
			owner := newFakeTransformLogStream()
			owners <- owner
			return owner, nil
		})
		defer stream.Close()
		time.Sleep(2 * idleStreamTimeout)
		require.Zero(t, attempts.Load(), "no demand must mean no acquisition")
		subscribe := func() wal.TransformLogSubscription {
			sub, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: newRecordingHandler()})
			require.NoError(t, err)
			return sub
		}
		first := subscribe()
		owner := <-owners
		time.Sleep(2 * idleStreamTimeout)
		requireStreamOpen(t, owner.Done(), "quiet active subscriptions must keep the stream alive")
		require.NoError(t, first.Close())
		time.Sleep(idleStreamTimeout / 2)
		second := subscribe()
		require.EqualValues(t, 1, attempts.Load(), "reuse during the grace period")
		time.Sleep(idleStreamTimeout)
		requireStreamOpen(t, owner.Done(), "new subscription must stop idle retirement")
		require.NoError(t, second.Close())
		time.Sleep(idleStreamTimeout / 2)
		requireStreamOpen(t, owner.Done(), "each idle period gets a fresh grace period")
		time.Sleep(idleStreamTimeout)
		select {
		case <-owner.Done():
		default:
			t.Fatal("idle underlying stream was not retired")
		}
		time.Sleep(2 * idleStreamTimeout)
		require.EqualValues(t, 1, attempts.Load(), "retirement must not reacquire an empty stream")
		requireStreamOpen(t, stream.Done(), "logical stream remains reusable")
		third := subscribe()
		require.EqualValues(t, 2, attempts.Load())
		last := <-owners
		require.NoError(t, third.Close())
		require.NoError(t, stream.Close())
		synctest.Wait()
		select {
		case <-last.Done():
		default:
			t.Fatal("explicit Close must release the stream without an idle wait")
		}
	})
}

func TestResumableStreamCancelsAcquisitionWithoutSubscribers(t *testing.T) {
	entered := make(chan context.Context, 2)
	stream := NewResumableStream(t.Context(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		entered <- ctx
		<-ctx.Done()
		// A failed concrete factory can return a typed nil through the interface.
		return (*fakeTransformLogStream)(nil), ctx.Err()
	})
	defer stream.Close()
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: newRecordingHandler()})
		result <- err
	}()
	acquireCtx := receiveSubscribeClose(t, entered)
	cancel()
	require.ErrorIs(t, receiveSubscribeClose(t, result), context.Canceled)
	receiveSubscribeClose(t, acquireCtx.Done())
	requireStreamOpen(t, stream.Done(), "canceling one acquisition must not end the logical stream")

	go func() {
		_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "next", Handler: newRecordingHandler()})
		result <- err
	}()
	nextCtx := receiveSubscribeClose(t, entered)
	require.NoError(t, nextCtx.Err(), "new demand gets a fresh acquisition context")
	require.NoError(t, stream.Close())
	require.Error(t, receiveSubscribeClose(t, result))
}

func requireStreamOpen(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(message)
	default:
	}
}
