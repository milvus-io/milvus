package transformlog

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func TestResumableStreamRetriesAllSubscribeErrors(t *testing.T) {
	for _, err := range []error{context.Canceled, status.AsStreamingError(context.Canceled), wal.ErrTransformLogVChannelUnavailable, status.NewUnknownError("read failed"), status.NewInvalidArgument("rejected")} {
		t.Run(err.Error(), func(t *testing.T) {
			first := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
			second := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
			defer first.Close()
			defer second.Close()
			var original func(*walsummary.Stream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
			patch := mockey.Mock((*walsummary.Stream).Subscribe).Origin(&original).To(func(s *walsummary.Stream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				if s == first {
					return nil, err
				}
				return original(s, ctx, opt)
			}).Build()
			defer patch.UnPatch()
			calls := 0
			stream := NewResumableStream(context.Background(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
				calls++
				if calls == 1 {
					return first, nil
				}
				return second, nil
			})
			defer stream.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			handler := newRecordingHandler()
			sub, e := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
			require.NoError(t, e)
			defer sub.Close()
			event := recvStreamEvent(t, handler.events)
			require.NotNil(t, event.SyncUp)
			select {
			case <-first.Done():
			default:
				t.Fatal("failed attempt was not closed")
			}
		})
	}
}

func TestResumableStreamClosesFailedConnectionAttempt(t *testing.T) {
	attempts := make(chan context.Context, 4)
	second := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
	calls := 0
	stream := NewResumableStream(context.Background(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		calls++
		attempts <- ctx
		if calls == 1 {
			return nil, status.NewOnShutdownError("owner is migrating")
		}
		return second, nil
	})
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler := newRecordingHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	firstCtx := <-attempts
	require.ErrorIs(t, firstCtx.Err(), context.Canceled)
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	require.NoError(t, stream.Close())
	require.ErrorIs(t, (<-attempts).Err(), context.Canceled)
	select {
	case <-stream.Done():
	default:
		t.Fatal("explicit close did not finish")
	}
}

func TestResumableStreamResetsBackoffAfterAcceptedProgress(t *testing.T) {
	var resets atomic.Int32
	var original func(*backoff.ExponentialBackOff)
	patch := mockey.Mock((*backoff.ExponentialBackOff).Reset).Origin(&original).To(func(b *backoff.ExponentialBackOff) {
		resets.Add(1)
		original(b)
	}).Build()
	defer patch.UnPatch()
	owners := make(chan *walsummary.Stream, 4)
	var attempts atomic.Int32
	stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		if attempts.Add(1) <= 2 {
			return nil, status.NewOnShutdownError("owner migrating")
		}
		owner := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
		owners <- owner
		return owner, nil
	})
	defer stream.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handler := newRecordingHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	before := resets.Load()
	require.NoError(t, (<-owners).Close())
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	require.Equal(t, before+1, resets.Load(), "successful delivery starts a fresh reconnect backoff")
}
