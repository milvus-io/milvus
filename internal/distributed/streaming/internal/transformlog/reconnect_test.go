package transformlog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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

// Keep retry waits under test control while exercising the real resume loop,
// per-subscription checkpoints and event delivery.
func TestResumableStreamBackoffSurvivesSiblingProgress(t *testing.T) {
	for _, advancing := range []bool{false, true} {
		t.Run(fmt.Sprintf("advancing=%t", advancing), func(t *testing.T) {
			b := backoff.NewExponentialBackOff()
			b.RandomizationFactor = 0
			backoffPatch := mockey.Mock(backoff.NewExponentialBackOff).Return(b).Build()
			defer backoffPatch.UnPatch()
			delays := make(chan time.Duration, 1)
			retry := make(chan struct{})
			waitPatch := mockey.Mock((*resumableStream).waitNextRetry).To(func(s *resumableStream, delay time.Duration) error {
				delays <- delay
				select {
				case <-retry:
					return nil
				case <-s.ctx.Done():
					return s.ctx.Err()
				}
			}).Build()
			defer waitPatch.UnPatch()
			owners := make(chan *fakeTransformLogStream, 1)
			stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
				owner := newFakeTransformLogStream()
				owners <- owner
				return owner, nil
			})
			defer stream.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			a, healthy := newRecordingHandler(), newRecordingHandler()
			_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "a", StartAfterTimeTick: 10, Handler: a})
			require.NoError(t, err)
			_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "b", StartAfterTimeTick: 30, Handler: healthy})
			require.NoError(t, err)
			tick := uint64(30)
			for _, expected := range []time.Duration{100 * time.Millisecond, 150 * time.Millisecond, 225 * time.Millisecond} {
				owner := receiveSubscribeClose(t, owners)
				require.Eventually(t, func() bool { return owner.subscriptionID("a") != 0 && owner.subscriptionID("b") != 0 }, time.Second, time.Millisecond)
				require.EqualValues(t, 10, owner.startAfter("a"))
				require.Equal(t, tick, owner.startAfter("b"))
				if advancing {
					tick++
					owner.emit(wal.TransformLogStreamEvent{SubscriptionID: owner.subscriptionID("b"), Entry: &streamingpb.TransformLogEntry{TimeTick: tick}})
					require.Equal(t, tick, recvStreamEvent(t, healthy.events).Entry.GetTimeTick())
				}
				owner.emit(wal.TransformLogStreamEvent{SubscriptionID: owner.subscriptionID("b"), SyncUp: &wal.TransformLogSyncUp{TimeTick: tick}})
				if advancing || expected == 100*time.Millisecond {
					require.Equal(t, tick, recvStreamEvent(t, healthy.events).SyncUp.TimeTick)
				} else {
					require.Empty(t, healthy.events, "duplicate SyncUp must be filtered")
				}
				owner.finish(walsummary.ErrStoreCorrupted)
				require.Equal(t, expected, receiveSubscribeClose(t, delays))
				if expected != 225*time.Millisecond {
					retry <- struct{}{}
				}
			}
			require.Empty(t, a.events)
			require.NoError(t, stream.Close(), "Close must interrupt failure backoff")
		})
	}
}

func TestResumableStreamResetsBackoffAfterStableAttempt(t *testing.T) {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	backoffPatch := mockey.Mock(backoff.NewExponentialBackOff).Return(b).Build()
	defer backoffPatch.UnPatch()
	delays := make(chan time.Duration, 4)
	waitPatch := mockey.Mock((*resumableStream).waitNextRetry).To(func(s *resumableStream, delay time.Duration) error {
		delays <- delay
		return s.ctx.Err()
	}).Build()
	defer waitPatch.UnPatch()
	owners := make(chan *fakeTransformLogStream, 1)
	calls := 0
	stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		calls++
		if calls <= 2 {
			return nil, status.NewOnShutdownError("owner migrating")
		}
		owner := newFakeTransformLogStream()
		owners <- owner
		return owner, nil
	})
	defer stream.Close()
	_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: newRecordingHandler()})
	require.NoError(t, err)
	require.Equal(t, 100*time.Millisecond, receiveSubscribeClose(t, delays))
	require.Equal(t, 150*time.Millisecond, receiveSubscribeClose(t, delays))
	first := receiveSubscribeClose(t, owners)
	// A quiet subscription is healthy too. No handler event is needed.
	time.Sleep(reconnectBackoffResetInterval + 100*time.Millisecond)
	first.finish(walsummary.ErrStoreCorrupted)
	require.Equal(t, 100*time.Millisecond, receiveSubscribeClose(t, delays))
	receiveSubscribeClose(t, owners)
	require.NoError(t, stream.Close())
}

func TestResumableStreamFailureBackoffIgnoresWake(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &resumableStream{ctx: ctx, wake: make(chan struct{}, 1)}
	stream.wakeResume() // A pending Subscribe must not skip failure backoff.
	finished := make(chan error, 1)
	go func() { finished <- stream.waitNextRetry(time.Hour) }()
	select {
	case err := <-finished:
		t.Fatalf("subscription wake skipped backoff: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, receiveSubscribeClose(t, finished), context.Canceled)

	// Expiry still advances retries even when no new subscription arrives.
	stream.ctx = t.Context()
	started := time.Now()
	require.NoError(t, stream.waitNextRetry(10*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(started), 10*time.Millisecond)
}
