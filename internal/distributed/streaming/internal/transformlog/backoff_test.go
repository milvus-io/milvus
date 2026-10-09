package transformlog

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type backoffStream struct{ wal.TransformLogStream }

func (*backoffStream) Subscribe(context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	panic("mockey")
}
func (*backoffStream) Close() error { panic("mockey") }

type backoffSubscription struct{ wal.TransformLogSubscription }

func (*backoffSubscription) ID() int64    { panic("mockey") }
func (*backoffSubscription) Close() error { panic("mockey") }

func TestReconnectBackoffTracksConsecutiveFailures(t *testing.T) {
	for _, failSubscription := range []bool{false, true} {
		name := "factory failure"
		if failSubscription {
			name = "subscription failure"
		}
		t.Run(name, func(t *testing.T) {
			patch := func(p *mockey.Mocker) { t.Cleanup(func() { p.UnPatch() }) }
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Use the real backoff calculation without jitter or wall-clock sleeps.
			var newBackoff func() *backoff.ExponentialBackOff
			patch(mockey.Mock(backoff.NewExponentialBackOff).Origin(&newBackoff).To(func() *backoff.ExponentialBackOff {
				b := newBackoff()
				b.RandomizationFactor = 0
				return b
			}).Build())
			var nextBackoff func(*backoff.ExponentialBackOff) time.Duration
			delays := make(chan time.Duration, 20)
			patch(mockey.Mock((*backoff.ExponentialBackOff).NextBackOff).Origin(&nextBackoff).To(func(b *backoff.ExponentialBackOff) time.Duration {
				delay := nextBackoff(b)
				select {
				case delays <- delay:
				case <-ctx.Done():
				}
				return time.Duration(0)
			}).Build())
			patch(mockey.Mock((*backoffStream).Close).Return(nil).Build())
			patch(mockey.Mock((*backoffSubscription).Close).Return(nil).Build())
			patch(mockey.Mock((*backoffSubscription).ID).Return(int64(1)).Build())
			patch(mockey.Mock((*demandHandler).Close).Return().Build())
			// All counters and failure decisions belong to resumeLoop's goroutine.
			// Every third attempt succeeds, ending a run of two failed attempts.
			calls := 0
			patch(mockey.Mock((*backoffStream).Subscribe).To(func(*backoffStream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				if failSubscription && calls%3 != 0 {
					return nil, context.DeadlineExceeded
				}
				return &backoffSubscription{}, nil
			}).Build())
			connected, disconnect := make(chan struct{}), make(chan struct{})
			patch(mockey.Mock((*resumableStream).waitUntilUnavailable).To(func(_ *resumableStream, attempt context.Context, _ wal.TransformLogStream) error {
				// This boundary is reached after subscribePending has returned success.
				// An early Entry/SyncUp callback does not guarantee that ordering.
				select {
				case connected <- struct{}{}:
				case <-attempt.Done():
					return attempt.Err()
				}
				select {
				case <-disconnect:
					return io.EOF
				case <-attempt.Done():
					return attempt.Err()
				}
			}).Build())
			stream := NewResumableStream(ctx, "p", func(context.Context, string) (wal.TransformLogStream, error) {
				calls++
				if !failSubscription && calls%3 != 0 {
					return nil, context.DeadlineExceeded
				}
				return &backoffStream{}, nil
			})
			defer stream.Close()
			_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "p_1v0", Handler: &demandHandler{}})
			require.NoError(t, err)
			waitConnected := func() {
				t.Helper()
				select {
				case <-connected:
				case <-ctx.Done():
					t.Fatal("subscription restoration did not complete")
				}
			}
			expectDelay := func(want time.Duration) {
				t.Helper()
				select {
				case got := <-delays:
					require.Equal(t, want, got)
				case <-ctx.Done():
					t.Fatal("missing retry interval")
				}
			}
			waitConnected()
			expectDelay(100 * time.Millisecond)
			expectDelay(150 * time.Millisecond)
			for range 3 {
				select {
				case disconnect <- struct{}{}:
				case <-ctx.Done():
					t.Fatal("connection did not stop")
				}
				waitConnected()
				// The disconnect starts at 100ms; two subsequent restoration failures
				// increase the interval. A successful factory alone must not reset it.
				expectDelay(100 * time.Millisecond)
				expectDelay(150 * time.Millisecond)
				expectDelay(225 * time.Millisecond)
			}
			require.NoError(t, stream.Close())
			require.Empty(t, delays, "owner close must not schedule another retry")
		})
	}
}
