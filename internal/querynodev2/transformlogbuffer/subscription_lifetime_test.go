package transformlogbuffer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type subscriptionWaitContext struct {
	context.Context
	waiting chan struct{}
}

func (*subscriptionWaitContext) Done() <-chan struct{} { panic("mockey") }

type subscriptionAcquireResult struct {
	guard qnview.TransformLogGuard
	err   error
}

func acquireSubscriptionAsync(ctx context.Context, b *Buffer, channel string) <-chan subscriptionAcquireResult {
	result := make(chan subscriptionAcquireResult, 1)
	go func() {
		guard, err := b.Acquire(ctx, newTestQueryView(channel, 50))
		result <- subscriptionAcquireResult{guard: guard, err: err}
	}()
	return result
}

func awaitSubscriptionResult(t *testing.T, result <-chan subscriptionAcquireResult) subscriptionAcquireResult {
	t.Helper()
	select {
	case r := <-result:
		if r.guard != nil {
			t.Cleanup(r.guard.Release)
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire did not finish")
		return subscriptionAcquireResult{}
	}
}

func TestPendingSubscriptionSurvivesCallerCancellation(t *testing.T) {
	for _, canceled := range []int{0, 1} {
		name := "first caller"
		if canceled == 1 {
			name = "second caller"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			firstCtx, cancelFirst := context.WithCancel(ctx)
			defer cancelFirst()
			secondBase, cancelSecond := context.WithCancel(ctx)
			defer cancelSecond()
			waiting := make(chan struct{}, 1)
			secondCtx := &subscriptionWaitContext{Context: secondBase, waiting: waiting}
			entered := make(chan context.Context, 1)
			proceed := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(proceed) })
			var calls atomic.Int32
			patch := mockey.Mock((*fakeStream).Subscribe).To(func(_ *fakeStream, attempt context.Context, _ wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				calls.Add(1)
				entered <- attempt
				select {
				case <-proceed:
					return fakeSubscription{id: 1}, nil
				case <-attempt.Done():
					return nil, attempt.Err()
				}
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			// Observe entry into the caller's wait without patching the production
			// wait function or using Mockey Origin concurrently.
			waitPatch := mockey.Mock((*subscriptionWaitContext).Done).To(func(c *subscriptionWaitContext) <-chan struct{} {
				select {
				case c.waiting <- struct{}{}:
				default:
				}
				return c.Context.Done()
			}).Build()
			t.Cleanup(func() { waitPatch.UnPatch() })
			buffer := New(newFakeStreamManager(), 1)
			first := acquireSubscriptionAsync(firstCtx, buffer, "p_1v0")
			var attemptCtx context.Context
			select {
			case attemptCtx = <-entered:
			case <-ctx.Done():
				t.Fatal("Subscribe did not start")
			}
			second := acquireSubscriptionAsync(secondCtx, buffer, "p_1v0")
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("second caller did not join the shared attempt")
			}
			results := []<-chan subscriptionAcquireResult{first, second}
			[]context.CancelFunc{cancelFirst, cancelSecond}[canceled]()
			require.ErrorIs(t, awaitSubscriptionResult(t, results[canceled]).err, context.Canceled)
			require.NoError(t, attemptCtx.Err(), "a remaining reference still owns the subscription")
			once.Do(func() { close(proceed) })
			r := awaitSubscriptionResult(t, results[1-canceled])
			require.NoError(t, r.err)
			require.NotNil(t, r.guard)
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestLastPendingReferenceCancelsSubscription(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		name := "canceled"
		if lateSuccess {
			name = "late success"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered := make(chan context.Context, 1)
			proceed, finished := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(proceed) })
			var calls atomic.Int32
			patch := mockey.Mock((*fakeStream).Subscribe).To(func(_ *fakeStream, attempt context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				if opt.VChannel != "p_1v0" {
					return fakeSubscription{id: 3}, nil
				}
				if calls.Add(1) > 1 {
					return fakeSubscription{id: 2}, nil
				}
				defer close(finished)
				entered <- attempt
				<-attempt.Done()
				<-proceed
				if lateSuccess {
					return fakeSubscription{id: 1}, nil
				}
				return nil, attempt.Err()
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			var oldCloses, newCloses atomic.Int32
			closePatch := mockey.Mock(fakeSubscription.Close).To(func(s fakeSubscription) error {
				switch s.id {
				case 1:
					oldCloses.Add(1)
				case 2:
					newCloses.Add(1)
				}
				return nil
			}).Build()
			t.Cleanup(func() { closePatch.UnPatch() })
			streams := newFakeStreamManager()
			buffer := New(streams, 1)
			other, err := buffer.Acquire(ctx, newTestQueryView("p_2v0", 50))
			require.NoError(t, err)
			t.Cleanup(other.Release)
			caller, cancelCaller := context.WithCancel(ctx)
			defer cancelCaller()
			pending := acquireSubscriptionAsync(caller, buffer, "p_1v0")
			var attemptCtx context.Context
			select {
			case attemptCtx = <-entered:
			case <-ctx.Done():
				t.Fatal("Subscribe did not start")
			}
			secondCtx, cancelSecond := context.WithCancel(ctx)
			defer cancelSecond()
			second := acquireSubscriptionAsync(secondCtx, buffer, "p_1v0")
			require.Eventually(t, func() bool {
				buffer.mu.Lock()
				defer buffer.mu.Unlock()
				buf := buffer.channels["p_1v0"]
				buf.mu.Lock()
				defer buf.mu.Unlock()
				return buf.guards[50] == 2
			}, time.Second, time.Millisecond)
			cancelCaller()
			require.ErrorIs(t, awaitSubscriptionResult(t, pending).err, context.Canceled)
			require.NoError(t, attemptCtx.Err())
			cancelSecond()
			require.ErrorIs(t, awaitSubscriptionResult(t, second).err, context.Canceled)
			require.ErrorIs(t, attemptCtx.Err(), context.Canceled)
			// The old attempt is still returning while this VChannel is reacquired.
			fresh, err := buffer.Acquire(ctx, newTestQueryView("p_1v0", 50))
			require.NoError(t, err)
			t.Cleanup(fresh.Release)
			once.Do(func() { close(proceed) })
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("old Subscribe did not finish")
			}
			if lateSuccess {
				require.Eventually(t, func() bool { return oldCloses.Load() == 1 }, time.Second, time.Millisecond)
			}
			require.Zero(t, newCloses.Load())
			require.Equal(t, 1, streams.callCount("p"), "other VChannel retains the logical stream")
			select {
			case <-streams.stream("p").Done():
				t.Fatal("pending cancellation closed another VChannel's stream")
			default:
			}
			require.NoError(t, fresh.WaitTransformVisible(ctx, 50))
			fresh.Release()
			require.EqualValues(t, 1, newCloses.Load())
		})
	}
}
