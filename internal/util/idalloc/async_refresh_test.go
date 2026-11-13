package idalloc

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newRefreshTestAllocator(t *testing.T, allocate func(context.Context, uint32) (uint64, int, error)) *allocatorImpl {
	t.Helper()
	patch := mockey.Mock((*tsoAllocator).batchAllocate).To(func(_ *tsoAllocator, ctx context.Context, count uint32) (uint64, int, error) {
		return allocate(ctx, count)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	a := NewTSOAllocator(nil).(*allocatorImpl)
	t.Cleanup(func() {
		a.Sync()
		waitRefresh(t, a)
	})
	return a
}

func waitRefresh(t *testing.T, a *allocatorImpl) {
	t.Helper()
	a.cond.L.Lock()
	request := a.refresh
	a.cond.L.Unlock()
	if request != nil {
		waitRefreshSignal(t, request.done)
	}
}

func waitRefreshSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for allocator progress")
	}
}

func allocateRefreshTest(t *testing.T, a *allocatorImpl) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := a.Allocate(ctx)
	require.NoError(t, err)
	return id
}

func TestTSORefreshKeepsCacheAndCoalesces(t *testing.T) {
	var calls atomic.Uint64
	entered, release := make(chan struct{}), make(chan struct{})
	a := newRefreshTestAllocator(t, func(ctx context.Context, count uint32) (uint64, int, error) {
		n := calls.Add(1)
		if n == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return 0, 0, context.Cause(ctx)
			}
		}
		return (n-1)*uint64(count) + 1, int(count), nil
	})
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.SyncIfExpired(-1)
	waitRefreshSignal(t, entered)
	var triggers sync.WaitGroup
	for i := 0; i < 64; i++ {
		triggers.Go(func() { a.SyncIfExpired(-1) })
	}
	triggers.Wait()
	// These must all finish while the RPC gate is still closed. A synchronous
	// cache miss cannot pass this test, regardless of the machine's speed.
	var maxLatency time.Duration
	for i := 2; i <= 501; i++ {
		start := time.Now()
		require.Equal(t, uint64(i), allocateRefreshTest(t, a))
		maxLatency = max(maxLatency, time.Since(start))
	}
	require.Equal(t, uint64(2), calls.Load())
	t.Logf("500 cached allocations completed with refresh RPC blocked; maximum latency=%s", maxLatency)
	close(release)
	waitRefresh(t, a)
	require.Equal(t, uint64(1001), allocateRefreshTest(t, a))
}

func TestTSORefreshExhaustionAndWaiterCancellation(t *testing.T) {
	var calls atomic.Uint64
	entered, release := make(chan struct{}), make(chan struct{})
	a := newRefreshTestAllocator(t, func(ctx context.Context, _ uint32) (uint64, int, error) {
		n := calls.Add(1)
		if n == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return 0, 0, context.Cause(ctx)
			}
		}
		return (n-1)*2 + 1, 2, nil
	})
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.SyncIfExpired(-1)
	waitRefreshSignal(t, entered)
	require.Equal(t, uint64(2), allocateRefreshTest(t, a))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := a.Allocate(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, uint64(2), calls.Load())
	close(release)
	require.Equal(t, uint64(3), allocateRefreshTest(t, a))
	require.Equal(t, uint64(2), calls.Load(), "exhaustion should reuse the refresh response")
}

func TestTSORefreshFailureRetainsCacheAndRetries(t *testing.T) {
	var calls atomic.Uint64
	failure := merr.WrapErrServiceUnavailable("coordinator recovering")
	a := newRefreshTestAllocator(t, func(_ context.Context, count uint32) (uint64, int, error) {
		n := calls.Add(1)
		if n == 2 {
			return 0, 0, failure
		}
		return (n-1)*uint64(count) + 1, int(count), nil
	})
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.cond.L.Lock()
	a.lastSyncTime = time.Time{}
	a.lastRefreshTime = time.Time{}
	a.cond.L.Unlock()
	a.SyncIfExpired(time.Hour)
	waitRefresh(t, a)
	require.Equal(t, uint64(2), calls.Load())
	require.Equal(t, uint64(2), allocateRefreshTest(t, a))
	for i := 0; i < 100; i++ {
		a.SyncIfExpired(time.Hour)
	}
	require.Equal(t, uint64(2), calls.Load(), "failed periodic refreshes must be rate limited")
	a.SyncIfExpired(-1)
	waitRefresh(t, a)
	require.Equal(t, uint64(2001), allocateRefreshTest(t, a))
	a.SyncIfExpired(time.Hour)
	require.Equal(t, uint64(3), calls.Load(), "a successful refresh resets freshness")
}

func TestTSOSyncFencesInflightResponse(t *testing.T) {
	for _, background := range []bool{true, false} {
		t.Run(map[bool]string{true: "background refresh", false: "foreground refill"}[background], func(t *testing.T) {
			var calls atomic.Uint64
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			a := newRefreshTestAllocator(t, func(ctx context.Context, count uint32) (uint64, int, error) {
				n := calls.Add(1)
				if n == 2 {
					close(entered)
					<-ctx.Done()
					close(canceled)
					// Model a successful response racing with RPC cancellation.
					<-release
				}
				return (n-1)*uint64(count) + 1, int(count), nil
			})
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			require.Equal(t, uint64(1), allocateRefreshTest(t, a))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type result struct {
				id  uint64
				err error
			}
			allocated := make(chan result, 1)
			allocate := func() {
				id, err := a.Allocate(ctx)
				allocated <- result{id, err}
			}
			if background {
				a.SyncIfExpired(-1)
			} else {
				a.Sync()
				go allocate()
			}
			waitRefreshSignal(t, entered)
			a.Sync()
			a.Sync() // Repeated fences must not create more in-flight requests.
			waitRefreshSignal(t, canceled)
			if background {
				go allocate()
			}
			require.Equal(t, uint64(2), calls.Load())
			releaseOnce.Do(func() { close(release) })
			select {
			case got := <-allocated:
				require.NoError(t, got.err)
				require.Equal(t, uint64(2001), got.id, "pre-Sync response must never refill cache")
			case <-ctx.Done():
				t.Fatal("post-Sync allocation stalled")
			}
			require.Equal(t, uint64(2002), allocateRefreshTest(t, a))
		})
	}
}

func TestTSOBarrierFencesRefresh(t *testing.T) {
	var calls atomic.Uint64
	entered, canceled := make(chan struct{}), make(chan struct{})
	a := newRefreshTestAllocator(t, func(ctx context.Context, count uint32) (uint64, int, error) {
		n := calls.Add(1)
		if n == 2 {
			close(entered)
			<-ctx.Done()
			close(canceled)
		}
		return (n-1)*uint64(count) + 1, int(count), nil
	})
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.SyncIfExpired(-1)
	waitRefreshSignal(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, a.BarrierUntil(ctx, 1500))
	waitRefreshSignal(t, canceled)
	require.Equal(t, uint64(2002), allocateRefreshTest(t, a))
	// A barrier already covered by the cache does not cause a remote request.
	require.NoError(t, a.BarrierUntil(ctx, 2003))
	require.Equal(t, uint64(2004), allocateRefreshTest(t, a))
	require.Equal(t, uint64(3), calls.Load())
}

func TestTSORefillFailureAndOwnerCancellation(t *testing.T) {
	var calls atomic.Uint64
	entered := make(chan struct{})
	failure := merr.WrapErrServiceUnavailable("coordinator recovering")
	a := newRefreshTestAllocator(t, func(ctx context.Context, count uint32) (uint64, int, error) {
		switch calls.Add(1) {
		case 1:
			close(entered)
			<-ctx.Done()
			return 0, 0, context.Cause(ctx)
		case 2:
			return 0, 0, failure
		default:
			return 1001, int(count), nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := a.Allocate(ctx); finished <- err }()
	waitRefreshSignal(t, entered)
	cancel()
	require.ErrorIs(t, <-finished, context.Canceled)
	waitRefresh(t, a)
	_, err := a.Allocate(context.Background())
	require.ErrorIs(t, err, failure)
	require.Equal(t, uint64(1001), allocateRefreshTest(t, a))
	// Canceled cache hits must not consume a timestamp either.
	_, err = a.Allocate(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(1002), allocateRefreshTest(t, a))
}

func TestTSOConcurrentRefreshAllocations(t *testing.T) {
	var next atomic.Uint64
	a := newRefreshTestAllocator(t, func(_ context.Context, count uint32) (uint64, int, error) {
		return next.Add(uint64(count)) - uint64(count) + 1, int(count), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const workers, perWorker = 16, 400
	ids := make(chan uint64, workers*perWorker)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			var previous uint64
			for j := 0; j < perWorker; j++ {
				if j%10 == 0 {
					a.SyncIfExpired(-1)
				}
				id, err := a.Allocate(ctx)
				if err != nil {
					errs <- err
					return
				}
				if id <= previous {
					errs <- merr.WrapErrServiceInternalMsg("timestamps regressed: %d <= %d", id, previous)
					return
				}
				previous = id
				ids <- id
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	close(ids)
	all := make([]uint64, 0, workers*perWorker)
	for id := range ids {
		all = append(all, id)
	}
	require.Len(t, all, workers*perWorker)
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for i := 1; i < len(all); i++ {
		require.Greater(t, all[i], all[i-1], "duplicate timestamp")
	}
}

func TestIDExpirationRemainsSynchronous(t *testing.T) {
	var calls atomic.Uint64
	patch := mockey.Mock((*idAllocator).batchAllocate).To(func(_ *idAllocator, _ context.Context, count uint32) (uint64, int, error) {
		n := calls.Add(1)
		return (n-1)*uint64(count) + 1, int(count), nil
	}).Build()
	defer patch.UnPatch()
	a := NewIDAllocator(nil).(*allocatorImpl)
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.SyncIfExpired(-1)
	require.Equal(t, uint64(1), calls.Load(), "ID expiration must not start background allocation")
	require.Equal(t, uint64(1001), allocateRefreshTest(t, a))
}

func TestTSOExhaustedRefreshFailure(t *testing.T) {
	var calls atomic.Uint64
	entered, release := make(chan struct{}), make(chan struct{})
	failure := merr.WrapErrServiceUnavailable("refresh failed")
	a := newRefreshTestAllocator(t, func(ctx context.Context, _ uint32) (uint64, int, error) {
		n := calls.Add(1)
		if n == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return 0, 0, context.Cause(ctx)
			}
			return 0, 0, failure
		}
		return n, 1, nil
	})
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.SyncIfExpired(-1)
	waitRefreshSignal(t, entered)
	// Observe the waiter entering select before releasing the failed RPC.
	waiting := make(chan struct{})
	var once sync.Once
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitPatch := mockey.Mock((*refreshWaitContext).Done).To(func(c *refreshWaitContext) <-chan struct{} {
		once.Do(func() { close(waiting) })
		return c.Context.Done()
	}).Build()
	defer waitPatch.UnPatch()
	finished := make(chan error, 1)
	go func() { _, err := a.Allocate(&refreshWaitContext{ctx}); finished <- err }()
	waitRefreshSignal(t, waiting)
	close(release)
	require.ErrorIs(t, <-finished, failure)
	require.Equal(t, uint64(2), calls.Load(), "Allocate must not hide failures in an unbounded retry loop")
	require.Equal(t, uint64(3), allocateRefreshTest(t, a))
}

type refreshWaitContext struct{ context.Context }

func TestTSOBarrierSlowPathProgressAndCancellation(t *testing.T) {
	var next atomic.Uint64
	a := newRefreshTestAllocator(t, func(_ context.Context, _ uint32) (uint64, int, error) {
		return next.Add(2) - 1, 2, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, a.BarrierUntil(ctx, 100), context.DeadlineExceeded)
	// The canceled slow-path waiter must release the mutex.
	require.Equal(t, uint64(4), allocateRefreshTest(t, a))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	finished := make(chan error, 1)
	go func() { finished <- a.BarrierUntil(ctx2, 100) }()
	for i := 0; i < 100; i++ {
		allocateRefreshTest(t, a)
	}
	require.NoError(t, <-finished)
	require.Greater(t, allocateRefreshTest(t, a), uint64(100))
}

func TestTSOSyncDuringBarrierRefill(t *testing.T) {
	var calls atomic.Uint64
	refreshEntered, barrierEntered := make(chan struct{}), make(chan struct{})
	a := newRefreshTestAllocator(t, func(ctx context.Context, count uint32) (uint64, int, error) {
		n := calls.Add(1)
		switch n {
		case 2:
			close(refreshEntered)
			<-ctx.Done() // Barrier invalidates this periodic refresh.
		case 3:
			close(barrierEntered)
			<-ctx.Done() // Explicit Sync invalidates the barrier's own refill.
		}
		// Even successful responses arriving after cancellation are obsolete.
		return (n-1)*uint64(count) + 1, int(count), nil
	})
	require.Equal(t, uint64(1), allocateRefreshTest(t, a))
	a.SyncIfExpired(-1)
	waitRefreshSignal(t, refreshEntered)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- a.BarrierUntil(ctx, 1500) }()
	waitRefreshSignal(t, barrierEntered)
	a.Sync()
	require.NoError(t, <-finished)
	require.Equal(t, uint64(3002), allocateRefreshTest(t, a))
	require.Equal(t, uint64(4), calls.Load())
}
