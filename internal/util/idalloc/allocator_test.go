package idalloc

import (
	"context"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestTimestampAllocator(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := NewMockRootCoordClient(t)
	f := syncutil.NewFuture[types.MixCoordClient]()
	f.Set(client)
	allocator := NewTSOAllocator(f)

	for i := 0; i < 5000; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
	}

	for i := 0; i < 100; i++ {
		ts, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.NotZero(t, ts)
		time.Sleep(time.Millisecond * 1)
		allocator.Sync()
	}

	// error test
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Unset()
	client.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			return &rootcoordpb.AllocTimestampResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_ForceDeny,
				},
			}, nil
		},
	)

	allocator = NewTSOAllocator(f)
	_, err := allocator.Allocate(context.Background())
	assert.Error(t, err)
}

func TestIDAllocator(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := NewMockRootCoordClient(t)
	f := syncutil.NewFuture[types.MixCoordClient]()
	f.Set(client)

	allocator := NewIDAllocator(f)

	// Make local dirty
	allocator.Allocate(context.Background())
	// Test barrier fast path.
	resp, err := client.AllocID(context.Background(), &rootcoordpb.AllocIDRequest{
		Count: 100,
	})
	assert.NoError(t, err)
	err = allocator.BarrierUntil(context.Background(), uint64(resp.ID))
	assert.NoError(t, err)
	newBarrierTimeTick, err := allocator.Allocate(context.Background())
	assert.NoError(t, err)
	assert.Greater(t, newBarrierTimeTick, uint64(resp.ID))

	// Test slow path.
	ch := make(chan struct{})
	go func() {
		barrier := newBarrierTimeTick + 1*batchAllocateSize
		err := allocator.BarrierUntil(context.Background(), barrier)
		assert.NoError(t, err)
		newBarrierTimeTick, err := allocator.Allocate(context.Background())
		assert.NoError(t, err)
		assert.Greater(t, newBarrierTimeTick, barrier)
		close(ch)
	}()
	select {
	case <-ch:
		assert.Fail(t, "should not finish")
	case <-time.After(time.Millisecond * 20):
	}
	allocator.Sync()
	_, err = allocator.Allocate(context.Background())
	assert.NoError(t, err)
	<-ch

	allocator.SyncIfExpired(time.Millisecond * 50)
	time.Sleep(time.Millisecond * 10)
	allocator.SyncIfExpired(time.Millisecond * 10)
}

const (
	// testWaitTimeout is how long a test waits for something which must happen.
	testWaitTimeout = 10 * time.Second
	// testSettleTime is how long a test watches for something which must not happen.
	testSettleTime = 50 * time.Millisecond
	// testExpired makes SyncIfExpired always consider the cache expired.
	testExpired = -time.Second
)

var errTestRemote = errors.New("remote failed")

// gatedRemote is a remote allocator whose responses are released by the test.
// A call is served when it arrives, like a real TSO, so its range is decided
// at that time however late the response is. It ignores the cancellation on
// purpose to prove that a late response is harmless.
type gatedRemote struct {
	mu       sync.Mutex
	next     uint64
	numCalls int
	calls    chan *gatedCall
}

type gatedCall struct {
	ctx     context.Context
	start   uint64
	count   int
	release chan error
}

func newGatedRemote() *gatedRemote {
	return &gatedRemote{next: batchAllocateSize, calls: make(chan *gatedCall, 128)}
}

func (r *gatedRemote) batchAllocate(ctx context.Context, count uint32) (uint64, int, error) {
	r.mu.Lock()
	c := &gatedCall{ctx: ctx, start: r.next, count: int(count), release: make(chan error, 1)}
	r.next += uint64(count)
	r.numCalls++
	r.mu.Unlock()

	r.calls <- c
	if err := <-c.release; err != nil {
		return 0, 0, err
	}
	return c.start, c.count, nil
}

// serve allocates one id directly from the remote, like another node does.
func (r *gatedRemote) serve() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	return r.next - 1
}

func (r *gatedRemote) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.numCalls
}

func (r *gatedRemote) take(t *testing.T) *gatedCall {
	t.Helper()
	select {
	case c := <-r.calls:
		return c
	case <-time.After(testWaitTimeout):
		require.Fail(t, "remote allocator is not called")
		return nil
	}
}

func (r *gatedRemote) assertNoNewCall(t *testing.T, expected int) {
	t.Helper()
	time.Sleep(testSettleTime)
	require.Equal(t, expected, r.callCount())
}

func newGatedAllocator() (*allocatorImpl, *gatedRemote) {
	remote := newGatedRemote()
	return &allocatorImpl{
		cond:            syncutil.NewContextCond(&sync.Mutex{}),
		remoteAllocator: remote,
		localAllocator:  newLocalAllocator(),
	}, remote
}

type allocResult struct {
	id  uint64
	err error
}

func allocateAsync(ctx context.Context, a Allocator) <-chan allocResult {
	ch := make(chan allocResult, 1)
	go func() {
		id, err := a.Allocate(ctx)
		ch <- allocResult{id, err}
	}()
	return ch
}

func asyncError(f func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- f() }()
	return ch
}

func waitResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(testWaitTimeout):
		require.Fail(t, "operation is blocked")
		panic("unreachable")
	}
}

func assertBlocked[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case r := <-ch:
		require.Failf(t, "operation should be blocked", "got %v", r)
	case <-time.After(testSettleTime):
	}
}

func mustAllocate(t *testing.T, a Allocator) uint64 {
	t.Helper()
	r := waitResult(t, allocateAsync(context.Background(), a))
	require.NoError(t, r.err)
	return r.id
}

// warm fills the cache with the first range and returns its start.
func warm(t *testing.T, a Allocator, remote *gatedRemote) uint64 {
	t.Helper()
	result := allocateAsync(context.Background(), a)
	c := remote.take(t)
	c.release <- nil
	r := waitResult(t, result)
	require.NoError(t, r.err)
	require.Equal(t, c.start, r.id)
	return r.id
}

// waitBarrierInFastPath waits until the local allocator reaches nextStartID, which
// means the barrier calls took their first id and only wait for a later fetch.
func waitBarrierInFastPath(t *testing.T, a *allocatorImpl, nextStartID uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		a.cond.L.Lock()
		defer a.cond.L.Unlock()
		return a.localAllocator.nextStartID >= nextStartID
	}, testWaitTimeout, time.Millisecond)
}

func TestAllocatorCachedAllocationsCompleteDuringRefresh(t *testing.T) {
	a, remote := newGatedAllocator()
	first := warm(t, a, remote)

	a.SyncIfExpired(testExpired)
	for i := uint64(1); i <= 10; i++ {
		require.Equal(t, first+i, mustAllocate(t, a), "the cached range must stay usable during the refresh")
	}

	c := remote.take(t)
	c.release <- nil
	require.NoError(t, waitResult(t, asyncError(func() error { return a.WaitRefresh(context.Background()) })))
	require.Equal(t, c.start, mustAllocate(t, a), "the refreshed range must be published")
	require.Greater(t, c.start, first+10)
}

func TestAllocatorRefreshIsCoalesced(t *testing.T) {
	a, remote := newGatedAllocator()
	warm(t, a, remote)
	first := a.localAllocator.nextStartID - 1

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.SyncIfExpired(testExpired)
		}()
	}
	wg.Wait()
	c := remote.take(t)
	remote.assertNoNewCall(t, 2)

	// A genuine exhaustion joins the refresh too.
	for i := uint64(1); i < batchAllocateSize; i++ {
		require.Equal(t, first+i, mustAllocate(t, a))
	}
	results := []<-chan allocResult{
		allocateAsync(context.Background(), a),
		allocateAsync(context.Background(), a),
	}
	assertBlocked(t, results[0])
	remote.assertNoNewCall(t, 2)

	c.release <- nil
	ids := []uint64{waitResult(t, results[0]).id, waitResult(t, results[1]).id}
	slices.Sort(ids)
	require.Equal(t, []uint64{c.start, c.start + 1}, ids)
	remote.assertNoNewCall(t, 2)
}

func TestAllocatorFailedRefreshKeepsRange(t *testing.T) {
	a, remote := newGatedAllocator()
	first := warm(t, a, remote)

	a.SyncIfExpired(testExpired)
	c := remote.take(t)
	wait := asyncError(func() error { return a.WaitRefresh(context.Background()) })
	assertBlocked(t, wait)
	c.release <- errTestRemote
	require.ErrorIs(t, waitResult(t, wait), errTestRemote)

	// Every remaining id is usable without any remote call.
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	for i := first + 1; i < first+batchAllocateSize; i++ {
		id, err := a.Allocate(ctx)
		require.NoError(t, err)
		require.Equal(t, i, id)
	}
	remote.assertNoNewCall(t, 2)

	// A genuine exhaustion reports the failure of its fetch, and the next call retries it.
	result := allocateAsync(context.Background(), a)
	c = remote.take(t)
	c.release <- errTestRemote
	require.ErrorIs(t, waitResult(t, result).err, errTestRemote)

	result = allocateAsync(context.Background(), a)
	c = remote.take(t)
	c.release <- nil
	require.Equal(t, c.start, waitResult(t, result).id)
}

func TestAllocatorFailedRefreshIsPacedByExpiration(t *testing.T) {
	a, remote := newGatedAllocator()
	warm(t, a, remote)
	expire := func() {
		a.cond.L.Lock()
		defer a.cond.L.Unlock()
		a.lastSyncTime = time.Now().Add(-time.Hour)
	}

	expire()
	a.SyncIfExpired(time.Minute)
	c := remote.take(t)
	wait := asyncError(func() error { return a.WaitRefresh(context.Background()) })
	assertBlocked(t, wait)
	c.release <- errTestRemote
	require.ErrorIs(t, waitResult(t, wait), errTestRemote)

	// A trigger right after the failure doesn't retry.
	a.SyncIfExpired(time.Minute)
	remote.assertNoNewCall(t, 2)

	// The next one after the expiration does.
	expire()
	a.SyncIfExpired(time.Minute)
	c = remote.take(t)
	c.release <- nil
}

func TestAllocatorExhaustionIsCancellable(t *testing.T) {
	a, remote := newGatedAllocator()

	first := allocateAsync(context.Background(), a)
	c := remote.take(t)

	// A caller which is waiting behind the fetch of another caller can be canceled.
	ctx, cancel := context.WithCancelCause(context.Background())
	second := allocateAsync(ctx, a)
	assertBlocked(t, second)
	stopped := errors.New("caller stopped")
	cancel(stopped)
	require.ErrorIs(t, waitResult(t, second).err, stopped)
	assertBlocked(t, first)

	// A canceled caller doesn't touch the remote at all.
	r := waitResult(t, allocateAsync(ctx, a))
	require.ErrorIs(t, r.err, stopped)
	remote.assertNoNewCall(t, 1)

	c.release <- nil
	require.Equal(t, c.start, waitResult(t, first).id)
	remote.assertNoNewCall(t, 1)
}

func TestAllocatorCanceledCallerDoesNotStartFetch(t *testing.T) {
	a, remote := newGatedAllocator()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, waitResult(t, allocateAsync(ctx, a)).err, context.Canceled)
	remote.assertNoNewCall(t, 0)
}

func TestAllocatorFetchOutlivesCanceledCaller(t *testing.T) {
	a, remote := newGatedAllocator()

	ctx, cancel := context.WithCancel(context.Background())
	starter := allocateAsync(ctx, a)
	c := remote.take(t)
	cancel()
	require.ErrorIs(t, waitResult(t, starter).err, context.Canceled)
	require.NoError(t, c.ctx.Err(), "the fetch is useful for later allocations")

	// The next caller joins the fetch rather than sending another rpc.
	result := allocateAsync(context.Background(), a)
	assertBlocked(t, result)
	remote.assertNoNewCall(t, 1)
	c.release <- nil
	require.Equal(t, c.start, waitResult(t, result).id)
	remote.assertNoNewCall(t, 1)
}

func TestAllocatorSyncAbandonsFetchInFlight(t *testing.T) {
	t.Run("late response cannot satisfy a waiter", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		obsolete := remote.take(t)

		a.Sync()
		require.Error(t, obsolete.ctx.Err(), "the abandoned rpc should be canceled")

		// The new fetch doesn't wait for the abandoned one.
		result := allocateAsync(context.Background(), a)
		c := remote.take(t)
		obsolete.release <- nil
		assertBlocked(t, result)
		c.release <- nil
		require.Equal(t, c.start, waitResult(t, result).id)
	})

	t.Run("late response cannot replace a newer range", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		obsolete := remote.take(t)

		a.Sync()
		result := allocateAsync(context.Background(), a)
		c := remote.take(t)
		c.release <- nil
		require.Equal(t, c.start, waitResult(t, result).id)

		obsolete.release <- nil
		time.Sleep(testSettleTime)
		require.Equal(t, c.start+1, mustAllocate(t, a))
	})

	t.Run("late response before next allocation is not published", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		obsolete := remote.take(t)

		a.Sync()
		obsolete.release <- nil
		time.Sleep(testSettleTime)

		result := allocateAsync(context.Background(), a)
		c := remote.take(t)
		c.release <- nil
		require.Equal(t, c.start, waitResult(t, result).id)
	})

	t.Run("failure of abandoned fetch is not reported to its waiters", func(t *testing.T) {
		a, remote := newGatedAllocator()
		result := allocateAsync(context.Background(), a)
		obsolete := remote.take(t)

		// Sync runs while the allocation is waiting for the remote.
		waitResult(t, asyncError(func() error {
			a.Sync()
			return nil
		}))
		c := remote.take(t)
		obsolete.release <- errTestRemote
		assertBlocked(t, result)
		c.release <- nil
		r := waitResult(t, result)
		require.NoError(t, r.err)
		require.Equal(t, c.start, r.id)
	})

	t.Run("expiration does not bypass sync", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)
		a.Sync()

		// A background refresh started after the sync is a valid source of the next id,
		// and the cached range never comes back.
		a.SyncIfExpired(testExpired)
		c := remote.take(t)
		result := allocateAsync(context.Background(), a)
		assertBlocked(t, result)
		c.release <- nil
		require.Equal(t, c.start, waitResult(t, result).id)
	})
}

func TestAllocatorBarrierIgnoresFetchStartedBeforeIt(t *testing.T) {
	t.Run("needs a new fetch", func(t *testing.T) {
		a, remote := newGatedAllocator()
		first := warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		stale := remote.take(t)

		// The fetch in flight is served before the barrier is allocated.
		barrier := remote.serve()
		done := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier) })
		waitBarrierInFastPath(t, a, first+2)

		// Allocations are not blocked by the barrier, and stay below it.
		require.Equal(t, first+2, mustAllocate(t, a))
		assertBlocked(t, done)

		stale.release <- nil
		c := remote.take(t)
		assertBlocked(t, done)
		require.Equal(t, stale.start, mustAllocate(t, a))

		c.release <- nil
		require.NoError(t, waitResult(t, done))
		require.Greater(t, mustAllocate(t, a), barrier)
	})

	t.Run("concurrent barriers share the next fetch", func(t *testing.T) {
		a, remote := newGatedAllocator()
		first := warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		stale := remote.take(t)

		barrier1, barrier2 := remote.serve(), remote.serve()
		done1 := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier1) })
		done2 := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier2) })
		waitBarrierInFastPath(t, a, first+3)

		stale.release <- nil
		c := remote.take(t)
		remote.assertNoNewCall(t, 3)

		c.release <- nil
		require.NoError(t, waitResult(t, done1))
		require.NoError(t, waitResult(t, done2))
		require.Greater(t, mustAllocate(t, a), barrier2)
		remote.assertNoNewCall(t, 3)
	})

	t.Run("failure of the stale fetch is not its failure", func(t *testing.T) {
		a, remote := newGatedAllocator()
		first := warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		stale := remote.take(t)

		barrier := remote.serve()
		done := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier) })
		waitBarrierInFastPath(t, a, first+2)

		stale.release <- errTestRemote
		c := remote.take(t)
		assertBlocked(t, done)
		c.release <- nil
		require.NoError(t, waitResult(t, done))
		require.Greater(t, mustAllocate(t, a), barrier)
	})

	t.Run("failure of its fetch keeps the range", func(t *testing.T) {
		a, remote := newGatedAllocator()
		first := warm(t, a, remote)

		barrier := remote.serve()
		done := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier) })
		c := remote.take(t)
		c.release <- errTestRemote
		require.ErrorIs(t, waitResult(t, done), errTestRemote)
		require.Equal(t, first+2, mustAllocate(t, a))
	})

	t.Run("sync during its fetch", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)

		barrier := remote.serve()
		done := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier) })
		obsolete := remote.take(t)
		a.Sync()
		c := remote.take(t)
		obsolete.release <- nil
		assertBlocked(t, done)
		c.release <- nil
		require.NoError(t, waitResult(t, done))
		require.Greater(t, mustAllocate(t, a), barrier)
	})

	t.Run("cancellation", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)

		ctx, cancel := context.WithCancel(context.Background())
		barrier := remote.serve()
		done := asyncError(func() error { return a.BarrierUntil(ctx, barrier) })
		c := remote.take(t)
		cancel()
		require.ErrorIs(t, waitResult(t, done), context.Canceled)
		c.release <- nil
	})

	t.Run("future barrier falls back to slow path", func(t *testing.T) {
		a, remote := newGatedAllocator()
		first := warm(t, a, remote)

		barrier := first + 10*batchAllocateSize
		done := asyncError(func() error { return a.BarrierUntil(context.Background(), barrier) })
		c := remote.take(t)
		c.release <- nil
		assertBlocked(t, done)

		// Only an allocation which passes the barrier releases it.
		remote.mu.Lock()
		remote.next = barrier
		remote.mu.Unlock()
		a.Sync()
		result := allocateAsync(context.Background(), a)
		c = remote.take(t)
		c.release <- nil
		require.Equal(t, barrier, waitResult(t, result).id)
		require.NoError(t, waitResult(t, done))
	})
}

// TestAllocatorRefreshKeepsTimeTickFresh is the flow of a TimeTick sync: the sync
// waits for the refresh it triggers while the appends go on with the cached range.
func TestAllocatorRefreshKeepsTimeTickFresh(t *testing.T) {
	a, remote := newGatedAllocator()
	first := warm(t, a, remote)

	guarantee := remote.serve()
	a.SyncIfExpired(testExpired)
	c := remote.take(t)
	wait := asyncError(func() error { return a.WaitRefresh(context.Background()) })
	assertBlocked(t, wait)
	require.Equal(t, first+1, mustAllocate(t, a), "an append must not wait for the refresh")
	assertBlocked(t, wait)

	c.release <- nil
	require.NoError(t, waitResult(t, wait))
	require.GreaterOrEqual(t, mustAllocate(t, a), guarantee)
}

func TestAllocatorWaitRefresh(t *testing.T) {
	t.Run("nothing in flight", func(t *testing.T) {
		a, _ := newGatedAllocator()
		require.NoError(t, waitResult(t, asyncError(func() error { return a.WaitRefresh(context.Background()) })))
	})

	t.Run("cancellation", func(t *testing.T) {
		a, remote := newGatedAllocator()
		first := warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		c := remote.take(t)

		ctx, cancel := context.WithCancel(context.Background())
		wait := asyncError(func() error { return a.WaitRefresh(ctx) })
		assertBlocked(t, wait)
		cancel()
		require.ErrorIs(t, waitResult(t, wait), context.Canceled)
		require.Equal(t, first+1, mustAllocate(t, a))
		c.release <- nil
	})

	t.Run("abandoned by sync", func(t *testing.T) {
		a, remote := newGatedAllocator()
		warm(t, a, remote)
		a.SyncIfExpired(testExpired)
		c := remote.take(t)

		wait := asyncError(func() error { return a.WaitRefresh(context.Background()) })
		assertBlocked(t, wait)
		a.Sync()
		require.NoError(t, waitResult(t, wait))
		c.release <- nil
	})
}

func TestAllocatorRejectsRangeWhichDoesNotAdvance(t *testing.T) {
	for name, mutate := range map[string]func(c *gatedCall){
		"rollback":    func(c *gatedCall) { c.start = 1 },
		"empty range": func(c *gatedCall) { c.count = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			a, remote := newGatedAllocator()
			first := warm(t, a, remote)

			// The cached range is kept.
			a.SyncIfExpired(testExpired)
			c := remote.take(t)
			mutate(c)
			wait := asyncError(func() error { return a.WaitRefresh(context.Background()) })
			assertBlocked(t, wait)
			c.release <- nil
			require.ErrorIs(t, waitResult(t, wait), errExhausted)
			require.Equal(t, first+1, mustAllocate(t, a))

			// Nothing to allocate from, it fails rather than spinning on the remote.
			a.Sync()
			result := allocateAsync(context.Background(), a)
			c = remote.take(t)
			mutate(c)
			c.release <- nil
			require.ErrorIs(t, waitResult(t, result).err, errExhausted)
		})
	}
}

func TestSharedAllocatorRefreshFromRemote(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)

	client := NewMockRootCoordClient(t)
	f := syncutil.NewFuture[types.MixCoordClient]()
	f.Set(client)

	for name, allocator := range map[string]Allocator{
		"tso": NewTSOAllocator(f),
		"id":  NewIDAllocator(f),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			last := uint64(0)
			for i := 0; i < 20; i++ {
				allocator.SyncIfExpired(testExpired)
				for j := 0; j < 300; j++ {
					id, err := allocator.Allocate(ctx)
					require.NoError(t, err)
					require.Greater(t, id, last)
					last = id
				}
				require.NoError(t, allocator.WaitRefresh(ctx))
			}
		})
	}
}

// stressRemote is a remote allocator which responds slowly and fails sometimes.
type stressRemote struct {
	next atomic.Uint64
}

func (r *stressRemote) batchAllocate(ctx context.Context, count uint32) (uint64, int, error) {
	start := r.next.Add(uint64(count)) - uint64(count)
	time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
	if rand.Intn(10) == 0 {
		return 0, 0, errTestRemote
	}
	return start, int(count), nil
}

// TestAllocatorConcurrentInvariants runs writers, TimeTick syncs, explicit syncs
// and barriers together, and verifies what each of them is promised.
func TestAllocatorConcurrentInvariants(t *testing.T) {
	remote := &stressRemote{}
	remote.next.Store(batchAllocateSize)
	a := &allocatorImpl{
		cond:            syncutil.NewContextCond(&sync.Mutex{}),
		remoteAllocator: remote,
		localAllocator:  newLocalAllocator(),
	}

	const rounds = 1000
	var observed atomic.Uint64
	// allocate verifies that an id is greater than every id returned before the call.
	allocate := func() (uint64, bool) {
		before := observed.Load()
		id, err := a.Allocate(context.Background())
		if err != nil {
			assert.ErrorIs(t, err, errTestRemote)
			return 0, false
		}
		assert.Greater(t, id, before)
		for {
			current := observed.Load()
			if id <= current || observed.CompareAndSwap(current, id) {
				return id, true
			}
		}
	}

	workers := []func(round int, ids *[]uint64){
		// writers.
		func(_ int, ids *[]uint64) {
			if id, ok := allocate(); ok {
				*ids = append(*ids, id)
			}
		},
		func(_ int, ids *[]uint64) {
			if id, ok := allocate(); ok {
				*ids = append(*ids, id)
			}
		},
		// a writer which gives up quickly.
		func(round int, ids *[]uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(round%3)*50*time.Microsecond)
			defer cancel()
			if id, err := a.Allocate(ctx); err == nil {
				*ids = append(*ids, id)
			}
		},
		// time tick syncs.
		func(_ int, ids *[]uint64) {
			a.SyncIfExpired(testExpired)
			if err := a.WaitRefresh(context.Background()); err != nil {
				assert.ErrorIs(t, err, errTestRemote)
			}
			if id, ok := allocate(); ok {
				*ids = append(*ids, id)
			}
		},
		// explicit syncs: the next id comes from a request made after the sync.
		func(round int, ids *[]uint64) {
			if round%8 != 0 {
				return
			}
			fence := remote.next.Load()
			a.Sync()
			if id, ok := allocate(); ok {
				*ids = append(*ids, id)
				assert.GreaterOrEqual(t, id, fence)
			}
		},
		// barriers: the next id is greater than the barrier.
		func(round int, ids *[]uint64) {
			if round%4 != 0 {
				return
			}
			barrier := remote.next.Add(1) - 1
			if err := a.BarrierUntil(context.Background(), barrier); err != nil {
				assert.ErrorIs(t, err, errTestRemote)
				return
			}
			if id, ok := allocate(); ok {
				*ids = append(*ids, id)
				assert.Greater(t, id, barrier)
			}
		},
	}

	allIDs := make([][]uint64, len(workers))
	var wg sync.WaitGroup
	for i, worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				worker(round, &allIDs[i])
			}
		}()
	}
	wg.Wait()

	ids := slices.Concat(allIDs...)
	require.NotEmpty(t, ids)
	slices.Sort(ids)
	require.Len(t, slices.Compact(ids), len(ids), "ids must be unique")
}
