package syncutil

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testWaitTimeout = 200 * time.Millisecond
	testShortWait   = 30 * time.Millisecond
)

// waitForWaiterCount polls until WaiterCount reaches want or the deadline hits.
func waitForWaiterCount(t *testing.T, n *TargetNotifier, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.WaiterCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, want, n.WaiterCount(), "WaiterCount did not converge")
}

func TestTargetNotifier_ImmediateWhenReached(t *testing.T) {
	n := NewTargetNotifier(10)
	assert.Equal(t, uint64(10), n.Load())

	// A target at or below the current value returns immediately, progressed.
	progressed, err := n.WaitFor(context.Background(), 10, testWaitTimeout)
	require.NoError(t, err)
	assert.True(t, progressed)

	progressed, err = n.WaitFor(context.Background(), 5, testWaitTimeout)
	require.NoError(t, err)
	assert.True(t, progressed)
	assert.Equal(t, 0, n.WaiterCount())
}

func TestTargetNotifier_WakesOnAdvanceToTarget(t *testing.T) {
	n := NewTargetNotifier(0)

	done := make(chan struct{})
	var progressed bool
	var werr error
	go func() {
		progressed, werr = n.WaitFor(context.Background(), 10, 2*time.Second)
		close(done)
	}()

	waitForWaiterCount(t, n, 1)
	assert.True(t, n.Advance(10))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by Advance to target")
	}
	require.NoError(t, werr)
	assert.True(t, progressed)
	assert.Equal(t, 0, n.WaiterCount())
}

// TestTargetNotifier_TargetedWakeup is the core property: a small advance wakes
// only the waiters it satisfies, leaving farther-target waiters parked.
func TestTargetNotifier_TargetedWakeup(t *testing.T) {
	n := NewTargetNotifier(0)

	nearDone := make(chan struct{})
	farDone := make(chan struct{})
	go func() { n.WaitFor(context.Background(), 5, 5*time.Second); close(nearDone) }()
	go func() { n.WaitFor(context.Background(), 100, 5*time.Second); close(farDone) }()

	waitForWaiterCount(t, n, 2)

	// Advance past the near target but well below the far target.
	assert.True(t, n.Advance(5))

	select {
	case <-nearDone:
	case <-time.After(2 * time.Second):
		t.Fatal("near-target waiter should have woken on Advance(5)")
	}

	// The far waiter must still be parked — this is what saves the wakeups.
	waitForWaiterCount(t, n, 1)
	select {
	case <-farDone:
		t.Fatal("far-target waiter must NOT wake on an advance below its target")
	case <-time.After(testShortWait):
	}

	// Advancing to the far target releases it.
	assert.True(t, n.Advance(100))
	select {
	case <-farDone:
	case <-time.After(2 * time.Second):
		t.Fatal("far-target waiter should have woken on Advance(100)")
	}
	assert.Equal(t, 0, n.WaiterCount())
}

func TestTargetNotifier_MonotonicAdvance(t *testing.T) {
	n := NewTargetNotifier(10)

	assert.False(t, n.Advance(10), "advance to equal value is a no-op")
	assert.False(t, n.Advance(5), "advance to a smaller value is a no-op")
	assert.Equal(t, uint64(10), n.Load())

	assert.True(t, n.Advance(11))
	assert.Equal(t, uint64(11), n.Load())
	assert.False(t, n.Advance(11))
}

func TestTargetNotifier_WakeAll(t *testing.T) {
	n := NewTargetNotifier(0)

	const nWaiters = 8
	var wg sync.WaitGroup
	progressedCount := int32(0)
	for i := 0; i < nWaiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// High targets so only WakeAll can release them.
			progressed, err := n.WaitFor(context.Background(), 1_000_000, 5*time.Second)
			assert.NoError(t, err)
			if progressed {
				atomic.AddInt32(&progressedCount, 1)
			}
		}()
	}

	waitForWaiterCount(t, n, nWaiters)
	n.WakeAll()

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("WakeAll did not release all waiters")
	}

	assert.Equal(t, 0, n.WaiterCount())
	// WakeAll did not advance the value, so no waiter reports progress.
	assert.Equal(t, int32(0), atomic.LoadInt32(&progressedCount))
	assert.Equal(t, uint64(0), n.Load())
}

func TestTargetNotifier_ContextCancel(t *testing.T) {
	n := NewTargetNotifier(0)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := n.WaitFor(ctx, 10, 5*time.Second)
		done <- err
	}()

	waitForWaiterCount(t, n, 1)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("context cancel did not unblock WaitFor")
	}
	// The waiter must deregister itself on cancel — no leak.
	waitForWaiterCount(t, n, 0)
}

func TestTargetNotifier_ContextDeadline(t *testing.T) {
	n := NewTargetNotifier(0)
	ctx, cancel := context.WithTimeout(context.Background(), testShortWait)
	defer cancel()

	_, err := n.WaitFor(ctx, 10, 5*time.Second)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	waitForWaiterCount(t, n, 0)
}

// TestTargetNotifier_ProgressWithoutReachingTarget verifies the stall-vs-progress
// distinction: a value that advances but does not reach the waiter's target must
// (a) NOT wake the waiter early, and (b) be reported as progressed on timeout, so
// callers do not mistake a slowly-advancing value for a stalled one.
func TestTargetNotifier_ProgressWithoutReachingTarget(t *testing.T) {
	n := NewTargetNotifier(0)

	type result struct {
		progressed bool
		elapsed    time.Duration
	}
	res := make(chan result, 1)
	start := time.Now()
	go func() {
		p, err := n.WaitFor(context.Background(), 100, testWaitTimeout)
		assert.NoError(t, err)
		res <- result{p, time.Since(start)}
	}()

	waitForWaiterCount(t, n, 1)
	// Advance below the target: the value moved, but the waiter cannot proceed.
	time.Sleep(testShortWait)
	assert.True(t, n.Advance(50))

	r := <-res
	// It must have parked for the full timeout (not woken by the sub-target advance).
	assert.GreaterOrEqual(t, r.elapsed, testWaitTimeout,
		"waiter woke before timeout despite target not being reached")
	// And it must report progress, because the value did advance meanwhile.
	assert.True(t, r.progressed, "advance below target must still count as progress")
	assert.Equal(t, 0, n.WaiterCount())
}

// TestTargetNotifier_StallReported verifies that a full timeout with no advance
// at all is reported as progressed=false (the caller's stall signal).
func TestTargetNotifier_StallReported(t *testing.T) {
	n := NewTargetNotifier(0)
	progressed, err := n.WaitFor(context.Background(), 100, testShortWait)
	require.NoError(t, err)
	assert.False(t, progressed, "no advance within timeout must report no progress")
	assert.Equal(t, 0, n.WaiterCount())
}

// TestTargetNotifier_NoLostWakeup hammers the register-vs-advance race: for each
// round a waiter targets current+1 while another goroutine advances to exactly
// that value. The waiter must always be released (never hang).
func TestTargetNotifier_NoLostWakeup(t *testing.T) {
	n := NewTargetNotifier(0)
	for i := uint64(1); i <= 2000; i++ {
		target := i
		done := make(chan bool, 1)
		go func() {
			p, err := n.WaitFor(context.Background(), target, 2*time.Second)
			assert.NoError(t, err)
			done <- p
		}()
		// Race the advance against the registration.
		go n.Advance(target)

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("lost wakeup: waiter for target %d never released", target)
		}
	}
	waitForWaiterCount(t, n, 0)
	assert.Equal(t, uint64(2000), n.Load())
}

// TestTargetNotifier_ConcurrentStress runs many waiters against a stream of small
// advances plus periodic WakeAll, asserting no panic, no leak, and that every
// waiter eventually returns. Meant to be run under -race.
func TestTargetNotifier_ConcurrentStress(t *testing.T) {
	n := NewTargetNotifier(0)
	const (
		nWaiters  = 64
		maxTarget = 500
	)
	var wg sync.WaitGroup

	stop := make(chan struct{})
	// Advancer: many small steps.
	go func() {
		v := uint64(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			v++
			n.Advance(v)
			time.Sleep(time.Microsecond * 50)
		}
	}()
	// Occasional WakeAll to exercise the shutdown-style path.
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				n.WakeAll()
			}
		}
	}()

	for i := 0; i < nWaiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i)))
			for j := 0; j < 50; j++ {
				target := uint64(rng.Intn(maxTarget) + 1)
				_, err := n.WaitFor(context.Background(), target, 20*time.Millisecond)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()
	close(stop)
	// Give the background goroutines a moment to observe stop.
	time.Sleep(10 * time.Millisecond)
	waitForWaiterCount(t, n, 0)
}
