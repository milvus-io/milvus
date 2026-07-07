package syncutil

import (
	"context"
	"sync"
	"time"
)

// TargetNotifier coordinates goroutines that wait for a monotonically
// non-decreasing uint64 value (for example a QueryNode delegator tSafe) to
// reach a per-waiter target.
//
// Unlike a broadcast condition variable (see ContextCond), advancing the value
// wakes ONLY the waiters whose target has been reached. This avoids the
// thundering-herd cost of waking every waiter on every small advance when most
// of them still cannot make progress: under high-concurrency strong-consistency
// workloads many requests wait on the same value with different targets, and a
// broadcast forces every one of them to re-check and re-park on each tick.
//
// The zero value is not usable; construct one with NewTargetNotifier.
type TargetNotifier struct {
	mu          sync.Mutex
	current     uint64
	lastAdvance time.Time
	waiters     map[*targetWaiter]struct{}
}

// targetWaiter is a single parked goroutine waiting for current to reach target.
// ch is closed exactly once, by whichever of Advance/WakeAll releases it; the
// releaser removes the waiter from the set under the lock, so a WaitFor that
// instead leaves via ctx/timeout only ever deletes (never closes) the entry.
type targetWaiter struct {
	target uint64
	ch     chan struct{}
}

// NewTargetNotifier creates a TargetNotifier seeded with an initial value.
func NewTargetNotifier(initial uint64) *TargetNotifier {
	return &TargetNotifier{
		current:     initial,
		lastAdvance: time.Now(),
		waiters:     make(map[*targetWaiter]struct{}),
	}
}

// Load returns the current value.
func (n *TargetNotifier) Load() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.current
}

// Advance sets the value to next and wakes every waiter whose target has been
// reached. It is a monotonic no-op that returns false when next <= current, so
// concurrent callers can never move the value backwards.
func (n *TargetNotifier) Advance(next uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if next <= n.current {
		return false
	}
	n.current = next
	n.lastAdvance = time.Now()
	for w := range n.waiters {
		if w.target <= next {
			close(w.ch)
			delete(n.waiters, w)
		}
	}
	return true
}

// WakeAll wakes every waiter regardless of target, without changing the value.
// Use it on shutdown, or when a higher-level serviceability condition flips, so
// parked waiters unblock and re-evaluate their own exit condition.
func (n *TargetNotifier) WakeAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for w := range n.waiters {
		close(w.ch)
		delete(n.waiters, w)
	}
}

// WaiterCount returns the number of goroutines currently parked in WaitFor.
// Intended for observability (for example a gauge of active tSafe waiters).
func (n *TargetNotifier) WaiterCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.waiters)
}

// WaitFor blocks until one of: the value reaches target (targeted wake),
// WakeAll is called, ctx is done, or waitTimeout elapses.
//
// It returns progressed=true when the value advanced at all while this call was
// waiting (including reaching target), and progressed=false only when
// waitTimeout elapsed with no advance observed. This lets the caller own its
// stall policy (for example: fast-fail only after the value has been frozen for
// an interval) without this primitive baking one in. err is non-nil only when
// ctx is canceled or expires, in which case it carries context.Cause(ctx).
//
// The check-and-register is done under the same lock Advance takes, so a value
// that reaches target concurrently with registration can never be missed
// (no lost wakeup).
func (n *TargetNotifier) WaitFor(ctx context.Context, target uint64, waitTimeout time.Duration) (progressed bool, err error) {
	n.mu.Lock()
	if n.current >= target {
		n.mu.Unlock()
		return true, nil
	}
	w := &targetWaiter{target: target, ch: make(chan struct{})}
	n.waiters[w] = struct{}{}
	startAdvance := n.lastAdvance
	n.mu.Unlock()

	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	select {
	case <-w.ch:
		// Released by Advance (target reached) or WakeAll; the releaser already
		// removed w from the set under the lock.
		return n.advancedSince(startAdvance), nil
	case <-ctx.Done():
		return n.finish(w, startAdvance), context.Cause(ctx)
	case <-timer.C:
		return n.finish(w, startAdvance), nil
	}
}

// advancedSince reports whether the value advanced after the given timestamp.
func (n *TargetNotifier) advancedSince(since time.Time) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastAdvance.After(since)
}

// finish deregisters w (if it is still registered) and reports whether the
// value advanced since the given timestamp, under a single lock acquisition.
// Deleting an already-released waiter is a safe no-op.
func (n *TargetNotifier) finish(w *targetWaiter, since time.Time) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.waiters, w)
	return n.lastAdvance.After(since)
}
