// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newTestGuard(t *testing.T, cpu float64, memBytes int64) *guard {
	t.Helper()
	paramtable.Init()
	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: cpu, Memory: memBytes})
	return g
}

// This is the core property: admission consults the ledger only. Three tasks
// each estimated at 30 units must fill a 100 unit budget even though none of
// them has touched a byte of memory yet -- exactly the situation in issue
// #52180 where tasks were admitted while earlier ones had not yet started
// downloading.
//
// The observed-memory reader is patched to record every call rather than to
// return a convenient zero: a mock that is merely *allowed* to go unused would
// prove nothing. Any admission path that peeks at live memory trips the
// assertion below, and the probe at the end proves the patch was really live so
// that the assertion cannot pass vacuously.
func TestAcquireChargesCommitmentNotObservation(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	var observed atomic.Int32
	// Both flags are atomic because a mockey patch is process-wide: paramtable
	// and friends run background goroutines, and any of them calling
	// GetUsedMemoryCount would land in this body concurrently with the test.
	var probing atomic.Bool
	mk := mockey.Mock(hardware.GetUsedMemoryCount).To(func() uint64 {
		observed.Add(1)
		if !probing.Load() {
			t.Errorf("admission must not consult observed memory; the ledger is the only input")
		}
		return 0
	}).Build()
	defer mk.UnPatch()

	for i := int64(1); i <= 3; i++ {
		ok, _, err := g.TryAcquire(i, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 30})
		require.NoError(t, err)
		require.True(t, ok, "task %d should be admitted", i)
	}

	ok, _, err := g.TryAcquire(4, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 30})
	require.NoError(t, err)
	assert.False(t, ok, "the fourth task must queue: 90+30 > 100")

	// Blocking admission must not consult observed memory either.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	assert.Error(t, g.Acquire(ctx, 5, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 30}))

	require.Zero(t, observed.Load(), "admission read observed memory")

	probing.Store(true)
	_ = hardware.GetUsedMemoryCount()
	require.Equal(t, int32(1), observed.Load(),
		"mockey did not patch hardware.GetUsedMemoryCount, so the assertion above proved nothing "+
			"(build the test with -gcflags=\"all=-N -l\")")
}

func TestReleaseReturnsBudget(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	ok, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 80})
	require.NoError(t, err)
	require.True(t, ok)

	ok, _, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 80})
	require.False(t, ok)

	g.Release(1)

	ok, _, err = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 80})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestReleaseIsIdempotent(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	_, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 40})
	require.NoError(t, err)

	g.Release(1)
	g.Release(1)
	g.Release(1)
	// An id the ledger has never seen must be inert too.
	g.Release(999)

	snap := g.Snapshot()
	assert.Equal(t, int64(0), snap.Reserved.Memory, "double release must not create budget")
	assert.Equal(t, float64(0), snap.Reserved.CPU, "double release must not create budget")

	// The budget must not have grown: a full-node task still fits exactly once.
	ok, _, err := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 100})
	require.NoError(t, err)
	require.True(t, ok)
	ok, _, err = g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{Memory: 1})
	require.NoError(t, err)
	assert.False(t, ok, "releasing three times must not have widened the budget")
}

// A task larger than the node can never be satisfied. Blocking on it would
// deadlock the queue forever, so it must fail immediately and let the caller
// report it upward.
func TestOversizedTaskFailsImmediately(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	ok, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 500})
	assert.False(t, ok)
	assert.ErrorIs(t, err, ErrResourceExhausted)

	// A CPU request beyond the node is equally impossible.
	ok, _, err = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 16, Memory: 1})
	assert.False(t, ok)
	assert.ErrorIs(t, err, ErrResourceExhausted)

	// The deadline is generous on purpose: if Acquire ever queued an impossible
	// request it would sit here and come back with a deadline error instead of
	// ErrResourceExhausted, which is the failure this asserts against.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err = g.Acquire(ctx, 3, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 500})
	assert.ErrorIs(t, err, ErrResourceExhausted, "Acquire must not block on an impossible request")
	assert.Less(t, time.Since(start), 500*time.Millisecond, "Acquire waited on an unsatisfiable request")
	assert.Zero(t, g.waiterCount(), "an impossible request must not be queued")
}

func TestAcquireBlocksUntilRelease(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 80})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 80})
	}()

	select {
	case <-done:
		t.Fatal("Acquire returned before budget was released")
	case <-time.After(200 * time.Millisecond):
	}

	g.Release(1)
	require.NoError(t, <-done)

	// The reservation really landed in the ledger, and the waiter list is clean.
	snap := g.Snapshot()
	assert.Equal(t, int64(80), snap.Reserved.Memory)
	assert.Eventually(t, func() bool { return g.waiterCount() == 0 }, time.Second, 10*time.Millisecond)
}

func TestAcquireRespectsContextCancellation(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 90})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 90})
	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrResourceExhausted)

	// Giving up must not leave a phantom reservation or a stale waiter behind.
	assert.Equal(t, int64(90), g.Snapshot().Reserved.Memory)
	assert.Eventually(t, func() bool { return g.waiterCount() == 0 }, time.Second, 10*time.Millisecond)
}

// Budget that is already free must be taken without ever touching the waiter
// channel, so a Release that happened before the call cannot strand it.
func TestAcquireSucceedsImmediatelyWhenBudgetIsFree(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 80})
	require.NoError(t, err)
	require.Zero(t, g.waiterCount())

	// No waiter exists yet, so this wakes nobody -- the budget is simply free by
	// the time Acquire looks at the ledger.
	g.Release(1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 80}))
}

// A departing waiter frees budget just as surely as a Release does: while it
// was queued, the head-of-line rule was holding budget back on its behalf.
// Whoever it was blocking must be woken, or it sleeps until its own deadline
// even though the ledger would admit it right now.
func TestGivingUpWakesTheWaitersItWasBlocking(t *testing.T) {
	g := newTestGuard(t, 100, 100)
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key, "true")
	defer pt.Reset(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})
	require.NoError(t, err)

	// The head waits for more than is left, and will give up on its own.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	// This one fits behind the head (60+30 <= 100) but the head-of-line rule
	// holds it back for as long as the head is queued.
	small := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		small <- g.Acquire(ctx, 3, taskcommon.Compaction, taskresource.Requirement{Memory: 30})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 2 }, time.Second, 5*time.Millisecond)

	// Nothing is released here. The head simply times out and leaves, and that
	// alone must let the small task in -- well before its own 10s deadline.
	select {
	case err := <-small:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("a waiter that gave up did not wake the tasks its reservation was blocking")
	}
}

// Without a head-of-line reservation, a stream of small tasks keeps the ledger
// just full enough that a large waiting task never gets in.
func TestHeadOfLineReservationPreventsStarvation(t *testing.T) {
	g := newTestGuard(t, 100, 100)
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key, "true")
	defer pt.Reset(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})
	require.NoError(t, err)

	// A large task starts waiting.
	waiting := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waiting <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 10*time.Millisecond)

	// A small task arrives while the large one waits; it must not jump ahead.
	ok, _, err := g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{Memory: 30})
	require.NoError(t, err)
	assert.False(t, ok, "small task must not consume budget reserved for the waiting large task")

	g.Release(1)
	require.NoError(t, <-waiting)
}

// With the reservation off, the same interleaving lets the latecomer in. This
// is the control for the test above: it shows the previous refusal comes from
// the head-of-line rule and not from some unrelated shortage.
func TestHeadOfLineReservationCanBeDisabled(t *testing.T) {
	g := newTestGuard(t, 100, 100)
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key, "false")
	defer pt.Reset(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	waiting := make(chan error, 1)
	go func() {
		waiting <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 10*time.Millisecond)

	ok, _, err := g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{Memory: 30})
	require.NoError(t, err)
	assert.True(t, ok, "with the reservation off the small task starves the large one")

	<-waiting
}

// Re-reserving a live task id must not be charged twice, otherwise a retried
// admission would leak budget that Release can never give back.
func TestTryAcquireIsIdempotentForLiveTask(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	for i := 0; i < 3; i++ {
		ok, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 40})
		require.NoError(t, err)
		require.True(t, ok)
	}

	assert.Equal(t, int64(40), g.Snapshot().Reserved.Memory)
	assert.Equal(t, float64(1), g.Snapshot().Reserved.CPU)

	g.Release(1)
	assert.Equal(t, int64(0), g.Snapshot().Reserved.Memory)

	// The same id can come back later as a retry. It must be charged afresh,
	// which is also what proves Release dropped the ledger entry rather than
	// merely zeroing the balance -- a surviving entry would make every future
	// admission of this id free, and the node oversubscribed.
	ok, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 40})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(40), g.Snapshot().Reserved.Memory, "a re-admitted task must be charged again")
}

func TestConcurrentAcquireReleaseKeepsLedgerConsistent(t *testing.T) {
	g := newTestGuard(t, 1000, 1_000_000)

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			ok, _, err := g.TryAcquire(id, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 1000})
			if err == nil && ok {
				g.Release(id)
			}
		}(int64(i))
	}
	wg.Wait()

	snap := g.Snapshot()
	assert.Equal(t, int64(0), snap.Reserved.Memory)
	assert.Equal(t, float64(0), snap.Reserved.CPU)
}

// The budget is deliberately too small for everyone, so admissions and
// releases race against a genuinely contended ledger rather than sailing past
// it. reserved <= budget must hold at every observable moment, and the ledger
// must drain to exactly zero once every admitted task has released.
func TestConcurrentContendedLedgerNeverOversubscribes(t *testing.T) {
	const budget = int64(10_000)
	g := newTestGuard(t, 1000, budget)

	stop := make(chan struct{})
	var watcherDone sync.WaitGroup
	watcherDone.Add(1)
	var breach atomic.Int64
	go func() {
		defer watcherDone.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if snap := g.Snapshot(); snap.Reserved.Memory > snap.Total.Memory {
				breach.Store(snap.Reserved.Memory)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	var admitted atomic.Int64
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				ok, _, err := g.TryAcquire(id, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 1000})
				assert.NoError(t, err)
				if ok {
					admitted.Add(1)
					g.Release(id)
				}
			}
		}(int64(i))
	}
	wg.Wait()
	close(stop)
	watcherDone.Wait()

	assert.Zero(t, breach.Load(), "reserved memory exceeded the node budget")
	assert.Positive(t, admitted.Load(), "nothing was ever admitted; the test proved nothing")

	snap := g.Snapshot()
	assert.Equal(t, int64(0), snap.Reserved.Memory)
	assert.Equal(t, float64(0), snap.Reserved.CPU)
}

func TestSnapshotReportsTotals(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	_, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})
	require.NoError(t, err)

	snap := g.Snapshot()
	assert.Equal(t, int64(1000), snap.Total.Memory)
	assert.Equal(t, float64(8), snap.Total.CPU)
	assert.Equal(t, int64(300), snap.Reserved.Memory)
	assert.Equal(t, float64(2), snap.Reserved.CPU)
	assert.False(t, snap.Frozen)
	assert.Equal(t, int64(0), snap.NonTask)
}

// Without an override the guard must fall back to the real node capacity, so
// the production path is not left untested by the injection helper.
func TestGuardWithoutOverrideUsesNodeCapacity(t *testing.T) {
	paramtable.Init()
	g := NewGuard()

	want := taskresource.NodeCapacity()
	snap := g.Snapshot()
	assert.Equal(t, want.Memory, snap.Total.Memory)
	assert.Equal(t, want.CPU, snap.Total.CPU)
}

// nonTask memory is charged against the same budget tasks draw on, so it can
// only ever shrink what admission has to give away -- never widen it. Task 6
// drives this field; the arithmetic it depends on is pinned here.
func TestNonTaskMemoryShrinksTheBudget(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	g.mu.Lock()
	g.nonTask = 400
	g.mu.Unlock()

	assert.Equal(t, int64(600), g.Snapshot().Total.Memory)

	ok, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 700})
	assert.False(t, ok)
	assert.ErrorIs(t, err, ErrResourceExhausted, "700 no longer fits the node once 400 is spoken for")

	// More non-task memory than the node has must floor the budget at zero, not
	// wrap negative and let everything in.
	g.mu.Lock()
	g.nonTask = 5000
	g.mu.Unlock()
	assert.Equal(t, int64(0), g.Snapshot().Total.Memory)
}

// While frozen, nothing new is admitted, but this is "not right now" rather
// than "never": the error must stay nil so callers keep retrying.
func TestFrozenGuardRefusesAdmissionWithoutError(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()

	ok, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 10})
	assert.False(t, ok)
	assert.NoError(t, err, "a freeze is temporary; it must not look like exhaustion")
	assert.True(t, g.Snapshot().Frozen)
	assert.Equal(t, int64(0), g.Snapshot().Reserved.Memory, "a frozen guard must not charge anything")

	g.mu.Lock()
	g.frozen = false
	g.mu.Unlock()

	ok, _, err = g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 10})
	require.NoError(t, err)
	assert.True(t, ok, "thawing must let the same task in")
}

// The second return value is the headroom left once the call has been decided,
// so a caller can log how far short it fell and Task 12 can export it. CPU and
// memory are deliberately given different numbers here: a test that used the
// same value for both would not notice the two fields being swapped.
func TestTryAcquireReportsRemainingHeadroom(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	// Admitted: the headroom reported must already exclude this task's charge,
	// so it agrees with a Snapshot taken straight afterwards.
	ok, avail, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, float64(6), avail.CPU, "admitted: headroom must be net of the charge just made")
	assert.Equal(t, int64(700), avail.Memory, "admitted: headroom must be net of the charge just made")

	snap := g.Snapshot()
	assert.Equal(t, snap.Total.CPU-snap.Reserved.CPU, avail.CPU)
	assert.Equal(t, snap.Total.Memory-snap.Reserved.Memory, avail.Memory)

	// Short: nothing was charged, so the headroom is unchanged and shows the
	// caller it was 200 bytes short of the 900 it asked for.
	ok, avail, err = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 900})
	require.NoError(t, err)
	require.False(t, ok)
	assert.Equal(t, float64(6), avail.CPU)
	assert.Equal(t, int64(700), avail.Memory)

	// Exhausted: the headroom still describes the node, which is what makes the
	// "impossible" verdict legible in a log line.
	ok, avail, err = g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 5000})
	require.ErrorIs(t, err, ErrResourceExhausted)
	require.False(t, ok)
	assert.Equal(t, float64(6), avail.CPU)
	assert.Equal(t, int64(700), avail.Memory)

	// Re-requesting a live id reports headroom too, without charging again.
	ok, avail, err = g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, float64(6), avail.CPU)
	assert.Equal(t, int64(700), avail.Memory)
}

// Headroom goes negative when the budget shrinks below what is already
// committed. That is the over-commitment Task 6 needs to see, so it must not be
// clamped away to zero.
func TestHeadroomGoesNegativeWhenOverCommitted(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	_, _, err := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})
	require.NoError(t, err)

	g.mu.Lock()
	g.nonTask = 900 // budget drops to 100, but 300 is already committed
	g.mu.Unlock()

	_, avail, err := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(-200), avail.Memory, "over-commitment must be reported, not clamped")
}

// A running task holds its charge; a budget that shrinks under it does not
// un-admit it. Re-requesting must still say "you have it" rather than
// ErrResourceExhausted -- "impossible on this node, forever" is a verdict for
// work that has not started, and a running task acting on it would abort while
// still holding a reservation.
func TestShrunkBudgetDoesNotEvictALiveReservation(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	ok, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 800})
	require.NoError(t, err)
	require.True(t, ok)

	// The node shrinks under the running task -- exactly what Task 6 does when
	// it raises nonTask after observing memory outside the ledger.
	g.mu.Lock()
	g.nonTask = 700 // budget is now 300, less than the 800 already committed
	g.mu.Unlock()

	ok, _, err = g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 800})
	require.NoError(t, err, "a live reservation must not be reported as impossible")
	assert.True(t, ok)
	assert.Equal(t, int64(800), g.Snapshot().Reserved.Memory, "and must not be charged twice")

	// The blocking form must not queue it either.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, g.Acquire(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 800}))

	// A *new* task of the same size is still correctly impossible.
	ok, _, err = g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{Memory: 800})
	assert.False(t, ok)
	assert.ErrorIs(t, err, ErrResourceExhausted)
}

// A freeze stops new charges; it does not un-admit work that is already
// running. A task holding a reservation must still be told it has one, or it
// would queue behind its own charge -- holding budget while waiting for budget,
// and blocking everyone behind it as head of the line.
func TestFrozenGuardStillHonoursExistingReservations(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	ok, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 100})
	require.NoError(t, err)
	require.True(t, ok)

	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()

	ok, _, err = g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 100})
	require.NoError(t, err)
	assert.True(t, ok, "a live reservation must survive a freeze")
	assert.Equal(t, int64(100), g.Snapshot().Reserved.Memory, "and must not be charged twice")

	// Nor may the blocking form queue it.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, g.Acquire(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 100}),
		"a task that already holds its reservation must not wait for one")
}

// A queued request can become impossible while it waits, if the budget shrinks
// under it. It must then be told so rather than waiting out its deadline for
// something that can no longer happen.
func TestQueuedRequestFailsOnceItNoLongerFitsTheNode(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	_, _, err := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 80})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	// The node shrinks below what the queued task asked for.
	g.setCapacityForTest(taskresource.Capacity{CPU: 100, Memory: 50})
	g.Release(1)

	select {
	case err := <-done:
		assert.ErrorIs(t, err, ErrResourceExhausted)
	case <-time.After(3 * time.Second):
		t.Fatal("a queued request that became impossible kept waiting")
	}
}

func TestGetGuardIsProcessWideSingleton(t *testing.T) {
	paramtable.Init()
	a := GetGuard()
	b := GetGuard()
	require.NotNil(t, a)
	assert.Same(t, a, b)
}
