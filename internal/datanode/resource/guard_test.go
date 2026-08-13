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

// mustAcquire admits a task a test is setting up, and fails the test if the
// guard refuses it -- a setup step that silently did not happen would leave the
// assertions that follow measuring nothing.
func mustAcquire(t *testing.T, g *guard, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) {
	t.Helper()
	ok, _ := g.TryAcquire(taskID, taskType, req)
	require.True(t, ok, "setup: task %d should have been admitted", taskID)
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
		ok, _ := g.TryAcquire(i, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 30})
		require.True(t, ok, "task %d should be admitted", i)
	}

	ok, _ := g.TryAcquire(4, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 30})
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

	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 80})
	require.True(t, ok)

	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 80})
	require.False(t, ok)

	g.Release(1)

	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 80})
	assert.True(t, ok)
}

func TestReleaseIsIdempotent(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 40})

	g.Release(1)
	g.Release(1)
	g.Release(1)
	// An id the ledger has never seen must be inert too.
	g.Release(999)

	snap := g.Snapshot()
	assert.Equal(t, int64(0), snap.Reserved.Memory, "double release must not create budget")
	assert.Equal(t, float64(0), snap.Reserved.CPU, "double release must not create budget")

	// The budget must not have grown: a full-node task still fits exactly once.
	ok, _ := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 100})
	require.True(t, ok)
	ok, _ = g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{Memory: 1})
	assert.False(t, ok, "releasing three times must not have widened the budget")
}

// A request beyond the node in the CPU dimension is oversized too: the node is
// two-dimensional, and either dimension alone makes a task unable to share it.
func TestOversizednessIsJudgedOnBothDimensions(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	ok, _ := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 10})
	require.True(t, ok)

	// Memory fits easily; the core count does not.
	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 16, Memory: 1})
	assert.False(t, ok, "a task beyond the node's cores must wait for it to drain")

	g.Release(1)
	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 16, Memory: 1})
	require.True(t, ok)
	assert.Equal(t, int64(2), g.Snapshot().ExclusiveTaskID, "and then runs alone")
}

func TestAcquireBlocksUntilRelease(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 80})

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

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 90})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 90})
	assert.ErrorIs(t, err, context.DeadlineExceeded, "ctx is the only error Acquire can return")

	// Giving up must not leave a phantom reservation or a stale waiter behind.
	assert.Equal(t, int64(90), g.Snapshot().Reserved.Memory)
	assert.Eventually(t, func() bool { return g.waiterCount() == 0 }, time.Second, 10*time.Millisecond)
}

// Budget that is already free must be taken without ever touching the waiter
// channel, so a Release that happened before the call cannot strand it.
func TestAcquireSucceedsImmediatelyWhenBudgetIsFree(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 80})
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

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})

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

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})

	// A large task starts waiting.
	waiting := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waiting <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 10*time.Millisecond)

	// A small task arrives while the large one waits; it must not jump ahead.
	ok, _ := g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{Memory: 30})
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

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	waiting := make(chan error, 1)
	go func() {
		waiting <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 10*time.Millisecond)

	ok, _ := g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{Memory: 30})
	assert.True(t, ok, "with the reservation off the small task starves the large one")

	<-waiting
}

// Re-reserving a live task id must not be charged twice, otherwise a retried
// admission would leak budget that Release can never give back.
func TestTryAcquireIsIdempotentForLiveTask(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	for i := 0; i < 3; i++ {
		ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 40})
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
	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 40})
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
			ok, _ := g.TryAcquire(id, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 1000})
			if ok {
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
				ok, _ := g.TryAcquire(id, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 1000})
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

	mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})

	snap := g.Snapshot()
	assert.Equal(t, int64(1000), snap.Total.Memory)
	assert.Equal(t, float64(8), snap.Total.CPU)
	assert.Equal(t, int64(300), snap.Reserved.Memory)
	assert.Equal(t, float64(2), snap.Reserved.CPU)
	assert.False(t, snap.Frozen)
	assert.Equal(t, int64(0), snap.NonTask)
	assert.Equal(t, int64(0), snap.ExclusiveTaskID, "an ordinary task does not take the node exclusively")
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

	// 700 fits the node but not what is left of it, so it waits. The node is
	// empty here, which is what makes this discriminating: were oversizedness
	// judged against the reduced budget instead of the node's capacity, this
	// request would be admitted for *exclusive* execution, and a passing spike
	// in non-task memory would serialize the whole node.
	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 700})
	assert.False(t, ok, "700 no longer fits once 400 is spoken for")
	assert.Zero(t, g.Snapshot().ExclusiveTaskID,
		"a task that fits the node must never be treated as oversized, however tight the budget")

	// More non-task memory than the node has must floor the budget at zero, not
	// wrap negative and let everything in.
	g.mu.Lock()
	g.nonTask = 5000
	g.mu.Unlock()
	assert.Equal(t, int64(0), g.Snapshot().Total.Memory)
}

// While frozen, nothing new is admitted, but this is "not right now" rather
// than "never": the very same request goes through once the freeze lifts.
func TestFrozenGuardRefusesAdmissionTemporarily(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()

	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 10})
	assert.False(t, ok)
	assert.True(t, g.Snapshot().Frozen)
	assert.Equal(t, int64(0), g.Snapshot().Reserved.Memory, "a frozen guard must not charge anything")

	g.mu.Lock()
	g.frozen = false
	g.mu.Unlock()

	ok, _ = g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 10})
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
	ok, avail := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})
	require.True(t, ok)
	assert.Equal(t, float64(6), avail.CPU, "admitted: headroom must be net of the charge just made")
	assert.Equal(t, int64(700), avail.Memory, "admitted: headroom must be net of the charge just made")

	snap := g.Snapshot()
	assert.Equal(t, snap.Total.CPU-snap.Reserved.CPU, avail.CPU)
	assert.Equal(t, snap.Total.Memory-snap.Reserved.Memory, avail.Memory)

	// Short: nothing was charged, so the headroom is unchanged and shows the
	// caller it was 200 bytes short of the 900 it asked for.
	ok, avail = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 900})
	require.False(t, ok)
	assert.Equal(t, float64(6), avail.CPU)
	assert.Equal(t, int64(700), avail.Memory)

	// Oversized, so it waits for the node to drain: the headroom still describes
	// the node, which is what makes the refusal legible in a log line.
	ok, avail = g.TryAcquire(3, taskcommon.Index, taskresource.Requirement{CPU: 1, Memory: 5000})
	require.False(t, ok)
	assert.Equal(t, float64(6), avail.CPU)
	assert.Equal(t, int64(700), avail.Memory)

	// Re-requesting a live id reports headroom too, without charging again.
	ok, avail = g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})
	require.True(t, ok)
	assert.Equal(t, float64(6), avail.CPU)
	assert.Equal(t, int64(700), avail.Memory)
}

// Headroom goes negative when the budget shrinks below what is already
// committed. That is the over-commitment Task 6 needs to see, so it must not be
// clamped away to zero.
func TestHeadroomGoesNegativeWhenOverCommitted(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{CPU: 2, Memory: 300})

	g.mu.Lock()
	g.nonTask = 900 // budget drops to 100, but 300 is already committed
	g.mu.Unlock()

	_, avail := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 10})
	assert.Equal(t, int64(-200), avail.Memory, "over-commitment must be reported, not clamped")
}

// A running task holds its charge; a budget that shrinks under it does not
// un-admit it. Re-requesting must still say "you have it" rather than sending
// it to the queue: a running task told to wait would hold a reservation while
// waiting for one, blocking everyone behind it as head of the line.
func TestShrunkBudgetDoesNotEvictALiveReservation(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	ok, _ := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 800})
	require.True(t, ok)

	// The node shrinks under the running task -- exactly what Task 6 does when
	// it raises nonTask after observing memory outside the ledger.
	g.mu.Lock()
	g.nonTask = 700 // budget is now 300, less than the 800 already committed
	g.mu.Unlock()

	ok, _ = g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 800})
	assert.True(t, ok, "a live reservation must not be sent back to the queue")
	assert.Equal(t, int64(800), g.Snapshot().Reserved.Memory, "and must not be charged twice")

	// The blocking form must not queue it either.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, g.Acquire(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 800}))

	// A *new* task of the same size still has to wait for the budget it needs.
	ok, _ = g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{Memory: 800})
	assert.False(t, ok)
}

// A freeze stops new charges; it does not un-admit work that is already
// running. A task holding a reservation must still be told it has one, or it
// would queue behind its own charge -- holding budget while waiting for budget,
// and blocking everyone behind it as head of the line.
func TestFrozenGuardStillHonoursExistingReservations(t *testing.T) {
	g := newTestGuard(t, 8, 1000)

	ok, _ := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 100})
	require.True(t, ok)

	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()

	ok, _ = g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 100})
	assert.True(t, ok, "a live reservation must survive a freeze")
	assert.Equal(t, int64(100), g.Snapshot().Reserved.Memory, "and must not be charged twice")

	// Nor may the blocking form queue it.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, g.Acquire(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 100}),
		"a task that already holds its reservation must not wait for one")
}

// A queued request can outgrow the node while it waits, if the node shrinks
// under it. That does not fail it: it simply stops waiting for a gap and starts
// waiting for the node to drain, and then runs alone.
func TestQueuedRequestTurnsExclusiveWhenItOutgrowsTheNode(t *testing.T) {
	g := newTestGuard(t, 100, 100)

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})

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
		require.NoError(t, err, "a request larger than the node waits; it is never refused")
	case <-time.After(3 * time.Second):
		t.Fatal("a queued request that outgrew the node never ran")
	}
	assert.Equal(t, int64(2), g.Snapshot().ExclusiveTaskID, "and it holds the node alone")
}

// An oversized task is never refused outright. The coordinator deliberately
// places it on the emptiest worker; the worker's job is to run it alone, not
// to reject it.
func TestOversizedTaskWaitsInsteadOfFailing(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	// Something else already holds part of the node.
	ok, _ := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 10})
	require.True(t, ok)

	// Larger than the whole node.
	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 500})
	assert.False(t, ok, "oversized task must wait, not be admitted")

	// Draining the node lets it in.
	g.Release(1)
	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 500})
	assert.True(t, ok, "oversized task runs once the node is empty")
}

func TestOversizedTaskExcludesEveryoneElse(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 500})
	require.True(t, ok, "empty node admits the oversized task")
	assert.Equal(t, int64(1), g.Snapshot().ExclusiveTaskID)

	// Even a one-byte task must not join it.
	ok, _ = g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{Memory: 1})
	assert.False(t, ok, "nothing shares the node with an oversized task")

	g.Release(1)
	assert.Equal(t, int64(0), g.Snapshot().ExclusiveTaskID)

	ok, _ = g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{Memory: 1})
	assert.True(t, ok, "normal admission resumes once it finishes")
}

// Exclusivity is a promise made at admission, and the node growing underneath
// does not release the guard from it. Node capacity is not constant: the memory
// and CPU ratios are runtime config, so a task admitted as oversized can find
// itself fitting comfortably a moment later. It still owns the node until it
// releases -- otherwise it would end up sharing after all, which is precisely
// the concurrency that issue #52180 was about.
func TestGrowingTheNodeDoesNotBreakExclusivity(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 500})
	require.True(t, ok)
	require.Equal(t, int64(1), g.Snapshot().ExclusiveTaskID)

	// The node is now far bigger than the reservation it is holding.
	g.setCapacityForTest(taskresource.Capacity{CPU: 8, Memory: 10000})

	ok, _ = g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{Memory: 1})
	assert.False(t, ok, "a task admitted exclusively keeps the node to itself until it releases")
	assert.Equal(t, int64(1), g.Snapshot().ExclusiveTaskID)

	g.Release(1)
	ok, _ = g.TryAcquire(2, taskcommon.Compaction, taskresource.Requirement{Memory: 1})
	assert.True(t, ok)
}

// "Every previously admitted task has finished" is a statement about tasks, so
// both the oversized branch and Release read it off the ledger rather than off
// the reserved total. This is the total's side of that: the dust Release leaves
// behind must not survive an empty ledger. Nothing ever subtracts it again --
// Sub clamps at negative and never snaps to zero, and CPU requirements are
// fractional (estimate_import.go charges 0.1 per import) -- so from the moment
// it appears, a task sized exactly to the budget is refused forever on a node
// the ledger says is empty.
//
// The refusal needs the budget to be small enough for the residue to survive
// the addition: at a CPU budget of 8 an ulp is 1.8e-15 and 8 + 2.8e-17 rounds
// back to exactly 8, so the task still fits. The node is shrunk below that
// threshold here to show the consequence rather than to argue it away -- the
// dust is a random walk across a node's lifetime, and the exact-zero assertion
// above it holds at every budget.
func TestEmptyLedgerLeavesNoReservedResidue(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	for i := int64(1); i <= 3; i++ {
		mustAcquire(t, g, i, taskcommon.Import, taskresource.Requirement{CPU: 0.1, Memory: 1})
	}

	// Anti-vacuity: subtraction alone really would leave something behind. This
	// runs the same arithmetic Release runs, from the same starting value.
	g.mu.Lock()
	bySubtraction := g.reserved
	g.mu.Unlock()
	for i := 0; i < 3; i++ {
		bySubtraction = bySubtraction.Sub(taskresource.Requirement{CPU: 0.1, Memory: 1})
	}
	require.Positive(t, bySubtraction.CPU,
		"subtraction left no residue; this test would pass vacuously")

	for i := int64(1); i <= 3; i++ {
		g.Release(i)
	}

	snap := g.Snapshot()
	assert.Equal(t, float64(0), snap.Reserved.CPU, "an empty ledger means exactly zero reserved")
	assert.Equal(t, int64(0), snap.Reserved.Memory)

	// The consequence: on a budget fine enough to feel the dust, a task sized to
	// the whole budget still fits.
	g.setCapacityForTest(taskresource.Capacity{CPU: 0.1, Memory: 100})
	ok, _ := g.TryAcquire(4, taskcommon.Compaction, taskresource.Requirement{CPU: 0.1, Memory: 1})
	assert.True(t, ok, "a task sized exactly to the budget must fit an empty node")
}

// The other half of the same defect: a task charged nothing at all is still a
// task on the node. Sizing it by the reserved total would make it invisible and
// let it share the node with an oversized task.
func TestOversizedTaskWaitsForAZeroCostTaskToo(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{})
	require.Equal(t, int64(0), g.Snapshot().Reserved.Memory, "it is charged nothing")

	ok, _ := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 500})
	assert.False(t, ok, "nothing shares the node with an oversized task, not even a task charged nothing")

	g.Release(1)
	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 500})
	assert.True(t, ok)
}

// Taking the whole node must not become a way to jump the queue. An ordinary
// task is parked as head with the budget being held for it; an oversized
// latecomer finds the node empty and must leave it alone, or the head waits out
// the latecomer's entire runtime -- the starvation the head-of-line rule exists
// to prevent, arriving through the exclusive path.
func TestOversizedLatecomerDoesNotPreemptAnOrdinaryHead(t *testing.T) {
	g := newTestGuard(t, 8, 100)
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key, "true")
	defer pt.Reset(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key)

	// The node is empty but its budget is not: 70 fits the node and not the
	// budget, so the head stays parked without holding anything itself.
	g.mu.Lock()
	g.nonTask = 50
	g.mu.Unlock()

	waiting := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		waiting <- g.Acquire(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 5*time.Millisecond)
	g.mu.Lock()
	require.Empty(t, g.ledger, "the node really is empty; only the queue is not")
	g.mu.Unlock()

	ok, _ := g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 500})
	assert.False(t, ok, "an oversized latecomer must not take the node from the head it is reserved for")
	assert.Zero(t, g.Snapshot().ExclusiveTaskID)

	// And the head still gets what it was held out for.
	g.mu.Lock()
	g.nonTask = 0
	g.mu.Unlock()
	g.wakeWaiters()
	require.NoError(t, <-waiting)
	assert.Equal(t, int64(70), g.Snapshot().Reserved.Memory)
}

// An oversized task holds the line from wherever it is queued, not only from
// the front. Behind an ordinary head with the knob off it would otherwise be
// starved by exactly the trickle the special case exists to stop: it can never
// be admitted by a lucky gap, only by a drain the trickle prevents.
func TestOversizedWaiterHoldsTheLineFromBehindTheHead(t *testing.T) {
	g := newTestGuard(t, 8, 100)
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key, "false")
	defer pt.Reset(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key)

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})

	ordinary := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ordinary <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 70})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 5*time.Millisecond)

	oversized := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		oversized <- g.Acquire(ctx, 3, taskcommon.Index, taskresource.Requirement{Memory: 500})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 2 }, time.Second, 5*time.Millisecond)

	// The trickle: it fits behind the running task, and the head is ordinary, so
	// only the oversized task queued *second* can hold it back.
	ok, _ := g.TryAcquire(4, taskcommon.Compaction, taskresource.Requirement{Memory: 5})
	assert.False(t, ok, "an oversized waiter must hold the line from second place too")

	// Both queued tasks still get through, in order.
	g.Release(1)
	require.NoError(t, <-ordinary)
	g.Release(2)
	require.NoError(t, <-oversized)
	assert.Equal(t, int64(3), g.Snapshot().ExclusiveTaskID)
}

// Two oversized tasks racing through the blocking path is the interleaving this
// design's new risk profile most deserves pinned: each can only run on a
// drained node, and each drains the node for the other only by releasing. They
// must not deadlock, and they must not overlap.
func TestTwoOversizedTasksContendThroughAcquire(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	start := make(chan struct{})
	ran := make(chan int64, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []int64{1, 2} {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := g.Acquire(ctx, id, taskcommon.Index, taskresource.Requirement{Memory: 500}); err != nil {
				errs <- err
				return
			}
			// Hold the node for a while and check throughout that it is really
			// held alone.
			for i := 0; i < 5; i++ {
				assert.Equal(t, id, g.Snapshot().ExclusiveTaskID, "two oversized tasks overlapped")
				time.Sleep(10 * time.Millisecond)
			}
			g.Release(id)
			ran <- id
		}(id)
	}
	close(start)
	wg.Wait()

	assert.Empty(t, errs, "neither task may be refused or time out")
	assert.Len(t, ran, 2, "both oversized tasks must run, one after the other")
	assert.Zero(t, g.Snapshot().ExclusiveTaskID)
}

// Without this, turning the head-of-line knob off would starve oversized
// tasks forever: they can never be admitted by luck, only by draining.
func TestOversizedHeadDrainsTheNodeEvenWithHeadOfLineDisabled(t *testing.T) {
	g := newTestGuard(t, 8, 100)
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key, "false")
	defer pt.Reset(pt.DataNodeCfg.ResourceHeadOfLineReserve.Key)

	ok, _ := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{Memory: 60})
	require.True(t, ok)

	waiting := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waiting <- g.Acquire(ctx, 2, taskcommon.Index, taskresource.Requirement{Memory: 500})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 10*time.Millisecond)

	// A small task must not slip in ahead of the waiting oversized task.
	ok, _ = g.TryAcquire(3, taskcommon.Compaction, taskresource.Requirement{Memory: 5})
	assert.False(t, ok, "oversized head holds the line regardless of the knob")

	g.Release(1)
	require.NoError(t, <-waiting)
}

func TestAcquireNeverFailsOnAnOversizedRequest(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// No error, and it is admitted, because the node is empty.
	require.NoError(t, g.Acquire(ctx, 1, taskcommon.Index, taskresource.Requirement{Memory: 5000}))
	assert.Equal(t, int64(1), g.Snapshot().ExclusiveTaskID)
}

func TestTwoOversizedTasksRunOneAfterTheOther(t *testing.T) {
	g := newTestGuard(t, 8, 100)

	ok, _ := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 500})
	require.True(t, ok)

	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 600})
	assert.False(t, ok, "the second oversized task waits for the first")

	g.Release(1)
	ok, _ = g.TryAcquire(2, taskcommon.Index, taskresource.Requirement{Memory: 600})
	assert.True(t, ok)
	assert.Equal(t, int64(2), g.Snapshot().ExclusiveTaskID)
}

func TestGetGuardIsProcessWideSingleton(t *testing.T) {
	paramtable.Init()
	a := GetGuard()
	b := GetGuard()
	require.NotNil(t, a)
	assert.Same(t, a, b)
}
