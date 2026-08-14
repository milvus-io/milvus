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

// saveParam sets a config key for the duration of the test. The floor in
// particular defaults to 1GiB, which would swamp the deliberately tiny budgets
// these tests use and make every refusal look like exhaustion rather than the
// behavior under test.
func saveParam(t *testing.T, item *paramtable.ParamItem, value string) {
	t.Helper()
	pt := paramtable.Get()
	pt.Save(item.Key, value)
	t.Cleanup(func() { pt.Reset(item.Key) })
}

// usedMemoryStub feeds the observed-memory reading these tests step through.
// It exists because a mockey patch is process-wide: a test that re-patched by
// hand and then failed an assertion mid-way would leave the patch installed and
// every later test in the package would die on "re-mock" instead of reporting
// its own result. Rebinding through one stub with a registered cleanup keeps a
// failure local to the test that caused it.
type usedMemoryStub struct{ mk *mockey.Mocker }

func stubUsedMemory(t *testing.T) *usedMemoryStub {
	t.Helper()
	s := &usedMemoryStub{}
	t.Cleanup(s.clear)
	return s
}

func (s *usedMemoryStub) set(v uint64) {
	s.clear()
	s.mk = mockey.Mock(hardware.GetUsedMemoryCount).Return(v).Build()
}

func (s *usedMemoryStub) clear() {
	if s.mk != nil {
		s.mk.UnPatch()
		s.mk = nil
	}
}

func TestFreezeAboveHighWatermarkBlocksAdmission(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(900) // 0.9 > 0.85
	g.sampleOnce()

	ok, avail := g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 1})
	assert.False(t, ok, "no admission while frozen, however much ledger room remains")
	assert.True(t, g.Snapshot().Frozen)
	require.Positive(t, avail.Memory,
		"the ledger had room to spare, so the refusal above can only have come from the freeze")

	// The control: the very same request goes through once the freeze lifts,
	// with the non-task reservation still in force. Without it, a refusal caused
	// by anything other than the freeze would satisfy the assertion above.
	used.set(700)
	g.sampleOnce()
	require.False(t, g.Snapshot().Frozen)

	ok, _ = g.TryAcquire(1, taskcommon.Index, taskresource.Requirement{Memory: 1})
	assert.True(t, ok, "thawing must admit the request the freeze was refusing")
}

func TestUnfreezeOnlyBelowLowWatermark(t *testing.T) {
	g := newTestGuard(t, 100, 1000)

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(900)
	g.sampleOnce()
	require.True(t, g.Snapshot().Frozen)

	// Between the two marks: hysteresis keeps it frozen. Collapse the band onto
	// a single threshold and the node thaws here, then freezes again on the next
	// admission -- the oscillation the two marks exist to prevent.
	used.set(800)
	g.sampleOnce()
	assert.True(t, g.Snapshot().Frozen, "0.80 is between low and high, must stay frozen")

	used.set(700)
	g.sampleOnce()
	assert.False(t, g.Snapshot().Frozen)
}

// The same band seen from below: an unfrozen node must not freeze until it
// crosses the *high* mark, or the band collapses onto the low mark instead.
func TestFreezeOnlyAboveHighWatermark(t *testing.T) {
	g := newTestGuard(t, 100, 1000)

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(800)
	g.sampleOnce()
	assert.False(t, g.Snapshot().Frozen, "0.80 is below the high mark, an unfrozen node stays unfrozen")
}

// Both marks are boundaries, and both belong to the conservative side: a node
// exactly at the high mark is frozen, and a frozen node exactly at the low mark
// stays frozen. Only strictly below the low mark does admission resume.
func TestWatermarkBoundariesFavorTheConservativeSide(t *testing.T) {
	g := newTestGuard(t, 100, 1000)

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(850) // exactly 0.85
	g.sampleOnce()
	require.True(t, g.Snapshot().Frozen, "the high watermark is inclusive")

	used.set(750) // exactly 0.75
	g.sampleOnce()
	assert.True(t, g.Snapshot().Frozen, "0.75 is not *below* the low watermark")

	used.set(749)
	g.sampleOnce()
	assert.False(t, g.Snapshot().Frozen)
}

// A total of zero means the reading failed, not that the node has no memory.
// Dividing by it yields +Inf, which would freeze the node on a measurement
// error alone.
func TestZeroTotalMemoryLeavesFreezeStateUnchanged(t *testing.T) {
	g := newTestGuard(t, 100, 1000)

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(0)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)
	used.set(900)

	g.sampleOnce()
	assert.False(t, g.Snapshot().Frozen, "a failed memory reading must not freeze the node")

	// Nor may it thaw a frozen one.
	g.mu.Lock()
	g.frozen = true
	g.mu.Unlock()
	g.sampleOnce()
	assert.True(t, g.Snapshot().Frozen, "a failed memory reading must not thaw the node")
}

// The invariant: a measured signal may only make the node more conservative.
// A low reading while admitted tasks have not yet grown must not widen the
// budget, or "judge by current state" sneaks back in through the budget.
func TestNonTaskMemoryNeverWidensBudgetImmediately(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceSlowGrowPeriods, "3")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	// A high non-task reading tightens the budget at once.
	used.set(400)
	g.sampleOnce()
	require.Equal(t, int64(400), g.Snapshot().NonTask, "a rise must take effect on the first sample")
	require.Equal(t, int64(600), g.Snapshot().Total.Memory)

	// A low reading must not relax it until the run is long enough.
	used.set(10)
	g.sampleOnce()
	assert.Equal(t, int64(400), g.Snapshot().NonTask, "budget must not widen on one low sample")
	g.sampleOnce()
	assert.Equal(t, int64(400), g.Snapshot().NonTask, "budget must not widen on two low samples either")

	// Only on the slowGrowPeriods'th consecutive low sample does it relax.
	g.sampleOnce()
	assert.Equal(t, int64(10), g.Snapshot().NonTask)
	assert.Equal(t, int64(990), g.Snapshot().Total.Memory)
}

// The run of low samples has to be consecutive. A rise in between both takes
// effect at once and restarts the count, so a node that dips and recovers never
// accumulates its way to a wider budget.
func TestNonTaskMemoryRiseResetsTheLowRun(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceSlowGrowPeriods, "3")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(400)
	g.sampleOnce()
	require.Equal(t, int64(400), g.Snapshot().NonTask)

	// Two low samples: not yet enough to relax.
	used.set(100)
	g.sampleOnce()
	g.sampleOnce()
	require.Equal(t, int64(400), g.Snapshot().NonTask)

	// A rise lands immediately and must clear the run.
	used.set(500)
	g.sampleOnce()
	require.Equal(t, int64(500), g.Snapshot().NonTask)

	// So these two low samples start a fresh run rather than finishing the old
	// one, and the third is what finally relaxes the reservation.
	used.set(100)
	g.sampleOnce()
	g.sampleOnce()
	assert.Equal(t, int64(500), g.Snapshot().NonTask, "the earlier low samples must not count towards this run")
	g.sampleOnce()
	assert.Equal(t, int64(100), g.Snapshot().NonTask)
}

// slowGrowPeriods decides *when* the reservation may relax; the run's maximum
// decides *how far*. Committing the run's last sample would let two ordinary
// samples tick the counter and one outlier dictate the new value, handing back
// budget that nothing else in the run supports -- sustained evidence applied to
// the timing while the magnitude rests on a single reading.
func TestRelaxationCommitsTheRunMaximumNotTheLastSample(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceSlowGrowPeriods, "3")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(400)
	g.sampleOnce()
	require.Equal(t, int64(400), g.Snapshot().NonTask)

	// Two samples barely under the reservation, then one far under it.
	used.set(399)
	g.sampleOnce()
	g.sampleOnce()
	used.set(1)
	g.sampleOnce()

	assert.Equal(t, int64(399), g.Snapshot().NonTask,
		"the run relaxes to its own maximum, not to the outlier that ended it")
	assert.Equal(t, int64(601), g.Snapshot().Total.Memory)

	// The maximum belongs to the run that produced it: once committed it must
	// not survive into the next run and hold the reservation up.
	g.sampleOnce()
	g.sampleOnce()
	g.sampleOnce()
	assert.Equal(t, int64(1), g.Snapshot().NonTask, "a fresh run of 1s must relax to 1")
}

func TestNonTaskMemoryExcludesLedger(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)
	used.set(300)

	mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{Memory: 250})

	g.sampleOnce()
	// 300 observed - 250 already accounted for by the ledger = 50 outside it.
	assert.Equal(t, int64(50), g.Snapshot().NonTask)
}

// An observation below what the ledger has committed says only that the
// admitted tasks have not grown into their estimates yet. It says nothing about
// what else is resident, so it is not evidence about non-task memory at all:
// the reservation must neither move nor edge closer to relaxing. Reading it as
// "there is no non-task memory" would let the ledger's own headroom decay the
// reservation away -- a low reading widening the budget, the one thing this
// package may never do.
func TestObservationBelowTheLedgerCarriesNoInformation(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceSlowGrowPeriods, "3")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	// Establish a reservation from a real reading first.
	used.set(400)
	g.sampleOnce()
	require.Equal(t, int64(400), g.Snapshot().NonTask)

	// Now the node takes on work the tasks have not grown into.
	mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{Memory: 500})
	used.set(450) // below the 500 committed

	// However many such samples arrive, none of them is evidence.
	for i := 0; i < 6; i++ { // twice slowGrowPeriods
		g.sampleOnce()
		require.Equal(t, int64(400), g.Snapshot().NonTask,
			"a sample below the ledger must not move the reservation (sample %d)", i)
	}

	// And they must not have counted towards a relaxation either: a genuine run
	// still needs its full slowGrowPeriods afterwards.
	used.set(600) // 600 - 500 committed = 100 outside the ledger
	g.sampleOnce()
	assert.Equal(t, int64(400), g.Snapshot().NonTask, "the skipped samples must not have shortened the run")
	g.sampleOnce()
	assert.Equal(t, int64(400), g.Snapshot().NonTask)
	g.sampleOnce()
	assert.Equal(t, int64(100), g.Snapshot().NonTask)
}

// The floor is a lower bound. A misconfigured negative one must not turn into a
// license to hand out memory the node does not have -- nothing validates this
// key, so the sample may only ever be raised by it.
func TestNegativeFloorCannotCreateBudget(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "-1000000")
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceSlowGrowPeriods, "1")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	mustAcquire(t, g, 1, taskcommon.Index, taskresource.Requirement{Memory: 100})

	used.set(500) // 500 - 100 committed = 400 outside the ledger
	g.sampleOnce()
	assert.Equal(t, int64(400), g.Snapshot().NonTask, "the floor may only raise a sample, never lower it")
	assert.Equal(t, int64(600), g.Snapshot().Total.Memory)

	// Sustained lower readings relax the reservation towards the observation,
	// never towards the nonsense floor.
	used.set(300)
	g.sampleOnce()
	assert.Equal(t, int64(200), g.Snapshot().NonTask)
	assert.Equal(t, int64(800), g.Snapshot().Total.Memory, "the budget must never exceed the node")
}

func TestNonTaskMemoryRespectsFloor(t *testing.T) {
	g := newTestGuard(t, 100, 1<<30)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "104857600") // 100MiB

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1) << 30).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)
	// One byte above the empty ledger: a real reading, and a negligible one, so
	// whatever comes out is the floor's doing.
	used.set(1)

	g.sampleOnce()
	assert.GreaterOrEqual(t, g.Snapshot().NonTask, int64(104857600))
}

// Clearing the freeze widens the budget without going through Release, so the
// waiters already parked in their select have to be woken explicitly. Nothing
// else will: a waiter only re-checks the ledger after its channel fires.
func TestUnfreezingWakesBlockedWaiters(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)

	used.set(900)
	g.sampleOnce()
	require.True(t, g.Snapshot().Frozen)

	// The budget has room for this; only the freeze is holding it back.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		done <- g.Acquire(ctx, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 50})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 5*time.Millisecond)
	require.Len(t, done, 0, "a frozen guard must not have admitted it")

	used.set(700)
	g.sampleOnce()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("thawing did not wake the waiter it unblocked")
	}
}

// The other budget-widening path with no Release behind it: a sustained run of
// low samples shrinking the non-task reservation.
func TestLoweredNonTaskMemoryWakesBlockedWaiters(t *testing.T) {
	g := newTestGuard(t, 100, 1000)
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceNonTaskMemoryFloor, "0")
	saveParam(t, &paramtable.Get().DataNodeCfg.ResourceSlowGrowPeriods, "2")

	// Well below the low mark throughout, so the freeze plays no part here.
	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(10000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)
	used.set(600)

	mustAcquire(t, g, 1, taskcommon.Compaction, taskresource.Requirement{Memory: 300})

	g.sampleOnce() // 600 observed - 300 in the ledger = 300 outside it
	require.False(t, g.Snapshot().Frozen)
	require.Equal(t, int64(300), g.Snapshot().NonTask)
	require.Equal(t, int64(700), g.Snapshot().Total.Memory)

	// 300 + 500 > 700: it fits the node but not the tightened budget.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		done <- g.Acquire(ctx, 2, taskcommon.Compaction, taskresource.Requirement{Memory: 500})
	}()
	require.Eventually(t, func() bool { return g.waiterCount() == 1 }, time.Second, 5*time.Millisecond)
	require.Len(t, done, 0)

	// Still above the 300 committed, so these are real readings of non-task
	// memory rather than samples the ledger swallows.
	used.set(400)
	g.sampleOnce() // first low sample: not enough on its own
	require.Equal(t, int64(300), g.Snapshot().NonTask)
	require.Len(t, done, 0, "one low sample must not have widened the budget")

	g.sampleOnce() // second: the reservation relaxes and the budget widens
	require.Equal(t, int64(100), g.Snapshot().NonTask)
	require.Equal(t, int64(900), g.Snapshot().Total.Memory)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("relaxing the non-task reservation did not wake the waiter it unblocked")
	}
}

func TestWatermarkLoopSamplesUntilContextCanceled(t *testing.T) {
	g := newTestGuard(t, 100, 1000)

	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build()
	defer mkTotal.UnPatch()
	used := stubUsedMemory(t)
	used.set(900)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		g.watermarkLoop(ctx, 5*time.Millisecond)
	}()

	assert.Eventually(t, func() bool { return g.Snapshot().Frozen }, 3*time.Second, 5*time.Millisecond,
		"the loop never sampled")

	// The patches are removed when this function returns, so the loop must be
	// gone by then rather than merely on its way out.
	cancel()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("the watermark loop ignored context cancellation")
	}
}

// startWatermarkLoop is what production calls: it must detach rather than run
// the loop on the caller's goroutine.
func TestStartWatermarkLoopReturnsImmediately(t *testing.T) {
	g := newTestGuard(t, 100, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	returned := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(returned)
		g.startWatermarkLoop(ctx, stopped)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("startWatermarkLoop must not block the caller")
	}

	// And the loop it started signals when it has actually finished, so a
	// caller that cancels can wait for the last sample instead of racing it.
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("the loop must close its done channel once ctx ends")
	}
}
