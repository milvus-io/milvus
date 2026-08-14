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
	"time"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Snapshot struct {
	Total    taskresource.Capacity
	Reserved taskresource.Capacity
	Frozen   bool
	NonTask  int64
	// ExclusiveTaskID is the task currently occupying the node alone, or 0.
	ExclusiveTaskID int64
}

type Guard interface {
	// TryAcquire reserves budget without blocking.
	//   admitted=true    reserved
	//   admitted=false   not right now; retry later
	//
	// There is no permanent refusal. A request larger than the whole node is not
	// impossible, only exclusive: the coordinator places such a task on the
	// emptiest worker on purpose, and it is admitted once every other
	// reservation has been released, after which nothing else is admitted until
	// it finishes. The same answer is given to a request that merely exceeds
	// the current budget once the ledger has emptied: at that point no Release
	// can ever help it, so waiting longer is refusal by another name.
	//
	// An oversized request has one further condition, which matters to
	// TryAcquire callers: it is admitted only from the front of the waiter queue
	// or with that queue empty, so that taking the whole node cannot become a
	// way to jump ahead of tasks already waiting. It can therefore be refused on
	// a node that is demonstrably empty, if someone else is queued. Acquire is
	// the intended path for an oversized request -- the queue is FIFO, so
	// waiting is what carries it to the front.
	//
	// The second value is the headroom left on the node *after* this call has
	// been decided: budget minus everything the ledger has committed, including
	// the charge just made when admitted=true. It therefore always agrees with a
	// Snapshot taken immediately afterwards, and a refused caller can report how
	// far short it fell by comparing it against its own requirement. It goes
	// negative when the budget has shrunk below what is already committed, and is
	// deliberately not clamped: that over-commitment is a signal, not noise.
	TryAcquire(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity)

	// Acquire blocks until the reservation succeeds or ctx ends. The only error
	// it can return is ctx's: no request is ever refused permanently.
	Acquire(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error

	// Release returns a task's reservation. It is idempotent.
	Release(taskID int64)

	Snapshot() Snapshot
}

type waiter struct {
	taskID int64
	req    taskresource.Requirement
	ch     chan struct{}
}

type guard struct {
	mu sync.Mutex

	// capacityOverride is nil in production, letting tests inject a fixed budget.
	capacityOverride *taskresource.Capacity

	ledger   map[int64]taskresource.Requirement
	reserved taskresource.Requirement

	// exclusiveTaskID is the task currently occupying the node alone, or 0.
	exclusiveTaskID int64

	waiters []*waiter

	// frozen and nonTask are driven by the watermark loop; see watermark.go.
	// nonTaskPeak is the reservation currently in force; lowSampleCount and
	// lowRunMax describe the run of samples below it -- how many have arrived,
	// and the largest of them, which is what the reservation relaxes to.
	frozen         bool
	nonTask        int64
	nonTaskPeak    int64
	lowSampleCount int
	lowRunMax      int64
}

var (
	globalGuard       Guard
	globalGuardOnce   sync.Once
	globalGuardCancel context.CancelFunc
	// globalGuardDone closes once the singleton's sampling loop has returned.
	globalGuardDone chan struct{}
)

// GetGuard returns the process-wide guard shared by the compaction, index and
// import executors. Before this, each of those kept its own counter and none of
// them knew what the others had taken.
func GetGuard() Guard {
	globalGuardOnce.Do(func() {
		// The cancel is kept in globalGuardCancel and called by
		// stopGlobalGuardForTest; gosec's G118 does not follow a cancel stored
		// in a variable, and the repo carries the same suppression elsewhere
		// (e.g. distributed/cdc/service.go).
		ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec
		globalGuardCancel = cancel
		globalGuardDone = make(chan struct{})
		g := NewGuard()
		g.startWatermarkLoop(ctx, globalGuardDone)
		globalGuard = g
	})
	return globalGuard
}

// stopGlobalGuardForTest ends the singleton's sampling loop and lets the next
// GetGuard build a fresh one.
//
// Without it, the one test that touches the singleton leaves a goroutine
// calling hardware.GetUsedMemoryCount every three seconds for the rest of the
// package binary's life. Any later test that mockey-patches that function --
// TestAcquireChargesCommitmentNotObservation calls t.Errorf from inside its
// patch -- then fails or not depending on which test ran first. Declaration
// order happens to save it today; -shuffle, a -run filter or a file rename does
// not.
// Canceling is not enough on its own: a sample already inside sampleOnce keeps
// running after the cancel returns, and would call the patched
// hardware.GetUsedMemoryCount after the caller's cleanup had torn the patch
// down. Waiting for the loop to actually return closes that window -- the same
// class of hazard as the leaked goroutine itself, just narrower.
func stopGlobalGuardForTest() {
	if globalGuardCancel != nil {
		globalGuardCancel()
		globalGuardCancel = nil
	}
	if globalGuardDone != nil {
		<-globalGuardDone
		globalGuardDone = nil
	}
	globalGuard = nil
	globalGuardOnce = sync.Once{}
}

func NewGuard() *guard {
	return &guard{ledger: make(map[int64]taskresource.Requirement)}
}

func (g *guard) setCapacityForTest(c taskresource.Capacity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.capacityOverride = &c
}

// nodeCapacityLocked is capacity BEFORE the non-task reduction. Oversizedness
// is a property of the machine, not of a transient memory reading: judging it
// against the reduced budget would let a passing spike in non-task memory flip
// an ordinary task into exclusive mode and serialize the whole node.
func (g *guard) nodeCapacityLocked() taskresource.Capacity {
	if g.capacityOverride != nil {
		return *g.capacityOverride
	}
	return taskresource.NodeCapacity()
}

// budgetLocked is the capacity actually available to tasks: node capacity minus
// the memory observed outside the ledger. nonTask only ever shrinks the budget;
// see watermark.go for why it can never widen it.
func (g *guard) budgetLocked() taskresource.Capacity {
	c := g.nodeCapacityLocked()
	c.Memory -= g.nonTask
	if c.Memory < 0 {
		c.Memory = 0
	}
	return c
}

// isOversizedLocked reports whether req can never run alongside anything else.
// It is the single definition of oversizedness; everything that needs to ask
// the question goes through here, including hasOversizedWaiterLocked, which
// used to inline the same comparison. Two copies of one rule is the defect
// class this whole effort keeps finding, and it also made the rule untestable:
// a mutation of this function did not reach the queue, so nothing could tell
// the two definitions apart.
func (g *guard) isOversizedLocked(req taskresource.Requirement) bool {
	return !req.FitsIn(g.nodeCapacityLocked())
}

func (g *guard) TryAcquire(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tryAcquireLocked(taskID, taskType, req)
}

// availLocked is the headroom the node has left: the budget minus everything
// the ledger has committed. It is evaluated at each return site rather than once
// up front, so an admitted caller sees the figure net of its own charge. It is
// not clamped -- a negative result means the budget has shrunk below what is
// already committed, which callers and metrics need to see.
func (g *guard) availLocked(budget taskresource.Capacity) taskresource.Capacity {
	return taskresource.Capacity{
		CPU:    budget.CPU - g.reserved.CPU,
		Memory: budget.Memory - g.reserved.Memory,
	}
}

// tryAcquireLocked decides one admission.
//
// Note what is *not* consulted anywhere below: the process's current memory
// usage. Admission is decided by the ledger of commitments alone, because a
// task that was admitted a moment ago has not allocated its peak yet. Judging
// by observation is what let issue #52180 admit eight compactions in a row
// while every one of them was still downloading.
func (g *guard) tryAcquireLocked(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity) {
	budget := g.budgetLocked()

	// An id already on the books is answered before every rule below. A task
	// that already holds its charge is running: it must never be told to wait
	// for a reservation it owns. The budget can shrink under a running task --
	// that is precisely what Task 6 does when it raises nonTask -- and sending
	// it back to the queue there would make it wait while still holding a
	// reservation, head of a line it can never leave. Re-requests are idempotent
	// for the same reason.
	if _, exists := g.ledger[taskID]; exists {
		return true, g.availLocked(budget)
	}
	if g.frozen {
		return g.deferLocked(taskType, reasonFrozen, budget)
	}
	// While an oversized task occupies the node, nothing else is admitted. The
	// arithmetic below would refuse everyone anyway as things stand -- reserved
	// exceeds the node, so nothing more can fit -- but node capacity is runtime
	// config and can grow under the task, and the promise made at admission has
	// to outlive that.
	if g.exclusiveTaskID != 0 {
		return g.deferLocked(taskType, reasonExclusive, budget)
	}

	if g.isOversizedLocked(req) {
		// Taking the whole node must not be a way to jump the queue, and this
		// check -- not where blockedByHeadOfLineLocked sits below -- is what
		// stops it. That rule holds the line only for a head that does not fit,
		// and on a drained node every ordinary head fits: in the window between
		// a Release and the head goroutine waking to retry it would wave an
		// oversized latecomer straight past the head it is reserved for, and the
		// head would then wait out the latecomer's whole runtime. Evaluating it
		// earlier would not change that, because the arithmetic it ends in does
		// not depend on the order. An oversized task may claim the node only
		// from the front of the queue, or when nobody is queued at all.
		if len(g.waiters) > 0 && g.waiters[0].taskID != taskID {
			return g.deferLocked(taskType, reasonHeadOfLine, budget)
		}
		// It cannot share the node, so it runs alone: admitted only once every
		// other task has finished. The test is the ledger, not the reserved
		// total -- CPU requirements are fractional (see estimate_import.go), so
		// a released ledger leaves a float residue behind that never compares
		// equal to zero, and a task charged a zero Requirement would not show up
		// in the total at all. Both would be read as "the node is busy" or "the
		// node is empty" against the evidence. It is never refused outright: the
		// coordinator places such a task on the emptiest worker on purpose and
		// relies on this wait.
		if len(g.ledger) != 0 {
			return g.deferLocked(taskType, reasonAwaitingDrain, budget)
		}
		return g.admitExclusiveLocked(taskID, taskType, req, budget, "larger than node capacity")
	}

	if g.blockedByHeadOfLineLocked(taskID, budget) {
		return g.deferLocked(taskType, reasonHeadOfLine, budget)
	}
	if !g.reserved.Add(req).FitsIn(budget) {
		// Nothing else is running, and the request still does not fit. No
		// future Release can help, because there is nothing left to release:
		// this is as much budget as the node will ever offer. Deferring again
		// is not "try later", it is forever, and as queue head this task then
		// blocks every other task through blockedByHeadOfLineLocked while the
		// ledger stays empty -- so the node also reports itself completely free
		// to DataCoord and keeps being fed work it will never start.
		//
		// This band exists because oversizedness is judged against node
		// CAPACITY (deliberately -- see isOversizedLocked) while admission is
		// judged against the BUDGET, which is capacity minus the non-task
		// reservation. A requirement in (budget, capacity] falls between the
		// two: never oversized, never admissible. nonTask cannot rescue it
		// either, since with everything blocked the observed memory is just the
		// resident baseline and the reservation relaxes only to the floor.
		//
		// So the invariant enforced here is progress, not classification: a
		// request that cannot be met on an empty node is run the same way an
		// oversized one is, alone. That is strictly safer than running it
		// concurrently, which is what the node did before this branch.
		//
		// budget.Memory > 0 is the one case where the premise above is false.
		// Once nonTask reaches capacity the budget floors at zero, so EVERY
		// request fails the fit check and the terminal case would admit every
		// one of them -- the memory check switched off entirely, which is the
		// opposite of what it is for. And "as much budget as the node will ever
		// offer" is untrue there: nonTask is a decaying peak-hold, so a zero
		// budget is a transient state that genuinely does resolve. The 0.85
		// freeze bounds how long, but standalone can sit in the 0.75-0.85 band
		// indefinitely with another role's memory counted as non-task, so this
		// is reachable rather than theoretical.
		if len(g.ledger) == 0 && budget.Memory > 0 {
			// Same queue rule as the oversized path above, for the same reason:
			// taking the whole node must not be a way past tasks already
			// waiting. Acquire is FIFO, so waiting carries this task to the
			// front and the terminal case is reached from there.
			if len(g.waiters) > 0 && g.waiters[0].taskID != taskID {
				return g.deferLocked(taskType, reasonHeadOfLine, budget)
			}
			return g.admitExclusiveLocked(taskID, taskType, req, budget, "exceeds the current budget on an empty node")
		}
		return g.deferLocked(taskType, reasonInsufficient, budget)
	}

	g.reserved = g.reserved.Add(req)
	g.ledger[taskID] = req
	mlog.Info(context.TODO(), "task resource reserved",
		mlog.Int64("taskID", taskID),
		mlog.String("taskType", taskType),
		mlog.String("requirement", req.String()),
		mlog.String("reserved", g.reserved.String()))
	return true, g.availLocked(budget)
}

// admitExclusiveLocked charges req and marks the node as belonging to taskID
// alone. Both callers have already established the two things that justify it:
// the ledger is empty, and req cannot be satisfied any other way. why names
// which of the two routes got here, because they are worth telling apart in a
// log -- one is a task bigger than the machine, the other a task bigger than
// what non-task memory has left of the machine.
func (g *guard) admitExclusiveLocked(taskID int64, taskType taskcommon.Type, req taskresource.Requirement, budget taskresource.Capacity, why string) (bool, taskresource.Capacity) {
	g.exclusiveTaskID = taskID
	g.reserved = g.reserved.Add(req)
	g.ledger[taskID] = req
	mlog.Info(context.TODO(), "task admitted for exclusive execution",
		mlog.Int64("taskID", taskID),
		mlog.String("taskType", taskType),
		mlog.String("reason", why),
		mlog.String("requirement", req.String()),
		mlog.String("budget", budget.String()),
		mlog.String("nodeCapacity", g.nodeCapacityLocked().String()))
	return true, g.availLocked(budget)
}

// blockedByHeadOfLineLocked keeps the longest-waiting task's budget from being
// eaten by later, smaller arrivals. Without it a steady trickle of small tasks
// starves every large one.
func (g *guard) blockedByHeadOfLineLocked(taskID int64, budget taskresource.Capacity) bool {
	if len(g.waiters) == 0 {
		return false
	}
	head := g.waiters[0]
	if head.taskID == taskID {
		return false
	}
	// An oversized waiter can only ever run on a drained node, so it holds the
	// line from wherever it sits in the queue and regardless of the
	// configuration knob. Position does not matter because the reason does not:
	// unlike an ordinary task it can never be admitted by a lucky gap, so a
	// trickle of small tasks that keeps the node non-empty starves it forever.
	// Letting the knob release it would do the same.
	if g.hasOversizedWaiterLocked(taskID) {
		return true
	}
	if !paramtable.Get().DataNodeCfg.ResourceHeadOfLineReserve.GetAsBool() {
		return false
	}
	// The head is waiting because it does not fit yet. Anyone else taking
	// budget now pushes its admission further away.
	return !g.reserved.Add(head.req).FitsIn(budget)
}

// hasOversizedWaiterLocked reports whether anyone other than exceptTaskID is
// queued for the whole node.
func (g *guard) hasOversizedWaiterLocked(exceptTaskID int64) bool {
	for _, w := range g.waiters {
		if w.taskID != exceptTaskID && g.isOversizedLocked(w.req) {
			return true
		}
	}
	return false
}

func (g *guard) Acquire(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error {
	start := time.Now()

	g.mu.Lock()
	ok, _ := g.tryAcquireLocked(taskID, taskType, req)
	if ok {
		g.mu.Unlock()
		observeAdmissionWait(taskType, time.Since(start))
		return nil
	}

	w := &waiter{taskID: taskID, req: req, ch: make(chan struct{}, 1)}
	g.waiters = append(g.waiters, w)
	g.mu.Unlock()

	defer g.removeWaiter(w)

	// Retry before waiting, every time round: the reservation attempt must stay
	// at the top of this loop, ahead of the channel receive, if it is ever
	// rewritten. Two things depend on that order.
	//
	// First, wakeups can be dropped. w.ch is buffered to one, so a send is
	// discarded whenever an unconsumed token is already sitting in it. Release
	// mutates the ledger under the lock and only then sends, so a dropped send
	// implies the waiter had not yet consumed the earlier token -- and consuming
	// it leads straight back to a retry, which therefore strictly follows the
	// mutation that freed the budget. Retrying first is what makes that hold;
	// select-first would only be safe here by accident.
	//
	// Second, Release is not the only thing that can make a waiter admissible.
	// Task 6's watermark loop widens the budget by lowering nonTask or clearing
	// frozen, neither of which goes through Release. A waiter that blocked
	// without re-checking would sleep through those changes.
	//
	// (What this ordering does *not* guard is a Release slipping between the
	// first failed attempt and the append above: both happen under a single
	// lock hold, so that interleaving cannot occur.)
	for {
		g.mu.Lock()
		ok, _ = g.tryAcquireLocked(taskID, taskType, req)
		g.mu.Unlock()
		if ok {
			observeAdmissionWait(taskType, time.Since(start))
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.ch:
		}
	}
}

// removeWaiter dequeues w, whether it was admitted or gave up, and then wakes
// whoever is left. Leaving the queue frees budget just as a Release does: while
// w was queued the head-of-line rule was holding budget back on its behalf, so
// tasks that only w was blocking become admissible the moment it goes. Without
// this wake they would sleep until their own deadlines.
func (g *guard) removeWaiter(w *waiter) {
	g.mu.Lock()
	for i, x := range g.waiters {
		if x == w {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			break
		}
	}
	g.mu.Unlock()

	g.wakeWaiters()
}

func (g *guard) Release(taskID int64) {
	g.mu.Lock()
	req, exists := g.ledger[taskID]
	if !exists {
		// Unknown or already released: a no-op. Subtracting again would hand
		// out budget nobody ever charged.
		g.mu.Unlock()
		return
	}
	delete(g.ledger, taskID)
	g.reserved = g.reserved.Sub(req)
	if len(g.ledger) == 0 {
		// An empty ledger means nothing is reserved, by definition, and the
		// arithmetic is not required to agree. Requirement.Sub clamps at
		// negative but never snaps to zero, and CPU requirements are fractional
		// (estimate_import.go charges 0.1 per import), so subtracting exactly
		// what was added can leave a positive residue of a few 1e-17 behind --
		// permanently, since nothing ever subtracts it again. Small as it is, it
		// is enough to refuse a task sized exactly to the budget whenever the
		// budget is small enough that the residue survives the addition, on a
		// node the ledger says is empty.
		g.reserved = taskresource.Requirement{}
	}
	// The node stops being exclusive the moment its occupant lets go.
	if g.exclusiveTaskID == taskID {
		g.exclusiveTaskID = 0
	}
	g.mu.Unlock()

	g.wakeWaiters()
}

func (g *guard) wakeWaiters() {
	g.mu.Lock()
	waiters := make([]*waiter, len(g.waiters))
	copy(waiters, g.waiters)
	g.mu.Unlock()

	// Signal outside the lock: a waiter's channel is buffered, but the mutex
	// must never be held across a send.
	for _, w := range waiters {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

func (g *guard) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()

	budget := g.budgetLocked()
	return Snapshot{
		Total:           budget,
		Reserved:        taskresource.Capacity{CPU: g.reserved.CPU, Memory: g.reserved.Memory},
		Frozen:          g.frozen,
		NonTask:         g.nonTask,
		ExclusiveTaskID: g.exclusiveTaskID,
	}
}

func (g *guard) waiterCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.waiters)
}
