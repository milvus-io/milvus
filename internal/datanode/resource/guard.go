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
	// it finishes.
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
	globalGuard     Guard
	globalGuardOnce sync.Once
)

// GetGuard returns the process-wide guard shared by the compaction, index and
// import executors. Before this, each of those kept its own counter and none of
// them knew what the others had taken.
func GetGuard() Guard {
	globalGuardOnce.Do(func() {
		g := NewGuard()
		g.startWatermarkLoop(context.Background())
		globalGuard = g
	})
	return globalGuard
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
// an ordinary task into exclusive mode and serialise the whole node.
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
		return false, g.availLocked(budget)
	}
	// While an oversized task occupies the node, nothing else is admitted. The
	// arithmetic below would refuse everyone anyway as things stand -- reserved
	// exceeds the node, so nothing more can fit -- but node capacity is runtime
	// config and can grow under the task, and the promise made at admission has
	// to outlive that.
	if g.exclusiveTaskID != 0 {
		return false, g.availLocked(budget)
	}

	if g.blockedByHeadOfLineLocked(taskID, budget) {
		return false, g.availLocked(budget)
	}

	if g.isOversizedLocked(req) {
		// Taking the whole node must not be a way to jump the queue. The
		// head-of-line rule above only holds the line for a head that does not
		// fit, and on a drained node every ordinary head fits -- so between a
		// Release and the head goroutine waking to retry there is a window in
		// which that rule alone would let an oversized latecomer walk off with
		// the node the head was reserved for, and the head would then wait out
		// its whole runtime. An oversized task may claim the node only from the
		// front of the queue, or when nobody is queued at all.
		if len(g.waiters) > 0 && g.waiters[0].taskID != taskID {
			return false, g.availLocked(budget)
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
			return false, g.availLocked(budget)
		}
		g.exclusiveTaskID = taskID
		g.reserved = g.reserved.Add(req)
		g.ledger[taskID] = req
		mlog.Info(context.TODO(), "oversized task admitted for exclusive execution",
			mlog.Int64("taskID", taskID),
			mlog.String("taskType", taskType),
			mlog.String("requirement", req.String()))
		return true, g.availLocked(budget)
	}

	if !g.reserved.Add(req).FitsIn(budget) {
		return false, g.availLocked(budget)
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
// queued for the whole node. Capacity is read once rather than per waiter: the
// production path recomputes it from paramtable and hardware on every call.
func (g *guard) hasOversizedWaiterLocked(exceptTaskID int64) bool {
	capacity := g.nodeCapacityLocked()
	for _, w := range g.waiters {
		if w.taskID != exceptTaskID && !w.req.FitsIn(capacity) {
			return true
		}
	}
	return false
}

func (g *guard) Acquire(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error {
	g.mu.Lock()
	ok, _ := g.tryAcquireLocked(taskID, taskType, req)
	if ok {
		g.mu.Unlock()
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
