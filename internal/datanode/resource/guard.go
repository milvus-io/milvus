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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ErrResourceExhausted means the request can never be satisfied on this node,
// however long the caller waits. It is distinct from "not right now".
var ErrResourceExhausted = errors.New("task resource requirement exceeds node capacity")

type Snapshot struct {
	Total    taskresource.Capacity
	Reserved taskresource.Capacity
	Frozen   bool
	NonTask  int64
}

type Guard interface {
	// TryAcquire reserves budget without blocking.
	//   admitted=true             reserved
	//   admitted=false, err=nil   temporarily short, or frozen; retry later
	//   err=ErrResourceExhausted  impossible on this node; retrying is futile
	TryAcquire(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity, error)

	// Acquire blocks until the reservation succeeds or ctx ends. It returns
	// ErrResourceExhausted immediately for requests larger than the node.
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

	waiters []*waiter

	frozen  bool
	nonTask int64
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

// budgetLocked is the capacity actually available to tasks: node capacity minus
// the memory observed outside the ledger. nonTask only ever shrinks the budget;
// see watermark.go for why it can never widen it.
func (g *guard) budgetLocked() taskresource.Capacity {
	var c taskresource.Capacity
	if g.capacityOverride != nil {
		c = *g.capacityOverride
	} else {
		c = taskresource.NodeCapacity()
	}
	c.Memory -= g.nonTask
	if c.Memory < 0 {
		c.Memory = 0
	}
	return c
}

func (g *guard) TryAcquire(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tryAcquireLocked(taskID, taskType, req)
}

func (g *guard) tryAcquireLocked(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) (bool, taskresource.Capacity, error) {
	budget := g.budgetLocked()
	avail := taskresource.Capacity{
		CPU:    budget.CPU - g.reserved.CPU,
		Memory: budget.Memory - g.reserved.Memory,
	}

	// Note what is *not* consulted here: the process's current memory usage.
	// Admission is decided by the ledger of commitments alone, because a task
	// that was admitted a moment ago has not allocated its peak yet. Judging by
	// observation is what let issue #52180 admit eight compactions in a row
	// while every one of them was still downloading.
	if !req.FitsIn(budget) {
		return false, avail, ErrResourceExhausted
	}
	// An id already on the books is answered before any of the rules below.
	// Retries stay idempotent, and -- more importantly -- a task that already
	// holds its charge is never sent to wait for one: doing so would have it
	// hold budget while queueing for budget, blocking everyone behind it.
	if _, exists := g.ledger[taskID]; exists {
		return true, avail, nil
	}
	if g.frozen {
		return false, avail, nil
	}
	if g.blockedByHeadOfLineLocked(taskID, budget) {
		return false, avail, nil
	}
	if !g.reserved.Add(req).FitsIn(budget) {
		return false, avail, nil
	}

	g.reserved = g.reserved.Add(req)
	g.ledger[taskID] = req
	mlog.Info(context.TODO(), "task resource reserved",
		mlog.Int64("taskID", taskID),
		mlog.String("taskType", taskType),
		mlog.String("requirement", req.String()),
		mlog.String("reserved", g.reserved.String()))
	return true, avail, nil
}

// blockedByHeadOfLineLocked keeps the longest-waiting task's budget from being
// eaten by later, smaller arrivals. Without it a steady trickle of small tasks
// starves every large one.
func (g *guard) blockedByHeadOfLineLocked(taskID int64, budget taskresource.Capacity) bool {
	if !paramtable.Get().DataNodeCfg.ResourceHeadOfLineReserve.GetAsBool() {
		return false
	}
	if len(g.waiters) == 0 {
		return false
	}
	head := g.waiters[0]
	if head.taskID == taskID {
		return false
	}
	// The head is waiting because it does not fit yet. Anyone else taking
	// budget now pushes its admission further away.
	return !g.reserved.Add(head.req).FitsIn(budget)
}

func (g *guard) Acquire(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error {
	g.mu.Lock()
	ok, _, err := g.tryAcquireLocked(taskID, taskType, req)
	if err != nil {
		g.mu.Unlock()
		return err
	}
	if ok {
		g.mu.Unlock()
		return nil
	}

	w := &waiter{taskID: taskID, req: req, ch: make(chan struct{}, 1)}
	g.waiters = append(g.waiters, w)
	g.mu.Unlock()

	defer g.removeWaiter(w)

	// Retry before waiting, every time round. A Release that lands between the
	// failed attempt above and the append would otherwise wake a waiter list
	// that does not yet contain w, and this call would block until its context
	// expired even though budget was free. Keep the reservation attempt at the
	// top of the loop, ahead of the channel receive, if this is ever rewritten.
	for {
		g.mu.Lock()
		ok, _, err = g.tryAcquireLocked(taskID, taskType, req)
		g.mu.Unlock()
		if err != nil {
			return err
		}
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
		Total:    budget,
		Reserved: taskresource.Capacity{CPU: g.reserved.CPU, Memory: g.reserved.Memory},
		Frozen:   g.frozen,
		NonTask:  g.nonTask,
	}
}

func (g *guard) waiterCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.waiters)
}

// startWatermarkLoop is a placeholder. Task 6 replaces it with the real
// sampling loop in watermark.go, which is what will drive the frozen and
// nonTask fields this file already reads.
func (g *guard) startWatermarkLoop(ctx context.Context) {}
