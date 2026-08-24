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
)

// This package does BOOKKEEPING, not judgement.
//
// Placement is decided by DataCoord, which is the side that holds the segment
// statistics an estimate is derived from and the side that can see the whole
// cluster. What this node owes in return is an honest statement of what it has
// taken on, so that the coordinator's next round is not fooled.
//
// That statement is the reason a ledger exists here at all, and it is why it
// counts COMMITMENTS rather than measurements. A task accepted a moment ago has
// not allocated its peak yet; a node that reported its observed memory would
// look empty for as long as its tasks were still downloading, and the
// coordinator would keep feeding it. That is the shape of issue #52180's first
// incident -- eight compactions, 36GiB of input, every one of them still
// reading when the next was placed.
//
// What this package deliberately does NOT do any more:
//
//   - It does not refuse a task. The coordinator already decided this node has
//     room, from a report this node published. Re-deciding here with a second,
//     independently derived estimate is what produced two implementations of
//     one contract that drifted apart, and on storage V3 the local one had no
//     data to work from at all.
//   - It does not run a waiter queue, a head-of-line reservation or an
//     exclusive-execution mode. Ordering is the scheduler's job; a task larger
//     than the node is placed on the emptiest worker by the picker.
//   - It does not feed measured memory back into a budget. That loop charged
//     freed-but-not-yet-returned RSS as though it belonged to someone else, and
//     in a standalone deployment charged the other roles' memory the same way.
//
// The one thing measured memory still does is the safety valve below.

// Snapshot is what the node publishes about itself.
type Snapshot struct {
	// Capacity is the machine's budget for tasks.
	Capacity taskresource.Capacity
	// Committed is the sum over tasks accepted and not yet finished.
	Committed taskresource.Capacity
	// Admitting is false while the safety valve is engaged.
	Admitting bool
}

type Guard interface {
	// Accept records a task's commitment and returns.
	//
	// It does not decide whether the task should run here; that decision was
	// made by the coordinator. The only thing that can delay it is the safety
	// valve -- measured memory above the high watermark -- and the only error
	// it can return is ctx's.
	Accept(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error

	// Release returns a task's commitment. It is idempotent.
	Release(taskID int64)

	Snapshot() Snapshot
}

type guard struct {
	mu sync.Mutex

	// capacityOverride is nil in production, letting tests inject a budget.
	capacityOverride *taskresource.Capacity

	ledger    map[int64]taskresource.Requirement
	committed taskresource.Requirement

	// frozen is the safety valve; see watermark.go.
	frozen bool
	// thaw is closed and replaced whenever the valve disengages, so a waiting
	// Accept wakes without polling.
	thaw chan struct{}
}

var (
	globalGuard       Guard
	globalGuardOnce   sync.Once
	globalGuardCancel context.CancelFunc
	// globalGuardDone closes once the singleton's sampling loop has returned.
	globalGuardDone chan struct{}
)

// GetGuard returns the process-wide ledger shared by the compaction, index and
// import executors.
func GetGuard() Guard {
	globalGuardOnce.Do(func() {
		// The cancel is kept in globalGuardCancel and called by
		// stopGlobalGuardForTest; gosec's G118 does not follow a cancel stored
		// in a variable, and the repo carries the same suppression elsewhere.
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
// Canceling is not enough on its own: a sample already inside sampleOnce keeps
// running after the cancel returns, and would call a patched
// hardware.GetUsedMemoryCount after the caller's cleanup had torn the patch
// down. Waiting for the loop to actually return closes that window.
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
	return &guard{
		ledger: make(map[int64]taskresource.Requirement),
		thaw:   make(chan struct{}),
	}
}

func (g *guard) setCapacityForTest(c taskresource.Capacity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.capacityOverride = &c
}

func (g *guard) capacityLocked() taskresource.Capacity {
	if g.capacityOverride != nil {
		return *g.capacityOverride
	}
	return taskresource.NodeCapacity()
}

func (g *guard) Accept(ctx context.Context, taskID int64, taskType taskcommon.Type, req taskresource.Requirement) error {
	start := time.Now()
	for {
		g.mu.Lock()
		if !g.frozen {
			g.recordLocked(taskID, taskType, req)
			g.mu.Unlock()
			observeAdmissionWait(taskType, time.Since(start))
			return nil
		}
		// Re-read the channel under the lock so the wait cannot miss a thaw
		// that happens between the check and the receive.
		thaw := g.thaw
		g.mu.Unlock()

		mlog.Warn(ctx, "task is waiting for the memory safety valve to disengage",
			mlog.Int64("taskID", taskID),
			mlog.String("taskType", taskType),
			mlog.String("requirement", req.String()))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-thaw:
		}
	}
}

// recordLocked books the commitment. A task already on the books is left
// alone: re-recording it would double-charge the node for one task, and a
// re-delivered RPC for a task already running is an ordinary event.
func (g *guard) recordLocked(taskID int64, taskType taskcommon.Type, req taskresource.Requirement) {
	if _, exists := g.ledger[taskID]; exists {
		return
	}
	g.ledger[taskID] = req
	g.committed = g.committed.Add(req)
	mlog.Info(context.TODO(), "task resource committed",
		mlog.Int64("taskID", taskID),
		mlog.String("taskType", taskType),
		mlog.String("requirement", req.String()),
		mlog.String("committed", g.committed.String()))
}

func (g *guard) Release(taskID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	req, exists := g.ledger[taskID]
	if !exists {
		// Unknown or already released: a no-op. Subtracting again would hand
		// back capacity nobody ever took.
		return
	}
	delete(g.ledger, taskID)
	g.committed = g.committed.Sub(req)
	if len(g.ledger) == 0 {
		// An empty ledger means nothing is committed, by definition, and the
		// arithmetic is not required to agree. Requirement.Sub clamps at zero
		// but never snaps to it, and CPU requirements are fractional, so
		// subtracting exactly what was added can leave a residue of a few
		// 1e-17 behind -- permanently, since nothing subtracts it again.
		g.committed = taskresource.Requirement{}
	}
}

func (g *guard) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshotLocked()
}

func (g *guard) snapshotLocked() Snapshot {
	return Snapshot{
		Capacity:  g.capacityLocked(),
		Committed: taskresource.Capacity{CPU: g.committed.CPU, Memory: g.committed.Memory},
		Admitting: !g.frozen,
	}
}
