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

package task

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	paramtable.Init()
}

// taskLockHeld reports whether something currently holds the task's key lock.
// The worker-drop tests use it to prove the drop was sent after the unlock.
func taskLockHeld(s *globalTaskScheduler, taskID int64) bool {
	if !s.mu.TryLock(taskID) {
		return true
	}
	s.mu.Unlock(taskID)
	return false
}

// ownTask makes the scheduler own a task directly, standing in for an Enqueue
// whose bookkeeping the test does not care about.
func ownTask(s *globalTaskScheduler, task Task) {
	s.tasks.Insert(task.GetTaskID(), &taskEntry{task: task})
}

// runRoundSync runs one scheduling round with the check pass inline. round()
// detaches it on purpose -- a slow worker poll must not delay dispatch -- but a
// test needs it finished before it asserts on the outcome. Dispatch and
// checking are unordered with respect to each other in production, so
// serializing them here is a legitimate choice of one interleaving.
func runRoundSync(s *globalTaskScheduler) {
	pending, running, done := s.partition()
	s.releaseDoneTasks(done)
	// The worker drops for released tasks are detached onto a wg-counted
	// goroutine in production; join it so assertions that follow see them.
	// Only valid on a scheduler that was never Start()ed -- otherwise this
	// would wait for the loops themselves.
	s.wg.Wait()
	s.schedule(pending)
	s.check(running)
}

type versionAwareSchedulerCluster struct {
	session.Cluster
	slots map[int64]*session.WorkerSlots
}

func (c *versionAwareSchedulerCluster) QuerySlot() map[int64]*session.WorkerSlots {
	return c.slots
}

type versionAwareSchedulerTask struct {
	id             int64
	slot           int64
	minimumVersion semver.Version
	state          atomic.Int32
	nodeID         atomic.Int64
}

func newVersionAwareSchedulerTask(id int64, minimumVersion semver.Version) *versionAwareSchedulerTask {
	task := &versionAwareSchedulerTask{id: id, slot: 1, minimumVersion: minimumVersion}
	task.state.Store(int32(taskcommon.Init))
	task.nodeID.Store(NullNodeID)
	return task
}

func (t *versionAwareSchedulerTask) GetTaskID() int64             { return t.id }
func (t *versionAwareSchedulerTask) GetTaskType() taskcommon.Type { return taskcommon.CopySegment }
func (t *versionAwareSchedulerTask) GetTaskState() taskcommon.State {
	return taskcommon.State(t.state.Load())
}
func (t *versionAwareSchedulerTask) GetTaskSlot() int64                         { return t.slot }
func (t *versionAwareSchedulerTask) SetTaskTime(taskcommon.TimeType, time.Time) {}
func (t *versionAwareSchedulerTask) GetTaskTime(taskcommon.TimeType) time.Time  { return time.Time{} }
func (t *versionAwareSchedulerTask) GetTaskVersion() int64                      { return 0 }
func (t *versionAwareSchedulerTask) MinimumWorkerVersion() semver.Version {
	return t.minimumVersion
}

func (t *versionAwareSchedulerTask) CreateTaskOnWorker(nodeID int64, _ session.Cluster) {
	t.nodeID.Store(nodeID)
	t.state.Store(int32(taskcommon.InProgress))
}
func (t *versionAwareSchedulerTask) QueryTaskOnWorker(session.Cluster) {}
func (t *versionAwareSchedulerTask) DropTaskOnWorker(session.Cluster)  {}

func TestGlobalScheduler_Enqueue(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1)
	task.EXPECT().GetTaskState().Return(taskcommon.Init)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).tasks.Len())
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Compaction))
	scheduler.Enqueue(task)
	assert.Equal(t, 1, scheduler.(*globalTaskScheduler).tasks.Len())
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Compaction))

	task = NewMockTask(t)
	task.EXPECT().GetTaskID().Return(2)
	task.EXPECT().GetTaskState().Return(taskcommon.InProgress)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.Equal(t, 2, scheduler.(*globalTaskScheduler).tasks.Len())
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Compaction))
	scheduler.Enqueue(task)
	assert.Equal(t, 2, scheduler.(*globalTaskScheduler).tasks.Len())
}

func TestGlobalScheduler_GetPendingTaskCountIsScopedByTaskType(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)

	enqueue := func(taskID int64, taskType taskcommon.Type) {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(taskID)
		task.EXPECT().GetTaskState().Return(taskcommon.Init)
		task.EXPECT().GetTaskType().Return(taskType)
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
		scheduler.Enqueue(task)
	}

	enqueue(1, taskcommon.Stats)
	enqueue(2, taskcommon.Compaction)
	enqueue(3, taskcommon.Index)
	enqueue(4, taskcommon.Compaction)

	// An index/compaction backlog must not consume the stats admission budget.
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Stats))
	assert.Equal(t, 2, scheduler.GetPendingTaskCount(taskcommon.Compaction))
	assert.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Index))
}

func TestGlobalScheduler_GetPendingTaskCountIncludesBackoff(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "60")
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffInterval.Key)

	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)
	globalScheduler := scheduler.(*globalTaskScheduler)

	tasks := make(map[int64]Task)
	for taskID := int64(1); taskID <= 2; taskID++ {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(taskID)
		task.EXPECT().GetTaskState().Return(taskcommon.Init)
		task.EXPECT().GetTaskType().Return(taskcommon.Stats)
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
		scheduler.Enqueue(task)
		tasks[taskID] = task
	}

	// A task waiting on its retry backoff still occupies queue depth: excluding it
	// would let a worker-side failure storm silently disable the admission gate.
	globalScheduler.mu.Lock(tasks[2].GetTaskID())
	globalScheduler.recordFailureUnderTaskLock(tasks[2])
	globalScheduler.mu.Unlock(tasks[2].GetTaskID())
	assert.Equal(t, 2, scheduler.GetPendingTaskCount(taskcommon.Stats))
}

func TestGlobalScheduler_AbortAndRemoveTask(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1)
	task.EXPECT().GetTaskState().Return(taskcommon.Init)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.True(t, scheduler.(*globalTaskScheduler).tasks.Contain(1))
	scheduler.AbortAndRemoveTask(1)
	assert.False(t, scheduler.(*globalTaskScheduler).tasks.Contain(1))

	task = NewMockTask(t)
	task.EXPECT().GetTaskID().Return(2)
	task.EXPECT().GetTaskState().Return(taskcommon.InProgress)
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction)
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return()
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return()
	scheduler.Enqueue(task)
	assert.True(t, scheduler.(*globalTaskScheduler).tasks.Contain(2))
	scheduler.AbortAndRemoveTask(2)
	assert.False(t, scheduler.(*globalTaskScheduler).tasks.Contain(2))
}

// The worker drop is an RPC bounded only by dataCoord.requestTimeoutSeconds.
// Ownership is given up under the key lock, but the drop must be sent after the
// unlock, or an unresponsive node holds the key for a second full timeout.
func TestGlobalScheduler_AbortDropRunsOutsideTaskLock(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	dropSawUnlockedTask := false
	task.EXPECT().DropTaskOnWorker(mock.Anything).Run(func(session.Cluster) {
		dropSawUnlockedTask = !taskLockHeld(scheduler, 1)
	}).Once()

	scheduler.Enqueue(task)
	scheduler.AbortAndRemoveTask(1)

	assert.True(t, dropSawUnlockedTask,
		"worker cleanup must run after releasing the scheduler task lock")
	assert.False(t, scheduler.tasks.Contain(1))
}

// terminalDropTask ends its attempt on the first worker callback and records
// whether the per-task lock was already released by the time the drop was sent.
type terminalDropTask struct {
	*versionAwareSchedulerTask
	scheduler           *globalTaskScheduler
	dropSawUnlockedTask atomic.Bool
}

func (t *terminalDropTask) CreateTaskOnWorker(nodeID int64, _ session.Cluster) {
	t.nodeID.Store(nodeID)
	t.state.Store(int32(taskcommon.Failed))
}

func (t *terminalDropTask) QueryTaskOnWorker(session.Cluster) {
	t.state.Store(int32(taskcommon.Failed))
}

func (t *terminalDropTask) DropTaskOnWorker(session.Cluster) {
	t.dropSawUnlockedTask.Store(!taskLockHeld(t.scheduler, t.id))
}

// The terminal-state worker drop is an RPC bounded only by
// dataCoord.requestTimeoutSeconds, and Finalize -- which compaction cleanup runs
// under -- waits on the same per-task lock. Sending the drop inside the critical
// section lets a node that stopped answering hold that lock for a second full
// timeout on top of the one its query already spent, stalling cleanup and with
// it the cleanup slot and channel exclusion cleanup is holding. Both dispatch
// paths must therefore relinquish ownership under the lock and send afterwards.
func TestGlobalScheduler_TerminalDropRunsOutsideTaskLock(t *testing.T) {
	newTask := func(scheduler *globalTaskScheduler, id int64) *terminalDropTask {
		return &terminalDropTask{
			versionAwareSchedulerTask: newVersionAwareSchedulerTask(id, semver.Version{}),
			scheduler:                 scheduler,
		}
	}

	t.Run("query ended the attempt", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)
		task := newTask(scheduler, 1)
		task.state.Store(int32(taskcommon.InProgress))
		ownTask(scheduler, task)

		runRoundSync(scheduler)

		assert.True(t, task.dropSawUnlockedTask.Load(),
			"check must release the task lock before the terminal worker drop")
		assert.False(t, scheduler.tasks.Contain(task.GetTaskID()),
			"ownership is relinquished under the lock, not after the drop")
	})

	t.Run("create ended the attempt", func(t *testing.T) {
		cluster := &versionAwareSchedulerCluster{slots: map[int64]*session.WorkerSlots{
			1: {NodeID: 1, AvailableSlots: 10},
		}}
		scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)
		task := newTask(scheduler, 2)
		ownTask(scheduler, task)

		runRoundSync(scheduler)

		assert.True(t, task.dropSawUnlockedTask.Load(),
			"schedule must release the task lock before the terminal worker drop")
		assert.False(t, scheduler.tasks.Contain(task.GetTaskID()),
			"a task that terminated on create is released, not left owned")
	})
}

// enqueueDuringCreateTask re-enqueues itself from inside CreateTaskOnWorker,
// standing in for an inspector tick that lands while a dispatch is mid-RPC.
// Inspectors call Enqueue without the per-task lock (import_inspector.inspect
// re-enqueues every Pending task on every tick), so this is the real ordering.
type enqueueDuringCreateTask struct {
	*versionAwareSchedulerTask
	scheduler *globalTaskScheduler
}

func (t *enqueueDuringCreateTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	t.scheduler.Enqueue(t)
	t.versionAwareSchedulerTask.CreateTaskOnWorker(nodeID, cluster)
}

// An Enqueue landing while the task is being dispatched must be a no-op. It is
// the single ownership entry that makes this hold with no lock: GetOrInsert sees
// the task is already the scheduler's and returns, whatever phase it is in.
func TestGlobalScheduler_ConcurrentEnqueueDoesNotDuplicateDispatchingTask(t *testing.T) {
	cluster := &versionAwareSchedulerCluster{slots: map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 10},
	}}
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)
	task := &enqueueDuringCreateTask{
		versionAwareSchedulerTask: newVersionAwareSchedulerTask(1, semver.Version{}),
		scheduler:                 scheduler,
	}

	scheduler.Enqueue(task)
	runRoundSync(scheduler)

	assert.Equal(t, 1, scheduler.tasks.Len(), "the racing Enqueue must not add a second entry")
	owned, _ := scheduler.tasks.Get(1)
	assert.Equal(t, Task(task), owned.task)
	assert.Equal(t, phaseCheck, taskPhase(task), "the dispatch itself must still have happened")
}

// Both loops pick their work from an unlocked snapshot, so an abort can land
// before the callback acquires the key lock. The callback must notice it no
// longer owns the task rather than push it onto a worker and re-claim it,
// resurrecting a task whose meta is already retired.
func TestGlobalScheduler_AbortDuringDispatchIsNotResurrected(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	// The abort's own worker cleanup is expected; the dispatch must add no
	// CreateTaskOnWorker of its own, which the mock enforces by not declaring it.
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return().Once()

	scheduler.Enqueue(task)
	// Stand in for schedule() having picked this task but not yet reached
	// createUnderTaskLock.
	dispatching, _, _ := scheduler.partition()
	assert.Len(t, dispatching, 1)

	scheduler.AbortAndRemoveTask(1)

	assert.False(t, scheduler.createUnderTaskLock(dispatching[0].task, 1),
		"a dispatch that lost ownership must not report a worker drop")
	assert.False(t, scheduler.tasks.Contain(1),
		"an aborted task must not be re-claimed by the dispatch that lost it")
}

// Ownership is per task object, not per ID: an abort followed by an Enqueue of a
// rebuilt task leaves a different object under the same ID, and the stale
// dispatch must not act on it.
func TestGlobalScheduler_StaleDispatchDoesNotAdoptRebuiltTask(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	stale := newVersionAwareSchedulerTask(1, semver.Version{})
	scheduler.Enqueue(stale)
	dispatching, _, _ := scheduler.partition()

	scheduler.AbortAndRemoveTask(1)
	rebuilt := newVersionAwareSchedulerTask(1, semver.Version{})
	scheduler.Enqueue(rebuilt)

	assert.False(t, scheduler.createUnderTaskLock(dispatching[0].task, 1))
	assert.Equal(t, int64(NullNodeID), stale.nodeID.Load(),
		"the stale task must not be pushed to a worker")
	owned, _ := scheduler.tasks.Get(1)
	assert.Equal(t, Task(rebuilt), owned.task, "the rebuilt task keeps the ownership entry")
}

// A task enqueued in Retry has already been sent to a worker once, and no
// CreateTaskOnWorker implementation drops the previous attempt before sending a
// new one. Dispatching it again would leave that attempt running on its old node
// holding a slot nobody collects, so it must be polled first -- the poll is what
// drops the stale copy and resets the state to Init.
func TestGlobalScheduler_RetryTaskIsPolledNotRedispatched(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Retry).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Index).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	// CreateTaskOnWorker is deliberately not declared: the mock fails the test if
	// the dispatch path reaches for it.
	task.EXPECT().QueryTaskOnWorker(mock.Anything).Return().Once()

	scheduler.Enqueue(task)
	assert.Equal(t, phaseCheck, taskPhase(task))

	runRoundSync(scheduler)
}

// A task driven terminal by its owner without an abort or a Finalize (a
// compaction the inspector timed out, say) has nothing left for the scheduler to
// do. Nobody would ever look at it again, so the round releases it rather than
// letting the entry sit there until datacoord restarts. Its accumulated backoff
// goes with it, because it is the same entry.
func TestGlobalScheduler_TerminalTaskIsReleased(t *testing.T) {
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{}).Maybe()
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Failed).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return().Once()

	scheduler.tasks.Insert(1, &taskEntry{task: task, failures: 2, notBefore: time.Now()})

	runRoundSync(scheduler)

	assert.False(t, scheduler.tasks.Contain(1),
		"releasing the entry releases the task and its backoff together")
}

// importChecker.tryFailingTasks and the external-refresh timeout path both end
// an attempt by writing Failed straight to meta, including for tasks that are
// running on a worker right now. Nothing tells the worker, so the release has to
// carry the drop -- otherwise the attempt keeps running and holds its slot until
// it finishes on its own.
func TestGlobalScheduler_RunningTaskKilledInMetaIsDroppedOnWorker(t *testing.T) {
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{}).Maybe()
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	state := taskcommon.InProgress
	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Import).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State { return state }).Maybe()

	dropped := false
	task.EXPECT().DropTaskOnWorker(mock.Anything).Run(func(session.Cluster) {
		dropped = true
	}).Once()

	scheduler.Enqueue(task)
	assert.Equal(t, phaseCheck, taskPhase(task))

	// The owner ends the attempt in meta without telling the scheduler.
	state = taskcommon.Failed

	runRoundSync(scheduler)

	assert.False(t, scheduler.tasks.Contain(1))
	assert.True(t, dropped, "a released task that may still be on a worker must be dropped there")
}

func TestGlobalScheduler_pickNode(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)

	// Tie: either node may be returned, but the most-available is always picked.
	tie := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 30},
		2: {NodeID: 2, AvailableSlots: 30},
	}
	nodeID := scheduler.pickNode(tie, 1)
	assert.True(t, nodeID == int64(1) || nodeID == int64(2))

	// Least-loaded selection: node 2 has more available slots, so it wins even
	// though node 1 also fits and might be iterated first in the map.
	leastLoaded := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 20},
		2: {NodeID: 2, AvailableSlots: 80},
	}
	assert.Equal(t, int64(2), scheduler.pickNode(leastLoaded, 10))

	// Route by the QuerySlot map key instead of relying on WorkerSlots.NodeID.
	keyOnly := map[int64]*session.WorkerSlots{
		10: {AvailableSlots: 20},
		20: {AvailableSlots: 80},
	}
	assert.Equal(t, int64(20), scheduler.pickNode(keyOnly, 10))
	assert.Equal(t, int64(70), keyOnly[20].AvailableSlots)

	// Fallback: no node can fully satisfy the request, pick the most-available
	// node and drain its slots to 0.
	noEnough := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 20},
		2: {NodeID: 2, AvailableSlots: 30},
	}
	assert.Equal(t, int64(2), scheduler.pickNode(noEnough, 100))
	assert.Equal(t, int64(0), noEnough[2].AvailableSlots)

	// Single node: slots decrement across successive picks, then fall back.
	single := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
	}
	assert.Equal(t, int64(1), scheduler.pickNode(single, 10))
	assert.Equal(t, int64(90), single[1].AvailableSlots)
	assert.Equal(t, int64(1), scheduler.pickNode(single, 10))
	assert.Equal(t, int64(80), single[1].AvailableSlots)
	assert.Equal(t, int64(1), scheduler.pickNode(single, 100)) // 80 < 100, fallback
	assert.Equal(t, int64(0), single[1].AvailableSlots)

	// No available slots at all.
	empty := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 0},
		2: {NodeID: 2, AvailableSlots: 0},
	}
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(empty, 1))

	// Zero-slot cleanup work should still be dispatched even when every node is
	// exhausted, and it should not consume any slots.
	zeroSlot := map[int64]*session.WorkerSlots{
		10: {AvailableSlots: 0},
		20: {AvailableSlots: 0},
	}
	nodeID = scheduler.pickNode(zeroSlot, 0)
	assert.True(t, nodeID == int64(10) || nodeID == int64(20))
	assert.Equal(t, int64(0), zeroSlot[10].AvailableSlots)
	assert.Equal(t, int64(0), zeroSlot[20].AvailableSlots)
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(zeroSlot, 1))

	// Empty cluster.
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(nil, 1))
}

func TestGlobalScheduler_pickNodeWithMinimumVersion(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)
	minimumVersion := semver.MustParse("3.0.1")
	nodes := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100, Version: "3.0.0"},
		2: {NodeID: 2, AvailableSlots: 20, Version: "3.0.1"},
	}

	assert.Equal(t, int64(2), scheduler.pickNodeWithMinimumVersion(nodes, 10, minimumVersion))
	assert.Equal(t, int64(10), nodes[2].AvailableSlots)
	assert.Equal(t, int64(1), scheduler.pickNode(nodes, 10))
	assert.Equal(t, int64(90), nodes[1].AvailableSlots)
}

func TestWorkerSupportsMinimumVersion(t *testing.T) {
	minimumVersion := semver.MustParse("3.0.1")
	assert.False(t, workerSupportsMinimumVersion("", minimumVersion))
	assert.False(t, workerSupportsMinimumVersion("3.0.0", minimumVersion))
	assert.True(t, workerSupportsMinimumVersion("3.0.1-rc.1", minimumVersion))
	assert.True(t, workerSupportsMinimumVersion("v3.0.2", minimumVersion))
	assert.True(t, workerSupportsMinimumVersion("master-20260810-deadbeef", minimumVersion))
	assert.True(t, workerSupportsMinimumVersion("", semver.Version{}))
}

func TestGlobalScheduler_IncompatibleTaskDoesNotBlockOrdinaryTask(t *testing.T) {
	cluster := &versionAwareSchedulerCluster{
		slots: map[int64]*session.WorkerSlots{
			1: {NodeID: 1, AvailableSlots: 10, Version: "3.0.0"},
		},
	}
	scheduler := NewGlobalTaskScheduler(context.Background(), cluster).(*globalTaskScheduler)
	externalTask := newVersionAwareSchedulerTask(1, semver.MustParse("3.0.1"))
	ordinaryTask := newVersionAwareSchedulerTask(2, semver.Version{})

	scheduler.Enqueue(externalTask)
	scheduler.Enqueue(ordinaryTask)
	runRoundSync(scheduler)

	assert.Equal(t, int64(NullNodeID), externalTask.nodeID.Load())
	assert.True(t, scheduler.tasks.Contain(externalTask.GetTaskID()))
	assert.Equal(t, phaseDispatch, taskPhase(externalTask),
		"the incompatible task is still waiting for a worker")
	assert.Equal(t, int64(1), ordinaryTask.nodeID.Load())
	assert.Equal(t, phaseCheck, taskPhase(ordinaryTask))
}

// TestGlobalScheduler_pickNode_Balancing verifies that successive picks spread
// tasks evenly across nodes (water-filling) instead of packing one node first.
func TestGlobalScheduler_pickNode_Balancing(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)

	nodes := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
		2: {NodeID: 2, AvailableSlots: 100},
		3: {NodeID: 3, AvailableSlots: 100},
	}

	assigned := map[int64]int{}
	// Each task needs 10 slots; 30 tasks should be spread 10 per node.
	for i := 0; i < 30; i++ {
		nodeID := scheduler.pickNode(nodes, 10)
		assert.NotEqual(t, int64(NullNodeID), nodeID)
		assigned[nodeID]++
	}

	for nodeID, ws := range nodes {
		assert.Equal(t, 10, assigned[nodeID], "node %d should receive an even share", nodeID)
		assert.Equal(t, int64(0), ws.AvailableSlots, "node %d should be fully drained", nodeID)
	}

	// All nodes are now empty: further picks return NullNodeID.
	assert.Equal(t, int64(NullNodeID), scheduler.pickNode(nodes, 1))
}

func TestGlobalScheduler_pickNode_MixedTaskSizes(t *testing.T) {
	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)

	nodes := map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
		2: {NodeID: 2, AvailableSlots: 80},
		3: {NodeID: 3, AvailableSlots: 60},
	}

	assert.Equal(t, int64(1), scheduler.pickNode(nodes, 30))
	assert.Equal(t, int64(2), scheduler.pickNode(nodes, 70))
	assert.Equal(t, int64(1), scheduler.pickNode(nodes, 50))
	assert.Equal(t, int64(3), scheduler.pickNode(nodes, 90))

	assert.Equal(t, int64(20), nodes[1].AvailableSlots)
	assert.Equal(t, int64(10), nodes[2].AvailableSlots)
	assert.Equal(t, int64(0), nodes[3].AvailableSlots)
}

func TestGlobalScheduler_TestSchedule(t *testing.T) {
	newCluster := func() session.Cluster {
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
			1: {
				NodeID:         1,
				AvailableSlots: 100,
			},
			2: {
				NodeID:         2,
				AvailableSlots: 100,
			},
		}).Maybe()
		return cluster
	}

	newTask := func() *MockTask {
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(1).Maybe()
		task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
		task.EXPECT().GetTaskSlot().Return(1).Maybe()
		return task
	}

	t.Run("task retry when CreateTaskOnWorker", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), newCluster())
		scheduler.Start()
		defer scheduler.Stop()

		task := newTask()
		var stateCounter atomic.Int32

		// Set initial state
		task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
			counter := stateCounter.Load()
			if counter == 0 {
				return taskcommon.Init
			}
			return taskcommon.Retry
		}).Maybe()

		task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			stateCounter.Store(1) // Mark that CreateTaskOnWorker was called
		}).Maybe()

		scheduler.Enqueue(task)
		assert.Eventually(t, func() bool {
			s := scheduler.(*globalTaskScheduler)
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			return task.GetTaskState() == taskcommon.Retry && s.tasks.Len() == 1
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("task retry when QueryTaskOnWorker", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), newCluster())
		scheduler.Start()
		defer scheduler.Stop()

		task := newTask()
		var stateCounter atomic.Int32

		task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
			counter := stateCounter.Load()
			switch counter {
			case 0:
				return taskcommon.Init
			case 1:
				return taskcommon.InProgress
			default:
				return taskcommon.Retry
			}
		}).Maybe()

		task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			stateCounter.Store(1) // CreateTaskOnWorker called
		}).Maybe()

		task.EXPECT().QueryTaskOnWorker(mock.Anything).Run(func(cluster session.Cluster) {
			stateCounter.Store(2) // QueryTaskOnWorker called
		}).Maybe()

		scheduler.Enqueue(task)
		assert.Eventually(t, func() bool {
			s := scheduler.(*globalTaskScheduler)
			s.mu.RLock(1)
			defer s.mu.RUnlock(1)
			return stateCounter.Load() >= 2 && taskPhase(task) == phaseCheck
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("zero slot task dispatched when nodes exhausted", func(t *testing.T) {
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
			10: {AvailableSlots: 0},
			20: {AvailableSlots: 0},
		}).Once()

		scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)
		task := NewMockTask(t)
		task.EXPECT().GetTaskID().Return(1).Maybe()
		task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
		task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
		task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
		task.EXPECT().GetTaskSlot().Return(int64(0)).Once()

		var dispatched atomic.Bool
		task.EXPECT().CreateTaskOnWorker(mock.MatchedBy(func(nodeID int64) bool {
			return nodeID == 10 || nodeID == 20
		}), mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			dispatched.Store(true)
		}).Once()

		scheduler.Enqueue(task)
		runRoundSync(scheduler)

		assert.True(t, dispatched.Load())
	})

	t.Run("normal case", func(t *testing.T) {
		scheduler := NewGlobalTaskScheduler(context.TODO(), newCluster())
		scheduler.Start()
		defer scheduler.Stop()

		task := newTask()
		var stateCounter atomic.Int32

		task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
			counter := stateCounter.Load()
			switch counter {
			case 0:
				return taskcommon.Init
			case 1:
				return taskcommon.InProgress
			default:
				return taskcommon.Finished
			}
		}).Maybe()

		task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
			stateCounter.Store(1) // CreateTaskOnWorker called
		}).Maybe()

		task.EXPECT().QueryTaskOnWorker(mock.Anything).Run(func(cluster session.Cluster) {
			stateCounter.Store(2) // QueryTaskOnWorker called
		}).Maybe()

		task.EXPECT().DropTaskOnWorker(mock.Anything).Run(func(cluster session.Cluster) {
			stateCounter.Store(3) // DropTaskOnWorker called
		}).Maybe()

		scheduler.Enqueue(task)
		assert.Eventually(t, func() bool {
			s := scheduler.(*globalTaskScheduler)
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			return task.GetTaskState() == taskcommon.Finished && s.tasks.Len() == 0
		}, 10*time.Second, 10*time.Millisecond)
	})
}

func TestGlobalScheduler_RecordTaskFailureBackoff(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "1")
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffMaxInterval.Key, "4")
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffInterval.Key)
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffMaxInterval.Key)

	scheduler := NewGlobalTaskScheduler(context.TODO(), nil).(*globalTaskScheduler)
	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(7).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Index).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()

	// The backoff lives in the ownership entry, so it is only ever recorded
	// against a task the scheduler still owns.
	ownTask(scheduler, task)

	// exponential: 1s, 2s, 4s, then capped at the 4s max
	start := time.Now()
	scheduler.recordFailureUnderTaskLock(task)
	entry, ok := scheduler.tasks.Get(7)
	assert.True(t, ok)
	assert.Equal(t, 1, entry.failures)
	assert.InDelta(t, 1.0, entry.notBefore.Sub(start).Seconds(), 0.5)
	assert.True(t, entry.dispatchDelayed())

	scheduler.recordFailureUnderTaskLock(task)
	scheduler.recordFailureUnderTaskLock(task)
	scheduler.recordFailureUnderTaskLock(task)
	entry, _ = scheduler.tasks.Get(7)
	assert.Equal(t, 4, entry.failures)
	assert.InDelta(t, 4.0, time.Until(entry.notBefore).Seconds(), 0.5)

	// Releasing the task releases its backoff with it: there is no second entry
	// left behind to hold a task re-enqueued under the same ID back.
	scheduler.releaseUnderTaskLock(7)
	ownTask(scheduler, task)
	entry, _ = scheduler.tasks.Get(7)
	assert.Equal(t, 0, entry.failures)
	assert.False(t, entry.dispatchDelayed())

	// A failure recorded against a task the scheduler no longer owns is dropped
	// rather than putting an unowned entry back.
	scheduler.releaseUnderTaskLock(7)
	scheduler.recordFailureUnderTaskLock(task)
	assert.False(t, scheduler.tasks.Contain(7))

	// interval 0 disables the mechanism entirely
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "0")
	ownTask(scheduler, task)
	scheduler.recordFailureUnderTaskLock(task)
	entry, _ = scheduler.tasks.Get(7)
	assert.False(t, entry.dispatchDelayed())
}

func TestGlobalScheduler_FailedTaskBacksOffBeforeRedispatch(t *testing.T) {
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.TaskRetryBackoffInterval.Key, "1")
	defer pt.Reset(pt.DataCoordCfg.TaskRetryBackoffInterval.Key)

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
	}).Maybe()

	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster)
	scheduler.Start()
	defer scheduler.Stop()

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Index).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	task.EXPECT().GetTaskSlot().Return(1).Maybe()
	// CreateTaskOnWorker never flips the state away from Init: every dispatch fails
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
	var createCalls atomic.Int32
	task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
		createCalls.Add(1)
	}).Maybe()

	scheduler.Enqueue(task)

	// the first dispatch happens promptly
	assert.Eventually(t, func() bool { return createCalls.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	// during the 1s backoff the ~100ms scheduling tick must NOT re-dispatch
	// (without backoff this would already be ~5 more dispatches)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), createCalls.Load())
	// after the backoff elapses it is dispatched again
	assert.Eventually(t, func() bool { return createCalls.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
}

// TestGlobalScheduler_TerminalTaskClearsBackoff guards against a backoff leak:
// when CreateTaskOnWorker drives a task straight to a terminal state it never
// reaches the check pass, so a cleanup that only ran there would never run. The
// backoff riding in the ownership entry is what makes the dispatch path release
// it too, without a second cleanup that could be forgotten.
func TestGlobalScheduler_TerminalTaskClearsBackoff(t *testing.T) {
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 100},
	}).Maybe()

	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(9).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Index).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	task.EXPECT().GetTaskSlot().Return(1).Maybe()

	// CreateTaskOnWorker drives the task straight to a terminal state (e.g. its
	// segment was compacted away), so it never reaches the check loop.
	var created atomic.Bool
	task.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
		if created.Load() {
			return taskcommon.None
		}
		return taskcommon.Init
	}).Maybe()
	task.EXPECT().CreateTaskOnWorker(mock.Anything, mock.Anything).Run(func(nodeID int64, cluster session.Cluster) {
		created.Store(true)
	}).Maybe()
	task.EXPECT().DropTaskOnWorker(mock.Anything).Return().Once()

	// Seed accumulated failures from earlier dispatches whose delay has already
	// elapsed, so the task is eligible for dispatch this round.
	scheduler.tasks.Insert(9, &taskEntry{task: task, failures: 3, notBefore: time.Now().Add(-time.Second)})

	runRoundSync(scheduler)

	assert.Equal(t, 0, scheduler.tasks.Len(),
		"the entry -- task and backoff both -- must be gone once the task is terminal")
}

// Finalize is how an owner takes a task back from the scheduler. Two properties
// make it the handover point: the task can no longer be dispatched, and the
// callback the owner runs cannot interleave with a worker callback.
func TestGlobalScheduler_FinalizeRemovesTaskFromDispatch(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
	task.EXPECT().GetTaskSlot().Return(1).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	task.EXPECT().GetTaskTime(mock.Anything).Return(time.Now()).Maybe()
	task.EXPECT().GetTaskVersion().Return(0).Maybe()
	scheduler.Enqueue(task)
	assert.True(t, scheduler.tasks.Contain(1))

	ran := false
	scheduler.Finalize(1, func() { ran = true })

	assert.True(t, ran, "Finalize must run the owner's callback")
	assert.False(t, scheduler.tasks.Contain(1),
		"a finalized task must not be dispatchable again")
}

func TestGlobalScheduler_FinalizeWaitsForInFlightCallback(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	// Stand in for a worker callback holding the per-task lock across its RPC.
	scheduler.mu.Lock(int64(1))

	done := make(chan struct{})
	go func() {
		scheduler.Finalize(1, func() { close(done) })
	}()

	select {
	case <-done:
		t.Fatal("Finalize ran the owner's callback while a worker callback held the task")
	case <-time.After(200 * time.Millisecond):
	}

	scheduler.mu.Unlock(int64(1))
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Finalize did not proceed after the worker callback drained")
	}
}

// Update borrows the task lock so an owner's state machine cannot interleave
// with a worker callback. It waits for an in-flight callback rather than
// skipping the round, so the owner always gets its turn.
func TestGlobalScheduler_UpdateWaitsForInFlightCallback(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	// Stand in for a worker callback holding the per-task lock across its RPC.
	scheduler.mu.Lock(int64(1))

	ran := make(chan struct{})
	go func() {
		scheduler.Update(1, func() { close(ran) })
	}()

	select {
	case <-ran:
		t.Fatal("Update ran while a worker callback held the task")
	case <-time.After(200 * time.Millisecond):
	}

	scheduler.mu.Unlock(int64(1))
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("Update did not proceed after the worker callback drained")
	}
}

// A dispatch that already popped a task re-inserts it while still holding the
// per-task lock. Finalize must therefore remove under the lock, or the handover
// is silently undone and the task gets dispatched again after cleanup.
func TestGlobalScheduler_FinalizeOutlastsInFlightDispatchReinsert(t *testing.T) {
	cluster := session.NewMockCluster(t)
	scheduler := NewGlobalTaskScheduler(context.TODO(), cluster).(*globalTaskScheduler)

	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(1).Maybe()
	task.EXPECT().GetTaskState().Return(taskcommon.Init).Maybe()
	task.EXPECT().GetTaskSlot().Return(1).Maybe()
	task.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
	task.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
	task.EXPECT().GetTaskTime(mock.Anything).Return(time.Now()).Maybe()
	task.EXPECT().GetTaskVersion().Return(0).Maybe()

	// Stand in for a dispatch that popped the task and is mid-CreateTaskOnWorker:
	// it holds the lock and will push the task back when its RPC fails.
	scheduler.mu.Lock(int64(1))
	go func() {
		time.Sleep(100 * time.Millisecond)
		ownTask(scheduler, task)
		scheduler.mu.Unlock(int64(1))
	}()

	scheduler.Finalize(1, func() {})

	assert.False(t, scheduler.tasks.Contain(1),
		"a task re-inserted by an in-flight callback must not survive Finalize")
}
