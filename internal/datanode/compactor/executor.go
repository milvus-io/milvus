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

package compactor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	maxTaskQueueNum = 1024
)

type Executor interface {
	Start(ctx context.Context)
	Enqueue(task Compactor) (bool, error)
	Slots() int64
	RemoveTask(planID int64)                                // Deprecated in 2.6
	GetResults(planID int64) []*datapb.CompactionPlanResult // Deprecated in 2.6
	RemoveExpiredTasks(ctx context.Context, cutoff time.Time) int
}

// taskState represents the state of a compaction task
// State transitions:
//   - executing -> completed (success)
//   - executing -> failed (error)
//
// Once a task reaches completed/failed state, it stays there until removed
type taskState struct {
	compactor Compactor
	state     datapb.CompactionTaskState
	result    *datapb.CompactionPlanResult
	// failReason is why Compact returned an error, kept so the plan result the
	// coordinator polls carries something to diagnose with. Empty unless state
	// is failed.
	failReason string
	// dropped records that DataCoord asked to remove this task while it was
	// still executing. The entry cannot be dropped right then -- the compactor
	// is running and completeTask still needs it -- so the removal is deferred
	// to completion instead of being silently ignored.
	dropped   bool
	startedAt time.Time
	// cancelRequested records that DropTask or the expiration sweep has already
	// requested cancellation. The entry and its slots remain owned until
	// completeTask runs.
	cancelRequested bool
}

type executor struct {
	mu sync.RWMutex

	tasks map[int64]*taskState // planID -> task state

	// Task queue for pending work
	taskCh chan Compactor

	// Slot tracking for resource management
	usingSlots int64

	// Slots(Slots Cap for DataCoord), ExecPool(MaxCompactionConcurrency) are all trying to control concurrency and resource usage,
	// which creates unnecessary complexity. We should use a single resource pool instead.
}

func NewExecutor() *executor {
	return &executor{
		tasks:      make(map[int64]*taskState),
		taskCh:     make(chan Compactor, maxTaskQueueNum),
		usingSlots: 0,
	}
}

func getTaskSlotUsage(task Compactor) int64 {
	// Calculate slot usage
	taskSlotUsage := task.GetSlotUsage()
	// compatible for old datacoord or unexpected request
	if taskSlotUsage <= 0 {
		switch task.GetCompactionType() {
		case datapb.CompactionType_ClusteringCompaction:
			taskSlotUsage = paramtable.Get().DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64()
		case datapb.CompactionType_MixCompaction:
			taskSlotUsage = paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64()
		case datapb.CompactionType_Level0DeleteCompaction:
			taskSlotUsage = paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64()
		case datapb.CompactionType_BumpSchemaVersionCompaction:
			taskSlotUsage = paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionSlotUsage.GetAsInt64()
		}
		mlog.Warn(context.TODO(), "illegal task slot usage, change it to a default value",
			mlog.Int64("illegalSlotUsage", task.GetSlotUsage()),
			mlog.Int64("defaultSlotUsage", taskSlotUsage),
			mlog.String("type", task.GetCompactionType().String()))
	}

	return taskSlotUsage
}

func (e *executor) Enqueue(task Compactor) (bool, error) {
	e.mu.Lock()

	planID := task.GetPlanID()

	// Check for duplicate task
	if _, exists := e.tasks[planID]; exists {
		e.mu.Unlock()
		mlog.Warn(context.TODO(), "duplicated compaction task",
			mlog.Int64("planID", planID),
			mlog.String("channel", task.GetChannelName()))
		return false, merr.WrapErrDuplicatedCompactionTask()
	}

	// Update slots and add task
	e.usingSlots += getTaskSlotUsage(task)
	e.tasks[planID] = &taskState{
		compactor: task,
		state:     datapb.CompactionTaskState_executing,
		result:    nil,
		startedAt: time.Now(),
	}
	e.mu.Unlock()

	e.taskCh <- task
	return true, nil
}

// Slots returns the used slots for compaction
func (e *executor) Slots() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.usingSlots
}

// completeTask updates task state to completed and adjusts slot usage.
//
// failErr preserves the worker failure reason for DataCoord metadata and logs.
func (e *executor) completeTask(planID int64, result *datapb.CompactionPlanResult, failErr error) {
	e.mu.Lock()

	if task, exists := e.tasks[planID]; exists {
		// Update state based on result
		if result != nil {
			task.state = datapb.CompactionTaskState_completed
			task.result = result
		} else {
			task.state = datapb.CompactionTaskState_failed
			if failErr != nil {
				task.failReason = failErr.Error()
			}
		}

		// Adjust slot usage
		e.usingSlots -= getTaskSlotUsage(task.compactor)
		if e.usingSlots < 0 {
			e.usingSlots = 0
		}
		// Honor a drop that arrived while this task was still running.
		if task.dropped {
			mlog.Info(context.TODO(), "removing compaction task dropped while it was executing",
				mlog.Int64("planID", planID))
			delete(e.tasks, planID)
		}
		e.mu.Unlock()

		task.compactor.Complete()
		return
	}

	e.mu.Unlock()
}

func (e *executor) RemoveTask(planID int64) {
	e.mu.Lock()

	task, exists := e.tasks[planID]
	if !exists {
		// Either the plan was already collected, or the drop outran its create
		// (CompactionV2 can still be in chunk-manager setup when DataCoord
		// abandons the RPC and drops the plan). In the latter case the late
		// Enqueue installs an entry no drop will ever follow -- the shared
		// DataNode expiration sweep is what reclaims it.
		e.mu.Unlock()
		return
	}
	if task.state == datapb.CompactionTaskState_executing {
		// Cannot remove its entry now, so remember the request. Without this the entry
		// would stay resident until this DataNode restarts, while DropTask still
		// reported success -- DataCoord would never ask again.
		task.dropped = true
		if task.cancelRequested {
			e.mu.Unlock()
			return
		}
		task.cancelRequested = true
		compactor := task.compactor
		e.mu.Unlock()

		mlog.Info(context.TODO(), "compaction task drop requested cancellation",
			mlog.Int64("planID", planID),
			mlog.FieldVChannel(compactor.GetChannelName()))
		// Cancel outside executor.mu: it may wake Compact, whose completion path
		// takes the same lock.
		compactor.Cancel()
		return
	}
	mlog.Info(context.TODO(), "Compaction task removed",
		mlog.Int64("planID", planID),
		mlog.FieldVChannel(task.compactor.GetChannelName()),
		mlog.String("state", task.state.String()))
	delete(e.tasks, planID)
	e.mu.Unlock()
}

func (e *executor) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-e.taskCh:
			GetExecPool().Submit(func() (any, error) {
				e.executeTask(task)
				return nil, nil
			})
		}
	}
}

// RemoveExpiredTasks removes terminal tasks older than cutoff and cancels old
// executing tasks. An executing entry stays registered with dropped=true until
// completeTask finishes, because that callback still owns the entry.
func (e *executor) RemoveExpiredTasks(ctx context.Context, cutoff time.Time) int {
	type expiredTask struct {
		planID    int64
		compactor Compactor
		state     datapb.CompactionTaskState
		startedAt time.Time
		executing bool
	}

	e.mu.Lock()
	expired := make([]expiredTask, 0)
	for planID, task := range e.tasks {
		if task.startedAt.IsZero() || task.startedAt.After(cutoff) {
			continue
		}
		if task.state == datapb.CompactionTaskState_executing {
			task.dropped = true
			if task.cancelRequested {
				continue
			}
			task.cancelRequested = true
			expired = append(expired, expiredTask{
				planID: planID, compactor: task.compactor, state: task.state,
				startedAt: task.startedAt, executing: true,
			})
			continue
		}
		expired = append(expired, expiredTask{
			planID: planID, compactor: task.compactor, state: task.state, startedAt: task.startedAt,
		})
		delete(e.tasks, planID)
	}
	e.mu.Unlock()

	for _, expiredTask := range expired {
		mlog.Info(ctx, "reclaiming uncollected compaction task entry",
			mlog.Int64("planID", expiredTask.planID),
			mlog.String("state", expiredTask.state.String()),
			mlog.Duration("age", time.Since(expiredTask.startedAt)))
		if expiredTask.executing {
			// Cancellation is non-blocking. A native task that does not promptly
			// observe it remains registered and keeps its slots until Compact returns
			// and completeTask performs the sole release.
			expiredTask.compactor.Cancel()
		}
	}
	return len(expired)
}

func (e *executor) executeTask(task Compactor) {
	log := mlog.With(
		mlog.Int64("planID", task.GetPlanID()),
		mlog.Int64("collection", task.GetCollection()),
		mlog.String("channel", task.GetChannelName()),
		mlog.String("type", task.GetCompactionType().String()),
	)

	log.Info(context.TODO(), "start to execute compaction")

	result, err := task.Compact()
	if err != nil {
		log.Warn(context.TODO(), "compaction task failed", mlog.Err(err))
		e.completeTask(task.GetPlanID(), nil, err)
		return
	}

	// Update task with result
	e.completeTask(task.GetPlanID(), result, nil)

	// Emit metrics
	getDataCount := func(binlogs []*datapb.FieldBinlog) int64 {
		count := int64(0)
		for _, binlog := range binlogs {
			for _, fbinlog := range binlog.GetBinlogs() {
				count += fbinlog.GetEntriesNum()
			}
		}
		return count
	}

	var entityCount int64
	var deleteCount int64
	lo.ForEach(result.Segments, func(seg *datapb.CompactionSegment, _ int) {
		entityCount += seg.GetNumOfRows()
		deleteCount += getDataCount(seg.GetDeltalogs())
	})
	metrics.DataNodeWriteDataCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		metrics.CompactionDataSourceLabel,
		metrics.InsertLabel,
		fmt.Sprint(task.GetCollection())).Add(float64(entityCount))
	metrics.DataNodeWriteDataCount.WithLabelValues(
		paramtable.GetStringNodeID(),
		metrics.CompactionDataSourceLabel,
		metrics.DeleteLabel,
		fmt.Sprint(task.GetCollection())).Add(float64(deleteCount))
	log.Info(context.TODO(), "end to execute compaction")
}

func (e *executor) GetResults(planID int64) []*datapb.CompactionPlanResult {
	if planID != 0 {
		result := e.getCompactionResult(planID)
		return []*datapb.CompactionPlanResult{result}
	}
	return e.getAllCompactionResults()
}

func (e *executor) getCompactionResult(planID int64) *datapb.CompactionPlanResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if task, exists := e.tasks[planID]; exists {
		if task.result != nil {
			return task.result
		}
		return &datapb.CompactionPlanResult{
			State:  task.state,
			PlanID: planID,
			Reason: task.failReason,
		}
	}

	// Task not found, return failed state
	return &datapb.CompactionPlanResult{
		PlanID: planID,
		State:  datapb.CompactionTaskState_failed,
		Reason: "compaction plan is unknown to this worker",
	}
}

func (e *executor) getAllCompactionResults() []*datapb.CompactionPlanResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		executing          []int64
		completed          []int64
		completedLevelZero []int64
	)

	results := make([]*datapb.CompactionPlanResult, 0)

	// Collect results from all tasks
	for planID, task := range e.tasks {
		if task.state == datapb.CompactionTaskState_executing {
			executing = append(executing, planID)
			results = append(results, &datapb.CompactionPlanResult{
				State:  datapb.CompactionTaskState_executing,
				PlanID: planID,
			})
		} else if task.result != nil {
			completed = append(completed, planID)
			results = append(results, task.result)

			if task.result.GetType() == datapb.CompactionType_Level0DeleteCompaction {
				completedLevelZero = append(completedLevelZero, planID)
			}
		}
	}

	// Remove completed level zero compaction tasks
	for _, planID := range completedLevelZero {
		delete(e.tasks, planID)
	}

	if len(results) > 0 {
		mlog.Info(context.TODO(), "DataNode Compaction results",
			mlog.Int64s("executing", executing),
			mlog.Int64s("completed", completed),
			mlog.Int64s("completed levelzero", completedLevelZero),
		)
	}

	return results
}
