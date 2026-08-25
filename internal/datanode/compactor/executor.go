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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/datanode/resource"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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

// taskRequirement is what this compaction is charged against the node.
//
// The coordinator's figure wins whenever it is there, and that is not a matter
// of taste. DataCoord prices from SegmentInfo.Stats, which is populated on
// every storage version; recomputing here can only read the plan's
// per-FieldBinlog arrays, and those are EMPTY for a storage-V3 segment once
// DataCoord has restarted -- kv_catalog.AlterSegments skips writing the
// per-FieldBinlog KVs for V3 and the data is described by the manifest
// instead. A local recompute therefore prices a multi-GiB V3 mix or sort
// compaction at the estimator's 64MiB floor, which is exactly the workload
// issue #52180 is about and exactly the protection this charge exists to
// provide.
//
// The two fallbacks below are for an un-upgraded coordinator, in descending
// order of how much the request still says:
//
//   - No vector but a plan: recompute from the binlog arrays. Correct on V1/V2,
//     and on V3 no worse than the pre-branch behavior of not charging at all.
//   - No plan: the legacy scalar slot, floored at one so an absent slot is
//     never read as "this task is free".
func taskRequirement(task Compactor) taskresource.Requirement {
	plan := task.GetPlan()
	if plan == nil {
		return taskresource.LegacySlotToRequirement(getTaskSlotUsage(task))
	}
	if req, ok := taskresource.RequirementFromProto(plan.GetTaskResources()); ok {
		return req
	}
	return taskresource.RequirementForCompaction(plan)
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

// completeTask updates task state to completed and adjusts slot usage
func (e *executor) completeTask(planID int64, result *datapb.CompactionPlanResult) {
	e.mu.Lock()

	if task, exists := e.tasks[planID]; exists {
		// Update state based on result
		if result != nil {
			task.state = datapb.CompactionTaskState_completed
			task.result = result
		} else {
			task.state = datapb.CompactionTaskState_failed
		}

		// Adjust slot usage
		e.usingSlots -= getTaskSlotUsage(task.compactor)
		if e.usingSlots < 0 {
			e.usingSlots = 0
		}
		e.mu.Unlock()

		task.compactor.Complete()

		// Publish filesystem metrics after compaction task completion
		storageConfig := task.compactor.GetStorageConfig()
		if _, err := storagev2.PublishFilesystemMetricsWithConfig(storageConfig); err != nil {
			mlog.Warn(context.TODO(), "failed to publish filesystem metrics", mlog.Err(err))
		}
		return
	}

	e.mu.Unlock()
}

func (e *executor) RemoveTask(planID int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if task, exists := e.tasks[planID]; exists {
		// Only remove completed/failed tasks, not executing ones
		if task.state != datapb.CompactionTaskState_executing {
			mlog.Info(context.TODO(), "Compaction task removed",
				mlog.Int64("planID", planID),
				mlog.String("channel", task.compactor.GetChannelName()),
				mlog.String("state", task.state.String()))
			delete(e.tasks, planID)
		}
	}
}

func (e *executor) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-e.taskCh:
			GetExecPool().Submit(func() (any, error) {
				e.executeTask(ctx, task)
				return nil, nil
			})
		}
	}
}

// executeTask runs one compaction. ctx is the node's lifecycle context, handed
// down from Start: it is the only thing that bounds how long the task waits for
// the node's budget, so a shutdown does not leave a task queued forever.
func (e *executor) executeTask(ctx context.Context, task Compactor) {
	planID := task.GetPlanID()
	log := mlog.With(
		mlog.Int64("planID", planID),
		mlog.Int64("collection", task.GetCollection()),
		mlog.String("channel", task.GetChannelName()),
		mlog.String("type", task.GetCompactionType().String()),
	)

	// Reserve the node's budget before any work starts. Admission happens here,
	// where the task actually begins, and not when the RPC arrived: waiting then
	// shows up as a task sitting in this node's queue rather than as a dispatch
	// the coordinator watches time out.
	//
	// The guard never refuses permanently -- a task larger than the whole node
	// waits for it to drain and then runs alone -- so ctx is the only bound on
	// this wait, and it is the node's lifecycle context. A timer here would only
	// convert waiting into a re-dispatch of the same task by the coordinator,
	// which is the same wait with a round trip added.
	req := taskRequirement(task)
	if err := resource.GetGuard().Accept(ctx, planID, taskcommon.Compaction, req); err != nil {
		log.Warn(ctx, "compaction gave up waiting for the node's budget",
			mlog.String("requirement", req.String()), mlog.Err(err))
		// Nothing was reserved, so nothing is released. The executor's own slot
		// accounting still has to be unwound, which completeTask does.
		e.completeTask(planID, nil)
		return
	}
	defer resource.GetGuard().Release(planID)

	log.Info(ctx, "start to execute compaction")

	result, err := task.Compact()
	if err != nil {
		log.Warn(ctx, "compaction task failed", mlog.Err(err))
		e.completeTask(planID, nil)
		return
	}

	// Update task with result
	e.completeTask(planID, result)

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
	log.Info(ctx, "end to execute compaction")
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
		}
	}

	// Task not found, return failed state
	return &datapb.CompactionPlanResult{
		PlanID: planID,
		State:  datapb.CompactionTaskState_failed,
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
