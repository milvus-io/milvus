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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		}
		log.Warn("illegal task slot usage, change it to a default value",
			zap.Int64("illegalSlotUsage", task.GetSlotUsage()),
			zap.Int64("defaultSlotUsage", taskSlotUsage),
			zap.String("type", task.GetCompactionType().String()))
	}

	return taskSlotUsage
}

func (e *executor) Enqueue(task Compactor) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	planID := task.GetPlanID()

	// Check for duplicate task
	if _, exists := e.tasks[planID]; exists {
		log.Warn("duplicated compaction task",
			zap.Int64("planID", planID),
			zap.String("channel", task.GetChannelName()))
		return false, merr.WrapErrDuplicatedCompactionTask()
	}

	// Update slots and add task
	e.usingSlots += getTaskSlotUsage(task)
	e.tasks[planID] = &taskState{
		compactor: task,
		state:     datapb.CompactionTaskState_executing,
		result:    nil,
	}

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
	defer e.mu.Unlock()

	if task, exists := e.tasks[planID]; exists {
		task.compactor.Complete()

		// Update state based on result
		if result != nil {
			task.state = datapb.CompactionTaskState_completed
			task.result = result
		} else {
			task.state = datapb.CompactionTaskState_failed
		}

		// Publish filesystem metrics after compaction task completion
		storageConfig := task.compactor.GetStorageConfig()
		if _, err := storagev2.PublishFilesystemMetricsWithConfig(storageConfig); err != nil {
			log.Warn("failed to publish filesystem metrics", zap.Error(err))
		}

		// Adjust slot usage
		e.usingSlots -= getTaskSlotUsage(task.compactor)
		if e.usingSlots < 0 {
			e.usingSlots = 0
		}
	}
}

func (e *executor) RemoveTask(planID int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if task, exists := e.tasks[planID]; exists {
		// Only remove completed/failed tasks, not executing ones
		if task.state != datapb.CompactionTaskState_executing {
			log.Info("Compaction task removed",
				zap.Int64("planID", planID),
				zap.String("channel", task.compactor.GetChannelName()),
				zap.String("state", task.state.String()))
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
				e.executeTask(task)
				return nil, nil
			})
		}
	}
}

func (e *executor) executeTask(task Compactor) {
	log := log.With(
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("collection", task.GetCollection()),
		zap.String("channel", task.GetChannelName()),
		zap.String("type", task.GetCompactionType().String()),
	)

	log.Info("start to execute compaction")

	result, err := task.Compact()
	if err != nil {
		log.Warn("compaction task failed", zap.Error(err))
		e.completeTask(task.GetPlanID(), nil)
		return
	}

	// Update task with result
	e.completeTask(task.GetPlanID(), result)

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
	log.Info("end to execute compaction")
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
		log.Info("DataNode Compaction results",
			zap.Int64s("executing", executing),
			zap.Int64s("completed", completed),
			zap.Int64s("completed levelzero", completedLevelZero),
		)
	}

	return results
}
