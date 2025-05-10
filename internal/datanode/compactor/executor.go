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
	"golang.org/x/sync/semaphore"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	maxTaskQueueNum    = 1024
	maxParallelTaskNum = 10
)

type Executor interface {
	Start(ctx context.Context)
	Execute(task Compactor) (bool, error)
	Slots() int64
	RemoveTask(planID int64)
	GetResults(planID int64) []*datapb.CompactionPlanResult
	DiscardByDroppedChannel(channel string)
	DiscardPlan(channel string)
}

type executor struct {
	executing          *typeutil.ConcurrentMap[int64, Compactor]                    // planID to compactor
	completedCompactor *typeutil.ConcurrentMap[int64, Compactor]                    // planID to compactor
	completed          *typeutil.ConcurrentMap[int64, *datapb.CompactionPlanResult] // planID to CompactionPlanResult
	taskCh             chan Compactor
	taskSem            *semaphore.Weighted             // todo remove this, unify with slot logic
	dropped            *typeutil.ConcurrentSet[string] // vchannel dropped
	usingSlots         int64
	slotMu             sync.RWMutex

	// To prevent concurrency of release channel and compaction get results
	// all released channel's compaction tasks will be discarded
	resultGuard sync.RWMutex
}

func NewExecutor() *executor {
	return &executor{
		executing:          typeutil.NewConcurrentMap[int64, Compactor](),
		completedCompactor: typeutil.NewConcurrentMap[int64, Compactor](),
		completed:          typeutil.NewConcurrentMap[int64, *datapb.CompactionPlanResult](),
		taskCh:             make(chan Compactor, maxTaskQueueNum),
		taskSem:            semaphore.NewWeighted(maxParallelTaskNum),
		dropped:            typeutil.NewConcurrentSet[string](),
		usingSlots:         0,
	}
}

func (e *executor) Execute(task Compactor) (bool, error) {
	e.slotMu.Lock()
	defer e.slotMu.Unlock()
	newSlotUsage := task.GetSlotUsage()
	// compatible for old datacoord or unexpected request
	if task.GetSlotUsage() <= 0 {
		switch task.GetCompactionType() {
		case datapb.CompactionType_ClusteringCompaction:
			newSlotUsage = paramtable.Get().DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64()
		case datapb.CompactionType_MixCompaction:
			newSlotUsage = paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64()
		case datapb.CompactionType_Level0DeleteCompaction:
			newSlotUsage = paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64()
		}
		log.Warn("illegal task slot usage, change it to a default value", zap.Int64("illegalSlotUsage", task.GetSlotUsage()), zap.Int64("newSlotUsage", newSlotUsage))
	}
	e.usingSlots = e.usingSlots + newSlotUsage
	_, ok := e.executing.GetOrInsert(task.GetPlanID(), task)
	if ok {
		log.Warn("duplicated compaction task",
			zap.Int64("planID", task.GetPlanID()),
			zap.String("channel", task.GetChannelName()))
		return false, merr.WrapErrDuplicatedCompactionTask()
	}
	e.taskCh <- task
	return true, nil
}

// Slots returns the available slots for compaction
func (e *executor) Slots() int64 {
	return e.getUsingSlots()
}

func (e *executor) getUsingSlots() int64 {
	e.slotMu.RLock()
	defer e.slotMu.RUnlock()
	return e.usingSlots
}

func (e *executor) toCompleteState(task Compactor) {
	task.Complete()
	e.getAndRemoveExecuting(task.GetPlanID())
}

func (e *executor) getAndRemoveExecuting(planID typeutil.UniqueID) (Compactor, bool) {
	task, ok := e.executing.GetAndRemove(planID)
	if ok {
		e.slotMu.Lock()
		e.usingSlots = e.usingSlots - task.GetSlotUsage()
		e.slotMu.Unlock()
	}
	return task, ok
}

func (e *executor) RemoveTask(planID int64) {
	e.completed.GetAndRemove(planID)
	task, loaded := e.completedCompactor.GetAndRemove(planID)
	if loaded {
		log.Info("Compaction task removed", zap.Int64("planID", planID), zap.String("channel", task.GetChannelName()))
	}
}

func (e *executor) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-e.taskCh:
			err := e.taskSem.Acquire(ctx, 1)
			if err != nil {
				return
			}
			go func() {
				defer e.taskSem.Release(1)
				e.executeTask(task)
			}()
		}
	}
}

func (e *executor) executeTask(task Compactor) {
	log := log.With(
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("Collection", task.GetCollection()),
		zap.String("channel", task.GetChannelName()),
	)

	defer func() {
		e.toCompleteState(task)
	}()

	log.Info("start to execute compaction")

	result, err := task.Compact()
	if err != nil {
		log.Warn("compaction task failed", zap.Error(err))
		return
	}
	e.completed.Insert(result.GetPlanID(), result)
	e.completedCompactor.Insert(result.GetPlanID(), task)

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
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.CompactionDataSourceLabel,
		metrics.InsertLabel,
		fmt.Sprint(task.GetCollection())).Add(float64(entityCount))
	metrics.DataNodeWriteDataCount.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.CompactionDataSourceLabel,
		metrics.DeleteLabel,
		fmt.Sprint(task.GetCollection())).Add(float64(deleteCount))
	log.Info("end to execute compaction")
}

func (e *executor) stopTask(planID int64) {
	task, loaded := e.getAndRemoveExecuting(planID)
	if loaded {
		log.Warn("compaction executor stop task", zap.Int64("planID", planID), zap.String("vChannelName", task.GetChannelName()))
		task.Stop()
	}
}

func (e *executor) isValidChannel(channel string) bool {
	// if vchannel marked dropped, compaction should not proceed
	return !e.dropped.Contain(channel)
}

func (e *executor) DiscardByDroppedChannel(channel string) {
	e.dropped.Insert(channel)
	e.DiscardPlan(channel)
}

func (e *executor) DiscardPlan(channel string) {
	e.resultGuard.Lock()
	defer e.resultGuard.Unlock()

	e.executing.Range(func(planID int64, task Compactor) bool {
		if task.GetChannelName() == channel {
			e.stopTask(planID)
		}
		return true
	})

	// remove all completed plans of channel
	e.completed.Range(func(planID int64, result *datapb.CompactionPlanResult) bool {
		if result.GetChannel() == channel {
			e.RemoveTask(planID)
			log.Info("remove compaction plan and results",
				zap.String("channel", channel),
				zap.Int64("planID", planID))
		}
		return true
	})
}

func (e *executor) GetResults(planID int64) []*datapb.CompactionPlanResult {
	if planID != 0 {
		result := e.getCompactionResult(planID)
		return []*datapb.CompactionPlanResult{result}
	}
	return e.getAllCompactionResults()
}

func (e *executor) getCompactionResult(planID int64) *datapb.CompactionPlanResult {
	e.resultGuard.RLock()
	defer e.resultGuard.RUnlock()
	_, ok := e.executing.Get(planID)
	if ok {
		result := &datapb.CompactionPlanResult{
			State:  datapb.CompactionTaskState_executing,
			PlanID: planID,
		}
		return result
	}
	result, ok2 := e.completed.Get(planID)
	if !ok2 {
		return &datapb.CompactionPlanResult{
			PlanID: planID,
			State:  datapb.CompactionTaskState_failed,
		}
	}
	return result
}

func (e *executor) getAllCompactionResults() []*datapb.CompactionPlanResult {
	e.resultGuard.RLock()
	defer e.resultGuard.RUnlock()
	var (
		executing          []int64
		completed          []int64
		completedLevelZero []int64
	)
	results := make([]*datapb.CompactionPlanResult, 0)
	// get executing results
	e.executing.Range(func(planID int64, task Compactor) bool {
		executing = append(executing, planID)
		results = append(results, &datapb.CompactionPlanResult{
			State:  datapb.CompactionTaskState_executing,
			PlanID: planID,
		})
		return true
	})

	// get completed results
	e.completed.Range(func(planID int64, result *datapb.CompactionPlanResult) bool {
		completed = append(completed, planID)
		results = append(results, result)

		if result.GetType() == datapb.CompactionType_Level0DeleteCompaction {
			completedLevelZero = append(completedLevelZero, planID)
		}
		return true
	})

	// remove level zero results
	lo.ForEach(completedLevelZero, func(planID int64, _ int) {
		e.completed.Remove(planID)
		e.completedCompactor.Remove(planID)
	})

	if len(results) > 0 {
		log.Info("DataNode Compaction results",
			zap.Int64s("executing", executing),
			zap.Int64s("completed", completed),
			zap.Int64s("completed levelzero", completedLevelZero),
		)
	}

	return results
}
