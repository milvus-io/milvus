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

package datanode

import (
	"context"
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	maxTaskNum = 1024
)

type compactionExecutor struct {
	executing          *typeutil.ConcurrentMap[int64, compactor]                    // planID to compactor
	completedCompactor *typeutil.ConcurrentMap[int64, compactor]                    // planID to compactor
	completed          *typeutil.ConcurrentMap[int64, *datapb.CompactionPlanResult] // planID to CompactionPlanResult
	taskCh             chan compactor
	dropped            *typeutil.ConcurrentSet[string] // vchannel dropped

	// To prevent concurrency of release channel and compaction get results
	// all released channel's compaction tasks will be discarded
	resultGuard sync.RWMutex
}

func newCompactionExecutor() *compactionExecutor {
	return &compactionExecutor{
		executing:          typeutil.NewConcurrentMap[int64, compactor](),
		completedCompactor: typeutil.NewConcurrentMap[int64, compactor](),
		completed:          typeutil.NewConcurrentMap[int64, *datapb.CompactionPlanResult](),
		taskCh:             make(chan compactor, maxTaskNum),
		dropped:            typeutil.NewConcurrentSet[string](),
	}
}

func (c *compactionExecutor) execute(task compactor) {
	c.taskCh <- task
	c.toExecutingState(task)
}

func (c *compactionExecutor) toExecutingState(task compactor) {
	c.executing.Insert(task.getPlanID(), task)
}

func (c *compactionExecutor) toCompleteState(task compactor) {
	task.complete()
	c.executing.GetAndRemove(task.getPlanID())
}

func (c *compactionExecutor) removeTask(planID UniqueID) {
	c.completed.GetAndRemove(planID)
	task, loaded := c.completedCompactor.GetAndRemove(planID)
	if loaded {
		log.Info("Compaction task removed", zap.Int64("planID", planID), zap.String("channel", task.getChannelName()))
	}
}

// These two func are bounded for waitGroup
func (c *compactionExecutor) executeWithState(task compactor) {
	go c.executeTask(task)
}

func (c *compactionExecutor) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-c.taskCh:
			c.executeWithState(task)
		}
	}
}

func (c *compactionExecutor) executeTask(task compactor) {
	log := log.With(
		zap.Int64("planID", task.getPlanID()),
		zap.Int64("Collection", task.getCollection()),
		zap.String("channel", task.getChannelName()),
	)

	defer func() {
		c.toCompleteState(task)
	}()

	log.Info("start to execute compaction")

	result, err := task.compact()
	if err != nil {
		log.Warn("compaction task failed", zap.Error(err))
		return
	}
	c.completed.Insert(result.GetPlanID(), result)
	c.completedCompactor.Insert(result.GetPlanID(), task)

	log.Info("end to execute compaction")
}

func (c *compactionExecutor) stopTask(planID UniqueID) {
	task, loaded := c.executing.GetAndRemove(planID)
	if loaded {
		log.Warn("compaction executor stop task", zap.Int64("planID", planID), zap.String("vChannelName", task.getChannelName()))
		task.stop()
	}
}

func (c *compactionExecutor) isValidChannel(channel string) bool {
	// if vchannel marked dropped, compaction should not proceed
	return !c.dropped.Contain(channel)
}

func (c *compactionExecutor) discardByDroppedChannel(channel string) {
	c.dropped.Insert(channel)
	c.discardPlan(channel)
}

func (c *compactionExecutor) discardPlan(channel string) {
	c.resultGuard.Lock()
	defer c.resultGuard.Unlock()

	c.executing.Range(func(planID int64, task compactor) bool {
		if task.getChannelName() == channel {
			c.stopTask(planID)
		}
		return true
	})

	// remove all completed plans of channel
	c.completed.Range(func(planID int64, result *datapb.CompactionPlanResult) bool {
		if result.GetChannel() == channel {
			c.removeTask(planID)
			log.Info("remove compaction plan and results",
				zap.String("channel", channel),
				zap.Int64("planID", planID))
		}
		return true
	})
}

func (c *compactionExecutor) getAllCompactionResults() []*datapb.CompactionPlanResult {
	c.resultGuard.RLock()
	defer c.resultGuard.RUnlock()
	var (
		executing          []int64
		completed          []int64
		completedLevelZero []int64
	)
	results := make([]*datapb.CompactionPlanResult, 0)
	// get executing results
	c.executing.Range(func(planID int64, task compactor) bool {
		executing = append(executing, planID)
		results = append(results, &datapb.CompactionPlanResult{
			State:  commonpb.CompactionState_Executing,
			PlanID: planID,
		})
		return true
	})

	// get completed results
	c.completed.Range(func(planID int64, result *datapb.CompactionPlanResult) bool {
		completed = append(completed, planID)
		results = append(results, result)

		if result.GetType() == datapb.CompactionType_Level0DeleteCompaction {
			completedLevelZero = append(completedLevelZero, planID)
		}
		return true
	})

	// remote level zero results
	lo.ForEach(completedLevelZero, func(planID int64, _ int) {
		c.completed.Remove(planID)
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
