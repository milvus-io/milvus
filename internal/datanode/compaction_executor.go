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
	executing          *typeutil.ConcurrentMap[int64, compactor]                // planID to compactor
	completedCompactor *typeutil.ConcurrentMap[int64, compactor]                // planID to compactor
	completed          *typeutil.ConcurrentMap[int64, *datapb.CompactionResult] // planID to CompactionResult
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
		completed:          typeutil.NewConcurrentMap[int64, *datapb.CompactionResult](),
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

func (c *compactionExecutor) injectDone(planID UniqueID, success bool) {
	c.completed.GetAndRemove(planID)
	task, loaded := c.completedCompactor.GetAndRemove(planID)
	if loaded {
		task.injectDone(success)
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
	defer func() {
		c.toCompleteState(task)
	}()

	log.Info("start to execute compaction", zap.Int64("planID", task.getPlanID()), zap.Int64("Collection", task.getCollection()), zap.String("channel", task.getChannelName()))

	result, err := task.compact()
	if err != nil {
		log.Warn("compaction task failed",
			zap.Int64("planID", task.getPlanID()),
			zap.Error(err),
		)
	} else {
		c.completed.Insert(task.getPlanID(), result)
		c.completedCompactor.Insert(task.getPlanID(), task)
	}

	log.Info("end to execute compaction", zap.Int64("planID", task.getPlanID()))
}

func (c *compactionExecutor) stopTask(wg *sync.WaitGroup, planID UniqueID) {
	defer wg.Done()
	task, loaded := c.executing.GetAndRemove(planID)
	if loaded {
		log.Warn("executor stopping compaction task...", zap.Int64("planID", planID), zap.String("vChannelName", task.getChannelName()))
		task.stop()
		log.Warn("compaction task stopped", zap.Int64("planID", planID), zap.String("vChannelName", task.getChannelName()))
	}
}

func (c *compactionExecutor) channelValidateForCompaction(vChannelName string) bool {
	// if vchannel marked dropped, compaction should not proceed
	return !c.dropped.Contain(vChannelName)
}

func (c *compactionExecutor) discardByDroppedChannel(channel string) {
	c.dropped.Insert(channel)
	c.discardPlan(channel)
}

func (c *compactionExecutor) discardPlan(channel string) {
	c.resultGuard.Lock()
	defer c.resultGuard.Unlock()

	wg := &sync.WaitGroup{}
	c.executing.Range(func(planID int64, task compactor) bool {
		if task.getChannelName() == channel {
			wg.Add(1)
			go c.stopTask(wg, planID)
		}
		return true
	})
	wg.Wait()

	// remove all completed plans for channel
	c.completed.Range(func(planID int64, result *datapb.CompactionResult) bool {
		if result.GetChannel() == channel {
			c.injectDone(planID, true)
			log.Info("remove compaction plan",
				zap.String("channel", channel),
				zap.Int64("planID", planID))
		}
		return true
	})
}

func (c *compactionExecutor) getAllCompactionResults() []*datapb.CompactionStateResult {
	c.resultGuard.RLock()
	defer c.resultGuard.RUnlock()

	var (
		executing []int64
		completed []int64
	)
	results := make([]*datapb.CompactionStateResult, 0)
	// get executing results
	c.executing.Range(func(planID int64, task compactor) bool {
		executing = append(executing, planID)
		results = append(results, &datapb.CompactionStateResult{
			State:  commonpb.CompactionState_Executing,
			PlanID: planID,
		})
		return true
	})

	// get completed results
	c.completed.Range(func(planID int64, result *datapb.CompactionResult) bool {
		completed = append(completed, planID)
		results = append(results, &datapb.CompactionStateResult{
			State:  commonpb.CompactionState_Completed,
			PlanID: planID,
			Result: result,
		})

		return true
	})

	if len(results) > 0 {
		log.Info("DataNode Compaction results",
			zap.Int64s("executing", executing),
			zap.Int64s("completed", completed),
		)
	}

	return results
}
