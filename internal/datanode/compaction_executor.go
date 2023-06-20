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

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

const (
	maxTaskNum = 1024
)

type compactionExecutor struct {
	executing          sync.Map // planID to compactor
	completedCompactor sync.Map // planID to compactor
	completed          sync.Map // planID to CompactionResult
	taskCh             chan compactor
	dropped            sync.Map // vchannel dropped
}

func newCompactionExecutor() *compactionExecutor {
	return &compactionExecutor{
		executing: sync.Map{},
		taskCh:    make(chan compactor, maxTaskNum),
	}
}

func (c *compactionExecutor) execute(task compactor) {
	c.taskCh <- task
	c.toExecutingState(task)
}

func (c *compactionExecutor) toExecutingState(task compactor) {
	c.executing.Store(task.getPlanID(), task)
}

func (c *compactionExecutor) toCompleteState(task compactor) {
	task.complete()
	c.executing.Delete(task.getPlanID())
}

func (c *compactionExecutor) injectDone(planID UniqueID, success bool) {
	c.completed.Delete(planID)
	task, loaded := c.completedCompactor.LoadAndDelete(planID)
	if loaded {
		task.(compactor).injectDone(success)
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
		c.completed.Store(task.getPlanID(), result)
		c.completedCompactor.Store(task.getPlanID(), task)
	}

	log.Info("end to execute compaction", zap.Int64("planID", task.getPlanID()))
}

func (c *compactionExecutor) stopTask(planID UniqueID) {
	task, loaded := c.executing.LoadAndDelete(planID)
	if loaded {
		log.Warn("compaction executor stop task", zap.Int64("planID", planID), zap.String("vChannelName", task.(compactor).getChannelName()))
		task.(compactor).stop()
	}
}

func (c *compactionExecutor) channelValidateForCompaction(vChannelName string) bool {
	// if vchannel marked dropped, compaction should not proceed
	_, loaded := c.dropped.Load(vChannelName)
	return !loaded
}

func (c *compactionExecutor) stopExecutingtaskByVChannelName(vChannelName string) {
	c.dropped.Store(vChannelName, struct{}{})
	c.executing.Range(func(key interface{}, value interface{}) bool {
		if value.(compactor).getChannelName() == vChannelName {
			c.stopTask(key.(UniqueID))
		}
		return true
	})
	// remove all completed plans for vChannelName
	c.completed.Range(func(key interface{}, value interface{}) bool {
		if value.(*datapb.CompactionResult).GetChannel() == vChannelName {
			c.injectDone(key.(UniqueID), true)
			log.Info("remove compaction results for dropped channel",
				zap.String("channel", vChannelName),
				zap.Int64("planID", key.(UniqueID)))
		}
		return true
	})
}
