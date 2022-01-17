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

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const (
	maxTaskNum = 1024
)

var maxParallelCompactionNum = calculateParallel()

type compactionExecutor struct {
	parallelCh chan struct{}
	executing  sync.Map // planID to compactor
	taskCh     chan compactor
	dropped    sync.Map // vchannel dropped
}

// 0.5*min(8, NumCPU/2)
func calculateParallel() int {
	return 2
	//cores := runtime.NumCPU()
	//if cores < 16 {
	//return 4
	//}
	//return cores / 2
}

func newCompactionExecutor() *compactionExecutor {
	return &compactionExecutor{
		parallelCh: make(chan struct{}, maxParallelCompactionNum),
		executing:  sync.Map{},
		taskCh:     make(chan compactor, maxTaskNum),
	}
}

func (c *compactionExecutor) execute(task compactor) {
	c.taskCh <- task
}

func (c *compactionExecutor) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-c.taskCh:
			go c.executeTask(task)
		}
	}
}

func (c *compactionExecutor) executeTask(task compactor) {
	c.parallelCh <- struct{}{}
	defer func() {
		<-c.parallelCh
	}()

	c.executing.Store(task.getPlanID(), task)
	log.Info("start to execute compaction", zap.Int64("planID", task.getPlanID()))

	err := task.compact()
	if err != nil {
		log.Warn("compaction task failed",
			zap.Int64("planID", task.getPlanID()),
			zap.Error(err),
		)
	}

	c.executing.Delete(task.getPlanID())
	log.Info("end to execute compaction", zap.Int64("planID", task.getPlanID()))
}

func (c *compactionExecutor) stopTask(planID UniqueID) {
	task, loaded := c.executing.LoadAndDelete(planID)
	if loaded {
		log.Warn("compaction executor stop task", zap.Int64("planID", planID))
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
		log.Warn(value.(compactor).getChannelName())
		return true
	})
}
