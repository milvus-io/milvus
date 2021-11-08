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
	"runtime"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const (
	maxTaskNum = 1024
)

var maxParallelCompactionNum = calculeateParallel()

type compactionExecutor struct {
	parallelCh chan struct{}
	taskCh     chan compactor
}

// 0.5*min(8, NumCPU/2)
func calculeateParallel() int {
	cores := runtime.NumCPU()
	if cores < 16 {
		return 4
	}
	return cores / 2
}

func newCompactionExecutor() *compactionExecutor {
	return &compactionExecutor{
		parallelCh: make(chan struct{}, maxParallelCompactionNum),
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

	log.Info("start to execute compaction", zap.Int64("planID", task.getPlanID()))

	err := task.compact()
	if err != nil {
		log.Warn("compaction task failed",
			zap.Int64("planID", task.getPlanID()),
			zap.Error(err),
		)
	}

	log.Info("end to execute compaction", zap.Int64("planID", task.getPlanID()))
}
