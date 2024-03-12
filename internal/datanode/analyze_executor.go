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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	maxAnalyzeTaskNum = 1024
)

type analyzeExecutor struct {
	executing *typeutil.ConcurrentMap[int64, analyzer]                   // planID to analyzer
	completed *typeutil.ConcurrentMap[int64, *datapb.AnalyzeStatsResult] // planID to AnalyzeStatsResult
	taskCh    chan analyzer
	dropped   *typeutil.ConcurrentSet[string] // vchannel dropped
}

func newAnalyzeExecutor() *analyzeExecutor {
	return &analyzeExecutor{
		executing: typeutil.NewConcurrentMap[int64, analyzer](),
		completed: typeutil.NewConcurrentMap[int64, *datapb.AnalyzeStatsResult](),
		taskCh:    make(chan analyzer, maxAnalyzeTaskNum),
		dropped:   typeutil.NewConcurrentSet[string](),
	}
}

func (c *analyzeExecutor) execute(task analyzer) {
	c.taskCh <- task
	c.toExecutingState(task)
}

func (c *analyzeExecutor) toExecutingState(task analyzer) {
	c.executing.Insert(task.getPlanID(), task)
}

func (c *analyzeExecutor) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-c.taskCh:
			go c.executeTask(task)
		}
	}
}

func (c *analyzeExecutor) executeTask(task analyzer) {
	log := log.With(zap.Int64("planID", task.getPlanID()))

	defer func() {
		c.executing.GetAndRemove(task.getPlanID())
	}()

	log.Info("start to execute analyze")

	result, err := task.execute()
	if err != nil {
		log.Warn("analyze task failed", zap.Error(err))
		c.completed.Insert(result.GetPlanID(), &datapb.AnalyzeStatsResult{
			PlanID:       task.getPlanID(),
			Finished:     true,
			ErrorMessage: err.Error(),
		})
	} else {
		c.completed.Insert(result.GetPlanID(), result)
	}

	log.Info("end to execute analyze", zap.Int64("planID", task.getPlanID()))
}

func (c *analyzeExecutor) getTaskState(planID int64) (*datapb.AnalyzeStatsResult, error) {
	_, executing := c.executing.Get(planID)
	if executing {
		return &datapb.AnalyzeStatsResult{PlanID: planID, Finished: false}, nil
	}
	result, finished := c.completed.Get(planID)
	if finished {
		return result, nil
	}
	// should not go here
	return nil, errors.New("task not found")
}
