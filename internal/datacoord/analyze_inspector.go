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

package datacoord

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type analyzeInspector struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mt *meta

	scheduler task.GlobalScheduler
	handler   Handler
}

func newAnalyzeInspector(ctx context.Context,
	mt *meta,
	scheduler task.GlobalScheduler,
	handler Handler,
) *analyzeInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &analyzeInspector{
		ctx:       ctx,
		cancel:    cancel,
		mt:        mt,
		scheduler: scheduler,
		handler:   handler,
	}
}

func (ai *analyzeInspector) Start() {
	ai.enqueueActiveTasks()
	ai.wg.Add(1)
	go ai.retryLoop()
}

func (ai *analyzeInspector) Stop() {
	ai.cancel()
	ai.wg.Wait()
}

func (ai *analyzeInspector) retryLoop() {
	defer ai.wg.Done()
	interval := Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ai.ctx.Done():
			return
		case <-ticker.C:
			ai.enqueueActiveTasks()
		}
	}
}

// enqueueActiveTasks recovers dispatchable or assigned analyze tasks. Retry is
// terminal for this analyze attempt: its parent clustering compaction observes
// it and uses the existing replan path to allocate a fresh plan/task identity.
func (ai *analyzeInspector) enqueueActiveTasks() {
	analyzeTasks := ai.mt.analyzeMeta.GetAllTasks()
	for _, t := range analyzeTasks {
		if t == nil || t.GetState() == indexpb.JobState_JobStateRetry {
			continue
		}
		if t.GetState() != indexpb.JobState_JobStateInit &&
			t.GetState() != indexpb.JobState_JobStateInProgress {
			continue
		}
		ai.scheduler.Enqueue(newAnalyzeTask(
			proto.Clone(t).(*indexpb.AnalyzeTask),
			ai.mt,
			ai.handler,
		))
	}
}
