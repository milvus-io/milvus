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

package scheduler

import (
	"context"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ExecuteDQLQueue admits batch orchestration independently of child DQL work.
type ExecuteDQLQueue struct {
	*BaseTaskQueue
	ctx     context.Context
	workers int
}

// WithExecuteDQLQueue sizes the independent waiting and execution capacities.
func WithExecuteDQLQueue(capacity, workers int) SchedOpt {
	return func(s *TaskScheduler) {
		s.ExecuteDQLQueue.SetMaxTaskNum(int64(capacity))
		s.ExecuteDQLQueue.workers = workers
	}
}

func (queue *ExecuteDQLQueue) fullError() error {
	return merr.WrapErrTooManyRequests(int32(queue.GetMaxTaskNum()), "ExecuteDQL queue is full")
}

func (queue *ExecuteDQLQueue) CheckAdmission() error {
	if queue.IsFull() {
		return queue.fullError()
	}
	return nil
}

func (queue *ExecuteDQLQueue) Enqueue(t taskmodel.Task) error {
	if queue.ctx.Err() != nil {
		return merr.WrapErrServiceUnavailableMsg("ExecuteDQL scheduler is stopping")
	}
	element, err := queue.enqueue(t)
	if IsAdmissionQueueFull(err) {
		return queue.fullError()
	}
	if err != nil {
		return err
	}
	t.SetOnContextDone(func() {
		queue.removeUnissuedTask(element)
	})
	return nil
}

func (sched *TaskScheduler) executeDQLLoop() {
	defer sched.wg.Done()
	queue := sched.ExecuteDQLQueue
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-queue.utChan():
			if sched.ctx.Err() != nil {
				return
			}
			task := queue.PopUnissuedTask()
			if !queue.utEmpty() {
				queue.notify()
			}
			if task != nil {
				sched.processTask(task, queue)
			}
		}
	}
}
