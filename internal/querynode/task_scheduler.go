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

package querynode

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg    sync.WaitGroup
	queue taskQueue
}

func newTaskScheduler(ctx context.Context) *taskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:    ctx1,
		cancel: cancel,
	}
	s.queue = newQueryNodeTaskQueue(s)
	return s
}

func (s *taskScheduler) processTask(t task, q taskQueue) {
	// TODO: ctx?
	err := t.PreExecute(s.ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Warn(err.Error())
		return
	}

	q.AddActiveTask(t)
	defer func() {
		q.PopActiveTask(t.ID())
	}()

	err = t.Execute(s.ctx)
	if err != nil {
		log.Warn(err.Error())
		return
	}
	err = t.PostExecute(s.ctx)
}

func (s *taskScheduler) taskLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.queue.utChan():
			if !s.queue.utEmpty() {
				t := s.queue.PopUnissuedTask()
				s.processTask(t, s.queue)
			}
		}
	}
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.taskLoop()
}

func (s *taskScheduler) Close() {
	s.cancel()
	s.wg.Wait()
}
