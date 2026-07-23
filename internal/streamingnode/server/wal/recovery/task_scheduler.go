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

package recovery

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type scopedTaskScheduler struct {
	inner nodescheduler.Scheduler

	mu      sync.Mutex
	nextID  uint64
	tasks   map[uint64]nodescheduler.TaskHandle
	changed chan struct{}
	closed  bool
}

func newScopedTaskScheduler(inner nodescheduler.Scheduler) *scopedTaskScheduler {
	return &scopedTaskScheduler{
		inner:   inner,
		tasks:   make(map[uint64]nodescheduler.TaskHandle),
		changed: make(chan struct{}),
	}
}

func (s *scopedTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		handle := s.inner.Submit(task)
		handle.Cancel()
		return handle
	}

	id := s.nextID
	s.nextID++
	handle := s.inner.Submit(&trackedTask{
		owner: s,
		id:    id,
		task:  task,
	})
	s.tasks[id] = handle
	s.signalChangedLocked()
	s.mu.Unlock()
	return handle
}

func (s *scopedTaskScheduler) WaitIdle(ctx context.Context) error {
	for {
		s.mu.Lock()
		if len(s.tasks) == 0 {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *scopedTaskScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = s.WaitIdle(context.Background())
		return
	}
	s.closed = true
	tasks := make(map[uint64]nodescheduler.TaskHandle, len(s.tasks))
	for id, handle := range s.tasks {
		tasks[id] = handle
	}
	s.mu.Unlock()

	for _, handle := range tasks {
		handle.Cancel()
	}
	for id, handle := range tasks {
		_ = handle.Wait(context.Background())
		s.finish(id)
	}
}

func (s *scopedTaskScheduler) finish(id uint64) {
	s.mu.Lock()
	if _, ok := s.tasks[id]; ok {
		delete(s.tasks, id)
		s.signalChangedLocked()
	}
	s.mu.Unlock()
}

func (s *scopedTaskScheduler) signalChangedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

type trackedTask struct {
	owner *scopedTaskScheduler
	id    uint64
	task  nodescheduler.Task
}

func (t *trackedTask) Execute(ctx context.Context) error {
	err := t.task.Execute(ctx)
	if !errors.Is(err, nodescheduler.ErrDelay) {
		t.owner.finish(t.id)
	}
	return err
}
