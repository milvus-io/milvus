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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// scopedTaskScheduler tracks one WAL's tasks without owning execution resources.
// Concurrency and delayed retries are managed by the shared NodeScheduler.
type scopedTaskScheduler struct {
	inner nodescheduler.Scheduler

	mu      sync.Mutex
	nextID  uint64
	tasks   map[uint64]*scopedTaskEntry
	changed chan struct{}
	closed  bool
}

func newScopedTaskScheduler(inner nodescheduler.Scheduler) *scopedTaskScheduler {
	return &scopedTaskScheduler{
		inner:   inner,
		tasks:   make(map[uint64]*scopedTaskEntry),
		changed: make(chan struct{}),
	}
}

func (s *scopedTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	entry := &scopedTaskEntry{done: make(chan struct{})}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		entry.finish()
		return scopedTaskHandle{owner: s, entry: entry}
	}

	entry.id = s.nextID
	s.nextID++
	s.tasks[entry.id] = entry
	// TODO: Add fairness between PChannels in NodeScheduler so a busy WAL
	// cannot monopolize the shared queue and workers.
	entry.inner = s.inner.Submit(&trackedTask{owner: s, id: entry.id, task: task})
	return scopedTaskHandle{owner: s, entry: entry}
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

// closeWaitTimeout bounds how long Close waits for already-canceled tasks to
// drain. Tasks are canceled before waiting, so this only fires when a task
// ignores cancellation (e.g. blocked in a non-context-aware call) and would
// otherwise hang Close forever.
const closeWaitTimeout = 30 * time.Second

func (s *scopedTaskScheduler) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), closeWaitTimeout)
	defer cancel()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		if err := s.WaitIdle(ctx); err != nil {
			mlog.Warn(ctx, "scoped task scheduler close: wait idle timeout", mlog.Err(err))
		}
		return
	}
	s.closed = true
	handles := make(map[uint64]nodescheduler.TaskHandle, len(s.tasks))
	for id, entry := range s.tasks {
		handles[id] = entry.inner
	}
	s.mu.Unlock()

	for _, handle := range handles {
		handle.Cancel()
	}
	for id, handle := range handles {
		if err := handle.Wait(ctx); err != nil {
			mlog.Warn(ctx, "scoped task scheduler close: wait task timeout", mlog.Uint64("taskID", id), mlog.Err(err))
		}
		s.finish(id)
	}
}

func (s *scopedTaskScheduler) finish(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.tasks[id]; ok {
		delete(s.tasks, id)
		entry.finish()
		close(s.changed)
		s.changed = make(chan struct{})
	}
}

type scopedTaskEntry struct {
	id    uint64
	inner nodescheduler.TaskHandle
	done  chan struct{}
}

func (e *scopedTaskEntry) finish() {
	close(e.done)
}

type scopedTaskHandle struct {
	owner *scopedTaskScheduler
	entry *scopedTaskEntry
}

func (h scopedTaskHandle) Cancel() {
	// A submission rejected after Close never enters the shared scheduler.
	if h.entry.inner == nil {
		return
	}
	h.entry.inner.Cancel()
	go func() {
		// A canceled task may be skipped without Execute being called.
		_ = h.entry.inner.Wait(context.Background())
		h.owner.finish(h.entry.id)
	}()
}

func (h scopedTaskHandle) Wait(ctx context.Context) error {
	select {
	case <-h.entry.done:
		return nil
	default:
	}

	select {
	case <-h.entry.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
