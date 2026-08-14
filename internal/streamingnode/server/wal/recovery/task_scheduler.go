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
	inner      nodescheduler.Scheduler
	maxRunning int

	mu      sync.Mutex
	nextID  uint64
	tasks   map[uint64]*scopedTaskEntry
	pending []*scopedTaskEntry
	running int
	changed chan struct{}
	closed  bool
}

func newScopedTaskScheduler(inner nodescheduler.Scheduler, maxRunning ...int) *scopedTaskScheduler {
	limit := 0
	if len(maxRunning) > 0 {
		limit = maxRunning[0]
	}
	return &scopedTaskScheduler{
		inner:      inner,
		maxRunning: limit,
		tasks:      make(map[uint64]*scopedTaskEntry),
		changed:    make(chan struct{}),
	}
}

func (s *scopedTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	entry := &scopedTaskEntry{
		owner: s,
		task:  task,
		done:  make(chan struct{}),
	}
	entry.ctx, entry.cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel is stored and called when the task finishes

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		entry.finish()
		return scopedTaskHandle{owner: s, entry: entry}
	}

	id := s.nextID
	s.nextID++
	entry.id = id
	s.tasks[id] = entry
	s.pending = append(s.pending, entry)
	s.signalChangedLocked()
	s.dispatchLocked()
	s.mu.Unlock()
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

func (s *scopedTaskScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = s.WaitIdle(context.Background())
		return
	}
	s.closed = true
	handles := make(map[uint64]nodescheduler.TaskHandle, len(s.tasks))
	for id, entry := range s.tasks {
		entry.cancel()
		if entry.running {
			handles[id] = entry.inner
			continue
		}
		s.finishEntryLocked(entry)
	}
	s.mu.Unlock()

	for _, handle := range handles {
		if handle != nil {
			handle.Cancel()
		}
	}
	for id, handle := range handles {
		if handle != nil {
			_ = handle.Wait(context.Background())
		}
		s.finish(id)
	}
}

func (s *scopedTaskScheduler) finish(id uint64) {
	s.mu.Lock()
	if entry, ok := s.tasks[id]; ok {
		s.finishEntryLocked(entry)
		s.dispatchLocked()
	}
	s.mu.Unlock()
}

func (s *scopedTaskScheduler) delay(id uint64) {
	s.mu.Lock()
	if entry, ok := s.tasks[id]; ok {
		if entry.running {
			s.running--
			entry.running = false
			entry.inner = nil
		}
		if s.closed || entry.ctx.Err() != nil {
			s.finishEntryLocked(entry)
		} else {
			s.pending = append(s.pending, entry)
		}
		s.dispatchLocked()
	}
	s.mu.Unlock()
}

func (s *scopedTaskScheduler) cancel(id uint64) nodescheduler.TaskHandle {
	s.mu.Lock()
	entry, ok := s.tasks[id]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	entry.cancel()
	if !entry.running {
		s.finishEntryLocked(entry)
		s.dispatchLocked()
		s.mu.Unlock()
		return nil
	}
	handle := entry.inner
	s.mu.Unlock()
	return handle
}

func (s *scopedTaskScheduler) finishEntryLocked(entry *scopedTaskEntry) {
	if _, ok := s.tasks[entry.id]; !ok {
		return
	}
	delete(s.tasks, entry.id)
	if entry.running {
		s.running--
		entry.running = false
	}
	entry.finish()
	s.signalChangedLocked()
}

func (s *scopedTaskScheduler) dispatchLocked() {
	for len(s.pending) > 0 && (s.maxRunning <= 0 || s.running < s.maxRunning) && !s.closed {
		entry := s.pending[0]
		copy(s.pending, s.pending[1:])
		s.pending[len(s.pending)-1] = nil
		s.pending = s.pending[:len(s.pending)-1]
		if _, ok := s.tasks[entry.id]; !ok {
			continue
		}
		if entry.ctx.Err() != nil {
			s.finishEntryLocked(entry)
			continue
		}
		entry.running = true
		s.running++
		entry.inner = s.inner.Submit(&trackedTask{
			owner: s,
			id:    entry.id,
			task:  entry.task,
		})
	}
}

func (s *scopedTaskScheduler) signalChangedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

type scopedTaskEntry struct {
	owner *scopedTaskScheduler
	id    uint64
	task  nodescheduler.Task

	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	doneOnce sync.Once

	running bool
	inner   nodescheduler.TaskHandle
}

func (e *scopedTaskEntry) finish() {
	e.doneOnce.Do(func() {
		e.cancel()
		close(e.done)
	})
}

type scopedTaskHandle struct {
	owner *scopedTaskScheduler
	entry *scopedTaskEntry
}

func (h scopedTaskHandle) Cancel() {
	handle := h.owner.cancel(h.entry.id)
	if handle == nil {
		return
	}
	handle.Cancel()
	go func() {
		_ = handle.Wait(context.Background())
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
	if errors.Is(err, nodescheduler.ErrDelay) {
		t.owner.delay(t.id)
		return nil
	}
	t.owner.finish(t.id)
	return err
}
