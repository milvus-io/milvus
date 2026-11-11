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

package nodescheduler

import (
	"container/heap"
	"container/list"
	"context"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Scheduler interface {
	// Submit enqueues the task into an unbounded queue and returns without
	// waiting for queue capacity or task execution. Tasks may call Submit from
	// Execute, so implementations must preserve this non-blocking contract to
	// avoid exhausting all workers on nested submissions.
	// Each submission owns its retry backoff, configurable with WithBackoff.
	Submit(Task, ...Option) TaskHandle
}

type Task interface {
	Execute(context.Context) error
}

type TaskHandle interface {
	Cancel()
	Wait(context.Context) error
}

// PriorityTaskHandle is the optional handle extension supported by the
// schedulers returned by New and Get.
// Scheduler implementations that wrap their handles should forward this
// capability when available; TaskHandle and Scheduler remain compatible.
type PriorityTaskHandle interface {
	TaskHandle
	// SetPriority replaces the absolute priority, preserving the current ready
	// queue age. It never interrupts an execution or advances a retry deadline.
	// A running task uses the new value only when it requeues. It returns false
	// after completion; a true result does not imply that the task is runnable.
	SetPriority(int32) bool
}

type scheduleErrorKind int

const scheduleErrorKindDelay scheduleErrorKind = iota + 1

type ScheduleError struct {
	kind    scheduleErrorKind
	nextRun time.Time
}

func (e *ScheduleError) Error() string {
	if e != nil && e.kind == scheduleErrorKindDelay {
		return "delay node scheduler task"
	}
	return "unknown node scheduler error"
}

func (e *ScheduleError) Is(target error) bool {
	other, ok := target.(*ScheduleError)
	return ok && e != nil && other != nil && e.kind == other.kind
}

var ErrDelay = &ScheduleError{kind: scheduleErrorKindDelay}

// DelayUntil asks the scheduler to retry no earlier than nextRun. Wrapping this
// error preserves both its deadline and errors.Is(err, ErrDelay). The bare
// sentinel uses the submitted task's exponential retry backoff.
func DelayUntil(nextRun time.Time) *ScheduleError {
	return &ScheduleError{kind: scheduleErrorKindDelay, nextRun: nextRun}
}

type nodeScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu                 sync.Mutex
	cond               *sync.Cond
	queue              *list.List
	ready              readyTasks
	priorityDispatches int
	delayed            delayedTasks
	timer              *time.Timer
	sequence           uint64
	closed             bool

	concurrency int
	workerCount int
	workers     sync.WaitGroup
}

type taskEntry struct {
	priority   int32
	readyIndex int
	readyOrder uint64
	task       Task
	ctx        context.Context
	cancel     context.CancelFunc
	handle     *taskHandle
	backoff    *backoff.ExponentialBackOff // Accessed only by the worker executing this entry.
	// nextRun is the earliest time a delayed (ErrDelay) requeue may execute
	// again. Zero means the entry may run immediately.
	nextRun   time.Time
	heapIndex int
	order     uint64
	queued    *list.Element
}

type taskHandle struct {
	done        chan struct{}
	mu          sync.Mutex
	cancelTask  func()
	setPriority func(int32) bool
}

func (h *taskHandle) Cancel() {
	h.mu.Lock()
	cancelTask := h.cancelTask
	h.mu.Unlock()
	// Do not hold the handle lock while taking the scheduler lock: completion
	// clears this callback while holding the scheduler lock.
	if cancelTask != nil {
		cancelTask()
	}
}

func (h *taskHandle) Wait(ctx context.Context) error {
	select {
	case <-h.done:
		return nil
	default:
	}

	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func New(concurrency int) *nodeScheduler {
	if concurrency <= 0 {
		panic("node scheduler concurrency must be greater than zero")
	}

	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &nodeScheduler{
		ctx:    ctx,
		cancel: cancel,
		queue:  list.New(),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	scheduler.resize(concurrency)
	return scheduler
}

func (s *nodeScheduler) resize(concurrency int) {
	if concurrency <= 0 {
		panic("node scheduler concurrency must be greater than zero")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.concurrency == concurrency {
		return
	}

	s.concurrency = concurrency
	if concurrency > s.workerCount {
		additional := concurrency - s.workerCount
		s.workerCount += additional
		s.workers.Add(additional)
		for i := 0; i < additional; i++ {
			go s.runWorker()
		}
		return
	}
	// Shrinking: wake the excess workers so they observe workerCount > concurrency
	// and exit from dequeue. Broadcasting is required since an arbitrary number of
	// workers may need to wake and terminate.
	s.cond.Broadcast()
}

func (s *nodeScheduler) Submit(task Task, opts ...Option) TaskHandle {
	options := newTaskOptions(opts...)
	backoff := newTaskBackoff(options)
	ctx, cancel := context.WithCancel(s.ctx) // #nosec G118 -- task completion invokes the retained cancel function.
	handle := &taskHandle{done: make(chan struct{})}
	entry := &taskEntry{
		task:       task,
		priority:   options.priority,
		readyIndex: -1,
		ctx:        ctx,
		cancel:     cancel,
		handle:     handle,
		heapIndex:  -1,
		backoff:    backoff,
	}
	handle.cancelTask = func() { s.cancelTask(entry) }
	handle.setPriority = func(priority int32) bool { return s.setPriority(entry, priority) }

	s.mu.Lock()
	if s.closed {
		s.finishLocked(entry)
		s.mu.Unlock()
		return handle
	}
	// The queue is intentionally unbounded: Submit must never wait for capacity.
	s.enqueueLocked(entry)
	s.cond.Signal()
	s.mu.Unlock()
	return handle
}

func (s *nodeScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.workers.Wait()
		return
	}

	s.closed = true
	for element := s.queue.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*taskEntry)
		entry.queued = nil
		entry.readyIndex = -1
		s.finishLocked(entry)
	}
	s.queue.Init()
	clear(s.ready)
	s.ready = nil
	for len(s.delayed) > 0 {
		s.finishLocked(heap.Pop(&s.delayed).(*taskEntry))
	}
	s.delayed = nil
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.cancel()
	s.cond.Broadcast()
	s.mu.Unlock()

	s.workers.Wait()
}

// cancelTask removes queued/delayed work immediately. A running task owns its
// completion until Execute returns, even after cancellation.
func (s *nodeScheduler) cancelTask(entry *taskEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Cancel may have copied its callback immediately before completion detached
	// the handle. The entry is already finished in that case.
	if entry.handle == nil {
		return
	}
	entry.cancel()
	if entry.queued != nil {
		s.removeReadyLocked(entry)
		s.finishLocked(entry)
	} else if entry.heapIndex >= 0 {
		heap.Remove(&s.delayed, entry.heapIndex)
		s.resetTimerLocked()
		s.finishLocked(entry)
	}
}

func (s *nodeScheduler) finish(entry *taskEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishLocked(entry)
}

// finishLocked is called only after removal from the queue/heap, or by the
// worker after Execute returns. Completion releases all execution references
// before waking waiters, even when callers retain the handle indefinitely.
func (s *nodeScheduler) finishLocked(entry *taskEntry) {
	handle := entry.handle
	entry.cancel()
	entry.task = nil
	entry.ctx = nil
	entry.cancel = nil
	entry.backoff = nil
	entry.handle = nil
	entry.nextRun = time.Time{}
	handle.mu.Lock()
	handle.cancelTask = nil
	handle.setPriority = nil
	close(handle.done)
	handle.mu.Unlock()
}

func (s *nodeScheduler) runWorker() {
	defer s.workers.Done()
	for {
		entry := s.dequeue()
		if entry == nil {
			return
		}
		if entry.ctx.Err() != nil {
			s.finish(entry)
			continue
		}

		err := entry.task.Execute(entry.ctx)
		if entry.ctx.Err() != nil {
			// Context canceled (e.g. shutdown): finish the entry without
			// requeueing. Note the task itself may not have done its queue
			// bookkeeping — with a canceled ctx a retryable segment task
			// stays in pendingTasks[0] and the segment stops submitting. That
			// is confined to the shutdown path (Submit handles are dropped,
			// Cancel is never called) and must be drained by the owner before
			// Close completes; see ViewConfig.Runtime.
			s.finish(entry)
			continue
		}
		if errors.Is(err, ErrDelay) {
			var nextRun time.Time
			var delay *ScheduleError
			if errors.As(err, &delay) && !delay.nextRun.IsZero() {
				nextRun = delay.nextRun
			} else {
				nextRun = time.Now().Add(entry.backoff.NextBackOff())
			}
			if s.requeue(entry, nextRun) {
				continue
			}
			s.finish(entry)
			continue
		}
		if err != nil {
			mlog.Error(entry.ctx, "node scheduler task failed",
				mlog.String("taskType", reflect.TypeOf(entry.task).String()),
				mlog.Err(err))
		}
		s.finish(entry)
	}
}

func (s *nodeScheduler) dequeue() *taskEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.closed || s.workerCount > s.concurrency {
			s.workerCount--
			return nil
		}
		if element := s.queue.Front(); element != nil {
			entry := s.ready[0]
			if s.priorityDispatches >= priorityBurst {
				entry = element.Value.(*taskEntry)
				s.priorityDispatches = 0
			} else {
				s.priorityDispatches++
			}
			s.removeReadyLocked(entry)
			return entry
		}
		s.cond.Wait()
	}
}

func (s *nodeScheduler) requeue(entry *taskEntry, nextRun time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || entry.ctx.Err() != nil {
		return false
	}
	if !nextRun.After(time.Now()) {
		s.enqueueLocked(entry)
		s.cond.Signal()
		return true
	}
	entry.nextRun = nextRun
	s.sequence++
	entry.order = s.sequence
	heap.Push(&s.delayed, entry)
	if entry.heapIndex == 0 {
		s.resetTimerLocked()
	}
	return true
}

// One timer services the heap minimum. Stale timer callbacks are harmless: they
// recheck deadlines under mu and never execute a task before its requested time.
func (s *nodeScheduler) resetTimerLocked() {
	if s.closed || len(s.delayed) == 0 {
		s.delayed = nil
		if s.timer != nil {
			s.timer.Stop()
		}
		return
	}
	delay := max(0, time.Until(s.delayed[0].nextRun))
	if s.timer == nil {
		s.timer = time.AfterFunc(delay, s.promoteDelayed)
	} else {
		s.timer.Reset(delay)
	}
}

func (s *nodeScheduler) promoteDelayed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	now := time.Now()
	promoted := false
	for len(s.delayed) > 0 && !s.delayed[0].nextRun.After(now) {
		entry := heap.Pop(&s.delayed).(*taskEntry)
		s.enqueueLocked(entry)
		promoted = true
	}
	s.resetTimerLocked()
	if promoted {
		s.cond.Broadcast()
	}
}

var getGlobalScheduler = sync.OnceValue(func() *nodeScheduler {
	params := paramtable.Get()
	ratioParam := &params.CommonCfg.NodeSchedulerMaxConcurrencyRatio
	cpu := hardware.GetCPUNum()
	concurrency, ok := concurrencyFromRatio(cpu, ratioParam.GetAsFloat())
	if !ok {
		concurrency = cpu
		mlog.Warn(context.TODO(), "invalid node scheduler concurrency ratio, use default concurrency",
			mlog.String("value", ratioParam.GetValue()),
			mlog.Int("concurrency", concurrency))
	}

	scheduler := New(concurrency)
	params.Watch(ratioParam.Key, config.NewHandler("node-scheduler-concurrency", func(event *config.Event) {
		if !event.HasUpdated {
			return
		}
		ratio := ratioParam.GetAsFloat()
		concurrency, ok := concurrencyFromRatio(hardware.GetCPUNum(), ratio)
		if !ok {
			mlog.Warn(context.TODO(), "ignore invalid node scheduler concurrency ratio",
				mlog.String("value", ratioParam.GetValue()))
			return
		}
		scheduler.resize(concurrency)
		mlog.Info(context.TODO(), "node scheduler concurrency resized",
			mlog.Float64("ratio", ratio),
			mlog.Int("concurrency", concurrency))
	}))
	return scheduler
})

func concurrencyFromRatio(cpu int, ratio float64) (int, bool) {
	if cpu <= 0 || ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		return 0, false
	}
	return max(1, int(float64(cpu)*ratio)), true
}

func Get() Scheduler {
	return getGlobalScheduler()
}
