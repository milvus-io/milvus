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
	"context"
	"math"
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
	// The built-in handle panics for values outside [MinPriority, MaxPriority].
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
	ctx     context.Context
	cancel  context.CancelFunc
	stats   *schedulerStats
	metrics *schedulerMetrics

	mu                 sync.Mutex
	cond               *sync.Cond
	oldest             [priorityBands]ageTasks
	running            [priorityBands]int // Cumulative running attempts at or below each band.
	ready              readyTasks
	priorityDispatches int
	delayed            delayedTasks
	timer              *time.Timer
	sequence           uint64
	closed             bool

	concurrency int
	workerCount int
	workers     sync.WaitGroup
	reporter    sync.WaitGroup
}

type taskEntry struct {
	priority    int32
	readyIndex  int
	ageIndex    int
	runningBand int // Admission band for the current attempt; -1 while not running.
	readyOrder  uint64
	task        Task
	ctx         context.Context
	cancel      context.CancelFunc
	handle      *taskHandle
	backoff     *backoff.ExponentialBackOff // Accessed only by the worker executing this entry.
	// nextRun is the earliest time a delayed (ErrDelay) requeue may execute
	// again. Zero means the entry may run immediately.
	nextRun   time.Time
	heapIndex int
	order     uint64
	taskType  string
	queuedAt  time.Time
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
		ctx:     ctx,
		cancel:  cancel,
		stats:   newSchedulerStats(concurrency),
		metrics: newSchedulerMetrics(),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	scheduler.resize(concurrency)
	scheduler.reporter.Add(1)
	go scheduler.reportStats()
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

	s.metrics.adjustConcurrency(concurrency - s.concurrency)
	s.concurrency = concurrency
	s.stats.setCapacity(concurrency)
	if concurrency > s.workerCount {
		additional := concurrency - s.workerCount
		s.workerCount += additional
		s.workers.Add(additional)
		for i := 0; i < additional; i++ {
			go s.runWorker()
		}
	}
	// Wake all workers: shrinking retires excess workers, while growing may
	// unblock existing workers waiting for a priority band's capacity.
	s.cond.Broadcast()
}

func (s *nodeScheduler) Submit(task Task, opts ...Option) TaskHandle {
	options := newTaskOptions(opts...)
	backoff := newTaskBackoff(options)
	ctx, cancel := context.WithCancel(s.ctx) // #nosec G118 -- task completion invokes the retained cancel function.
	handle := &taskHandle{done: make(chan struct{})}
	entry := &taskEntry{
		task:        task,
		taskType:    TaskTypeName(task),
		queuedAt:    time.Now(),
		priority:    options.priority,
		readyIndex:  -1,
		ageIndex:    -1,
		runningBand: -1,
		ctx:         ctx,
		cancel:      cancel,
		handle:      handle,
		heapIndex:   -1,
		backoff:     backoff,
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
	s.stats.submit(entry.taskType)
	s.metrics.observeEnqueued(entry.taskType)
	s.cond.Signal()
	s.mu.Unlock()
	return handle
}

func (s *nodeScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.workers.Wait()
		s.reporter.Wait()
		return
	}

	s.closed = true
	s.metrics.adjustConcurrency(-s.concurrency)
	for _, entry := range s.ready {
		entry.readyIndex = -1
		entry.ageIndex = -1
		s.stats.cancelQueued(entry.taskType)
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.queuedAt))
		s.finishLocked(entry)
	}
	s.ready = nil
	s.oldest = [priorityBands]ageTasks{}
	for len(s.delayed) > 0 {
		entry := heap.Pop(&s.delayed).(*taskEntry)
		s.stats.cancelQueued(entry.taskType)
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.queuedAt))
		s.finishLocked(entry)
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
	s.reporter.Wait()
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
	if entry.readyIndex >= 0 {
		s.removeReadyLocked(entry)
		s.stats.cancelQueued(entry.taskType)
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.queuedAt))
		s.finishLocked(entry)
	} else if entry.heapIndex >= 0 {
		heap.Remove(&s.delayed, entry.heapIndex)
		s.resetTimerLocked()
		s.stats.cancelQueued(entry.taskType)
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.queuedAt))
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
	s.releaseWorkerLocked(entry)
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
		s.metrics.observeDequeued(entry.taskType, time.Since(entry.queuedAt))
		if entry.ctx.Err() != nil {
			s.stats.cancelStarted(entry.taskType)
			s.finish(entry)
			continue
		}

		startedAt := time.Now()
		s.metrics.observeExecutionStarted(entry.taskType)
		err := entry.task.Execute(entry.ctx)
		executeDuration := time.Since(startedAt)
		s.metrics.observeExecutionFinished(
			entry.taskType,
			taskExecutionStatus(entry.ctx.Err(), err),
			executeDuration,
		)
		if entry.ctx.Err() != nil {
			// Context canceled (e.g. shutdown): finish the entry without
			// requeueing. Note the task itself may not have done its queue
			// bookkeeping — with a canceled ctx a retryable segment task
			// stays in pendingTasks[0] and the segment stops submitting. That
			// is confined to the shutdown path (Submit handles are dropped,
			// Cancel is never called) and must be drained by the owner before
			// Close completes; see ViewConfig.Runtime.
			s.stats.finishCanceled(entry.taskType, executeDuration)
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
			if s.requeue(entry, nextRun, executeDuration) {
				continue
			}
			s.stats.finishDelayed(entry.taskType, executeDuration, false)
			s.finish(entry)
			continue
		}
		if err != nil {
			s.stats.finishFailed(entry.taskType, executeDuration)
			mlog.Error(entry.ctx, "node scheduler task failed",
				mlog.String("taskType", entry.taskType),
				mlog.Err(err))
		} else {
			s.stats.finishCompleted(entry.taskType, executeDuration)
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
		if entry := s.nextReadyLocked(); entry != nil {
			s.removeReadyLocked(entry)
			s.stats.start(entry.taskType, time.Since(entry.queuedAt))
			entry.runningBand = priorityBand(entry.priority)
			for band := entry.runningBand; band < priorityBands; band++ {
				s.running[band]++
			}
			return entry
		}
		s.cond.Wait()
	}
}

func (s *nodeScheduler) requeue(entry *taskEntry, nextRun time.Time, executeDuration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseWorkerLocked(entry)
	if s.closed || entry.ctx.Err() != nil {
		return false
	}
	entry.queuedAt = time.Now()
	s.stats.finishDelayed(entry.taskType, executeDuration, true)
	s.metrics.observeEnqueued(entry.taskType)
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
