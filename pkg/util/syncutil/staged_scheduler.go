// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncutil

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 60 * time.Second
	retryMultiplier      = 2.0
)

// StagedTask is staged work whose Poll calls may move between CPU-bound and
// IO-bound worker pools.
//
// Contract with StagedScheduler:
//   - Poll runs one stage (e.g., serialize, then upload) and returns either
//     nil (terminal success), ErrContinue (yield for the next stage), a
//     retryable error (transient failure; back off and retry), or any other
//     error (terminal failure).
//   - Poll owns terminal notification. When Poll returns nil or a terminal
//     error, the task must have already published its result/error to its owner.
//     If ctx is canceled, Poll should publish ErrStagedSchedulerClosed and
//     return nil so the scheduler can drop the task.
//   - CPUBound reports whether the next Poll stage is CPU-bound. When Poll
//     returns ErrContinue, the scheduler reads CPUBound to pick the target pool.
//   - Key is used for log/metric attribution and retry accounting. It must be
//     unique among active tasks in the same StagedScheduler.
type StagedTask interface {
	CPUBound() bool
	Poll(ctx context.Context) error
	Key() string
}

// ErrContinue signals that a Poll stage completed and the task should be
// re-polled (the task itself will have advanced its internal stage and may have
// flipped CPUBound() to target the other pool). Returning ErrContinue is not
// a failure — it's just a yield point so the scheduler can hand the task off
// between the CPU and IO worker pools.
var ErrContinue = errors.New("staged task should continue")

// ErrStagedSchedulerClosed reports that a staged task was terminated because
// its scheduler was closed before the task reached a terminal state.
var ErrStagedSchedulerClosed = errors.New("staged scheduler closed")

// retryableError wraps a transient error so the scheduler knows to put the
// task on the retry heap with exponential backoff rather than fail it outright.
type retryableError struct {
	inner error
}

// NewRetryableError wraps err so that the scheduler treats the failure as
// transient and retries it with exponential backoff. Callers should use this
// for IO errors against external systems, network blips, etc.
func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{inner: err}
}

func (e *retryableError) Error() string {
	return "retryable: " + e.inner.Error()
}

func (e *retryableError) Unwrap() error { return e.inner }

// IsRetryable reports whether err was wrapped by NewRetryableError.
func IsRetryable(err error) bool {
	var r *retryableError
	return errors.As(err, &r)
}

// StagedScheduler decouples CPU-bound and IO-bound stages into independent worker
// pools. A task's Poll is called repeatedly; after each Poll it is
// re-dispatched to the pool matching its next stage, so CPU and IO slots are
// never blocked on each other.
//
// Failures:
//   - nil                 — terminal exit; task owns result/error notification
//   - ErrContinue         — yield; re-enqueue in CPU or IO pool per task.CPUBound()
//   - retryableError      — transient failure; push onto retry heap with exp backoff
//   - any other error     — terminal exit; task owns result/error notification
type StagedScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger

	cpuLimit *atomic.Int32
	ioLimit  *atomic.Int32

	mu         sync.Mutex
	cond       *sync.Cond
	cpuTasks   *list.List
	ioTasks    *list.List
	retries    typeutil.Heap[retryEntry]
	retryTries map[string]int
	retryWake  chan struct{}
	cpuRunning int32
	ioRunning  int32
	closed     bool
}

type retryEntry struct {
	task    StagedTask
	dueAt   time.Time
	attempt int
}

func newRetryHeap() typeutil.Heap[retryEntry] {
	return typeutil.NewObjectArrayBasedMinimumHeap[retryEntry, int64](nil, func(entry retryEntry) int64 {
		return entry.dueAt.UnixNano()
	})
}

// NewStagedScheduler creates a scheduler with the given pool sizes.
func NewStagedScheduler(cpuLimit, ioLimit int) *StagedScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &StagedScheduler{
		ctx:        ctx,
		cancel:     cancel,
		logger:     zap.NewNop(),
		cpuLimit:   atomic.NewInt32(int32(cpuLimit)),
		ioLimit:    atomic.NewInt32(int32(ioLimit)),
		cpuTasks:   list.New(),
		ioTasks:    list.New(),
		retries:    newRetryHeap(),
		retryTries: make(map[string]int),
		retryWake:  make(chan struct{}, 1),
	}
	s.cond = sync.NewCond(&s.mu)
	s.wg.Add(2)
	go s.scheduleLoop()
	go s.retryLoop()
	return s
}

// SetLogger attaches a logger to the scheduler. Nil resets it to a no-op
// logger, keeping syncutil independent from the Milvus log package.
func (s *StagedScheduler) SetLogger(logger *zap.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if logger == nil {
		logger = zap.NewNop()
	}
	s.logger = logger
}

// AddTask enqueues a task in the pool matching task.CPUBound().
func (s *StagedScheduler) AddTask(task StagedTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Notify through Poll's context contract. The task owns completion.
		go s.notifyClosed(task)
		return
	}
	s.pushLocked(task)
}

// pushLocked places a task onto the correct pool queue. Caller must hold s.mu.
func (s *StagedScheduler) pushLocked(task StagedTask) {
	if task.CPUBound() {
		s.cpuTasks.PushBack(task)
	} else {
		s.ioTasks.PushBack(task)
	}
	s.cond.Broadcast()
}

// UpdateLimits adjusts the pool sizes at runtime.
func (s *StagedScheduler) UpdateLimits(cpuLimit, ioLimit int) {
	s.cpuLimit.Store(int32(cpuLimit))
	s.ioLimit.Store(int32(ioLimit))
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
}

// scheduleLoop dispatches runnable tasks from cpuTasks/ioTasks to worker goroutines.
func (s *StagedScheduler) scheduleLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for !s.closed &&
			(s.cpuRunning >= s.cpuLimit.Load() || s.cpuTasks.Len() == 0) &&
			(s.ioRunning >= s.ioLimit.Load() || s.ioTasks.Len() == 0) {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		for s.cpuRunning < s.cpuLimit.Load() && s.cpuTasks.Len() > 0 {
			elem := s.cpuTasks.Front()
			s.cpuTasks.Remove(elem)
			s.cpuRunning++
			s.wg.Add(1)
			go s.executeTask(elem.Value.(StagedTask), true)
		}
		for s.ioRunning < s.ioLimit.Load() && s.ioTasks.Len() > 0 {
			elem := s.ioTasks.Front()
			s.ioTasks.Remove(elem)
			s.ioRunning++
			s.wg.Add(1)
			go s.executeTask(elem.Value.(StagedTask), false)
		}
		s.mu.Unlock()
	}
}

// executeTask runs a single Poll and then either completes the task, yields
// it back to a pool, or puts it on the retry heap. If Close raced with this
// Poll before a terminal result, the task is notified by polling once more with
// the scheduler's canceled context.
func (s *StagedScheduler) executeTask(task StagedTask, wasCPU bool) {
	defer s.wg.Done()
	err := task.Poll(s.ctx)

	s.mu.Lock()
	if wasCPU {
		s.cpuRunning--
	} else {
		s.ioRunning--
	}
	closed := s.closed

	if closed {
		s.cond.Broadcast()
		s.mu.Unlock()
		if errors.Is(err, ErrContinue) || IsRetryable(err) {
			s.notifyClosed(task)
		}
		return
	}
	switch {
	case err == nil:
		s.forgetRetryLocked(task)
		s.cond.Broadcast()
		s.mu.Unlock()
	case errors.Is(err, ErrContinue):
		s.pushLocked(task)
		s.mu.Unlock()
	case IsRetryable(err):
		s.scheduleRetryLocked(task, err)
		s.mu.Unlock()
	default:
		s.forgetRetryLocked(task)
		s.logger.Warn("staged task failed with unretryable error",
			zap.String("key", task.Key()), zap.Error(err))
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// scheduleRetryLocked registers a transient-failure task for a later wake-up.
// Caller must hold s.mu.
func (s *StagedScheduler) scheduleRetryLocked(task StagedTask, cause error) {
	key := task.Key()
	attempt := s.retryTries[key] + 1
	s.retryTries[key] = attempt
	backoff := computeBackoff(attempt)
	s.retries.Push(retryEntry{
		task:    task,
		dueAt:   time.Now().Add(backoff),
		attempt: attempt,
	})
	s.logger.Warn("staged task entered retry heap",
		zap.String("key", task.Key()),
		zap.Int("attempt", attempt),
		zap.Duration("backoff", backoff),
		zap.Error(cause))
	s.cond.Broadcast()
	select {
	case s.retryWake <- struct{}{}:
	default:
	}
}

func (s *StagedScheduler) forgetRetry(task StagedTask) {
	s.mu.Lock()
	s.forgetRetryLocked(task)
	s.mu.Unlock()
}

func (s *StagedScheduler) forgetRetryLocked(task StagedTask) {
	delete(s.retryTries, task.Key())
}

// retryLoop wakes periodically (or on broadcast) to promote due retry entries
// back onto the active pool queues.
func (s *StagedScheduler) retryLoop() {
	defer s.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		s.mu.Lock()
		for !s.closed && s.retries.Len() == 0 {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		now := time.Now()
		promoted := false
		for s.retries.Len() > 0 {
			next := s.retries.Peek()
			if next.dueAt.After(now) {
				break
			}
			s.pushLocked(s.retries.Pop().task)
			promoted = true
		}
		if promoted {
			s.mu.Unlock()
			continue
		}
		nextDue := s.retries.Peek().dueAt
		s.mu.Unlock()

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(time.Until(nextDue))

		select {
		case <-s.ctx.Done():
			// Drain retries and notify through Poll's context contract.
			s.mu.Lock()
			toFail := make([]StagedTask, 0, s.retries.Len())
			for s.retries.Len() > 0 {
				toFail = append(toFail, s.retries.Pop().task)
			}
			s.retries = newRetryHeap()
			s.mu.Unlock()
			for _, task := range toFail {
				s.notifyClosed(task)
			}
			return
		case <-timer.C:
		case <-s.retryWake:
		}
	}
}

// computeBackoff returns the delay before the Nth retry attempt (1-indexed),
// with exponential growth capped at retryMaxInterval.
func computeBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return retryInitialInterval
	}
	d := retryInitialInterval
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * retryMultiplier)
		if d >= retryMaxInterval {
			return retryMaxInterval
		}
	}
	return d
}

// Drain waits until all queued, retrying, and running work has finished. It
// does not close the scheduler; callers may continue to add tasks after Drain
// returns.
func (s *StagedScheduler) Drain(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		idle := !s.closed &&
			s.cpuTasks.Len() == 0 &&
			s.ioTasks.Len() == 0 &&
			s.retries.Len() == 0 &&
			s.cpuRunning == 0 &&
			s.ioRunning == 0
		closed := s.closed
		s.mu.Unlock()
		if idle {
			return nil
		}
		if closed {
			return ErrStagedSchedulerClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Close stops the scheduler; in-flight tasks finish their current Poll and
// observe the scheduler context cancellation. Queued and retrying tasks are
// polled once with the canceled context so they can publish their own shutdown
// result and return nil.
func (s *StagedScheduler) Close() {
	s.cancel()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	// Drain queues and notify everything through Poll's context contract.
	var pending []StagedTask
	for e := s.cpuTasks.Front(); e != nil; e = e.Next() {
		pending = append(pending, e.Value.(StagedTask))
	}
	s.cpuTasks.Init()
	for e := s.ioTasks.Front(); e != nil; e = e.Next() {
		pending = append(pending, e.Value.(StagedTask))
	}
	s.ioTasks.Init()
	for s.retries.Len() > 0 {
		pending = append(pending, s.retries.Pop().task)
	}
	s.retries = newRetryHeap()
	s.retryTries = make(map[string]int)
	s.cond.Broadcast()
	s.mu.Unlock()

	for _, t := range pending {
		s.notifyClosed(t)
	}
	s.wg.Wait()
}

func (s *StagedScheduler) notifyClosed(task StagedTask) {
	if err := task.Poll(s.ctx); err != nil {
		s.logger.Warn("staged task returned error while handling scheduler close",
			zap.String("key", task.Key()), zap.Error(err))
	}
}
