package preconditioned

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type Precondition interface {
	Ready() bool
}

type PreconditionFunc func() bool

func (f PreconditionFunc) Ready() bool {
	return f()
}

type TaskHandle interface {
	Done() bool
}

type AlwaysReady struct{}

func (AlwaysReady) Ready() bool {
	return true
}

func After(handle TaskHandle) Precondition {
	if handle == nil {
		return AlwaysReady{}
	}
	return afterPrecondition{handle: handle}
}

type afterPrecondition struct {
	handle TaskHandle
}

func (p afterPrecondition) Ready() bool {
	return p.handle.Done()
}

func All(preconditions ...Precondition) Precondition {
	nonNil := make([]Precondition, 0, len(preconditions))
	for _, precondition := range preconditions {
		if precondition != nil {
			nonNil = append(nonNil, precondition)
		}
	}
	if len(nonNil) == 0 {
		return AlwaysReady{}
	}
	return allPrecondition(nonNil)
}

type allPrecondition []Precondition

func (p allPrecondition) Ready() bool {
	for _, precondition := range p {
		if !precondition.Ready() {
			return false
		}
	}
	return true
}

type Task interface {
	Name() string
	Precondition() Precondition
	Run(ctx context.Context) error
}

type taskHandle struct {
	done atomic.Bool
}

func newDoneTaskHandle() *taskHandle {
	handle := &taskHandle{}
	handle.done.Store(true)
	return handle
}

func (h *taskHandle) Done() bool {
	return h.done.Load()
}

func (h *taskHandle) markDone() {
	h.done.Store(true)
}

type taskEntry struct {
	task   Task
	handle *taskHandle
}

type Option func(*Scheduler)

func WithRetryInterval(interval time.Duration) Option {
	return func(s *Scheduler) {
		s.retryInterval = interval
	}
}

type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	retryInterval time.Duration
	notifyCh      chan struct{}

	mu      sync.Mutex
	pending []taskEntry
	running int

	loopWG sync.WaitGroup
	taskWG sync.WaitGroup
}

func New(ctx context.Context, opts ...Option) *Scheduler {
	schedulerCtx, cancel := context.WithCancel(ctx)
	s := &Scheduler{
		ctx:           schedulerCtx,
		cancel:        cancel,
		retryInterval: 100 * time.Millisecond,
		notifyCh:      make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.loopWG.Add(1)
	go s.run()
	return s
}

func (s *Scheduler) Submit(task Task) TaskHandle {
	if task == nil {
		return newDoneTaskHandle()
	}
	if s.ctx.Err() != nil {
		return newDoneTaskHandle()
	}
	handle := &taskHandle{}
	s.mu.Lock()
	s.pending = append(s.pending, taskEntry{
		task:   task,
		handle: handle,
	})
	s.mu.Unlock()
	s.Notify()
	return handle
}

func (s *Scheduler) Notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *Scheduler) Close() {
	s.cancel()
	s.Notify()
	s.loopWG.Wait()
	s.taskWG.Wait()
}

func (s *Scheduler) WaitIdle(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.idle() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending) == 0 && s.running == 0
}

func (s *Scheduler) run() {
	defer s.loopWG.Done()
	for {
		if s.ctx.Err() != nil {
			return
		}
		s.scheduleReadyTasks()
		select {
		case <-s.ctx.Done():
			return
		case <-s.notifyCh:
		}
	}
}

func (s *Scheduler) scheduleReadyTasks() {
	if s.ctx.Err() != nil {
		return
	}
	readyTasks := make([]taskEntry, 0)

	s.mu.Lock()
	remaining := s.pending[:0]
	for _, entry := range s.pending {
		if !preconditionReady(entry.task.Precondition()) {
			remaining = append(remaining, entry)
			continue
		}
		s.running++
		readyTasks = append(readyTasks, entry)
	}
	s.pending = remaining
	s.mu.Unlock()

	for _, entry := range readyTasks {
		s.taskWG.Add(1)
		go s.runTask(entry)
	}
}

func preconditionReady(precondition Precondition) bool {
	if precondition == nil {
		return true
	}
	return precondition.Ready()
}

func (s *Scheduler) runTask(entry taskEntry) {
	defer func() {
		s.mu.Lock()
		s.running--
		s.mu.Unlock()
		s.Notify()
		s.taskWG.Done()
	}()

	for {
		if s.ctx.Err() != nil {
			return
		}
		if err := entry.task.Run(s.ctx); err == nil {
			entry.handle.markDone()
			return
		}
		timer := time.NewTimer(s.retryInterval)
		select {
		case <-s.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}
