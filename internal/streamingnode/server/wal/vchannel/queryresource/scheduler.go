package queryresource

import (
	"context"
	"errors"
	"sync"
)

type Scheduler interface {
	Submit(task BuildTask)
	Close()
}

type BuildTask interface {
	Run()
	Done() <-chan struct{}
	Result() (*QueryRuntime, error)
	Cancel()
}

type defaultScheduler struct {
	sem    chan struct{}
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func NewScheduler(concurrency int) Scheduler {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &defaultScheduler{
		sem:    make(chan struct{}, concurrency),
		closed: make(chan struct{}),
	}
}

func (s *defaultScheduler) Submit(task BuildTask) {
	select {
	case <-s.closed:
		task.Cancel()
		return
	default:
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-s.closed:
			task.Cancel()
			return
		}
		task.Run()
	}()
}

func (s *defaultScheduler) Close() {
	s.once.Do(func() {
		close(s.closed)
		s.wg.Wait()
	})
}

type resourceBuildTask struct {
	ctx    context.Context
	cancel context.CancelFunc
	build  func(context.Context) (*QueryRuntime, error)

	done chan struct{}

	mu       sync.Mutex
	started  bool
	finished bool
	runtime  *QueryRuntime
	err      error
}

func newResourceBuildTask(parent context.Context, build func(context.Context) (*QueryRuntime, error)) *resourceBuildTask {
	ctx, cancel := context.WithCancel(parent)
	return &resourceBuildTask{
		ctx:    ctx,
		cancel: cancel,
		build:  build,
		done:   make(chan struct{}),
	}
}

func (t *resourceBuildTask) Run() {
	t.mu.Lock()
	if t.started || t.finished {
		t.mu.Unlock()
		return
	}
	t.started = true
	t.mu.Unlock()

	runtime, err := t.build(t.ctx)
	t.finish(runtime, err)
}

func (t *resourceBuildTask) Done() <-chan struct{} {
	return t.done
}

func (t *resourceBuildTask) Result() (*QueryRuntime, error) {
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runtime, t.err
}

func (t *resourceBuildTask) Cancel() {
	t.cancel()
	t.mu.Lock()
	if !t.started && !t.finished {
		t.err = t.ctx.Err()
		if t.err == nil {
			t.err = context.Canceled
		}
		t.finished = true
		close(t.done)
	}
	t.mu.Unlock()
}

func (t *resourceBuildTask) finish(runtime *QueryRuntime, err error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		if runtime != nil {
			runtime.Close()
		}
		return
	}
	if err == nil && t.ctx.Err() != nil {
		err = t.ctx.Err()
		if err == nil {
			err = context.Canceled
		}
	}
	if err != nil && errors.Is(err, context.Canceled) && t.ctx.Err() != nil {
		err = t.ctx.Err()
	}
	t.runtime = runtime
	t.err = err
	t.finished = true
	close(t.done)
	t.mu.Unlock()
}
