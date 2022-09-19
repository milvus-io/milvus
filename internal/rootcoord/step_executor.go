package rootcoord

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.uber.org/zap"
)

const (
	defaultBgExecutingParallel = 4
	defaultBgExecutingInterval = time.Second
)

type StepExecutor interface {
	Start()
	Stop()
	AddSteps(s *stepStack)
}

type stepStack struct {
	steps []nestedStep
}

func (s *stepStack) Execute(ctx context.Context) *stepStack {
	steps := s.steps
	for len(steps) > 0 {
		l := len(steps)
		todo := steps[l-1]
		childSteps, err := todo.Execute(ctx)
		if retry.IsUnRecoverable(err) {
			log.Warn("failed to execute step, not able to reschedule", zap.Error(err), zap.String("step", todo.Desc()))
			return nil
		}
		if err != nil {
			s.steps = nil // let s can be collected.
			log.Warn("failed to execute step, wait for reschedule", zap.Error(err), zap.String("step", todo.Desc()))
			return &stepStack{steps: steps}
		}
		// this step is done.
		steps = steps[:l-1]
		steps = append(steps, childSteps...)
	}
	// everything is done.
	return nil
}

type selectStepPolicy func(map[*stepStack]struct{}) []*stepStack

func randomSelect(parallel int, m map[*stepStack]struct{}) []*stepStack {
	if parallel <= 0 {
		parallel = defaultBgExecutingParallel
	}
	res := make([]*stepStack, 0, parallel)
	for s := range m {
		if len(res) >= parallel {
			break
		}
		res = append(res, s)
	}
	return res
}

func randomSelectPolicy(parallel int) selectStepPolicy {
	return func(m map[*stepStack]struct{}) []*stepStack {
		return randomSelect(parallel, m)
	}
}

func defaultSelectPolicy() selectStepPolicy {
	return randomSelectPolicy(defaultBgExecutingParallel)
}

type bgOpt func(*bgStepExecutor)

func withSelectStepPolicy(policy selectStepPolicy) bgOpt {
	return func(bg *bgStepExecutor) {
		bg.selector = policy
	}
}

func withBgInterval(interval time.Duration) bgOpt {
	return func(bg *bgStepExecutor) {
		bg.interval = interval
	}
}

type bgStepExecutor struct {
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	bufferedSteps map[*stepStack]struct{}
	selector      selectStepPolicy
	mu            sync.Mutex
	notify        chan struct{}
	interval      time.Duration
}

func newBgStepExecutor(ctx context.Context, opts ...bgOpt) *bgStepExecutor {
	ctx1, cancel := context.WithCancel(ctx)
	bg := &bgStepExecutor{
		ctx:           ctx1,
		cancel:        cancel,
		wg:            sync.WaitGroup{},
		bufferedSteps: make(map[*stepStack]struct{}),
		selector:      defaultSelectPolicy(),
		mu:            sync.Mutex{},
		notify:        make(chan struct{}, 1),
		interval:      defaultBgExecutingInterval,
	}
	for _, opt := range opts {
		opt(bg)
	}
	return bg
}

func (bg *bgStepExecutor) Start() {
	bg.wg.Add(1)
	go bg.scheduleLoop()
}

func (bg *bgStepExecutor) Stop() {
	bg.cancel()
	bg.wg.Wait()
}

func (bg *bgStepExecutor) AddSteps(s *stepStack) {
	bg.mu.Lock()
	bg.addStepsInternal(s)
	bg.mu.Unlock()

	select {
	case bg.notify <- struct{}{}:
	default:
	}
}

func (bg *bgStepExecutor) process(steps []*stepStack) {
	wg := sync.WaitGroup{}
	for i := range steps {
		s := steps[i]
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			child := s.Execute(bg.ctx)
			if child != nil {
				bg.AddSteps(child)
			}
		}()
	}
	wg.Wait()
}

func (bg *bgStepExecutor) schedule() {
	bg.mu.Lock()
	selected := bg.selector(bg.bufferedSteps)
	for _, s := range selected {
		bg.removeStepsInternal(s)
	}
	bg.mu.Unlock()

	bg.process(selected)
}

func (bg *bgStepExecutor) scheduleLoop() {
	defer bg.wg.Done()

	ticker := time.NewTicker(bg.interval)
	defer ticker.Stop()

	for {
		select {
		case <-bg.ctx.Done():
			return
		case <-bg.notify:
			bg.schedule()
		case <-ticker.C:
			bg.schedule()
		}
	}
}

func (bg *bgStepExecutor) addStepsInternal(s *stepStack) {
	bg.bufferedSteps[s] = struct{}{}
}

func (bg *bgStepExecutor) removeStepsInternal(s *stepStack) {
	delete(bg.bufferedSteps, s)
}
