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

package rootcoord

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/retry"
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

func (s *stepStack) totalPriority() int {
	total := 0
	for _, step := range s.steps {
		total += int(step.Weight())
	}
	return total
}

func (s *stepStack) Execute(ctx context.Context) *stepStack {
	steps := s.steps
	for len(steps) > 0 {
		l := len(steps)
		todo := steps[l-1]
		log.Debug("step task begin", zap.String("step", todo.Desc()))
		childSteps, err := todo.Execute(ctx)

		// TODO: maybe a interface `step.LogOnError` is better.
		_, isWaitForTsSyncedStep := todo.(*waitForTsSyncedStep)
		_, isConfirmGCStep := todo.(*confirmGCStep)
		skipLog := isWaitForTsSyncedStep || isConfirmGCStep

		if !retry.IsRecoverable(err) {
			if !skipLog {
				log.Ctx(ctx).Warn("failed to execute step, not able to reschedule", zap.Error(err), zap.String("step", todo.Desc()))
			}
			return nil
		}
		if err != nil {
			s.steps = nil // let's can be collected.
			if !skipLog {
				log.Ctx(ctx).Warn("failed to execute step, wait for reschedule", zap.Error(err), zap.String("step", todo.Desc()))
			}
			return &stepStack{steps: steps}
		}
		log.Debug("step task done", zap.String("step", todo.Desc()))
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

func selectByPriority(parallel int, m map[*stepStack]struct{}) []*stepStack {
	h := make([]*stepStack, 0, len(m))
	for k := range m {
		h = append(h, k)
	}
	sort.Slice(h, func(i, j int) bool {
		return h[i].totalPriority() > h[j].totalPriority()
	})
	if len(h) <= parallel {
		return h
	}
	return h[:parallel]
}

func selectByPriorityPolicy(parallel int) selectStepPolicy {
	return func(m map[*stepStack]struct{}) []*stepStack {
		return selectByPriority(parallel, m)
	}
}

func defaultSelectPolicy() selectStepPolicy {
	return selectByPriorityPolicy(defaultBgExecutingParallel)
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
	notifyChan    chan struct{}
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
		notifyChan:    make(chan struct{}, 1),
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
	bg.addStepsInternal(s)
	bg.notify()
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
				// don't notify, wait for reschedule.
				bg.addStepsInternal(child)
			}
		}()
	}
	wg.Wait()
}

func (bg *bgStepExecutor) schedule() {
	bg.mu.Lock()
	selected := bg.selector(bg.bufferedSteps)
	for _, s := range selected {
		bg.unlockRemoveSteps(s)
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
		case <-bg.notifyChan:
			bg.schedule()
		case <-ticker.C:
			bg.schedule()
		}
	}
}

func (bg *bgStepExecutor) addStepsInternal(s *stepStack) {
	bg.mu.Lock()
	bg.unlockAddSteps(s)
	bg.mu.Unlock()
}

func (bg *bgStepExecutor) unlockAddSteps(s *stepStack) {
	bg.bufferedSteps[s] = struct{}{}
}

func (bg *bgStepExecutor) unlockRemoveSteps(s *stepStack) {
	delete(bg.bufferedSteps, s)
}

func (bg *bgStepExecutor) notify() {
	select {
	case bg.notifyChan <- struct{}{}:
	default:
	}
}
