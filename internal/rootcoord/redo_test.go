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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

type mockFailStep struct {
	baseStep
	calledChan chan struct{}
	called     bool
	err        error
}

func newMockFailStep() *mockFailStep {
	return &mockFailStep{calledChan: make(chan struct{}, 1), called: false}
}

func (m *mockFailStep) Execute(ctx context.Context) ([]nestedStep, error) {
	m.called = true
	m.calledChan <- struct{}{}
	if m.err != nil {
		return nil, m.err
	}
	return nil, errors.New("error mock Execute")
}

type mockNormalStep struct {
	nestedStep
	calledChan chan struct{}
	called     bool
}

func newMockNormalStep() *mockNormalStep {
	return &mockNormalStep{calledChan: make(chan struct{}, 1), called: false}
}

func (m *mockNormalStep) Execute(ctx context.Context) ([]nestedStep, error) {
	m.called = true
	m.calledChan <- struct{}{}
	return nil, nil
}

func newTestRedoTask() *baseRedoTask {
	stepExecutor := newMockStepExecutor()
	stepExecutor.AddStepsFunc = func(s *stepStack) {
		// no schedule, execute directly.
		s.Execute(context.Background())
	}
	redo := newBaseRedoTask(stepExecutor)
	return redo
}

func Test_baseRedoTask_redoAsyncSteps(t *testing.T) {
	t.Run("partial error", func(t *testing.T) {
		redo := newTestRedoTask()
		steps := []nestedStep{newMockNormalStep(), newMockFailStep(), newMockNormalStep()}
		for _, step := range steps {
			redo.AddAsyncStep(step)
		}
		redo.redoAsyncSteps(context.TODO())
		assert.True(t, steps[0].(*mockNormalStep).called)
		assert.False(t, steps[2].(*mockNormalStep).called)
	})

	t.Run("normal case", func(t *testing.T) {
		redo := newTestRedoTask()
		n := 10
		steps := make([]nestedStep, 0, n)
		for i := 0; i < n; i++ {
			steps = append(steps, newMockNormalStep())
		}
		for _, step := range steps {
			redo.AddAsyncStep(step)
		}
		redo.redoAsyncSteps(context.TODO())
		for _, step := range steps {
			assert.True(t, step.(*mockNormalStep).called)
		}
	})
}

func Test_baseRedoTask_Execute(t *testing.T) {
	t.Run("sync not finished, no async task", func(t *testing.T) {
		redo := newTestRedoTask()
		syncSteps := []nestedStep{newMockFailStep()}
		asyncNum := 10
		asyncSteps := make([]nestedStep, 0, asyncNum)
		for i := 0; i < asyncNum; i++ {
			asyncSteps = append(asyncSteps, newMockNormalStep())
		}
		for _, step := range asyncSteps {
			redo.AddAsyncStep(step)
		}
		for _, step := range syncSteps {
			redo.AddSyncStep(step)
		}
		err := redo.Execute(context.Background())
		assert.Error(t, err)
		for _, step := range asyncSteps {
			assert.False(t, step.(*mockNormalStep).called)
		}
	})

	// TODO: sync finished, but some async fail.

	t.Run("normal case", func(t *testing.T) {
		redo := newTestRedoTask()
		syncNum := 10
		syncSteps := make([]nestedStep, 0, syncNum)
		asyncNum := 10
		asyncSteps := make([]nestedStep, 0, asyncNum)
		for i := 0; i < syncNum; i++ {
			syncSteps = append(syncSteps, newMockNormalStep())
		}
		for i := 0; i < asyncNum; i++ {
			asyncSteps = append(asyncSteps, newMockNormalStep())
		}
		for _, step := range asyncSteps {
			redo.AddAsyncStep(step)
		}
		for _, step := range syncSteps {
			redo.AddSyncStep(step)
		}
		err := redo.Execute(context.Background())
		assert.NoError(t, err)
		for _, step := range asyncSteps {
			<-step.(*mockNormalStep).calledChan
			assert.True(t, step.(*mockNormalStep).called)
		}
	})
}
