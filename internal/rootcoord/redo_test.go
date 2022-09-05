package rootcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockFailStep struct {
	calledChan chan struct{}
	called     bool
}

func newMockFailStep() *mockFailStep {
	return &mockFailStep{calledChan: make(chan struct{}, 1), called: false}
}

func (m *mockFailStep) Execute(ctx context.Context) error {
	m.called = true
	m.calledChan <- struct{}{}
	return errors.New("error mock Execute")
}

type mockNormalStep struct {
	calledChan chan struct{}
	called     bool
}

func newMockNormalStep() *mockNormalStep {
	return &mockNormalStep{calledChan: make(chan struct{}, 1), called: false}
}

func (m *mockNormalStep) Execute(ctx context.Context) error {
	m.called = true
	m.calledChan <- struct{}{}
	return nil
}

func Test_baseRedoTask_redoAsyncSteps(t *testing.T) {
	t.Run("partial error", func(t *testing.T) {
		redo := newBaseRedoTask()
		steps := []Step{newMockNormalStep(), newMockFailStep(), newMockNormalStep()}
		for _, step := range steps {
			redo.AddAsyncStep(step)
		}
		redo.redoAsyncSteps()
		assert.True(t, steps[0].(*mockNormalStep).called)
		assert.False(t, steps[2].(*mockNormalStep).called)
	})

	t.Run("normal case", func(t *testing.T) {
		redo := newBaseRedoTask()
		n := 10
		steps := make([]Step, 0, n)
		for i := 0; i < n; i++ {
			steps = append(steps, newMockNormalStep())
		}
		for _, step := range steps {
			redo.AddAsyncStep(step)
		}
		redo.redoAsyncSteps()
		for _, step := range steps {
			assert.True(t, step.(*mockNormalStep).called)
		}
	})
}

func Test_baseRedoTask_Execute(t *testing.T) {
	t.Run("sync not finished, no async task", func(t *testing.T) {
		redo := newBaseRedoTask()
		syncSteps := []Step{newMockFailStep()}
		asyncNum := 10
		asyncSteps := make([]Step, 0, asyncNum)
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
		redo := newBaseRedoTask()
		syncNum := 10
		syncSteps := make([]Step, 0, syncNum)
		asyncNum := 10
		asyncSteps := make([]Step, 0, asyncNum)
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
