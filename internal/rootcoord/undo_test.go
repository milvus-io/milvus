package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestUndoTask() *baseUndoTask {
	stepExecutor := newMockStepExecutor()
	stepExecutor.AddStepsFunc = func(s *stepStack) {
		// no schedule, execute directly.
		s.Execute(context.Background())
	}
	undoTask := newBaseUndoTask(stepExecutor)
	return undoTask
}

func Test_baseUndoTask_Execute(t *testing.T) {
	t.Run("should not happen", func(t *testing.T) {
		undoTask := newTestUndoTask()
		undoTask.todoStep = append(undoTask.todoStep, newMockNormalStep())
		err := undoTask.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case, no undo step will be called", func(t *testing.T) {
		undoTask := newTestUndoTask()
		n := 10
		todoSteps, undoSteps := make([]nestedStep, 0, n), make([]nestedStep, 0, n)
		for i := 0; i < n; i++ {
			normalTodoStep := newMockNormalStep()
			normalUndoStep := newMockNormalStep()
			todoSteps = append(todoSteps, normalTodoStep)
			undoSteps = append(undoSteps, normalUndoStep)
		}
		for i := 0; i < n; i++ {
			undoTask.AddStep(todoSteps[i], undoSteps[i])
		}
		err := undoTask.Execute(context.Background())
		assert.NoError(t, err)
		// make sure no undo steps will be called.
		for _, step := range undoSteps {
			assert.False(t, step.(*mockNormalStep).called)
		}
	})

	t.Run("partial error, undo from last finished", func(t *testing.T) {
		undoTask := newTestUndoTask()
		todoSteps := []nestedStep{
			newMockNormalStep(),
			newMockFailStep(),
			newMockNormalStep(),
		}
		undoSteps := []nestedStep{
			newMockNormalStep(),
			newMockNormalStep(),
			newMockNormalStep(),
		}
		l := len(todoSteps)
		for i := 0; i < l; i++ {
			undoTask.AddStep(todoSteps[i], undoSteps[i])
		}
		err := undoTask.Execute(context.Background())
		assert.Error(t, err)
		assert.True(t, todoSteps[0].(*mockNormalStep).called)
		assert.True(t, todoSteps[1].(*mockFailStep).called)
		assert.False(t, todoSteps[2].(*mockNormalStep).called)

		<-undoSteps[0].(*mockNormalStep).calledChan
		assert.True(t, undoSteps[0].(*mockNormalStep).called)
		assert.False(t, undoSteps[1].(*mockNormalStep).called)
		assert.False(t, undoSteps[2].(*mockNormalStep).called)
	})

	t.Run("partial error, undo meet error also", func(t *testing.T) {
		undoTask := newTestUndoTask()
		todoSteps := []nestedStep{
			newMockNormalStep(),
			newMockNormalStep(),
			newMockFailStep(),
		}
		undoSteps := []nestedStep{
			newMockNormalStep(),
			newMockFailStep(),
			newMockNormalStep(),
		}
		l := len(todoSteps)
		for i := 0; i < l; i++ {
			undoTask.AddStep(todoSteps[i], undoSteps[i])
		}
		err := undoTask.Execute(context.Background())
		assert.Error(t, err)
		assert.True(t, todoSteps[0].(*mockNormalStep).called)
		assert.True(t, todoSteps[1].(*mockNormalStep).called)
		assert.True(t, todoSteps[2].(*mockFailStep).called)
		assert.False(t, undoSteps[0].(*mockNormalStep).called)
		<-undoSteps[1].(*mockFailStep).calledChan
		assert.True(t, undoSteps[1].(*mockFailStep).called)
		assert.False(t, undoSteps[2].(*mockNormalStep).called)
	})
}
