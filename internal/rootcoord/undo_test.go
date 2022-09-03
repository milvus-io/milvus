package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_baseUndoTask_Execute(t *testing.T) {
	t.Run("should not happen", func(t *testing.T) {
		undoTask := newBaseUndoTask()
		undoTask.todoStep = append(undoTask.todoStep, newMockNormalStep())
		err := undoTask.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case, no undo step will be called", func(t *testing.T) {
		undoTask := newBaseUndoTask()
		n := 10
		todoSteps, undoSteps := make([]Step, 0, n), make([]Step, 0, n)
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
		undoTask := newBaseUndoTask()
		todoSteps := []Step{
			newMockNormalStep(),
			newMockFailStep(),
			newMockNormalStep(),
		}
		undoSteps := []Step{
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
		undoTask := newBaseUndoTask()
		todoSteps := []Step{
			newMockNormalStep(),
			newMockNormalStep(),
			newMockFailStep(),
		}
		undoSteps := []Step{
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
