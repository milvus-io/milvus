package rootcoord

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type baseUndoTask struct {
	todoStep     []nestedStep // steps to execute
	undoStep     []nestedStep // steps to undo
	stepExecutor StepExecutor
}

func newBaseUndoTask(stepExecutor StepExecutor) *baseUndoTask {
	return &baseUndoTask{
		todoStep:     make([]nestedStep, 0),
		undoStep:     make([]nestedStep, 0),
		stepExecutor: stepExecutor,
	}
}

func (b *baseUndoTask) AddStep(todoStep, undoStep nestedStep) {
	b.todoStep = append(b.todoStep, todoStep)
	b.undoStep = append(b.undoStep, undoStep)
}

func (b *baseUndoTask) Execute(ctx context.Context) error {
	if len(b.todoStep) != len(b.undoStep) {
		return fmt.Errorf("todo step and undo step length not equal")
	}
	for i := 0; i < len(b.todoStep); i++ {
		todoStep := b.todoStep[i]
		// no children step in normal case.
		if _, err := todoStep.Execute(ctx); err != nil {
			log.Warn("failed to execute step, trying to undo", zap.Error(err), zap.String("desc", todoStep.Desc()))
			undoSteps := b.undoStep[:i]
			b.undoStep = nil // let baseUndoTask can be collected.
			go b.stepExecutor.AddSteps(&stepStack{undoSteps})
			return err
		}
	}
	return nil
}
