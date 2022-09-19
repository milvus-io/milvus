package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type baseRedoTask struct {
	syncTodoStep  []nestedStep // steps to execute synchronously
	asyncTodoStep []nestedStep // steps to execute asynchronously
	stepExecutor  StepExecutor
}

func newBaseRedoTask(stepExecutor StepExecutor) *baseRedoTask {
	return &baseRedoTask{
		syncTodoStep:  make([]nestedStep, 0),
		asyncTodoStep: make([]nestedStep, 0),
		stepExecutor:  stepExecutor,
	}
}

func (b *baseRedoTask) AddSyncStep(step nestedStep) {
	b.syncTodoStep = append(b.syncTodoStep, step)
}

func (b *baseRedoTask) AddAsyncStep(step nestedStep) {
	b.asyncTodoStep = append(b.asyncTodoStep, step)
}

func (b *baseRedoTask) redoAsyncSteps() {
	l := len(b.asyncTodoStep)
	steps := make([]nestedStep, 0, l)
	for i := l - 1; i >= 0; i-- {
		steps = append(steps, b.asyncTodoStep[i])
	}
	b.asyncTodoStep = nil // make baseRedoTask can be collected.
	b.stepExecutor.AddSteps(&stepStack{steps: steps})
}

func (b *baseRedoTask) Execute(ctx context.Context) error {
	for i := 0; i < len(b.syncTodoStep); i++ {
		todo := b.syncTodoStep[i]
		// no children step in sync steps.
		if _, err := todo.Execute(ctx); err != nil {
			log.Error("failed to execute step", zap.Error(err), zap.String("desc", todo.Desc()))
			return err
		}
	}
	go b.redoAsyncSteps()
	return nil
}
