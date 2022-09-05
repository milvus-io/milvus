package rootcoord

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type baseUndoTask struct {
	todoStep []Step // steps to execute
	undoStep []Step // steps to undo
}

func newBaseUndoTask() *baseUndoTask {
	return &baseUndoTask{
		todoStep: make([]Step, 0),
		undoStep: make([]Step, 0),
	}
}

func (b *baseUndoTask) AddStep(todoStep, undoStep Step) {
	b.todoStep = append(b.todoStep, todoStep)
	b.undoStep = append(b.undoStep, undoStep)
}

func (b *baseUndoTask) undoFromLastFinished(lastFinished int) {
	// You cannot just use the ctx of task, since it will be canceled after response is returned.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for i := lastFinished; i >= 0; i-- {
		undo := b.undoStep[i]
		if err := undo.Execute(ctx); err != nil {
			// You depend on the collection meta to do other gc.
			// TODO: add ddl logger after other service can be idempotent enough, then you can do separate steps
			//		independently.
			log.Error("failed to execute step, garbage may be generated", zap.Error(err))
			return
		}
	}
}

func (b *baseUndoTask) Execute(ctx context.Context) error {
	if len(b.todoStep) != len(b.undoStep) {
		return fmt.Errorf("todo step and undo step length not equal")
	}
	for i := 0; i < len(b.todoStep); i++ {
		todoStep := b.todoStep[i]
		err := todoStep.Execute(ctx)
		if err != nil {
			go b.undoFromLastFinished(i - 1)
			log.Warn("failed to execute step, trying to undo", zap.Error(err))
			return err
		}
	}
	return nil
}
