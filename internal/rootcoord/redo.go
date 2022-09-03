package rootcoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type baseRedoTask struct {
	syncTodoStep  []Step // steps to execute synchronously
	asyncTodoStep []Step // steps to execute asynchronously
}

func newBaseRedoTask() *baseRedoTask {
	return &baseRedoTask{
		syncTodoStep:  make([]Step, 0),
		asyncTodoStep: make([]Step, 0),
	}
}

func (b *baseRedoTask) AddSyncStep(step Step) {
	b.syncTodoStep = append(b.syncTodoStep, step)
}

func (b *baseRedoTask) AddAsyncStep(step Step) {
	b.asyncTodoStep = append(b.asyncTodoStep, step)
}

func (b *baseRedoTask) redoAsyncSteps() {
	// You cannot just use the ctx of task, since it will be canceled after response is returned.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for i := 0; i < len(b.asyncTodoStep); i++ {
		todo := b.asyncTodoStep[i]
		if err := todo.Execute(ctx); err != nil {
			// You depend on the collection meta to do other gc.
			// TODO: add ddl logger after other service can be idempotent enough, then you can do separate steps
			//		independently.
			log.Error("failed to execute step, garbage may be generated", zap.Error(err))
			return
		}
	}
}

func (b *baseRedoTask) Execute(ctx context.Context) error {
	for i := 0; i < len(b.syncTodoStep); i++ {
		todo := b.syncTodoStep[i]
		if err := todo.Execute(ctx); err != nil {
			log.Error("failed to execute step", zap.Error(err))
			return err
		}
	}
	go b.redoAsyncSteps()
	return nil
}
