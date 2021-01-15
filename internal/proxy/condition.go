package proxy

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type Condition interface {
	WaitToFinish() error
	Notify(err error)
}

type TaskCondition struct {
	done chan error
	ctx  context.Context
}

func (tc *TaskCondition) WaitToFinish() error {
	for {
		select {
		case <-tc.ctx.Done():
			return errors.New("timeout")
		case err := <-tc.done:
			return err
		}
	}
}

func (tc *TaskCondition) Notify(err error) {
	tc.done <- err
}

func NewTaskCondition(ctx context.Context) *TaskCondition {
	return &TaskCondition{
		done: make(chan error),
		ctx:  ctx,
	}
}
