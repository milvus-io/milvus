package queryservice

import (
	"context"
	"errors"
)

type Condition interface {
	WaitToFinish() error
	Notify(err error)
	Ctx() context.Context
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

func (tc *TaskCondition) Ctx() context.Context {
	return tc.ctx
}

func NewTaskCondition(ctx context.Context) *TaskCondition {
	return &TaskCondition{
		done: make(chan error, 1),
		ctx:  ctx,
	}
}
