package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

type Checker interface {
	ID() int64
	SetID(id int64)
	Description() string
	Check(ctx context.Context) []task.Task
}

type baseChecker struct {
	id int64
}

func (checker *baseChecker) ID() int64 {
	return checker.id
}

func (checker *baseChecker) SetID(id int64) {
	checker.id = id
}
