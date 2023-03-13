package tasks

import "github.com/cockroachdb/errors"

var (
	ErrTaskQueueFull = errors.New("TaskQueueFull")
)
