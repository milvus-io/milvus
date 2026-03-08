package syncutil

import "context"

// NewAsyncTaskNotifier creates a new async task notifier.
func NewAsyncTaskNotifier[T any]() *AsyncTaskNotifier[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncTaskNotifier[T]{
		ctx:    ctx,
		cancel: cancel,
		future: NewFuture[T](),
	}
}

// AsyncTaskNotifier is a notifier for async task.
type AsyncTaskNotifier[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	future *Future[T]
}

// Context returns the context of the async task.
func (n *AsyncTaskNotifier[T]) Context() context.Context {
	return n.ctx
}

// Cancel cancels the async task, the async task can receive the cancel signal from Context.
func (n *AsyncTaskNotifier[T]) Cancel() {
	n.cancel()
}

// BlockAndGetResult returns the result of the async task.
func (n *AsyncTaskNotifier[T]) BlockAndGetResult() T {
	return n.future.Get()
}

// BlockUntilFinish blocks until the async task is finished.
func (n *AsyncTaskNotifier[T]) BlockUntilFinish() {
	<-n.future.Done()
}

// FinishChan returns a channel that will be closed when the async task is finished.
func (n *AsyncTaskNotifier[T]) FinishChan() <-chan struct{} {
	return n.future.Done()
}

// Finish finishes the async task with a result.
func (n *AsyncTaskNotifier[T]) Finish(result T) {
	n.future.Set(result)
}
