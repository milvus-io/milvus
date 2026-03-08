package syncutil

import (
	"context"
)

// Future is a future value that can be set and retrieved.
type Future[T any] struct {
	ch    chan struct{}
	value T
}

// NewFuture creates a new future.
func NewFuture[T any]() *Future[T] {
	return &Future[T]{
		ch: make(chan struct{}),
	}
}

// Set sets the value of the future.
func (f *Future[T]) Set(value T) {
	f.value = value
	close(f.ch)
}

// GetWithContext retrieves the value of the future if set, otherwise block until set or the context is done.
func (f *Future[T]) GetWithContext(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var val T
		return val, ctx.Err()
	case <-f.ch:
		return f.value, nil
	}
}

// MustGet retrieves the value of the future if set, otherwise panic if the ready yet.
func (f *Future[T]) MustGet() T {
	if !f.Ready() {
		panic("future is not ready")
	}
	return f.Get()
}

// Get retrieves the value of the future if set, otherwise block until set.
func (f *Future[T]) Get() T {
	<-f.ch
	return f.value
}

// Done returns a channel that is closed when the future is set.
func (f *Future[T]) Done() <-chan struct{} {
	return f.ch
}

// Ready returns true if the future is set.
func (f *Future[T]) Ready() bool {
	select {
	case <-f.ch:
		return true
	default:
		return false
	}
}
