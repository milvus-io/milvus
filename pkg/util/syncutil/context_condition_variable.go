package syncutil

import (
	"context"
	"sync"
)

// NewContextCond creates a new condition variable that can be used with context.
// Broadcast is implemented using a channel, so the performance may not be as good as sync.Cond.
func NewContextCond(l sync.Locker) *ContextCond {
	return &ContextCond{L: l}
}

// ContextCond is a condition variable implementation that can be used with context.
type ContextCond struct {
	noCopy noCopy

	L  sync.Locker
	ch chan struct{}
}

// LockAndBroadcast locks the underlying locker and performs a broadcast.
// It notifies all goroutines waiting on the condition variable.
//
//	c.LockAndBroadcast()
//	... make some change ...
//	c.L.Unlock()
func (cv *ContextCond) LockAndBroadcast() {
	cv.L.Lock()
	if cv.ch != nil {
		close(cv.ch)
		cv.ch = nil
	}
}

// UnsafeBroadcast performs a broadcast without locking.
// !!! Must be called with the lock held !!!
func (cv *ContextCond) UnsafeBroadcast() {
	if cv.ch != nil {
		close(cv.ch)
		cv.ch = nil
	}
}

// Wait waits for a broadcast or context timeout.
// It blocks until either a broadcast is received or the context is canceled or times out.
// Returns an error if the context is canceled or times out.
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	c.L.Lock()
//	for !condition() {
//	    if err := c.Wait(ctx); err != nil {
//	           return err
//	       }
//	   }
//	... make use of condition ...
//	c.L.Unlock()
func (cv *ContextCond) Wait(ctx context.Context) error {
	if cv.ch == nil {
		cv.ch = make(chan struct{})
	}
	ch := cv.ch
	cv.L.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	cv.L.Lock()
	return nil
}

// WaitChan returns a channel that can be used to wait for a broadcast.
// Should be called after Lock.
// The channel is closed when a broadcast is received.
func (cv *ContextCond) WaitChan() <-chan struct{} {
	if cv.ch == nil {
		cv.ch = make(chan struct{})
	}
	ch := cv.ch
	cv.L.Unlock()
	return ch
}

// noCopy may be added to structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
//
// Note that it must not be embedded, due to the Lock and Unlock methods.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
