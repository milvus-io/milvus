package syncutil

import (
	"context"
	"sync"
)

// Semaphore is a counting semaphore with dynamically adjustable capacity.
//
// It supports context-aware Acquire (can be canceled/timed out) and non-blocking
// TryAcquire. The capacity can be changed at runtime via SetCapacity; waiters are
// woken up when capacity increases.
type Semaphore struct {
	mu   sync.Mutex
	cond *ContextCond

	capacity int // maximum number of concurrent holders
	current  int // number of currently held tokens
}

// NewSemaphore creates a semaphore with the given initial capacity.
// Panics if capacity <= 0.
func NewSemaphore(capacity int) *Semaphore {
	if capacity <= 0 {
		panic("syncutil: semaphore capacity must be positive")
	}
	s := &Semaphore{
		capacity: capacity,
	}
	s.cond = NewContextCond(&s.mu)
	return s
}

// Acquire blocks until a token is available or ctx is canceled.
// Returns nil on success, or the context error on cancellation/timeout.
func (s *Semaphore) Acquire(ctx context.Context) error {
	s.mu.Lock()
	for s.current >= s.capacity {
		if err := s.cond.Wait(ctx); err != nil {
			// cond.Wait does NOT re-acquire the lock on error.
			return err
		}
	}
	s.current++
	s.mu.Unlock()
	return nil
}

// TryAcquire attempts to acquire a token without blocking.
// Returns true if a token was acquired, false if the semaphore is full.
func (s *Semaphore) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current >= s.capacity {
		return false
	}
	s.current++
	return true
}

// Release returns a token to the semaphore, waking one waiter if any.
func (s *Semaphore) Release() {
	s.cond.LockAndBroadcast()
	if s.current <= 0 {
		s.mu.Unlock()
		panic("syncutil: semaphore release without acquire")
	}
	s.current--
	s.mu.Unlock()
}

// SetCapacity dynamically adjusts the semaphore capacity.
// If the new capacity is larger, blocked Acquire calls may proceed.
// If smaller, no tokens are revoked — the semaphore simply won't grant new
// tokens until current holders release enough to drop below the new capacity.
// Panics if capacity <= 0.
func (s *Semaphore) SetCapacity(capacity int) {
	if capacity <= 0 {
		panic("syncutil: semaphore capacity must be positive")
	}
	s.cond.LockAndBroadcast()
	s.capacity = capacity
	s.mu.Unlock()
}

// Cap returns the current capacity.
func (s *Semaphore) Cap() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capacity
}

// Current returns the number of currently held tokens.
func (s *Semaphore) Current() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}
