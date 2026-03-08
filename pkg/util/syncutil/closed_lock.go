package syncutil

import "sync"

type ClosableLock struct {
	mu     sync.Mutex
	closed bool
}

func (l *ClosableLock) LockIfNotClosed() bool {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return false
	}
	return true
}

func (l *ClosableLock) Unlock() {
	l.mu.Unlock()
}

func (l *ClosableLock) Close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
}
