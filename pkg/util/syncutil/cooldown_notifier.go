package syncutil

import (
	"sync"
	"time"
)

// NewCooldownNotifier creates a new CooldownNotifier with the specified cooldown duration and buffer size.
func NewCooldownNotifier[T any](cooldown time.Duration, buffed int) *CooldownNotifier[T] {
	return &CooldownNotifier[T]{
		cooldown: cooldown,
		notifier: make(chan T, buffed),
	}
}

// CooldownNotifier is a utility that sends notifications after a specified cooldown period.
type CooldownNotifier[T any] struct {
	mu                sync.Mutex
	lastNotifyInstant time.Time
	cooldown          time.Duration
	notifier          chan T
}

// Notify sends a notification if the cooldown period has passed since the last notification.
func (cn *CooldownNotifier[T]) Notify(t T) {
	cn.mu.Lock()
	now := time.Now()
	if now.Sub(cn.lastNotifyInstant) < cn.cooldown {
		cn.mu.Unlock()
		return
	}
	cn.lastNotifyInstant = now
	cn.mu.Unlock()

	select {
	case cn.notifier <- t:
	default:
	}
}

// Chan returns a channel that is closed when the cooldown is over.
func (cn *CooldownNotifier[T]) Chan() <-chan T {
	return cn.notifier
}
