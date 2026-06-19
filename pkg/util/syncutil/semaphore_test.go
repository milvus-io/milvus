package syncutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestSemaphoreAcquireRelease(t *testing.T) {
	s := NewSemaphore(2)
	assert.Equal(t, 2, s.Cap())
	assert.Equal(t, 0, s.Current())

	assert.NoError(t, s.Acquire(context.Background()))
	assert.Equal(t, 1, s.Current())

	assert.NoError(t, s.Acquire(context.Background()))
	assert.Equal(t, 2, s.Current())

	s.Release()
	assert.Equal(t, 1, s.Current())

	s.Release()
	assert.Equal(t, 0, s.Current())
}

func TestSemaphoreTryAcquire(t *testing.T) {
	s := NewSemaphore(1)

	assert.True(t, s.TryAcquire())
	assert.False(t, s.TryAcquire())

	s.Release()
	assert.True(t, s.TryAcquire())
}

func TestSemaphoreBlocksWhenFull(t *testing.T) {
	s := NewSemaphore(1)
	assert.NoError(t, s.Acquire(context.Background()))

	acquired := atomic.NewBool(false)
	go func() {
		assert.NoError(t, s.Acquire(context.Background()))
		acquired.Store(true)
	}()

	time.Sleep(50 * time.Millisecond)
	assert.False(t, acquired.Load(), "acquire must block when semaphore is full")

	s.Release()
	assert.Eventually(t, acquired.Load, time.Second, 10*time.Millisecond)
}

func TestSemaphoreContextCancellation(t *testing.T) {
	s := NewSemaphore(1)
	assert.NoError(t, s.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := s.Acquire(ctx)
	assert.Error(t, err)
	assert.Equal(t, 1, s.Current(), "failed acquire must not change current count")

	s.Release()
}

func TestSemaphoreSetCapacityIncrease(t *testing.T) {
	s := NewSemaphore(1)
	assert.NoError(t, s.Acquire(context.Background()))

	// Semaphore is full. Start a goroutine that will block on Acquire.
	acquired := atomic.NewBool(false)
	go func() {
		assert.NoError(t, s.Acquire(context.Background()))
		acquired.Store(true)
	}()

	time.Sleep(50 * time.Millisecond)
	assert.False(t, acquired.Load())

	// Increase capacity — blocked goroutine should proceed.
	s.SetCapacity(2)
	assert.Eventually(t, acquired.Load, time.Second, 10*time.Millisecond)
	assert.Equal(t, 2, s.Current())
}

func TestSemaphoreSetCapacityDecrease(t *testing.T) {
	s := NewSemaphore(3)
	assert.NoError(t, s.Acquire(context.Background()))
	assert.NoError(t, s.Acquire(context.Background()))

	// Decrease capacity below current holders — no tokens revoked.
	s.SetCapacity(1)
	assert.Equal(t, 2, s.Current())
	assert.Equal(t, 1, s.Cap())

	// New acquires should block until current drops below new capacity.
	acquired := atomic.NewBool(false)
	go func() {
		assert.NoError(t, s.Acquire(context.Background()))
		acquired.Store(true)
	}()

	time.Sleep(50 * time.Millisecond)
	assert.False(t, acquired.Load())

	// Release two tokens — now current=0 < capacity=1, goroutine should proceed.
	s.Release()
	s.Release()
	assert.Eventually(t, acquired.Load, time.Second, 10*time.Millisecond)
}

func TestSemaphorePanicOnInvalidCapacity(t *testing.T) {
	assert.Panics(t, func() { NewSemaphore(0) })
	assert.Panics(t, func() { NewSemaphore(-1) })

	s := NewSemaphore(1)
	assert.Panics(t, func() { s.SetCapacity(0) })
}

func TestSemaphorePanicOnReleaseWithoutAcquire(t *testing.T) {
	s := NewSemaphore(1)
	assert.Panics(t, func() { s.Release() })
}

func TestSemaphoreConcurrent(t *testing.T) {
	s := NewSemaphore(3)
	maxConcurrent := atomic.NewInt32(0)
	current := atomic.NewInt32(0)

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			assert.NoError(t, s.Acquire(context.Background()))
			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			current.Add(-1)
			s.Release()
			done <- struct{}{}
		}()
	}

	for i := 0; i < 20; i++ {
		<-done
	}

	assert.LessOrEqual(t, maxConcurrent.Load(), int32(3))
	assert.Equal(t, 0, s.Current())
}
