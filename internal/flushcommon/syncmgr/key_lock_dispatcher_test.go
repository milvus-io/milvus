package syncmgr

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type KeyLockDispatcherSuite struct {
	suite.Suite
}

// TestSameKeySerial verifies that tasks with the same key execute serially.
func (s *KeyLockDispatcherSuite) TestSameKeySerial() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newKeyLockDispatcher[int64](2)

	blocker := make(chan struct{})
	t1 := NewMockTask(s.T())
	t1.EXPECT().Run(ctx).Run(func(_ context.Context) {
		<-blocker
	}).Return(nil)

	t2Started := atomic.NewBool(false)
	t2 := NewMockTask(s.T())
	t2.EXPECT().Run(ctx).Run(func(_ context.Context) {
		t2Started.Store(true)
	}).Return(nil)

	// Submit both tasks for key=1. Submit returns immediately (non-blocking).
	f1 := d.Submit(ctx, 1, t1)
	f2 := d.Submit(ctx, 1, t2)

	// t2 must not start while t1 is still running (same key).
	time.Sleep(50 * time.Millisecond)
	s.False(t2Started.Load(), "task 2 must not start before task 1 completes")

	// Complete t1.
	close(blocker)
	_, err := f1.Await()
	s.NoError(err)

	// t2 should now complete.
	_, err = f2.Await()
	s.NoError(err)
	s.True(t2Started.Load())
}

// TestCrossKeyConcurrent verifies that tasks with different keys run concurrently.
func (s *KeyLockDispatcherSuite) TestCrossKeyConcurrent() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newKeyLockDispatcher[int64](4)

	blocker1 := make(chan struct{})
	blocker2 := make(chan struct{})

	t1Started := atomic.NewBool(false)
	t2Started := atomic.NewBool(false)

	t1 := NewMockTask(s.T())
	t1.EXPECT().Run(ctx).Run(func(_ context.Context) {
		t1Started.Store(true)
		<-blocker1
	}).Return(nil)

	t2 := NewMockTask(s.T())
	t2.EXPECT().Run(ctx).Run(func(_ context.Context) {
		t2Started.Store(true)
		<-blocker2
	}).Return(nil)

	d.Submit(ctx, 1, t1)
	d.Submit(ctx, 2, t2)

	// Both tasks should be running concurrently.
	s.Eventually(func() bool { return t1Started.Load() && t2Started.Load() },
		time.Second, 10*time.Millisecond,
		"tasks with different keys must run concurrently")

	close(blocker1)
	close(blocker2)
}

// TestSemaphoreBackpressure verifies that Submit blocks when pending tasks reach the limit.
func (s *KeyLockDispatcherSuite) TestSemaphoreBackpressure() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pool size 1, semaphore capacity = max(1*2, 4) = 4.
	// Use same key so tasks queue internally (only 1 pool worker needed at a time).
	d := newKeyLockDispatcher[int64](1)
	semCap := d.semaphore.Cap()

	blocker := make(chan struct{})

	// Submit semCap tasks for the same key. First runs, rest queue internally.
	for i := 0; i < semCap; i++ {
		t := NewMockTask(s.T())
		t.EXPECT().Run(mock.Anything).Run(func(_ context.Context) {
			<-blocker
		}).Return(nil).Maybe()
		d.Submit(ctx, 1, t)
	}

	// Next Submit must block (semaphore full).
	extraTask := NewMockTask(s.T())
	extraTask.EXPECT().Run(mock.Anything).Run(func(_ context.Context) {
		<-blocker
	}).Return(nil).Maybe()

	submitted := atomic.NewBool(false)
	go func() {
		d.Submit(ctx, 1, extraTask)
		submitted.Store(true)
	}()

	time.Sleep(100 * time.Millisecond)
	s.False(submitted.Load(), "submit must block when semaphore is full")

	// Release tasks to free semaphore slots.
	close(blocker)

	s.Eventually(submitted.Load, 5*time.Second, 10*time.Millisecond,
		"submit must unblock after tasks complete")
}

// TestCallbackPropagation verifies callbacks are called and errors propagated.
func (s *KeyLockDispatcherSuite) TestCallbackPropagation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newKeyLockDispatcher[int64](2)

	t := NewMockTask(s.T())
	t.EXPECT().Run(ctx).Return(nil)

	callbackCalled := atomic.NewBool(false)
	f := d.Submit(ctx, 1, t, func(err error) error {
		callbackCalled.Store(true)
		return err
	})

	_, err := f.Await()
	s.NoError(err)
	s.True(callbackCalled.Load())
}

// TestMixedKeysConcurrency verifies a realistic scenario: multiple segments
// syncing concurrently while same-segment syncs remain serial.
func (s *KeyLockDispatcherSuite) TestMixedKeysConcurrency() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := newKeyLockDispatcher[int64](4)

	var mu sync.Mutex
	executionOrder := make([]int64, 0)

	makeTask := func(key int64, delay time.Duration) *MockTask {
		t := NewMockTask(s.T())
		t.EXPECT().Run(ctx).Run(func(_ context.Context) {
			time.Sleep(delay)
			mu.Lock()
			executionOrder = append(executionOrder, key)
			mu.Unlock()
		}).Return(nil)
		return t
	}

	// Key 1: two serial tasks (slow, 100ms each)
	f1a := d.Submit(ctx, 1, makeTask(1, 100*time.Millisecond))
	f1b := d.Submit(ctx, 1, makeTask(1, 100*time.Millisecond))

	// Key 2: one fast task (10ms)
	f2 := d.Submit(ctx, 2, makeTask(2, 10*time.Millisecond))

	// Wait for all to complete.
	_, _ = f2.Await()
	_, _ = f1a.Await()
	_, _ = f1b.Await()

	// Key 2 (10ms) should finish before key 1's second task (starts after 100ms).
	mu.Lock()
	defer mu.Unlock()
	s.Len(executionOrder, 3)
	key1Count := 0
	for _, k := range executionOrder {
		if k == 2 {
			s.Less(key1Count, 2, "key 2 should finish before both key 1 tasks complete")
			break
		}
		if k == 1 {
			key1Count++
		}
	}
}

func TestKeyLockDispatcher(t *testing.T) {
	suite.Run(t, new(KeyLockDispatcherSuite))
}
