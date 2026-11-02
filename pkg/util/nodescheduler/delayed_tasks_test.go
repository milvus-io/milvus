// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nodescheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDeadlineRetriesOrderAndDoNotBlockReadyTasks(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type execution struct {
		id int
		at time.Time
	}
	ran := make(chan execution, 4)
	later := time.Now().Add(300 * time.Millisecond)
	earlier := later.Add(-150 * time.Millisecond)
	for _, item := range []struct {
		id int
		at time.Time
	}{{1, later}, {2, earlier}, {3, earlier}} {
		item := item
		attempt := 0
		s.Submit(TaskFunc(func(context.Context) error {
			attempt++
			if attempt == 1 {
				return merr.Wrap(DelayUntil(item.at), "waiting for publication")
			}
			ran <- execution{item.id, time.Now()}
			return nil
		}))
	}
	s.Submit(TaskFunc(func(context.Context) error { ran <- execution{4, time.Now()}; return nil }))
	for _, expected := range []int{4, 2, 3, 1} {
		select {
		case result := <-ran:
			require.Equal(t, expected, result.id)
			if expected != 4 {
				deadline := earlier
				if expected == 1 {
					deadline = later
				}
				require.False(t, result.at.Before(deadline))
			}
		case <-ctx.Done():
			t.Fatal("deadline task was not scheduled")
		}
	}
	require.ErrorIs(t, DelayUntil(later), ErrDelay)
}

func TestCancelAndCloseFinishDelayedTasksImmediately(t *testing.T) {
	s := New(1)
	defer s.Close()
	var executions atomic.Int32
	handles := make([]TaskHandle, 0, 3)
	for range 3 {
		handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
			executions.Add(1)
			return DelayUntil(time.Now().Add(time.Hour))
		})))
	}
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.delayed) == 3
	}, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handles[1].Cancel() // Remove a non-root heap entry.
	require.NoError(t, handles[1].Wait(ctx))
	s.mu.Lock()
	delayedCount := len(s.delayed)
	s.mu.Unlock()
	require.Equal(t, 2, delayedCount)
	s.Close()
	for _, handle := range handles {
		require.NoError(t, handle.Wait(ctx))
		handle.Cancel()
	}
	require.EqualValues(t, 3, executions.Load())
}

func TestExpiredDeadlineRequeuesAtReadyQueueTail(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var attempts atomic.Int32
	order := make(chan int, 2)
	h := s.Submit(TaskFunc(func(context.Context) error {
		if attempts.Add(1) == 1 {
			s.Submit(TaskFunc(func(context.Context) error {
				order <- 1
				return nil
			}))
			return DelayUntil(time.Now().Add(-time.Second))
		}
		order <- 2
		return nil
	}))
	require.NoError(t, h.Wait(ctx))
	require.EqualValues(t, 2, attempts.Load())
	require.Equal(t, 1, <-order)
	require.Equal(t, 2, <-order)
}

func TestDefaultExponentialRetryBackoff(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"bare", ErrDelay},
		{"marked", errors.Mark(merr.ErrServiceNotReady, ErrDelay)},
		{"wrapped", merr.Wrap(ErrDelay, "waiting for resource")},
		{"zero-deadline", DelayUntil(time.Time{})},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := New(1)
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var runs []time.Time
			h := s.Submit(TaskFunc(func(context.Context) error {
				runs = append(runs, time.Now())
				if len(runs) <= 2 {
					return test.err
				}
				return nil
			}))
			require.NoError(t, h.Wait(ctx))
			require.Len(t, runs, 3)
			require.GreaterOrEqual(t, runs[1].Sub(runs[0]), 10*time.Millisecond)
			require.GreaterOrEqual(t, runs[2].Sub(runs[1]), 20*time.Millisecond)
			require.Equal(t, 40*time.Millisecond, h.(*taskHandle).entry.backoff.NextBackOff())
		})
	}
}

func TestCancelQueuedTaskCompletesWhileWorkerIsBusy(t *testing.T) {
	s := New(1)
	defer s.Close()
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	s.Submit(TaskFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	}))
	<-started
	var ran atomic.Bool
	h := s.Submit(TaskFunc(func(context.Context) error {
		ran.Store(true)
		return nil
	}))
	h.Cancel()
	h.Cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.Wait(ctx))
	require.False(t, ran.Load())
	s.mu.Lock()
	queued := s.queue.Len()
	s.mu.Unlock()
	require.Zero(t, queued)
}

func TestCancelAndCloseWaitForRunningExecute(t *testing.T) {
	s := New(1)
	defer s.Close()
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	started := make(chan struct{})
	canceled := make(chan struct{})
	h := s.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return DelayUntil(time.Now().Add(time.Hour))
	}))
	<-started
	h.Cancel()
	<-canceled
	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closed
	}, time.Second, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, h.Wait(ctx), context.Canceled)
	select {
	case <-closed:
		t.Fatal("Close completed while Execute still owned the task")
	default:
	}
	unblock()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	require.NoError(t, h.Wait(waitCtx))
	select {
	case <-closed:
	case <-waitCtx.Done():
		t.Fatal("Close did not finish after Execute returned")
	}
	s.mu.Lock()
	delayed := len(s.delayed)
	s.mu.Unlock()
	require.Zero(t, delayed)
}

func TestCancelHeapMinimumAndIgnoreStaleTimerCallback(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var executions atomic.Int32
	handles := make([]TaskHandle, 0, 2)
	for _, delay := range []time.Duration{time.Hour, 2 * time.Hour} {
		deadline := time.Now().Add(delay)
		handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
			executions.Add(1)
			return DelayUntil(deadline)
		})))
	}
	// A ready task on the sole worker runs after both retries enter the heap.
	barrier := s.Submit(TaskFunc(func(context.Context) error { return nil }))
	require.NoError(t, barrier.Wait(ctx))
	s.mu.Lock()
	timer := s.timer
	s.mu.Unlock()
	require.NotNil(t, timer)
	handles[0].Cancel()
	require.NoError(t, handles[0].Wait(ctx))
	// Simulate the old minimum's callback already waiting on mu during Cancel.
	s.promoteDelayed()
	s.mu.Lock()
	delayed, queued, currentTimer := len(s.delayed), s.queue.Len(), s.timer
	s.mu.Unlock()
	require.Equal(t, 1, delayed)
	require.Zero(t, queued)
	require.Same(t, timer, currentTimer)
	require.EqualValues(t, 2, executions.Load())
	handles[1].Cancel()
	require.NoError(t, handles[1].Wait(ctx))
	s.promoteDelayed() // The empty heap must not resurrect canceled tasks.
	s.Close()
	s.promoteDelayed() // Nor may a callback queued before Close restart work.
}

func TestConcurrentCancelCloseAndTimerCallback(t *testing.T) {
	s := New(2)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var executions atomic.Int32
	handles := make([]TaskHandle, 0, 16)
	for range 16 {
		handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
			executions.Add(1)
			return DelayUntil(time.Now().Add(time.Hour))
		})))
	}
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.delayed) == len(handles)
	}, time.Second, time.Millisecond)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, action := range []func(){
		func() {
			for _, handle := range handles {
				handle.Cancel()
			}
		},
		s.Close,
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			action()
		}()
	}
	close(start)
	s.promoteDelayed()
	wg.Wait()
	for _, handle := range handles {
		require.NoError(t, handle.Wait(ctx))
	}
	s.mu.Lock()
	delayed, queued := len(s.delayed), s.queue.Len()
	s.mu.Unlock()
	require.Zero(t, delayed)
	require.Zero(t, queued)
	require.EqualValues(t, len(handles), executions.Load())
}
