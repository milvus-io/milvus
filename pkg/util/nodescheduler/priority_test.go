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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPriorityOrderAndNonPreemption(t *testing.T) {
	s := New(1)
	defer s.Close()
	started, release := make(chan struct{}), make(chan struct{})
	first := s.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})).(PriorityTaskHandle)
	<-started
	order := make(chan int, 4)
	submit := func(id int, p int32) PriorityTaskHandle {
		return s.Submit(TaskFunc(func(context.Context) error { order <- id; return nil }), WithPriority(p)).(PriorityTaskHandle)
	}
	low := submit(1, -10)
	high := submit(2, 10)
	same := submit(3, 10)
	canceled := submit(4, 20)
	canceled.Cancel()
	require.True(t, low.SetPriority(20))
	require.True(t, first.SetPriority(-20))
	require.Empty(t, order, "priority changes must not interrupt the running task")
	close(release)
	for _, h := range []TaskHandle{first, low, high, same, canceled} {
		require.NoError(t, h.Wait(t.Context()))
	}
	require.Equal(t, 1, <-order)
	require.Equal(t, 2, <-order)
	require.Equal(t, 3, <-order)
	require.False(t, low.SetPriority(20))
	require.False(t, canceled.SetPriority(20))
	s.Close()
	h := submit(5, 0)
	require.False(t, h.SetPriority(10))
}

func TestPriorityFairnessAndDelayedDeadline(t *testing.T) {
	s := New(1)
	defer s.Close()
	started, release := make(chan struct{}), make(chan struct{})
	s.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))
	<-started
	order := make(chan int, priorityBurst*2+1)
	handles := []TaskHandle{s.Submit(TaskFunc(func(context.Context) error { order <- -1; return nil }), WithPriority(-20))}
	for i := 0; i < priorityBurst*2; i++ {
		handles = append(handles, s.Submit(TaskFunc(func(context.Context) error { order <- 1; return nil }), WithPriority(10)))
	}
	close(release)
	for _, h := range handles {
		require.NoError(t, h.Wait(t.Context()))
	}
	found := false
	for i := 0; i <= priorityBurst; i++ {
		if <-order == -1 {
			found = true
			break
		}
	}
	require.True(t, found, "oldest runnable task must receive a bounded dispatch opportunity")

	deadline := time.Now().Add(time.Hour)
	attempt := 0
	h := s.Submit(TaskFunc(func(context.Context) error { attempt++; return DelayUntil(deadline) })).(PriorityTaskHandle)
	barrier := s.Submit(TaskFunc(func(context.Context) error { return nil }))
	require.NoError(t, barrier.Wait(t.Context()))
	require.True(t, h.SetPriority(20))
	s.mu.Lock()
	require.Len(t, s.delayed, 1)
	require.Equal(t, deadline, s.delayed[0].nextRun)
	require.EqualValues(t, 20, s.delayed[0].priority)
	s.mu.Unlock()
	h.Cancel()
	require.NoError(t, h.Wait(t.Context()))
	require.Equal(t, 1, attempt)
}

// Occupy the sole worker so assertions do not depend on goroutine scheduling.
func blockPriorityWorker(t *testing.T, s *nodeScheduler) func() {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	s.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	return func() { close(release) }
}

func TestPrioritySignedRangeAndStableUpdates(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(map[bool]string{false: "signed-range", true: "update-preserves-age"}[update], func(t *testing.T) {
			s := New(1)
			defer s.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			release := blockPriorityWorker(t, s)
			priorities := []int32{MinPriority, -1, 0, MaxPriority, MaxPriority, -1}
			order := make(chan int, len(priorities))
			handles := make([]PriorityTaskHandle, 0, len(priorities))
			for i, priority := range priorities {
				handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
					order <- i
					return nil
				}), WithPriority(priority)).(PriorityTaskHandle))
			}
			want := []int{3, 4, 2, 1, 5, 0}
			if update {
				// Exercise both directions of heap.Fix, preserving the original
				// ready order when an entry returns to a tied priority.
				require.True(t, handles[0].SetPriority(MaxPriority))
				require.True(t, handles[3].SetPriority(MinPriority))
				require.True(t, handles[3].SetPriority(MaxPriority))
				require.True(t, handles[4].SetPriority(MaxPriority))
				want = []int{0, 3, 4, 2, 1, 5}
			}
			release()
			for _, h := range handles {
				require.NoError(t, h.Wait(ctx))
			}
			for _, expected := range want {
				require.Equal(t, expected, <-order)
			}
		})
	}
}

func TestPriorityRunningUpdateAppliesToRetry(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var attempts int
	order := make(chan string, 2)
	first := s.Submit(TaskFunc(func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return DelayUntil(time.Now().Add(-time.Second))
		}
		order <- "retry"
		return nil
	}), WithPriority(-20)).(PriorityTaskHandle)
	<-started
	other := s.Submit(TaskFunc(func(context.Context) error { order <- "other"; return nil }), WithPriority(10))
	require.True(t, first.SetPriority(20))
	require.Empty(t, order)
	close(release)
	require.NoError(t, first.Wait(ctx))
	require.NoError(t, other.Wait(ctx))
	require.Equal(t, "retry", <-order)
	require.Equal(t, "other", <-order)
	require.Equal(t, 2, attempts)
}

func TestPriorityDelayedUpdateAppliesAfterDeadline(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var attempts int
	var retriedAt time.Time
	firstAttempt := make(chan struct{})
	finishFirstAttempt := make(chan struct{})
	deadline := time.Now().Add(100 * time.Millisecond)
	order := make(chan string, 2)
	retry := s.Submit(TaskFunc(func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			close(firstAttempt)
			select {
			case <-finishFirstAttempt:
			case <-ctx.Done():
			}
			return DelayUntil(deadline)
		}
		retriedAt = time.Now()
		order <- "retry"
		return nil
	}), WithPriority(-20)).(PriorityTaskHandle)
	<-firstAttempt
	// Queue the blocker before allowing the first attempt to finish. Its higher
	// priority keeps it ahead of the retry even if the deadline has elapsed.
	blockerStarted := make(chan struct{})
	release := make(chan struct{})
	s.Submit(TaskFunc(func(ctx context.Context) error {
		close(blockerStarted)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))
	close(finishFirstAttempt)
	select {
	case <-blockerStarted:
	case <-ctx.Done():
		t.Fatal("blocker did not start")
	}
	require.True(t, retry.SetPriority(20))
	other := s.Submit(TaskFunc(func(context.Context) error { order <- "other"; return nil }), WithPriority(10))
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.delayed) == 0 && len(s.ready) == 2
	}, 5*time.Second, time.Millisecond)
	require.Empty(t, order)
	close(release)
	require.NoError(t, retry.Wait(ctx))
	require.NoError(t, other.Wait(ctx))
	require.False(t, retriedAt.Before(deadline))
	require.Equal(t, "retry", <-order)
	require.Equal(t, "other", <-order)
}

func TestPriorityFairnessWithContinuousArrivals(t *testing.T) {
	s := New(1)
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release := blockPriorityWorker(t, s)
	var highRuns atomic.Int32
	var lowRuns atomic.Int32
	counts := make(chan int32, 3)
	handles := make([]PriorityTaskHandle, 0, 3)
	for range 3 {
		handles = append(handles, s.Submit(TaskFunc(func(context.Context) error {
			counts <- highRuns.Load()
			lowRuns.Add(1)
			return nil
		}), WithPriority(MinPriority)).(PriorityTaskHandle))
	}
	// Changing priority must not erase age even across multiple fairness turns.
	require.True(t, handles[0].SetPriority(-1))
	require.True(t, handles[0].SetPriority(MinPriority))
	var high TaskFunc
	high = func(context.Context) error {
		highRuns.Add(1)
		if lowRuns.Load() < 3 {
			s.Submit(high, WithPriority(MaxPriority))
		}
		return nil
	}
	s.Submit(high, WithPriority(MaxPriority))
	release()
	for i, h := range handles {
		require.NoError(t, h.Wait(ctx))
		require.LessOrEqual(t, <-counts, int32((i+1)*priorityBurst))
	}
}

func TestPriorityUpdateAfterFinishIgnoresStaleCallback(t *testing.T) {
	s := New(1)
	defer s.Close()
	release := blockPriorityWorker(t, s)
	h := s.Submit(TaskFunc(func(context.Context) error { return nil })).(*taskHandle)
	h.mu.Lock()
	update := h.setPriority
	h.mu.Unlock()
	h.Cancel()
	require.False(t, update(100))
	require.False(t, h.SetPriority(20))
	release()
}
