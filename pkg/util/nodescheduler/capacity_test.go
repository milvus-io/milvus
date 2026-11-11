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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPriorityBounds(t *testing.T) {
	s := New(1)
	defer s.Close()
	h := s.Submit(TaskFunc(func(context.Context) error { return nil })).(PriorityTaskHandle)
	for _, value := range []int32{MinPriority - 1, MaxPriority + 1} {
		require.Panics(t, func() { WithPriority(value) })
		require.Panics(t, func() { h.SetPriority(value) })
	}
}

// Keep attempts alive so admission counts cannot be hidden by fast completion.
func holdCapacity(t *testing.T, s *nodeScheduler, priority int32) (PriorityTaskHandle, <-chan struct{}, func()) {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	h := s.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}), WithPriority(priority)).(PriorityTaskHandle)
	return h, started, func() { close(release) }
}

func awaitCapacityStart(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("eligible task did not start")
	}
}

func TestCumulativeWorkerLimits(t *testing.T) {
	s := New(20)
	defer s.Close()
	var blocked []<-chan struct{}
	for _, band := range []struct {
		priority int32
		count    int
	}{{0, 10}, {5, 5}, {10, 3}, {20, 2}} {
		for range band.count {
			_, started, _ := holdCapacity(t, s, band.priority)
			awaitCapacityStart(t, started)
		}
		_, started, _ := holdCapacity(t, s, band.priority)
		blocked = append(blocked, started)
	}
	for _, started := range blocked {
		select {
		case <-started:
			t.Fatal("task exceeded its cumulative worker limit")
		default:
		}
	}
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	require.Equal(t, [priorityBands]int{10, 15, 18, 20}, running)
}

func TestPriorityChangePreservesRunningAdmission(t *testing.T) {
	s := New(4)
	defer s.Close()
	first, started, release := holdCapacity(t, s, 0)
	awaitCapacityStart(t, started)
	_, started, _ = holdCapacity(t, s, 0)
	awaitCapacityStart(t, started)
	queued, promoted, _ := holdCapacity(t, s, 0)
	_, low, _ := holdCapacity(t, s, -1)
	require.True(t, first.SetPriority(20))
	require.True(t, queued.SetPriority(5))
	awaitCapacityStart(t, promoted)
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	require.Equal(t, [priorityBands]int{2, 3, 3, 3}, running,
		"running priority updates must not free the original band's slot")
	release()
	awaitCapacityStart(t, low)
}

func TestFairnessSkipsCapacityBlockedTasks(t *testing.T) {
	s := New(4)
	defer s.Close()
	for range 2 {
		_, started, _ := holdCapacity(t, s, 0)
		awaitCapacityStart(t, started)
	}
	_, low, _ := holdCapacity(t, s, -20)
	s.mu.Lock()
	s.priorityDispatches = priorityBurst
	s.mu.Unlock()
	_, high, _ := holdCapacity(t, s, 20)
	awaitCapacityStart(t, high)
	select {
	case <-low:
		t.Fatal("fairness bypassed the data worker limit")
	default:
	}
}

func TestCapacityResizeDoesNotPreempt(t *testing.T) {
	s := New(4)
	defer s.Close()
	first, started, releaseFirst := holdCapacity(t, s, 0)
	awaitCapacityStart(t, started)
	second, started, releaseSecond := holdCapacity(t, s, 0)
	awaitCapacityStart(t, started)
	_, queued, _ := holdCapacity(t, s, 0)
	s.resize(2)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	releaseFirst()
	require.NoError(t, first.Wait(ctx))
	select {
	case <-queued:
		t.Fatal("shrinking admitted another low-priority task above quota")
	default:
	}
	releaseSecond()
	require.NoError(t, second.Wait(ctx))
	awaitCapacityStart(t, queued)
	_, next, _ := holdCapacity(t, s, 0)
	s.resize(4)
	awaitCapacityStart(t, next)
}

func TestWorkerLimitRounding(t *testing.T) {
	for _, tc := range []struct {
		workers int
		limits  [priorityBands]int
	}{
		{1, [priorityBands]int{1, 1, 1, 1}},
		{2, [priorityBands]int{1, 1, 1, 2}},
		{3, [priorityBands]int{1, 2, 2, 3}},
		{10, [priorityBands]int{5, 7, 9, 10}},
	} {
		s := &nodeScheduler{concurrency: tc.workers}
		for band, limit := range tc.limits {
			for i := band; i < priorityBands; i++ {
				s.running[i] = limit - 1
			}
			require.True(t, s.canRunLocked(band))
			for i := band; i < priorityBands; i++ {
				s.running[i] = limit
			}
			require.False(t, s.canRunLocked(band))
			s.running = [priorityBands]int{}
		}
	}
}

func TestDelayedAttemptReleasesBandCapacity(t *testing.T) {
	s := New(4)
	defer s.Close()
	_, started, _ := holdCapacity(t, s, 0)
	awaitCapacityStart(t, started)
	entered, release := make(chan struct{}), make(chan struct{})
	delayed := s.Submit(TaskFunc(func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return DelayUntil(time.Now().Add(time.Hour))
	}))
	awaitCapacityStart(t, entered)
	_, next, _ := holdCapacity(t, s, 0)
	close(release)
	awaitCapacityStart(t, next)
	s.mu.Lock()
	running, delayedCount := s.running, len(s.delayed)
	s.mu.Unlock()
	require.Equal(t, [priorityBands]int{2, 2, 2, 2}, running)
	require.Equal(t, 1, delayedCount)
	delayed.Cancel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, delayed.Wait(ctx))
	s.Close()
	require.Empty(t, s.ready)
	for _, band := range s.oldest {
		require.Nil(t, band)
	}
	require.Equal(t, [priorityBands]int{}, s.running)
}
