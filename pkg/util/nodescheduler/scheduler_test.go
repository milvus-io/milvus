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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchedulerExecutesTasksInFIFOOrder(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	var mu sync.Mutex
	order := make([]int, 0, 3)
	handles := make([]TaskHandle, 0, 3)
	for i := 0; i < 3; i++ {
		value := i
		handles = append(handles, scheduler.Submit(TaskFunc(func(context.Context) error {
			mu.Lock()
			order = append(order, value)
			mu.Unlock()
			return nil
		})))
	}

	for _, handle := range handles {
		require.NoError(t, handle.Wait(context.Background()))
	}
	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestSchedulerMovesDelayedTaskToQueueTail(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	var mu sync.Mutex
	order := make([]string, 0, 3)
	firstStarted := make(chan struct{})
	allowDelay := make(chan struct{})
	var attempts atomic.Int32

	first := scheduler.Submit(TaskFunc(func(context.Context) error {
		attempt := attempts.Add(1)
		mu.Lock()
		order = append(order, fmt.Sprintf("first-%d", attempt))
		mu.Unlock()
		if attempt == 1 {
			close(firstStarted)
			<-allowDelay
			return errors.Mark(errors.New("not ready"), ErrDelay)
		}
		return nil
	}))

	<-firstStarted
	second := scheduler.Submit(TaskFunc(func(context.Context) error {
		mu.Lock()
		order = append(order, "second")
		mu.Unlock()
		return nil
	}))
	close(allowDelay)

	require.NoError(t, first.Wait(context.Background()))
	require.NoError(t, second.Wait(context.Background()))
	assert.Equal(t, []string{"first-1", "second", "first-2"}, order)
}

func TestSchedulerHonorsConcurrencyLimit(t *testing.T) {
	scheduler := New(2)
	defer scheduler.Close()

	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var running atomic.Int32
	var maxRunning atomic.Int32

	newTask := func() Task {
		return TaskFunc(func(ctx context.Context) error {
			current := running.Add(1)
			defer running.Add(-1)
			for {
				maximum := maxRunning.Load()
				if current <= maximum || maxRunning.CompareAndSwap(maximum, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		})
	}

	handles := []TaskHandle{
		scheduler.Submit(newTask()),
		scheduler.Submit(newTask()),
		scheduler.Submit(newTask()),
	}

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for task to start")
		}
	}
	select {
	case <-started:
		t.Fatal("third task started before a worker became available")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	for _, handle := range handles {
		require.NoError(t, handle.Wait(context.Background()))
	}
	assert.Equal(t, int32(2), maxRunning.Load())
}

func TestSchedulerResizeScalesUp(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	newTask := func() Task {
		return TaskFunc(func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		})
	}
	first := scheduler.Submit(newTask())
	second := scheduler.Submit(newTask())

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first task to start")
	}
	select {
	case <-started:
		t.Fatal("second task started before scheduler resize")
	case <-time.After(20 * time.Millisecond):
	}

	scheduler.resize(2)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scaled-up worker")
	}
	close(release)
	require.NoError(t, first.Wait(context.Background()))
	require.NoError(t, second.Wait(context.Background()))
}

func TestSchedulerResizeScalesDownBetweenTasks(t *testing.T) {
	scheduler := New(2)
	defer scheduler.Close()

	initialStarted := make(chan struct{}, 2)
	releaseInitial := make(chan struct{})
	initialTask := func() Task {
		return TaskFunc(func(context.Context) error {
			initialStarted <- struct{}{}
			<-releaseInitial
			return nil
		})
	}
	first := scheduler.Submit(initialTask())
	second := scheduler.Submit(initialTask())
	for i := 0; i < 2; i++ {
		select {
		case <-initialStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial task to start")
		}
	}

	scheduler.resize(1)
	close(releaseInitial)
	require.NoError(t, first.Wait(context.Background()))
	require.NoError(t, second.Wait(context.Background()))

	nextStarted := make(chan struct{}, 2)
	releaseNext := make(chan struct{})
	nextTask := func() Task {
		return TaskFunc(func(context.Context) error {
			nextStarted <- struct{}{}
			<-releaseNext
			return nil
		})
	}
	third := scheduler.Submit(nextTask())
	fourth := scheduler.Submit(nextTask())
	select {
	case <-nextStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for resized worker")
	}
	select {
	case <-nextStarted:
		t.Fatal("second task started after scheduler scaled down to one worker")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseNext)
	require.NoError(t, third.Wait(context.Background()))
	require.NoError(t, fourth.Wait(context.Background()))
}

func TestSchedulerResizeDoesNotOverProvisionPendingWorkers(t *testing.T) {
	scheduler := New(2)
	defer scheduler.Close()

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	newTask := func() Task {
		return TaskFunc(func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		})
	}
	first := scheduler.Submit(newTask())
	second := scheduler.Submit(newTask())
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for task to start")
		}
	}

	scheduler.resize(1)
	scheduler.resize(2)
	scheduler.mu.Lock()
	workerCount := scheduler.workerCount
	scheduler.mu.Unlock()
	assert.Equal(t, 2, workerCount)

	close(release)
	require.NoError(t, first.Wait(context.Background()))
	require.NoError(t, second.Wait(context.Background()))
}

func TestSchedulerAbandonsOrdinaryErrorAfterOneAttempt(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	var attempts atomic.Int32
	handle := scheduler.Submit(TaskFunc(func(context.Context) error {
		attempts.Add(1)
		return errors.New("business failure")
	}))

	require.NoError(t, handle.Wait(context.Background()))
	assert.Equal(t, int32(1), attempts.Load())
}

func TestSchedulerCancelsQueuedTask(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	release := make(chan struct{})
	firstStarted := make(chan struct{})
	first := scheduler.Submit(TaskFunc(func(context.Context) error {
		close(firstStarted)
		<-release
		return nil
	}))
	<-firstStarted

	var secondRan atomic.Bool
	second := scheduler.Submit(TaskFunc(func(context.Context) error {
		secondRan.Store(true)
		return nil
	}))
	second.Cancel()
	close(release)

	require.NoError(t, first.Wait(context.Background()))
	require.NoError(t, second.Wait(context.Background()))
	assert.False(t, secondRan.Load())
}

func TestSchedulerPassesCancellationToRunningTask(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	started := make(chan struct{})
	observed := make(chan error, 1)
	handle := scheduler.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		observed <- ctx.Err()
		return ctx.Err()
	}))
	<-started

	handle.Cancel()
	require.ErrorIs(t, <-observed, context.Canceled)
	require.NoError(t, handle.Wait(context.Background()))
}

func TestTaskHandleSupportsConcurrentWaiters(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	release := make(chan struct{})
	handle := scheduler.Submit(TaskFunc(func(context.Context) error {
		<-release
		return nil
	}))

	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			results <- handle.Wait(context.Background())
		}()
	}
	close(release)
	for i := 0; i < 8; i++ {
		require.NoError(t, <-results)
	}
}

func TestTaskHandleWaitReturnsWaitContextError(t *testing.T) {
	scheduler := New(1)
	defer scheduler.Close()

	release := make(chan struct{})
	handle := scheduler.Submit(TaskFunc(func(context.Context) error {
		<-release
		return nil
	}))
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, handle.Wait(waitCtx), context.DeadlineExceeded)
	close(release)
	require.NoError(t, handle.Wait(context.Background()))
}

func TestSchedulerCloseCancelsQueuedAndRunningTasks(t *testing.T) {
	scheduler := New(1)

	started := make(chan struct{})
	observedCancellation := make(chan struct{})
	running := scheduler.Submit(TaskFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(observedCancellation)
		return ctx.Err()
	}))
	<-started

	var queuedRan atomic.Bool
	queued := scheduler.Submit(TaskFunc(func(context.Context) error {
		queuedRan.Store(true)
		return nil
	}))

	scheduler.Close()
	require.NoError(t, running.Wait(context.Background()))
	require.NoError(t, queued.Wait(context.Background()))
	assert.False(t, queuedRan.Load())
	select {
	case <-observedCancellation:
	default:
		t.Fatal("running task did not observe scheduler cancellation")
	}
}

func TestSchedulerSubmitAfterCloseReturnsCompletedHandle(t *testing.T) {
	scheduler := New(1)
	scheduler.Close()

	var ran atomic.Bool
	handle := scheduler.Submit(TaskFunc(func(context.Context) error {
		ran.Store(true)
		return nil
	}))

	require.NoError(t, handle.Wait(context.Background()))
	assert.False(t, ran.Load())
}

func TestSchedulerRejectsNonPositiveConcurrency(t *testing.T) {
	require.Panics(t, func() { New(0) })
	require.Panics(t, func() { New(-1) })

	scheduler := New(1)
	defer scheduler.Close()
	require.Panics(t, func() { scheduler.resize(0) })
	require.Panics(t, func() { scheduler.resize(-1) })
}

func TestConcurrencyFromRatio(t *testing.T) {
	concurrency, ok := concurrencyFromRatio(8, 1)
	assert.True(t, ok)
	assert.Equal(t, 8, concurrency)

	concurrency, ok = concurrencyFromRatio(8, 0.5)
	assert.True(t, ok)
	assert.Equal(t, 4, concurrency)

	concurrency, ok = concurrencyFromRatio(8, 0.01)
	assert.True(t, ok)
	assert.Equal(t, 1, concurrency)

	_, ok = concurrencyFromRatio(8, 0)
	assert.False(t, ok)
	_, ok = concurrencyFromRatio(8, -1)
	assert.False(t, ok)
}

func TestGlobalSchedulerLazyInitializationAndDynamicResize(t *testing.T) {
	params := paramtable.Get()
	ratioParam := &params.CommonCfg.NodeSchedulerMaxConcurrencyRatio
	require.NoError(t, params.Reset(ratioParam.Key))

	first := Get()
	assert.Same(t, first, Get())
	scheduler := first.(*nodeScheduler)

	cpu := hardware.GetCPUNum()
	assert.Eventually(t, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return scheduler.concurrency == 2*cpu
	}, time.Second, 10*time.Millisecond)

	ratioForTwoWorkers := 2 / float64(cpu)
	require.NoError(t, params.Save(ratioParam.Key, strconv.FormatFloat(ratioForTwoWorkers, 'g', -1, 64)))
	assert.Eventually(t, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return scheduler.concurrency == 2
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, params.Save(ratioParam.Key, "0"))
	time.Sleep(20 * time.Millisecond)
	scheduler.mu.Lock()
	assert.Equal(t, 2, scheduler.concurrency)
	scheduler.mu.Unlock()

	require.NoError(t, params.Reset(ratioParam.Key))
	assert.Eventually(t, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return scheduler.concurrency == 2*cpu
	}, time.Second, 10*time.Millisecond)
}

type TaskFunc func(context.Context) error

func (f TaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}
