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

package recovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestScopedTaskSchedulerWaitIdle(t *testing.T) {
	inner := nodescheduler.New(2)
	defer inner.Close()

	scheduler := newScopedTaskScheduler(inner)
	releaseTracked := make(chan struct{})
	scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		<-releaseTracked
		return nil
	}))

	releaseUnrelated := make(chan struct{})
	unrelated := inner.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		<-releaseUnrelated
		return nil
	}))

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, scheduler.WaitIdle(waitCtx), context.DeadlineExceeded)

	close(releaseTracked)
	require.NoError(t, scheduler.WaitIdle(context.Background()))
	close(releaseUnrelated)
	require.NoError(t, unrelated.Wait(context.Background()))
}

func TestScopedTaskSchedulerTracksDelayedTaskUntilSuccess(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)

	ready := atomic.Bool{}
	scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		if !ready.Load() {
			return nodescheduler.ErrDelay
		}
		return nil
	}))

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, scheduler.WaitIdle(waitCtx), context.DeadlineExceeded)
	ready.Store(true)
	require.NoError(t, scheduler.WaitIdle(context.Background()))
}

func TestScopedTaskSchedulerUsesNodeConcurrency(t *testing.T) {
	// One WAL can use all available workers for the highest-priority band,
	// including more than the former per-PChannel default of 16.
	const concurrency = 20
	inner := nodescheduler.New(concurrency)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)
	defer scheduler.Close()
	started := make(chan struct{}, concurrency)
	for range concurrency {
		scheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}), nodescheduler.WithPriority(nodescheduler.MaxPriority))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range concurrency {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("WAL tasks did not use all available node scheduler workers")
		}
	}
}

func TestScopedTaskSchedulerCloseCancelsDelayedTask(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)

	started := make(chan struct{})
	scheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return errors.Mark(ctx.Err(), nodescheduler.ErrDelay)
	}))
	<-started

	scheduler.Close()
	require.NoError(t, scheduler.WaitIdle(context.Background()))
}

type nodeschedulerTaskFunc func(context.Context) error

func (f nodeschedulerTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

func TestScopedTaskSchedulerDelegatesDelayedRetry(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)
	defer scheduler.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	var attempts []time.Time
	var order []string
	first := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		attempts = append(attempts, time.Now())
		order = append(order, "retry")
		if len(attempts) == 1 {
			close(started)
			<-release
		}
		if len(attempts) <= 2 {
			return nodescheduler.ErrDelay
		}
		return nil
	}), nodescheduler.WithBackoff(typeutil.BackoffConfig{
		InitialInterval: 150 * time.Millisecond,
		Multiplier:      2,
		MaxInterval:     600 * time.Millisecond,
	}))
	<-started
	second := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		order = append(order, "other")
		return nil
	}))
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, first.Wait(ctx))
	require.NoError(t, second.Wait(ctx))
	require.Len(t, attempts, 3)
	require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 150*time.Millisecond)
	require.GreaterOrEqual(t, attempts[2].Sub(attempts[1]), 300*time.Millisecond)
	require.Equal(t, []string{"retry", "other", "retry", "retry"}, order)
	require.NoError(t, scheduler.WaitIdle(ctx))
}

func TestScopedTaskSchedulerCancelsDelayedTask(t *testing.T) {
	for _, closeScheduler := range []bool{false, true} {
		name := "cancel"
		if closeScheduler {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			inner := nodescheduler.New(1)
			defer inner.Close()
			scheduler := newScopedTaskScheduler(inner)
			defer scheduler.Close()
			attempts := atomic.Int32{}
			handle := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
				attempts.Add(1)
				return nodescheduler.ErrDelay
			}))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			// A task on the same worker proves the first attempt returned and
			// NodeScheduler is now responsible for its delayed retry.
			require.NoError(t, inner.Submit(nodeschedulerTaskFunc(func(context.Context) error {
				return nil
			})).Wait(ctx))
			require.Positive(t, attempts.Load())
			if closeScheduler {
				scheduler.Close()
			} else {
				handle.Cancel()
			}
			require.NoError(t, handle.Wait(ctx))
			require.NoError(t, scheduler.WaitIdle(ctx))
			count := attempts.Load()
			require.Never(t, func() bool {
				return attempts.Load() != count
			}, 150*time.Millisecond, time.Millisecond)
			// Closing one WAL must not close the shared executor.
			require.NoError(t, inner.Submit(nodeschedulerTaskFunc(func(context.Context) error {
				return nil
			})).Wait(ctx))
		})
	}
}

func TestScopedTaskSchedulerCancelQueuedTask(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)
	defer scheduler.Close()
	release := make(chan struct{})
	unrelated := inner.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		<-release
		return nil
	}))
	ran := atomic.Bool{}
	handle := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		ran.Store(true)
		return nil
	}))
	handle.Cancel()
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, handle.Wait(ctx))
	require.NoError(t, scheduler.WaitIdle(ctx))
	require.NoError(t, unrelated.Wait(ctx))
	require.False(t, ran.Load())
}

func TestScopedTaskSchedulerSubmitAfterClose(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)
	scheduler.Close()
	ran := atomic.Bool{}
	handle := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		ran.Store(true)
		return nil
	}))
	handle.Cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, handle.Wait(ctx))
	require.NoError(t, scheduler.WaitIdle(ctx))
	require.False(t, ran.Load())
}

func TestScopedTaskSchedulerPriority(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)
	defer scheduler.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	running := scheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})).(nodescheduler.PriorityTaskHandle)
	<-started
	order := make(chan int, 3)
	submit := func(id int, priority int32) nodescheduler.PriorityTaskHandle {
		return scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
			order <- id
			return nil
		}), nodescheduler.WithPriority(priority)).(nodescheduler.PriorityTaskHandle)
	}
	low, high, canceled := submit(1, -10), submit(2, 10), submit(3, 20)
	require.True(t, low.SetPriority(20))
	require.True(t, running.SetPriority(-20))
	canceled.Cancel()
	require.NoError(t, canceled.Wait(ctx))
	require.False(t, canceled.SetPriority(20))
	require.Empty(t, order)
	close(release)
	for _, h := range []nodescheduler.TaskHandle{running, low, high} {
		require.NoError(t, h.Wait(ctx))
	}
	require.Equal(t, 1, <-order)
	require.Equal(t, 2, <-order)
	require.False(t, low.SetPriority(20))
	require.NoError(t, scheduler.WaitIdle(ctx))
	scheduler.Close()
	require.False(t, submit(4, 10).SetPriority(20))
	// The scoped owner still does not own or shut down the shared workers.
	require.NoError(t, inner.Submit(nodeschedulerTaskFunc(func(context.Context) error { return nil })).Wait(ctx))
}

func TestScopedTaskHandlePriorityWithoutInnerSupport(t *testing.T) {
	// An open scoped entry may wrap a legacy TaskHandle implementation. The
	// optional extension must reject the update without affecting its lifecycle.
	inner := nodescheduler.New(1)
	defer inner.Close()
	legacy := struct{ nodescheduler.TaskHandle }{
		inner.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})),
	}
	handle := scopedTaskHandle{entry: &scopedTaskEntry{inner: legacy, done: make(chan struct{})}}
	require.False(t, handle.SetPriority(10))
	legacy.Cancel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, legacy.Wait(ctx))
}

func TestScopedTaskSchedulerPriorityRetry(t *testing.T) {
	inner := nodescheduler.New(1)
	defer inner.Close()
	scheduler := newScopedTaskScheduler(inner)
	defer scheduler.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	order := make(chan string, 2)
	var attempts int
	retry := scheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nodescheduler.DelayUntil(time.Now().Add(-time.Second))
		}
		order <- "retry"
		return nil
	}), nodescheduler.WithPriority(-10)).(nodescheduler.PriorityTaskHandle)
	<-started
	other := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		order <- "other"
		return nil
	}))
	require.True(t, retry.SetPriority(10))
	close(release)
	require.NoError(t, retry.Wait(ctx))
	require.NoError(t, other.Wait(ctx))
	require.NoError(t, scheduler.WaitIdle(ctx))
	require.Equal(t, "retry", <-order)
	require.Equal(t, "other", <-order)
	require.Equal(t, 2, attempts)
}
