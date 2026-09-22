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
	// One WAL can use all available workers, including more than the former
	// per-PChannel default of 16.
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
		}))
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
			return nodescheduler.ErrDelay
		}
		return nil
	}))
	<-started
	second := scheduler.Submit(nodeschedulerTaskFunc(func(context.Context) error {
		order = append(order, "other")
		return nil
	}))
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, first.Wait(ctx))
	require.NoError(t, second.Wait(ctx))
	require.Len(t, attempts, 2)
	require.GreaterOrEqual(t, attempts[1].Sub(attempts[0]), 100*time.Millisecond)
	require.Equal(t, []string{"retry", "other", "retry"}, order)
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
