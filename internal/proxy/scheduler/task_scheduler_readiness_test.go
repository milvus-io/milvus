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

package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type readinessDMLTask struct {
	*mockDmlTask
	pre     func(context.Context) error
	execute func(context.Context) error
	done    chan error
}

func newReadinessDMLTask(ctx context.Context) *readinessDMLTask {
	return &readinessDMLTask{mockDmlTask: newMockDmlTask(ctx), done: make(chan error, 1)}
}

func (t *readinessDMLTask) PreExecute(ctx context.Context) error {
	if t.pre != nil {
		return t.pre(ctx)
	}
	return nil
}

func (t *readinessDMLTask) Execute(ctx context.Context) error {
	if t.execute != nil {
		return t.execute(ctx)
	}
	return nil
}

func (t *readinessDMLTask) Notify(err error) {
	t.mockDmlTask.Notify(err)
	t.done <- err
}

func startReadinessDMLScheduler(t *testing.T) *TaskScheduler {
	t.Helper()
	paramtable.Init()
	cfg := paramtable.Get()
	previous := cfg.ProxyCfg.MaxTaskNum.GetValue()
	require.NoError(t, cfg.Save(cfg.ProxyCfg.MaxTaskNum.Key, "1"))
	t.Cleanup(func() { require.NoError(t, cfg.Save(cfg.ProxyCfg.MaxTaskNum.Key, previous)) })
	sched, err := NewTaskScheduler(context.Background(), newMockTsoAllocator())
	require.NoError(t, err)
	sched.wg.Add(1)
	go sched.manipulationLoop()
	t.Cleanup(sched.Close)
	return sched
}

func awaitReadinessDML[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("DML scheduler failed to make progress")
		var zero T
		return zero
	}
}

func TestDMLReadRetryBudgetAndCancellation(t *testing.T) {
	sched := startReadinessDMLScheduler(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	started := make(chan struct{})
	cold := newReadinessDMLTask(ctx)
	cold.pre = func(ctx context.Context) error {
		return taskmodel.RunDMLReadRetry(ctx, func(ctx context.Context, read func(context.Context) error) error {
			close(started)
			<-ctx.Done()
			return context.Cause(ctx)
		}, func(context.Context) error { t.Error("canceled readiness reached the read"); return nil })
	}
	cold.execute = func(context.Context) error { t.Error("canceled readiness reached the mutation"); return nil }
	require.NoError(t, sched.DmQueue.Enqueue(cold))
	awaitReadinessDML(t, started)

	var retries atomic.Int32
	makeRead := func() *readinessDMLTask {
		task := newReadinessDMLTask(t.Context())
		task.pre = func(ctx context.Context) error {
			return taskmodel.RunDMLReadRetry(ctx, func(ctx context.Context, read func(context.Context) error) error {
				retries.Add(1)
				return read(ctx)
			}, func(context.Context) error { return nil })
		}
		return task
	}
	overflow := makeRead()
	require.NoError(t, sched.DmQueue.Enqueue(overflow))
	require.ErrorIs(t, awaitReadinessDML(t, overflow.done), merr.ErrServiceTooManyRequests)
	require.Zero(t, retries.Load(), "overload must fail before issuing another readiness RPC")

	ordinary := newReadinessDMLTask(t.Context())
	require.NoError(t, sched.DmQueue.Enqueue(ordinary))
	require.NoError(t, awaitReadinessDML(t, ordinary.done))
	cause := merr.WrapErrServiceUnavailableMsg("operator stopped the request")
	cancel(cause)
	require.ErrorIs(t, awaitReadinessDML(t, cold.done), cause)

	// Cancellation restores the separate read budget as well as execution capacity.
	after := makeRead()
	require.NoError(t, sched.DmQueue.Enqueue(after))
	require.NoError(t, awaitReadinessDML(t, after.done))
	require.EqualValues(t, 1, retries.Load())
}

func TestDMLReadRetryReacquiresSlotAndStopsOnCancellation(t *testing.T) {
	for _, stage := range []string{"read", "continuation"} {
		for _, stop := range []string{"cancel", "shutdown"} {
			t.Run(stage+"/"+stop, func(t *testing.T) {
				sched := startReadinessDMLScheduler(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				waiting, ready, retryReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var readCalls, writes atomic.Int32
				cold := newReadinessDMLTask(ctx)
				cold.pre = func(ctx context.Context) error {
					return taskmodel.RunDMLReadRetry(ctx, func(ctx context.Context, read func(context.Context) error) error {
						if stage == "continuation" {
							if err := read(ctx); err != nil {
								return err
							}
						}
						close(waiting)
						select {
						case <-ready:
						case <-ctx.Done():
							return context.Cause(ctx)
						}
						close(retryReturned)
						if stage == "read" {
							return read(ctx)
						}
						return nil
					}, func(context.Context) error { readCalls.Add(1); return nil })
				}
				cold.execute = func(context.Context) error { writes.Add(1); return nil }
				require.NoError(t, sched.DmQueue.Enqueue(cold))
				awaitReadinessDML(t, waiting)

				busy, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				other := newReadinessDMLTask(t.Context())
				other.execute = func(context.Context) error { close(busy); <-release; return nil }
				require.NoError(t, sched.DmQueue.Enqueue(other))
				awaitReadinessDML(t, busy)
				close(ready)
				awaitReadinessDML(t, retryReturned)
				if stop == "cancel" {
					cancel()
				} else {
					closed := make(chan struct{})
					go func() { sched.Close(); close(closed) }()
					awaitReadinessDML(t, closed)
				}
				require.ErrorIs(t, awaitReadinessDML(t, cold.done), context.Canceled)
				require.Zero(t, writes.Load(), "a task without a slot must not continue to its mutation")
				if stage == "read" {
					require.Zero(t, readCalls.Load())
				} else {
					require.EqualValues(t, 1, readCalls.Load())
				}
			})
		}
	}
}

func TestDMLReadRetryDuringExecuteAndShutdown(t *testing.T) {
	sched := startReadinessDMLScheduler(t)
	waiting := make(chan struct{})
	task := newReadinessDMLTask(t.Context())
	var reads atomic.Int32
	read := func(context.Context) error { reads.Add(1); return nil }
	task.pre = func(ctx context.Context) error {
		return taskmodel.RunDMLReadRetry(ctx, func(ctx context.Context, read func(context.Context) error) error { return read(ctx) }, read)
	}
	// A CAS retry uses the same task's Execute context after its first read.
	task.execute = func(ctx context.Context) error {
		return taskmodel.RunDMLReadRetry(ctx, func(ctx context.Context, read func(context.Context) error) error {
			close(waiting)
			<-ctx.Done()
			return context.Cause(ctx)
		}, read)
	}
	require.NoError(t, sched.DmQueue.Enqueue(task))
	awaitReadinessDML(t, waiting)
	other := newReadinessDMLTask(t.Context())
	require.NoError(t, sched.DmQueue.Enqueue(other))
	require.NoError(t, awaitReadinessDML(t, other.done))
	sched.Close()
	require.ErrorIs(t, awaitReadinessDML(t, task.done), context.Canceled)
	require.EqualValues(t, 1, reads.Load())
}
