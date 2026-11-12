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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type queryLoopTestTask struct {
	*mockDqlTask
	subTask          bool
	preExecute       func()
	execute          func(context.Context) error
	preExecuteCalls  atomic.Int32
	executeCalls     atomic.Int32
	postExecuteCalls atomic.Int32
	notified         chan error
}

func (t *queryLoopTestTask) IsSubTask() bool {
	return t.subTask
}

func (t *queryLoopTestTask) PreExecute(context.Context) error {
	t.preExecuteCalls.Add(1)
	if t.preExecute != nil {
		t.preExecute()
	}
	return nil
}

func (t *queryLoopTestTask) Execute(ctx context.Context) error {
	t.executeCalls.Add(1)
	if t.execute != nil {
		return t.execute(ctx)
	}
	return nil
}

func (t *queryLoopTestTask) PostExecute(context.Context) error {
	t.postExecuteCalls.Add(1)
	return nil
}

func (t *queryLoopTestTask) Notify(err error) {
	t.mockDqlTask.Notify(err)
	t.notified <- err
}

func newSingleWorkerDQLScheduler(t *testing.T) *TaskScheduler {
	t.Helper()
	params := paramtable.Get()
	key := params.ProxyCfg.MaxTaskNum.Key
	previous := params.ProxyCfg.MaxTaskNum.GetValue()
	require.NoError(t, params.Save(key, "1"))
	t.Cleanup(func() { require.NoError(t, params.Save(key, previous)) })

	sched, err := NewTaskScheduler(t.Context(), newMockTsoAllocator())
	require.NoError(t, err)
	t.Cleanup(sched.Close)
	return sched
}

func dqlMetrics(sched *TaskScheduler) metricsinfo.TaskQueueMetrics {
	for _, metric := range sched.GetMetrics() {
		if metric.Type == "dql" {
			return metric
		}
	}
	return metricsinfo.TaskQueueMetrics{}
}

func TestTaskScheduler_NestedDQLMakesProgress(t *testing.T) {
	sched := newSingleWorkerDQLScheduler(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// Leave room for a waiting Search and one nested task to exercise dispatch
	// independently of the admission limit.
	sched.DqQueue.SetMaxTaskNum(2)
	require.NoError(t, sched.Start())

	started := make(chan struct{})
	launchNested := make(chan struct{})
	executing := make(chan int64, 2)
	parent := &queryLoopTestTask{
		mockDqlTask: newMockDqlTask(ctx),
		notified:    make(chan error, 1),
		execute: func(ctx context.Context) error {
			close(started)
			select {
			case <-launchNested:
			case <-ctx.Done():
				return ctx.Err()
			}
			for _, name := range []string{taskmodel.QueryTaskName, "Highlight"} {
				task := &queryLoopTestTask{
					mockDqlTask: newMockDqlTask(ctx),
					subTask:     true,
					notified:    make(chan error, 1),
					execute: func(context.Context) error {
						executing <- dqlMetrics(sched).ExecutingCount
						return nil
					},
				}
				task.name = name
				if err := sched.DqQueue.Enqueue(task); err != nil {
					return err
				}
				if err := task.WaitToFinish(); err != nil {
					return err
				}
			}
			return nil
		},
	}
	parent.name = taskmodel.SearchTaskName
	require.NoError(t, sched.DqQueue.Enqueue(parent))
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Search did not start")
	}

	waiting := newMockDqlTask(ctx)
	waiting.name = taskmodel.SearchTaskName
	require.NoError(t, sched.DqQueue.Enqueue(waiting))
	close(launchNested)

	require.NoError(t, parent.WaitToFinish(), "Search waiting for requery and highlight must finish")
	require.NoError(t, waiting.WaitToFinish(), "waiting Search must also finish")
	require.Len(t, executing, 2)
	require.EqualValues(t, 2, <-executing, "count the Search and its running requery")
	require.EqualValues(t, 2, <-executing, "count the Search and its running highlight")
}

func TestTaskScheduler_NestedDQLAdmissionAndClear(t *testing.T) {
	sched := newSingleWorkerDQLScheduler(t)
	queue := sched.DqQueue
	search := newMockDqlTask(t.Context())
	search.name = taskmodel.SearchTaskName
	requery := &queryLoopTestTask{
		mockDqlTask: newMockDqlTask(t.Context()),
		subTask:     true,
		notified:    make(chan error, 1),
	}
	requery.name = taskmodel.QueryTaskName
	highlight := &queryLoopTestTask{
		mockDqlTask: newMockDqlTask(t.Context()),
		subTask:     true,
		notified:    make(chan error, 1),
	}
	highlight.name = "Highlight"

	require.NoError(t, queue.Enqueue(requery))
	require.False(t, queue.IsFull(), "a full nested queue must allow ordinary Search")
	require.NoError(t, queue.Enqueue(search))
	require.True(t, queue.IsFull())
	require.ErrorIs(t, queue.Enqueue(newDefaultMockDqlTask()), merr.ErrServiceTooManyRequests)
	require.ErrorIs(t, queue.Enqueue(highlight), merr.ErrServiceTooManyRequests)
	require.EqualValues(t, 2, dqlMetrics(sched).PendingCount)

	cleared := sched.ClearDQLQueue("query", "test")
	require.EqualValues(t, 1, cleared.QueuedCleared)
	require.ErrorIs(t, requery.WaitToFinish(), context.Canceled)
	require.True(t, queue.IsFull(), "clearing requery must leave the waiting Search")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := &queryLoopTestTask{
		mockDqlTask: newMockDqlTask(ctx),
		subTask:     true,
		notified:    make(chan error, 1),
	}
	require.NoError(t, queue.Enqueue(canceled))
	cancel()
	require.ErrorIs(t, canceled.WaitToFinish(), context.Canceled)
	require.NoError(t, queue.Enqueue(highlight), "a full main queue must allow highlight")

	cleared = sched.ClearDQLQueue("all", "")
	require.EqualValues(t, 2, cleared.QueuedCleared)
	require.ErrorIs(t, search.WaitToFinish(), context.Canceled)
	require.ErrorIs(t, highlight.WaitToFinish(), context.Canceled)
	require.Zero(t, dqlMetrics(sched).PendingCount)
	require.False(t, queue.IsFull())
}

func TestTaskScheduler_QueryLoopContextDoneBeforeExecution(t *testing.T) {
	for _, subTask := range []bool{false, true} {
		poolName := "main pool"
		if subTask {
			poolName = "subtask pool"
		}
		t.Run(poolName, func(t *testing.T) {
			for _, outcome := range []string{"canceled", "deadline exceeded", "live"} {
				t.Run(outcome, func(t *testing.T) {
					sched := newSingleWorkerDQLScheduler(t)
					release := make(chan struct{})
					var releaseOnce sync.Once
					releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
					t.Cleanup(releaseWorker)
					require.NoError(t, sched.Start())

					started := make(chan struct{})
					blockingTask := &queryLoopTestTask{
						mockDqlTask: newDefaultMockDqlTask(),
						subTask:     subTask,
						preExecute: func() {
							close(started)
							<-release
						},
						notified: make(chan error, 1),
					}
					require.NoError(t, sched.DqQueue.Enqueue(blockingTask))
					select {
					case <-started:
					case <-time.After(5 * time.Second):
						t.Fatal("first task did not occupy the worker")
					}

					ctx, cancel := context.WithCancel(context.Background())
					if outcome == "deadline exceeded" {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					}
					t.Cleanup(cancel)
					task := &queryLoopTestTask{
						mockDqlTask: newMockDqlTask(ctx),
						subTask:     subTask,
						notified:    make(chan error, 1),
					}
					require.NoError(t, sched.DqQueue.Enqueue(task))

					require.Eventually(t, func() bool {
						// The running task still owns the only worker in this lane.
						metrics := dqlMetrics(sched)
						return metrics.Type == "dql" && metrics.PendingCount == 0
					}, 5*time.Second, time.Millisecond)

					var expectedErr error
					switch outcome {
					case "canceled":
						require.NoError(t, ctx.Err(), "cancel only after Submit starts waiting")
						cancel()
						expectedErr = context.Canceled
					case "deadline exceeded":
						<-ctx.Done()
						expectedErr = context.DeadlineExceeded
					}
					if expectedErr != nil {
						require.ErrorIs(t, task.WaitToFinish(), expectedErr)
					}
					releaseWorker()

					select {
					case err := <-task.notified:
						require.ErrorIs(t, err, expectedErr)
					case <-time.After(5 * time.Second):
						t.Fatal("submitted task was not notified")
					}
					var expectedCalls int32
					if expectedErr == nil {
						expectedCalls = 1
						require.NoError(t, task.WaitToFinish())
					}
					require.Equal(t, expectedCalls, task.preExecuteCalls.Load())
					require.Equal(t, expectedCalls, task.executeCalls.Load())
					require.Equal(t, expectedCalls, task.postExecuteCalls.Load())
					require.Eventually(t, func() bool {
						return sched.DqQueue.getTaskByReqID(task.ID()) == nil
					}, 5*time.Second, time.Millisecond)
				})
			}
		})
	}
}
