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

package proxy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/conc"
)

type queryLoopTestTask struct {
	*mockDqlTask
	subTask          bool
	preExecute       func()
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

func (t *queryLoopTestTask) Execute(context.Context) error {
	t.executeCalls.Add(1)
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

func TestTaskScheduler_QueryLoopContextDoneBeforeExecution(t *testing.T) {
	for _, subTask := range []bool{false, true} {
		poolName := "main pool"
		if subTask {
			poolName = "subtask pool"
		}
		t.Run(poolName, func(t *testing.T) {
			for _, outcome := range []string{"canceled", "deadline exceeded", "live"} {
				t.Run(outcome, func(t *testing.T) {
					pools := []*conc.Pool[struct{}]{conc.NewPool[struct{}](1), conc.NewPool[struct{}](1)}
					t.Cleanup(func() {
						for _, pool := range pools {
							pool.Release()
						}
					})

					// Keep the actual pools so Waiting proves Submit is blocked before cancellation.
					nextPool := 0
					poolMock := mockey.MockGeneric(conc.NewPool[struct{}]).To(func(_ int, _ ...conc.PoolOption) *conc.Pool[struct{}] {
						pool := pools[nextPool]
						nextPool++
						return pool
					}).Build()
					t.Cleanup(func() { poolMock.UnPatch() })

					sched, err := newTaskScheduler(context.Background(), newMockTsoAllocator())
					require.NoError(t, err)
					release := make(chan struct{})
					var releaseOnce sync.Once
					releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
					t.Cleanup(func() {
						releaseWorker()
						sched.Close()
					})
					sched.wg.Add(1)
					go sched.queryLoop()

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
					require.NoError(t, sched.dqQueue.Enqueue(blockingTask))
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
					require.NoError(t, sched.dqQueue.Enqueue(task))

					pool := pools[0]
					if subTask {
						pool = pools[1]
					}
					require.Eventually(t, func() bool { return pool.Waiting() == 1 }, 5*time.Second, time.Millisecond)
					require.True(t, sched.dqQueue.utEmpty())

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
						return sched.dqQueue.getTaskByReqID(task.ID()) == nil
					}, 5*time.Second, time.Millisecond)
				})
			}
		})
	}
}
