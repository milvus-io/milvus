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

package importv2

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResizePools(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	pt := paramtable.Get()

	defer func() {
		_ = pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)
	}()

	t.Run("ExecPool", func(t *testing.T) {
		cpuNum := hardware.GetCPUNum()
		expectedCap := cpuNum * pt.DataNodeCfg.ImportConcurrencyPerCPUCore.GetAsInt()
		assert.Equal(t, expectedCap, GetExecPool().Cap())
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap())

		_ = pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, fmt.Sprintf("%d", expectedCap*2))
		expectedCap = cpuNum * pt.DataNodeCfg.ImportConcurrencyPerCPUCore.GetAsInt()
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap())

		_ = pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "0")
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap(), "pool shall not be resized when newSize is 0")

		_ = pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "invalid")
		resizeExecPool(&config.Event{
			HasUpdated: true,
		})
		assert.Equal(t, expectedCap, GetExecPool().Cap())
	})
}

func TestOrderedExecPoolSubmitOrder(t *testing.T) {
	const (
		poolSize = 3
		taskNum  = 12
	)

	inner := conc.NewPool[any](poolSize, conc.WithDisablePurge(true))
	pool := newOrderedExecPool(inner)
	defer closeOrderedExecPoolForTest(pool)

	started := make(chan int, taskNum)
	releases := make([]chan struct{}, taskNum)
	futures := make([]*conc.Future[any], 0, taskNum)

	for i := 0; i < taskNum; i++ {
		taskID := i
		releases[taskID] = make(chan struct{})
		future := pool.Submit(func() (any, error) {
			started <- taskID
			<-releases[taskID]
			return taskID, nil
		})
		futures = append(futures, future)
	}

	startedTasks := make([]int, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		startedTasks = append(startedTasks, requireStarted(t, started))
	}
	assert.ElementsMatch(t, []int{0, 1, 2}, startedTasks)
	requireNoBufferedStart(t, started)

	close(releases[poolSize-1])
	value, err := awaitFuture(t, futures[poolSize-1])
	require.NoError(t, err)
	assert.Equal(t, poolSize-1, value)
	assert.False(t, futures[0].Done())
	assert.False(t, futures[1].Done())
	require.Equal(t, poolSize, requireStarted(t, started))

	for i := 0; i < taskNum-poolSize-1; i++ {
		taskID := i
		if i >= poolSize-1 {
			taskID++
		}
		close(releases[taskID])
		value, err := awaitFuture(t, futures[taskID])
		require.NoError(t, err)
		assert.Equal(t, taskID, value)

		require.Equal(t, i+poolSize+1, requireStarted(t, started))
	}

	for i := taskNum - poolSize; i < taskNum; i++ {
		close(releases[i])
		value, err := awaitFuture(t, futures[i])
		require.NoError(t, err)
		assert.Equal(t, i, value)
	}
}

func TestOrderedExecPoolHOLBlocking(t *testing.T) {
	const taskNum = 6

	inner := conc.NewPool[any](1, conc.WithDisablePurge(true))
	pool := newOrderedExecPool(inner)
	defer closeOrderedExecPoolForTest(pool)

	releaseOccupy := make(chan struct{})
	occupy := inner.Submit(func() (any, error) {
		<-releaseOccupy
		return nil, nil
	})

	started := make(chan int, taskNum)
	releases := make([]chan struct{}, taskNum)
	futures := make([]*conc.Future[any], 0, taskNum)

	for i := 0; i < taskNum; i++ {
		taskID := i
		releases[taskID] = make(chan struct{})
		future := pool.Submit(func() (any, error) {
			started <- taskID
			<-releases[taskID]
			return taskID, nil
		})
		futures = append(futures, future)
	}

	waitQueueLen(t, &pool.queue, taskNum-1)
	requireNoBufferedStart(t, started)

	close(releaseOccupy)
	_, err := awaitFuture(t, occupy)
	require.NoError(t, err)

	for i := 0; i < taskNum; i++ {
		require.Equal(t, i, requireStarted(t, started))
		requireNoBufferedStart(t, started)
		close(releases[i])
		value, err := awaitFuture(t, futures[i])
		require.NoError(t, err)
		assert.Equal(t, i, value)
	}
}

func closeOrderedExecPoolForTest(pool *orderedExecPool) {
	close(pool.closeCh)
	pool.inner.Release()
}

func requireStarted(t *testing.T, started <-chan int) int {
	t.Helper()

	select {
	case taskID := <-started:
		return taskID
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for task start")
		return 0
	}
}

func requireNoBufferedStart(t *testing.T, started <-chan int) {
	t.Helper()

	select {
	case taskID := <-started:
		require.FailNowf(t, "unexpected task start", "task %d started unexpectedly", taskID)
	default:
	}
}

func waitQueueLen[T any](t *testing.T, queue *conc.ConcurrentQueue[T], expected int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return queue.Len() == expected
	}, time.Second, 10*time.Millisecond)
}

func awaitFuture(t *testing.T, future *conc.Future[any]) (any, error) {
	t.Helper()

	select {
	case <-future.Inner():
		return future.Await()
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for future")
		return nil, nil
	}
}
