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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBaseTaskQueue_isFull(t *testing.T) {
	queue := newBaseTaskQueue(newMockTsoAllocator())
	queue.SetMaxTaskNum(2)

	assert.False(t, queue.IsFull())
	assert.NoError(t, queue.Enqueue(newDefaultMockTask()))
	assert.False(t, queue.IsFull())
	assert.NoError(t, queue.Enqueue(newDefaultMockTask()))
	assert.True(t, queue.IsFull())

	queue.PopUnissuedTask()
	assert.False(t, queue.IsFull())
}

func TestBaseTaskQueue(t *testing.T) {
	var err error
	var unissuedTask taskmodel.Task
	var activeTask taskmodel.Task

	tsoAllocatorIns := newMockTsoAllocator()
	queue := newBaseTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockTask()
	stID := st.ID()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	// task enqueue, only one task in queue

	err = queue.Enqueue(st)
	assert.NoError(t, err)

	assert.False(t, queue.utEmpty())
	assert.False(t, queue.utFull())
	assert.Equal(t, 1, queue.unissuedTasks.Len())
	assert.Equal(t, 1, len(queue.utChan()))

	unissuedTask = queue.FrontUnissuedTask()
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	// test utFull
	queue.SetMaxTaskNum(10) // utBufChan is an edge-triggered notifier; capacity is tracked by utFull
	for i := 0; i < int(queue.GetMaxTaskNum()); i++ {
		err = queue.Enqueue(newDefaultMockTask())
		assert.NoError(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockTask())
	assert.Error(t, err)
}

func TestDqTaskQueue_RemoveUnissuedTaskOnWaitError(t *testing.T) {
	t.Run("remove queued task", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := newDqTaskQueue(newMockTsoAllocator())
		task := newMockDqlTask(ctx)

		assert.NoError(t, queue.Enqueue(task))
		assert.Equal(t, 1, queue.unissuedTasks.Len())

		cancel()
		assert.Error(t, task.WaitToFinish())
		assert.True(t, queue.utEmpty())
		assert.Nil(t, queue.getTaskByReqID(task.ID()))
	})

	t.Run("popped task is unchanged", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		queue := newDqTaskQueue(newMockTsoAllocator())
		poppedTask := newMockDqlTask(ctx)
		queuedTask := newDefaultMockDqlTask()

		assert.NoError(t, queue.Enqueue(poppedTask))
		assert.NoError(t, queue.Enqueue(queuedTask))
		assert.Same(t, poppedTask, queue.PopUnissuedTask())

		cancel()
		assert.Error(t, poppedTask.WaitToFinish())
		assert.Equal(t, 1, queue.unissuedTasks.Len())
		assert.Same(t, queuedTask, queue.FrontUnissuedTask())
	})
}

func TestDdTaskQueue(t *testing.T) {
	var err error
	var unissuedTask taskmodel.Task
	var activeTask taskmodel.Task

	tsoAllocatorIns := newMockTsoAllocator()
	queue := newDdTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockDdlTask()
	stID := st.ID()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	// task enqueue, only one task in queue

	err = queue.Enqueue(st)
	assert.NoError(t, err)

	assert.False(t, queue.utEmpty())
	assert.False(t, queue.utFull())
	assert.Equal(t, 1, queue.unissuedTasks.Len())
	assert.Equal(t, 1, len(queue.utChan()))

	unissuedTask = queue.FrontUnissuedTask()
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	// test utFull
	queue.SetMaxTaskNum(10) // utBufChan is an edge-triggered notifier; capacity is tracked by utFull
	for i := 0; i < int(queue.GetMaxTaskNum()); i++ {
		err = queue.Enqueue(newDefaultMockDdlTask())
		assert.NoError(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockDdlTask())
	assert.Error(t, err)
}

// test the logic of queue
func TestDmTaskQueue_Basic(t *testing.T) {
	var err error
	var unissuedTask taskmodel.Task
	var activeTask taskmodel.Task

	tsoAllocatorIns := newMockTsoAllocator()
	queue := newDmTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockDmlTask()
	stID := st.ID()

	// no task in queue
	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	// task enqueue, only one task in queue

	err = queue.Enqueue(st)
	assert.NoError(t, err)

	assert.False(t, queue.utEmpty())
	assert.False(t, queue.utFull())
	assert.Equal(t, 1, queue.unissuedTasks.Len())
	assert.Equal(t, 1, len(queue.utChan()))

	unissuedTask = queue.FrontUnissuedTask()
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	// test utFull
	queue.SetMaxTaskNum(10) // utBufChan is an edge-triggered notifier; capacity is tracked by utFull
	for i := 0; i < int(queue.GetMaxTaskNum()); i++ {
		err = queue.Enqueue(newDefaultMockDmlTask())
		assert.NoError(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockDmlTask())
	assert.Error(t, err)
}

// test the timestamp statistics
func TestDmTaskQueue_TimestampStatistics(t *testing.T) {
	var err error
	var unissuedTask taskmodel.Task

	tsoAllocatorIns := newMockTsoAllocator()
	queue := newDmTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	st := newDefaultMockDmlTask()
	stPChans := st.pchans

	err = queue.Enqueue(st)
	assert.NoError(t, err)

	stats, err := queue.getPChanStatsInfo()
	assert.NoError(t, err)
	assert.Equal(t, len(stPChans), len(stats))
	unissuedTask = queue.FrontUnissuedTask()
	assert.NotNil(t, unissuedTask)
	for _, stat := range stats {
		assert.Equal(t, unissuedTask.BeginTs(), stat.MinTs)
		assert.Equal(t, unissuedTask.EndTs(), stat.MaxTs)
	}

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())

	queue.AddActiveTask(unissuedTask)

	queue.PopActiveTask(unissuedTask.ID())

	stats, err = queue.getPChanStatsInfo()
	assert.NoError(t, err)
	assert.Zero(t, len(stats))
}

// test the timestamp statistics
func TestDmTaskQueue_TimestampStatistics2(t *testing.T) {
	tsoAllocatorIns := newMockTsoAllocator()
	queue := newDmTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	prefix := funcutil.GenRandomStr()
	insertNum := 100

	var processWg sync.WaitGroup
	processWg.Add(1)
	processCtx, processCancel := context.WithCancel(context.TODO())
	processCount := insertNum
	var processCountMut sync.RWMutex
	go func() {
		defer processWg.Done()
		var workerWg sync.WaitGroup
		workerWg.Add(insertNum)
		for processCtx.Err() == nil {
			if queue.utEmpty() {
				continue
			}
			utTask := queue.PopUnissuedTask()
			go func(ut taskmodel.Task) {
				defer workerWg.Done()
				assert.NotNil(t, ut)
				queue.AddActiveTask(ut)
				dur := time.Duration(50+rand.Int()%10) * time.Millisecond
				time.Sleep(dur)
				queue.PopActiveTask(ut.ID())
				processCountMut.Lock()
				defer processCountMut.Unlock()
				processCount--
			}(utTask)
		}
		workerWg.Wait()
	}()

	var currPChanStats map[taskmodel.PChan]*taskmodel.PChanStatistics
	var wgSchedule sync.WaitGroup
	scheduleCtx, scheduleCancel := context.WithCancel(context.TODO())
	schedule := func() {
		defer wgSchedule.Done()
		ticker := time.NewTicker(time.Millisecond * 10)
		defer ticker.Stop()
		for {
			select {
			case <-scheduleCtx.Done():
				return
			case <-ticker.C:
				stats, err := queue.getPChanStatsInfo()
				assert.NoError(t, err)
				if currPChanStats == nil {
					currPChanStats = stats
				} else {
					// assure minTs and maxTs will not go back
					for p, stat := range stats {
						curInfo, ok := currPChanStats[p]
						if ok {
							fmt.Println("stat.MinTs", stat.MinTs, " ", "curInfo.MinTs:", curInfo.MinTs)
							fmt.Println("stat.MaxTs", stat.MaxTs, " ", "curInfo.MinTs:", curInfo.MaxTs)
							assert.True(t, stat.MinTs >= curInfo.MinTs)
							curInfo.MinTs = stat.MinTs
							assert.True(t, stat.MaxTs >= curInfo.MaxTs)
							curInfo.MaxTs = stat.MaxTs
						}
					}
				}
			}
		}
	}
	wgSchedule.Add(1)
	go schedule()

	var wg sync.WaitGroup
	wg.Add(insertNum)
	for i := 0; i < insertNum; i++ {
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			st := newDefaultMockDmlTask()
			vChannels := make([]string, 2)
			vChannels[0] = prefix + "_1"
			vChannels[1] = prefix + "_2"
			st.vchans = vChannels
			st.pchans = vChannels
			err := queue.Enqueue(st)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	// time.Sleep(time.Millisecond*100)
	needLoop := true
	for needLoop {
		processCountMut.RLock()
		needLoop = processCount != 0
		processCountMut.RUnlock()
	}
	processCancel()
	processWg.Wait()

	scheduleCancel()
	wgSchedule.Wait()

	stats, err := queue.getPChanStatsInfo()
	assert.NoError(t, err)
	assert.Zero(t, len(stats))
}

func TestDqTaskQueue(t *testing.T) {
	var err error
	var unissuedTask taskmodel.Task
	var activeTask taskmodel.Task

	tsoAllocatorIns := newMockTsoAllocator()
	queue := newDqTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockDqlTask()
	stID := st.ID()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	// task enqueue, only one task in queue

	err = queue.Enqueue(st)
	assert.NoError(t, err)

	assert.False(t, queue.utEmpty())
	assert.False(t, queue.utFull())
	assert.Equal(t, 1, queue.unissuedTasks.Len())
	assert.Equal(t, 1, len(queue.utChan()))

	unissuedTask = queue.FrontUnissuedTask()
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	// test utFull
	queue.SetMaxTaskNum(10) // utBufChan is an edge-triggered notifier; capacity is tracked by utFull
	for i := 0; i < int(queue.GetMaxTaskNum()); i++ {
		err = queue.Enqueue(newDefaultMockDqlTask())
		assert.NoError(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockDqlTask())
	assert.Error(t, err)
}

func TestTaskScheduler(t *testing.T) {
	var err error

	ctx := context.Background()
	tsoAllocatorIns := newMockTsoAllocator()

	sched, err := NewTaskScheduler(ctx, tsoAllocatorIns)
	assert.NoError(t, err)
	assert.NotNil(t, sched)

	err = sched.Start()
	assert.NoError(t, err)
	defer sched.Close()

	stats, err := sched.GetPChanStatistics()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(stats))

	ddNum := rand.Int() % 10
	dmNum := rand.Int() % 10
	dqNum := rand.Int() % 10

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < ddNum; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := sched.DdQueue.Enqueue(newDefaultMockDdlTask())
				assert.NoError(t, err)
			}()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < dmNum; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := sched.DmQueue.Enqueue(newDefaultMockDmlTask())
				assert.NoError(t, err)
			}()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < dqNum; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := sched.DqQueue.Enqueue(newDefaultMockDqlTask())
				assert.NoError(t, err)
			}()
		}
	}()

	wg.Wait()
}

func TestTaskScheduler_concurrentPushAndPop(t *testing.T) {
	tsoAllocatorIns := newMockTsoAllocator()
	scheduler, err := NewTaskScheduler(context.Background(), tsoAllocatorIns)
	assert.NoError(t, err)

	run := func(wg *sync.WaitGroup) {
		defer wg.Done()
		it := newDefaultMockDmlTask()
		err := scheduler.DmQueue.Enqueue(it)
		assert.NoError(t, err)
		task := scheduler.scheduleDmTask()
		scheduler.DmQueue.AddActiveTask(task)
		scheduler.DmQueue.PopActiveTask(task.ID()) // assert no panic
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go run(wg)
	}
	wg.Wait()
}

func TestTaskScheduler_SkipAllocTimestamp(t *testing.T) {
	mockMetaCache := NewMockCache(t)

	tsoAllocatorIns := newMockTsoAllocator()
	queue := newBaseTaskQueue(tsoAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	mockMetaCache.EXPECT().AllocID(mock.Anything).Return(1, nil).Twice()

	t.Run("query", func(t *testing.T) {
		qt := newSkipAllocMockTask(mockMetaCache)
		qt.name = "query"
		err := queue.Enqueue(qt)
		assert.NoError(t, err)
	})

	t.Run("search", func(t *testing.T) {
		st := newSkipAllocMockTask(mockMetaCache)
		st.name = "search"
		err := queue.Enqueue(st)
		assert.NoError(t, err)
	})

	mockMetaCache.EXPECT().AllocID(mock.Anything).Return(0, errors.New("mock error")).Once()
	t.Run("failed", func(t *testing.T) {
		st := newSkipAllocMockTask(mockMetaCache)
		st.name = "search"
		err := queue.Enqueue(st)
		assert.Error(t, err)
	})
}

// blockingTsoAllocator blocks AllocOne on the caller's context so that a test
// can distinguish "we reached the TSO allocator" from "we fast-failed".
type blockingTsoAllocator struct {
	calls atomic.Int64
}

func (b *blockingTsoAllocator) AllocOne(ctx context.Context) (taskmodel.Timestamp, error) {
	b.calls.Add(1)
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestBaseTaskQueue_EnqueueFastFailBeforeAlloc verifies that Enqueue rejects
// a task immediately with ErrServiceTooManyRequests when the queue is already
// full, without invoking the TSO allocator. Regression test for #49223.
func TestBaseTaskQueue_EnqueueFastFailBeforeAlloc(t *testing.T) {
	tsoAllocatorIns := newMockTsoAllocator()
	queue := newBaseTaskQueue(tsoAllocatorIns)
	queue.SetMaxTaskNum(2)

	// Fill queue to capacity with the non-blocking mock allocator.
	for i := 0; i < 2; i++ {
		assert.NoError(t, queue.Enqueue(newDefaultMockTask()))
	}
	assert.True(t, queue.utFull())

	// Swap in an allocator that would hang forever if reached.
	blocking := &blockingTsoAllocator{}
	queue.tsoAllocatorIns = blocking

	done := make(chan error, 1)
	go func() {
		done <- queue.Enqueue(newDefaultMockTask())
	}()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	case <-time.After(2 * time.Second):
		t.Fatalf("Enqueue did not fast-fail; reached blocking TSO allocator")
	}
	assert.Equal(t, int64(0), blocking.calls.Load(),
		"Enqueue must not reach the TSO allocator when the queue is already full")
}

// TestBaseTaskQueue_NotifierCoalesces verifies that utBufChan is an edge-
// triggered notifier: many enqueues produce at most one pending token.
func TestBaseTaskQueue_NotifierCoalesces(t *testing.T) {
	queue := newBaseTaskQueue(newMockTsoAllocator())
	queue.SetMaxTaskNum(100)

	for i := 0; i < 10; i++ {
		assert.NoError(t, queue.Enqueue(newDefaultMockTask()))
	}

	assert.Equal(t, 1, cap(queue.utBufChan), "utBufChan must be a capacity-1 notifier")
	assert.Equal(t, 1, len(queue.utChan()), "concurrent enqueues must coalesce to a single pending token")
}

// TestBaseTaskQueue_EnqueueNotifierNonBlocking verifies that a flood of
// enqueues completes without a consumer draining utBufChan — i.e. the
// notifier send must not block Enqueue.
func TestBaseTaskQueue_EnqueueNotifierNonBlocking(t *testing.T) {
	queue := newBaseTaskQueue(newMockTsoAllocator())
	queue.SetMaxTaskNum(1024)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 64; i++ {
			assert.NoError(t, queue.Enqueue(newDefaultMockTask()))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Enqueue blocked on utBufChan send")
	}
	assert.Equal(t, 1, len(queue.utChan()))
}
