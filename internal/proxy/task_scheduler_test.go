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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func TestBaseTaskQueue(t *testing.T) {

	var err error
	var unissuedTask task
	var activeTask task

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
	queue.setMaxTaskNum(10) // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.getMaxTaskNum()); i++ {
		err = queue.Enqueue(newDefaultMockTask())
		assert.NoError(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockTask())
	assert.Error(t, err)
}

func TestDdTaskQueue(t *testing.T) {

	var err error
	var unissuedTask task
	var activeTask task

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
	queue.setMaxTaskNum(10) // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.getMaxTaskNum()); i++ {
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
	var unissuedTask task
	var activeTask task

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
	queue.setMaxTaskNum(10) // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.getMaxTaskNum()); i++ {
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
	var unissuedTask task

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
		assert.Equal(t, unissuedTask.BeginTs(), stat.minTs)
		assert.Equal(t, unissuedTask.EndTs(), stat.maxTs)
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
			go func(ut task) {
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

	var currPChanStats map[pChan]*pChanStatistics
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
							fmt.Println("stat.minTs", stat.minTs, " ", "curInfo.minTs:", curInfo.minTs)
							fmt.Println("stat.maxTs", stat.maxTs, " ", "curInfo.minTs:", curInfo.maxTs)
							assert.True(t, stat.minTs >= curInfo.minTs)
							curInfo.minTs = stat.minTs
							assert.True(t, stat.maxTs >= curInfo.maxTs)
							curInfo.maxTs = stat.maxTs
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
	//time.Sleep(time.Millisecond*100)
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
	var unissuedTask task
	var activeTask task

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
	queue.setMaxTaskNum(10) // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.getMaxTaskNum()); i++ {
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
	factory := newSimpleMockMsgStreamFactory()

	sched, err := newTaskScheduler(ctx, tsoAllocatorIns, factory)
	assert.NoError(t, err)
	assert.NotNil(t, sched)

	err = sched.Start()
	assert.NoError(t, err)
	defer sched.Close()

	stats, err := sched.getPChanStatistics()
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

				err := sched.ddQueue.Enqueue(newDefaultMockDdlTask())
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

				err := sched.dmQueue.Enqueue(newDefaultMockDmlTask())
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

				err := sched.dqQueue.Enqueue(newDefaultMockDqlTask())
				assert.NoError(t, err)
			}()
		}
	}()

	wg.Wait()
}

func TestTaskScheduler_concurrentPushAndPop(t *testing.T) {
	collectionID := UniqueID(0)
	collectionName := "col-0"
	channels := []pChan{"mock-chan-0", "mock-chan-1"}
	cache := NewMockCache(t)
	cache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)
	globalMetaCache = cache
	tsoAllocatorIns := newMockTsoAllocator()
	factory := newSimpleMockMsgStreamFactory()
	scheduler, err := newTaskScheduler(context.Background(), tsoAllocatorIns, factory)
	assert.NoError(t, err)

	run := func(wg *sync.WaitGroup) {
		defer wg.Done()
		chMgr := newMockChannelsMgr()
		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return channels, nil
		}
		it := &insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: msgpb.InsertRequest{
					Base:           &commonpb.MsgBase{},
					CollectionName: collectionName,
				},
			},
			chMgr: chMgr,
		}
		err := scheduler.dmQueue.Enqueue(it)
		assert.NoError(t, err)
		task := scheduler.scheduleDmTask()
		scheduler.dmQueue.AddActiveTask(task)
		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return nil, fmt.Errorf("mock err")
		}
		scheduler.dmQueue.PopActiveTask(task.ID()) // assert no panic
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go run(wg)
	}
	wg.Wait()
}
