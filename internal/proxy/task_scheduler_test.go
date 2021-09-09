// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBaseTaskQueue(t *testing.T) {
	var err error
	var unissuedTask task
	var activeTask task
	var done bool

	tsoAllocatorIns := newMockTsoAllocator()
	idAllocatorIns := newMockIDAllocatorInterface()
	queue := newBaseTaskQueue(tsoAllocatorIns, idAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockTask()
	stID := st.ID()
	stTs := st.BeginTs()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	done = queue.TaskDoneTest(stTs)
	assert.True(t, done)

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

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test utFull
	queue.maxTaskNum = 10 // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.maxTaskNum); i++ {
		err = queue.Enqueue(newDefaultMockTask())
		assert.Nil(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockTask())
	assert.NotNil(t, err)
}

func TestDdTaskQueue(t *testing.T) {
	var err error
	var unissuedTask task
	var activeTask task
	var done bool

	tsoAllocatorIns := newMockTsoAllocator()
	idAllocatorIns := newMockIDAllocatorInterface()
	queue := newDdTaskQueue(tsoAllocatorIns, idAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockDdlTask()
	stID := st.ID()
	stTs := st.BeginTs()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	done = queue.TaskDoneTest(stTs)
	assert.True(t, done)

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

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test utFull
	queue.maxTaskNum = 10 // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.maxTaskNum); i++ {
		err = queue.Enqueue(newDefaultMockDdlTask())
		assert.Nil(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockDdlTask())
	assert.NotNil(t, err)
}

// test the logic of queue
func TestDmTaskQueue_Basic(t *testing.T) {
	var err error
	var unissuedTask task
	var activeTask task
	var done bool

	tsoAllocatorIns := newMockTsoAllocator()
	idAllocatorIns := newMockIDAllocatorInterface()
	queue := newDmTaskQueue(tsoAllocatorIns, idAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockDmlTask()
	stID := st.ID()
	stTs := st.BeginTs()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	done = queue.TaskDoneTest(stTs)
	assert.True(t, done)

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

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test utFull
	queue.maxTaskNum = 10 // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.maxTaskNum); i++ {
		err = queue.Enqueue(newDefaultMockDmlTask())
		assert.Nil(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockDmlTask())
	assert.NotNil(t, err)
}

// test the timestamp statistics
func TestDmTaskQueue_TimestampStatistics(t *testing.T) {
	var err error
	var unissuedTask task

	tsoAllocatorIns := newMockTsoAllocator()
	idAllocatorIns := newMockIDAllocatorInterface()
	queue := newDmTaskQueue(tsoAllocatorIns, idAllocatorIns)
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

func TestDqTaskQueue(t *testing.T) {
	var err error
	var unissuedTask task
	var activeTask task
	var done bool

	tsoAllocatorIns := newMockTsoAllocator()
	idAllocatorIns := newMockIDAllocatorInterface()
	queue := newDqTaskQueue(tsoAllocatorIns, idAllocatorIns)
	assert.NotNil(t, queue)

	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	st := newDefaultMockDqlTask()
	stID := st.ID()
	stTs := st.BeginTs()

	// no task in queue

	unissuedTask = queue.FrontUnissuedTask()
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.getTaskByReqID(stID)
	assert.Nil(t, unissuedTask)

	unissuedTask = queue.PopUnissuedTask()
	assert.Nil(t, unissuedTask)

	done = queue.TaskDoneTest(stTs)
	assert.True(t, done)

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

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	unissuedTask = queue.PopUnissuedTask()
	assert.NotNil(t, unissuedTask)
	assert.True(t, queue.utEmpty())
	assert.False(t, queue.utFull())

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test active list, no task in queue

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.Nil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.Nil(t, activeTask)

	// test active list, no task in unissued list, only one task in active list

	queue.AddActiveTask(unissuedTask)

	activeTask = queue.getTaskByReqID(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.False(t, done)

	activeTask = queue.PopActiveTask(unissuedTask.ID())
	assert.NotNil(t, activeTask)

	done = queue.TaskDoneTest(unissuedTask.BeginTs() + 1)
	assert.True(t, done)

	// test utFull
	queue.maxTaskNum = 10 // not accurate, full also means utBufChan block
	for i := 0; i < int(queue.maxTaskNum); i++ {
		err = queue.Enqueue(newDefaultMockDqlTask())
		assert.Nil(t, err)
	}
	assert.True(t, queue.utFull())
	err = queue.Enqueue(newDefaultMockDqlTask())
	assert.NotNil(t, err)
}

func TestTaskScheduler(t *testing.T) {
	var err error

	ctx := context.Background()
	tsoAllocatorIns := newMockTsoAllocator()
	idAllocatorIns := newMockIDAllocatorInterface()
	factory := newSimpleMockMsgStreamFactory()

	sched, err := newTaskScheduler(ctx, idAllocatorIns, tsoAllocatorIns, factory)
	assert.NoError(t, err)
	assert.NotNil(t, sched)

	err = sched.Start()
	assert.NoError(t, err)
	defer sched.Close()

	assert.True(t, sched.TaskDoneTest(Timestamp(time.Now().Nanosecond())))

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
