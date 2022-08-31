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

package querynode

import (
	"container/list"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

const maxTaskNum = 1024

type taskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(tID UniqueID) task
	Enqueue(t task) error
}

type baseTaskQueue struct {
	utMu          sync.RWMutex // guards unissuedTasks
	unissuedTasks *list.List

	atMu        sync.Mutex // guards activeTasks
	activeTasks map[UniqueID]task

	maxTaskNum int64    // maxTaskNum should keep still
	utBufChan  chan int // to block scheduler

	scheduler *taskScheduler
}

type queryNodeTaskQueue struct {
	baseTaskQueue
	mu sync.Mutex
}

// baseTaskQueue
func (queue *baseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *baseTaskQueue) utEmpty() bool {
	queue.utMu.RLock()
	defer queue.utMu.RUnlock()
	return queue.unissuedTasks.Len() == 0
}

func (queue *baseTaskQueue) utFull() bool {
	queue.utMu.RLock()
	defer queue.utMu.RUnlock()
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *baseTaskQueue) addUnissuedTask(t task) error {
	if queue.utFull() {
		return errors.New("task queue is full")
	}

	queue.utMu.Lock()
	defer queue.utMu.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		queue.unissuedTasks.PushBack(t)
		queue.utBufChan <- 1
		return nil
	}

	if t.Timestamp() >= queue.unissuedTasks.Back().Value.(task).Timestamp() {
		queue.unissuedTasks.PushBack(t)
		queue.utBufChan <- 1
		return nil
	}

	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if t.Timestamp() <= e.Value.(task).Timestamp() {
			queue.unissuedTasks.InsertBefore(t, e)
			queue.utBufChan <- 1
			return nil
		}
	}
	return errors.New("unexpected error in addUnissuedTask")
}

func (queue *baseTaskQueue) PopUnissuedTask() task {
	queue.utMu.Lock()
	defer queue.utMu.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Fatal("unissued task list is empty!")
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *baseTaskQueue) AddActiveTask(t task) {
	queue.atMu.Lock()
	defer queue.atMu.Unlock()

	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		log.Ctx(t.Ctx()).Warn("queryNode", zap.Int64("task with ID already in active task list!", tID))
	}

	queue.activeTasks[tID] = t
}

func (queue *baseTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atMu.Lock()
	defer queue.atMu.Unlock()

	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		return t
	}
	log.Info("queryNode", zap.Int64("cannot found ID in the active task list!", tID))
	return nil
}

func (queue *baseTaskQueue) Enqueue(t task) error {
	err := t.OnEnqueue()
	if err != nil {
		return err
	}
	return queue.addUnissuedTask(t)
}

// queryNodeTaskQueue
func (queue *queryNodeTaskQueue) Enqueue(t task) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.baseTaskQueue.Enqueue(t)
}

func newQueryNodeTaskQueue(scheduler *taskScheduler) *queryNodeTaskQueue {
	return &queryNodeTaskQueue{
		baseTaskQueue: baseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[UniqueID]task),
			maxTaskNum:    maxTaskNum,
			utBufChan:     make(chan int, maxTaskNum),
			scheduler:     scheduler,
		},
	}
}
