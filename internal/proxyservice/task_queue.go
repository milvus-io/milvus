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

package proxyservice

import (
	"container/list"
	"errors"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/log"
)

type taskQueue interface {
	Chan() <-chan int
	Empty() bool
	Full() bool
	addTask(t task) error
	FrontTask() task
	PopTask() task
	Enqueue(t task) error
}

type baseTaskQueue struct {
	tasks *list.List
	mtx   sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	bufChan chan int // to block scheduler
}

func (queue *baseTaskQueue) Chan() <-chan int {
	return queue.bufChan
}

func (queue *baseTaskQueue) Empty() bool {
	return queue.tasks.Len() <= 0
}

func (queue *baseTaskQueue) Full() bool {
	return int64(queue.tasks.Len()) >= queue.maxTaskNum
}

func (queue *baseTaskQueue) addTask(t task) error {
	queue.mtx.Lock()
	defer queue.mtx.Unlock()

	if queue.Full() {
		return errors.New("task queue is full")
	}
	queue.tasks.PushBack(t)
	queue.bufChan <- 1
	return nil
}

func (queue *baseTaskQueue) FrontTask() task {
	queue.mtx.Lock()
	defer queue.mtx.Unlock()

	if queue.tasks.Len() <= 0 {
		log.Warn("sorry, but the task list is empty!")
		return nil
	}

	return queue.tasks.Front().Value.(task)
}

func (queue *baseTaskQueue) PopTask() task {
	queue.mtx.Lock()
	defer queue.mtx.Unlock()

	if queue.tasks.Len() <= 0 {
		log.Warn("sorry, but the task list is empty!")
		return nil
	}

	ft := queue.tasks.Front()
	queue.tasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *baseTaskQueue) Enqueue(t task) error {
	return queue.addTask(t)
}

func newBaseTaskQueue() *baseTaskQueue {
	return &baseTaskQueue{
		tasks:      list.New(),
		maxTaskNum: 1024,
		bufChan:    make(chan int, 1024),
	}
}
