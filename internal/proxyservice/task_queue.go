package proxyservice

import (
	"container/list"
	"errors"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/log"
)

type TaskQueue interface {
	Chan() <-chan int
	Empty() bool
	Full() bool
	addTask(t task) error
	FrontTask() task
	PopTask() task
	Enqueue(t task) error
}

type BaseTaskQueue struct {
	tasks *list.List
	mtx   sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	bufChan chan int // to block scheduler
}

func (queue *BaseTaskQueue) Chan() <-chan int {
	return queue.bufChan
}

func (queue *BaseTaskQueue) Empty() bool {
	return queue.tasks.Len() <= 0
}

func (queue *BaseTaskQueue) Full() bool {
	return int64(queue.tasks.Len()) >= queue.maxTaskNum
}

func (queue *BaseTaskQueue) addTask(t task) error {
	queue.mtx.Lock()
	defer queue.mtx.Unlock()

	if queue.Full() {
		return errors.New("task queue is full")
	}
	queue.tasks.PushBack(t)
	queue.bufChan <- 1
	return nil
}

func (queue *BaseTaskQueue) FrontTask() task {
	queue.mtx.Lock()
	defer queue.mtx.Unlock()

	if queue.tasks.Len() <= 0 {
		log.Panic("sorry, but the task list is empty!")
		return nil
	}

	return queue.tasks.Front().Value.(task)
}

func (queue *BaseTaskQueue) PopTask() task {
	queue.mtx.Lock()
	defer queue.mtx.Unlock()

	if queue.tasks.Len() <= 0 {
		log.Panic("sorry, but the task list is empty!")
		return nil
	}

	ft := queue.tasks.Front()
	queue.tasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	return queue.addTask(t)
}

func NewBaseTaskQueue() TaskQueue {
	return &BaseTaskQueue{
		tasks:      list.New(),
		maxTaskNum: 1024,
		bufChan:    make(chan int, 1024),
	}
}
