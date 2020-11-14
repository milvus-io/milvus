package proxy

import (
	"container/list"
	"context"
	"errors"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
)

type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(ts Timestamp) task
	getTaskByReqID(reqID UniqueID) task
	TaskDoneTest(ts Timestamp) bool
	Enqueue(t task) error
}

type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[Timestamp]task
	utLock        sync.Mutex
	atLock        sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan int // to block scheduler
}

func (queue *BaseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *BaseTaskQueue) utEmpty() bool {
	return queue.unissuedTasks.Len() == 0
}

func (queue *BaseTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *BaseTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.utBufChan <- 1
	return nil
}

func (queue *BaseTaskQueue) FrontUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Panic("sorry, but the unissued task list is empty!")
		return nil
	}

	return queue.unissuedTasks.Front().Value.(task)
}

func (queue *BaseTaskQueue) PopUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Fatal("sorry, but the unissued task list is empty!")
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *BaseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	ts := t.EndTs()
	_, ok := queue.activeTasks[ts]
	if ok {
		log.Fatalf("task with timestamp %v already in active task list!", ts)
	}

	queue.activeTasks[ts] = t
}

func (queue *BaseTaskQueue) PopActiveTask(ts Timestamp) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[ts]
	if ok {
		delete(queue.activeTasks, ts)
		return t
	}

	log.Fatalf("sorry, but the timestamp %d was not found in the active task list!", ts)
	return nil
}

func (queue *BaseTaskQueue) getTaskByReqID(reqID UniqueID) task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).ID() == reqID {
			return e.Value.(task)
		}
	}

	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	for ats := range queue.activeTasks {
		if queue.activeTasks[ats].ID() == reqID {
			return queue.activeTasks[ats]
		}
	}

	return nil
}

func (queue *BaseTaskQueue) TaskDoneTest(ts Timestamp) bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if e.Value.(task).EndTs() >= ts {
			return false
		}
	}

	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	for ats := range queue.activeTasks {
		if ats >= ts {
			return false
		}
	}

	return true
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	// TODO: set Ts, ReqId, ProxyId
	return queue.addUnissuedTask(t)
}

type DdTaskQueue struct {
	BaseTaskQueue
	lock sync.Mutex
}

type DmTaskQueue struct {
	BaseTaskQueue
}

type DqTaskQueue struct {
	BaseTaskQueue
}

func (queue *DdTaskQueue) Enqueue(t task) error {

	queue.lock.Lock()
	defer queue.lock.Unlock()

	// TODO: set Ts, ReqId, ProxyId
	return queue.addUnissuedTask(t)

}

func NewDdTaskQueue() *DdTaskQueue {
	return &DdTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
		},
	}
}

func NewDmTaskQueue() *DmTaskQueue {
	return &DmTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
		},
	}
}

func NewDqTaskQueue() *DqTaskQueue {
	return &DqTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
		},
	}
}

type TaskScheduler struct {
	DdQueue TaskQueue
	DmQueue TaskQueue
	DqQueue TaskQueue

	idAllocator  *allocator.IDAllocator
	tsoAllocator *allocator.TimestampAllocator

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context,
	idAllocator *allocator.IDAllocator,
	tsoAllocator *allocator.TimestampAllocator) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		DdQueue:      NewDdTaskQueue(),
		DmQueue:      NewDmTaskQueue(),
		DqQueue:      NewDqTaskQueue(),
		idAllocator:  idAllocator,
		tsoAllocator: tsoAllocator,
		ctx:          ctx1,
		cancel:       cancel,
	}

	return s, nil
}

func (sched *TaskScheduler) scheduleDdTask() task {
	return sched.DdQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDmTask() task {
	return sched.DmQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleDqTask() task {
	return sched.DqQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) getTaskByReqID(collMeta UniqueID) task {
	if t := sched.DdQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	if t := sched.DmQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	if t := sched.DqQueue.getTaskByReqID(collMeta); t != nil {
		return t
	}
	return nil
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {

	err := t.PreExecute()

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		return
	}

	q.AddActiveTask(t)
	defer q.PopActiveTask(t.EndTs())

	err = t.Execute()
	if err != nil {
		log.Printf("execute definition task failed, error = %v", err)
		return
	}
	err = t.PostExecute()
}

func (sched *TaskScheduler) definitionLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DdQueue.utChan():
			if !sched.DdQueue.utEmpty() {
				t := sched.scheduleDdTask()
				sched.processTask(t, sched.DdQueue)
			}
		}
	}
}

func (sched *TaskScheduler) manipulationLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DmQueue.utChan():
			if !sched.DmQueue.utEmpty() {
				t := sched.scheduleDmTask()
				go sched.processTask(t, sched.DmQueue)
			}
		}
	}
}

func (sched *TaskScheduler) queryLoop() {
	defer sched.wg.Done()

	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DqQueue.utChan():
			if !sched.DqQueue.utEmpty() {
				t := sched.scheduleDqTask()
				go sched.processTask(t, sched.DqQueue)
			}
		}
	}
}

func (sched *TaskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.definitionLoop()

	sched.wg.Add(1)
	go sched.manipulationLoop()

	sched.wg.Add(1)
	go sched.queryLoop()

	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

func (sched *TaskScheduler) TaskDoneTest(ts Timestamp) bool {
	ddTaskDone := sched.DdQueue.TaskDoneTest(ts)
	dmTaskDone := sched.DmQueue.TaskDoneTest(ts)
	dqTaskDone := sched.DqQueue.TaskDoneTest(ts)
	return ddTaskDone && dmTaskDone && dqTaskDone
}
