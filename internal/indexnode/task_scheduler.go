package indexnode

import (
	"container/list"
	"context"
	"errors"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
)

type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(tID UniqueID) task
	Enqueue(t task) error
}

type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[UniqueID]task
	utLock        sync.Mutex
	atLock        sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan int // to block scheduler

	sched *TaskScheduler
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

	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		log.Fatalf("task with ID %v already in active task list!", tID)
	}

	queue.activeTasks[tID] = t
}

func (queue *BaseTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		return t
	}
	log.Fatalf("sorry, but the ID %d was not found in the active task list!", tID)
	return nil
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	tID, _ := queue.sched.idAllocator.AllocOne()
	log.Printf("[Builder] allocate reqID: %v", tID)
	t.SetID(tID)
	err := t.OnEnqueue()
	if err != nil {
		return err
	}
	return queue.addUnissuedTask(t)
}

type IndexAddTaskQueue struct {
	BaseTaskQueue
	lock sync.Mutex
}

type IndexBuildTaskQueue struct {
	BaseTaskQueue
}

func (queue *IndexAddTaskQueue) Enqueue(t task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.BaseTaskQueue.Enqueue(t)
}

func NewIndexAddTaskQueue(sched *TaskScheduler) *IndexAddTaskQueue {
	return &IndexAddTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[UniqueID]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

func NewIndexBuildTaskQueue(sched *TaskScheduler) *IndexBuildTaskQueue {
	return &IndexBuildTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[UniqueID]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

type TaskScheduler struct {
	IndexAddQueue   TaskQueue
	IndexBuildQueue TaskQueue

	idAllocator *allocator.IDAllocator
	metaTable   *metaTable
	kv          kv.Base

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context,
	idAllocator *allocator.IDAllocator,
	kv kv.Base,
	table *metaTable) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		idAllocator: idAllocator,
		metaTable:   table,
		kv:          kv,
		ctx:         ctx1,
		cancel:      cancel,
	}
	s.IndexAddQueue = NewIndexAddTaskQueue(s)
	s.IndexBuildQueue = NewIndexBuildTaskQueue(s)

	return s, nil
}

func (sched *TaskScheduler) scheduleIndexAddTask() task {
	return sched.IndexAddQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) scheduleIndexBuildTask() task {
	return sched.IndexBuildQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {

	err := t.PreExecute()

	defer func() {
		t.Notify(err)
		log.Printf("notify with error: %v", err)
	}()
	if err != nil {
		return
	}

	q.AddActiveTask(t)
	log.Printf("task add to active list ...")
	defer func() {
		q.PopActiveTask(t.ID())
		log.Printf("pop from active list ...")
	}()

	err = t.Execute()
	if err != nil {
		log.Printf("execute definition task failed, error = %v", err)
		return
	}
	log.Printf("task execution done ...")
	err = t.PostExecute()
	log.Printf("post execute task done ...")
}

func (sched *TaskScheduler) indexBuildLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.IndexBuildQueue.utChan():
			if !sched.IndexBuildQueue.utEmpty() {
				t := sched.scheduleIndexBuildTask()
				sched.processTask(t, sched.IndexBuildQueue)
			}
		}
	}
}

func (sched *TaskScheduler) indexAddLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.IndexAddQueue.utChan():
			if !sched.IndexAddQueue.utEmpty() {
				t := sched.scheduleIndexAddTask()
				go sched.processTask(t, sched.IndexAddQueue)
			}
		}
	}
}

func (sched *TaskScheduler) Start() error {

	sched.wg.Add(1)
	go sched.indexAddLoop()

	sched.wg.Add(1)
	go sched.indexBuildLoop()
	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
