package proxy

import (
	"container/list"
	"context"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
)

type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[Timestamp]task
	utLock        sync.Mutex
	atLock        sync.Mutex
}

func (queue *BaseTaskQueue) Empty() bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	return queue.unissuedTasks.Len() <= 0 && len(queue.activeTasks) <= 0
}

func (queue *BaseTaskQueue) AddUnissuedTask(t task) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	queue.unissuedTasks.PushBack(t)
}

func (queue *BaseTaskQueue) FrontUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	if queue.unissuedTasks.Len() <= 0 {
		log.Fatal("sorry, but the unissued task list is empty!")
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
	return queue.unissuedTasks.Remove(ft).(task)
}

func (queue *BaseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Lock()
	ts := t.EndTs()
	_, ok := queue.activeTasks[ts]
	if ok {
		log.Fatalf("task with timestamp %v already in active task list!", ts)
	}
	queue.activeTasks[ts] = t
}

func (queue *BaseTaskQueue) PopActiveTask(ts Timestamp) task {
	queue.atLock.Lock()
	defer queue.atLock.Lock()
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
	defer queue.utLock.Lock()
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
	queue.AddUnissuedTask(t)
	return nil
}

func (queue *DmTaskQueue) Enqueue(t task) error {
	// TODO: set Ts, ReqId, ProxyId
	queue.AddUnissuedTask(t)
	return nil
}

func (queue *DqTaskQueue) Enqueue(t task) error {
	// TODO: set Ts, ReqId, ProxyId
	queue.AddUnissuedTask(t)
	return nil
}

func NewDdTaskQueue() *DdTaskQueue {
	return &DdTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
		},
	}
}

func NewDmTaskQueue() *DmTaskQueue {
	return &DmTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
		},
	}
}

func NewDqTaskQueue() *DqTaskQueue {
	return &DqTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
		},
	}
}

type TaskScheduler struct {
	DdQueue *DdTaskQueue
	DmQueue *DmTaskQueue
	DqQueue *DqTaskQueue

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

func (sched *TaskScheduler) definitionLoop() {
	defer sched.wg.Done()
	defer sched.cancel()

	for {
		if sched.DdQueue.Empty() {
			continue
		}

		//sched.DdQueue.atLock.Lock()
		t := sched.scheduleDdTask()

		err := t.PreExecute()
		if err != nil {
			return
		}
		err = t.Execute()
		if err != nil {
			log.Printf("execute definition task failed, error = %v", err)
		}
		t.Notify(err)

		sched.DdQueue.AddActiveTask(t)

		t.WaitToFinish()
		t.PostExecute()

		sched.DdQueue.PopActiveTask(t.EndTs())
	}
}

func (sched *TaskScheduler) manipulationLoop() {
	defer sched.wg.Done()
	defer sched.cancel()

	for {
		if sched.DmQueue.Empty() {
			continue
		}

		sched.DmQueue.atLock.Lock()
		t := sched.scheduleDmTask()

		if err := t.PreExecute(); err != nil {
			return
		}

		go func() {
			err := t.Execute()
			if err != nil {
				log.Printf("execute manipulation task failed, error = %v", err)
			}
			t.Notify(err)
		}()

		sched.DmQueue.AddActiveTask(t)
		sched.DmQueue.atLock.Unlock()

		go func() {
			t.WaitToFinish()
			t.PostExecute()

			// remove from active list
			sched.DmQueue.PopActiveTask(t.EndTs())
		}()
	}
}

func (sched *TaskScheduler) queryLoop() {
	defer sched.wg.Done()
	defer sched.cancel()

	for {
		if sched.DqQueue.Empty() {
			continue
		}

		sched.DqQueue.atLock.Lock()
		t := sched.scheduleDqTask()

		if err := t.PreExecute(); err != nil {
			return
		}

		go func() {
			err := t.Execute()
			if err != nil {
				log.Printf("execute query task failed, error = %v", err)
			}
			t.Notify(err)
		}()

		sched.DqQueue.AddActiveTask(t)
		sched.DqQueue.atLock.Unlock()

		go func() {
			t.WaitToFinish()
			t.PostExecute()

			// remove from active list
			sched.DqQueue.PopActiveTask(t.EndTs())
		}()
	}
}

func (sched *TaskScheduler) Start() error {
	sched.wg.Add(3)

	go sched.definitionLoop()
	go sched.manipulationLoop()
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
