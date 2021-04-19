package proxy

import (
	"container/list"
	"log"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type baseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[typeutil.Timestamp]*task
	utLock        sync.Mutex
	atLock        sync.Mutex
}

type ddTaskQueue struct {
	baseTaskQueue
	lock sync.Mutex
}

type dmTaskQueue struct {
	baseTaskQueue
}

type dqTaskQueue struct {
	baseTaskQueue
}

func (queue *baseTaskQueue) Empty() bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	queue.atLock.Lock()
	defer queue.atLock.Unlock()
	return queue.unissuedTasks.Len() <= 0 && len(queue.activeTasks) <= 0
}

func (queue *baseTaskQueue) AddUnissuedTask(t *task) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	queue.unissuedTasks.PushBack(t)
}

func (queue *baseTaskQueue) FrontUnissuedTask() *task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	if queue.unissuedTasks.Len() <= 0 {
		log.Fatal("sorry, but the unissued task list is empty!")
		return nil
	}
	return queue.unissuedTasks.Front().Value.(*task)
}

func (queue *baseTaskQueue) PopUnissuedTask() *task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	if queue.unissuedTasks.Len() <= 0 {
		log.Fatal("sorry, but the unissued task list is empty!")
		return nil
	}
	ft := queue.unissuedTasks.Front()
	return queue.unissuedTasks.Remove(ft).(*task)
}

func (queue *baseTaskQueue) AddActiveTask(t *task) {
	queue.atLock.Lock()
	defer queue.atLock.Lock()
	ts := (*t).GetTs()
	_, ok := queue.activeTasks[ts]
	if ok {
		log.Fatalf("task with timestamp %d already in active task list!", ts)
	}
	queue.activeTasks[ts] = t
}

func (queue *baseTaskQueue) PopActiveTask(ts typeutil.Timestamp) *task {
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

func (queue *baseTaskQueue) TaskDoneTest(ts typeutil.Timestamp) bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if (*(e.Value.(*task))).GetTs() >= ts {
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

func (queue *ddTaskQueue) Enqueue(t *task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	// TODO: set Ts, ReqId, ProxyId
	queue.AddUnissuedTask(t)
	return nil
}

func (queue *dmTaskQueue) Enqueue(t *task) error {
	// TODO: set Ts, ReqId, ProxyId
	queue.AddUnissuedTask(t)
	return nil
}

func (queue *dqTaskQueue) Enqueue(t *task) error {
	// TODO: set Ts, ReqId, ProxyId
	queue.AddUnissuedTask(t)
	return nil
}

type taskScheduler struct {
	DdQueue *ddTaskQueue
	DmQueue *dmTaskQueue
	DqQueue *dqTaskQueue

	// tsAllocator, ReqIdAllocator
}

func (sched *taskScheduler) scheduleDdTask() *task {
	return sched.DdQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDmTask() *task {
	return sched.DmQueue.PopUnissuedTask()
}

func (sched *taskScheduler) scheduleDqTask() *task {
	return sched.DqQueue.PopUnissuedTask()
}

func (sched *taskScheduler) Start() error {
	go func() {
		for {
			if sched.DdQueue.Empty() {
				continue
			}
			t := sched.scheduleDdTask()
			if err := (*t).PreExecute(); err != nil {
				return
			}
			if err := (*t).Execute(); err != nil {
				return
			}
			if err := (*t).PostExecute(); err != nil {
				return
			}
			if err := (*t).WaitToFinish(); err != nil {
				return
			}
			if err := (*t).Notify(); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			if sched.DdQueue.Empty() {
				continue
			}
			t := sched.scheduleDmTask()
			if err := (*t).PreExecute(); err != nil {
				return
			}
			if err := (*t).Execute(); err != nil {
				return
			}
			if err := (*t).PostExecute(); err != nil {
				return
			}
			if err := (*t).WaitToFinish(); err != nil {
				return
			}
			if err := (*t).Notify(); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			if sched.DdQueue.Empty() {
				continue
			}
			t := sched.scheduleDqTask()
			if err := (*t).PreExecute(); err != nil {
				return
			}
			if err := (*t).Execute(); err != nil {
				return
			}
			if err := (*t).PostExecute(); err != nil {
				return
			}
			if err := (*t).WaitToFinish(); err != nil {
				return
			}
			if err := (*t).Notify(); err != nil {
				return
			}
		}
	}()
	return nil
}

func (sched *taskScheduler) TaskDoneTest(ts typeutil.Timestamp) bool {
	ddTaskDone := sched.DdQueue.TaskDoneTest(ts)
	dmTaskDone := sched.DmQueue.TaskDoneTest(ts)
	dqTaskDone := sched.DqQueue.TaskDoneTest(ts)
	return ddTaskDone && dmTaskDone && dqTaskDone
}
