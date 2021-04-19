package indexservice

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
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
	tryToRemoveUselessIndexAddTask(indexID UniqueID)
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
		log.Warn("indexservice", zap.Int64("task with ID already in active task list!", tID))
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
	log.Debug("indexservice", zap.Int64("sorry, but the ID was not found in the active task list!", tID))
	return nil
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	tID, _ := queue.sched.idAllocator.AllocOne()
	log.Debug("indexservice", zap.Int64("[Builder] allocate reqID", tID))
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

func (queue *IndexAddTaskQueue) Enqueue(t task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.BaseTaskQueue.Enqueue(t)
}

// Note: tryToRemoveUselessIndexAddTask must be called by DropIndex
func (queue *IndexAddTaskQueue) tryToRemoveUselessIndexAddTask(indexID UniqueID) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	var next *list.Element
	for e := queue.unissuedTasks.Front(); e != nil; e = next {
		next = e.Next()
		indexAddTask, ok := e.Value.(*IndexAddTask)
		if !ok {
			continue
		}
		if indexAddTask.req.IndexID == indexID {
			queue.unissuedTasks.Remove(e)
		}
	}
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

type TaskScheduler struct {
	IndexAddQueue TaskQueue

	idAllocator *allocator.GlobalIDAllocator
	metaTable   *metaTable
	kv          kv.Base

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context,
	idAllocator *allocator.GlobalIDAllocator,
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
	return s, nil
}

func (sched *TaskScheduler) scheduleIndexAddTask() task {
	return sched.IndexAddQueue.PopUnissuedTask()
}

//func (sched *TaskScheduler) scheduleIndexBuildClient() indexnode.Interface {
//	return sched.IndexAddQueue.PopUnissuedTask()
//}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {
	span, ctx := trace.StartSpanFromContext(t.Ctx(),
		opentracing.Tags{
			"Type": t.Name(),
		})
	defer span.Finish()
	span.LogFields(oplog.String("scheduler process PreExecute", t.Name()))
	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
		log.Debug("indexservice", zap.String("notify with error", err.Error()))
	}()
	if err != nil {
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.String("scheduler process AddActiveTask", t.Name()))
	q.AddActiveTask(t)
	defer func() {
		span.LogFields(oplog.String("scheduler process PopActiveTask", t.Name()))
		q.PopActiveTask(t.ID())
	}()

	span.LogFields(oplog.String("scheduler process Execute", t.Name()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		return
	}
	span.LogFields(oplog.String("scheduler process PostExecute", t.Name()))
	err = t.PostExecute(ctx)
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

	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
