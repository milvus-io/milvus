package indexnode

import (
	"container/list"
	"context"
	"errors"
	"log"
	"sync"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/zilliztech/milvus-distributed/internal/kv"
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
	tryToRemoveUselessIndexBuildTask(indexID UniqueID)
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
		log.Println("FrontUnissuedTask sorry, but the unissued task list is empty!")
		return nil
	}

	return queue.unissuedTasks.Front().Value.(task)
}

func (queue *BaseTaskQueue) PopUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Println("PopUnissuedtask sorry, but the unissued task list is empty!")
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

func (queue *BaseTaskQueue) tryToRemoveUselessIndexBuildTask(indexID UniqueID) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	var next *list.Element
	for e := queue.unissuedTasks.Front(); e != nil; e = next {
		next = e.Next()
		indexBuildTask, ok := e.Value.(*IndexBuildTask)
		if !ok {
			continue
		}
		if indexBuildTask.cmd.Req.IndexID == indexID {
			queue.unissuedTasks.Remove(e)
		}
	}
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	err := t.OnEnqueue()
	if err != nil {
		return err
	}
	return queue.addUnissuedTask(t)
}

type IndexBuildTaskQueue struct {
	BaseTaskQueue
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
	IndexBuildQueue TaskQueue

	buildParallel int
	kv            kv.Base
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewTaskScheduler(ctx context.Context,
	kv kv.Base) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		kv:            kv,
		ctx:           ctx1,
		cancel:        cancel,
		buildParallel: 1, // default value
	}
	s.IndexBuildQueue = NewIndexBuildTaskQueue(s)

	return s, nil
}

func (sched *TaskScheduler) setParallelism(parallel int) {
	if parallel <= 0 {
		log.Println("can not set parallelism to less than zero!")
		return
	}
	sched.buildParallel = parallel
}

func (sched *TaskScheduler) scheduleIndexBuildTask() []task {
	ret := make([]task, 0)
	for i := 0; i < sched.buildParallel; i++ {
		t := sched.IndexBuildQueue.PopUnissuedTask()
		if t == nil {
			return ret
		}
		ret = append(ret, t)
	}
	return ret
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {
	span, ctx := trace.StartSpanFromContext(t.Ctx(),
		opentracing.Tags{
			"Type": t.Name(),
			"ID":   t.ID(),
		})
	defer span.Finish()
	span.LogFields(oplog.Int64("scheduler process PreExecute", t.ID()))
	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
		// log.Printf("notify with error: %v", err)
	}()
	if err != nil {
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.Int64("scheduler process AddActiveTask", t.ID()))
	q.AddActiveTask(t)
	// log.Printf("task add to active list ...")
	defer func() {
		span.LogFields(oplog.Int64("scheduler process PopActiveTask", t.ID()))
		q.PopActiveTask(t.ID())
		// log.Printf("pop from active list ...")
	}()

	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		log.Printf("execute definition task failed, error = %v", err)
		return
	}
	// log.Printf("task execution done ...")
	span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
	err = t.PostExecute(ctx)
	// log.Printf("post execute task done ...")
}

func (sched *TaskScheduler) indexBuildLoop() {
	log.Println("index build loop ...")
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.IndexBuildQueue.utChan():
			if !sched.IndexBuildQueue.utEmpty() {
				tasks := sched.scheduleIndexBuildTask()
				var wg sync.WaitGroup
				for _, t := range tasks {
					wg.Add(1)
					go func(group *sync.WaitGroup, t task) {
						defer group.Done()
						sched.processTask(t, sched.IndexBuildQueue)
					}(&wg, t)
				}
				wg.Wait()
			}
		}
	}
}

func (sched *TaskScheduler) Start() error {

	sched.wg.Add(1)
	go sched.indexBuildLoop()
	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
