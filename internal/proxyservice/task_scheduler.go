package proxyservice

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type TaskScheduler struct {
	RegisterLinkTaskQueue                  TaskQueue
	RegisterNodeTaskQueue                  TaskQueue
	InvalidateCollectionMetaCacheTaskQueue TaskQueue

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)

	return &TaskScheduler{
		RegisterLinkTaskQueue:                  NewBaseTaskQueue(),
		RegisterNodeTaskQueue:                  NewBaseTaskQueue(),
		InvalidateCollectionMetaCacheTaskQueue: NewBaseTaskQueue(),
		ctx:                                    ctx1,
		cancel:                                 cancel,
	}
}

func (sched *TaskScheduler) scheduleRegisterLinkTask() task {
	return sched.RegisterLinkTaskQueue.PopTask()
}

func (sched *TaskScheduler) scheduleRegisterNodeTask() task {
	return sched.RegisterNodeTaskQueue.PopTask()
}

func (sched *TaskScheduler) scheduleInvalidateCollectionMetaCacheTask() task {
	return sched.InvalidateCollectionMetaCacheTaskQueue.PopTask()
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {
	span, ctx := trace.StartSpanFromContext(t.Ctx(),
		opentracing.Tags{
			"Type": t.Name(),
		})
	defer span.Finish()
	span.LogFields(oplog.String("scheduler process PreExecute", t.Name()))
	err := t.PreExecute(ctx)

	defer func() {
		trace.LogError(span, err)
		t.Notify(err)
	}()
	if err != nil {
		return
	}

	span.LogFields(oplog.String("scheduler process Execute", t.Name()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		return
	}
	span.LogFields(oplog.String("scheduler process PostExecute", t.Name()))
	err = t.PostExecute(ctx)
}

func (sched *TaskScheduler) registerLinkLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.RegisterLinkTaskQueue.Chan():
			if !sched.RegisterLinkTaskQueue.Empty() {
				t := sched.scheduleRegisterLinkTask()
				go sched.processTask(t, sched.RegisterLinkTaskQueue)
			}
		}
	}
}

func (sched *TaskScheduler) registerNodeLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.RegisterNodeTaskQueue.Chan():
			if !sched.RegisterNodeTaskQueue.Empty() {
				t := sched.scheduleRegisterNodeTask()
				go sched.processTask(t, sched.RegisterNodeTaskQueue)
			}
		}
	}
}

func (sched *TaskScheduler) invalidateCollectionMetaCacheLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.InvalidateCollectionMetaCacheTaskQueue.Chan():
			if !sched.InvalidateCollectionMetaCacheTaskQueue.Empty() {
				t := sched.scheduleInvalidateCollectionMetaCacheTask()
				go sched.processTask(t, sched.InvalidateCollectionMetaCacheTaskQueue)
			}
		}
	}
}

func (sched *TaskScheduler) Start() {
	sched.wg.Add(1)
	go sched.registerLinkLoop()

	sched.wg.Add(1)
	go sched.registerNodeLoop()

	sched.wg.Add(1)
	go sched.invalidateCollectionMetaCacheLoop()
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
