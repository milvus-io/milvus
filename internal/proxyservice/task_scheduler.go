package proxyservice

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type taskScheduler struct {
	RegisterLinkTaskQueue                  taskQueue
	RegisterNodeTaskQueue                  taskQueue
	InvalidateCollectionMetaCacheTaskQueue taskQueue

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func newTaskScheduler(ctx context.Context) *taskScheduler {
	ctx1, cancel := context.WithCancel(ctx)

	return &taskScheduler{
		RegisterLinkTaskQueue:                  newBaseTaskQueue(),
		RegisterNodeTaskQueue:                  newBaseTaskQueue(),
		InvalidateCollectionMetaCacheTaskQueue: newBaseTaskQueue(),
		ctx:                                    ctx1,
		cancel:                                 cancel,
	}
}

func (sched *taskScheduler) scheduleRegisterLinkTask() task {
	return sched.RegisterLinkTaskQueue.PopTask()
}

func (sched *taskScheduler) scheduleRegisterNodeTask() task {
	return sched.RegisterNodeTaskQueue.PopTask()
}

func (sched *taskScheduler) scheduleInvalidateCollectionMetaCacheTask() task {
	return sched.InvalidateCollectionMetaCacheTaskQueue.PopTask()
}

func (sched *taskScheduler) processTask(t task, q taskQueue) {
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

func (sched *taskScheduler) registerLinkLoop() {
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

func (sched *taskScheduler) registerNodeLoop() {
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

func (sched *taskScheduler) invalidateCollectionMetaCacheLoop() {
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

func (sched *taskScheduler) Start() {
	sched.wg.Add(1)
	go sched.registerLinkLoop()

	sched.wg.Add(1)
	go sched.registerNodeLoop()

	sched.wg.Add(1)
	go sched.invalidateCollectionMetaCacheLoop()
}

func (sched *taskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
