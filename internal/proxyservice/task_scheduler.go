package proxyservice

import (
	"context"
	"sync"
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
	var err error
	err = t.PreExecute()

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		return
	}

	err = t.Execute()
	if err != nil {
		return
	}
	err = t.PostExecute()
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
