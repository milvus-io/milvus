package querynode

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/log"
)

type taskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg    sync.WaitGroup
	queue taskQueue
}

func newTaskScheduler(ctx context.Context) *taskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &taskScheduler{
		ctx:    ctx1,
		cancel: cancel,
	}
	s.queue = newLoadAndReleaseTaskQueue(s)
	return s
}

func (s *taskScheduler) processTask(t task, q taskQueue) {
	// TODO: ctx?
	err := t.PreExecute(s.ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Error(err.Error())
		return
	}

	q.AddActiveTask(t)
	defer func() {
		q.PopActiveTask(t.ID())
	}()

	err = t.Execute(s.ctx)
	if err != nil {
		log.Error(err.Error())
		return
	}
	err = t.PostExecute(s.ctx)
}

func (s *taskScheduler) loadAndReleaseLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.queue.utChan():
			if !s.queue.utEmpty() {
				t := s.queue.PopUnissuedTask()
				s.processTask(t, s.queue)
			}
		}
	}
}

func (s *taskScheduler) Start() {
	s.wg.Add(1)
	go s.loadAndReleaseLoop()
}

func (s *taskScheduler) Close() {
	s.cancel()
	s.wg.Wait()
}
