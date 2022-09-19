package rootcoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/tso"

	"github.com/milvus-io/milvus/internal/allocator"
)

type IScheduler interface {
	Start()
	Stop()
	AddTask(t taskV2) error
}

type scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	idAllocator  allocator.GIDAllocator
	tsoAllocator tso.Allocator

	taskChan chan taskV2

	lock sync.Mutex
}

func newScheduler(ctx context.Context, idAllocator allocator.GIDAllocator, tsoAllocator tso.Allocator) *scheduler {
	ctx1, cancel := context.WithCancel(ctx)
	// TODO
	n := 1024 * 10
	return &scheduler{
		ctx:          ctx1,
		cancel:       cancel,
		idAllocator:  idAllocator,
		tsoAllocator: tsoAllocator,
		taskChan:     make(chan taskV2, n),
	}
}

func (s *scheduler) Start() {
	s.wg.Add(1)
	go s.taskLoop()
}

func (s *scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *scheduler) execute(task taskV2) {
	if err := task.Prepare(task.GetCtx()); err != nil {
		task.NotifyDone(err)
		return
	}
	err := task.Execute(task.GetCtx())
	task.NotifyDone(err)
}

func (s *scheduler) taskLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.taskChan:
			s.execute(task)
		}
	}
}

func (s *scheduler) setID(task taskV2) error {
	id, err := s.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	task.SetID(id)
	return nil
}

func (s *scheduler) setTs(task taskV2) error {
	ts, err := s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}
	task.SetTs(ts)
	return nil
}

func (s *scheduler) enqueue(task taskV2) {
	s.taskChan <- task
}

func (s *scheduler) AddTask(task taskV2) error {
	// make sure that setting ts and enqueue is atomic.
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.setID(task); err != nil {
		return err
	}
	if err := s.setTs(task); err != nil {
		return err
	}
	s.enqueue(task)
	return nil
}
