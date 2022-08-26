package typeutil

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
)

// GroupScheduler schedules requests,
// all requests within the same partition & node will run sequentially,
// with group commit
const (
	taskQueueCap = 16
	waitQueueCap = 128
)

type MergeableTask[K comparable, R any] interface {
	ID() K
	Execute() error
	Merge(other MergeableTask[K, R])
	SetResult(R)
	SetError(error)
	Done()
	Wait() (R, error)
}

type GroupScheduler[K comparable, R any] struct {
	stopCh chan struct{}
	wg     sync.WaitGroup

	processors *ConcurrentSet[K]              // Tasks of having processor
	queues     map[K]chan MergeableTask[K, R] // TaskID -> Queue
	waitQueue  chan MergeableTask[K, R]
}

func NewGroupScheduler[K comparable, R any]() *GroupScheduler[K, R] {
	return &GroupScheduler[K, R]{
		stopCh:     make(chan struct{}),
		processors: NewConcurrentSet[K](),
		queues:     make(map[K]chan MergeableTask[K, R]),
		waitQueue:  make(chan MergeableTask[K, R], waitQueueCap),
	}
}

func (scheduler *GroupScheduler[K, R]) Start(ctx context.Context) {
	scheduler.wg.Add(1)
	go scheduler.schedule(ctx)
}

func (scheduler *GroupScheduler[K, R]) Stop() {
	close(scheduler.stopCh)
	scheduler.wg.Wait()
}

func (scheduler *GroupScheduler[K, R]) schedule(ctx context.Context) {
	defer scheduler.wg.Done()

	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("GroupScheduler stopped due to context canceled")
				return

			case <-scheduler.stopCh:
				log.Info("GroupScheduler stopped")
				return

			case task := <-scheduler.waitQueue:
				queue, ok := scheduler.queues[task.ID()]
				if !ok {
					queue = make(chan MergeableTask[K, R], taskQueueCap)
					scheduler.queues[task.ID()] = queue
				}
			outer:
				for {
					select {
					case queue <- task:
						break outer
					default: // Queue full, flush and retry
						scheduler.startProcessor(task.ID(), queue)
					}
				}

			case <-ticker.C:
				for id, queue := range scheduler.queues {
					if len(queue) > 0 {
						scheduler.startProcessor(id, queue)
					} else {
						// Release resource if no job for the task
						delete(scheduler.queues, id)
					}
				}
			}
		}
	}()
}

func (scheduler *GroupScheduler[K, R]) isStopped() bool {
	select {
	case <-scheduler.stopCh:
		return true
	default:
		return false
	}
}

func (scheduler *GroupScheduler[K, R]) Add(job MergeableTask[K, R]) {
	scheduler.waitQueue <- job
}

func (scheduler *GroupScheduler[K, R]) startProcessor(id K, queue chan MergeableTask[K, R]) {
	if scheduler.isStopped() {
		return
	}
	if !scheduler.processors.Insert(id) {
		return
	}

	scheduler.wg.Add(1)
	go scheduler.processQueue(id, queue)
}

// processQueue processes tasks in the given queue,
// it only processes tasks with the number of the length of queue at the time,
// to avoid leaking goroutines
func (scheduler *GroupScheduler[K, R]) processQueue(id K, queue chan MergeableTask[K, R]) {
	defer scheduler.wg.Done()
	defer scheduler.processors.Remove(id)

	len := len(queue)
	buffer := make([]MergeableTask[K, R], len)
	for i := range buffer {
		buffer[i] = <-queue
		if i > 0 {
			buffer[0].Merge(buffer[i])
		}
	}

	buffer[0].Execute()
	buffer[0].Done()
	result, err := buffer[0].Wait()
	for _, buffer := range buffer[1:] {
		buffer.SetResult(result)
		buffer.SetError(err)
		buffer.Done()
	}
}
