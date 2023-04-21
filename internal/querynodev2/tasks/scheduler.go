package tasks

import (
	"context"
	"fmt"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/panjf2000/ants/v2"
)

const (
	MaxProcessTaskNum = 1024 * 10
)

type Scheduler struct {
	searchProcessNum   *atomic.Int32
	searchWaitQueue    chan *SearchTask
	mergingSearchTasks []*SearchTask
	mergedSearchTasks  chan *SearchTask

	queryProcessQueue chan *QueryTask
	queryWaitQueue    chan *QueryTask

	pool *conc.Pool[any]
}

func NewScheduler() *Scheduler {
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxReceiveChanSize.GetAsInt()
	maxReadConcurrency := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt()
	return &Scheduler{
		searchProcessNum:   atomic.NewInt32(0),
		searchWaitQueue:    make(chan *SearchTask, maxWaitTaskNum),
		mergingSearchTasks: make([]*SearchTask, 0),
		mergedSearchTasks:  make(chan *SearchTask, maxReadConcurrency),
		// queryProcessQueue: make(chan),

		pool: conc.NewPool[any](maxReadConcurrency, ants.WithPreAlloc(true)),
	}
}

func (s *Scheduler) Add(task Task) bool {
	switch t := task.(type) {
	case *SearchTask:
		t.tr.RecordSpan()
		select {
		case s.searchWaitQueue <- t:
			metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
		default:
			return false
		}
	}

	return true
}

// schedule all tasks in the order:
// try execute merged tasks
// try execute waiting tasks
func (s *Scheduler) Schedule(ctx context.Context) {
	go s.processAll(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case t := <-s.searchWaitQueue:
			if err := t.Canceled(); err != nil {
				t.Done(err)
				continue
			}

			mergeCount := 0
			mergeLimit := paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt()
		outer:
			for i := 0; i < mergeLimit; i++ {
				s.mergeTasks(t)
				mergeCount++
				metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()

				select {
				case t = <-s.searchWaitQueue:
					// Continue the loop to merge task
				default:
					break outer
				}
			}

			for i := range s.mergingSearchTasks {
				s.mergedSearchTasks <- s.mergingSearchTasks[i]
			}
			s.mergingSearchTasks = s.mergingSearchTasks[:0]
			metrics.QueryNodeReadTaskReadyLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(mergeCount))
		}
	}
}

func (s *Scheduler) processAll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case task := <-s.mergedSearchTasks:
			inQueueDuration := task.tr.RecordSpan()
			metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				metrics.SearchLabel).
				Observe(float64(inQueueDuration.Milliseconds()))

			s.process(task)
		}
	}
}

func (s *Scheduler) process(t Task) {
	s.pool.Submit(func() (any, error) {
		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()

		err := t.Execute()
		t.Done(err)

		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		return nil, err
	})
}

// mergeTasks merge the given task with one of merged tasks,
func (s *Scheduler) mergeTasks(t Task) {
	switch t := t.(type) {
	case *SearchTask:
		merged := false
		for _, task := range s.mergingSearchTasks {
			if task.Merge(t) {
				merged = true
				break
			}
		}
		if !merged {
			s.mergingSearchTasks = append(s.mergingSearchTasks, t)
		}
	}
}
