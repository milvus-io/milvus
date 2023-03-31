package tasks

import (
	"context"
	"fmt"
	"runtime"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/util/conc"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
)

const (
	MaxProcessTaskNum = 1024 * 10
)

type Scheduler struct {
	searchProcessNum  *atomic.Int32
	searchWaitQueue   chan *SearchTask
	mergedSearchTasks typeutil.Set[*SearchTask]

	queryProcessQueue chan *QueryTask
	queryWaitQueue    chan *QueryTask

	pool *conc.Pool
}

func NewScheduler() *Scheduler {
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxReceiveChanSize.GetAsInt()
	pool := conc.NewPool(runtime.GOMAXPROCS(0)*2, ants.WithPreAlloc(true))
	return &Scheduler{
		searchProcessNum:  atomic.NewInt32(0),
		searchWaitQueue:   make(chan *SearchTask, maxWaitTaskNum),
		mergedSearchTasks: typeutil.NewSet[*SearchTask](),
		// queryProcessQueue: make(chan),

		pool: pool,
	}
}

func (s *Scheduler) Add(task Task) bool {
	switch t := task.(type) {
	case *SearchTask:
		select {
		case s.searchWaitQueue <- t:
			t.tr.RecordSpan()
			metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
		default:
			return false
		}
	}

	return true
}

// schedule all tasks in the order:
// try execute merged tasks
// try execute waitting tasks
func (s *Scheduler) Schedule(ctx context.Context) {
	for {
		if len(s.mergedSearchTasks) > 0 {
			for task := range s.mergedSearchTasks {
				if !s.tryPromote(task) {
					break
				}

				inQueueDuration := task.tr.RecordSpan()
				metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
					fmt.Sprint(paramtable.GetNodeID()),
					metrics.SearchLabel).
					Observe(float64(inQueueDuration.Milliseconds()))
				s.process(task)
				s.mergedSearchTasks.Remove(task)
			}
		}

		select {
		case <-ctx.Done():
			return

		case t := <-s.searchWaitQueue:
			if err := t.Canceled(); err != nil {
				t.Done(err)
				continue
			}

			// Now we have no enough resource to execute this task,
			// just wait and try to merge it with another tasks
			if !s.tryPromote(t) {
				s.mergeTasks(t)
			} else {
				s.process(t)
			}

			metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		}

		metrics.QueryNodeReadTaskReadyLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(s.mergedSearchTasks.Len()))
	}
}

func (s *Scheduler) tryPromote(t Task) bool {
	current := s.searchProcessNum.Load()
	if current >= MaxProcessTaskNum ||
		!s.searchProcessNum.CAS(current, current+1) {
		return false
	}

	return true
}

func (s *Scheduler) process(t Task) {
	s.pool.Submit(func() (interface{}, error) {
		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()

		err := t.Execute()
		t.Done(err)
		s.searchProcessNum.Dec()

		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		return nil, err
	})
}

func (s *Scheduler) mergeTasks(t Task) {
	switch t := t.(type) {
	case *SearchTask:
		merged := false
		for task := range s.mergedSearchTasks {
			if task.Merge(t) {
				merged = true
				break
			}
		}
		if !merged {
			s.mergedSearchTasks.Insert(t)
		}
	}
}
