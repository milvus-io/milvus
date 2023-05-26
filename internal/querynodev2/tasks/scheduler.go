package tasks

import (
	"context"
	"fmt"
	"sync"

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

	pool             *conc.Pool[any]
	runningThreadNum int
	cond             *sync.Cond
}

func NewScheduler() *Scheduler {
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxReceiveChanSize.GetAsInt()
	maxReadConcurrency := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt()
	return &Scheduler{
		searchProcessNum:   atomic.NewInt32(0),
		searchWaitQueue:    make(chan *SearchTask, maxWaitTaskNum),
		mergingSearchTasks: make([]*SearchTask, 0),
		mergedSearchTasks:  make(chan *SearchTask),
		// queryProcessQueue: make(chan),

		pool: conc.NewPool[any](maxReadConcurrency, ants.WithPreAlloc(true)),
		cond: sync.NewCond(&sync.Mutex{}),
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
		if len(s.mergingSearchTasks) > 0 { // wait for an idle worker or a new task
			task := s.mergingSearchTasks[0]
			select {
			case <-ctx.Done():
				return

			case task = <-s.searchWaitQueue:
				s.schedule(task)

			case s.mergedSearchTasks <- task:
				s.mergingSearchTasks = s.mergingSearchTasks[1:]
			}
		} else { // wait for a new task if no task
			select {
			case <-ctx.Done():
				return

			case task := <-s.searchWaitQueue:
				s.schedule(task)
			}
		}
	}
}

func (s *Scheduler) schedule(task Task) {
	// add this task
	if err := task.Canceled(); err != nil {
		task.Done(err)
		return
	}
	s.mergeTasks(task)
	metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()

	mergeLimit := paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt()
	mergeCount := 1

	// try to merge the coming tasks
outer:
	for mergeCount < mergeLimit {
		select {
		case t := <-s.searchWaitQueue:
			if err := t.Canceled(); err != nil {
				t.Done(err)
				continue
			}
			s.mergeTasks(t)
			mergeCount++
			metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		default:
			break outer
		}
	}

	// submit existing tasks to the pool
	processedCount := 0
processOuter:
	for i := range s.mergingSearchTasks {
		select {
		case s.mergedSearchTasks <- s.mergingSearchTasks[i]:
			processedCount++
		default:
			break processOuter
		}
	}
	s.mergingSearchTasks = s.mergingSearchTasks[processedCount:]
	metrics.QueryNodeReadTaskReadyLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(processedCount))
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
	s.cond.L.Lock()
	for s.runningThreadNum >= s.pool.Cap() {
		s.cond.Wait()
	}
	s.runningThreadNum += t.Weight()
	s.cond.L.Unlock()

	s.pool.Submit(func() (any, error) {
		defer func() {
			s.cond.L.Lock()
			defer s.cond.L.Unlock()
			s.runningThreadNum -= t.Weight()
			if s.runningThreadNum < s.pool.Cap() {
				s.cond.Broadcast()
			}
		}()

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
