package tasks

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/atomic"
)

const (
	MaxProcessTaskNum = 1024 * 10
)

type Scheduler struct {
	searchWaitQueue    chan *SearchTask
	mergingSearchTasks []*SearchTask
	mergedSearchTasks  chan *SearchTask

	pool               *conc.Pool[any]
	waitingTaskTotalNQ atomic.Int64
}

func NewScheduler() *Scheduler {
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxReceiveChanSize.GetAsInt()
	maxReadConcurrency := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt()
	return &Scheduler{
		searchWaitQueue:    make(chan *SearchTask, maxWaitTaskNum),
		mergingSearchTasks: make([]*SearchTask, 0),
		mergedSearchTasks:  make(chan *SearchTask),

		pool:               conc.NewPool[any](maxReadConcurrency, conc.WithPreAlloc(true)),
		waitingTaskTotalNQ: *atomic.NewInt64(0),
	}
}

func (s *Scheduler) Add(task Task) bool {
	switch t := task.(type) {
	case *SearchTask:
		t.tr.RecordSpan()
		select {
		case s.searchWaitQueue <- t:
			collector.Counter.Inc(metricsinfo.ReadyQueueType, 1)
			s.waitingTaskTotalNQ.Add(t.nq)
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
				collector.Counter.Dec(metricsinfo.ReadyQueueType, 1)
				s.schedule(task)

			case s.mergedSearchTasks <- task:
				s.mergingSearchTasks = s.mergingSearchTasks[1:]
			}
		} else { // wait for a new task if no task
			select {
			case <-ctx.Done():
				return

			case task := <-s.searchWaitQueue:
				collector.Counter.Dec(metricsinfo.ReadyQueueType, 1)
				s.schedule(task)
			}
		}
	}
}

func (s *Scheduler) schedule(task Task) {
	collector.Counter.Inc(metricsinfo.ExecuteQueueType, 1)
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
	s.pool.Submit(func() (any, error) {
		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
		err := t.PreExecute()
		if err != nil {
			return nil, err
		}

		err = t.Execute()
		t.Done(err)

		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		return nil, err
	})

	switch t := t.(type) {
	case *SearchTask:
		s.waitingTaskTotalNQ.Sub(t.nq)
	}
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

func (s *Scheduler) GetWaitingTaskTotalNQ() int64 {
	return s.waitingTaskTotalNQ.Load()
}
