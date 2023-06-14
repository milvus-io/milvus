package tasks

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	maxReceiveChanBatchConsumeNum = 100
)

// newScheduler create a scheduler with given schedule policy.
func newScheduler(policy schedulePolicy) Scheduler {
	maxReadConcurrency := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt()
	maxReceiveChanSize := paramtable.Get().QueryNodeCfg.MaxReceiveChanSize.GetAsInt()
	log.Info("query node use concurrent safe scheduler", zap.Int("max_concurrency", maxReadConcurrency))
	return &scheduler{
		policy:           policy,
		receiveChan:      make(chan addTaskReq, maxReceiveChanSize),
		execChan:         make(chan Task),
		pool:             conc.NewPool[any](maxReadConcurrency, conc.WithPreAlloc(true)),
		schedulerCounter: schedulerCounter{},
	}
}

type addTaskReq struct {
	task Task
	err  chan<- error
}

// scheduler is a general concurrent safe scheduler implementation by wrapping a schedule policy.
type scheduler struct {
	policy      schedulePolicy
	receiveChan chan addTaskReq
	execChan    chan Task
	pool        *conc.Pool[any]
	schedulerCounter
}

// Add a new task into scheduler,
// error will be returned if scheduler reaches some limit.
func (s *scheduler) Add(task Task) (err error) {
	errCh := make(chan error, 1)

	// TODO: add operation should be fast, is UnsolveLen metric unnesscery?
	metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()

	// start a new in queue span and send task to add chan
	s.receiveChan <- addTaskReq{
		task: task,
		err:  errCh,
	}
	err = <-errCh

	metrics.QueryNodeReadTaskUnsolveLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
	return
}

// Start schedule the owned task asynchronously and continuously.
// Start should be only call once.
func (s *scheduler) Start(ctx context.Context) {
	// Start a background task executing loop.
	go s.exec(ctx)

	// Begin to schedule tasks.
	go s.schedule(ctx)
}

// schedule the owned task asynchronously and continuously.
func (s *scheduler) schedule(ctx context.Context) {
	var task Task
	for {
		s.setupReadyLenMetric()

		var execChan chan Task
		task, execChan = s.setupExecListener(task)

		select {
		case <-ctx.Done():
			log.Warn("unexpected quit of schedule loop")
			return
		case req := <-s.receiveChan:
			// Receive add operation request and return the process result.
			// And consume recv chan as much as possible.
			s.consumeRecvChan(req, maxReceiveChanBatchConsumeNum)
		case execChan <- task:
			// Task sent, drop the ownership of sent task.
			// And produce new task into execChan as much as possible.
			task = s.produceExecChan()
		}
	}
}

// consumeRecvChan consume the recv chan as much as possible.
func (s *scheduler) consumeRecvChan(req addTaskReq, limit int) {
	// Check the dynamic wait task limit.
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.GetAsInt64()
	if !s.handleAddTaskRequest(req, maxWaitTaskNum) {
		return
	}

	// consume the add chan until reaching the batch operation limit
	for i := 1; i < limit; i++ {
		select {
		case req := <-s.receiveChan:
			if !s.handleAddTaskRequest(req, maxWaitTaskNum) {
				return
			}
		default:
			return
		}
	}
}

// HandleAddTaskRequest handle a add task request.
// Return true if the process can be continued.
func (s *scheduler) handleAddTaskRequest(req addTaskReq, maxWaitTaskNum int64) bool {
	if err := req.task.Canceled(); err != nil {
		log.Warn("task canceled before enqueue", zap.Error(err))
		req.err <- err
	} else {
		// Push the task into the policy to schedule and update the counter of the ready queue.
		nq := req.task.NQ()
		newTaskAdded, err := s.policy.Push(req.task)
		if err == nil {
			s.updateWaitingTaskCounter(int64(newTaskAdded), nq)
		}
		req.err <- err
	}

	// Continue processing if the queue isn't reach the max limit.
	return s.GetWaitingTaskTotal() < maxWaitTaskNum
}

// produceExecChan produce task from scheduler into exec chan as much as possible
func (s *scheduler) produceExecChan() Task {
	var task Task
	for {
		var execChan chan Task
		task, execChan = s.setupExecListener(task)

		select {
		case execChan <- task:
			// Task sent, drop the ownership of sent task.
			task = nil
		default:
			return task
		}
	}
}

// exec exec the ready task in background continuously.
func (s *scheduler) exec(ctx context.Context) {
	log.Info("start execute loop")
	for {
		select {
		case <-ctx.Done():
			log.Warn("unexpected quit of exec loop")
			return
		case t := <-s.execChan:
			// Skip this task if task is canceled.
			if err := t.Canceled(); err != nil {
				log.Warn("task canceled before executing", zap.Error(err))
				t.Done(err)
				continue
			}
			if err := t.PreExecute(); err != nil {
				log.Warn("failed to pre-execute task", zap.Error(err))
				t.Done(err)
				continue
			}

			s.pool.Submit(func() (any, error) {
				// Update concurrency metric and notify task done.
				metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
				collector.Counter.Inc(metricsinfo.ExecuteQueueType, 1)

				err := t.Execute()

				// Update all metric after task finished.
				s.updateWaitingTaskCounter(-1, -t.NQ())
				metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
				collector.Counter.Dec(metricsinfo.ExecuteQueueType, -1)

				// Notify task done.
				t.Done(err)
				return nil, err
			})
		}
	}
}

// setupExecListener setup the execChan and next task to run.
func (s *scheduler) setupExecListener(lastWaitingTask Task) (Task, chan Task) {
	var execChan chan Task
	if lastWaitingTask == nil {
		// No task is waiting to send to execChan, schedule a new one from queue.
		lastWaitingTask = s.policy.Pop()
	}
	if lastWaitingTask != nil {
		// Try to sent task to execChan if there is a task ready to run.
		execChan = s.execChan
	}
	return lastWaitingTask, execChan
}

// setupReadyLenMetric update the read task ready len metric.
func (s *scheduler) setupReadyLenMetric() {
	waitingTaskCount := s.GetWaitingTaskTotal()

	// Update the ReadyQueue counter for quota.
	collector.Counter.Set(metricsinfo.ReadyQueueType, waitingTaskCount)
	// Record the waiting task length of policy as ready task metric.
	metrics.QueryNodeReadTaskReadyLen.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(waitingTaskCount))
}

// scheduler counter implement, concurrent safe.
type schedulerCounter struct {
	waitingTaskTotal   atomic.Int64
	waitingTaskTotalNQ atomic.Int64
}

// GetWaitingTaskTotal get ready task counts.
func (s *schedulerCounter) GetWaitingTaskTotal() int64 {
	return s.waitingTaskTotal.Load()
}

// GetWaitingTaskTotalNQ get ready task NQ.
func (s *schedulerCounter) GetWaitingTaskTotalNQ() int64 {
	return s.waitingTaskTotalNQ.Load()
}

// updateWaitingTaskCounter update the waiting task counter for observing.
func (s *schedulerCounter) updateWaitingTaskCounter(num int64, nq int64) {
	s.waitingTaskTotal.Add(num)
	s.waitingTaskTotalNQ.Add(nq)
}
