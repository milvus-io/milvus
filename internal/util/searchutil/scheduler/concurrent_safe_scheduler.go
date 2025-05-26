package scheduler

import (
	"fmt"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		gpuPool:          conc.NewPool[any](paramtable.Get().QueryNodeCfg.MaxGpuReadConcurrency.GetAsInt(), conc.WithPreAlloc(true)),
		schedulerCounter: schedulerCounter{},
		lifetime:         lifetime.NewLifetime(lifetime.Initializing),
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
	gpuPool     *conc.Pool[any]

	// wg is the waitgroup for internal worker goroutine
	wg sync.WaitGroup
	// lifetime controls scheduler State & make sure all requests accepted will be processed
	lifetime lifetime.Lifetime[lifetime.State]

	schedulerCounter
}

// Add a new task into scheduler,
// error will be returned if scheduler reaches some limit.
func (s *scheduler) Add(task Task) (err error) {
	if err := s.lifetime.Add(lifetime.IsWorking); err != nil {
		return err
	}
	defer s.lifetime.Done()

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
func (s *scheduler) Start() {
	s.wg.Add(2)

	// Start a background task executing loop.
	go s.exec()

	// Begin to schedule tasks.
	go s.schedule()

	s.lifetime.SetState(lifetime.Working)
}

func (s *scheduler) Stop() {
	s.lifetime.SetState(lifetime.Stopped)
	// wait all accepted Add done
	s.lifetime.Wait()
	// close receiveChan start stopping process for `schedule`
	close(s.receiveChan)
	// wait workers quit
	s.wg.Wait()
	if s.pool != nil {
		s.pool.Release()
	}
	if s.gpuPool != nil {
		s.gpuPool.Release()
	}
}

// schedule the owned task asynchronously and continuously.
func (s *scheduler) schedule() {
	defer s.wg.Done()
	var task Task
	for {
		s.setupReadyLenMetric()

		var execChan chan Task
		nq := int64(0)
		task, nq, execChan = s.setupExecListener(task)

		select {
		case req, ok := <-s.receiveChan:
			if !ok {
				log.Info("receiveChan closed, processing remaining request")
				// drain policy maintained task
				for task != nil {
					execChan <- task
					s.updateWaitingTaskCounter(-1, -nq)
					task = s.produceExecChan()
				}
				log.Info("all task put into exeChan, schedule worker exit")
				close(s.execChan)
				return
			}
			// Receive add operation request and return the process result.
			// And consume recv chan as much as possible.
			s.consumeRecvChan(req, maxReceiveChanBatchConsumeNum)
		case execChan <- task:
			// Task sent, drop the ownership of sent task.
			// Update waiting task counter.
			s.updateWaitingTaskCounter(-1, -nq)
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
		case req, ok := <-s.receiveChan:
			if !ok {
				return
			}
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
		nq := int64(0)
		task, nq, execChan = s.setupExecListener(task)

		select {
		case execChan <- task:
			// Update waiting task counter.
			s.updateWaitingTaskCounter(-1, -nq)
			// Task sent, drop the ownership of sent task.
			task = nil
		default:
			return task
		}
	}
}

// exec exec the ready task in background continuously.
func (s *scheduler) exec() {
	defer s.wg.Done()
	log.Info("start execute loop")
	for {
		t, ok := <-s.execChan
		if !ok {
			log.Info("scheduler execChan closed, worker exit")
			return
		}
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

		s.getPool(t).Submit(func() (any, error) {
			// Update concurrency metric and notify task done.
			metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
			collector.Counter.Inc(metricsinfo.ExecuteQueueType)

			err := t.Execute()

			// Update all metric after task finished.
			metrics.QueryNodeReadTaskConcurrency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
			collector.Counter.Dec(metricsinfo.ExecuteQueueType)

			// Notify task done.
			t.Done(err)
			return nil, err
		})
	}
}

func (s *scheduler) getPool(t Task) *conc.Pool[any] {
	if t.IsGpuIndex() {
		return s.gpuPool
	}

	return s.pool
}

// setupExecListener setup the execChan and next task to run.
func (s *scheduler) setupExecListener(lastWaitingTask Task) (Task, int64, chan Task) {
	var execChan chan Task
	nq := int64(0)
	if lastWaitingTask == nil {
		// No task is waiting to send to execChan, schedule a new one from queue.
		lastWaitingTask = s.policy.Pop()
	}
	if lastWaitingTask != nil {
		// Try to sent task to execChan if there is a task ready to run.
		execChan = s.execChan
		nq = lastWaitingTask.NQ()
	}

	return lastWaitingTask, nq, execChan
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
