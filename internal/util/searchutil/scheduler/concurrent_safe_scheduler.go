package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	maxReceiveChanBatchConsumeNum = 100

	readTaskQueueOutcomeScheduled = "scheduled"
	readTaskQueueOutcomeExpired   = "expired"
	readTaskQueueOutcomeCleared   = "cleared"
)

// newScheduler create a scheduler with given schedule policy.
func newScheduler(policy schedulePolicy) Scheduler {
	maxReadConcurrency := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt()
	log.Info("query node use concurrent safe scheduler", zap.Int("max_concurrency", maxReadConcurrency))
	return &scheduler{
		policy:           policy,
		receiveChan:      make(chan addTaskReq),
		clearChan:        make(chan clearQueuedReq),
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

type clearQueuedReq struct {
	filter TaskFilter
	reason string
	resp   chan<- clearQueuedResp
}

type clearQueuedResp struct {
	result ClearResult
	err    error
}

// scheduler is a general concurrent safe scheduler implementation by wrapping a schedule policy.
type scheduler struct {
	policy      schedulePolicy
	receiveChan chan addTaskReq
	clearChan   chan clearQueuedReq
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

	req := addTaskReq{
		task: task,
		err:  errCh,
	}

	// start a new in queue span and send task to add chan
	ctx := task.Context()
	select {
	case s.receiveChan <- req:
		err = <-errCh
	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}

func (s *scheduler) ClearQueued(ctx context.Context, filter TaskFilter, reason string) (ClearResult, error) {
	if err := s.lifetime.Add(lifetime.IsWorking); err != nil {
		return ClearResult{}, err
	}
	defer s.lifetime.Done()

	respCh := make(chan clearQueuedResp, 1)
	select {
	case s.clearChan <- clearQueuedReq{filter: filter, reason: reason, resp: respCh}:
		resp := <-respCh
		return resp.result, resp.err
	case <-ctx.Done():
		return ClearResult{}, ctx.Err()
	}
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
	var task *queuedTask
	for {
		s.setupReadyLenMetric()

		var execChan chan Task
		var execTask Task
		nq := int64(0)
		now := time.Now()
		task, nq, execChan = s.setupExecListener(task, now)
		if task.valid() {
			execTask = task.Task
		}

		select {
		case req, ok := <-s.receiveChan:
			if !ok {
				log.Info("receiveChan closed, processing remaining request")
				// drain policy maintained task
				for task.valid() {
					execChan <- task.Task
					s.updateWaitingTaskCounter(-1, -nq)
					task = s.produceExecChan(now)
				}
				log.Info("all task put into exeChan, schedule worker exit")
				close(s.execChan)
				return
			}
			// Receive add operation request and return the process result.
			// And consume recv chan as much as possible.
			s.consumeRecvChan(req, maxReceiveChanBatchConsumeNum, now)
		case req := <-s.clearChan:
			var result ClearResult
			result, task = s.clearQueuedTasks(req.filter, req.reason, task, now)
			req.resp <- clearQueuedResp{result: result}
		case execChan <- execTask:
			// Task sent, drop the ownership of sent task.
			// Update waiting task counter.
			s.updateWaitingTaskCounter(-1, -nq)
			// And produce new task into execChan as much as possible.
			task = s.produceExecChan(now)
		}
	}
}

// consumeRecvChan consume the recv chan as much as possible.
func (s *scheduler) consumeRecvChan(req addTaskReq, limit int, now time.Time) {
	// Check the dynamic wait task limit.
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.GetAsInt64()
	if !s.handleAddTaskRequest(req, maxWaitTaskNum, now) {
		return
	}

	// consume the add chan until reaching the batch operation limit
	for i := 1; i < limit; i++ {
		select {
		case req, ok := <-s.receiveChan:
			if !ok {
				return
			}
			if !s.handleAddTaskRequest(req, maxWaitTaskNum, now) {
				return
			}
		default:
			return
		}
	}
}

// HandleAddTaskRequest handle a add task request.
// Return true if the process can be continued.
func (s *scheduler) handleAddTaskRequest(req addTaskReq, maxWaitTaskNum int64, now time.Time) bool {
	if maxWaitTaskNum > 0 && s.GetWaitingTaskTotal() >= maxWaitTaskNum {
		s.cleanupExpiredTasks(now)
	}

	if err := req.task.Context().Err(); err != nil {
		log.Warn("task canceled before enqueue", zap.Error(err))
		req.err <- err
	} else if maxWaitTaskNum > 0 && s.GetWaitingTaskTotal() >= maxWaitTaskNum {
		err := merr.WrapErrTooManyRequests(
			int32(maxWaitTaskNum),
			fmt.Sprintf("limit by %s", paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.Key),
		)
		req.err <- err
	} else {
		// Push the task into the policy to schedule and update the counter of the ready queue.
		queued := newQueuedTask(req.task, now)
		nq := queued.NQ()
		newTaskAdded, err := s.policy.Push(queued)
		if err == nil {
			s.updateWaitingTaskCounter(int64(newTaskAdded), nq)
		}
		req.err <- err
	}

	// Continue processing if the queue isn't reach the max limit.
	return maxWaitTaskNum <= 0 || s.GetWaitingTaskTotal() < maxWaitTaskNum
}

// produceExecChan produce task from scheduler into exec chan as much as possible
func (s *scheduler) produceExecChan(now time.Time) *queuedTask {
	var task *queuedTask
	for {
		var execChan chan Task
		var execTask Task
		nq := int64(0)
		task, nq, execChan = s.setupExecListener(task, now)
		if task.valid() {
			execTask = task.Task
		}

		select {
		case execChan <- execTask:
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
		if err := t.Context().Err(); err != nil {
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
			metrics.QueryNodeReadTaskConcurrency.WithLabelValues(paramtable.GetStringNodeID()).Inc()
			collector.Counter.Inc(metricsinfo.ExecuteQueueType)

			executeStart := time.Now()
			err := t.Execute()
			metrics.QueryNodeReadTaskExecuteDuration.WithLabelValues(
				paramtable.GetStringNodeID(),
				readTaskExecuteOutcome(err),
			).Observe(float64(time.Since(executeStart).Microseconds()) / 1000.0)

			// Update all metric after task finished.
			metrics.QueryNodeReadTaskConcurrency.WithLabelValues(paramtable.GetStringNodeID()).Dec()
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

func readTaskExecuteOutcome(err error) string {
	if err == nil {
		return metrics.SuccessLabel
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return metrics.CancelLabel
	}
	return metrics.FailLabel
}

// setupExecListener setup the execChan and next task to run.
func (s *scheduler) setupExecListener(lastWaitingTask *queuedTask, now time.Time) (*queuedTask, int64, chan Task) {
	var execChan chan Task
	nq := int64(0)
	if !lastWaitingTask.valid() {
		// No task is waiting to send to execChan, schedule a new one from queue.
		for {
			lastWaitingTask = s.policy.Pop(now)
			if !lastWaitingTask.valid() {
				break
			}
			if err := lastWaitingTask.Context().Err(); err != nil {
				s.updateWaitingTaskCounter(-1, -lastWaitingTask.NQ())
				s.recordReadTaskQueueDuration(lastWaitingTask, now, readTaskQueueOutcomeExpired)
				lastWaitingTask.Done(err)
				lastWaitingTask = nil
				continue
			}
			s.recordReadTaskQueueDuration(lastWaitingTask, now, readTaskQueueOutcomeScheduled)
			break
		}
	}
	if lastWaitingTask.valid() {
		// Try to sent task to execChan if there is a task ready to run.
		execChan = s.execChan
		nq = lastWaitingTask.NQ()
	}

	return lastWaitingTask, nq, execChan
}

func (s *scheduler) cleanupExpiredTasks(now time.Time) {
	deadlineAdvance := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskDeadlineAdvance.GetAsDurationByParse()
	cleanupTime := now.Add(deadlineAdvance)
	tasks := s.policy.Cleanup(cleanupTime)
	for _, task := range tasks {
		s.updateWaitingTaskCounter(-1, -task.NQ())
		s.recordReadTaskQueueDuration(task, now, readTaskQueueOutcomeExpired)
		task.Done(cleanupTaskError(task))
	}
}

func (s *scheduler) clearQueuedTasks(filter TaskFilter, reason string, task *queuedTask, now time.Time) (ClearResult, *queuedTask) {
	removed := s.policy.Remove(filter, now)
	if task.valid() && (filter == nil || filter(task.Task)) {
		removed = append(removed, task)
		task = nil
	}

	clearErr := clearTaskQueueError(reason)
	var result ClearResult
	for _, removedTask := range removed {
		if !removedTask.valid() {
			continue
		}
		nq := removedTask.NQ()
		result.QueuedCleared++
		result.QueuedNQCleared += nq
		s.updateWaitingTaskCounter(-1, -nq)
		s.recordReadTaskQueueDuration(removedTask, now, readTaskQueueOutcomeCleared)
		removedTask.Done(clearErr)
	}
	return result, task
}

func clearTaskQueueError(reason string) error {
	if reason == "" {
		return errors.Wrap(context.Canceled, "read task queue cleared by admin")
	}
	return errors.Wrap(context.Canceled, fmt.Sprintf("read task queue cleared by admin: %s", reason))
}

// setupReadyLenMetric update the read task ready len metric.
func (s *scheduler) setupReadyLenMetric() {
	waitingTaskCount := s.GetWaitingTaskTotal()

	// Update the ReadyQueue counter for quota.
	collector.Counter.Set(metricsinfo.ReadyQueueType, waitingTaskCount)
	// Record the waiting task length of policy as ready task metric.
	metrics.QueryNodeReadTaskReadyLen.WithLabelValues(paramtable.GetStringNodeID()).Set(float64(waitingTaskCount))
	metrics.QueryNodeReadTaskReadyNQ.WithLabelValues(paramtable.GetStringNodeID()).Set(float64(s.GetWaitingTaskTotalNQ()))
}

func (s *scheduler) recordReadTaskQueueDuration(task *queuedTask, now time.Time, outcome string) {
	if !task.valid() {
		return
	}
	metrics.QueryNodeReadTaskQueueDuration.WithLabelValues(
		paramtable.GetStringNodeID(),
		outcome,
	).Observe(float64(task.queueDuration(now).Microseconds()) / 1000.0)
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
