package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	mlog.Info(context.TODO(), "query node use concurrent safe scheduler", mlog.Int("max_concurrency", maxReadConcurrency))
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

	return err
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
				mlog.Info(context.TODO(), "receiveChan closed, processing remaining request")
				// drain policy maintained task
				for task.valid() {
					execChan <- task.Task
					s.onTaskServed(task)
					s.onTaskDequeued(task, task.NQ())
					task = s.produceExecChan(now)
				}
				mlog.Info(context.TODO(), "all task put into exeChan, schedule worker exit")
				close(s.execChan)
				return
			}
			// Receive add operation request and return the process result.
			// And consume recv chan as much as possible.
			s.consumeRecvChan(req, maxReceiveChanBatchConsumeNum, now, &task)
		case req := <-s.clearChan:
			var result ClearResult
			result, task = s.clearQueuedTasks(req.filter, req.reason, task, now)
			req.resp <- clearQueuedResp{result: result}
		case execChan <- execTask:
			// Task sent, drop the ownership of sent task.
			// Update waiting task counter.
			s.onTaskServed(task)
			s.onTaskDequeued(task, nq)
			// And produce new task into execChan as much as possible.
			task = s.produceExecChan(now)
		}
	}
}

// consumeRecvChan consume the recv chan as much as possible.
func (s *scheduler) consumeRecvChan(req addTaskReq, limit int, now time.Time, staged **queuedTask) {
	// Check the dynamic wait task limit.
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.GetAsInt64()
	if !s.handleAddTaskRequest(req, maxWaitTaskNum, now, staged) {
		return
	}

	// consume the add chan until reaching the batch operation limit
	for i := 1; i < limit; i++ {
		select {
		case req, ok := <-s.receiveChan:
			if !ok {
				return
			}
			if !s.handleAddTaskRequest(req, maxWaitTaskNum, now, staged) {
				return
			}
		default:
			return
		}
	}
}

// HandleAddTaskRequest handle a add task request.
// Return true if the process can be continued.
func (s *scheduler) handleAddTaskRequest(req addTaskReq, maxWaitTaskNum int64, now time.Time, staged **queuedTask) bool {
	requery := false
	if classifier, ok := s.policy.(laneClassifier); ok {
		requery = classifier.UseLane(req.task)
	}

	if err := req.task.Context().Err(); err != nil {
		mlog.Warn(context.TODO(), "task canceled before enqueue", mlog.Err(err))
		req.err <- err
		return maxWaitTaskNum <= 0 || s.getRegularWaitingTaskTotal() < maxWaitTaskNum
	}

	capacity, available := s.waitingTaskCapacityAvailable(requery, maxWaitTaskNum)
	if !available {
		s.cleanupExpiredTasks(now, staged)
		if err := req.task.Context().Err(); err != nil {
			mlog.Warn(context.TODO(), "task canceled before enqueue", mlog.Err(err))
			req.err <- err
			return maxWaitTaskNum <= 0 || s.getRegularWaitingTaskTotal() < maxWaitTaskNum
		}
		capacity, available = s.waitingTaskCapacityAvailable(requery, maxWaitTaskNum)
	}
	if !available {
		recordReadTaskReject(requery)
		if requery {
			req.err <- requeryLaneCapacityError(capacity)
		} else {
			req.err <- merr.WrapErrTooManyRequests(
				int32(capacity),
				fmt.Sprintf("limit by %s", paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.Key),
			)
		}
		return false
	}

	// Allocate only after the common canceled/full rejection paths have passed.
	queued := newQueuedTask(req.task, now)
	queued.requery = requery
	nq := queued.NQ()
	newTaskAdded, err := s.pushTask(queued, now, staged)
	if err == nil {
		s.updateWaitingTaskCounter(queued, int64(newTaskAdded), nq)
	}
	req.err <- err
	if errors.Is(err, merr.ErrServiceTooManyRequests) {
		recordReadTaskReject(requery)
		if requery {
			// The lane remained full after cleanup and retry. Yield the scheduler
			// loop so execChan handoff can drain work; unread requests remain pending.
			return false
		}
	}
	// Continue processing only while the regular queue still has room.
	return maxWaitTaskNum <= 0 || s.getRegularWaitingTaskTotal() < maxWaitTaskNum
}

func (s *scheduler) waitingTaskCapacityAvailable(requery bool, maxRegular int64) (int64, bool) {
	if requery {
		capacity := requeryLaneCapacity()
		return capacity, capacity > 0 && s.getRequeryWaitingTaskTotal() < capacity
	}
	return maxRegular, maxRegular <= 0 || s.getRegularWaitingTaskTotal() < maxRegular
}

// pushTask retries a requery-lane capacity rejection once after the scheduler
// has completed expired-task lifecycle handling. pushTaskOnce performs
// admission before mutating the policy.
func (s *scheduler) pushTask(task *queuedTask, now time.Time, staged **queuedTask) (int, error) {
	newTaskAdded, err := s.pushTaskOnce(task)
	if !task.requery || !errors.Is(err, merr.ErrServiceTooManyRequests) {
		return newTaskAdded, err
	}

	if ctxErr := task.Context().Err(); ctxErr != nil {
		return 0, ctxErr
	}
	s.cleanupExpiredTasks(now, staged)
	if ctxErr := task.Context().Err(); ctxErr != nil {
		return 0, ctxErr
	}
	return s.pushTaskOnce(task)
}

func (s *scheduler) pushTaskOnce(task *queuedTask) (int, error) {
	if task.requery {
		capacity := requeryLaneCapacity()
		if capacity <= 0 || s.getRequeryWaitingTaskTotal() >= capacity {
			return 0, requeryLaneCapacityError(capacity)
		}
	}
	return s.policy.Push(task)
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
			s.onTaskServed(task)
			s.onTaskDequeued(task, nq)
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
	mlog.Info(context.TODO(), "start execute loop")
	for {
		t, ok := <-s.execChan
		if !ok {
			mlog.Info(context.TODO(), "scheduler execChan closed, worker exit")
			return
		}
		// Skip this task if task is canceled.
		if err := t.Context().Err(); err != nil {
			mlog.Warn(context.TODO(), "task canceled before executing", mlog.Err(err))
			t.Done(err)
			continue
		}
		if err := t.PreExecute(); err != nil {
			mlog.Warn(context.TODO(), "failed to pre-execute task", mlog.Err(err))
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

// onTaskDequeued updates the matching queue-class counters when a task leaves
// scheduler ownership at execChan handoff, expiration, clear, or shutdown.
func (s *scheduler) onTaskDequeued(task *queuedTask, nq int64) {
	if !task.valid() {
		return
	}
	s.updateWaitingTaskCounter(task, -1, -nq)
}

// onTaskServed reports an execChan handoff to the policy. It runs on the
// schedule goroutine only, so the policy needs no locking.
func (s *scheduler) onTaskServed(task *queuedTask) {
	if !task.valid() {
		return
	}
	if observer, ok := s.policy.(taskServedObserver); ok {
		observer.onTaskServed(task)
	}
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
				s.onTaskDequeued(lastWaitingTask, lastWaitingTask.NQ())
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

func (s *scheduler) cleanupExpiredTasks(now time.Time, staged **queuedTask) {
	deadlineAdvance := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskDeadlineAdvance.GetAsDurationByParse()
	cleanupTime := now.Add(deadlineAdvance)
	if staged != nil && (*staged).cleanupReady(cleanupTime) {
		task := *staged
		*staged = nil
		s.onTaskDequeued(task, task.NQ())
		// Queue duration was already recorded when this task was selected by Pop.
		task.Done(cleanupTaskError(task))
	}
	tasks := s.policy.Cleanup(cleanupTime)
	for _, task := range tasks {
		s.onTaskDequeued(task, task.NQ())
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
		s.onTaskDequeued(removedTask, nq)
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
	nodeID := paramtable.GetStringNodeID()

	// Update the ReadyQueue counter for quota.
	collector.Counter.Set(metricsinfo.ReadyQueueType, waitingTaskCount)
	// Aggregate gauges include both regular and requery waiting tasks. The
	// lane-specific gauge uses the same counter as admission, so a task staged
	// for execChan handoff remains visible until ownership is transferred.
	metrics.QueryNodeReadTaskReadyLen.WithLabelValues(nodeID).Set(float64(waitingTaskCount))
	metrics.QueryNodeReadTaskReadyNQ.WithLabelValues(nodeID).Set(float64(s.GetWaitingTaskTotalNQ()))
	metrics.QueryNodeReadTaskRequeryReadyLen.WithLabelValues(nodeID).Set(float64(s.getRequeryWaitingTaskTotal()))
}

func (s *scheduler) recordReadTaskQueueDuration(task *queuedTask, now time.Time, outcome string) {
	if !task.valid() {
		return
	}
	nodeID := paramtable.GetStringNodeID()
	durationMs := float64(task.queueDuration(now).Microseconds()) / 1000.0
	// The aggregate histogram keeps its pre-existing label schema and covers
	// both classes; the requery-only subset goes to a separate metric.
	metrics.QueryNodeReadTaskQueueDuration.WithLabelValues(nodeID, outcome).Observe(durationMs)
	if task.requery {
		metrics.QueryNodeReadTaskRequeryQueueDuration.WithLabelValues(nodeID, outcome).Observe(durationMs)
	}
}

func laneLabel(requery bool) string {
	if requery {
		return metrics.ReQueryLabel
	}
	return metrics.RegularLabel
}

func recordReadTaskReject(requery bool) {
	metrics.QueryNodeReadTaskRejectCnt.WithLabelValues(paramtable.GetStringNodeID(), laneLabel(requery)).Inc()
}

// scheduler counter implement, concurrent safe.
type schedulerCounter struct {
	regularWaitingTaskTotal   atomic.Int64
	regularWaitingTaskTotalNQ atomic.Int64
	requeryWaitingTaskTotal   atomic.Int64
	requeryWaitingTaskTotalNQ atomic.Int64
}

// GetWaitingTaskTotal get ready task counts.
func (s *schedulerCounter) GetWaitingTaskTotal() int64 {
	return s.regularWaitingTaskTotal.Load() + s.requeryWaitingTaskTotal.Load()
}

// GetWaitingTaskTotalNQ get ready task NQ.
func (s *schedulerCounter) GetWaitingTaskTotalNQ() int64 {
	return s.regularWaitingTaskTotalNQ.Load() + s.requeryWaitingTaskTotalNQ.Load()
}

func (s *schedulerCounter) getRegularWaitingTaskTotal() int64 {
	return s.regularWaitingTaskTotal.Load()
}

func (s *schedulerCounter) getRegularWaitingTaskTotalNQ() int64 {
	return s.regularWaitingTaskTotalNQ.Load()
}

func (s *schedulerCounter) getRequeryWaitingTaskTotal() int64 {
	return s.requeryWaitingTaskTotal.Load()
}

func (s *schedulerCounter) getRequeryWaitingTaskTotalNQ() int64 {
	return s.requeryWaitingTaskTotalNQ.Load()
}

// updateWaitingTaskCounter dispatches updates by the immutable queue-class
// stamp so regular admission and exported load accounting can use different
// views without losing lifecycle symmetry.
func (s *schedulerCounter) updateWaitingTaskCounter(task *queuedTask, num int64, nq int64) {
	if task.requery {
		s.requeryWaitingTaskTotal.Add(num)
		s.requeryWaitingTaskTotalNQ.Add(nq)
		return
	}
	s.regularWaitingTaskTotal.Add(num)
	s.regularWaitingTaskTotalNQ.Add(nq)
}
