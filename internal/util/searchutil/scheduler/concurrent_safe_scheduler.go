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
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	maxReceiveChanBatchConsumeNum = 100

	readTaskQueueOutcomeScheduled = "scheduled"
	readTaskQueueOutcomeExpired   = "expired"
	// the task would wait, or has waited, in the queue for longer than it
	// may, see maxQueueWait
	readTaskQueueOutcomeOverloaded = "overloaded"
)

// newScheduler create a scheduler with given schedule policy.
func newScheduler(policy schedulePolicy) Scheduler {
	maxReadConcurrency := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt()
	log.Info("query node use concurrent safe scheduler", zap.Int("max_concurrency", maxReadConcurrency))
	return &scheduler{
		policy:           policy,
		receiveChan:      make(chan addTaskReq),
		execChan:         make(chan *queuedTask),
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
	execChan    chan *queuedTask
	pool        *conc.Pool[any]
	gpuPool     *conc.Pool[any]

	// wg is the waitgroup for internal worker goroutine
	wg sync.WaitGroup
	// lifetime controls scheduler State & make sure all requests accepted will be processed
	lifetime lifetime.Lifetime[lifetime.State]

	// head is the task that leaves the queue next: taken from the policy and
	// waiting for a worker. Nil when no task waits. Owned by the schedule
	// goroutine.
	head *queuedTask

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

		var execChan chan *queuedTask
		var execTask *queuedTask
		nq := int64(0)
		now := time.Now()
		task, nq, execChan = s.setupExecListener(task, now)
		if task.valid() {
			execTask = task
		}

		select {
		case req, ok := <-s.receiveChan:
			// The select may have blocked for long, on an idle node until
			// this request came in: take the time again, it is the time the
			// task is queued at.
			now = time.Now()
			if !ok {
				log.Info("receiveChan closed, processing remaining request")
				// drain policy maintained task
				for task.valid() {
					execChan <- task
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
		case execChan <- execTask:
			// Task sent, drop the ownership of sent task.
			// Update waiting task counter.
			s.updateWaitingTaskCounter(-1, -nq)
			// And produce new task into execChan as much as possible. The
			// select may have blocked for long: take the time again.
			task = s.produceExecChan(time.Now())
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
	} else if queued := newQueuedTask(req.task, now); queued.cleanupReady(now) {
		// Its deadline passed, though its context does not say so yet: it is
		// out of time, not refused for the load on this node.
		s.recordReadTaskQueueDuration(queued, now, readTaskQueueOutcomeExpired)
		req.err <- cleanupTaskError(queued)
	} else if queued.mustWaitTooLong(s.head, now) {
		// The task that leaves the queue next has waited so long that this
		// one would wait for longer than it may. Refuse it now, while the
		// request has all its time left, rather than when it has waited for
		// nothing.
		s.recordReadTaskQueueDuration(queued, now, readTaskQueueOutcomeOverloaded)
		req.err <- overloadedError(queued, s.head.queueDuration(now))
	} else {
		// Push the task into the policy to schedule and update the counter of the ready queue.
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
		var execChan chan *queuedTask
		var execTask *queuedTask
		nq := int64(0)
		task, nq, execChan = s.setupExecListener(task, now)
		if task.valid() {
			execTask = task
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
		queued, ok := <-s.execChan
		if !ok {
			log.Info("scheduler execChan closed, worker exit")
			return
		}

		s.getPool(queued.Task).Submit(func() (any, error) {
			// Drop members canceled or past their individual deadline/queue budget
			// between dequeue and execution. Members that
			// are dropped here have already been completed with their own
			// context error; the survivors keep running. The cause is logged
			// because it is what tells a client that went away from a request
			// that ran out of time.
			now := time.Now()
			survivor, dropped, cause := queued.prune(now)
			if dropped > 0 {
				if survivor == nil {
					log.Ctx(queued.Context()).Warn("task discarded before executing", zap.Error(cause))
					s.recordReadTaskQueueDuration(queued, time.Now(), prunedQueueOutcome(cause))
					return nil, cause
				}
				log.Ctx(queued.Context()).Warn("requests dropped from a search group before executing",
					zap.Int("dropped", dropped), zap.Error(cause))
			}
			queued.Task = survivor
			t := survivor
			if queued.cleanupReady(now) {
				s.recordReadTaskQueueDuration(queued, now, readTaskQueueOutcomeExpired)
				finishExpiredTask(queued)
				return nil, nil
			}
			if queued.overloaded(now) {
				s.recordReadTaskQueueDuration(queued, now, readTaskQueueOutcomeOverloaded)
				finishUnrunTask(queued, overloadedError(queued, queued.queueDuration(now)))
				return nil, nil
			}
			s.recordReadTaskQueueDuration(queued, now, readTaskQueueOutcomeScheduled)
			if err := t.PreExecute(); err != nil {
				log.Warn("failed to pre-execute task", zap.Error(err))
				t.Done(err)
				return nil, err
			}

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
func (s *scheduler) setupExecListener(lastWaitingTask *queuedTask, now time.Time) (*queuedTask, int64, chan *queuedTask) {
	var execChan chan *queuedTask
	nq := int64(0)
	if !lastWaitingTask.valid() {
		// No task is waiting to send to execChan, schedule a new one from queue.
		for {
			lastWaitingTask = s.policy.Pop(now)
			if !lastWaitingTask.valid() {
				break
			}
			// Remember the NQ the counters were credited with before any
			// member is pruned away.
			lastWaitingTask.accountedNQ = lastWaitingTask.NQ()
			// A merged group loses only members canceled or past their individual
			// deadline/queue budget. Dropped members receive their own error.
			survivor, dropped, cause := lastWaitingTask.prune(now)
			if dropped > 0 {
				// Said here as well as before execution, so that a request
				// whose client sees a cancellation can be found in the log
				// whichever of the two points dropped it.
				if survivor == nil {
					log.Ctx(lastWaitingTask.Context()).Warn("task discarded before it left the queue", zap.Error(cause))
				} else {
					log.Ctx(lastWaitingTask.Context()).Warn("requests dropped from a search group as it left the queue",
						zap.Int("dropped", dropped), zap.Error(cause))
				}
			}
			if survivor == nil {
				s.updateWaitingTaskCounter(-1, -lastWaitingTask.countedNQ())
				s.recordReadTaskQueueDuration(lastWaitingTask, now, prunedQueueOutcome(cause))
				lastWaitingTask = nil
				continue
			}
			lastWaitingTask.Task = survivor
			if lastWaitingTask.cleanupReady(now) {
				// Out of time even when its context does not say so yet, so
				// that it does not count as refused for the load on this node.
				s.updateWaitingTaskCounter(-1, -lastWaitingTask.countedNQ())
				s.recordReadTaskQueueDuration(lastWaitingTask, now, readTaskQueueOutcomeExpired)
				finishExpiredTask(lastWaitingTask)
				lastWaitingTask = nil
				continue
			}
			if lastWaitingTask.overloaded(now) {
				// The queue holds more than the node gets done in the time
				// the task may wait. Executing it would make the tasks behind
				// it wait as long: fail it now, so that the Proxy can turn to
				// another replica while the request still has time.
				s.updateWaitingTaskCounter(-1, -lastWaitingTask.countedNQ())
				s.recordReadTaskQueueDuration(lastWaitingTask, now, readTaskQueueOutcomeOverloaded)
				finishUnrunTask(lastWaitingTask, overloadedError(lastWaitingTask, lastWaitingTask.queueDuration(now)))
				lastWaitingTask = nil
				continue
			}
			break
		}
	}
	s.head = nil
	if lastWaitingTask.valid() {
		// Try to sent task to execChan if there is a task ready to run.
		execChan = s.execChan
		nq = lastWaitingTask.countedNQ()
		s.head = lastWaitingTask
	}

	return lastWaitingTask, nq, execChan
}

func prunedQueueOutcome(cause error) string {
	if errors.Is(cause, merr.ErrServiceTooManyRequests) {
		return readTaskQueueOutcomeOverloaded
	}
	return readTaskQueueOutcomeExpired
}

func finishExpiredTask(task *queuedTask) {
	if g, ok := task.Task.(ExpirableGroup); ok {
		g.FinishExpired()
		return
	}
	task.Done(cleanupTaskError(task))
}

func finishUnrunTask(task *queuedTask, err error) {
	if u, ok := task.Task.(interface{ FinishUnrun(error) }); ok {
		u.FinishUnrun(err)
		return
	}
	task.Done(err)
}

// overloadedError is what a task fails with that would wait, or has waited,
// for longer than it may. waited is how long the task that leaves the queue
// next has waited: the refused task itself, or the one it would queue behind.
func overloadedError(task *queuedTask, waited time.Duration) error {
	return queueOverloadedError(task.maxWait, waited)
}

func queueOverloadedError(maxWait, waited time.Duration) error {
	return merr.WrapErrTooManyRequests(
		int32(maxWait.Milliseconds()),
		fmt.Sprintf("read tasks wait %s in the queue, limit by %s and %s",
			waited,
			paramtable.Get().QueryNodeCfg.MaxQueueWaitRatio.Key,
			paramtable.Get().QueryNodeCfg.MaxQueueWait.Key),
	)
}

func (s *scheduler) cleanupExpiredTasks(now time.Time) {
	deadlineAdvance := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskDeadlineAdvance.GetAsDurationByParse()
	cleanupTime := now.Add(deadlineAdvance)
	tasks := s.policy.Cleanup(cleanupTime)
	for _, task := range tasks {
		s.updateWaitingTaskCounter(-1, -task.NQ())
		s.recordReadTaskQueueDuration(task, now, readTaskQueueOutcomeExpired)
		// A group is only taken out once every request in it is done, and
		// each is told its own reason rather than the owner's.
		if g, ok := task.Task.(ExpirableGroup); ok {
			g.FinishExpired()
			continue
		}
		task.Done(cleanupTaskError(task))
	}
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
