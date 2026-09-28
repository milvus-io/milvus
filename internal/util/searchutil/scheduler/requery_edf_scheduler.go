package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ Scheduler = (*requeryEDFScheduler)(nil)

// requeryEDFScheduler reuses admission channels, lifecycle, and public counters
// while selecting tasks only after execution capacity is available. The legacy
// scheduler's staged task and blocking execChan -> pool handoff are not used.
type requeryEDFScheduler struct {
	*scheduler
	edf       *requeryEDFPolicy
	completed chan bool // true releases a GPU slot; false releases a CPU slot
}

func newRequeryEDFScheduler() Scheduler {
	policy := newRequeryEDFPolicy()
	base := newScheduler(policy).(*scheduler)
	return &requeryEDFScheduler{
		scheduler: base,
		edf:       policy,
		completed: make(chan bool, base.pool.Cap()+base.gpuPool.Cap()),
	}
}

func (s *requeryEDFScheduler) Start() {
	mlog.Info(context.TODO(), "query node use requery EDF scheduler",
		mlog.Int("cpuSlots", s.pool.Cap()),
		mlog.Int("gpuSlots", s.gpuPool.Cap()),
		mlog.Int64("requeryCapacity", s.edf.requeryCapacity))
	s.wg.Add(1)
	go s.scheduleEDF()
	s.lifetime.SetState(lifetime.Working)
}

func (s *requeryEDFScheduler) scheduleEDF() {
	defer s.wg.Done()
	receiveChan := s.receiveChan
	cpuRunning, gpuRunning := 0, 0
	for {
		for {
			now := time.Now()
			task := s.edf.popReady(now, cpuRunning < s.pool.Cap(), gpuRunning < s.gpuPool.Cap())
			if !task.valid() {
				break
			}
			s.updateWaitingTaskCounter(-1, -task.NQ())
			if task.cleanupReady(now) {
				s.recordReadTaskQueueDuration(task, now, readTaskQueueOutcomeExpired)
				task.Done(cleanupTaskError(task))
				continue
			}
			s.recordReadTaskQueueDuration(task, now, readTaskQueueOutcomeScheduled)
			gpu := task.IsGpuIndex()
			if gpu {
				gpuRunning++
			} else {
				cpuRunning++
			}
			s.getPool(task).Submit(func() (any, error) {
				// The bounded channel cannot block while these slots are in use.
				defer func() { s.completed <- gpu }()
				err := s.executeEDFTask(task)
				task.Done(err)
				return nil, err
			})
		}
		s.setupReadyLenMetric()
		// Stop closes receiveChan; finish every accepted task before releasing
		// the pools, including tasks already running at shutdown.
		if receiveChan == nil && s.edf.Len() == 0 && cpuRunning+gpuRunning == 0 {
			return
		}
		select {
		case req, ok := <-receiveChan:
			if !ok {
				receiveChan = nil
				continue
			}
			s.admitEDFTask(req, time.Now())
		case req := <-s.clearChan:
			result, _ := s.clearQueuedTasks(req.filter, req.reason, nil, time.Now())
			req.resp <- clearQueuedResp{result: result}
		case gpu := <-s.completed:
			if gpu {
				gpuRunning--
			} else {
				cpuRunning--
			}
		}
	}
}

func (s *requeryEDFScheduler) admitEDFTask(req addTaskReq, now time.Time) {
	queued := newQueuedTask(req.task, now)
	if queued.cleanupReady(now) {
		req.err <- cleanupTaskError(queued)
		return
	}
	queued.schedulingDeadline, _ = req.task.Context().Deadline()
	nq := queued.NQ()
	added, err := s.edf.Push(queued)
	if errors.Is(err, merr.ErrServiceTooManyRequests) {
		// Retry admission once after real expiration cleanup, without the
		// legacy deadline advance or an O(queue length) scan at every dispatch.
		for _, expired := range s.edf.Cleanup(now) {
			s.updateWaitingTaskCounter(-1, -expired.NQ())
			s.recordReadTaskQueueDuration(expired, now, readTaskQueueOutcomeExpired)
			expired.Done(cleanupTaskError(expired))
		}
		if queued.cleanupReady(time.Now()) {
			req.err <- cleanupTaskError(queued)
			return
		}
		added, err = s.edf.Push(queued)
	}
	if err == nil {
		s.updateWaitingTaskCounter(int64(added), nq)
	}
	req.err <- err
}

func (s *requeryEDFScheduler) executeEDFTask(task *queuedTask) error {
	if task.cleanupReady(time.Now()) {
		return cleanupTaskError(task)
	}
	if err := task.PreExecute(); err != nil {
		return err
	}
	if task.cleanupReady(time.Now()) {
		return cleanupTaskError(task)
	}
	nodeID := paramtable.GetStringNodeID()
	metrics.QueryNodeReadTaskConcurrency.WithLabelValues(nodeID).Inc()
	collector.Counter.Inc(metricsinfo.ExecuteQueueType)
	defer func() {
		metrics.QueryNodeReadTaskConcurrency.WithLabelValues(nodeID).Dec()
		collector.Counter.Dec(metricsinfo.ExecuteQueueType)
	}()
	start := time.Now()
	err := task.Execute()
	metrics.QueryNodeReadTaskExecuteDuration.WithLabelValues(nodeID, readTaskExecuteOutcome(err)).
		Observe(float64(time.Since(start).Microseconds()) / 1000.0)
	return err
}
