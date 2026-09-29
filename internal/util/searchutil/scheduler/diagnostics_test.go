package scheduler

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func diagnosticCollectors() []prometheus.Collector {
	return []prometheus.Collector{metrics.QueryNodeSchedulerDiagnosticEvents, metrics.QueryNodeSchedulerDiagnosticDuration,
		metrics.QueryNodeSchedulerDiagnosticSlack, metrics.QueryNodeSchedulerDiagnosticGap, metrics.QueryNodeSchedulerDiagnosticShape,
		metrics.QueryNodeSchedulerDiagnosticMerge, metrics.QueryNodeSchedulerDiagnosticChoice, metrics.QueryNodeSchedulerDiagnosticCost,
		metrics.QueryNodeSchedulerDiagnosticChildren, metrics.QueryNodeSchedulerDiagnosticQueue,
		metrics.QueryNodeSchedulerDiagnosticCandidateDeadline}
}

func resetDiagnosticMetrics() {
	for _, c := range diagnosticCollectors() {
		c.(interface{ Reset() }).Reset()
	}
}

func diagnosticHistogram(t *testing.T, observer prometheus.Observer) *dto.Histogram {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram()
}

func diagnosticTask(t *testing.T, d *schedulerDiagnostics, deadline time.Time, merge bool, nq int64) *queuedTask {
	t.Helper()
	ctx := context.Background()
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}
	task := newMockTask(mockTaskConfig{ctx: ctx, mergeAble: merge, nq: nq})
	queued := newQueuedTask(task, time.Now())
	queued.diagnostics = d.admission(task, time.Now())
	return queued
}

func TestDiagnosticSeriesBudget(t *testing.T) {
	paramtable.Init()
	for _, policy := range []string{schedulePolicyNameFIFO, schedulePolicyNameRequeryEDF} {
		resetDiagnosticMetrics()
		newSchedulerDiagnostics(policy, false)
		registry := prometheus.NewRegistry()
		registry.MustRegister(diagnosticCollectors()...)
		families, err := registry.Gather()
		require.NoError(t, err)
		series := 0
		for _, family := range families {
			for _, metric := range family.Metric {
				if histogram := metric.GetHistogram(); histogram != nil {
					series += len(histogram.Bucket) + 3 // explicit buckets, +Inf, sum, count
				} else {
					series++
				}
			}
		}
		t.Logf("%s: %d series; task observation=%d bytes, queuedTask=%d bytes", policy, series, unsafe.Sizeof(TaskDiagnostics{}), unsafe.Sizeof(queuedTask{}))
		require.LessOrEqual(t, series, 600)
	}
}

func TestDiagnosticsMergeAccounting(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	q := newMergeTaskQueue("")
	deadline := time.Now().Add(time.Minute)
	parent := diagnosticTask(t, d, deadline, true, 2)
	q.push(parent)
	parent.diagnostics.pushed(1, nil)
	rejected := diagnosticTask(t, d, deadline.Add(time.Second), true, 2)
	q.push(rejected)
	rejected.diagnostics.pushed(1, nil)
	input := diagnosticTask(t, d, deadline.Add(-10*time.Millisecond), true, 3)
	require.True(t, q.tryMerge(input, 64, 16, 50*time.Millisecond))
	input.diagnostics.pushed(0, nil)
	require.EqualValues(t, 1, d.merge[mergeDeadline].value)
	require.Zero(t, d.merge[mergeNewAfterDeadline].value, "a rejected candidate does not imply a new group")
	require.EqualValues(t, 2, parent.diagnostics.requests)
	require.EqualValues(t, 5, parent.diagnostics.nq)
	require.Equal(t, input.diagnostics.deadline, parent.diagnostics.deadline)
	rootDeadline, _ := parent.Context().Deadline()
	require.Equal(t, deadline, rootDeadline, "diagnostics must not rewrite the cancellation context")
	selected := q.pop()
	selected.diagnostics.queueEvent(selected.Task, readTaskQueueOutcomeScheduled)
	require.InDelta(t, 10, diagnosticHistogram(t, d.rootDDL).GetSampleSum(), .01)
	selected.diagnostics.started(time.Now(), time.Now())
	selected.diagnostics.finished(nil, time.Millisecond, time.Now())
	d.flush(time.Now())
	k := &d.kinds[diagSearch]
	require.EqualValues(t, 3, testutil.ToFloat64(k.events[diagAdmitted][1].metric))
	require.EqualValues(t, 7, testutil.ToFloat64(k.events[diagAdmitted][2].metric))
	require.EqualValues(t, 2, testutil.ToFloat64(k.events[diagNewGroup][0].metric))
	require.EqualValues(t, 1, testutil.ToFloat64(k.events[diagMerged][1].metric))
	require.EqualValues(t, 2, testutil.ToFloat64(k.events[diagFinishSuccess][1].metric))
	require.EqualValues(t, 5, testutil.ToFloat64(k.events[diagFinishSuccess][2].metric))
	require.EqualValues(t, 1, testutil.ToFloat64(k.queueMetric))
	require.Zero(t, testutil.ToFloat64(k.pendingMetric))
	require.Zero(t, testutil.ToFloat64(k.runningMetric))
}

func TestDiagnosticsMergeRejectionsAndTombstones(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	q := newMergeTaskQueue("")
	deadline := time.Now().Add(time.Minute)
	q.push(diagnosticTask(t, d, deadline.Add(time.Second), true, 1))
	q.push(diagnosticTask(t, d, deadline, false, 1))
	q.push(diagnosticTask(t, d, deadline, true, 64))
	q.tasks = append(q.tasks, nil, &queuedTask{})
	input := diagnosticTask(t, d, deadline, true, 1)
	require.False(t, q.tryMerge(input, 64, 16, 50*time.Millisecond))
	input.diagnostics.pushed(1, nil)
	require.EqualValues(t, 2, d.merge[mergeInvalid].value)
	require.EqualValues(t, 1, d.merge[mergeNQ].value)
	require.EqualValues(t, 1, d.merge[mergeBusiness].value)
	require.EqualValues(t, 1, d.merge[mergeDeadline].value)
	require.EqualValues(t, 1, d.merge[mergeNewAfterDeadline].value)
	require.EqualValues(t, 5, diagnosticHistogram(t, d.scan).GetSampleSum())
}

func TestDiagnosticsIdleTimestampAndClear(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	s := &scheduler{policy: newFIFOPolicy(), diagnostics: d, execChan: make(chan Task)}
	stale := time.Now().Add(-time.Hour)
	for range 2 {
		task := newMockTask(mockTaskConfig{})
		errCh := make(chan error, 1)
		s.handleAddTaskRequest(addTaskReq{task: task, err: errCh, addStart: time.Now()}, stale)
		require.NoError(t, <-errCh)
	}
	staged, _, _ := s.setupExecListener(nil, time.Now())
	require.Equal(t, stale, staged.enqueueTime, "do not silently fix the existing timestamp contract")
	require.Less(t, diagnosticHistogram(t, d.kinds[diagSearch].duration[1]).GetSampleSum(), 1000.0)
	result, remaining := s.clearQueuedTasks(nil, "diagnostic test", staged, time.Now())
	require.Nil(t, remaining)
	require.EqualValues(t, 2, result.QueuedCleared)
	d.flush(time.Now())
	k := &d.kinds[diagSearch]
	require.EqualValues(t, 1, testutil.ToFloat64(k.events[diagQueueCleared][0].metric))
	require.EqualValues(t, 1, testutil.ToFloat64(k.events[diagStagedCleared][0].metric))
	require.Zero(t, testutil.ToFloat64(k.events[diagQueueCanceled][0].metric))
	require.Zero(t, testutil.ToFloat64(k.pendingMetric))
	require.Zero(t, testutil.ToFloat64(k.queueMetric))
	s.recordReadTaskQueueDuration(nil, time.Now(), readTaskQueueOutcomeCleared)
}

func TestDiagnosticOutcomeAndConcurrentCompletion(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	for _, tc := range []struct {
		err     error
		outcome int
	}{
		{nil, 0}, {errors.Wrap(context.DeadlineExceeded, "execute"), 1},
		{errors.Wrap(context.Canceled, "execute"), 2}, {errors.New("ordinary error"), 3},
		{errors.Mark(errors.New("segcore canceled"), context.DeadlineExceeded), 1},
		{errors.Mark(errors.New("segcore canceled"), context.Canceled), 2},
	} {
		require.Equal(t, tc.outcome, diagnosticOutcome(tc.err))
	}
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task := &TaskDiagnostics{owner: d, kind: diagSearch, requests: 2, nq: 7, deadline: time.Now().Add(-time.Second)}
			task.started(time.Now(), time.Now())
			task.finished([]error{nil, context.DeadlineExceeded, context.Canceled, errors.New("other")}[i%4], time.Millisecond, time.Now())
		}(i)
	}
	// The scheduler publishes its own counters concurrently with worker updates.
	for range 10 {
		d.flush(time.Now())
	}
	wg.Wait()
	for outcome := range 4 {
		require.EqualValues(t, 10, testutil.ToFloat64(d.kinds[diagSearch].executeCost[1][outcome][0]))
	}
	require.EqualValues(t, 40, testutil.ToFloat64(d.kinds[diagSearch].events[diagStartOverdue][0].metric))
	require.EqualValues(t, 10, testutil.ToFloat64(d.kinds[diagSearch].events[diagFinishOverdue][0].metric))
}

type diagnosticRunTask struct {
	Task
	pre     func() error
	execute func() error
}

func (t *diagnosticRunTask) PreExecute() error {
	if t.pre != nil {
		return t.pre()
	}
	return nil
}
func (t *diagnosticRunTask) Execute() error {
	if t.execute != nil {
		return t.execute()
	}
	return nil
}

func TestDiagnosticsRuntimeFlushAndExecutor(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg
	require.NoError(t, paramtable.Get().Save(cfg.SchedulerDiagnosticsEnabled.Key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.SchedulerDiagnosticsEnabled.Key) })
	for _, policy := range []string{schedulePolicyNameFIFO, schedulePolicyNameRequeryEDF} {
		t.Run(policy, func(t *testing.T) {
			resetDiagnosticMetrics()
			s := NewScheduler(policy).(*scheduler)
			s.Start()
			t.Cleanup(s.Stop)
			for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, errors.New("pre execute")} {
				task := &diagnosticRunTask{Task: newMockTask(mockTaskConfig{}), pre: func() error { return err }}
				require.NoError(t, s.Add(task))
				require.Equal(t, err, task.Wait())
			}
			k := &s.diagnostics.kinds[diagOther]
			// No more input: scheduler-owned observations must still be published.
			require.Eventually(t, func() bool { return testutil.ToFloat64(k.events[diagSelected][0].metric) == 4 }, 3*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 1, testutil.ToFloat64(k.events[diagFinishSuccess][0].metric))
			for _, event := range []int{diagBeforeCanceled, diagBeforeDeadline, diagBeforeOther} {
				require.EqualValues(t, 1, testutil.ToFloat64(k.events[event][0].metric))
			}
			require.Zero(t, testutil.ToFloat64(k.pendingMetric))
			require.Zero(t, testutil.ToFloat64(k.runningMetric))
		})
	}
}

func TestDiagnosticsPoolWaitDoesNotChangeCancellation(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	s.diagnostics = newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	s.pool.Release()
	s.pool = conc.NewPool[any](1)
	started, unblock := make(chan struct{}), make(chan struct{})
	first := &diagnosticRunTask{Task: newMockTask(mockTaskConfig{}), execute: func() error { close(started); <-unblock; return nil }}
	s.Start()
	t.Cleanup(s.Stop)
	require.NoError(t, s.Add(first))
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	pre := make(chan struct{})
	second := &diagnosticRunTask{Task: newMockTask(mockTaskConfig{ctx: ctx}), pre: func() error { close(pre); return nil }, execute: func() error { return ctx.Err() }}
	require.NoError(t, s.Add(second))
	<-pre
	cancel()
	close(unblock)
	require.NoError(t, first.Wait())
	require.ErrorIs(t, second.Wait(), context.Canceled)
	k := &s.diagnostics.kinds[diagOther]
	require.EqualValues(t, 1, testutil.ToFloat64(k.events[diagFinishCanceled][0].metric), "cancellation while waiting for the pool must still reach Execute, as before")
	require.Zero(t, testutil.ToFloat64(k.events[diagBeforeCanceled][0].metric))
	require.EqualValues(t, 2, diagnosticHistogram(t, k.duration[4]).GetSampleCount())
}

func TestDiagnosticsOff(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	s := NewScheduler(schedulePolicyNameRequeryEDF).(*scheduler)
	t.Cleanup(s.Stop)
	require.Nil(t, s.diagnostics)
	task := newMockTask(mockTaskConfig{})
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: task, err: errCh}, time.Now())
	require.NoError(t, <-errCh)
	queued, _, _ := s.setupExecListener(nil, time.Now())
	require.Nil(t, queued.diagnostics)
	require.Same(t, task, queued.executionTask())
	require.Nil(t, s.policy.(*requeryEDFPolicy).diagnostics)
}

func BenchmarkSchedulerDiagnostics(b *testing.B) {
	initDiagnosticBenchmark()
	for _, policy := range []string{schedulePolicyNameFIFO, schedulePolicyNameRequeryEDF} {
		for _, mode := range []string{"off", "metrics", "summary"} {
			for _, scenario := range []string{"merge", "deadline", "business", "scan1024", "cancel"} {
				b.Run(strings.Join([]string{policy, mode, scenario}, "/"), func(b *testing.B) {
					var d *schedulerDiagnostics
					if mode != "off" {
						d = newSchedulerDiagnostics(policy, mode == "summary")
					}
					baseCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
					defer cancel()
					otherCtx, otherCancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Hour))
					defer otherCancel()
					task := newMockTask(mockTaskConfig{ctx: baseCtx, mergeAble: true}).(*MockTask)
					candidate := newMockTask(mockTaskConfig{ctx: baseCtx, mergeAble: scenario != "business"}).(*MockTask)
					if scenario == "deadline" {
						candidate.ctx = otherCtx
					}
					if scenario == "cancel" {
						cancel()
					}
					width := 1
					if scenario == "scan1024" {
						width = 1024
						candidate.mergeAble = false
					}
					var p schedulePolicy = &fifoPolicy{queue: newMergeTaskQueue("")}
					q := p.(*fifoPolicy).queue
					if policy == schedulePolicyNameRequeryEDF {
						p = &requeryEDFPolicy{regular: p.(*fifoPolicy), requery: newMergeTaskQueue(""), diagnostics: d}
					}
					q.tasks = make([]*queuedTask, width, width+1)
					candidateQueued := newQueuedTask(candidate, time.Now())
					if d != nil {
						candidateQueued.diagnostics = d.admission(candidate, time.Now())
					}
					for i := range q.tasks {
						q.tasks[i] = candidateQueued
					}
					storage := q.tasks
					deadline, _ := candidate.Context().Deadline()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						now := time.Now()
						candidate.nq = 1
						candidateQueued.Task = candidate
						storage[0] = candidateQueued
						q.tasks = storage
						q.count = width
						queued := newQueuedTask(task, now)
						if d != nil {
							*candidateQueued.diagnostics = TaskDiagnostics{owner: d, kind: diagSearch, requests: 1, nq: 1, admitted: now, deadline: deadline}
							d.kinds[diagSearch].queue = int64(width)
							queued.diagnostics = d.admission(task, now)
						}
						added, err := p.Push(queued)
						if d != nil {
							queued.diagnostics.pushed(added, err)
							if i%1024 == 0 {
								d.flush(time.Now())
							}
						}
						if width == 1 {
							popped := p.Pop(time.Now())
							if d != nil && popped.diagnostics != nil {
								outcome := readTaskQueueOutcomeScheduled
								if scenario == "cancel" {
									outcome = readTaskQueueOutcomeExpired
								}
								popped.diagnostics.queueEvent(popped.Task, outcome)
								if outcome == readTaskQueueOutcomeScheduled {
									popped.diagnostics.beforeDrop(context.Canceled)
								}
							}
						}
					}
					b.StopTimer()
					if d != nil {
						d.flush(time.Now())
					}
				})
			}
		}
	}
}

func TestDiagnosticsChildContexts(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	observation := &TaskDiagnostics{owner: d}
	live := context.Background()
	expired, stop := context.WithDeadline(live, time.Now().Add(-time.Second))
	defer stop()
	canceled, cancel := context.WithCancel(live)
	cancel()
	for _, err := range []error{nil, context.DeadlineExceeded} {
		for _, ctx := range []context.Context{live, expired, canceled} {
			observation.ChildDone(ctx, err)
		}
	}
	for failed := range d.children {
		for state := range d.children[failed] {
			require.EqualValues(t, 1, testutil.ToFloat64(d.children[failed][state]))
		}
	}
}

func TestDiagnosticsEDFChoices(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameRequeryEDF, false)
	p := newRequeryEDFPolicy()
	p.diagnostics = d
	now := time.Now()
	regular := diagnosticTask(t, d, now.Add(time.Minute), false, 1)
	p.Push(regular)
	regular.diagnostics.pushed(1, nil)
	for range 2 {
		ctx, cancel := context.WithDeadline(context.Background(), now.Add(30*time.Second))
		defer cancel()
		task := newMockTask(mockTaskConfig{ctx: contextutil.WithQueryLabel(ctx, metrics.ReQueryLabel)})
		queued := newQueuedTask(task, now)
		queued.diagnostics = d.admission(task, now)
		p.Push(queued)
		queued.diagnostics.pushed(1, nil)
	}
	for range 3 {
		selected := p.Pop(now)
		selected.diagnostics.queueEvent(selected.Task, readTaskQueueOutcomeScheduled)
	}
	require.Nil(t, p.Pop(now))
	d.flush(time.Now())
	require.EqualValues(t, 2, testutil.ToFloat64(d.choice[1][edfChoiceEarlierDeadline].metric))
	require.EqualValues(t, 1, testutil.ToFloat64(d.choice[0][edfChoiceOnlyLane].metric))
	require.Zero(t, testutil.ToFloat64(d.choice[0][edfChoiceEarlierDeadline].metric))
	require.EqualValues(t, 2, testutil.ToFloat64(d.streakPeak))
	require.EqualValues(t, 1, diagnosticHistogram(t, d.streak).GetSampleCount())
	require.EqualValues(t, 2, diagnosticHistogram(t, d.streak).GetSampleSum())
}

func TestDiagnosticsRejectionsAndQueueCancellation(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	s := &scheduler{policy: newFIFOPolicy(), diagnostics: d, execChan: make(chan Task)}
	cfg := &paramtable.Get().QueryNodeCfg
	require.NoError(t, paramtable.Get().Save(cfg.MaxUnsolvedQueueSize.Key, "1"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.MaxUnsolvedQueueSize.Key) })
	add := func(ctx context.Context) error {
		errCh := make(chan error, 1)
		s.handleAddTaskRequest(addTaskReq{task: newMockTask(mockTaskConfig{ctx: ctx}), err: errCh}, time.Now())
		return <-errCh
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	require.ErrorIs(t, add(canceled), context.Canceled)
	require.ErrorIs(t, add(expired), context.DeadlineExceeded)
	ctx, cancelQueued := context.WithCancel(context.Background())
	require.NoError(t, add(ctx))
	require.Error(t, add(context.Background()))
	cancelQueued()
	s.cleanupExpiredTasks(time.Now())
	d.flush(time.Now())
	for _, event := range []int{diagRejectCanceled, diagRejectDeadline, diagRejectFull} {
		require.EqualValues(t, 1, testutil.ToFloat64(d.kinds[diagSearch].events[event][1].metric))
	}
	require.EqualValues(t, 1, testutil.ToFloat64(d.kinds[diagSearch].events[diagQueueCanceled][0].metric))
	require.Zero(t, d.kinds[diagSearch].queue)
}

func TestDiagnosticsPublisherRestartAndNodeIdentity(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	originalID := paramtable.GetNodeID()
	t.Cleanup(func() { paramtable.SetNodeID(originalID) })
	for _, nodeID := range []int64{10001, 10002} {
		paramtable.SetNodeID(nodeID)
		for range 2 {
			d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
			d.kinds[diagSearch].events[diagAdmitted][1].value++
			d.flush(time.Now())
			d.flush(time.Now()) // delta publication is idempotent, including after scheduler recreation.
		}
		counter := metrics.QueryNodeSchedulerDiagnosticEvents.WithLabelValues(paramtable.GetStringNodeID(), "fifo", "search", "admitted", "requests")
		require.EqualValues(t, 2, testutil.ToFloat64(counter))
	}
}

func TestDiagnosticsSummaryGate(t *testing.T) {
	paramtable.Init()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	start := d.lastLog
	d.flush(start.Add(time.Minute))
	require.Equal(t, start, d.lastLog)
	d.logEnabled = true
	d.flush(start.Add(29 * time.Second))
	require.Equal(t, start, d.lastLog)
	d.flush(start.Add(30 * time.Second))
	require.Equal(t, start.Add(30*time.Second), d.lastLog)
	d.flush(start.Add(59 * time.Second))
	require.Equal(t, start.Add(30*time.Second), d.lastLog)
}

func TestDiagnosticCandidateDeadlineBuckets(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	d := newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	deadline := time.Now().Add(time.Minute)
	parent := diagnosticTask(t, d, deadline, true, 1)
	for _, gap := range []time.Duration{10, 25, 50, 100, 101} {
		input := diagnosticTask(t, d, deadline.Add(gap*time.Millisecond), true, 1)
		require.Equal(t, gap <= 50, canMergeDeadline(parent, input, 50*time.Millisecond))
	}
	d.flush(time.Now())
	for bucket := range 5 {
		require.EqualValues(t, 1, testutil.ToFloat64(d.candidateDDL[bucket].metric))
	}
	require.EqualValues(t, 286*time.Millisecond, d.candidateDDL[5].value)
}

func TestDiagnosticsHandoffCancellation(t *testing.T) {
	paramtable.Init()
	resetDiagnosticMetrics()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	s.diagnostics = newSchedulerDiagnostics(schedulePolicyNameFIFO, false)
	ctx, cancel := context.WithCancel(context.Background())
	task := newMockTask(mockTaskConfig{ctx: ctx})
	queued := newQueuedTask(task, time.Now())
	queued.diagnostics = s.diagnostics.admission(task, time.Now())
	queued.diagnostics.pushed(1, nil)
	queued.diagnostics.queueEvent(task, readTaskQueueOutcomeScheduled)
	cancel()
	s.wg.Add(1)
	go s.exec()
	s.execChan <- queued.executionTask()
	require.ErrorIs(t, task.Wait(), context.Canceled)
	close(s.execChan)
	s.wg.Wait()
	s.Stop()
	k := &s.diagnostics.kinds[diagSearch]
	require.EqualValues(t, 1, testutil.ToFloat64(k.events[diagBeforeCanceled][0].metric))
	require.Zero(t, testutil.ToFloat64(k.events[diagExecuteStart][0].metric))
	require.Zero(t, testutil.ToFloat64(k.pendingMetric))
}
