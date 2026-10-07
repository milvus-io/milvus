package scheduler

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	diagSearch = iota
	diagRequery
	diagOther
	diagKinds
)

const (
	diagAdmitted = iota
	diagMerged
	diagNewGroup
	diagSelected
	diagQueueDeadline
	diagQueueCanceled
	diagQueueCleared
	diagStagedCleared
	diagRejectDeadline
	diagRejectCanceled
	diagRejectFull
	diagRejectOther
	diagExecuteStart
	diagBeforeDeadline
	diagBeforeCanceled
	diagBeforeOther
	diagFinishSuccess
	diagFinishDeadline
	diagFinishCanceled
	diagFinishOther
	diagStartOverdue
	diagFinishOverdue
	diagAddDeadline
	diagAddCanceled
	diagEvents
)

var diagnosticEventNames = [...]string{
	"admitted", "merged", "new_group", "selected", "queue_deadline", "queue_canceled", "queue_cleared", "staged_cleared",
	"reject_deadline", "reject_canceled", "reject_full", "reject_other", "execute_start",
	"before_execute_deadline", "before_execute_canceled", "before_execute_other",
	"execute_success", "execute_deadline", "execute_canceled", "execute_other",
	"execute_start_overdue", "execute_success_overdue", "add_deadline", "add_canceled",
}

// MergeRejectReason describes only the first business check actually reached.
// These counters are updated by SearchTask on the scheduler goroutine.
type MergeRejectReason int

const (
	MergeFilterOnly MergeRejectReason = iota
	MergeExprCache
	MergeDatabase
	MergeCollection
	MergeMVCC
	MergeDSL
	MergeChannel
	MergeTopK
	MergePartitions
	MergeSegments
	MergePlan
	mergeNQ
	mergeDeadline
	mergeBusiness
	mergeInvalid
	mergeNotMergeable
	mergeInputTooLarge
	mergeNewAfterDeadline
	mergeInvalidTiming
	mergeReasons
)

var diagnosticMergeNames = [...]string{
	"filter_only", "expr_cache", "database", "collection", "mvcc", "dsl", "channel", "topk", "partitions", "segments", "plan",
	"candidate_nq", "candidate_deadline", "candidate_business", "invalid_slot", "non_mergeable_candidate", "input_max_nq",
	"new_group_after_deadline_rejection", "invalid_timing",
}

type diagnosticCounter struct {
	value, published uint64 // schedule goroutine only
	metric           prometheus.Counter
}

func (c *diagnosticCounter) flush() {
	if c.metric != nil && c.value != c.published {
		c.metric.Add(float64(c.value - c.published))
		c.published = c.value
	}
}

type kindDiagnostics struct {
	events        [diagEvents][3]diagnosticCounter // groups, original requests, NQ
	duration      [5]prometheus.Observer           // add, queue, handoff, pre-execute, pool
	slack         [3]prometheus.Observer           // admission, pop, execute
	arrival       prometheus.Observer
	lastArrival   time.Time
	lastDeadline  time.Time
	queue, peak   int64
	queueMetric   prometheus.Gauge
	peakMetric    prometheus.Gauge
	pendingMetric prometheus.Gauge
	runningMetric prometheus.Gauge
	executeCost   [5][4][2]prometheus.Counter // batch bucket, returned outcome, count/nanoseconds
}

type schedulerDiagnostics struct {
	kinds                     [diagKinds]kindDiagnostics
	merge                     [mergeReasons]diagnosticCounter
	shape                     [3][2]prometheus.Observer // start, finish, pre-exec drop; requests/NQ
	children                  [2][3]prometheus.Counter  // group failed; live/deadline/cancel context
	arrivalDDL                prometheus.Observer
	candidateDDL              [6]diagnosticCounter // <=10/25/50/100ms, >100ms, sum nanoseconds
	rootDDL                   prometheus.Observer
	batch, scan               prometheus.Observer
	streak                    prometheus.Observer
	requeryStreak             int64
	maxRequeryStreak          int64
	streakCurrent, streakPeak prometheus.Gauge
	logEnabled                bool
	lastLog                   time.Time
	policy                    string
	nodeID                    int64
}

func newSchedulerDiagnostics(policy string, logEnabled bool) *schedulerDiagnostics {
	nodeID := paramtable.GetStringNodeID()
	d := &schedulerDiagnostics{policy: policy, nodeID: paramtable.GetNodeID(), logEnabled: logEnabled, lastLog: time.Now()}
	for kind, name := range []string{"search", "requery", "other"} {
		k := &d.kinds[kind]
		for event, eventName := range diagnosticEventNames {
			for unit, unitName := range []string{"groups", "requests", "nq"} {
				// Do not allocate meaningless group counts for original-input events.
				input := event == diagAdmitted || event == diagMerged || event >= diagRejectDeadline && event <= diagRejectOther || event >= diagAddDeadline
				groupOnly := event == diagNewGroup || event == diagStartOverdue || event == diagFinishOverdue
				if input && unit == 0 || groupOnly && unit != 0 {
					continue
				}
				k.events[event][unit].metric = metrics.QueryNodeSchedulerDiagnosticEvents.WithLabelValues(nodeID, policy, name, eventName, unitName)
			}
		}
		for stage, nameStage := range []string{"add", "policy_queue", "pop_to_handoff", "pre_execute", "pool_wait"} {
			k.duration[stage] = metrics.QueryNodeSchedulerDiagnosticDuration.WithLabelValues(nodeID, policy, name, nameStage)
		}
		for stage, nameStage := range []string{"admission", "pop", "execute"} {
			k.slack[stage] = metrics.QueryNodeSchedulerDiagnosticSlack.WithLabelValues(nodeID, policy, name, nameStage)
		}
		k.arrival = metrics.QueryNodeSchedulerDiagnosticGap.WithLabelValues(nodeID, policy, name, "arrival")
		k.queueMetric = metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(nodeID, policy, name, "current")
		k.peakMetric = metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(nodeID, policy, name, "peak")
		k.pendingMetric = metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(nodeID, policy, name, "selected_not_started")
		k.runningMetric = metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(nodeID, policy, name, "running")
		for batch, batchName := range []string{"1", "2_4", "5_8", "9_16", "gt16"} {
			if kind != diagSearch && batch > 0 {
				break
			}
			for outcome, outcomeName := range []string{"success", "deadline", "cancel", "other"} {
				for unit, unitName := range []string{"groups", "nanoseconds"} {
					k.executeCost[batch][outcome][unit] = metrics.QueryNodeSchedulerDiagnosticCost.WithLabelValues(nodeID, policy, name, batchName, outcomeName, unitName)
				}
			}
		}
	}
	for reason, name := range diagnosticMergeNames {
		d.merge[reason].metric = metrics.QueryNodeSchedulerDiagnosticMerge.WithLabelValues(nodeID, policy, name)
	}
	for stage, name := range []string{"execute_start", "execute_finish", "pre_execute_drop"} {
		for unit, unitName := range []string{"requests", "nq"} {
			d.shape[stage][unit] = metrics.QueryNodeSchedulerDiagnosticShape.WithLabelValues(nodeID, policy, name, unitName)
		}
	}
	for failed := range d.children {
		for outcome, name := range []string{"live", "deadline", "cancel"} {
			d.children[failed][outcome] = metrics.QueryNodeSchedulerDiagnosticChildren.WithLabelValues(nodeID, policy, name, strconv.FormatBool(failed != 0))
		}
	}
	d.arrivalDDL = metrics.QueryNodeSchedulerDiagnosticGap.WithLabelValues(nodeID, policy, "search", "arrival_deadline")
	for bucket, name := range []string{"le_10ms", "10_25ms", "25_50ms", "50_100ms", "gt_100ms", "nanoseconds"} {
		d.candidateDDL[bucket].metric = metrics.QueryNodeSchedulerDiagnosticCandidateDeadline.WithLabelValues(nodeID, policy, name)
	}
	d.rootDDL = metrics.QueryNodeSchedulerDiagnosticGap.WithLabelValues(nodeID, policy, "search", "root_earliest_deadline")
	d.batch = metrics.QueryNodeSchedulerDiagnosticShape.WithLabelValues(nodeID, policy, "receive_batch", "requests")
	d.scan = metrics.QueryNodeSchedulerDiagnosticShape.WithLabelValues(nodeID, policy, "merge_scan", "slots")
	d.streakCurrent = metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(nodeID, policy, "requery", "streak_current")
	d.streakPeak = metrics.QueryNodeSchedulerDiagnosticQueue.WithLabelValues(nodeID, policy, "requery", "streak_peak")
	d.streak = metrics.QueryNodeSchedulerDiagnosticShape.WithLabelValues(nodeID, policy, "requery_streak", "groups")
	return d
}

func diagnosticKind(task Task) int {
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		return diagRequery
	}
	if _, ok := task.(MergeTask); ok {
		return diagSearch
	}
	return diagOther
}

// TaskDiagnostics is attached only when diagnostics are enabled. The queue owns
// it until handoff, then the executor owns it; no shared mutable task state is
// sampled by the periodic publisher. It never changes cancellation/deadlines.
type TaskDiagnostics struct {
	owner                      *schedulerDiagnostics
	kind                       int
	requests, nq               int64
	admitted, popped, deadline time.Time
	deadlineRejected           bool
	mergeAttempted             bool
	scanned                    uint32
}

func (d *schedulerDiagnostics) admission(task Task, addStart time.Time) *TaskDiagnostics {
	now := time.Now() // Never use schedule's pre-select timestamp after an idle wait.
	kind := diagnosticKind(task)
	k := &d.kinds[kind]
	if !k.lastArrival.IsZero() {
		observeMillis(k.arrival, now.Sub(k.lastArrival))
	}
	k.lastArrival = now
	deadline, _ := task.Context().Deadline()
	if kind == diagSearch && !deadline.IsZero() && !k.lastDeadline.IsZero() {
		observeMillis(d.arrivalDDL, deadline.Sub(k.lastDeadline).Abs())
	}
	k.lastDeadline = deadline
	if !addStart.IsZero() {
		observeMillis(k.duration[0], now.Sub(addStart))
	}
	observation := &TaskDiagnostics{owner: d, kind: kind, requests: 1, nq: task.NQ(), admitted: now, deadline: deadline}
	if task, ok := task.(interface{ SetSchedulerDiagnostics(*TaskDiagnostics) int64 }); ok {
		observation.requests = max(1, task.SetSchedulerDiagnostics(observation))
	}
	observation.observeSlack(0, now)
	return observation
}

func (t *TaskDiagnostics) event(event int, worker bool) {
	for unit, value := range [3]int64{1, t.requests, t.nq} {
		counter := &t.owner.kinds[t.kind].events[event][unit]
		if worker {
			if counter.metric != nil {
				counter.metric.Add(float64(value))
			}
		} else {
			counter.value += uint64(value)
		}
	}
}

func (t *TaskDiagnostics) pushed(added int, err error) {
	if t.mergeAttempted {
		t.owner.scan.Observe(float64(t.scanned))
	}
	if err != nil {
		t.event(diagRejectOther, false)
		return
	}
	t.event(diagAdmitted, false)
	if added == 0 {
		t.event(diagMerged, false)
	} else {
		t.event(diagNewGroup, false)
		k := &t.owner.kinds[t.kind]
		k.queue++
		k.peak = max(k.peak, k.queue)
		if t.deadlineRejected {
			t.owner.merge[mergeNewAfterDeadline].value++
		}
	}
}

func (t *TaskDiagnostics) merge(other *TaskDiagnostics) {
	if other == nil {
		return
	}
	t.requests += other.requests
	t.nq += other.nq
	if !other.deadline.IsZero() && (t.deadline.IsZero() || other.deadline.Before(t.deadline)) {
		t.deadline = other.deadline
	}
}

// RecordMergeRejected records the first business rejection without re-running
// any compatibility check or changing the merge decision.
func (t *TaskDiagnostics) RecordMergeRejected(reason MergeRejectReason) {
	if t != nil {
		t.owner.merge[reason].value++
	}
}

// ChildDone is called inside SearchTask's existing recursive Done traversal.
// A live child inheriting a failed group's result is evidence, not a new error.
func (t *TaskDiagnostics) ChildDone(ctx context.Context, err error) {
	failed, outcome := 0, 0
	if err != nil {
		failed = 1
	}
	switch diagnosticOutcome(ctx.Err()) {
	case 1:
		outcome = 1
	case 2:
		outcome = 2
	}
	if outcome == 0 {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			outcome = 1
		}
	}
	t.owner.children[failed][outcome].Inc()
}

func observeMillis(observer prometheus.Observer, value time.Duration) {
	observer.Observe(float64(value) / float64(time.Millisecond))
}

func (t *TaskDiagnostics) observeSlack(stage int, now time.Time) {
	if !t.deadline.IsZero() {
		observeMillis(t.owner.kinds[t.kind].slack[stage], t.deadline.Sub(now))
	}
}

func (t *TaskDiagnostics) shape(stage int) {
	if t.kind == diagSearch {
		t.owner.shape[stage][0].Observe(float64(t.requests))
		t.owner.shape[stage][1].Observe(float64(t.nq))
	}
}

func (t *TaskDiagnostics) queueEvent(task Task, outcome string) {
	now := time.Now()
	if !t.popped.IsZero() { // An admin clear can remove the already selected task.
		t.owner.kinds[t.kind].pendingMetric.Dec()
		t.event(diagStagedCleared, false)
		t.shape(2)
		return
	}
	k := &t.owner.kinds[t.kind]
	k.queue--
	if now.Before(t.admitted) {
		t.owner.merge[mergeInvalidTiming].value++
	} else {
		observeMillis(k.duration[1], now.Sub(t.admitted))
	}
	switch outcome {
	case readTaskQueueOutcomeScheduled:
		k.pendingMetric.Inc()
		t.popped = now
		t.event(diagSelected, false)
		t.observeSlack(1, now)
		if rootDeadline, ok := task.Context().Deadline(); ok && !t.deadline.IsZero() && t.kind == diagSearch {
			observeMillis(t.owner.rootDDL, rootDeadline.Sub(t.deadline))
		}
		if t.owner.streak != nil {
			if t.kind == diagRequery {
				t.owner.requeryStreak++
				t.owner.maxRequeryStreak = max(t.owner.maxRequeryStreak, t.owner.requeryStreak)
			} else {
				t.owner.finishStreak()
			}
		}
	case readTaskQueueOutcomeCleared:
		t.event(diagQueueCleared, false)
		t.shape(2)
	default:
		event := diagQueueDeadline
		if diagnosticOutcome(task.Context().Err()) == 2 {
			event = diagQueueCanceled
		}
		t.event(event, false)
		t.shape(2)
	}
}

func diagnosticOutcome(err error) int {
	// Context.Err returns these sentinels directly; retain graph traversal only
	// for wrapped/marked errors rather than paying for it on every cancellation.
	switch err {
	case nil:
		return 0
	case context.DeadlineExceeded:
		return 1
	case context.Canceled:
		return 2
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return 1
	case errors.Is(err, context.Canceled):
		return 2
	default:
		return 3
	}
}

func (t *TaskDiagnostics) beforeDrop(err error) {
	t.owner.kinds[t.kind].pendingMetric.Dec()
	t.event(diagBeforeDeadline+max(1, diagnosticOutcome(err))-1, true)
	t.shape(2)
}

func (t *TaskDiagnostics) started(submitted, now time.Time) {
	t.owner.kinds[t.kind].pendingMetric.Dec()
	t.owner.kinds[t.kind].runningMetric.Inc()
	observeMillis(t.owner.kinds[t.kind].duration[4], now.Sub(submitted))
	t.event(diagExecuteStart, true)
	t.shape(0)
	t.observeSlack(2, now)
	if !t.deadline.IsZero() && !now.Before(t.deadline) {
		t.event(diagStartOverdue, true)
	}
}

func (t *TaskDiagnostics) finished(err error, elapsed time.Duration, now time.Time) {
	t.owner.kinds[t.kind].runningMetric.Dec()
	outcome := diagnosticOutcome(err)
	t.event(diagFinishSuccess+outcome, true)
	t.shape(1)
	if err == nil && !t.deadline.IsZero() && !now.Before(t.deadline) {
		t.event(diagFinishOverdue, true)
	}
	batch := 0
	if t.kind == diagSearch {
		switch {
		case t.requests > 16:
			batch = 4
		case t.requests > 8:
			batch = 3
		case t.requests > 4:
			batch = 2
		case t.requests > 1:
			batch = 1
		}
	}
	cost := &t.owner.kinds[t.kind].executeCost[batch][outcome]
	cost[0].Inc()
	cost[1].Add(float64(elapsed))
}

func (d *schedulerDiagnostics) finishStreak() {
	if d.requeryStreak > 0 {
		d.streak.Observe(float64(d.requeryStreak))
		d.requeryStreak = 0
	}
}

func (d *schedulerDiagnostics) flush(now time.Time) {
	if d.streak != nil {
		d.streakCurrent.Set(float64(d.requeryStreak))
		d.streakPeak.Set(float64(d.maxRequeryStreak))
	}
	for i := range d.kinds {
		k := &d.kinds[i]
		for event := range k.events {
			for unit := range k.events[event] {
				k.events[event][unit].flush()
			}
		}
		k.queueMetric.Set(float64(k.queue))
		k.peakMetric.Set(float64(k.peak))
	}
	for i := range d.merge {
		d.merge[i].flush()
	}
	for i := range d.candidateDDL {
		d.candidateDDL[i].flush()
	}
	if d.logEnabled && now.Sub(d.lastLog) >= 30*time.Second {
		d.lastLog = now
		mlog.RatedInfo(context.TODO(), 1.0/30, "read scheduler diagnostic summary",
			mlog.FieldNodeID(d.nodeID), mlog.String("policy", d.policy), mlog.String("version", "1"), mlog.String("counters", "cumulative"),
			mlog.Int64("searchQueue", d.kinds[diagSearch].queue), mlog.Int64("requeryQueue", d.kinds[diagRequery].queue),
			mlog.Uint64("searchAdmitted", d.kinds[diagSearch].events[diagAdmitted][1].value),
			mlog.Uint64("searchMerged", d.kinds[diagSearch].events[diagMerged][1].value),
			mlog.Uint64("requeryAdmitted", d.kinds[diagRequery].events[diagAdmitted][1].value),
			mlog.Uint64("requerySelected", d.kinds[diagRequery].events[diagSelected][0].value),
			mlog.Uint64("requeryQueueDeadline", d.kinds[diagRequery].events[diagQueueDeadline][0].value),
			mlog.Uint64("requeryQueueCanceled", d.kinds[diagRequery].events[diagQueueCanceled][0].value),
			mlog.Int64("requerySelectionStreakPeak", d.maxRequeryStreak),
			mlog.Int64("requeryQueuePeak", d.kinds[diagRequery].peak),
			mlog.Uint64("newGroupAfterDeadlineReject", d.merge[mergeNewAfterDeadline].value))
	}
}
