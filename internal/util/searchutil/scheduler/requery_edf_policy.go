package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ schedulePolicy = (*requeryEDFPolicy)(nil)
var _ readTaskObserver = (*requeryEDFPolicy)(nil)

// requeryEDFPolicy compares the heads of two FIFO lanes, not every queued task.
// Queue ownership and capacity accounting stay on the scheduling goroutine.
type requeryEDFPolicy struct {
	regular          *fifoPolicy
	requery          *mergeTaskQueue
	requeryCapacity  int64
	stats            [2]edfLaneStats // regular, requery
	lastLog          time.Time
	requeryStreak    int64
	maxRequeryStreak int64
}

func newRequeryEDFPolicy() *requeryEDFPolicy {
	cfg := &paramtable.Get().QueryNodeCfg
	requeryCapacity := cfg.RequeryUnsolvedQueueSize.GetAsInt64()
	mlog.Info(context.TODO(), "requery EDF policy enabled",
		mlog.FieldNodeID(paramtable.GetNodeID()),
		mlog.Int64("regularCapacity", cfg.MaxUnsolvedQueueSize.GetAsInt64()),
		mlog.Int64("requeryCapacity", requeryCapacity),
		mlog.String("selection", "FIFO lane heads at Pop; finite deadline ties favor requery"))
	return &requeryEDFPolicy{
		regular:         &fifoPolicy{queue: newMergeTaskQueue("")},
		requery:         newMergeTaskQueue(metrics.ReQueryLabel),
		requeryCapacity: requeryCapacity,
		lastLog:         time.Now(),
	}
}

func (p *requeryEDFPolicy) CheckAdmission(task Task, _ int64) error {
	cfg := &paramtable.Get().QueryNodeCfg
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		if p.requeryCapacity > 0 && int64(p.requery.len()) >= p.requeryCapacity {
			return merr.WrapErrTooManyRequests(
				int32(p.requeryCapacity),
				fmt.Sprintf("limit by %s", cfg.RequeryUnsolvedQueueSize.Key),
			)
		}
		return nil
	}
	return p.regular.CheckAdmission(task, int64(p.regular.Len()))
}

func (p *requeryEDFPolicy) Push(task *queuedTask) (int, error) {
	task.schedulingDeadline, _ = task.Context().Deadline()
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		p.requery.push(task)
		p.stats[1].admitted++
		p.stats[1].queuePeak = max(p.stats[1].queuePeak, p.requery.len())
		return 1, nil
	}
	added, err := p.regular.Push(task)
	if err == nil {
		p.stats[0].admitted++
		if added == 0 {
			p.stats[0].merged++
		}
		p.stats[0].queuePeak = max(p.stats[0].queuePeak, p.regular.Len())
	}
	return added, err
}

// Pop compares lane heads at selection time. The generic scheduler may stage
// the selected task before execution; later arrivals do not replace it.
func (p *requeryEDFPolicy) Pop(now time.Time) *queuedTask {
	p.logStats(now)
	regular, requery := p.regular.queue.front(), p.requery.front()
	if regular.cleanupReady(now) {
		return p.regular.queue.pop()
	}
	if requery.cleanupReady(now) {
		return p.requery.pop()
	}
	switch {
	case regular.valid() && requery.valid():
		return p.earlierDeadlineQueue(regular, requery).pop()
	case regular.valid():
		return p.regular.queue.pop()
	case requery.valid():
		return p.requery.pop()
	default:
		return nil
	}
}

func (p *requeryEDFPolicy) earlierDeadlineQueue(regular, requery *queuedTask) *mergeTaskQueue {
	regularDeadline, requeryDeadline := regular.schedulingDeadline, requery.schedulingDeadline
	if regularDeadline.IsZero() && requeryDeadline.IsZero() {
		// Without deadlines, preserve arrival order across the two lanes.
		if regular.enqueueTime.Before(requery.enqueueTime) || regular.enqueueTime.Equal(requery.enqueueTime) {
			return p.recordChoice(p.regular.queue, edfChoiceMissingDeadline)
		}
		return p.recordChoice(p.requery, edfChoiceMissingDeadline)
	}
	if regularDeadline.IsZero() {
		return p.recordChoice(p.requery, edfChoiceMissingDeadline)
	}
	if requeryDeadline.IsZero() {
		return p.recordChoice(p.regular.queue, edfChoiceMissingDeadline)
	}
	if regularDeadline.Before(requeryDeadline) {
		return p.recordChoice(p.regular.queue, edfChoiceEarlierDeadline)
	}
	// Equal finite deadlines favor completion of an existing search request.
	if regularDeadline.Equal(requeryDeadline) {
		return p.recordChoice(p.requery, edfChoiceEqualDeadline)
	}
	return p.recordChoice(p.requery, edfChoiceEarlierDeadline)
}

func (p *requeryEDFPolicy) Cleanup(now time.Time) []*queuedTask {
	// EDF only reclaims tasks whose actual deadline has passed.
	return append(p.regular.queue.cleanup(now), p.requery.cleanup(now)...)
}

func (p *requeryEDFPolicy) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	return append(p.regular.Remove(filter, now), p.requery.remove(filter, now)...)
}

func (p *requeryEDFPolicy) Len() int {
	return p.regular.Len() + p.requery.len()
}

const (
	edfChoiceEarlierDeadline = iota
	edfChoiceEqualDeadline
	edfChoiceMissingDeadline
)

const (
	edfSuccess = iota
	edfTimeout
	edfCanceled
	edfOtherError
)

// Counters are cumulative, so adjacent logs can be differenced without racing
// worker resets; queue peaks, minimum slack and maximum streak cover a log window.
// Admitted includes merged requests; executed counts completed scheduler tasks,
// and executeNanos measures Execute wall time, not CPU time or client latency.
// Atomic fields are sampled independently, not as a transactional snapshot.
type edfLaneStats struct {
	admitted, merged, rejected, selected   int64
	queuedTimeout, queuedCanceled, cleared int64
	choices                                [3]int64 // Only selections where BOTH lane heads are live.
	queuePeak                              int
	deadlineSamples                        int64
	minDeadlineLeft                        time.Duration
	executeNanos                           atomic.Int64
	executed                               [4]atomic.Int64
	beforeExecute                          [4]atomic.Int64
}

func (p *requeryEDFPolicy) recordChoice(queue *mergeTaskQueue, reason int) *mergeTaskQueue {
	lane := 0
	if queue == p.requery {
		lane = 1
	}
	p.stats[lane].choices[reason]++
	return queue
}

func (p *requeryEDFPolicy) taskStats(task Task) *edfLaneStats {
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		return &p.stats[1]
	}
	return &p.stats[0]
}

func (p *requeryEDFPolicy) onTaskRejected(task Task) {
	// CheckAdmission may run several times; count only the final rejection.
	p.taskStats(task).rejected++
}

func (p *requeryEDFPolicy) onTaskQueueEvent(task *queuedTask, now time.Time, outcome string) {
	stats := p.taskStats(task.Task)
	switch outcome {
	case readTaskQueueOutcomeScheduled:
		stats.selected++
		if deadline := task.schedulingDeadline; !deadline.IsZero() {
			left := deadline.Sub(now)
			if stats.deadlineSamples == 0 || left < stats.minDeadlineLeft {
				stats.minDeadlineLeft = left
			}
			stats.deadlineSamples++
		}
		if stats == &p.stats[1] {
			p.requeryStreak++
			p.maxRequeryStreak = max(p.maxRequeryStreak, p.requeryStreak)
		} else {
			p.requeryStreak = 0
		}
	case readTaskQueueOutcomeExpired:
		if errors.Is(cleanupTaskError(task), context.Canceled) {
			stats.queuedCanceled++
		} else {
			stats.queuedTimeout++
		}
	case readTaskQueueOutcomeCleared:
		stats.cleared++
	}
}

func (p *requeryEDFPolicy) onTaskExecutionFinished(task Task, err error, elapsed time.Duration, executed bool) {
	// Classify the returned error only: a successful Execute is not proof that
	// the client received a result, and context cancellation is not a timeout.
	stats := p.taskStats(task)
	outcome := edfSuccess
	switch {
	case err == nil:
	case errors.Is(err, context.DeadlineExceeded):
		outcome = edfTimeout
	case errors.Is(err, context.Canceled):
		outcome = edfCanceled
	default:
		outcome = edfOtherError
	}
	if !executed {
		stats.beforeExecute[outcome].Inc()
		return
	}
	stats.executeNanos.Add(int64(elapsed))
	stats.executed[outcome].Inc()
}

func (p *requeryEDFPolicy) logStats(now time.Time) {
	// Activity-driven, using the scheduler's timestamp; idle periods emit no log.
	if now.Sub(p.lastLog) < 5*time.Second || !mlog.LevelEnabled(mlog.InfoLevel) {
		return
	}
	window := now.Sub(p.lastLog)
	p.lastLog = now
	// Two call sites keep RatedInfo from suppressing one lane's log; the time
	// gate above avoids per-task field construction, rate limiting or allocation.
	mlog.RatedInfo(context.TODO(), 1, "requery EDF lane stats",
		p.stats[0].logFields("regular", p.regular.Len(), now, window, p.maxRequeryStreak)...)
	mlog.RatedInfo(context.TODO(), 1, "requery EDF lane stats",
		p.stats[1].logFields("requery", p.requery.len(), now, window, p.maxRequeryStreak)...)
	p.stats[0].queuePeak, p.stats[1].queuePeak = p.regular.Len(), p.requery.len()
	for i := range p.stats {
		p.stats[i].deadlineSamples = 0
		p.stats[i].minDeadlineLeft = 0
	}
	p.maxRequeryStreak = p.requeryStreak
}

func (s *edfLaneStats) logFields(lane string, queueLen int, now time.Time, window time.Duration, maxRequeryStreak int64) []mlog.Field {
	return []mlog.Field{
		mlog.FieldNodeID(paramtable.GetNodeID()),
		mlog.String("lane", lane),
		mlog.Time("snapshotTime", now),
		mlog.Duration("window", window),
		mlog.String("counters", "cumulative"),
		mlog.Int("queueLen", queueLen),
		mlog.Int("windowQueuePeak", s.queuePeak),
		mlog.Int64("admitted", s.admitted),
		mlog.Int64("merged", s.merged),
		mlog.Int64("rejected", s.rejected),
		mlog.Int64("selected", s.selected),
		mlog.Int64("contendedEarlierDeadline", s.choices[edfChoiceEarlierDeadline]),
		mlog.Int64("contendedEqualDeadline", s.choices[edfChoiceEqualDeadline]),
		mlog.Int64("contendedMissingDeadline", s.choices[edfChoiceMissingDeadline]),
		mlog.Int64("windowMaxRequeryStreak", maxRequeryStreak),
		mlog.Int64("windowDeadlineSamples", s.deadlineSamples),
		mlog.Float64("windowMinDeadlineLeftMs", float64(s.minDeadlineLeft)/float64(time.Millisecond)),
		mlog.Int64("queuedTimeout", s.queuedTimeout),
		mlog.Int64("queuedCanceled", s.queuedCanceled),
		mlog.Int64("cleared", s.cleared),
		mlog.Float64("executeMs", float64(s.executeNanos.Load())/float64(time.Millisecond)),
		mlog.Int64("executeSuccess", s.executed[edfSuccess].Load()),
		mlog.Int64("executeTimeout", s.executed[edfTimeout].Load()),
		mlog.Int64("executeCanceled", s.executed[edfCanceled].Load()),
		mlog.Int64("executeOtherError", s.executed[edfOtherError].Load()),
		mlog.Int64("beforeExecuteTimeout", s.beforeExecute[edfTimeout].Load()),
		mlog.Int64("beforeExecuteCanceled", s.beforeExecute[edfCanceled].Load()),
		mlog.Int64("beforeExecuteOtherError", s.beforeExecute[edfOtherError].Load()),
	}
}
