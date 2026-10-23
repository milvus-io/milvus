package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var diagnosticBenchmarkInit sync.Once

func initDiagnosticBenchmark() {
	diagnosticBenchmarkInit.Do(func() {
		paramtable.InitWithBaseTable(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.Interval(time.Hour)))
	})
}

// The pre-diagnostics loop is kept only as a benchmark reference, so disabling
// diagnostics can be compared with the old path in the same binary/CPU run.
func legacyMergeForBenchmark(q *mergeTaskQueue, task *queuedTask, maxNQ int64, ratio float64, gap time.Duration) bool {
	mergeTask := tryIntoMergeTask(task.Task)
	if mergeTask == nil || mergeTask.NQ() >= maxNQ {
		return false
	}
	for i := len(q.tasks) - 1; i >= 0; i-- {
		candidate := q.tasks[i]
		if !candidate.valid() {
			continue
		}
		if mergeCandidate := tryIntoMergeTask(candidate.Task); mergeCandidate != nil {
			if canMergeNQ(mergeCandidate, mergeTask, maxNQ, ratio) && legacyDeadlineForBenchmark(candidate, task, gap) && mergeCandidate.MergeWith(mergeTask) {
				if deadline := task.schedulingDeadline; !deadline.IsZero() && (candidate.schedulingDeadline.IsZero() || deadline.Before(candidate.schedulingDeadline)) {
					candidate.schedulingDeadline = deadline
				}
				return true
			}
		}
	}
	return false
}

func legacyDeadlineForBenchmark(task, other *queuedTask, gap time.Duration) bool {
	if gap < 0 {
		return true
	}
	d, ok := task.Context().Deadline()
	od, otherOK := other.Context().Deadline()
	if !ok && !otherOK {
		return true
	}
	if ok != otherOK {
		return false
	}
	if d.After(od) {
		d, od = od, d
	}
	return od.Sub(d) <= gap
}

func BenchmarkDiagnosticsDisabledMerge(b *testing.B) {
	initDiagnosticBenchmark()
	for _, scenario := range []string{"merge", "deadline", "business", "scan1024"} {
		for _, legacy := range []bool{true, false} {
			name := "current_off"
			if legacy {
				name = "legacy"
			}
			b.Run(scenario+"/"+name, func(b *testing.B) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
				defer cancel()
				otherCtx, otherCancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Hour))
				defer otherCancel()
				input := newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx, mergeAble: true}), time.Now())
				candidate := newMockTask(mockTaskConfig{ctx: ctx, mergeAble: scenario == "merge"}).(*MockTask)
				if scenario == "deadline" {
					candidate.ctx = otherCtx
				}
				width := 1
				if scenario == "scan1024" {
					width = 1024
				}
				q := newMergeTaskQueue("")
				for range width {
					q.push(newQueuedTask(candidate, time.Now()))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					candidate.nq = 1
					if legacy {
						legacyMergeForBenchmark(q, input, 64, 16, 50*time.Millisecond)
					} else {
						q.tryMerge(input, 64, 16, 50*time.Millisecond)
					}
				}
			})
		}
	}
}

func BenchmarkDiagnosticsPublish(b *testing.B) {
	initDiagnosticBenchmark()
	d := newSchedulerDiagnostics(schedulePolicyNameRequeryEDF, false)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d.kinds[diagSearch].events[diagAdmitted][1].value++
		d.flush(time.Now())
	}
}

// Run with -benchtime=1x -count=1 to include one real log emission, rather than
// benchmarking a rate limiter suppressing repeated copies of the summary.
func BenchmarkDiagnosticsSummaryOnce(b *testing.B) {
	initDiagnosticBenchmark()
	d := newSchedulerDiagnostics(schedulePolicyNameRequeryEDF, true)
	d.lastLog = time.Time{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d.flush(time.Now())
	}
}

func BenchmarkRequeryPriorityPolicy(b *testing.B) {
	initDiagnosticBenchmark()
	regularTask := newMockTask(mockTaskConfig{})
	requeryCtx := contextutil.WithQueryLabel(context.Background(), metrics.ReQueryLabel)
	requeryTask := newMockTask(mockTaskConfig{ctx: requeryCtx})
	now := time.Now()
	for _, scenario := range []string{"regular_only", "requery_only", "contended", "full_requery"} {
		b.Run(scenario, func(b *testing.B) {
			width := 64
			if scenario == "full_requery" {
				width = 1024
			}
			regularSlots := make([]*queuedTask, width)
			requerySlots := make([]*queuedTask, width)
			policy := &requeryPriorityPolicy{
				requeryLanes: &requeryLanes{
					regular: &fifoPolicy{queue: newMergeTaskQueue("")},
					requery: newMergeTaskQueue(metrics.ReQueryLabel), requeryCapacity: 1024,
				},
				configuredCredit: 3, remainingCredit: 3,
			}
			resetQueues := func() {
				if scenario == "regular_only" || scenario == "contended" {
					for i := range regularSlots {
						regularSlots[i] = &queuedTask{Task: regularTask, enqueueTime: now}
					}
					policy.regular.queue.tasks = regularSlots
					policy.regular.queue.count = len(regularSlots)
				}
				if scenario != "regular_only" {
					for i := range requerySlots {
						requerySlots[i] = &queuedTask{Task: requeryTask, enqueueTime: now}
					}
					policy.requery.tasks = requerySlots
					policy.requery.count = len(requerySlots)
				}
			}
			resetQueues()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if policy.Len() == 0 || scenario == "contended" && (policy.regular.Len() == 0 || policy.requery.len() == 0) {
					b.StopTimer()
					resetQueues()
					b.StartTimer()
				}
				task := policy.Pop(now)
				policy.onTaskServed(task)
			}
		})
	}
}
