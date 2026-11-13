// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

type (
	dqlTimingKey struct{}
	dqlStage     uint8
	dqlTiming    struct {
		ctx                   context.Context
		operation, path       string
		started, stageStarted time.Time
		currentStage          dqlStage
		stageDurations        [dqlStageCount]time.Duration
		executionStarted      bool
		timer                 stage.Timer
	}
)

const (
	dqlStageNone dqlStage = iota
	dqlStageReadiness
	dqlStageExecution
	dqlStageRetryWait
	dqlStageCount
)

var dqlRecorders = map[string]*stage.Recorder{
	"Search":       stage.New("proxy", "Search", "request"),
	"HybridSearch": stage.New("proxy", "HybridSearch", "request"),
	"Query":        stage.New("proxy", "Query", "request"),
}

func startDQL(ctx context.Context, operation string) (context.Context, *dqlTiming) {
	started := time.Now()
	t := &dqlTiming{
		ctx:          ctx,
		operation:    operation,
		path:         stage.PathUnknown,
		started:      started,
		stageStarted: started,
		currentStage: dqlStageReadiness,
		timer:        dqlRecorders[operation].Begin(),
	}
	return context.WithValue(ctx, dqlTimingKey{}, t), t
}

func getDQLTiming(ctx context.Context) *dqlTiming {
	t, _ := ctx.Value(dqlTimingKey{}).(*dqlTiming)
	return t
}

// pathRank orders readiness paths from warmest to coldest. A retried request
// keeps the coldest path it observed, so a later attempt on the loaded
// collection cannot relabel a request that waited for the load.
var pathRank = map[string]int{
	stage.PathUnknown:  0,
	stage.PathDisabled: 1,
	stage.PathReady:    2,
	stage.PathLoading:  3,
	stage.PathUnloaded: 4,
}

// Only the caller sets its observed readiness path. Shared load workers outlive
// callers and must never mutate this request-local state. Unknown values, such
// as an empty path from an older QueryCoord, are ignored to keep labels bounded.
func setDQLPath(ctx context.Context, path string) {
	if t, ok := ctx.Value(dqlTimingKey{}).(*dqlTiming); ok {
		if rank, known := pathRank[path]; known && rank > pathRank[t.path] {
			t.path = path
		}
	}
}

func (t *dqlTiming) Readiness() { t.switchStage(dqlStageReadiness, time.Now()) }
func (t *dqlTiming) Ready()     { t.switchStage(dqlStageExecution, time.Now()) }
func (t *dqlTiming) RetryWait() { t.switchStage(dqlStageRetryWait, time.Now()) }
func (t *dqlTiming) Stop()      { t.stopStage(time.Now()) }

func (t *dqlTiming) switchStage(next dqlStage, now time.Time) {
	if t == nil || t.currentStage == next {
		return
	}
	t.stopStage(now)
	t.currentStage = next
	t.stageStarted = now
	if next == dqlStageExecution {
		t.executionStarted = true
	}
}

func (t *dqlTiming) stopStage(now time.Time) {
	if t == nil || t.currentStage == dqlStageNone {
		return
	}
	t.stageDurations[t.currentStage] += max(0, now.Sub(t.stageStarted))
	t.currentStage = dqlStageNone
}

func (t *dqlTiming) End(status *commonpb.Status, err error) {
	if err == nil {
		err = merr.Error(status)
	}
	t.finish(time.Now(), stage.Outcome(err))
}

func (t *dqlTiming) finish(ended time.Time, result stage.Result) {
	t.stopStage(ended)
	total := ended.Sub(t.started)
	readiness := t.stageDurations[dqlStageReadiness]
	execution := t.stageDurations[dqlStageExecution]
	retryWait := t.stageDurations[dqlStageRetryWait]
	// Attribute the small amount of request-local work outside retryDQL to the
	// phase the request reached, preserving an exact partition for the cohort.
	if remainder := total - readiness - execution - retryWait; remainder > 0 {
		if t.executionStarted {
			execution += remainder
		} else {
			readiness += remainder
		}
	}
	cohort := "le1s"
	if total > time.Second {
		cohort = "gt1s"
	}
	for i, name := range [...]string{"total", "readiness", "execution", "retry_wait"} {
		d := [...]time.Duration{total, readiness, execution, retryWait}[i]
		metrics.QueryRequestStageDuration.WithLabelValues(t.operation, t.path, cohort, name, result.String()).Observe(d.Seconds())
	}
	if cohort == "gt1s" || result != stage.Success {
		metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "slow_summary", "candidate").Inc()
		if mlog.LevelEnabled(mlog.InfoLevel) && dqlSummaryLimiter.Allow() {
			metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "slow_summary", "emitted").Inc()
			mlog.Info(t.ctx, "DQL request stages", mlog.String("operation", t.operation), mlog.String("loadPath", t.path),
				mlog.String("result", result.String()), mlog.Duration("total", total), mlog.Duration("readiness", readiness),
				mlog.Duration("execution", execution), mlog.Duration("retryWait", retryWait))
		} else {
			metrics.QueryStageItems.WithLabelValues("proxy", t.operation, "slow_summary", "dropped").Inc()
		}
	}
	t.timer.EndResult(result)
}

var dqlSummaryLimiter = rate.NewLimiter(2, 10)
