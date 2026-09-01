// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitTaskStateLabel turns a task state into a short metric label, e.g.
// SplitShardTaskState_SplitShardTaskRedistributing -> "redistributing".
func splitTaskStateLabel(state datapb.SplitShardTaskState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "SplitShardTask"))
}

// refreshMetrics recomputes the per-state gauge of the active split tasks from
// the current task map. It is called at the end of every detection tick so the
// gauge reflects how many tasks sit in each FSM state.
func (m *shardSplitManager) refreshMetrics() {
	counts := map[datapb.SplitShardTaskState]int{
		datapb.SplitShardTaskState_SplitShardTaskPreparing:      0,
		datapb.SplitShardTaskState_SplitShardTaskFencing:        0,
		datapb.SplitShardTaskState_SplitShardTaskRedistributing: 0,
		datapb.SplitShardTaskState_SplitShardTaskAdopting:       0,
	}
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if isSplitShardTaskActive(task) {
			counts[task.GetState()]++
		}
		return true
	})
	for state, count := range counts {
		metrics.DataCoordShardSplitTaskNum.
			WithLabelValues(splitTaskStateLabel(state)).
			Set(float64(count))
	}
	m.refreshRewriteProgressMetrics()
}

// refreshRewriteProgressMetrics publishes how much source data each in-flight
// hash split still has to rewrite, and how much it already has.
//
// Two gauges rather than a percentage: a percentage cannot distinguish a split
// that is finishing from one that stopped, and a rewrite that is progressing
// normally is only visible as a rate. The pair also makes an import into a
// fenced source legible -- pending rises while rewritten holds -- which a single
// ratio would smooth away.
//
// A collection's gauges are cleared when its split reaches a terminal state, so
// a finished split does not leave a stale "still has work" reading behind.
func (m *shardSplitManager) refreshRewriteProgressMetrics() {
	active := typeutil.NewSet[int64]()
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if !isSplitShardTaskActive(task) {
			return true
		}
		collectionID := fmt.Sprint(task.GetCollectionId())
		active.Insert(task.GetCollectionId())
		done, pending := m.rewriteProgress(task)
		metrics.DataCoordShardSplitRewrittenBytes.WithLabelValues(collectionID).Set(float64(done.RewrittenBytes))
		metrics.DataCoordShardSplitPendingBytes.WithLabelValues(collectionID).Set(float64(pending.PendingBytes))
		return true
	})
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if active.Contain(task.GetCollectionId()) {
			return true
		}
		collectionID := fmt.Sprint(task.GetCollectionId())
		metrics.DataCoordShardSplitRewrittenBytes.DeleteLabelValues(collectionID)
		metrics.DataCoordShardSplitPendingBytes.DeleteLabelValues(collectionID)
		return true
	})
}

// recordTerminalMetrics records the outcome counter and the wall-clock duration
// of a split task that just reached a terminal state (done or aborted).
func (m *shardSplitManager) recordTerminalMetrics(task *datapb.SplitShardTask) {
	outcome := splitTaskStateLabel(task.GetState())
	metrics.DataCoordShardSplitTaskTotal.WithLabelValues(outcome).Inc()
	if end, start := task.GetEndTime(), task.GetStartTime(); end >= start {
		// StartTime/EndTime are unix seconds; report the duration in millis to
		// match the shared longTaskBuckets histogram.
		metrics.DataCoordShardSplitDuration.
			WithLabelValues(outcome).
			Observe(float64((end - start) * 1000))
	}
}
