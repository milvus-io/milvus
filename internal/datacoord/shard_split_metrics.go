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
	"encoding/json"
	"strings"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// splitTaskStateLabel turns a task state into a short metric label, e.g.
// SplitShardTaskRedistributing -> "redistributing".
func splitTaskStateLabel(state datapb.SplitShardTaskState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "SplitShardTask"))
}

// refreshMetrics recomputes the per-state gauge of the tasks that are not
// terminal.
func (m *shardSplitManager) refreshMetrics() {
	counts := map[datapb.SplitShardTaskState]int{
		datapb.SplitShardTaskState_SplitShardTaskPreparing:      0,
		datapb.SplitShardTaskState_SplitShardTaskFencing:        0,
		datapb.SplitShardTaskState_SplitShardTaskRedistributing: 0,
		datapb.SplitShardTaskState_SplitShardTaskAdopting:       0,
	}
	for _, task := range m.store.list() {
		if isSplitShardTaskActive(task) {
			counts[task.GetState()]++
		}
	}
	for state, count := range counts {
		metrics.DataCoordShardSplitTaskNum.WithLabelValues(splitTaskStateLabel(state)).Set(float64(count))
	}
}

// recordTerminalMetrics records the outcome and the duration of a task that
// has just reached a terminal state.
func (m *shardSplitManager) recordTerminalMetrics(task *datapb.SplitShardTask) {
	outcome := splitTaskStateLabel(task.GetState())
	metrics.DataCoordShardSplitTaskTotal.WithLabelValues(outcome).Inc()
	if end, start := task.GetEndTime(), task.GetStartTime(); start != 0 && end >= start {
		// Unix seconds; reported in milliseconds like every long task.
		metrics.DataCoordShardSplitDuration.WithLabelValues(outcome).Observe(float64((end - start) * 1000))
	}
}

func unixSecondsString(seconds uint64) string {
	if seconds == 0 {
		return ""
	}
	return time.Unix(int64(seconds), 0).String()
}

// splitTaskStats renders one task for the shard_split_tasks listing.
func splitTaskStats(task *datapb.SplitShardTask) metricsinfo.ShardSplitTask {
	stats := metricsinfo.ShardSplitTask{
		TaskID:          task.GetTaskId(),
		CollectionID:    task.GetCollectionId(),
		State:           task.GetState().String(),
		Fenced:          task.GetFenced(),
		FailReason:      task.GetFailReason(),
		StartTime:       unixSecondsString(task.GetStartTime()),
		EndTime:         unixSecondsString(task.GetEndTime()),
		SourceVChannel:  splitTaskSource(task),
		TargetVChannels: splitTaskTargetVChannels(task),
		RoutingModulus:  task.GetRoutingModulus(),
	}
	for _, source := range task.GetSources() {
		stats.SwitchTimeTick = source.GetSwitchTimeTick()
		stats.PendingSegments += int64(len(source.GetPendingSegments()))
	}
	return stats
}

// TaskStatsJSON renders every split task datacoord records, terminal ones
// included, for the shard_split_tasks metrics request.
func (m *shardSplitManager) TaskStatsJSON() string {
	tasks := make([]metricsinfo.ShardSplitTask, 0)
	for _, task := range m.store.list() {
		tasks = append(tasks, splitTaskStats(task))
	}
	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}
