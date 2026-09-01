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
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// Progress reporting for shard splits.
//
// Nothing is reported TO datacoord for this: it owns the tasks, it owns the
// segment meta, and the rewrite results already reach it through the ordinary
// compaction result path. So this is a read, answered where the state already
// lives — the same shape as the import and compaction task listings, served
// through GetMetrics and reachable at /_dc/tasks/shard_split.
//
// The denominator is computed, not remembered. A task records what is left to
// rewrite, never how much there was to begin with, and with good reason: the
// work list is re-derived every round, so it GROWS when an import adds segments
// to a source the split has already fenced. A remembered total would let such a
// task report 100% with work outstanding; deriving both halves from live state
// keeps the fraction honest.

// splitTaskKind names a task's flavor for an operator reading the listing.
//
// The three are one task type with different shapes, and the difference matters
// when reading progress: a relabel moves no data, a doubling rewrites one
// shard, a rehash rewrites the whole collection.
func splitTaskKind(task *datapb.SplitShardTask) string {
	if !rewrites(task) {
		return "namespace"
	}
	if isRehashTask(task) {
		return "rehash"
	}
	return "hash"
}

func unixSecondsString(seconds uint64) string {
	if seconds == 0 {
		return ""
	}
	return time.Unix(int64(seconds), 0).String()
}

// rewriteProgress counts the source segments of a rewriting split that are
// already rewritten and those still outstanding, with their sizes.
//
// Both halves come from live state: the rewritten set from the outputs'
// compaction lineage, the pending set from the task's work list. A source
// segment stays on its source channel until adoption drops it, so both are still
// measurable while the task runs.
func (m *shardSplitManager) rewriteProgress(task *datapb.SplitShardTask) (done, pending metricsinfo.ShardSplitTask) {
	rewritten := m.rewrittenSourceSegments(task)
	for _, segmentID := range rewritten.Collect() {
		done.RewrittenSegments++
		if segment := m.meta.GetSegment(m.ctx, segmentID); segment != nil {
			done.RewrittenBytes += segment.getSegmentSize()
		}
	}
	for _, source := range task.GetSources() {
		for _, segmentID := range source.GetPendingSegments() {
			if rewritten.Contain(segmentID) {
				continue // already counted as done; the task has not retired it yet
			}
			pending.PendingSegments++
			if segment := m.meta.GetSegment(m.ctx, segmentID); segment != nil {
				pending.PendingBytes += segment.getSegmentSize()
			}
		}
	}
	return done, pending
}

// splitTaskStats renders one split task of either kind.
//
// A relabeling split carries no rewrite counters: it moves segment metadata
// rather than data, so there is no redistribution to be partway through and a
// zero-of-zero fraction would read as "stuck".
func (m *shardSplitManager) splitTaskStats(task *datapb.SplitShardTask) metricsinfo.ShardSplitTask {
	targets := make([]string, 0, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		targets = append(targets, target.GetVchannel())
	}
	stats := metricsinfo.ShardSplitTask{
		TaskID:          task.GetTaskId(),
		CollectionID:    task.GetCollectionId(),
		Kind:            splitTaskKind(task),
		State:           task.GetState().String(),
		FailReason:      task.GetFailReason(),
		StartTime:       unixSecondsString(task.GetStartTime()),
		EndTime:         unixSecondsString(task.GetEndTime()),
		SourceVChannels: splitSourceVChannels(task),
		TargetVChannels: targets,
	}
	if rewrites(task) {
		done, pending := m.rewriteProgress(task)
		stats.RewrittenSegments = done.RewrittenSegments
		stats.RewrittenBytes = done.RewrittenBytes
		stats.PendingSegments = pending.PendingSegments
		stats.PendingBytes = pending.PendingBytes
	}
	return stats
}

// TaskStatsJSON renders every split task datacoord currently holds, for the
// /_dc/tasks/shard_split listing.
func (m *shardSplitManager) TaskStatsJSON() string {
	tasks := make([]metricsinfo.ShardSplitTask, 0)
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		tasks = append(tasks, m.splitTaskStats(task))
		return true
	})
	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}
