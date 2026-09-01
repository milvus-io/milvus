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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

func decodeSplitTasks(t *testing.T, raw string) []metricsinfo.ShardSplitTask {
	t.Helper()
	var tasks []metricsinfo.ShardSplitTask
	require.NoError(t, json.Unmarshal([]byte(raw), &tasks))
	return tasks
}

func TestShardSplitTaskStatsReportsProgress(t *testing.T) {
	m := newHashRewriteMeta([]int64{201, 202})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{201, 202})
	mgr.tasks.Insert(task.GetTaskId(), task)

	// nothing rewritten yet: all the work is outstanding.
	tasks := decodeSplitTasks(t, mgr.TaskStatsJSON())
	require.Len(t, tasks, 1)
	assert.Equal(t, int64(2), tasks[0].PendingSegments)
	assert.Zero(t, tasks[0].RewrittenSegments)
	assert.Equal(t, "hash", tasks[0].Kind)
	assert.Equal(t, []string{hashSrcVChannel}, tasks[0].SourceVChannels)
	assert.ElementsMatch(t, []string{hashTgtA, hashTgtB}, tasks[0].TargetVChannels)

	// one rewritten: the split moves, and the denominator does not.
	addRewriteOutput(m, 9201, hashTgtA, 201)
	tasks = decodeSplitTasks(t, mgr.TaskStatsJSON())
	require.Len(t, tasks, 1)
	assert.Equal(t, int64(1), tasks[0].RewrittenSegments)
	assert.Equal(t, int64(1), tasks[0].PendingSegments,
		"a segment counted as rewritten must not also be counted as pending")
}

func TestShardSplitProgressDenominatorGrowsWithLateArrivals(t *testing.T) {
	// The reason progress is derived rather than remembered: an import is not
	// stopped by the fence, so a source can gain segments after the work list
	// was seeded. A total recorded at the start would let this task report
	// finished with work outstanding.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{201})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	mgr.advanceRewriting(task, d, 10)
	d.complete(d.dispatched[201])
	addRewriteOutput(m, 9201, hashTgtA, 201)

	// an import commits a segment onto the fenced source.
	m.segments.SetSegment(202, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 202, CollectionID: 1, InsertChannel: hashSrcVChannel,
		State: commonpb.SegmentState_Flushed, NumOfRows: 50,
	}})
	mgr.advanceRewriting(task, d, 10)

	tasks := decodeSplitTasks(t, mgr.TaskStatsJSON())
	require.Len(t, tasks, 1)
	assert.Equal(t, int64(1), tasks[0].RewrittenSegments)
	assert.Equal(t, int64(1), tasks[0].PendingSegments,
		"the late arrival must show as outstanding work, not as a finished split")
}

func TestShardSplitTaskStatsRendersBothKinds(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.tasks.Insert(7, newHashTask(nil))
	// a relabeling (namespace) split, the other kind the listing must render.
	mgr.tasks.Insert(8, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRelabel,
		TaskId:         8, CollectionId: 1, Sources: []*datapb.SplitShardTaskSource{{Vchannel: "ns-v0"}},
		State:   datapb.SplitShardTaskState_SplitShardTaskAdopting,
		Targets: []*datapb.SplitShardTaskTarget{{Vchannel: "ns-v1"}, {Vchannel: "ns-v2"}},
	})

	tasks := decodeSplitTasks(t, mgr.TaskStatsJSON())
	require.Len(t, tasks, 2)
	kinds := map[string]metricsinfo.ShardSplitTask{}
	for _, task := range tasks {
		kinds[task.Kind] = task
	}
	require.Contains(t, kinds, "namespace")
	require.Contains(t, kinds, "hash")
	// A namespace split relabels metadata; it has no rewrite to be partway
	// through, and reporting a zero-of-zero fraction would read as "stuck".
	assert.Zero(t, kinds["namespace"].PendingSegments)
	assert.Zero(t, kinds["namespace"].RewrittenSegments)
	assert.Equal(t, []string{"ns-v0"}, kinds["namespace"].SourceVChannels)
}

func TestShardSplitTaskStatsNamesARehash(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.Sources = append(task.Sources, &datapb.SplitShardTaskSource{Vchannel: "second-src"})
	mgr.tasks.Insert(task.GetTaskId(), task)

	tasks := decodeSplitTasks(t, mgr.TaskStatsJSON())
	require.Len(t, tasks, 1)
	assert.Equal(t, "rehash", tasks[0].Kind,
		"a task with more than one source rewrites the whole collection, not one shard")
}
