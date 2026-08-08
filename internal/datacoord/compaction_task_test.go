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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// TestCompactionTaskCleanupIsIdempotentAcrossTypes pins the contract every
// compaction task type owes the scheduler: cleanup is idempotent, and once a
// task is cleaned no worker callback may act on it. Ordering against in-flight
// callbacks is the scheduler's job (see TestFinalize*), but a callback that
// arrives after cleanup finished is fenced by the task itself.
func TestCompactionTaskCleanupIsIdempotentAcrossTypes(t *testing.T) {
	const (
		collectionID = int64(10)
		segmentID    = int64(100)
		planID       = int64(1)
		nodeID       = int64(11)
	)

	newTaskProto := func(compactionType datapb.CompactionType) *datapb.CompactionTask {
		return &datapb.CompactionTask{
			PlanID:        planID,
			CollectionID:  collectionID,
			Type:          compactionType,
			State:         datapb.CompactionTaskState_failed,
			NodeID:        nodeID,
			InputSegments: []int64{segmentID},
		}
	}

	cases := []struct {
		name string
		typ  datapb.CompactionType
		make func(*testing.T, *datapb.CompactionTask, CompactionMeta) CompactionTask
	}{
		{
			name: "mix",
			typ:  datapb.CompactionType_MixCompaction,
			make: func(_ *testing.T, proto *datapb.CompactionTask, meta CompactionMeta) CompactionTask {
				return newMixCompactionTask(proto, nil, meta, newMockVersionManager())
			},
		},
		{
			name: "l0",
			typ:  datapb.CompactionType_Level0DeleteCompaction,
			make: func(_ *testing.T, proto *datapb.CompactionTask, meta CompactionMeta) CompactionTask {
				return newL0CompactionTask(proto, nil, meta)
			},
		},
		{
			name: "clustering",
			typ:  datapb.CompactionType_ClusteringCompaction,
			make: func(t *testing.T, proto *datapb.CompactionTask, meta CompactionMeta) CompactionTask {
				return newClusteringCompactionTask(proto, nil, meta, NewNMockHandler(t), nil, newMockVersionManager())
			},
		},
		{
			name: "bumpSchemaVersion",
			typ:  datapb.CompactionType_BumpSchemaVersionCompaction,
			make: func(_ *testing.T, proto *datapb.CompactionTask, meta CompactionMeta) CompactionTask {
				return newBumpSchemaVersionTask(proto, nil, meta, newMockVersionManager())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Each type's doClean does different metastore work (clustering also
			// touches partition stats); this test is about the lifecycle wiring,
			// so let those calls pass while keeping the cluster strict.
			meta := NewMockCompactionMeta(t)
			meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
			meta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
			meta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
			meta.EXPECT().CleanPartitionStatsInfo(mock.Anything, mock.Anything).Return(nil).Maybe()
			meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil).Maybe()

			task := tc.make(t, newTaskProto(tc.typ), meta)

			require.True(t, task.Clean(), "cleanup must succeed against a healthy metastore")
			assert.Equal(t, datapb.CompactionTaskState_cleaned, task.GetTaskProto().GetState())

			// Idempotent: a second round must not re-run cleanup, and must still
			// report success so cleaningTasks drops the task.
			assert.True(t, task.Clean(), "a cleaned task must report success on retry")

			// No cluster expectations are registered, so any RPC issued by a stale
			// callback against a cleaned task fails this test.
			cluster := session.NewMockCluster(t)
			task.CreateTaskOnWorker(nodeID, cluster)
			task.QueryTaskOnWorker(cluster)
			assert.Equal(t, datapb.CompactionTaskState_cleaned, task.GetTaskProto().GetState(),
				"a stale worker callback must not regress a cleaned task")
		})
	}
}

// newOwnershipScheduler returns a MockGlobalScheduler whose ownership handover
// points run their callback inline. Only tests that care about the handover
// itself override these; everywhere else it keeps the scheduler transparent.
func newOwnershipScheduler(t interface {
	mock.TestingT
	Cleanup(func())
},
) *task.MockGlobalScheduler {
	s := task.NewMockGlobalScheduler(t)
	s.EXPECT().TryUpdate(mock.Anything, mock.Anything).
		RunAndReturn(func(_ int64, fn func()) bool {
			fn()
			return true
		}).Maybe()
	s.EXPECT().Finalize(mock.Anything, mock.Anything).
		Run(func(_ int64, fn func()) { fn() }).Return().Maybe()
	return s
}

// A terminal state is the inspector's decision that the task is finished and
// queued for cleanup. A worker callback that fails to probe its node writes
// pipelining back without looking at what it overwrites; that write must be
// dropped, or the scheduler would dispatch a plan for segments cleanup is about
// to release.
func TestCompactionTaskTerminalStateDoesNotRegress(t *testing.T) {
	for _, terminal := range []datapb.CompactionTaskState{
		datapb.CompactionTaskState_completed,
		datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_timeout,
		datapb.CompactionTaskState_cleaned,
	} {
		t.Run(terminal.String(), func(t *testing.T) {
			meta := NewMockCompactionMeta(t)
			// No SaveCompactionTask expectation: a rejected write must not reach
			// the metastore at all.
			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID: 1,
				Type:   datapb.CompactionType_MixCompaction,
				State:  terminal,
				NodeID: 11,
			}, nil, meta, newMockVersionManager())

			require.NoError(t, task.updateAndSaveTaskMeta(
				setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID)))
			assert.Equal(t, terminal, task.GetTaskProto().GetState(),
				"a stale callback must not resurrect a terminal task")
			assert.Equal(t, int64(11), task.GetTaskProto().GetNodeID(),
				"the whole write is dropped, not just the state")
		})
	}
}

func TestCompactionTaskTerminalStateStillAllowsCleanup(t *testing.T) {
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID: 1,
		Type:   datapb.CompactionType_MixCompaction,
		State:  datapb.CompactionTaskState_timeout,
	}, nil, meta, newMockVersionManager())

	require.NoError(t, task.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned)))
	assert.Equal(t, datapb.CompactionTaskState_cleaned, task.GetTaskProto().GetState(),
		"cleanup is the one transition out of a terminal state")
}

// Releasing the input segments must happen exactly once, in doClean. Unlocking
// again on the way to completed could release segments that another compaction
// has legitimately re-acquired in between.
func TestCompactionTaskReleasesInputSegmentsOnlyInCleanup(t *testing.T) {
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()

	unlocks := 0
	meta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).
		Run(func(_ context.Context, _ []int64, _ bool) { unlocks++ }).Return().Maybe()

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_completed,
		InputSegments: []int64{100},
	}, nil, meta, newMockVersionManager())

	require.True(t, task.Process(), "a completed task ends the state machine")
	assert.Equal(t, 0, unlocks, "reaching completed must not release the inputs")

	require.True(t, task.Clean())
	assert.Equal(t, 1, unlocks, "cleanup releases them exactly once")
}

// A callback that reaches a terminal task must stop before it touches anything.
// Clustering persists its result segments and only then records their IDs on the
// task; if the guard sat on the metadata write instead, those segments would be
// on disk with nothing left to reference them once cleanup ran.
func TestCompactionTaskWorkerCallbacksStopAtTerminalState(t *testing.T) {
	for _, terminal := range []datapb.CompactionTaskState{
		datapb.CompactionTaskState_completed,
		datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_timeout,
		datapb.CompactionTaskState_cleaned,
	} {
		t.Run(terminal.String(), func(t *testing.T) {
			// Neither mock has any expectation: an RPC or a metastore write from a
			// callback on a terminal task fails this test.
			meta := NewMockCompactionMeta(t)
			cluster := session.NewMockCluster(t)

			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID: 1,
				Type:   datapb.CompactionType_MixCompaction,
				State:  terminal,
				NodeID: 11,
			}, nil, meta, newMockVersionManager())

			task.CreateTaskOnWorker(11, cluster)
			task.QueryTaskOnWorker(cluster)
			assert.Equal(t, terminal, task.GetTaskProto().GetState())
		})
	}
}
