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

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
				return newMixCompactionTask(context.TODO(), proto, nil, meta, newMockVersionManager())
			},
		},
		{
			name: "l0",
			typ:  datapb.CompactionType_Level0DeleteCompaction,
			make: func(_ *testing.T, proto *datapb.CompactionTask, meta CompactionMeta) CompactionTask {
				return newL0CompactionTask(context.TODO(), proto, nil, meta)
			},
		},
		{
			name: "clustering",
			typ:  datapb.CompactionType_ClusteringCompaction,
			make: func(t *testing.T, proto *datapb.CompactionTask, meta CompactionMeta) CompactionTask {
				return newClusteringCompactionTask(context.TODO(), proto, nil, meta, NewNMockHandler(t), nil, newMockVersionManager())
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
			assert.Equal(t, datapb.CompactionTaskState_cleaned, task.GetTask().GetState())

			// Idempotent: a second round must not re-run cleanup, and must still
			// report success so cleaningTasks drops the task.
			assert.True(t, task.Clean(), "a cleaned task must report success on retry")

			// No cluster expectations are registered, so any RPC issued by a stale
			// callback against a cleaned task fails this test.
			cluster := session.NewMockCluster(t)
			task.CreateTaskOnWorker(nodeID, cluster)
			task.QueryTaskOnWorker(cluster)
			assert.Equal(t, datapb.CompactionTaskState_cleaned, task.GetTask().GetState(),
				"a stale worker callback must not regress a cleaned task")
		})
	}
}

// The compaction task gauge is keyed by node, and a task's node changes after
// it is admitted -- dispatch is asynchronous, so admission counts it under no
// owner and completion would otherwise decrement it under the worker's ID.
// Every bucket must come back to where it started once a task's lifecycle
// completes, or the unassigned bucket climbs forever while each worker's goes
// negative.
func TestCompactionTaskMetricBucketsBalance(t *testing.T) {
	const worker = int64(11)
	typ := datapb.CompactionType_MixCompaction
	gauge := func(node int64, state string) float64 {
		return testutil.ToFloat64(metrics.DataCoordCompactionTaskNum.
			WithLabelValues(compactionMetricNode(node), typ.String(), state))
	}
	unassignedPending := gauge(NullNodeID, metrics.Pending)
	unassignedExec := gauge(NullNodeID, metrics.Executing)
	workerExec := gauge(worker, metrics.Executing)

	// Admission: counted as pending, then executing, both under no owner --
	// a fresh task carries NodeID 0, which must land in the same bucket as
	// NullNodeID or the decrements below cannot find it.
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(0), typ.String(), metrics.Pending).Inc()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(0), typ.String(), metrics.Pending).Dec()
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(0), typ.String(), metrics.Executing).Inc()
	require.Equal(t, unassignedPending, gauge(NullNodeID, metrics.Pending))

	// Assignment moves the count onto the worker.
	moveExecutingCompactionMetric(typ, 0, worker)
	assert.Equal(t, unassignedExec, gauge(NullNodeID, metrics.Executing),
		"the unassigned bucket must not keep a count that moved to a worker")
	assert.Equal(t, workerExec+1, gauge(worker, metrics.Executing))

	// Completion decrements under the worker, which is where the count now is.
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(worker), typ.String(), metrics.Executing).Dec()
	assert.Equal(t, workerExec, gauge(worker, metrics.Executing))
	assert.Equal(t, unassignedExec, gauge(NullNodeID, metrics.Executing))

	// A task that never reaches a worker is decremented where it was counted:
	// NodeID stays 0, which normalizes to the same bucket the increment used.
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(0), typ.String(), metrics.Executing).Inc()
	moveExecutingCompactionMetric(typ, 0, 0) // no assignment ever happened
	metrics.DataCoordCompactionTaskNum.WithLabelValues(compactionMetricNode(0), typ.String(), metrics.Executing).Dec()
	assert.Equal(t, unassignedExec, gauge(NullNodeID, metrics.Executing))
}

// Which state an ended attempt lands in IS the decision about what happens
// next, and setAttemptEnded makes it once, from the record. Deciding here
// rather than re-deriving later is what keeps the answer stable when
// dataCoord.compaction.maxAttempts is changed at runtime.
func TestSetAttemptEndedRecordsTheRetryDecision(t *testing.T) {
	maxAttempts := int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt())
	require.Greater(t, maxAttempts, int32(1), "the cap must leave room for a retry")

	owed := &datapb.CompactionTask{RetryTimes: 0}
	setAttemptEnded()(owed)
	assert.Equal(t, datapb.CompactionTaskState_retrying, owed.GetState(),
		"a rebuild is still allowed, so the work is owed one")
	assert.True(t, isRetrying(owed))

	spent := &datapb.CompactionTask{RetryTimes: maxAttempts - 1}
	setAttemptEnded()(spent)
	assert.Equal(t, datapb.CompactionTaskState_failed, spent.GetState(),
		"the last attempt allowed is the trigger's outcome")
	assert.False(t, isRetrying(spent))

	// Both are terminal for the attempt: no worker callback may act on either.
	assert.True(t, isTerminalState(owed.GetState()))
	assert.True(t, isTerminalState(spent.GetState()))
	// And both still owe cleanup their input segments.
	assert.True(t, needsCleanup(owed.GetState()))
	assert.True(t, needsCleanup(spent.GetState()))
}

// One predicate answers "is a rebuild owed?", for both cleanup and the trigger
// summary, and it answers from the state alone. Deriving it from anything else
// -- RetryTimes against a refreshable cap, say -- would let the two callers see
// different answers across a configuration change, and could strand a record
// cleanup is already waiting to rebuild.
func TestCompactionTaskOwedRebuild(t *testing.T) {
	cap := int32(Params.DataCoordCfg.CompactionMaxAttempts.GetAsInt())
	require.Greater(t, cap, int32(1))

	// retrying carries the decision setAttemptEnded made and is honored as
	// written, whatever RetryTimes says -- the cap may have been lowered since.
	assert.True(t, isRetrying(&datapb.CompactionTask{
		State: datapb.CompactionTaskState_retrying, RetryTimes: 0,
	}))
	assert.True(t, isRetrying(&datapb.CompactionTask{
		State: datapb.CompactionTaskState_retrying, RetryTimes: cap + 5,
	}), "a decision already recorded is not revisited")

	// Nothing writes timeout any more -- every worker-reported timeout goes
	// through setAttemptEnded -- so a record carrying it predates this state and
	// is settled. Cleanup still releases its inputs; the trigger picks the work
	// up again. Reviving it here would put the cap back in a read path.
	for _, retryTimes := range []int32{0, cap} {
		assert.False(t, isRetrying(&datapb.CompactionTask{
			State: datapb.CompactionTaskState_timeout, RetryTimes: retryTimes,
		}), "a legacy timeout is settled regardless of RetryTimes")
	}

	for _, settled := range []datapb.CompactionTaskState{
		datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_completed,
		datapb.CompactionTaskState_cleaned,
		datapb.CompactionTaskState_executing,
	} {
		assert.False(t, isRetrying(&datapb.CompactionTask{State: settled}), settled.String())
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
	s.EXPECT().Update(mock.Anything, mock.Anything).
		Run(func(_ int64, fn func()) { fn() }).Return().Maybe()
	s.EXPECT().Finalize(mock.Anything, mock.Anything).
		Run(func(_ int64, fn func()) { fn() }).Return().Maybe()
	// Cleanup releases a clustering attempt's analyze job through the same
	// scheduler; tests that assert on that abort register their own expectation.
	s.EXPECT().AbortAndRemoveTask(mock.Anything).Return().Maybe()
	return s
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

	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
// Even though clustering durably records output IDs before publishing segment
// metadata, a callback after cleanup could create output after those IDs had
// already been classified and the inputs handed to a replacement attempt.
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

			task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
				PlanID: 1,
				Type:   datapb.CompactionType_MixCompaction,
				State:  terminal,
				NodeID: 11,
			}, nil, meta, newMockVersionManager())

			task.CreateTaskOnWorker(11, cluster)
			task.QueryTaskOnWorker(cluster)
			assert.Equal(t, terminal, task.GetTask().GetState())
		})
	}
}

// One rule for every RPC round, the same one the create path uses: a round
// that ends without an answer ends the attempt. The query already spent
// dataCoord.requestTimeoutSeconds on an operation a healthy worker answers in
// microseconds; abandoning is cheap by design -- the replan cannot collide
// with whatever the worker is still doing -- and maxAttempts bounds the churn.
func TestMixQueryAbandonsOnUnansweredQuery(t *testing.T) {
	newTask := func(t *testing.T) *mixCompactionTask {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		return newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
			PlanID: 1,
			Type:   datapb.CompactionType_MixCompaction,
			State:  datapb.CompactionTaskState_executing,
			NodeID: 11,
		}, nil, meta, newMockVersionManager())
	}

	t.Run("transport error", func(t *testing.T) {
		task := newTask(t)
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryCompaction(int64(11), mock.Anything).
			Return(nil, errors.New("context deadline exceeded")).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.CompactionTaskState_retrying, task.GetTask().GetState())
		assert.True(t, isRetrying(task.GetTask()), "the work must be rebuilt, not re-dispatched")
	})

	t.Run("nil result", func(t *testing.T) {
		// A nil result cannot come from the current DataNode, which always
		// answers with a result for the queried plan; it is no answer either.
		task := newTask(t)
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryCompaction(int64(11), mock.Anything).Return(nil, nil).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.CompactionTaskState_retrying, task.GetTask().GetState())
	})
}

func TestMixCreateReplansOnUnknownOutcome(t *testing.T) {
	newTask := func(t *testing.T, meta CompactionMeta) *mixCompactionTask {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Maybe()
		task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
			PlanID:        1,
			Type:          datapb.CompactionType_MixCompaction,
			State:         datapb.CompactionTaskState_pipelining,
			NodeID:        11,
			InputSegments: []int64{100},
			Schema:        &schemapb.CollectionSchema{},
		}, alloc, meta, newMockVersionManager())
		return task
	}
	healthySegment := func(meta *MockCompactionMeta) {
		meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *SegmentInfo {
				return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID: segID, State: commonpb.SegmentState_Flushed,
				}}
			}).Maybe()
	}

	t.Run("transport error abandons the attempt for a replan", func(t *testing.T) {
		// The response was lost after the worker may have enqueued the plan.
		// Sending this plan anywhere else could execute it twice against the
		// artifacts named after it, so the attempt is given up and the inspector
		// rebuilds the work under a fresh planID and a fresh output range.
		meta := NewMockCompactionMeta(t)
		healthySegment(meta)
		// Two saves: the assignment before the RPC, then the abandoned state.
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Twice()
		task := newTask(t, meta)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateCompaction(int64(11), mock.Anything, mock.Anything).
			Return(errors.New("context deadline exceeded")).Once()
		task.CreateTaskOnWorker(11, cluster)
		assert.Equal(t, datapb.CompactionTaskState_retrying, task.GetTask().GetState())
		assert.True(t, isRetrying(task.GetTask()), "the work must be rebuilt, not re-dispatched")
	})

	t.Run("worker refusal also abandons the attempt for a replan", func(t *testing.T) {
		// A refusal the worker actually sent does prove the plan is not running,
		// so reusing this planID and re-dispatching it elsewhere would be safe.
		// But that safety only holds if the refusal is classified correctly, and
		// getting it wrong hands a live plan to a second node. A fresh planID
		// removes the need to classify at all: this attempt is abandoned exactly
		// as it would be for an outcome we cannot read.
		meta := NewMockCompactionMeta(t)
		healthySegment(meta)
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Twice()
		task := newTask(t, meta)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateCompaction(int64(11), mock.Anything, mock.Anything).
			Return(merr.WrapErrServiceInternalMsg("no slot")).Once()
		task.CreateTaskOnWorker(11, cluster)
		assert.Equal(t, datapb.CompactionTaskState_retrying, task.GetTask().GetState())
		assert.True(t, isRetrying(task.GetTask()), "the work must be rebuilt, not re-dispatched")
	})

	t.Run("duplicated task keeps the assignment", func(t *testing.T) {
		// The RPC layer retried a request whose response was lost, and the
		// worker answered that it already has the plan. That proves the first
		// attempt was accepted, so the assignment must stay on this node --
		// re-dispatching would run the plan on two nodes with the same
		// pre-allocated segment and binlog IDs.
		meta := NewMockCompactionMeta(t)
		healthySegment(meta)
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		task := newTask(t, meta)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateCompaction(int64(11), mock.Anything, mock.Anything).
			Return(merr.WrapErrDuplicatedCompactionTask()).Once()
		task.CreateTaskOnWorker(11, cluster)
		assert.Equal(t, datapb.CompactionTaskState_executing, task.GetTask().GetState())
		assert.EqualValues(t, 11, task.GetTask().GetNodeID())
	})

	t.Run("unanswered query abandons the attempt", func(t *testing.T) {
		// A worker that keeps its session alive while black-holing RPCs never
		// converges to ErrNodeNotFound. Waiting more rounds would hold the
		// inputs -- isCompacting, and for sort still invisible -- with no
		// convergence in sight, so the first unanswered round ends the
		// attempt, exactly as it does on the create path.
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		alloc := allocator.NewMockAllocator(t)
		task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
			PlanID:        1,
			Type:          datapb.CompactionType_MixCompaction,
			State:         datapb.CompactionTaskState_executing,
			NodeID:        11,
			InputSegments: []int64{100},
		}, alloc, meta, newMockVersionManager())

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryCompaction(int64(11), mock.Anything).
			Return(nil, errors.New("connection reset")).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.CompactionTaskState_retrying, task.GetTask().GetState(),
			"an unanswered worker must not hold the task")
		assert.True(t, isRetrying(task.GetTask()),
			"the plan is never handed to another node; the work is rebuilt with new IDs")
	})

	t.Run("assignment save failure does not send the plan", func(t *testing.T) {
		// The RPC must not go out if the assignment could not be persisted:
		// the task would stay pipelining in memory while a worker ran the plan,
		// and the scheduler would hand the same output segment IDs to a second
		// node. No CreateCompaction expectation -- the mock fails the test if
		// the plan is sent.
		meta := NewMockCompactionMeta(t)
		healthySegment(meta)
		meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
			Return(errors.New("etcd unavailable")).Once()
		task := newTask(t, meta)

		task.CreateTaskOnWorker(11, session.NewMockCluster(t))
		assert.Equal(t, datapb.CompactionTaskState_pipelining, task.GetTask().GetState())
	})
}
