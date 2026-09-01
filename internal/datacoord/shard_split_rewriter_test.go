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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	hashSrcVChannel = "by-dev-rootcoord-dml_0_100v0"
	hashTgtA        = "by-dev-rootcoord-dml_1_100v0"
	hashTgtB        = "by-dev-rootcoord-dml_2_100v0"
)

// fakeRewriteDispatcher records dispatches and lets a test drive plan states.
type fakeRewriteDispatcher struct {
	nextPlanID  int64
	dispatched  map[int64]int64 // segmentID -> planID
	done        typeutil.Set[int64]
	running     typeutil.Set[int64]
	failSegment int64 // dispatch of this segment errors
}

func newFakeRewriteDispatcher() *fakeRewriteDispatcher {
	return &fakeRewriteDispatcher{
		nextPlanID: 5000,
		dispatched: map[int64]int64{},
		done:       typeutil.NewSet[int64](),
		running:    typeutil.NewSet[int64](),
	}
}

func (f *fakeRewriteDispatcher) DispatchHashSplit(task *datapb.SplitShardTask, segmentID int64) (int64, error) {
	if segmentID == f.failSegment {
		return 0, errors.New("dispatch refused")
	}
	if planID, ok := f.dispatched[segmentID]; ok {
		return planID, nil // idempotent per segment
	}
	f.nextPlanID++
	f.dispatched[segmentID] = f.nextPlanID
	f.running.Insert(f.nextPlanID)
	return f.nextPlanID, nil
}

func (f *fakeRewriteDispatcher) HashSplitPlanState(planID int64) (bool, bool) {
	return f.done.Contain(planID), f.running.Contain(planID)
}

func (f *fakeRewriteDispatcher) complete(planID int64) {
	f.running.Remove(planID)
	f.done.Insert(planID)
}

func (f *fakeRewriteDispatcher) lose(planID int64) {
	f.running.Remove(planID)
}

// newHashRewriteMeta builds a meta with sourceSegments on the source vchannel.
func newHashRewriteMeta(sourceSegments []int64) *meta {
	m := &meta{
		ctx:         context.Background(),
		catalog:     &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		channelCPs:  newChannelCps(),
	}
	m.collections.Insert(1, &collectionInfo{
		ID:            1,
		Schema:        &schemapb.CollectionSchema{Name: "hash_split_test"},
		VChannelNames: []string{hashSrcVChannel},
	})
	for _, id := range sourceSegments {
		m.segments.SetSegment(id, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            id,
				CollectionID:  1,
				InsertChannel: hashSrcVChannel,
				State:         commonpb.SegmentState_Flushed,
				NumOfRows:     100,
			},
		})
	}
	return m
}

// addRewriteOutput registers an output segment on a target vchannel that
// records sourceID in its compaction lineage, i.e. a committed rewrite.
func addRewriteOutput(m *meta, outID int64, vchannel string, sourceID int64) {
	m.segments.SetSegment(outID, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             outID,
			CollectionID:   1,
			InsertChannel:  vchannel,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      50,
			CompactionFrom: []int64{sourceID},
		},
	})
}

func newHashSplitTestManager(t *testing.T, m *meta) (*shardSplitManager, *mocks.DataCoordCatalog) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	mgr := &shardSplitManager{
		ctx:     context.Background(),
		meta:    m,
		catalog: catalog,
		// The write switch runs under the collection lock; these tests drive the
		// state machine without a streamingcoord to take it from.
		collectionLocker: func(string, string) (splitWriteSwitchLock, error) {
			return noopWriteSwitchLock{}, nil
		},
		tasks: typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask](),
	}
	return mgr, catalog
}

func newHashTask(pending []int64) *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId:       7,
		CollectionId: 1,
		Sources: []*datapb.SplitShardTaskSource{
			{Vchannel: hashSrcVChannel, PendingSegments: pending},
		},
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced:         true,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: hashTgtA, Buckets: []uint64{0}},
			{Vchannel: hashTgtB, Buckets: []uint64{2}},
		},
		// The source owned residue 0 at modulus 2 and had nothing left to
		// divide, so the split doubled the modulus to 4.
		RoutingModulus: 4,
	}
}

// fenceSources records a fence on every source of the task, the precondition
// commitRouting enforces before it will retire a source.
func fenceSources(task *datapb.SplitShardTask, tick uint64) *datapb.SplitShardTask {
	for _, source := range task.GetSources() {
		source.SwitchTimeTick = tick
	}
	return task
}

// allPendingSegments flattens a task's per-source rewrite work lists, so a
// single-source test can assert on them without naming the source.
func allPendingSegments(task *datapb.SplitShardTask) []int64 {
	var out []int64
	for _, source := range task.GetSources() {
		out = append(out, source.GetPendingSegments()...)
	}
	return out
}

func TestRewriteDispatchesPendingSegments(t *testing.T) {
	m := newHashRewriteMeta([]int64{101, 102, 103})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101, 102, 103})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	res := mgr.advanceRewriting(task, d, 2) // batch of 2
	assert.ElementsMatch(t, []int64{101, 102}, res.dispatched)
	assert.Len(t, d.dispatched, 2)

	// The task records the dispatched plans so a restart resumes them.
	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Len(t, updated.GetDispatchedPlanIds(), 2)
	assert.ElementsMatch(t, []int64{101, 102, 103}, allPendingSegments(updated))
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, updated.GetState())
}

func TestRewriteSkipsCompactingAndImportingSegments(t *testing.T) {
	m := newHashRewriteMeta([]int64{101, 102})
	// 101 is compacting, 102 is importing: both deferred to a later round,
	// exactly as the relabel path defers them.
	seg101 := m.segments.GetSegment(101)
	seg101.isCompacting = true
	m.segments.SetSegment(101, seg101)
	seg102 := m.segments.GetSegment(102)
	seg102.IsImporting = true
	m.segments.SetSegment(102, seg102)

	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101, 102})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	res := mgr.advanceRewriting(task, d, 10)
	assert.Empty(t, res.dispatched)
	assert.Equal(t, 2, res.skipped)
	assert.Empty(t, d.dispatched)
	// Still rewriting: the drain must not pass while work remains.
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestRewriteRetiresSegmentsWithCommittedOutputs(t *testing.T) {
	m := newHashRewriteMeta([]int64{101, 102})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101, 102})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	// Round 1 dispatches both.
	mgr.advanceRewriting(task, d, 10)
	task = mgr.mustGetTask(task.GetTaskId())

	// Segment 101's rewrite commits: outputs appear on both targets.
	addRewriteOutput(m, 901, hashTgtA, 101)
	addRewriteOutput(m, 902, hashTgtB, 101)
	d.complete(d.dispatched[101])

	mgr.advanceRewriting(task, d, 10)
	updated := mgr.mustGetTask(task.GetTaskId())
	// 101 retired, 102 still pending.
	assert.ElementsMatch(t, []int64{102}, allPendingSegments(updated))
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, updated.GetState())
}

func TestRewriteAdvancesToAdoptingWhenDrained(t *testing.T) {
	m := newHashRewriteMeta([]int64{101})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	mgr.advanceRewriting(task, d, 10)
	task = mgr.mustGetTask(task.GetTaskId())

	// The only source segment's rewrite commits.
	addRewriteOutput(m, 901, hashTgtA, 101)
	addRewriteOutput(m, 902, hashTgtB, 101)
	d.complete(d.dispatched[101])

	mgr.advanceRewriting(task, d, 10)
	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, updated.GetState())
	assert.Empty(t, allPendingSegments(updated))
	assert.Empty(t, updated.GetDispatchedPlanIds())
}

func TestRewriteWaitsForFenceFlush(t *testing.T) {
	// T_switch is set but the channel checkpoint has not reached it: the fence
	// sealed segments have not been reported yet, so the drain must not pass
	// even with nothing pending — otherwise those segments would be orphaned on
	// a dropped shard.
	m := newHashRewriteMeta(nil)
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: 50}
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.Sources[0].SwitchTimeTick = 100
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	mgr.advanceRewriting(task, d, 10)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		mgr.mustGetTask(task.GetTaskId()).GetState())

	// Once the checkpoint catches up to T_switch the drain passes.
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: 100}
	mgr.advanceRewriting(mgr.mustGetTask(task.GetTaskId()), d, 10)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestRewriteRedispatchesLostPlan(t *testing.T) {
	m := newHashRewriteMeta([]int64{101})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	mgr.advanceRewriting(task, d, 10)
	firstPlan := d.dispatched[101]
	task = mgr.mustGetTask(task.GetTaskId())
	require.Contains(t, task.GetDispatchedPlanIds(), firstPlan)

	// The plan dies without committing: the segment is still pending, so the
	// next round re-dispatches it. The rewrite is deterministic, so the retry
	// reproduces the same partition.
	d.lose(firstPlan)
	res := mgr.advanceRewriting(task, d, 10)
	assert.Contains(t, res.dispatched, int64(101))
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestRewriteToleratesDispatchFailure(t *testing.T) {
	// A segment that cannot be dispatched must not wedge the task: abort is
	// illegal past the fence, so it is skipped and retried next round.
	m := newHashRewriteMeta([]int64{101, 102})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101, 102})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()
	d.failSegment = 101

	res := mgr.advanceRewriting(task, d, 10)
	assert.Equal(t, 1, res.skipped)
	assert.ElementsMatch(t, []int64{102}, res.dispatched)
	assert.ElementsMatch(t, []int64{101, 102},
		allPendingSegments(mgr.mustGetTask(task.GetTaskId())))
}

func TestRewriteCrashRecoveryRetiresFromMeta(t *testing.T) {
	// A crash between "plan committed" and "task persisted" must still
	// converge: the next round sees the outputs in meta and retires the
	// segment without re-running the rewrite.
	m := newHashRewriteMeta([]int64{101})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{101})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	// Outputs exist in meta, but the task never learned about any plan.
	addRewriteOutput(m, 901, hashTgtA, 101)
	addRewriteOutput(m, 902, hashTgtB, 101)

	mgr.advanceRewriting(task, d, 10)
	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, updated.GetState())
	// It never re-dispatched: the outputs were already there.
	assert.Empty(t, d.dispatched)
}

func TestIsHashSplitTaskActive(t *testing.T) {
	for _, state := range []datapb.SplitShardTaskState{
		datapb.SplitShardTaskState_SplitShardTaskPreparing,
		datapb.SplitShardTaskState_SplitShardTaskFencing,
		datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		datapb.SplitShardTaskState_SplitShardTaskAdopting,
	} {
		task := &datapb.SplitShardTask{
			Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite, State: state,
		}
		assert.True(t, isSplitShardTaskActive(task), state.String())
	}
	for _, state := range []datapb.SplitShardTaskState{
		datapb.SplitShardTaskState_SplitShardTaskDone,
		datapb.SplitShardTaskState_SplitShardTaskAborted,
	} {
		task := &datapb.SplitShardTask{
			Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite, State: state,
		}
		assert.False(t, isSplitShardTaskActive(task), state.String())
	}
}

func TestAbortHashTaskRefusedAfterFence(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	// Not fenced: abort is allowed.
	task := newHashTask(nil)
	task.Fenced = false
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	mgr.tasks.Insert(task.GetTaskId(), task)
	mgr.abortTask(task, "test reason")
	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, updated.GetState())
	assert.Equal(t, "test reason", updated.GetFailReason())

	// Fenced: forward-only, abort refused.
	fenced := newHashTask(nil)
	fenced.TaskId = 8
	fenced.Fenced = true
	mgr.tasks.Insert(fenced.GetTaskId(), fenced)
	mgr.abortTask(fenced, "too late")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		mgr.mustGetTask(fenced.GetTaskId()).GetState())
}

func TestHashSplitTargetVChannels(t *testing.T) {
	task := newHashTask(nil)
	assert.Equal(t, []string{hashTgtA, hashTgtB}, hashSplitTargetVChannels(task.GetTargets()))
}

func TestInitialPendingSegments(t *testing.T) {
	m := newHashRewriteMeta([]int64{101, 102, 103})
	mgr, _ := newHashSplitTestManager(t, m)
	assert.ElementsMatch(t, []int64{101, 102, 103}, mgr.initialPendingSegments(hashSrcVChannel))
	assert.Empty(t, mgr.initialPendingSegments("nonexistent"))
}

func TestRewritePicksUpSegmentsThatArriveAfterSeeding(t *testing.T) {
	// An import is deliberately not stopped by the fence, and the drain waits
	// for it rather than failing it — so its segments land on a source the work
	// list has already been seeded from. They have to join the work list, or
	// adoption retires them (it drops every segment on the source channels) and
	// a split that reported success has deleted the imported rows.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{201})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	// round 1: the seeded segment is dispatched and completes.
	res := mgr.advanceRewriting(task, d, 10)
	require.ElementsMatch(t, []int64{201}, res.dispatched)
	d.complete(d.dispatched[201])
	addRewriteOutput(m, 9201, hashTgtA, 201)

	// an import commits a segment onto the source AFTER the seeding.
	m.segments.SetSegment(202, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 202, CollectionID: 1, InsertChannel: hashSrcVChannel,
		State: commonpb.SegmentState_Flushed, NumOfRows: 50,
	}})

	// round 2: it must be picked up, not silently left behind.
	res = mgr.advanceRewriting(task, d, 10)
	assert.ElementsMatch(t, []int64{202}, res.dispatched,
		"a segment that appeared after seeding must still be rewritten")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		mgr.mustGetTask(task.GetTaskId()).GetState(),
		"the drain must not pass while the new segment is unrewritten")
}

func TestRewriteDoesNotRequeueAnAlreadyRewrittenSegment(t *testing.T) {
	// The re-scan cannot use "is it on the source channel" as the test: a
	// rewritten source segment stays there until adoption. Lineage decides.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{201})
	mgr.tasks.Insert(task.GetTaskId(), task)
	d := newFakeRewriteDispatcher()

	res := mgr.advanceRewriting(task, d, 10)
	require.ElementsMatch(t, []int64{201}, res.dispatched)
	d.complete(d.dispatched[201])
	addRewriteOutput(m, 9201, hashTgtA, 201)

	// 201 is still on the source channel, but its outputs exist.
	require.Len(t, m.GetSegmentsByChannel(hashSrcVChannel), 1)
	res = mgr.advanceRewriting(task, d, 10)
	assert.Empty(t, res.dispatched, "a rewritten segment must not be queued again")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

// noopWriteSwitchLock stands in for the Broadcaster's collection exclusion in
// tests that are not about the exclusion itself.
type noopWriteSwitchLock struct{}

func (noopWriteSwitchLock) Close() {}
