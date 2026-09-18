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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	hashSrcVChannel = splitMgrV0
	hashTgtA        = splitMgrV1
	hashTgtB        = splitMgrV2
	hashTaskID      = int64(7)
	// hashFenceTick is the T_switch newHashTask records and the checkpoint
	// newHashRewriteMeta seeds. A Redistributing task always has its fence
	// recorded in production -- the ack callback is what moves it out of
	// Fencing -- and nothing is rewritten before the source's checkpoint
	// reaches it, so the default fixture is a source past its fence. A test
	// about the fence itself overrides one of the two.
	hashFenceTick = uint64(100)
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

func (f *fakeRewriteDispatcher) DispatchHashSplit(_ *datapb.SplitShardTask, segmentID int64) (int64, error) {
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

func (f *fakeRewriteDispatcher) HashSplitPlanState(planID int64) (bool, bool, []int64) {
	if f.done.Contain(planID) {
		return true, false, nil
	}
	if f.running.Contain(planID) {
		return false, true, nil
	}
	return false, false, f.segmentsOf(planID)
}

// segmentsOf returns the input segment recorded for planID, as the production
// dispatcher's plan record still would once the plan has left running/done.
// Nil once the test drops the plan entirely (lose).
func (f *fakeRewriteDispatcher) segmentsOf(planID int64) []int64 {
	for segmentID, id := range f.dispatched {
		if id == planID {
			return []int64{segmentID}
		}
	}
	return nil
}

func (f *fakeRewriteDispatcher) complete(planID int64) {
	f.running.Remove(planID)
	f.done.Insert(planID)
}

// lose makes the plan read as neither done nor running, with no known input of
// its own: the plan record is gone.
func (f *fakeRewriteDispatcher) lose(planID int64) {
	f.running.Remove(planID)
	for segmentID, id := range f.dispatched {
		if id == planID {
			delete(f.dispatched, segmentID)
		}
	}
}

// cleanUpAfterCommit makes the plan read as neither done nor running while
// still naming its input, as a plan record does once housekeeping moves a
// completed plan to "cleaned".
func (f *fakeRewriteDispatcher) cleanUpAfterCommit(planID int64) {
	f.done.Remove(planID)
	f.running.Remove(planID)
}

// newHashRewriteMeta builds a meta with flushed L1 sourceSegments on the source
// and the source's checkpoint at its fence.
func newHashRewriteMeta(t *testing.T, sourceSegments []int64) *meta {
	t.Helper()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{ChannelName: hashSrcVChannel, Timestamp: hashFenceTick}
	m.collections.Insert(splitMgrCollection, &collectionInfo{
		ID:            splitMgrCollection,
		Schema:        splitTestSchema(false),
		VChannelNames: []string{hashSrcVChannel},
	})
	for _, id := range sourceSegments {
		setSourceSegment(m, id, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1)
	}
	return m
}

// setSourceSegment puts a segment of the given state and level on the source.
func setSourceSegment(m *meta, id int64, state commonpb.SegmentState, level datapb.SegmentLevel) {
	m.segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: id, CollectionID: splitMgrCollection, PartitionID: 10, InsertChannel: hashSrcVChannel,
		State: state, Level: level, NumOfRows: 10,
	}})
}

// addRewriteOutput registers an output on a target that records sourceID in
// its lineage, and drops the source segment: what a committed rewrite leaves
// in meta (completeHashSplitCompactionMutation).
func addRewriteOutput(m *meta, outID int64, vchannel string, sourceID int64) {
	addRewriteOutputKeepingInput(m, outID, vchannel, sourceID)
	if source := m.segments.GetSegment(sourceID); source != nil && isSegmentHealthy(source) {
		dropped := source.Clone()
		dropped.State = commonpb.SegmentState_Dropped
		dropped.Compacted = true
		m.segments.SetSegment(sourceID, dropped)
	}
}

// addRewriteOutputKeepingInput registers only the output: meta that names a
// source in an output's lineage while the source itself is still live.
func addRewriteOutputKeepingInput(m *meta, outID int64, vchannel string, sourceID int64) {
	m.segments.SetSegment(outID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: outID, CollectionID: splitMgrCollection, PartitionID: 10, InsertChannel: vchannel,
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: 50,
		CompactionFrom: []int64{sourceID},
	}})
}

// rewriteTestCoordinator answers the drain from a datacoord server over the
// test's meta, and everything else from the fake.
type rewriteTestCoordinator struct {
	*fakeSplitCoordinator
	server *Server
}

func (c *rewriteTestCoordinator) splitDrainBlockReason(ctx context.Context, task *datapb.SplitShardTask) string {
	return c.server.splitDrainBlockReason(ctx, task)
}

func (c *rewriteTestCoordinator) fenceFlushBlockReason(task *datapb.SplitShardTask) string {
	return c.server.fenceFlushBlockReason(task)
}

// recordingRewriter runs the real round with a test dispatcher and keeps its
// result.
type recordingRewriter struct {
	manager    *shardSplitManager
	dispatcher rewritePlanDispatcher
	batchSize  int
	rounds     int
	last       rewriteRoundResult
}

func (r *recordingRewriter) redistribute(ctx context.Context, task *datapb.SplitShardTask) {
	r.rounds++
	r.last = r.manager.rewriteRound(ctx, task, r.dispatcher, r.batchSize)
}

// rewriteCase is a split manager over a test meta, driving one Redistributing
// task through its real tick with a fake dispatcher.
type rewriteCase struct {
	t          *testing.T
	meta       *meta
	manager    *shardSplitManager
	dispatcher *fakeRewriteDispatcher
	rewriter   *recordingRewriter
}

func newHashTask(pending []int64) *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId:       hashTaskID,
		CollectionId: splitMgrCollection,
		Sources: []*datapb.SplitShardTaskSource{
			{Vchannel: hashSrcVChannel, PendingSegments: pending, SwitchTimeTick: hashFenceTick},
		},
		State:  datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Fenced: true,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: hashTgtA, Buckets: []uint64{0}},
			{Vchannel: hashTgtB, Buckets: []uint64{2}},
		},
		// The source owned residue 0 at modulus 2 and had nothing left to
		// divide, so the split doubled the modulus to 4.
		RoutingModulus: 4,
	}
}

func newRewriteCase(t *testing.T, m *meta, task *datapb.SplitShardTask) *rewriteCase {
	t.Helper()
	coordinator := &rewriteTestCoordinator{
		fakeSplitCoordinator: &fakeSplitCoordinator{},
		server:               &Server{meta: m},
	}
	manager := newShardSplitManager(context.Background(), m, newMockAllocator(t), newShardSplitTasks(), coordinator)
	manager.controlChannel = func() string { return splitMgrControl }
	require.NoError(t, manager.store.create(context.Background(), manager.catalog, task))
	d := newFakeRewriteDispatcher()
	rewriter := &recordingRewriter{manager: manager, dispatcher: d, batchSize: 10}
	manager.setRedistributor(rewriter)
	return &rewriteCase{t: t, meta: m, manager: manager, dispatcher: d, rewriter: rewriter}
}

// tick runs one manager tick on the task and returns what its rewrite round
// did (nothing when the manager did not call it).
func (c *rewriteCase) tick() rewriteRoundResult {
	c.rewriter.last = rewriteRoundResult{}
	c.manager.advanceTask(c.task())
	return c.rewriter.last
}

// round runs one rewrite round directly, bypassing the manager's own gates.
func (c *rewriteCase) round(batchSize int) rewriteRoundResult {
	return c.manager.rewriteRound(context.Background(), c.task(), c.dispatcher, batchSize)
}

func (c *rewriteCase) task() *datapb.SplitShardTask {
	return mustTask(c.t, c.manager, hashTaskID)
}

func (c *rewriteCase) state() datapb.SplitShardTaskState {
	return c.task().GetState()
}

func (c *rewriteCase) pending() []int64 {
	var out []int64
	for _, source := range c.task().GetSources() {
		out = append(out, source.GetPendingSegments()...)
	}
	return out
}

func (c *rewriteCase) segmentState(id int64) commonpb.SegmentState {
	return c.meta.GetSegment(context.Background(), id).GetState()
}

func TestRewriteDispatchesPendingSegments(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101, 102, 103}), newHashTask([]int64{101, 102, 103}))

	res := c.round(2)
	assert.ElementsMatch(t, []int64{101, 102}, res.dispatched)
	assert.Len(t, c.dispatcher.dispatched, 2)
	// The task records the dispatched plans so a restart resumes them.
	assert.Len(t, c.task().GetDispatchedPlanIds(), 2)
	assert.ElementsMatch(t, []int64{101, 102, 103}, c.pending())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())
}

// The batch bounds the plans a task has in flight, not the plans one round
// dispatches: a plan still running holds its slot in the next round.
func TestRewriteBoundsThePlansInFlight(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101, 102, 103}), newHashTask(nil))

	require.ElementsMatch(t, []int64{101, 102}, c.round(2).dispatched)
	// 101 and 102 are still running: nothing is dispatched past the bound.
	assert.Empty(t, c.round(2).dispatched)
	assert.Len(t, c.task().GetDispatchedPlanIds(), 2)

	// 101 commits and frees its slot.
	addRewriteOutput(c.meta, 901, hashTgtA, 101)
	c.dispatcher.complete(c.dispatcher.dispatched[101])
	assert.ElementsMatch(t, []int64{103}, c.round(2).dispatched)
	assert.Len(t, c.task().GetDispatchedPlanIds(), 2)
}

func TestRewriteSkipsCompactingAndImportingSegments(t *testing.T) {
	m := newHashRewriteMeta(t, []int64{101, 102})
	m.segments.SetIsCompacting(101, true)
	importing := m.segments.GetSegment(102).Clone()
	importing.IsImporting = true
	m.segments.SetSegment(102, importing)
	c := newRewriteCase(t, m, newHashTask([]int64{101, 102}))

	res := c.tick()
	assert.Empty(t, res.dispatched)
	assert.Equal(t, 2, res.skipped)
	assert.Empty(t, c.dispatcher.dispatched)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())
}

func TestRewriteRetiresSegmentsWithCommittedOutputs(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101, 102}), newHashTask([]int64{101, 102}))
	c.tick()

	addRewriteOutput(c.meta, 901, hashTgtA, 101)
	addRewriteOutput(c.meta, 902, hashTgtB, 101)
	c.dispatcher.complete(c.dispatcher.dispatched[101])

	res := c.tick()
	assert.Equal(t, []int64{c.dispatcher.dispatched[101]}, res.completed)
	assert.ElementsMatch(t, []int64{102}, c.pending())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())
}

func TestRewriteAdvancesToAdoptingWhenDrained(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
	c.tick()

	addRewriteOutput(c.meta, 901, hashTgtA, 101)
	addRewriteOutput(c.meta, 902, hashTgtB, 101)
	c.dispatcher.complete(c.dispatcher.dispatched[101])

	c.tick()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
	assert.Empty(t, c.pending())
	assert.Empty(t, c.task().GetDispatchedPlanIds())
}

func TestRewriteWaitsForFenceFlush(t *testing.T) {
	// T_switch is recorded but the checkpoint has not reached it: the segments
	// the fence sealed have not been reported yet, so nothing is rewritten and
	// the drain does not pass even with nothing listed.
	m := newHashRewriteMeta(t, nil)
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick - 50}
	c := newRewriteCase(t, m, newHashTask(nil))

	c.tick()
	assert.Zero(t, c.rewriter.rounds, "the manager runs no round before the fence flushes")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())

	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick}
	c.tick()
	assert.Equal(t, 1, c.rewriter.rounds)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
}

func TestRewriteRedispatchesLostPlan(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
	c.tick()
	firstPlan := c.dispatcher.dispatched[101]
	require.Contains(t, c.task().GetDispatchedPlanIds(), firstPlan)

	// The plan dies without committing. The dead plan's inputs are released by
	// the inspector; the segment is still listed, so the next round dispatches
	// it again, and the rewrite is deterministic.
	c.dispatcher.lose(firstPlan)
	res := c.tick()
	assert.Contains(t, res.dispatched, int64(101))
	assert.NotContains(t, c.task().GetDispatchedPlanIds(), firstPlan)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())
}

func TestRewriteDoesNotRedispatchAPlanCommittedThenCleanedUp(t *testing.T) {
	// The plan commits, then housekeeping moves it out of running/done -- the
	// state a lost plan also reads as. Its input already has outputs, so it is
	// not dispatched again.
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
	c.tick()
	firstPlan := c.dispatcher.dispatched[101]

	addRewriteOutput(c.meta, 901, hashTgtA, 101)
	c.dispatcher.cleanUpAfterCommit(firstPlan)

	res := c.tick()
	assert.NotContains(t, res.dispatched, int64(101))
	assert.Len(t, c.dispatcher.dispatched, 1, "no second plan for the same segment")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
}

func TestRewriteToleratesDispatchFailure(t *testing.T) {
	// Past the fence the task cannot abort: a segment that cannot be dispatched
	// is skipped and retried next round.
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101, 102}), newHashTask([]int64{101, 102}))
	c.dispatcher.failSegment = 101

	res := c.tick()
	assert.Equal(t, 1, res.skipped)
	assert.ElementsMatch(t, []int64{102}, res.dispatched)
	assert.ElementsMatch(t, []int64{101, 102}, c.pending())

	c.dispatcher.failSegment = 0
	assert.ElementsMatch(t, []int64{101}, c.tick().dispatched)
}

// A plan the dispatcher already runs for a segment, but the task record lost
// (a crash between the enqueue and the task write), is watched again rather
// than duplicated.
func TestRewriteWatchesALivePlanTheRecordLost(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
	planID, err := c.dispatcher.DispatchHashSplit(c.task(), 101)
	require.NoError(t, err)

	res := c.tick()
	assert.ElementsMatch(t, []int64{101}, res.dispatched)
	assert.Equal(t, []int64{planID}, c.task().GetDispatchedPlanIds())
	assert.Len(t, c.dispatcher.dispatched, 1)
}

func TestRewriteCrashRecoveryRetiresFromMeta(t *testing.T) {
	// A crash between the commit and the task write still converges: the next
	// round sees the outputs in meta and retires the segment.
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
	addRewriteOutput(c.meta, 901, hashTgtA, 101)
	addRewriteOutput(c.meta, 902, hashTgtB, 101)

	c.tick()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
	assert.Empty(t, c.dispatcher.dispatched)
}

func TestRewriteInputIDs(t *testing.T) {
	m := newHashRewriteMeta(t, []int64{101, 102, 103})
	// Only settled L1 data is rewritten: an L0 carries deletes, not rows, and a
	// growing or flushing segment is still being written.
	setSourceSegment(m, 104, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
	setSourceSegment(m, 105, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1)
	setSourceSegment(m, 106, commonpb.SegmentState_Flushing, datapb.SegmentLevel_L1)
	c := newRewriteCase(t, m, newHashTask(nil))
	assert.ElementsMatch(t, []int64{101, 102, 103}, c.manager.rewriteInputIDs(hashSrcVChannel))
	assert.Empty(t, c.manager.rewriteInputIDs("nonexistent"))
}

func TestRewriteNeverDispatchesL0OrUnflushedSegments(t *testing.T) {
	// The commit drops a rewrite's input. Rewriting an L0 would drop its deletes
	// with it, and rewriting a growing segment would compact data a writer still
	// owns -- so neither ever reaches a plan, whether the re-scan finds it or it
	// was already on the persisted work list.
	newMeta := func(t *testing.T) *meta {
		m := newHashRewriteMeta(t, []int64{101})
		setSourceSegment(m, 102, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
		setSourceSegment(m, 103, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1)
		setSourceSegment(m, 104, commonpb.SegmentState_Sealed, datapb.SegmentLevel_L1)
		return m
	}

	t.Run("found by the re-scan", func(t *testing.T) {
		c := newRewriteCase(t, newMeta(t), newHashTask(nil))
		res := c.tick()
		assert.ElementsMatch(t, []int64{101}, res.dispatched)
		assert.Len(t, c.dispatcher.dispatched, 1)
		assert.ElementsMatch(t, []int64{101}, c.pending())
	})

	t.Run("already on the work list", func(t *testing.T) {
		c := newRewriteCase(t, newMeta(t), newHashTask([]int64{101, 102, 103, 104}))
		res := c.tick()
		assert.ElementsMatch(t, []int64{101}, res.dispatched)
		assert.Len(t, c.dispatcher.dispatched, 1)
		assert.ElementsMatch(t, []int64{101}, c.pending(),
			"what can never be rewritten is not work the task waits on")
	})
}

func TestRewriteAdoptingWaitsForTheSourceToBeEmpty(t *testing.T) {
	// The manager moves the task to Adopting on the adoption gate's own
	// predicate, so a source that still holds a live segment keeps it here.
	t.Run("an input its lineage names is still live", func(t *testing.T) {
		c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
		addRewriteOutputKeepingInput(c.meta, 901, hashTgtA, 101)

		c.tick()
		assert.Empty(t, c.pending(), "nothing left to rewrite")
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state(),
			"caught up is not drained while the source holds a live segment")

		setSourceSegment(c.meta, 101, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1)
		c.tick()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
	})

	t.Run("a growing segment is rewritten once it flushes", func(t *testing.T) {
		m := newHashRewriteMeta(t, []int64{101})
		setSourceSegment(m, 103, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1)
		c := newRewriteCase(t, m, newHashTask([]int64{101}))

		require.ElementsMatch(t, []int64{101}, c.tick().dispatched)
		addRewriteOutput(m, 901, hashTgtA, 101)
		c.dispatcher.complete(c.dispatcher.dispatched[101])

		assert.Empty(t, c.tick().dispatched)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state(),
			"the growing segment is still live on the source")

		setSourceSegment(m, 103, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1)
		assert.ElementsMatch(t, []int64{103}, c.tick().dispatched)
		addRewriteOutput(m, 903, hashTgtB, 103)
		c.dispatcher.complete(c.dispatcher.dispatched[103])

		c.tick()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
	})
}

func TestRewriteZeroOutputPlanDropsItsInputAndAdopts(t *testing.T) {
	// Every row of an input was deleted or expired, so its plan completes with
	// no output and no lineage will ever name it. Through the real commit: the
	// input is Dropped by it, no later round dispatches it again, and the task
	// adopts as soon as the drain holds.
	m := newHashRewriteMeta(t, []int64{101})
	// A growing segment holds the drain false; it is never dispatched.
	setSourceSegment(m, 102, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1)
	c := newRewriteCase(t, m, newHashTask([]int64{101}))

	require.ElementsMatch(t, []int64{101}, c.tick().dispatched)
	planID := c.dispatcher.dispatched[101]
	outputs, _, err := m.CompleteCompactionMutation(context.Background(), &datapb.CompactionTask{
		PlanID:           planID,
		CollectionID:     splitMgrCollection,
		Type:             datapb.CompactionType_HashSplitCompaction,
		Channel:          hashSrcVChannel,
		InputSegments:    []int64{101},
		Schema:           splitTestSchema(false),
		HashSplitTargets: c.task().GetTargets(),
	}, &datapb.CompactionPlanResult{PlanID: planID})
	require.NoError(t, err)
	require.Empty(t, outputs)
	require.Equal(t, commonpb.SegmentState_Dropped, c.segmentState(101), "the zero-output commit drops its input")
	c.dispatcher.complete(planID)

	for round := 0; round < 2; round++ {
		assert.Empty(t, c.tick().dispatched, "round %d dispatched a Dropped input again", round)
		assert.Empty(t, c.pending(), "a Dropped input is not pending")
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())
	}
	assert.Len(t, c.dispatcher.dispatched, 1, "the input was dispatched exactly once")

	setSourceSegment(m, 102, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1)
	c.tick()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
}

func TestRewriteForgetsAPendingSegmentItsCommitDropped(t *testing.T) {
	// An input whose rows were all deleted rewrites into empty halves published
	// Dropped, so no live output names it. Its commit dropped it all the same;
	// a Dropped segment is no longer work.
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask([]int64{101}))
	c.tick()
	c.dispatcher.complete(c.dispatcher.dispatched[101])
	setSourceSegment(c.meta, 101, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1)

	assert.Empty(t, c.tick().dispatched, "a Dropped segment is never dispatched again")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
	assert.Empty(t, c.pending())
}

func TestRewritePicksUpSegmentsThatArriveLater(t *testing.T) {
	// An import is not stopped by the fence, and the drain waits for it, so its
	// segments land on a source already scanned. They join the work list, or
	// the source never empties.
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{201}), newHashTask(nil))
	require.ElementsMatch(t, []int64{201}, c.tick().dispatched)
	c.dispatcher.complete(c.dispatcher.dispatched[201])
	addRewriteOutput(c.meta, 9201, hashTgtA, 201)

	setSourceSegment(c.meta, 202, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1)
	assert.ElementsMatch(t, []int64{202}, c.tick().dispatched)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state(),
		"the drain does not pass while the new segment is not rewritten")
}

func TestRewriteDoesNotRequeueAnAlreadyRewrittenSegment(t *testing.T) {
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{201}), newHashTask([]int64{201}))
	require.ElementsMatch(t, []int64{201}, c.tick().dispatched)
	c.dispatcher.complete(c.dispatcher.dispatched[201])
	addRewriteOutput(c.meta, 9201, hashTgtA, 201)

	require.Empty(t, c.meta.GetSegmentsByChannel(hashSrcVChannel))
	assert.Empty(t, c.tick().dispatched, "a rewritten segment is not queued again")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
}

func TestAllRewritten(t *testing.T) {
	rewritten := typeutil.NewSet[int64](101, 102)
	assert.True(t, allRewritten([]int64{101}, rewritten))
	assert.True(t, allRewritten([]int64{101, 102}, rewritten))
	assert.False(t, allRewritten([]int64{103}, rewritten))
	assert.False(t, allRewritten([]int64{101, 103}, rewritten))
	assert.False(t, allRewritten(nil, rewritten), "nothing to confirm against is never already rewritten")
}

// A plan must cover every L0 that will ever exist on its source. Past its
// T_switch the source's WAL is closed, so its L0 set is final; before that, a
// delete can still flush in after a plan was built. The manager calls no round
// before the fence flushes, and a round holds the same rule itself.
func TestRewriteRoundDispatchesNothingBeforeTheFenceFlushes(t *testing.T) {
	m := newHashRewriteMeta(t, []int64{101, 102})
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick - 1}
	c := newRewriteCase(t, m, newHashTask([]int64{101, 102}))

	assert.Empty(t, c.round(10).dispatched)
	assert.Empty(t, c.dispatcher.dispatched)
	assert.ElementsMatch(t, []int64{101, 102}, c.pending(), "the work list is untouched")

	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick}
	assert.ElementsMatch(t, []int64{101, 102}, c.round(10).dispatched)
}

// A source whose fence is not recorded at all is the same case: the tick to
// catch up to does not exist yet.
func TestRewriteRoundDispatchesNothingBeforeTheFenceIsRecorded(t *testing.T) {
	task := newHashTask([]int64{101})
	task.Sources[0].SwitchTimeTick = 0
	c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), task)
	assert.Empty(t, c.round(10).dispatched)
	assert.Empty(t, c.dispatcher.dispatched)
}

// A round blocked on the fence still records what it learned, so the fence
// flush does not wait for another re-scan and a restart does not lose it.
func TestRewriteBlockedOnTheFenceStillRecordsNewWork(t *testing.T) {
	m := newHashRewriteMeta(t, []int64{101})
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick - 1}
	c := newRewriteCase(t, m, newHashTask(nil))

	assert.Empty(t, c.round(10).dispatched)
	assert.ElementsMatch(t, []int64{101}, c.pending())

	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick}
	assert.ElementsMatch(t, []int64{101}, c.round(10).dispatched)
}

// Nothing left to rewrite while the drain still waits: the round names the
// conjunct, and writes nothing.
func TestRewriteWithNothingLeftWritesNothing(t *testing.T) {
	m := newHashRewriteMeta(t, nil)
	setSourceSegment(m, 103, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1)
	c := newRewriteCase(t, m, newHashTask(nil))
	before := c.task()

	res := c.round(10)
	assert.Empty(t, res.dispatched)
	assert.Same(t, before, c.task(), "an idle round does not rewrite the record")
}

// A round whose task has moved on (the manager adopted it meanwhile) writes
// nothing back, and a failed write keeps what the record had.
func TestRewriteRoundPersistence(t *testing.T) {
	t.Run("a task no longer redistributing is left alone", func(t *testing.T) {
		c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask(nil))
		stale := c.task()
		_, err := c.manager.store.modify(context.Background(), c.manager.catalog, hashTaskID, func(t *datapb.SplitShardTask) bool {
			t.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
			return true
		})
		require.NoError(t, err)

		c.manager.rewriteRound(context.Background(), stale, c.dispatcher, 10)
		assert.Empty(t, c.pending())
		assert.Empty(t, c.task().GetDispatchedPlanIds())
	})

	t.Run("a failed write is retried by the next round", func(t *testing.T) {
		c := newRewriteCase(t, newHashRewriteMeta(t, []int64{101}), newHashTask(nil))
		failing := mockey.Mock((*shardSplitTasks).modify).Return(nil, errors.New("catalog down")).Build()
		res := c.round(10)
		failing.UnPatch()
		assert.ElementsMatch(t, []int64{101}, res.dispatched)
		assert.Empty(t, c.pending())

		// The plan is live, so the next round watches it instead of fanning out.
		c.round(10)
		assert.Len(t, c.dispatcher.dispatched, 1)
		assert.ElementsMatch(t, []int64{101}, c.pending())
	})
}

// The production redistributor scopes the inspector dispatcher to the task and
// reads the in-flight bound from the configuration.
func TestHashSplitRewriterRunsARoundThroughTheInspector(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitRewriteBatchSize.Key, "1")
	defer params.Reset(params.DataCoordCfg.ShardSplitRewriteBatchSize.Key)

	m := newHashRewriteMeta(t, []int64{101, 102})
	c := newRewriteCase(t, m, newHashTask(nil))
	inspector := &fakeInspector{}
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}
	rewriter := newHashSplitRewriter(c.manager, newInspectorRewriteDispatcher(context.Background(), m, inspector, reader, &fakeAllocator{next: 900}))
	c.manager.setRedistributor(rewriter)

	c.manager.advanceTask(c.task())
	require.Len(t, inspector.enqueued, 1, "one plan in flight at most")
	assert.Equal(t, hashTaskID, inspector.enqueued[0].GetTriggerID())
	assert.Equal(t, []int64{101}, inspector.enqueued[0].GetInputSegments())
	assert.Equal(t, []int64{inspector.enqueued[0].GetPlanID()}, c.task().GetDispatchedPlanIds())
}

// L0-2: every plan folds the source's L0s, so they go once nothing on the
// source is left to fold them -- and not before, or the deletes an input still
// has to fold are lost.
func TestRewriteRetiresTheSourceL0sOnceTheInputsAreRewritten(t *testing.T) {
	m := newHashRewriteMeta(t, []int64{101})
	setSourceSegment(m, 102, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
	c := newRewriteCase(t, m, newHashTask([]int64{101}))

	assert.ElementsMatch(t, []int64{101}, c.tick().dispatched, "an L0 is never itself rewritten")
	assert.Equal(t, commonpb.SegmentState_Flushed, c.segmentState(102),
		"the L0 survives while an input still has to fold it")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())

	// 101's rewrite commits. Its plan carried the L0, so its outputs are
	// already delete-applied and the L0 may go.
	addRewriteOutput(m, 901, hashTgtA, 101)
	c.dispatcher.complete(c.dispatcher.dispatched[101])
	c.tick()
	assert.Equal(t, commonpb.SegmentState_Dropped, c.segmentState(102))
	assert.True(t, m.GetSegment(context.Background(), 102).GetCompacted())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state(),
		"and the drain passes in the same tick")
}

// A source may hold L0s and no rewrite input at all. No commit would ever run
// then, so the retire cannot live in the commit; the round does it.
func TestRewriteRetiresTheL0sOfASourceWithNoInput(t *testing.T) {
	m := newHashRewriteMeta(t, nil)
	setSourceSegment(m, 201, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
	setSourceSegment(m, 202, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
	c := newRewriteCase(t, m, newHashTask(nil))

	c.tick()
	for _, id := range []int64{201, 202} {
		segment := m.GetSegment(context.Background(), id)
		assert.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
		assert.True(t, segment.GetCompacted())
		assert.NotZero(t, segment.GetDroppedAt())
	}
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
}

// The retire waits for the fence too: before it, a segment that has not folded
// these L0s can still appear on the source.
func TestRewriteRoundDoesNotRetireL0sBeforeTheFenceFlushes(t *testing.T) {
	m := newHashRewriteMeta(t, nil)
	setSourceSegment(m, 201, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: hashFenceTick - 1}
	c := newRewriteCase(t, m, newHashTask(nil))

	c.round(10)
	assert.Equal(t, commonpb.SegmentState_Flushed, c.segmentState(201))
}

// A failed retire loses nothing: the drain refuses while the L0s live, and the
// next round tries again.
func TestRewriteRetriesAFailedL0Retire(t *testing.T) {
	m := newHashRewriteMeta(t, nil)
	setSourceSegment(m, 201, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
	c := newRewriteCase(t, m, newHashTask(nil))

	failing := mockey.Mock((*meta).RetireLevelZeroSegments).Return(errors.New("catalog down")).Build()
	c.tick()
	failing.UnPatch()
	assert.Equal(t, commonpb.SegmentState_Flushed, c.segmentState(201))
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())

	c.tick()
	assert.Equal(t, commonpb.SegmentState_Dropped, c.segmentState(201))
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
}

// A rewrite input is Flushed and non-L0, so guarding the retire on "no rewrite
// input" alone would let a Sealed, Growing or Flushing segment -- data about to
// become an input, which has folded nothing -- sit on the source while its
// deletes were thrown away. The guard is the drain's own scan.
func TestRewriteHoldsTheL0RetireWhileAnyDataRemains(t *testing.T) {
	for _, state := range []commonpb.SegmentState{
		commonpb.SegmentState_Sealed,
		commonpb.SegmentState_Growing,
		commonpb.SegmentState_Flushing,
	} {
		t.Run(state.String(), func(t *testing.T) {
			m := newHashRewriteMeta(t, nil)
			setSourceSegment(m, 103, state, datapb.SegmentLevel_L1)
			setSourceSegment(m, 201, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0)
			c := newRewriteCase(t, m, newHashTask(nil))

			c.tick()
			assert.Equal(t, commonpb.SegmentState_Flushed, c.segmentState(201),
				"the L0 outlives a segment that has not folded it")
			assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, c.state())

			setSourceSegment(m, 103, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1)
			c.tick()
			assert.Equal(t, commonpb.SegmentState_Dropped, c.segmentState(201))
			assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting, c.state())
		})
	}
}
