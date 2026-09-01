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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAdvanceHashPreparingSeedsWorkList(t *testing.T) {
	m := newHashRewriteMeta([]int64{201, 202, 203})
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.vchannelAllocator = &fakeVChannelAllocator{vchannels: []string{hashTgtA, hashTgtB}}
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	task.Fenced = false
	// The trigger leaves the vchannels empty; Preparing fills them in.
	task.Targets = []*datapb.SplitShardTaskTarget{
		{Buckets: []uint64{0}}, {Buckets: []uint64{2}},
	}
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, updated.GetState())
	// Every source segment is queued for the rewrite.
	assert.ElementsMatch(t, []int64{201, 202, 203}, allPendingSegments(updated))
	// The allocated vchannels are stamped onto the targets, whose predicates
	// the trigger already decided.
	assert.Equal(t, hashTgtA, updated.GetTargets()[0].GetVchannel())
	assert.Equal(t, hashTgtB, updated.GetTargets()[1].GetVchannel())
	assert.Equal(t, []uint64{0}, updated.GetTargets()[0].GetBuckets())
	assert.EqualValues(t, 4, updated.GetRoutingModulus())
}

func TestAdvanceHashPreparingWaitsWithoutAllocator(t *testing.T) {
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	task.Fenced = false
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestAdvanceHashPreparingAbortsOnMissingCollection(t *testing.T) {
	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		channelCPs:  newChannelCps(),
	}
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	task.Fenced = false
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestAdvanceHashPreparingAbortsOnWrongTargetCount(t *testing.T) {
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	task.Fenced = false
	task.Targets = task.Targets[:1] // only one target
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestAdvanceHashRewritingStallsWithoutDispatcher(t *testing.T) {
	// No compaction inspector wired: the round must be a no-op, not a panic,
	// and must leave the task in Rewriting so it resumes once wired.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask([]int64{201})
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestAdvanceHashAdoptingCompletesTask(t *testing.T) {
	m := newHashRewriteMeta(nil)
	coll, _ := m.collections.Get(1)
	coll.VChannelNames = []string{hashSrcVChannel}
	coll.DatabaseName = "db"
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.router = &captureRouter{}
	task := fenceSources(newHashTask(nil), 100)
	task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, updated.GetState())
	assert.NotZero(t, updated.GetEndTime()) // starts the reaping clock
}

func TestAdvanceHashAdoptingWaitsWithoutRouter(t *testing.T) {
	// The routing committer is wired during server initialization; a task that
	// ticks before that must wait for the next tick, not panic and not complete
	// without committing the routing.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestReapTerminalHashTask(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, catalog := newHashSplitTestManager(t, m)
	catalog.EXPECT().DropSplitShardTask(mock.Anything, mock.Anything).Return(nil).Maybe()

	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskDone
	task.EndTime = uint64(time.Now().Add(-2 * time.Hour).Unix())
	mgr.tasks.Insert(task.GetTaskId(), task)

	// Still inside the retention window: kept.
	mgr.reapTerminalTask(task, 4*time.Hour)
	_, ok := mgr.tasks.Get(task.GetTaskId())
	assert.True(t, ok)

	// Past the retention window: dropped from meta and cache.
	mgr.reapTerminalTask(task, time.Minute)
	_, ok = mgr.tasks.Get(task.GetTaskId())
	assert.False(t, ok)
}

func TestReapTerminalHashTaskSkipsUnstampedTask(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskDone
	task.EndTime = 0 // never stamped
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.reapTerminalTask(task, time.Nanosecond)
	_, ok := mgr.tasks.Get(task.GetTaskId())
	assert.True(t, ok, "a task without an end time must not be reaped")
}

// --- dispatcher ---

type fakePlanReader struct {
	byTrigger map[int64][]*datapb.CompactionTask
}

func (f *fakePlanReader) GetCompactionTasksByTriggerID(_ context.Context, triggerID int64) []*datapb.CompactionTask {
	return f.byTrigger[triggerID]
}

type fakeInspector struct {
	enqueued []*datapb.CompactionTask
	err      error
}

func (f *fakeInspector) enqueueCompaction(task *datapb.CompactionTask) error {
	if f.err != nil {
		return f.err
	}
	f.enqueued = append(f.enqueued, task)
	return nil
}

type fakeAllocator struct{ next int64 }

func (a *fakeAllocator) AllocTimestamp(context.Context) (uint64, error) {
	a.next++
	return uint64(a.next), nil
}
func (a *fakeAllocator) AllocID(context.Context) (int64, error) { a.next++; return a.next, nil }
func (a *fakeAllocator) AllocN(n int64) (int64, int64, error) {
	begin := a.next + 1
	a.next += n
	return begin, a.next + 1, nil
}

func TestDispatcherBuildsHashSplitPlan(t *testing.T) {
	m := newHashRewriteMeta([]int64{301})
	insp := &fakeInspector{}
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}
	d := newInspectorRewriteDispatcher(context.Background(), m, insp, reader, &fakeAllocator{next: 900})

	task := newHashTask([]int64{301})
	planID, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.NotZero(t, planID)
	require.Len(t, insp.enqueued, 1)

	plan := insp.enqueued[0]
	assert.Equal(t, datapb.CompactionType_HashSplitCompaction, plan.GetType())
	// The plan runs on the source channel (where its input lives)...
	assert.Equal(t, hashSrcVChannel, plan.GetChannel())
	assert.Equal(t, []int64{301}, plan.GetInputSegments())
	// ...and carries the targets, which tell the datanode how to partition rows
	// and which vchannel each output belongs to.
	require.Len(t, plan.GetHashSplitTargets(), 2)
	assert.Equal(t, hashTgtA, plan.GetHashSplitTargets()[0].GetVchannel())
	assert.Equal(t, hashTgtB, plan.GetHashSplitTargets()[1].GetVchannel())
	// The split task id is the trigger id, so the task's plans are findable.
	assert.Equal(t, task.GetTaskId(), plan.GetTriggerID())
	// Two output ids pre-allocated, one per target.
	assert.NotNil(t, plan.GetPreAllocatedSegmentIDs())
}

func TestDispatcherIsIdempotentPerSegment(t *testing.T) {
	m := newHashRewriteMeta([]int64{301})
	insp := &fakeInspector{}
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}
	d := newInspectorRewriteDispatcher(context.Background(), m, insp, reader, &fakeAllocator{next: 900})
	task := newHashTask([]int64{301})

	planID, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)

	// The enqueued plan is now visible to the reader (as it would be in meta).
	reader.byTrigger[task.GetTaskId()] = []*datapb.CompactionTask{{
		PlanID:        planID,
		TriggerID:     task.GetTaskId(),
		Type:          datapb.CompactionType_HashSplitCompaction,
		InputSegments: []int64{301},
		State:         datapb.CompactionTaskState_executing,
	}}

	// A repeated dispatch must reuse it, not fan out a second rewrite of the
	// same segment.
	again, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.Equal(t, planID, again)
	assert.Len(t, insp.enqueued, 1)
}

func TestDispatcherErrorsOnMissingSegment(t *testing.T) {
	m := newHashRewriteMeta(nil)
	d := newInspectorRewriteDispatcher(context.Background(), m, &fakeInspector{},
		&fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}, &fakeAllocator{})
	_, err := d.DispatchHashSplit(newHashTask(nil), 999)
	require.Error(t, err)
}

func TestHashSplitPlanStateMapping(t *testing.T) {
	cases := []struct {
		state   datapb.CompactionTaskState
		done    bool
		running bool
	}{
		{datapb.CompactionTaskState_completed, true, false},
		{datapb.CompactionTaskState_pipelining, false, true},
		{datapb.CompactionTaskState_executing, false, true},
		{datapb.CompactionTaskState_meta_saved, false, true},
		{datapb.CompactionTaskState_failed, false, false},
		{datapb.CompactionTaskState_timeout, false, false},
		{datapb.CompactionTaskState_cleaned, false, false},
	}
	for _, tc := range cases {
		done, running := hashSplitPlanTerminalState(tc.state)
		assert.Equal(t, tc.done, done, tc.state.String())
		assert.Equal(t, tc.running, running, tc.state.String())
	}
}

func TestDispatcherPlanStateLookup(t *testing.T) {
	m := newHashRewriteMeta([]int64{301})
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{
		7: {{PlanID: 555, TriggerID: 7, State: datapb.CompactionTaskState_completed}},
	}}
	d := newInspectorRewriteDispatcher(context.Background(), m, &fakeInspector{}, reader, &fakeAllocator{})
	scoped := d.forTask(7)

	done, running := scoped.HashSplitPlanState(555)
	assert.True(t, done)
	assert.False(t, running)

	// A plan absent from meta reads as neither: the round re-dispatches it.
	done, running = scoped.HashSplitPlanState(999)
	assert.False(t, done)
	assert.False(t, running)
}

func TestAdvanceHashAdoptingRetiresTheSourceSegments(t *testing.T) {
	// The rewrite leaves its inputs alive on purpose: the source delegator
	// serves the whole key space from them for the entire fronting window. Once
	// adoption hands the routing to the targets those rows exist in two places,
	// and the source's copy is what a read must never reach again — leaving it
	// behind is how a finished split started returning every primary key twice.
	m := newHashRewriteMeta([]int64{201, 202})
	coll, _ := m.collections.Get(1)
	coll.VChannelNames = []string{hashSrcVChannel}
	coll.DatabaseName = "db"
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.router = &captureRouter{}
	task := fenceSources(newHashTask(nil), 100)
	task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
	mgr.tasks.Insert(task.GetTaskId(), task)
	require.Len(t, m.GetSegmentsByChannel(hashSrcVChannel), 2)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone,
		mgr.mustGetTask(task.GetTaskId()).GetState())
	assert.Empty(t, m.GetSegmentsByChannel(hashSrcVChannel),
		"the handed-off source must hold no serving segment")
	for _, id := range []int64{201, 202} {
		assert.Equal(t, commonpb.SegmentState_Dropped,
			m.GetSegment(context.Background(), id).GetState())
	}
}

func TestAdvanceHashAdoptingKeepsSourceSegmentsUntilRoutingCommits(t *testing.T) {
	// Order matters more than either step: until the routing commit lands, the
	// source delegator is the only thing answering for its keys. A retire that
	// ran first — or ran anyway when the commit failed — would empty a shard
	// that is still being read.
	m := newHashRewriteMeta([]int64{201, 202})
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.router = nil // the committer is not wired yet
	task := fenceSources(newHashTask(nil), 100)
	task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting,
		mgr.mustGetTask(task.GetTaskId()).GetState())
	assert.Len(t, m.GetSegmentsByChannel(hashSrcVChannel), 2,
		"a source whose routing was never committed must keep serving")
}

// A collection dropped mid-split must not leave the task retrying forever.
// rootcoord owns collection existence, so datacoord's own meta can still hold
// one rootcoord has already dropped -- the nil check at the top of each phase
// passes and the routing commit is what reports the truth. Left to retry, the
// task never becomes terminal, is never reaped, and holds a slot in the
// cluster-wide concurrency budget across restarts, stopping every future split.
func TestADroppedCollectionRetiresTheTaskInsteadOfRetryingForever(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name  string
		state datapb.SplitShardTaskState
		want  string
	}{
		{
			"during the write switch", datapb.SplitShardTaskState_SplitShardTaskFencing,
			"collection dropped before the write switch",
		},
		{
			"during adoption", datapb.SplitShardTaskState_SplitShardTaskAdopting,
			"collection dropped before the routing was adopted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wal := mock_streaming.NewMockWALAccesser(t)
			wal.EXPECT().RawAppend(mock.Anything, mock.Anything, mock.Anything).
				Return(&types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 2000}, nil).Maybe()
			mgr, router := newFencingTestManager(t, wal)
			router.err = merr.WrapErrCollectionNotFound(int64(1))

			task := newMultiSourceHashTask()
			task.State = tc.state
			task.Fenced = true
			fenceSources(task, 2000)
			mgr.tasks.Insert(task.GetTaskId(), task)

			mgr.advanceTask(task)

			got := mgr.mustGetTask(task.GetTaskId())
			assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, got.GetState(),
				"the task must reach a terminal state so it is reaped and frees its slot")
			assert.Equal(t, tc.want, got.GetFailReason())
			assert.NotZero(t, got.GetEndTime())
		})
	}
}

func TestAnOrdinaryRoutingFailureStillRetries(t *testing.T) {
	// Only a missing collection is terminal. Anything transient -- rootcoord
	// restarting, a timeout -- must keep the task where it is.
	paramtable.Init()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().RawAppend(mock.Anything, mock.Anything, mock.Anything).
		Return(&types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 2000}, nil).Maybe()
	mgr, router := newFencingTestManager(t, wal)
	router.err = merr.WrapErrServiceInternal("rootcoord is restarting")

	task := newMultiSourceHashTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskAdopting
	task.Fenced = true
	fenceSources(task, 2000)
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestRedistributionRetiresWhenTheCollectionIsGone(t *testing.T) {
	// This phase never reaches a routing commit, so the check that retires the
	// fencing and adoption phases cannot cover it. Left running, the task
	// re-dispatches a rewrite for segments that no longer exist forever and
	// holds a slot in the cluster-wide concurrency budget, which stops every
	// future split in the cluster.
	paramtable.Init()
	for _, tc := range []struct {
		name string
		task *datapb.SplitShardTask
	}{
		{"rewrite", newMultiSourceHashTask()},
		{"relabel", func() *datapb.SplitShardTask {
			t := newMultiSourceHashTask()
			t.Redistribution = datapb.SplitShardRedistribution_SplitShardRelabel
			return t
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newHashRewriteMeta(nil)
			m.collections.Remove(1) // rootcoord dropped it mid-split
			mgr, _ := newHashSplitTestManager(t, m)

			task := tc.task
			task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
			task.Fenced = true
			fenceSources(task, 2000)
			mgr.tasks.Insert(task.GetTaskId(), task)

			mgr.advanceTask(task)

			got := mgr.mustGetTask(task.GetTaskId())
			assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, got.GetState())
			assert.Equal(t, "collection dropped during redistribution", got.GetFailReason())
			assert.NotZero(t, got.GetEndTime())
		})
	}
}

// --- target checkpoint seeding ----------------------------------------------
//
// A target vchannel that takes no live write before adoption has no checkpoint,
// and datacoord's seek fallback then answers with a rewrite segment's DML
// position: a timestamp, no message id, no WAL name. The dispatcher built on it
// skips its Seek and the querynode panics on the first read. Seeding the
// genesis position at creation is what keeps that fallback from ever being
// reached.

func genesisPos(vchannel string, ts uint64) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: vchannel,
		MsgID:       []byte{0x01, 0x02},
		WALName:     commonpb.WALName_RocksMQ,
		Timestamp:   ts,
	}
}

func TestSeedTargetCheckpointsWritesGenesisPositions(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	err := mgr.seedTargetCheckpoints([]*msgpb.MsgPosition{
		genesisPos(hashTgtA, 2100),
		genesisPos(hashTgtB, 2101),
	})
	assert.NoError(t, err)

	for vchannel, ts := range map[string]uint64{hashTgtA: 2100, hashTgtB: 2101} {
		cp := m.GetChannelCheckpoint(vchannel)
		assert.NotNil(t, cp, vchannel)
		assert.Equal(t, ts, cp.GetTimestamp())
		// The whole point: what lands must be seekable.
		assert.True(t, msgdispatcher.SeekablePosition(cp), vchannel)
	}
}

func TestSeedTargetCheckpointsLeavesAnExistingCheckpointAlone(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	// The target already advanced: the write switch is being retried and this
	// channel has taken writes since the first attempt created it.
	existing := genesisPos(hashTgtA, 5000)
	require.NoError(t, m.UpdateChannelCheckpoints(context.Background(), []*msgpb.MsgPosition{existing}))

	// The retry re-appends CreateVChannel and gets a LATER position. Writing it
	// would advance the checkpoint past messages nobody has consumed.
	err := mgr.seedTargetCheckpoints([]*msgpb.MsgPosition{
		genesisPos(hashTgtA, 9000),
		genesisPos(hashTgtB, 9001),
	})
	assert.NoError(t, err)

	assert.Equal(t, uint64(5000), m.GetChannelCheckpoint(hashTgtA).GetTimestamp(),
		"an existing checkpoint must not be overwritten by a later genesis position")
	assert.Equal(t, uint64(9001), m.GetChannelCheckpoint(hashTgtB).GetTimestamp(),
		"a channel that still has none is seeded")
}

func TestSeedTargetCheckpointsIgnoresNothingToDo(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	assert.NoError(t, mgr.seedTargetCheckpoints(nil))
	assert.NoError(t, mgr.seedTargetCheckpoints([]*msgpb.MsgPosition{nil}))
	assert.Nil(t, m.GetChannelCheckpoint(hashTgtA))
}
