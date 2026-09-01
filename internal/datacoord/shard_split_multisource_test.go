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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// A hash split may rewrite SEVERAL sources at once: changing a collection's
// shard count to an arbitrary M rehashes every key, so each target's bucket
// draws from every source. That makes the routing flip global — it cannot be
// applied one source at a time — and these tests pin the ordering rule that
// keeps it safe: the routing commit happens strictly after every source's fence.

const hashSrcVChannelB = "by-dev-rootcoord-dml_3_100v0"

// newMultiSourceHashTask builds a task rewriting two sources into two targets,
// the shape of a rehash (as opposed to a single shard's doubling).
func newMultiSourceHashTask() *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         9,
		CollectionId:   1,
		Sources: []*datapb.SplitShardTaskSource{
			{Vchannel: hashSrcVChannel},
			{Vchannel: hashSrcVChannelB},
		},
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: hashTgtA, Buckets: []uint64{0}},
			{Vchannel: hashTgtB, Buckets: []uint64{1}},
		},
		// A rehash to two shards: every target takes one residue at modulus 2.
		RoutingModulus: 2,
		State:          datapb.SplitShardTaskState_SplitShardTaskFencing,
	}
}

func TestAllHashSourcesFenced(t *testing.T) {
	// A task with no sources is not fenced: "every source is fenced" must not be
	// vacuously true, or an empty task would be allowed to commit a routing flip.
	assert.False(t, allHashSourcesFenced(&datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
	}))

	task := newMultiSourceHashTask()
	assert.False(t, allHashSourcesFenced(task))
	assert.Equal(t, 0, countFencedHashSources(task))

	// One fenced source is not enough — the other still accepts writes.
	task.Sources[0].SwitchTimeTick = 100
	assert.False(t, allHashSourcesFenced(task))
	assert.Equal(t, 1, countFencedHashSources(task))

	task.Sources[1].SwitchTimeTick = 120
	assert.True(t, allHashSourcesFenced(task))
	assert.Equal(t, 2, countFencedHashSources(task))
}

func TestMaxHashSwitchTimeTickTakesTheGreatest(t *testing.T) {
	// Each source is fenced by its own independently sequenced message, so the
	// targets' barrier must clear the LATEST fence, not just one of them.
	task := newMultiSourceHashTask()
	assert.Zero(t, maxHashSwitchTimeTick(task))

	task.Sources[0].SwitchTimeTick = 500
	task.Sources[1].SwitchTimeTick = 120
	assert.Equal(t, uint64(500), maxHashSwitchTimeTick(task))
}

func TestCommitHashRoutingRefusesAPartiallyFencedTask(t *testing.T) {
	// The invariant this guards: if the routing flipped while a source still
	// accepted writes, one primary key would have two live writers on two WALs
	// with no order between them, and a delete could be sequenced before the
	// insert it must remove. Enforced at the commit rather than trusted from the
	// caller's sequencing.
	mgr, router := newRoutingCommitManager(t,
		[]string{hashSrcVChannel, hashSrcVChannelB}, nil)
	task := newMultiSourceHashTask()
	task.Sources[0].SwitchTimeTick = 100 // only the first source is fenced

	err := mgr.commitRouting(task, mustCollection(t, mgr),
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 of 2 sources are not fenced")
	assert.Nil(t, router.last, "no routing must reach rootcoord while a source is unfenced")
}

func TestCommitHashRoutingRetiresEverySourceAndListsThemOnTargets(t *testing.T) {
	mgr, router := newRoutingCommitManager(t,
		[]string{hashSrcVChannel, hashSrcVChannelB}, nil)
	task := fenceSources(newMultiSourceHashTask(), 100)

	err := mgr.commitRouting(task, mustCollection(t, mgr),
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)
	require.NoError(t, err)
	require.NotNil(t, router.last)

	byVChannel := map[string]*schemapb.CollectionShardInfo{}
	for i, vchannel := range router.last.GetVirtualChannelNames() {
		byVChannel[vchannel] = router.last.GetShardInfos()[i]
	}

	// Both sources are retired together: a target's keys come from both, so
	// leaving one routable would keep two writers alive for the same key.
	for _, source := range []string{hashSrcVChannel, hashSrcVChannelB} {
		require.Contains(t, byVChannel, source)
		assert.Equal(t, schemapb.ShardState_ShardSplitting, byVChannel[source].GetState())
	}

	// Which sources a target was carved from is provenance and lives in the
	// split task, not in the collection meta -- so what the commit must get right
	// here is the state and the residues, and nothing names a source at all.
	for _, target := range []string{hashTgtA, hashTgtB} {
		require.Contains(t, byVChannel, target)
		assert.Equal(t, schemapb.ShardState_ShardCreating, byVChannel[target].GetState())
		assert.NotEmpty(t, byVChannel[target].GetHashRouting().GetBuckets())
	}
}

func TestHashFenceFlushedRequiresEverySourceToCatchUp(t *testing.T) {
	// The drain is per source: each source's checkpoint is compared against its
	// OWN T_switch, because each was fenced by a separate message at a separate
	// tick. One lagging source must hold the whole task back, or a target could
	// be adopted while part of its key range was still unflushed.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newMultiSourceHashTask()
	task.Sources[0].SwitchTimeTick = 100
	task.Sources[1].SwitchTimeTick = 200

	assert.False(t, mgr.hashFenceFlushed(task), "no checkpoints reported yet")

	m.channelCPs.checkpoints[hashSrcVChannel] = &msgpb.MsgPosition{Timestamp: 150}
	assert.False(t, mgr.hashFenceFlushed(task), "the second source has not caught up")

	m.channelCPs.checkpoints[hashSrcVChannelB] = &msgpb.MsgPosition{Timestamp: 150}
	assert.False(t, mgr.hashFenceFlushed(task), "150 < the second source's T_switch of 200")

	m.channelCPs.checkpoints[hashSrcVChannelB] = &msgpb.MsgPosition{Timestamp: 200}
	assert.True(t, mgr.hashFenceFlushed(task))
}

func TestRewriteDrainsOnlyWhenEverySourceIsEmpty(t *testing.T) {
	m := newHashRewriteMeta(nil)
	addSourceSegment(m, 301, hashSrcVChannel)
	addSourceSegment(m, 401, hashSrcVChannelB)
	mgr, _ := newHashSplitTestManager(t, m)

	task := fenceSources(newMultiSourceHashTask(), 0) // no fence tick: drain skips the flush check
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.Sources[0].PendingSegments = []int64{301}
	task.Sources[1].PendingSegments = []int64{401}
	mgr.tasks.Insert(task.GetTaskId(), task)

	d := newFakeRewriteDispatcher()
	res := mgr.advanceRewriting(task, d, 10)
	assert.ElementsMatch(t, []int64{301, 401}, res.dispatched,
		"segments of every source are dispatched, not just the first source's")

	// Only the first source's rewrite commits: the task must stay in Rewriting.
	addRewriteOutput(m, 9001, hashTgtA, 301)
	task = mgr.mustGetTask(task.GetTaskId())
	d.complete(d.dispatched[301])
	mgr.advanceRewriting(task, d, 10)
	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, updated.GetState(),
		"one drained source must not advance the task")
	assert.ElementsMatch(t, []int64{401}, allPendingSegments(updated))

	// The second source finishes too: now the task may advance.
	addRewriteOutput(m, 9002, hashTgtB, 401)
	d.complete(d.dispatched[401])
	mgr.advanceRewriting(mgr.mustGetTask(task.GetTaskId()), d, 10)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAdopting,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestRewriteBatchBoundsTheTaskNotEachSource(t *testing.T) {
	// The batch caps the task's total in-flight rewrites. A per-source bound
	// would multiply the compaction load by the number of sources, which is
	// exactly what a rehash of a large collection cannot afford.
	m := newHashRewriteMeta(nil)
	for _, id := range []int64{301, 302} {
		addSourceSegment(m, id, hashSrcVChannel)
	}
	for _, id := range []int64{401, 402} {
		addSourceSegment(m, id, hashSrcVChannelB)
	}
	mgr, _ := newHashSplitTestManager(t, m)

	task := newMultiSourceHashTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.Sources[0].PendingSegments = []int64{301, 302}
	task.Sources[1].PendingSegments = []int64{401, 402}
	mgr.tasks.Insert(task.GetTaskId(), task)

	res := mgr.advanceRewriting(task, newFakeRewriteDispatcher(), 3)
	assert.Len(t, res.dispatched, 3, "3 across both sources, not 3 per source")
}

func TestRetireRewrittenSegmentsSpansSources(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newMultiSourceHashTask()
	task.Sources[0].PendingSegments = []int64{301}
	task.Sources[1].PendingSegments = []int64{401}

	// One output per source, both committed on target A.
	addRewriteOutput(m, 9001, hashTgtA, 301)
	addRewriteOutput(m, 9002, hashTgtA, 401)

	pending := pendingRewriteSegments(task)
	require.Equal(t, 2, totalPendingRewrites(pending))
	assert.Equal(t, 2, retireRewrittenSegments(pending, mgr.rewrittenSourceSegments(task)))
	assert.Zero(t, totalPendingRewrites(pending))
}

func TestAdvanceHashPreparingSeedsEverySourcesWorkList(t *testing.T) {
	m := newHashRewriteMeta(nil)
	addSourceSegment(m, 301, hashSrcVChannel)
	addSourceSegment(m, 401, hashSrcVChannelB)
	coll, _ := m.collections.Get(1)
	coll.VChannelNames = []string{hashSrcVChannel, hashSrcVChannelB}

	mgr, _ := newHashSplitTestManager(t, m)
	mgr.vchannelAllocator = &fakeVChannelAllocator{vchannels: []string{hashTgtA, hashTgtB}}
	task := newMultiSourceHashTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceRewritePreparing(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, updated.GetState())
	// Each source keeps its own list, so a later per-source check can name the
	// segments it is waiting on.
	assert.Equal(t, []int64{301}, updated.GetSources()[0].GetPendingSegments())
	assert.Equal(t, []int64{401}, updated.GetSources()[1].GetPendingSegments())
}

func TestAdvanceHashPreparingAbortsWithoutSources(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newMultiSourceHashTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	task.Sources = nil
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceRewritePreparing(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, updated.GetState())
}

func TestHasActiveHashTaskCoversEverySource(t *testing.T) {
	// The trigger must not re-fire on a shard that is already a source of a
	// multi-source task, and compaction must not replace segments a rewrite is
	// reading — both go through this predicate.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newMultiSourceHashTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	mgr.tasks.Insert(task.GetTaskId(), task)

	assert.True(t, mgr.hasActiveHashTaskOnVChannel(hashSrcVChannel))
	assert.True(t, mgr.hasActiveHashTaskOnVChannel(hashSrcVChannelB),
		"a later source must be covered too, not only the first")
	assert.True(t, mgr.hasActiveHashTaskOnVChannel(hashTgtA))
	assert.False(t, mgr.hasActiveHashTaskOnVChannel("by-dev-rootcoord-dml_9_100v0"))
}

func TestAnyHashSourceImportingChecksEverySource(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	importMeta := NewMockImportMeta(t)
	mgr.setImportMeta(importMeta)
	task := newMultiSourceHashTask()

	importMeta.EXPECT().GetJobBy(mock.Anything, mock.Anything).Return(nil).Once()
	assert.False(t, mgr.anyHashSourceImporting(task))

	// An import landing on the SECOND source must still hold the drain back: a
	// check that stopped at the first source would let the rewrite declare the
	// collection drained while an import was still adding segments.
	importMeta.EXPECT().GetJobBy(mock.Anything, mock.Anything).Return([]ImportJob{
		&importJob{ImportJob: &datapb.ImportJob{
			Vchannels: []string{hashSrcVChannelB},
			State:     internalpb.ImportJobState_Pending,
		}},
	}).Once()
	assert.True(t, mgr.anyHashSourceImporting(task))
}

// newFencingTestManager wires a manager that can run the fencing phase: a
// mockable WAL, a monotonic timestamp allocator and a capturing router.
func newFencingTestManager(t *testing.T, wal streaming.WALAccesser) (*shardSplitManager, *captureRouter) {
	m := newHashRewriteMeta(nil)
	coll, _ := m.collections.Get(1)
	coll.VChannelNames = []string{hashSrcVChannel, hashSrcVChannelB}
	coll.DatabaseName = "db"
	coll.Partitions = []int64{10}

	mgr, _ := newHashSplitTestManager(t, m)
	alloc := allocator.NewMockAllocator(t)
	// Above every fence tick the test uses, so the barrier check passes.
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(9000), nil).Maybe()
	mgr.allocator = alloc
	mgr.wal = wal
	router := &captureRouter{}
	mgr.router = router
	return mgr, router
}

// splitShardOn matches a SplitShard fence addressed to the given vchannel.
func splitShardOn(vchannel string) any {
	return mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeSplitShard && msg.VChannel() == vchannel
	})
}

func TestAdvanceHashFencingHoldsTheRoutingUntilEverySourceIsFenced(t *testing.T) {
	// The ordering rule, end to end: one source fences, the other fails. The
	// task must NOT create targets and must NOT commit the routing, because a
	// live routing flip plus a still-writable source is exactly the double-writer
	// state the fence exists to prevent.
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannel)).
		Return(&types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 2000}, nil).Once()
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannelB)).
		Return(nil, errors.New("streamingnode unavailable")).Once()

	mgr, router := newFencingTestManager(t, wal)
	task := newMultiSourceHashTask()
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceFencing(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, updated.GetState())
	assert.Nil(t, router.last, "the routing must not flip while a source is still writable")
	// The landed fence is kept: it cannot be revoked, so the task is now
	// forward-only and must resume from here rather than start over.
	assert.True(t, updated.GetFenced())
	assert.Equal(t, uint64(2000), updated.GetSources()[0].GetSwitchTimeTick())
	assert.Zero(t, updated.GetSources()[1].GetSwitchTimeTick())
	// No CreateVChannel was appended — the mock would have failed on an
	// unexpected call, which is the assertion.
}

func TestTheFenceIntentIsPersistedBeforeTheFirstAppend(t *testing.T) {
	// The whole safety argument for canceling an unfenced task rests on this
	// ordering: the flag that closes the abort window is durable BEFORE any
	// fence reaches a WAL. Written the other way round -- as it was, from the
	// tick the fences landed -- a crash in between would leave a fenced vchannel
	// on a task that still looked abortable, and aborting it would strand that
	// vchannel SPLITTED with nothing left to finish the split.
	var fencedAtAppend []bool
	wal := mock_streaming.NewMockWALAccesser(t)
	mgr, _ := newFencingTestManager(t, wal)
	task := newMultiSourceHashTask()

	var mu sync.Mutex
	record := func(context.Context, message.MutableMessage, ...streaming.AppendOption) {
		mu.Lock()
		defer mu.Unlock()
		fencedAtAppend = append(fencedAtAppend, mgr.mustGetTask(task.GetTaskId()).GetFenced())
	}
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannel)).
		Run(record).Return(&types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 2000}, nil).Once()
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannelB)).
		Run(record).Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2500}, nil).Once()
	wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeCreateVChannel
	}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 9001}, nil).Times(2)

	require.False(t, task.GetFenced(), "the task starts abortable")
	mgr.tasks.Insert(task.GetTaskId(), task)
	mgr.advanceFencing(task)

	require.Len(t, fencedAtAppend, 2)
	assert.Equal(t, []bool{true, true}, fencedAtAppend,
		"every fence append must see the intent already persisted")
}

func TestAFenceIntentThatCannotBePersistedAppendsNothing(t *testing.T) {
	// If the flag cannot be written, appending anyway would create exactly the
	// unaccountable fence the ordering exists to prevent. The mock WAL has no
	// expectations, so any append fails this test.
	wal := mock_streaming.NewMockWALAccesser(t)
	mgr, _ := newFencingTestManager(t, wal)
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).
		Return(errors.New("etcd unavailable"))
	mgr.catalog = catalog

	task := newMultiSourceHashTask()
	mgr.tasks.Insert(task.GetTaskId(), task)
	mgr.fenceHashSources(task)

	assert.False(t, mgr.mustGetTask(task.GetTaskId()).GetFenced(),
		"the task stays abortable and retries on the next tick")
}

func TestAdvanceHashFencingCommitsOnceEverySourceIsFenced(t *testing.T) {
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannel)).
		Return(&types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 2000}, nil).Once()
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannelB)).
		Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2500}, nil).Once()
	wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeCreateVChannel
	}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 9001}, nil).Times(2)

	mgr, router := newFencingTestManager(t, wal)
	task := newMultiSourceHashTask()
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceFencing(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, updated.GetState())
	require.NotNil(t, router.last, "the routing commits once every source is fenced")
	// Both fences are recorded with their own tick: they are separate messages
	// in separate WALs, so there is no single collection-wide T_switch.
	assert.Equal(t, uint64(2000), updated.GetSources()[0].GetSwitchTimeTick())
	assert.Equal(t, uint64(2500), updated.GetSources()[1].GetSwitchTimeTick())
}

func TestAdvanceHashFencingResumesTheUnfencedSourceOnly(t *testing.T) {
	// A restart after a partial fence must re-ask only the source that has no
	// recorded tick. Re-fencing the first would be harmless (it is idempotent
	// and returns the same tick) but the task already knows its answer.
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().RawAppend(mock.Anything, splitShardOn(hashSrcVChannelB)).
		Return(&types.AppendResult{MessageID: rmq.NewRmqID(2), TimeTick: 2500}, nil).Once()
	wal.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeCreateVChannel
	}), mock.Anything).Return(&types.AppendResult{MessageID: rmq.NewRmqID(3), TimeTick: 9001}, nil).Times(2)

	mgr, _ := newFencingTestManager(t, wal)
	task := newMultiSourceHashTask()
	task.Fenced = true
	task.Sources[0].SwitchTimeTick = 2000 // survived the crash
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceFencing(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, updated.GetState())
	assert.Equal(t, uint64(2000), updated.GetSources()[0].GetSwitchTimeTick(),
		"the recorded tick is kept, not replaced by a second fence")
}

func TestAbortRefusedOnceAnySourceIsFenced(t *testing.T) {
	// Abort stops being possible at the FIRST fence, not the last: that fence
	// already marked its vchannel SPLITTED in the streamingnode's recovery info
	// and is never revoked, so there is no state to roll back to.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	task := newMultiSourceHashTask()
	task.Fenced = true
	task.Sources[0].SwitchTimeTick = 2000 // only one of two sources fenced
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.abortTask(task, "too late")

	assert.NotEqual(t, datapb.SplitShardTaskState_SplitShardTaskAborted,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

// addSourceSegment registers a flushed segment on the given source vchannel.
func addSourceSegment(m *meta, id int64, vchannel string) {
	m.segments.SetSegment(id, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  1,
			InsertChannel: vchannel,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     100,
		},
	})
}

// mustCollection returns the test collection the routing commit is built from.
func mustCollection(t *testing.T, mgr *shardSplitManager) *collectionInfo {
	coll := mgr.meta.GetCollection(1)
	require.NotNil(t, coll)
	return coll
}

func TestFrontingHostsAreAPartitionOfTheTargets(t *testing.T) {
	// The property the fix exists for: every target has exactly ONE fronting
	// source. A read fans out to all sources and each source merges its
	// children's results into its own, so a target fronted twice would have its
	// post-fence rows returned twice.
	task := newMultiSourceHashTask()
	task.Targets = append(task.Targets,
		&datapb.SplitShardTaskTarget{Vchannel: "tgt-c", Buckets: []uint64{2}},
		&datapb.SplitShardTaskTarget{Vchannel: "tgt-d", Buckets: []uint64{3}},
		&datapb.SplitShardTaskTarget{Vchannel: "tgt-e", Buckets: []uint64{7}})

	hosts := hashSplitFrontingHosts(task)
	require.Len(t, hosts, len(task.GetTargets()), "every target must have a host")
	for _, target := range task.GetTargets() {
		assert.Contains(t, []string{hashSrcVChannel, hashSrcVChannelB}, hosts[target.GetVchannel()],
			"the host must be one of the task's sources")
	}

	// Union over the per-source fence messages covers every target exactly once.
	seen := map[string]int{}
	for _, source := range task.GetSources() {
		for _, target := range toMessageHashSplitTargets(task, source.GetVchannel()) {
			seen[target.GetVchannel()]++
		}
	}
	require.Len(t, seen, len(task.GetTargets()))
	for vchannel, count := range seen {
		assert.Equal(t, 1, count, "target %s is fronted %d times", vchannel, count)
	}
}

func TestFrontingHostOfADoublingIsItsOnlySource(t *testing.T) {
	// The single-source case must be unchanged: that source fronts both targets,
	// which is what the namespace split and the size-triggered doubling rely on.
	task := newHashTask(nil)
	hosts := hashSplitFrontingHosts(task)
	assert.Equal(t, hashSrcVChannel, hosts[hashTgtA])
	assert.Equal(t, hashSrcVChannel, hosts[hashTgtB])
	assert.Len(t, toMessageHashSplitTargets(task, hashSrcVChannel), 2)
}

func TestFrontingHostIsStableAcrossCalls(t *testing.T) {
	// The live spawn (per-source fence message) and a querynode's rebuild from
	// meta (the committed fronting_source_vchannel) must agree, so the assignment
	// has to be a pure function of the persisted task rather than of call order.
	task := newMultiSourceHashTask()
	first := hashSplitFrontingHosts(task)
	second := hashSplitFrontingHosts(task)
	assert.Equal(t, first, second)
}

// The fronting host -- which single source's delegator fronts a target's reads
// during the window -- is derived from the task, not stored in the collection
// meta. It is provenance with the task's lifetime, so what the commit must get
// right is that every target is Creating and carries its residues; who fronts it
// is asked of the task.
func TestCommitHashRoutingLeavesFrontingToTheTask(t *testing.T) {
	mgr, router := newRoutingCommitManager(t,
		[]string{hashSrcVChannel, hashSrcVChannelB}, nil)
	task := fenceSources(newMultiSourceHashTask(), 100)

	require.NoError(t, mgr.commitRouting(task, mustCollection(t, mgr),
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))
	require.NotNil(t, router.last)

	// Every target has exactly one fronting host, and the assignment is stable:
	// the same task yields the same hosts, which is what lets a querynode
	// rebuilding after a restart front the same set the live spawn did.
	hosts := hashSplitFrontingHosts(task)
	again := hashSplitFrontingHosts(task)
	assert.Equal(t, hosts, again)

	creating := 0
	for i, vchannel := range router.last.GetVirtualChannelNames() {
		info := router.last.GetShardInfos()[i]
		if info.GetState() != schemapb.ShardState_ShardCreating {
			continue
		}
		creating++
		assert.NotEmpty(t, info.GetHashRouting().GetBuckets())
		assert.Contains(t, []string{hashSrcVChannel, hashSrcVChannelB}, hosts[vchannel])
	}
	assert.Equal(t, len(task.GetTargets()), creating)
}

func TestMergedRecoveryViewExcludesHashSplitTargets(t *testing.T) {
	// The merged recovery view exists for the RELABEL path, where a segment
	// moves off the source channel and would otherwise vanish from its view.
	//
	// A rewrite moves nothing — the source segments stay put and the outputs are
	// new segments holding copies of the same rows — so folding the targets in
	// would make the source delegator serve every rewritten row twice. This
	// pins that exclusion, because the omission reads like an oversight and
	// "fixing" it would introduce the duplication.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	task := newMultiSourceHashTask()
	task.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	mgr.tasks.Insert(task.GetTaskId(), task)

	for _, source := range []string{hashSrcVChannel, hashSrcVChannelB} {
		assert.Empty(t, mgr.SplitTargetsOfSource(source),
			"a hash split source must not fold its targets into its recovery view")
	}

	// A relabel task on the same channel still does, which is what tells the two
	// apart rather than the view simply being dead.
	mgr.tasks.Insert(100, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRelabel,
		TaskId:         100,
		CollectionId:   1,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: hashSrcVChannel}},
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "relabel-tgt-a"},
			{Vchannel: "relabel-tgt-b"},
		},
	})
	assert.ElementsMatch(t, []string{"relabel-tgt-a", "relabel-tgt-b"},
		mgr.SplitTargetsOfSource(hashSrcVChannel))
}

func TestSplitFreezeExemptsItsOwnRewrite(t *testing.T) {
	// The freeze rejects compactions on a splitting channel so nothing churns
	// the redistribution work list. A hash split's rewrite runs on the source
	// channel by construction, so without an exemption the split would block its
	// own redistribution and sit in Rewriting forever, retrying a dispatch that
	// can never succeed.
	inspector := &compactionInspector{
		isChannelSplitting: func(channel string) bool { return channel == hashSrcVChannel },
	}

	assert.True(t, inspector.frozenBySplit(&datapb.CompactionTask{
		Type: datapb.CompactionType_MixCompaction, Channel: hashSrcVChannel,
	}), "an ordinary compaction on a splitting channel stays frozen")

	assert.False(t, inspector.frozenBySplit(&datapb.CompactionTask{
		Type: datapb.CompactionType_HashSplitCompaction, Channel: hashSrcVChannel,
	}), "the split's own rewrite must not be blocked by the split's own freeze")

	assert.False(t, inspector.frozenBySplit(&datapb.CompactionTask{
		Type: datapb.CompactionType_MixCompaction, Channel: "not-splitting",
	}), "an unrelated channel is untouched")
}

// The routing table must become explicit the moment a collection is split, and
// the shards the split did not touch must keep the bucket they already owned.
//
// [vch0, vch1] routed by hash%2. Splitting vch0 must commit
//
//	{vch1:{2,1}, vch2:{4,0}, vch3:{4,2}}
//
// and NOT recompute vch1 against the grown shard count — vch1's keys were never
// part of this split, and moving them would strand every row already written
// there.
// The untouched shards must be carried across a doubling, not left behind at the
// old modulus. This is the rebase half of the commit, and getting it wrong opens
// exactly the same routing gap as omitting residues altogether.
func TestCommitRebasesUntouchedShardsOntoTheNewModulus(t *testing.T) {
	mgr, router := newRoutingCommitManager(t, []string{"vch0", "vch1"}, nil)
	collection := mustCollection(t, mgr)

	task := fenceSources(&datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         11,
		CollectionId:   1,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: "vch0"}},
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "vch2", Buckets: []uint64{0}},
			{Vchannel: "vch3", Buckets: []uint64{2}},
		},
		// vch0 owned residue 0 at modulus 2 and had nothing left to divide, so
		// the split doubled the modulus to 4.
		RoutingModulus: 4,
	}, 100)

	require.NoError(t, mgr.commitRouting(task, collection,
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))
	require.NotNil(t, router.last)

	byVChannel := map[string]*schemapb.CollectionShardInfo{}
	for i, vchannel := range router.last.GetVirtualChannelNames() {
		byVChannel[vchannel] = router.last.GetShardInfos()[i]
	}

	residuesOfShard := func(vchannel string) []uint64 {
		return byVChannel[vchannel].GetHashRouting().GetBuckets()
	}

	// The untouched shard owned residue 1 at modulus 2; the split doubled the
	// modulus, so its keys are now residues 1 and 3 at modulus 4. Writing 1 alone
	// would hand half of its keys to nobody.
	assert.Equal(t, []uint64{1, 3}, residuesOfShard("vch1"),
		"the untouched shard is rebased onto the new modulus, keeping the same keys")
	assert.Equal(t, []uint64{0}, residuesOfShard("vch2"))
	assert.Equal(t, []uint64{2}, residuesOfShard("vch3"))
	assert.EqualValues(t, 4, router.last.GetRoutingModulus())

	// And the committed topology must derive into a working table. Without the
	// rebase vch1 covers only half of what it owns and this fails with a routing
	// gap -- which is what would make the write path unusable after the split.
	shards, err := routing.ShardsFromMeta(
		router.last.GetVirtualChannelNames(), router.last.GetShardInfos())
	require.NoError(t, err)
	table, err := routing.Derive(router.last.GetRoutingModulus(),
		router.last.GetVirtualChannelNames(), shards)
	require.NoError(t, err, "the committed topology must tile the key space")

	// Every key that used to reach vch1 still reaches vch1.
	for h := uint64(0); h < 40; h++ {
		if h%2 != 1 {
			continue
		}
		got, err := table.Route(h)
		require.NoError(t, err, "hash %d must route somewhere", h)
		assert.Equal(t, "vch1", got, "hash %d moved off vch1", h)
	}
}

func TestResiduesIgnoreRetiredShards(t *testing.T) {
	// After an earlier split the vchannel list still carries the retired source,
	// so its length is a modulus the collection never routed by. The implicit
	// derivation must count only the routable shards.
	m := newHashRewriteMeta(nil)
	collection := m.GetCollection(1)
	collection.VChannelNames = []string{"retired", "live0", "live1"}
	collection.ShardInfos = map[string]*schemapb.CollectionShardInfo{
		"retired": {State: schemapb.ShardState_ShardDropped},
		"live0":   {State: schemapb.ShardState_ShardNormal},
		"live1":   {State: schemapb.ShardState_ShardNormal},
	}

	residues, err := residuesOf(collection)
	require.NoError(t, err)
	assert.EqualValues(t, 2, residues.modulus, "the modulus counts routable shards, not vchannels")
	assert.NotContains(t, residues.byVChannel, "retired")

	own, err := residues.of("live0")
	require.NoError(t, err)
	assert.Equal(t, []uint64{0}, own)
	own, err = residues.of("live1")
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, own)
}

func TestResiduesUseTheMetaWhereItExists(t *testing.T) {
	// A shard with explicit residues is not guessed at: its residues are what
	// the meta says, and a position-based guess would contradict them.
	m := newHashRewriteMeta(nil)
	collection := m.GetCollection(1)
	collection.RoutingModulus = 4
	collection.VChannelNames = []string{"explicit", "other"}
	collection.ShardInfos = map[string]*schemapb.CollectionShardInfo{
		"explicit": {
			State: schemapb.ShardState_ShardNormal,
			Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{3}},
			},
		},
		"other": {
			State: schemapb.ShardState_ShardNormal,
			Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 1, 2}},
			},
		},
	}

	residues, err := residuesOf(collection)
	require.NoError(t, err)
	assert.EqualValues(t, 4, residues.modulus)
	own, err := residues.of("explicit")
	require.NoError(t, err)
	assert.Equal(t, []uint64{3}, own)
}

func TestRoutingCommitRefreshesTheCachedTopology(t *testing.T) {
	// Nothing else does. BroadcastAlteredCollection refreshes only Properties
	// and Schema, so without this the cache still describes the PRE-split
	// collection after a commit — and every later decision is made against a
	// topology that no longer exists.
	//
	// Observed in a real run: the shard-count reconciler compared the requested
	// count against the stale shard set, concluded it was not reached, and
	// started the same rehash again — 26 times, re-fencing the collection on
	// every round.
	mgr, _ := newRoutingCommitManager(t,
		[]string{hashSrcVChannel, hashSrcVChannelB}, nil)
	collection := mustCollection(t, mgr)
	require.Len(t, collection.VChannelNames, 2)

	task := fenceSources(newMultiSourceHashTask(), 100)
	require.NoError(t, mgr.commitRouting(task, collection,
		schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal))

	refreshed := mgr.meta.GetCollection(1)
	require.NotNil(t, refreshed)
	assert.ElementsMatch(t,
		[]string{hashSrcVChannel, hashSrcVChannelB, hashTgtA, hashTgtB},
		refreshed.VChannelNames, "the cache must carry the post-commit vchannel list")

	// And the per-shard states, which is what the reconciler counts.
	require.Contains(t, refreshed.ShardInfos, hashTgtA)
	assert.Equal(t, schemapb.ShardState_ShardNormal, refreshed.ShardInfos[hashTgtA].GetState())
	assert.Equal(t, schemapb.ShardState_ShardDropped, refreshed.ShardInfos[hashSrcVChannel].GetState())

	// The reconciler's own predicate now sees the achieved count, so it stops.
	assert.Len(t, mgr.rehashSources(refreshed), 2,
		"the retired sources must no longer count as routable shards")
}

func TestReconcileConvergesAfterTheCommit(t *testing.T) {
	// End of the loop, stated as a property: once the collection HAS the
	// requested count, a further tick must start nothing.
	mgr, _ := newRoutingCommitManager(t,
		[]string{hashSrcVChannel, hashSrcVChannelB}, nil)
	collection := mustCollection(t, mgr)
	collection.RoutingModulus = 0
	collection.Properties = map[string]string{common.CollectionShardNum: "2"}

	task := fenceSources(newMultiSourceHashTask(), 100)
	require.NoError(t, mgr.commitRouting(task, collection,
		schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal))

	// 2 targets live, 2 sources retired: the desired count of 2 is reached.
	mgr.reconcileDesiredShardNum()

	count := 0
	mgr.tasks.Range(func(int64, *datapb.SplitShardTask) bool { count++; return true })
	assert.Zero(t, count, "a reached shard count must start no further rehash")
}
