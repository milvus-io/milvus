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
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// manualShardNumMode selects the mode in which the user owns the shard count.
// reconcileDesiredShardNum returns immediately in any other mode, and the
// feature is off by default, so a reconciliation test that skips this passes
// without ever reaching the code it means to exercise.
func manualShardNumMode(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "true")
	params.Save(params.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "false")
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.ShardSplitEnable.Key)
		params.Reset(params.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)
	})
}

// rehashCollection prepares collection 1 as a hash-routed collection with the
// given routable shards and, when non-empty, a declared shard count.
func rehashCollection(m *meta, vchannels []string, shardNum string) *collectionInfo {
	collection := m.GetCollection(1)
	collection.VChannelNames = vchannels
	collection.RoutingModulus = 0
	collection.Properties = map[string]string{}
	if shardNum != "" {
		collection.Properties[common.CollectionShardNum] = shardNum
	}
	return collection
}

// preparingRehash is a rehash toward targets shards that has not fenced: the
// only state in which a cancel is offered.
func preparingRehash(taskID int64, sources []string, targets int) *datapb.SplitShardTask {
	task := &datapb.SplitShardTask{
		TaskId:         taskID,
		CollectionId:   1,
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
	}
	for _, vchannel := range sources {
		task.Sources = append(task.Sources, &datapb.SplitShardTaskSource{Vchannel: vchannel})
	}
	for r := 0; r < targets; r++ {
		task.Targets = append(task.Targets, &datapb.SplitShardTaskTarget{
			Buckets: []uint64{uint64(r)},
		})
	}
	return task
}

func TestCancelWithdrawnRehashRetiresThePreparingTask(t *testing.T) {
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	// The property is gone: the collection has withdrawn its request.
	rehashCollection(m, []string{"v0", "v1"}, "")
	mgr.tasks.Insert(9, preparingRehash(9, []string{"v0", "v1"}, 4))

	mgr.reconcileDesiredShardNum()

	got := mgr.mustGetTask(9)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, got.GetState())
	assert.Contains(t, got.GetFailReason(), "withdrew")
	assert.NotZero(t, got.GetEndTime(), "a canceled task must be reapable like any other terminal one")
}

func TestCancelWithdrawnRehashStopsTheReconcilerFromStartingANewOne(t *testing.T) {
	// The cancel that matters most has no task to abort: an intent nobody can
	// satisfy is retried every tick until it is withdrawn.
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	rehashCollection(m, []string{"v0", "v1"}, "")

	mgr.reconcileDesiredShardNum()

	count := 0
	mgr.tasks.Range(func(int64, *datapb.SplitShardTask) bool { count++; return true })
	assert.Zero(t, count, "no request, no task")
}

func TestChangingTheShardCountSupersedesThePreparingRehash(t *testing.T) {
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	rehashCollection(m, []string{"v0", "v1"}, "8")
	// StartRehash asks the streamingcoord balancer singleton whether the cluster
	// is replicating. Answering it here keeps the test off a process-global that
	// other tests in this package register their own mocks into.
	replicating := mockey.Mock((*shardSplitManager).clusterReplicating).Return(false).Build()
	defer replicating.UnPatch()
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(10, nil)
	mgr.allocator = alloc
	mgr.tasks.Insert(9, preparingRehash(9, []string{"v0", "v1"}, 4))

	mgr.reconcileDesiredShardNum()

	superseded := mgr.mustGetTask(9)
	require.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, superseded.GetState())
	assert.Contains(t, superseded.GetFailReason(), "now asks for 8 shards")

	// Retiring it frees the collection-wide claim, so the rehash toward the new
	// count starts on this same tick rather than a later one.
	var replacement *datapb.SplitShardTask
	mgr.tasks.Range(func(id int64, task *datapb.SplitShardTask) bool {
		if id != 9 {
			replacement = task
		}
		return true
	})
	require.NotNil(t, replacement, "the new request must be picked up immediately")
	assert.Len(t, replacement.GetTargets(), 8)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, replacement.GetState())
}

func TestARehashPursuingTheDesiredCountIsLeftAlone(t *testing.T) {
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	rehashCollection(m, []string{"v0", "v1"}, "4")
	mgr.tasks.Insert(9, preparingRehash(9, []string{"v0", "v1"}, 4))

	mgr.reconcileDesiredShardNum()

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mgr.mustGetTask(9).GetState())
}

func TestAFencedRehashSurvivesAWithdrawal(t *testing.T) {
	// Once the fence step has been entered the append may already be durable in
	// a source's WAL, and a fence is never revoked: aborting there would strand
	// the vchannel SPLITTED with nothing left to finish the split.
	manualShardNumMode(t)
	for _, state := range []datapb.SplitShardTaskState{
		datapb.SplitShardTaskState_SplitShardTaskFencing,
		datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		datapb.SplitShardTaskState_SplitShardTaskAdopting,
	} {
		t.Run(state.String(), func(t *testing.T) {
			m := newHashRewriteMeta(nil)
			mgr, _ := newHashSplitTestManager(t, m)
			rehashCollection(m, []string{"v0", "v1"}, "")
			task := preparingRehash(9, []string{"v0", "v1"}, 4)
			task.State = state
			task.Fenced = true
			mgr.tasks.Insert(9, task)

			mgr.reconcileDesiredShardNum()

			assert.Equal(t, state, mgr.mustGetTask(9).GetState(),
				"a rehash past the point of no return runs to completion")
		})
	}
}

func TestAWithdrawalStillCancelsATaskWaitingToFence(t *testing.T) {
	// The window a cancel actually gets. The loop reconciles and then advances,
	// so a task created by the reconciler is prepared in that same tick and sits
	// in Fencing, unfenced, until the next one -- if only Preparing could be
	// canceled the window would be microseconds wide and unreachable.
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	rehashCollection(m, []string{"v0", "v1"}, "")
	task := preparingRehash(9, []string{"v0", "v1"}, 4)
	task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	task.Fenced = false
	mgr.tasks.Insert(9, task)

	mgr.reconcileDesiredShardNum()

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted,
		mgr.mustGetTask(9).GetState())
}

func TestCancellingARehashLeavesADoublingAlone(t *testing.T) {
	// A doubling carves one bucket and is driven by shard size, not by the shard
	// count property — one left over from before the cluster was switched to
	// manual control is not this reconciler's to retire.
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	rehashCollection(m, []string{"v0", "v1"}, "")
	doubling := newHashTask(nil)
	doubling.TaskId = 9
	doubling.State = datapb.SplitShardTaskState_SplitShardTaskPreparing
	doubling.Fenced = false
	require.False(t, tilesTheKeySpace(doubling.GetTargets()),
		"the fixture must be a bucket carve, or this test proves nothing")
	mgr.tasks.Insert(9, doubling)

	mgr.reconcileDesiredShardNum()

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mgr.mustGetTask(9).GetState())
}

func TestTilesTheKeySpaceRecognisesWhatPlanRehashBuilds(t *testing.T) {
	// The predicate and the planner have to agree, or a rehash could not be
	// canceled by the very property that started it.
	for _, shardNum := range []int32{2, 3, 8} {
		targets, err := planRehash(shardNum)
		require.NoError(t, err)
		assert.True(t, tilesTheKeySpace(targets), "planRehash(%d)", shardNum)
	}

	// A partial cover, a duplicated remainder, and a modulus that disagrees with
	// the target count are each disqualifying.
	assert.False(t, tilesTheKeySpace([]*datapb.SplitShardTaskTarget{
		{Buckets: []uint64{0}}, {Buckets: []uint64{2}},
	}))
	assert.False(t, tilesTheKeySpace([]*datapb.SplitShardTaskTarget{
		{Buckets: []uint64{0}}, {Buckets: []uint64{0}},
	}))
	assert.False(t, tilesTheKeySpace([]*datapb.SplitShardTaskTarget{{Buckets: []uint64{0}}}))
	assert.False(t, tilesTheKeySpace(nil))
}

func TestCancellingARehashLeavesANamespaceSplitAlone(t *testing.T) {
	// A relabeling split answers to namespace boundaries, not to the shard count
	// property, so a withdrawal must not touch it.
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	rehashCollection(m, []string{"v0", "v1"}, "")
	relabel := preparingRehash(9, []string{"v0"}, 2)
	relabel.Redistribution = datapb.SplitShardRedistribution_SplitShardRelabel
	mgr.tasks.Insert(9, relabel)

	mgr.reconcileDesiredShardNum()

	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mgr.mustGetTask(9).GetState())
}

func TestPlanRehashTilesTheKeySpace(t *testing.T) {
	targets, err := planRehash(5)
	require.NoError(t, err)
	require.Len(t, targets, 5)

	// The targets must be an exact cover: derive the routing table the write
	// path would build from them and let it do the checking, so this test fails
	// for the same reason live routing would reject the plan.
	shards := make([]routing.HashShard, 0, len(targets))
	for i, target := range targets {
		assert.Equal(t, []uint64{uint64(i)}, target.GetBuckets())
		shards = append(shards, routing.HashShard{
			Vchannel: string(rune('a' + i)),
			Buckets:  target.GetBuckets(),
		})
	}
	table, err := routing.DeriveHash(5, shards)
	require.NoError(t, err, "the rehash targets must tile the key space with no gap or overlap")
	assert.Equal(t, uint64(5), table.NumSlots())
}

func TestPlanRehashRejectsTooFewShards(t *testing.T) {
	for _, n := range []int32{-1, 0, 1} {
		_, err := planRehash(n)
		assert.Error(t, err, "shardNum=%d", n)
	}
}

func TestRehashHeadroomCountsSourcesAndTargetsTogether(t *testing.T) {
	// Sources are not retired until their data is rewritten, so both sets exist
	// at once and the peak demand is their sum — not max(N, M). Checking it
	// before the first fence matters because the task cannot be abandoned after
	// one (design §4.4).
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	collection := m.GetCollection(1)
	collection.VChannelNames = make([]string, 8) // 8 pchannels already held

	// 8 held + 8 new targets = 16, exactly the default dmlChannelNum.
	assert.NoError(t, mgr.checkRehashHeadroom(collection, 8, 8))

	err := mgr.checkRehashHeadroom(collection, 8, 9)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "needs 17 pchannels")
	assert.Contains(t, err.Error(), "rootCoord.dmlChannelNum",
		"the message must name what the operator can raise")
}

func TestRehashSourcesExcludeRetiredShards(t *testing.T) {
	// A shard an earlier split already retired owns no key range, so there is
	// nothing of its to rewrite and it must not become a source.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	collection := m.GetCollection(1)
	collection.VChannelNames = []string{"live", "fenced", "dropped", "creating"}
	collection.ShardInfos = map[string]*schemapb.CollectionShardInfo{
		"live":     {State: schemapb.ShardState_ShardNormal},
		"fenced":   {State: schemapb.ShardState_ShardSplitting},
		"dropped":  {State: schemapb.ShardState_ShardDropped},
		"creating": {State: schemapb.ShardState_ShardCreating},
	}

	assert.ElementsMatch(t, []string{"live", "creating"}, mgr.rehashSources(collection))
}

func TestStartRehashRefusesWhileAnotherSplitRunsOnTheCollection(t *testing.T) {
	// A rehash fences every shard. If another task already fenced one of them,
	// the rehash's fence on that shard returns the OTHER task's T_switch, and
	// both would believe they own it and wait to retire the same source.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	inflight := newHashTask(nil)
	inflight.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	mgr.tasks.Insert(inflight.GetTaskId(), inflight)

	assert.True(t, mgr.hasAnyActiveSplitOnCollection(1))

	// A finished task releases the collection again.
	inflight.State = datapb.SplitShardTaskState_SplitShardTaskDone
	mgr.tasks.Insert(inflight.GetTaskId(), inflight)
	assert.False(t, mgr.hasAnyActiveSplitOnCollection(1))
}

func TestHasAnyActiveSplitOnCollectionCoversNamespaceTasks(t *testing.T) {
	// The two task kinds contend for the same fences, so the gate must see both.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.tasks.Insert(100, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         100,
		CollectionId:   1,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: hashSrcVChannel}},
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
	})

	assert.True(t, mgr.hasAnyActiveSplitOnCollection(1))
	assert.False(t, mgr.hasAnyActiveSplitOnCollection(2))
}

func TestIsRehashTaskDistinguishesFromDoubling(t *testing.T) {
	assert.False(t, isRehashTask(newHashTask(nil)), "a doubling has one source")
	assert.True(t, isRehashTask(newMultiSourceHashTask()))
}

func TestHasActiveRehashOnCollectionIgnoresDoublings(t *testing.T) {
	// Two doublings on different shards are independent and must not block each
	// other; only a rehash claims the whole collection.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	doubling := newHashTask(nil)
	doubling.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	mgr.tasks.Insert(doubling.GetTaskId(), doubling)
	assert.False(t, mgr.hasActiveRehashOnCollection(1))

	rehash := newMultiSourceHashTask()
	rehash.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	mgr.tasks.Insert(rehash.GetTaskId(), rehash)
	assert.True(t, mgr.hasActiveRehashOnCollection(1))
	assert.False(t, mgr.hasActiveRehashOnCollection(2))
}

func TestDesiredShardNumReadsTheProperty(t *testing.T) {
	collection := &collectionInfo{ID: 1}
	num, err := desiredShardNum(collection)
	require.NoError(t, err)
	assert.Zero(t, num, "a collection that never asked for a count reconciles to nothing")

	collection.Properties = map[string]string{common.CollectionShardNum: "8"}
	num, err = desiredShardNum(collection)
	require.NoError(t, err)
	assert.Equal(t, int32(8), num)

	// Garbage must not be silently treated as "no request": rootcoord validates
	// on the way in, but a hand-edited meta must still fail loudly rather than
	// reconcile toward a number nobody asked for.
	collection.Properties[common.CollectionShardNum] = "eight"
	_, err = desiredShardNum(collection)
	assert.Error(t, err)

	collection.Properties[common.CollectionShardNum] = "1"
	_, err = desiredShardNum(collection)
	assert.Error(t, err)
}

func TestReconcileSkipsCollectionsAlreadyAtTheirDesiredCount(t *testing.T) {
	manualShardNumMode(t)
	// The reconciliation is declarative, so it must be a no-op once the count is
	// reached — otherwise every tick would start another rehash.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	collection := m.GetCollection(1)
	collection.VChannelNames = []string{"v0", "v1"}
	collection.RoutingModulus = 0
	collection.Properties = map[string]string{common.CollectionShardNum: "2"}

	// No catalog write is expected: the mock would fail the test on an
	// unexpected SaveSplitShardTask beyond the .Maybe() allowance, and the
	// task map stays empty.
	mgr.reconcileDesiredShardNum()

	count := 0
	mgr.tasks.Range(func(int64, *datapb.SplitShardTask) bool { count++; return true })
	assert.Zero(t, count)
}

func TestReconcileWaitsWhileAnotherSplitHoldsTheCollection(t *testing.T) {
	manualShardNumMode(t)
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	collection := m.GetCollection(1)
	collection.VChannelNames = []string{"v0", "v1"}
	collection.RoutingModulus = 0
	collection.Properties = map[string]string{common.CollectionShardNum: "4"}

	inflight := newHashTask(nil)
	inflight.State = datapb.SplitShardTaskState_SplitShardTaskRedistributing
	mgr.tasks.Insert(inflight.GetTaskId(), inflight)

	mgr.reconcileDesiredShardNum()

	// Still just the in-flight task: the request is not dropped, it waits for
	// the next tick.
	count := 0
	mgr.tasks.Range(func(int64, *datapb.SplitShardTask) bool { count++; return true })
	assert.Equal(t, 1, count)
}

func TestRehashHeadroomHonorsTheSystemShardLimit(t *testing.T) {
	// The same cap CreateCollection applies. datacoord checks it again rather
	// than trusting rootcoord's synchronous check, because the reconciliation
	// loop can also reach StartRehash from a property written before the limit
	// was lowered.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	collection := m.GetCollection(1)
	collection.VChannelNames = []string{"v0", "v1"}

	paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxShardNum.Key, "4")
	defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.MaxShardNum.Key)

	err := mgr.checkRehashHeadroom(collection, 2, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy.maxShardNum")

	assert.NoError(t, mgr.checkRehashHeadroom(collection, 2, 4))
}

func TestRehashHeadroomNamesTheKeyThatGovernsPChannels(t *testing.T) {
	// With pre-created topics, raising dmlChannelNum does nothing — the set is
	// common.topicNames and nothing even watches it. An error pointing at the
	// wrong key sends the operator to a setting with no effect.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	collection := m.GetCollection(1)
	collection.VChannelNames = []string{"v0", "v1"}

	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.PreCreatedTopicEnabled.Key, "true")
	defer pt.Reset(pt.CommonCfg.PreCreatedTopicEnabled.Key)
	pt.Save(pt.CommonCfg.TopicNames.Key, "t0,t1,t2")
	defer pt.Reset(pt.CommonCfg.TopicNames.Key)

	err := mgr.checkRehashHeadroom(collection, 2, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "common.topicNames")
	assert.NotContains(t, err.Error(), "dmlChannelNum")
}

func TestRehashFootprintGuard(t *testing.T) {
	// A rehash keeps a second copy of the whole collection until the rewrite is
	// adopted, so a collection the cluster can only hold once must be refused
	// before the first fence rather than discovered by an OOM mid-rewrite.
	params := paramtable.Get()
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	// two healthy segments, 3 GiB together
	gib := int64(1024 * 1024 * 1024)
	for i, size := range []int64{2 * gib, gib} {
		m.segments.SetSegment(int64(500+i), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: int64(500 + i), CollectionID: 1, InsertChannel: hashSrcVChannel,
				State: commonpb.SegmentState_Flushed, NumOfRows: 100,
				Stats: &datapb.Statistics{InsertBinlogSize: size},
			},
		})
	}
	collection := m.GetCollection(1)

	// off by default: no guess about the cluster is baked in.
	require.NoError(t, mgr.checkRehashFootprint(collection))

	params.Save(params.DataCoordCfg.ShardSplitRehashMaxCollectionSize.Key, "4")
	defer params.Reset(params.DataCoordCfg.ShardSplitRehashMaxCollectionSize.Key)
	assert.NoError(t, mgr.checkRehashFootprint(collection), "3 GiB fits under a 4 GiB bar")

	params.Save(params.DataCoordCfg.ShardSplitRehashMaxCollectionSize.Key, "2")
	err := mgr.checkRehashFootprint(collection)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "second copy")
	assert.Contains(t, err.Error(), "rehashMaxCollectionSize",
		"the message must name the knob the operator has to raise")
}

// --- projected shard load (§10.5.3) -----------------------------------------

func loadMeta(t *testing.T, collectionID int64, segments int, rowsEach int64, sizeEach int64) *meta {
	t.Helper()
	m := &meta{
		ctx:         context.Background(),
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		channelCPs:  newChannelCps(),
	}
	m.collections.Insert(collectionID, &collectionInfo{
		ID:     collectionID,
		Schema: &schemapb.CollectionSchema{Name: "shrink_load_test"},
	})
	for i := 0; i < segments; i++ {
		id := int64(9000 + i)
		m.segments.SetSegment(id, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           id,
				CollectionID: collectionID,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    rowsEach,
				// getSegmentSize reads the binlog stats, not a size field.
				Binlogs: []*datapb.FieldBinlog{{
					Binlogs: []*datapb.Binlog{{LogSize: sizeEach, MemorySize: sizeEach}},
				}},
			},
		})
	}
	return m
}

func TestCheckProjectedShardLoadOnlyGuardsShrinking(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitMaxShardRows.Key, "100")
	defer pt.Reset(pt.DataCoordCfg.ShardSplitMaxShardRows.Key)

	// 1000 rows total, far over the per-shard cap however it is divided.
	m := loadMeta(t, 55, 10, 100, 0)
	mgr, _ := newHashSplitTestManager(t, m)
	coll := m.GetCollection(55)

	// Growing divides the load further, so it can only move AWAY from the
	// threshold and is never refused on this ground.
	assert.NoError(t, mgr.checkProjectedShardLoad(coll, 2, 8))
	// Equal count is not a shrink either.
	assert.NoError(t, mgr.checkProjectedShardLoad(coll, 4, 4))
	// Shrinking concentrates it: 1000 rows over 2 shards is 500, over the 100 cap.
	err := mgr.checkProjectedShardLoad(coll, 8, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maxShardRows")
}

func TestCheckProjectedShardLoadAllowsAShrinkThatFits(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitMaxShardRows.Key, "1000")
	pt.Save(pt.DataCoordCfg.ShardSplitMaxShardSize.Key, "1024")
	defer func() {
		pt.Reset(pt.DataCoordCfg.ShardSplitMaxShardRows.Key)
		pt.Reset(pt.DataCoordCfg.ShardSplitMaxShardSize.Key)
	}()

	// 400 rows over 2 shards is 200 each, well under the cap.
	m := loadMeta(t, 56, 4, 100, 1024)
	mgr, _ := newHashSplitTestManager(t, m)
	assert.NoError(t, mgr.checkProjectedShardLoad(m.GetCollection(56), 8, 2))
}

func TestCheckProjectedShardLoadGuardsSize(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.ShardSplitMaxShardRows.Key, "0")
	pt.Save(pt.DataCoordCfg.ShardSplitMaxShardSize.Key, "1")
	defer func() {
		pt.Reset(pt.DataCoordCfg.ShardSplitMaxShardRows.Key)
		pt.Reset(pt.DataCoordCfg.ShardSplitMaxShardSize.Key)
	}()

	// 4 segments of 1 GiB = 4 GiB; over 2 shards that is 2 GiB each, over the
	// 1 GiB cap.
	gib := int64(1024 * 1024 * 1024)
	m := loadMeta(t, 57, 4, 1, gib)
	mgr, _ := newHashSplitTestManager(t, m)
	err := mgr.checkProjectedShardLoad(m.GetCollection(57), 8, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "maxShardSize")
}

// --- disk headroom (§16) ----------------------------------------------------

func TestCheckRehashDiskHeadroom(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	gibStr := func(g int) string { return fmt.Sprintf("%d", g*1024) } // MB

	// 4 segments of 1 GiB = 4 GiB in one collection.
	oneGiB := int64(1024 * 1024 * 1024)
	m := loadMeta(t, 61, 4, 1, oneGiB)
	mgr, _ := newHashSplitTestManager(t, m)
	coll := m.GetCollection(61)

	t.Run("off when disk protection is off", func(t *testing.T) {
		pt.Save(pt.QuotaConfig.DiskProtectionEnabled.Key, "false")
		defer pt.Reset(pt.QuotaConfig.DiskProtectionEnabled.Key)
		assert.NoError(t, mgr.checkRehashDiskHeadroom(coll))
	})

	t.Run("refuses when the second copy would pass the cluster quota", func(t *testing.T) {
		pt.Save(pt.QuotaConfig.DiskProtectionEnabled.Key, "true")
		// 6 GiB of room for 4 GiB used + 4 GiB more.
		pt.Save(pt.QuotaConfig.DiskQuota.Key, gibStr(6))
		pt.Save(pt.QuotaConfig.DiskQuotaPerCollection.Key, gibStr(100))
		defer func() {
			pt.Reset(pt.QuotaConfig.DiskProtectionEnabled.Key)
			pt.Reset(pt.QuotaConfig.DiskQuota.Key)
			pt.Reset(pt.QuotaConfig.DiskQuotaPerCollection.Key)
		}()
		err := mgr.checkRehashDiskHeadroom(coll)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "disk quota")
	})

	t.Run("allows when there is room for both copies", func(t *testing.T) {
		pt.Save(pt.QuotaConfig.DiskProtectionEnabled.Key, "true")
		pt.Save(pt.QuotaConfig.DiskQuota.Key, gibStr(100))
		pt.Save(pt.QuotaConfig.DiskQuotaPerCollection.Key, gibStr(100))
		defer func() {
			pt.Reset(pt.QuotaConfig.DiskProtectionEnabled.Key)
			pt.Reset(pt.QuotaConfig.DiskQuota.Key)
			pt.Reset(pt.QuotaConfig.DiskQuotaPerCollection.Key)
		}()
		assert.NoError(t, mgr.checkRehashDiskHeadroom(coll))
	})

	t.Run("refuses on the per-collection quota too", func(t *testing.T) {
		pt.Save(pt.QuotaConfig.DiskProtectionEnabled.Key, "true")
		pt.Save(pt.QuotaConfig.DiskQuota.Key, gibStr(1000))
		// 6 GiB per collection: one copy fits, two do not.
		pt.Save(pt.QuotaConfig.DiskQuotaPerCollection.Key, gibStr(6))
		defer func() {
			pt.Reset(pt.QuotaConfig.DiskProtectionEnabled.Key)
			pt.Reset(pt.QuotaConfig.DiskQuota.Key)
			pt.Reset(pt.QuotaConfig.DiskQuotaPerCollection.Key)
		}()
		err := mgr.checkRehashDiskHeadroom(coll)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "per-collection disk quota")
	})

	t.Run("empty collection is never refused", func(t *testing.T) {
		pt.Save(pt.QuotaConfig.DiskProtectionEnabled.Key, "true")
		pt.Save(pt.QuotaConfig.DiskQuota.Key, "1")
		defer func() {
			pt.Reset(pt.QuotaConfig.DiskProtectionEnabled.Key)
			pt.Reset(pt.QuotaConfig.DiskQuota.Key)
		}()
		empty := loadMeta(t, 62, 0, 0, 0)
		emptyMgr, _ := newHashSplitTestManager(t, empty)
		assert.NoError(t, emptyMgr.checkRehashDiskHeadroom(empty.GetCollection(62)))
	})
}

// The executor half of the per-collection mode. rootcoord accepting a hand-set
// count means nothing if the reconciler then refuses to act on it -- which is
// exactly what the first end-to-end run showed: the request was recorded and
// the collection never moved.
func TestManualShardNumAllowedIsPerCollection(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	manual := &collectionInfo{ID: 1, Properties: map[string]string{
		common.CollectionShardSplitMode: common.ShardSplitModeManual,
	}}
	auto := &collectionInfo{ID: 2}

	t.Run("feature off refuses everything", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "false")
		defer pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)
		assert.Error(t, manualShardNumAllowed(manual))
		assert.Error(t, manualShardNumAllowed(auto))
	})

	t.Run("trigger running: the collection's own mode decides", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "true")
		pt.Save(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "true")
		defer func() {
			pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)
			pt.Reset(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)
		}()
		assert.NoError(t, manualShardNumAllowed(manual),
			"a manual collection must be reconciled even while the trigger runs")
		err := manualShardNumAllowed(auto)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), common.CollectionShardSplitMode)
	})

	t.Run("kill switch off makes every collection manual", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.ShardSplitEnable.Key, "true")
		pt.Save(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key, "false")
		defer func() {
			pt.Reset(pt.DataCoordCfg.ShardSplitEnable.Key)
			pt.Reset(pt.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)
		}()
		assert.NoError(t, manualShardNumAllowed(manual))
		assert.NoError(t, manualShardNumAllowed(auto),
			"nothing sizes it automatically, so the count is the user's")
	})
}
