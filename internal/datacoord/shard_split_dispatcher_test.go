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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type fakePlanReader struct {
	byTrigger map[int64][]*datapb.CompactionTask
	calls     int // how many times the plans of a trigger were read
}

// GetCompactionTaskDigestsByTriggerID digests the plans through a real
// compactionTaskMeta, so the fields read are the production ones.
func (f *fakePlanReader) GetCompactionTaskDigestsByTriggerID(_ context.Context, triggerID int64) []compactionTaskDigest {
	f.calls++
	plans := make(map[int64]*datapb.CompactionTask, len(f.byTrigger[triggerID]))
	for _, plan := range f.byTrigger[triggerID] {
		plans[plan.GetPlanID()] = plan
	}
	csm := &compactionTaskMeta{compactionTasks: map[int64]map[int64]*datapb.CompactionTask{triggerID: plans}}
	digests := csm.GetCompactionTaskDigestsByTriggerID(triggerID)
	// Map order is random; keep the fixture's order.
	order := make(map[int64]int, len(f.byTrigger[triggerID]))
	for i, plan := range f.byTrigger[triggerID] {
		order[plan.GetPlanID()] = i
	}
	slices.SortFunc(digests, func(a, b compactionTaskDigest) int { return order[a.PlanID] - order[b.PlanID] })
	return digests
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

type fakeAllocator struct {
	next  int64
	idErr error
	nErr  error
}

func (a *fakeAllocator) AllocTimestamp(context.Context) (uint64, error) {
	a.next++
	return uint64(a.next), nil
}

func (a *fakeAllocator) AllocID(context.Context) (int64, error) {
	if a.idErr != nil {
		return 0, a.idErr
	}
	a.next++
	return a.next, nil
}

func (a *fakeAllocator) AllocN(n int64) (int64, int64, error) {
	if a.nErr != nil {
		return 0, 0, a.nErr
	}
	begin := a.next + 1
	a.next += n
	return begin, a.next + 1, nil
}

func newTestRewriteDispatcher(t *testing.T, sourceSegments []int64) (*inspectorRewriteDispatcher, *fakeInspector, *fakePlanReader, *meta) {
	m := newHashRewriteMeta(t, sourceSegments)
	inspector := &fakeInspector{}
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}
	return newInspectorRewriteDispatcher(context.Background(), m, inspector, reader, &fakeAllocator{next: 900}), inspector, reader, m
}

func TestDispatcherBuildsHashSplitPlan(t *testing.T) {
	d, inspector, _, _ := newTestRewriteDispatcher(t, []int64{301})
	task := newHashTask([]int64{301})

	planID, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.NotZero(t, planID)
	require.Len(t, inspector.enqueued, 1)

	plan := inspector.enqueued[0]
	assert.Equal(t, planID, plan.GetPlanID())
	assert.Equal(t, datapb.CompactionType_HashSplitCompaction, plan.GetType())
	assert.Equal(t, datapb.CompactionTaskState_pipelining, plan.GetState())
	// The plan runs on the source channel, where its input lives...
	assert.Equal(t, hashSrcVChannel, plan.GetChannel())
	assert.EqualValues(t, 10, plan.GetPartitionID())
	assert.Equal(t, []int64{301}, plan.GetInputSegments())
	// ...and carries the targets and the modulus their residues are taken
	// against, which is how the datanode routes every row.
	require.Len(t, plan.GetHashSplitTargets(), 2)
	assert.Equal(t, hashTgtA, plan.GetHashSplitTargets()[0].GetVchannel())
	assert.Equal(t, hashTgtB, plan.GetHashSplitTargets()[1].GetVchannel())
	assert.EqualValues(t, 4, plan.GetHashSplitModulus())
	// The split task id is the trigger id, so the task's plans are findable.
	assert.Equal(t, task.GetTaskId(), plan.GetTriggerID())
	// One output id per target, and an output size bound.
	assert.EqualValues(t, 2, plan.GetPreAllocatedSegmentIDs().GetEnd()-plan.GetPreAllocatedSegmentIDs().GetBegin())
	assert.Positive(t, plan.GetMaxSize())
	assert.NotNil(t, plan.GetSchema())
}

func TestDispatcherIsIdempotentPerSegment(t *testing.T) {
	d, inspector, reader, _ := newTestRewriteDispatcher(t, []int64{301})
	task := newHashTask([]int64{301})

	planID, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	reader.byTrigger[task.GetTaskId()] = []*datapb.CompactionTask{
		// Another compaction type and another segment's plan are not it.
		{PlanID: 1, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{301}, State: datapb.CompactionTaskState_executing},
		{PlanID: 2, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_HashSplitCompaction, InputSegments: []int64{302}, State: datapb.CompactionTaskState_executing},
		{PlanID: planID, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_HashSplitCompaction, InputSegments: []int64{301}, State: datapb.CompactionTaskState_executing},
	}

	again, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.Equal(t, planID, again)
	assert.Len(t, inspector.enqueued, 1, "no second rewrite of the same segment")

	// A failed plan is not live: once its backoff has passed, the segment is
	// dispatched again.
	reader.byTrigger[task.GetTaskId()][2].State = datapb.CompactionTaskState_failed
	reader.byTrigger[task.GetTaskId()][2].StartTime = time.Now().Add(-hashSplitRetryBackoffBase).Unix() - 1
	third, err := d.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.NotEqual(t, planID, third)
	assert.Len(t, inspector.enqueued, 2)
}

func TestDispatcherRefusesAPlanItCannotBuild(t *testing.T) {
	cases := []struct {
		name      string
		segmentID int64
		mutate    func(task *datapb.SplitShardTask, m *meta, alloc *fakeAllocator, inspector *fakeInspector)
		errIs     error
	}{
		{
			name:      "a segment absent from meta",
			segmentID: 999,
			mutate:    func(*datapb.SplitShardTask, *meta, *fakeAllocator, *fakeInspector) {},
			errIs:     merr.ErrSegmentNotFound,
		},
		{
			name:      "a segment of another channel",
			segmentID: 404,
			mutate: func(_ *datapb.SplitShardTask, m *meta, _ *fakeAllocator, _ *fakeInspector) {
				m.segments.SetSegment(404, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID: 404, CollectionID: splitMgrCollection, InsertChannel: splitMgrV3, State: commonpb.SegmentState_Flushed,
				}})
			},
			errIs: merr.ErrServiceInternal,
		},
		{
			name: "a collection datacoord does not hold",
			mutate: func(_ *datapb.SplitShardTask, m *meta, _ *fakeAllocator, _ *fakeInspector) {
				m.collections.Remove(splitMgrCollection)
			},
			errIs: merr.ErrCollectionNotFound,
		},
		{
			name: "a task without a modulus",
			mutate: func(task *datapb.SplitShardTask, _ *meta, _ *fakeAllocator, _ *fakeInspector) {
				task.RoutingModulus = 0
			},
			errIs: merr.ErrServiceInternal,
		},
		{
			name: "a task without its target vchannels",
			mutate: func(task *datapb.SplitShardTask, _ *meta, _ *fakeAllocator, _ *fakeInspector) {
				task.Targets[1].Vchannel = ""
			},
			errIs: merr.ErrServiceInternal,
		},
		{
			name: "no plan id",
			mutate: func(_ *datapb.SplitShardTask, _ *meta, alloc *fakeAllocator, _ *fakeInspector) {
				alloc.idErr = merr.WrapErrServiceUnavailable("allocator down")
			},
			errIs: merr.ErrServiceUnavailable,
		},
		{
			name: "no output ids",
			mutate: func(_ *datapb.SplitShardTask, _ *meta, alloc *fakeAllocator, _ *fakeInspector) {
				alloc.nErr = merr.WrapErrServiceUnavailable("allocator down")
			},
			errIs: merr.ErrServiceUnavailable,
		},
		{
			name: "the inspector refuses it",
			mutate: func(_ *datapb.SplitShardTask, _ *meta, _ *fakeAllocator, inspector *fakeInspector) {
				inspector.err = merr.WrapErrCompactionPlanConflict("segment is compacting")
			},
			errIs: merr.ErrCompactionPlanConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newHashRewriteMeta(t, []int64{301})
			inspector := &fakeInspector{}
			alloc := &fakeAllocator{next: 900}
			d := newInspectorRewriteDispatcher(context.Background(), m, inspector,
				&fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}, alloc)
			task := newHashTask([]int64{301})
			tc.mutate(task, m, alloc, inspector)
			segmentID := tc.segmentID
			if segmentID == 0 {
				segmentID = 301
			}
			_, err := d.DispatchHashSplit(task, segmentID)
			assert.ErrorIs(t, err, tc.errIs)
			assert.Empty(t, inspector.enqueued)
		})
	}
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
		{datapb.CompactionTaskState_statistic, false, true},
		{datapb.CompactionTaskState_indexing, false, true},
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
	d, _, reader, _ := newTestRewriteDispatcher(t, nil)
	reader.byTrigger[hashTaskID] = []*datapb.CompactionTask{
		{PlanID: 555, TriggerID: hashTaskID, State: datapb.CompactionTaskState_completed, InputSegments: []int64{301}},
		{PlanID: 556, TriggerID: hashTaskID, State: datapb.CompactionTaskState_executing, InputSegments: []int64{302}},
		// Cleaned by housekeeping: committed or failed is not decidable from
		// the state alone, but the record still names its input.
		{PlanID: 557, TriggerID: hashTaskID, State: datapb.CompactionTaskState_cleaned, InputSegments: []int64{303}},
	}
	scoped := d.forTask(hashTaskID)

	done, running, inputs := scoped.HashSplitPlanState(555)
	assert.True(t, done)
	assert.False(t, running)
	assert.Empty(t, inputs)

	done, running, inputs = scoped.HashSplitPlanState(556)
	assert.False(t, done)
	assert.True(t, running)
	assert.Empty(t, inputs)

	done, running, inputs = scoped.HashSplitPlanState(557)
	assert.False(t, done)
	assert.False(t, running)
	assert.Equal(t, []int64{303}, inputs)

	// Absent from meta: neither, and nothing to check against.
	done, running, inputs = scoped.HashSplitPlanState(999)
	assert.False(t, done)
	assert.False(t, running)
	assert.Empty(t, inputs)

	// Scoped to another task, the same plan is not found.
	done, running, _ = d.forTask(hashTaskID + 1).HashSplitPlanState(555)
	assert.False(t, done)
	assert.False(t, running)
}

// The rewrite drops rows the collection's TTL expired, the way every other
// compaction of the collection does.
func TestDispatcherCarriesTheCollectionTTL(t *testing.T) {
	t.Run("a collection TTL", func(t *testing.T) {
		d, inspector, _, m := newTestRewriteDispatcher(t, []int64{301})
		m.GetCollection(splitMgrCollection).Properties = map[string]string{common.CollectionTTLConfigKey: "3600"}
		_, err := d.DispatchHashSplit(newHashTask(nil), 301)
		require.NoError(t, err)
		require.Len(t, inspector.enqueued, 1)
		assert.Equal(t, time.Hour.Nanoseconds(), inspector.enqueued[0].GetCollectionTtl())
	})

	t.Run("no collection TTL", func(t *testing.T) {
		d, inspector, _, _ := newTestRewriteDispatcher(t, []int64{301})
		_, err := d.DispatchHashSplit(newHashTask(nil), 301)
		require.NoError(t, err)
		assert.EqualValues(t, -1, inspector.enqueued[0].GetCollectionTtl())
	})

	t.Run("an unparsable collection TTL", func(t *testing.T) {
		d, inspector, _, m := newTestRewriteDispatcher(t, []int64{301})
		m.GetCollection(splitMgrCollection).Properties = map[string]string{common.CollectionTTLConfigKey: "soon"}
		_, err := d.DispatchHashSplit(newHashTask(nil), 301)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.Empty(t, inspector.enqueued)
	})
}

// A rewrite that keeps failing -- a deterministic plan error, or an input too
// large to finish before the compaction timeout -- must not mint a new plan id,
// two segment ids and a persisted compaction task every round. The segment is
// retried after a backoff that doubles with every failed plan meta still
// holds for it, up to a ceiling.
func TestDispatcherBacksOffAFailingSegment(t *testing.T) {
	failedPlan := func(planID int64, state datapb.CompactionTaskState, endedAgo time.Duration) *datapb.CompactionTask {
		return &datapb.CompactionTask{
			PlanID: planID, TriggerID: hashTaskID, Type: datapb.CompactionType_HashSplitCompaction,
			InputSegments: []int64{301}, State: state,
			StartTime: time.Now().Add(-endedAgo).Add(-time.Hour).Unix(),
			EndTime:   time.Now().Add(-endedAgo).Unix(),
		}
	}
	dispatch := func(t *testing.T, plans ...*datapb.CompactionTask) (int64, *fakeInspector, *fakeAllocator) {
		m := newHashRewriteMeta(t, []int64{301})
		inspector := &fakeInspector{}
		alloc := &fakeAllocator{next: 900}
		reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{hashTaskID: plans}}
		d := newInspectorRewriteDispatcher(context.Background(), m, inspector, reader, alloc).forTask(hashTaskID)
		planID, err := d.DispatchHashSplit(newHashTask([]int64{301}), 301)
		require.NoError(t, err)
		return planID, inspector, alloc
	}

	for _, state := range []datapb.CompactionTaskState{
		datapb.CompactionTaskState_failed,
		datapb.CompactionTaskState_timeout,
		datapb.CompactionTaskState_cleaned,
	} {
		t.Run("a plan just "+state.String()+" defers the segment", func(t *testing.T) {
			planID, inspector, alloc := dispatch(t, failedPlan(1, state, 0))
			assert.Zero(t, planID, "deferred, not dispatched")
			assert.Empty(t, inspector.enqueued)
			assert.EqualValues(t, 900, alloc.next, "no plan id and no segment id is allocated")
		})
	}

	t.Run("past the backoff the segment is dispatched again", func(t *testing.T) {
		planID, inspector, _ := dispatch(t, failedPlan(1, datapb.CompactionTaskState_failed, hashSplitRetryBackoffBase+time.Second))
		assert.NotZero(t, planID)
		assert.Len(t, inspector.enqueued, 1)
	})

	t.Run("the backoff doubles with every failed plan", func(t *testing.T) {
		// Three failures: the wait is four times the base.
		ago := 3 * hashSplitRetryBackoffBase
		planID, _, _ := dispatch(t,
			failedPlan(1, datapb.CompactionTaskState_failed, time.Hour),
			failedPlan(2, datapb.CompactionTaskState_timeout, time.Hour),
			failedPlan(3, datapb.CompactionTaskState_failed, ago))
		assert.Zero(t, planID)

		planID, _, _ = dispatch(t,
			failedPlan(1, datapb.CompactionTaskState_failed, time.Hour),
			failedPlan(2, datapb.CompactionTaskState_timeout, time.Hour),
			failedPlan(3, datapb.CompactionTaskState_failed, 4*hashSplitRetryBackoffBase+time.Second))
		assert.NotZero(t, planID)
	})

	t.Run("the backoff is capped", func(t *testing.T) {
		plans := make([]*datapb.CompactionTask, 0, 64)
		for i := int64(1); i <= 64; i++ {
			plans = append(plans, failedPlan(i, datapb.CompactionTaskState_failed, hashSplitRetryBackoffMax+time.Second))
		}
		planID, _, _ := dispatch(t, plans...)
		assert.NotZero(t, planID, "a segment is never deferred longer than the ceiling")
	})

	t.Run("a live plan wins over the failed ones", func(t *testing.T) {
		live := failedPlan(9, datapb.CompactionTaskState_executing, 0)
		planID, inspector, _ := dispatch(t, failedPlan(1, datapb.CompactionTaskState_failed, 0), live)
		assert.EqualValues(t, 9, planID)
		assert.Empty(t, inspector.enqueued)
	})
}

// A split's plans all share its task id as their trigger id, so that entry of
// compaction meta grows with every plan the split ever dispatched, and each
// read of it used to deep-clone every plan -- schema included. A round reads
// it once, however many plans it harvests and segments it dispatches.
func TestRewriteRoundReadsTheSplitsPlansOnce(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitRewriteBatchSize.Key, "8")
	defer params.Reset(params.DataCoordCfg.ShardSplitRewriteBatchSize.Key)

	sources := []int64{101, 102, 103, 104, 105, 106, 107, 108, 109, 110}
	m := newHashRewriteMeta(t, sources)
	c := newRewriteCase(t, m, newHashTask(nil))
	inspector := &fakeInspector{}
	reader := &fakePlanReader{byTrigger: map[int64][]*datapb.CompactionTask{}}
	rewriter := newHashSplitRewriter(c.manager, newInspectorRewriteDispatcher(context.Background(), m, inspector, reader, &fakeAllocator{next: 900}))

	rewriter.redistribute(context.Background(), c.task())
	require.Len(t, inspector.enqueued, 8)
	assert.Equal(t, 1, reader.calls, "the first round reads the plans once for all 8 dispatches")

	// The plans are in flight: the next round harvests all 8 and dispatches
	// nothing new, still in one read.
	for _, plan := range inspector.enqueued {
		executing := proto.Clone(plan).(*datapb.CompactionTask)
		executing.State = datapb.CompactionTaskState_executing
		reader.byTrigger[hashTaskID] = append(reader.byTrigger[hashTaskID], executing)
		m.SetSegmentsCompacting(context.Background(), plan.GetInputSegments(), true)
	}
	reader.calls = 0
	rewriter.redistribute(context.Background(), c.task())
	assert.Equal(t, 1, reader.calls)
	assert.Len(t, c.task().GetDispatchedPlanIds(), 8)
}

// Within one round the plan just enqueued is in the round's read of the plans,
// so a second dispatch of the same segment still returns it.
func TestScopedDispatcherSeesThePlanItJustEnqueued(t *testing.T) {
	d, inspector, reader, _ := newTestRewriteDispatcher(t, []int64{301})
	scoped := d.forTask(hashTaskID)
	task := newHashTask([]int64{301})

	first, err := scoped.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	again, err := scoped.DispatchHashSplit(task, 301)
	require.NoError(t, err)
	assert.Equal(t, first, again)
	assert.Len(t, inspector.enqueued, 1)
	assert.Equal(t, 1, reader.calls)

	done, running, _ := scoped.HashSplitPlanState(first)
	assert.False(t, done)
	assert.True(t, running, "the enqueued plan reads as running for the rest of the round")
}
