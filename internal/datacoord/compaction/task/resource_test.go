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

package task

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

// taskPrice unpacks the two-value GetTaskResource in assertions.
func taskPrice(res taskcommon.Resource, _ bool) taskcommon.Resource { return res }

// newTaskMeta is a compaction task meta whose catalog accepts every save, for
// tests that drive CreateTaskOnWorker.
func newTaskMeta(t *testing.T) *CompactionTaskMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	return NewCompactionTaskMetaOverCatalog(catalog)
}

func TestCompactionTaskResource_Mix(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	calls := 0
	mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, id int64) *meta.SegmentInfo {
		calls++
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, NumOfRows: 100, Stats: &datapb.Statistics{InsertBinlogSize: 100 * mib},
		}}
	})
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10, 11}},
		nil, mt, nil, index.NewTestVersionManager())
	// Streamed input: the sum of both segments, well under one output segment.
	assert.Equal(t, taskresource.MixCompaction(200*mib, 0), taskPrice(task.GetTaskResource()))
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 200 * mib}, taskPrice(task.GetTaskResource()))
	assert.Equal(t, 2, calls) // cached after the first walk
}

func TestCompactionTaskResource_MixBoundedByOutput(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, id int64) *meta.SegmentInfo {
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, NumOfRows: 100, Stats: &datapb.Statistics{InsertBinlogSize: 900 * mib},
		}}
	})
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10, 11, 12}},
		nil, mt, nil, index.NewTestVersionManager())
	// 2700MiB of input streams through one 1024MiB output segment at a time.
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 1024 * mib}, taskPrice(task.GetTaskResource()))
}

// TestCompactionTaskResource_MixDeleteMaps: the delete maps are not streamed,
// so they come on top of the streaming cap. On the merge-sort path every
// input's map lives until the merge ends and they add up; mergeSplit holds one
// input's map at a time, so only the largest counts.
func TestCompactionTaskResource_MixDeleteMaps(t *testing.T) {
	paramtable.Init()
	const deletes = int64(3_000_000)
	perSegment := taskresource.DeleteMapBytes(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		Stats: &datapb.Statistics{DeleteNumRows: deletes},
	}}, schemapb.DataType_Int64)
	assert.Positive(t, perSegment)
	maxSegment := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * mib

	segments := func(sorted, sortedByNamespace bool) *MockCompactionMeta {
		mt := NewMockCompactionMeta(t)
		mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, id int64) *meta.SegmentInfo {
			// Segment 10 has twice the deletes of the others.
			n := deletes
			if id == 10 {
				n = 2 * deletes
			}
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: id, NumOfRows: 100, IsSorted: sorted, IsSortedByNamespace: sortedByNamespace,
				Stats: &datapb.Statistics{InsertBinlogSize: 900 * mib, DeleteNumRows: n},
			}}
		}).Maybe()
		return mt
	}
	inputs := []int64{10, 11, 12}
	mix := func(mt *MockCompactionMeta, schema *schemapb.CollectionSchema, ids []int64) taskcommon.Resource {
		task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: ids, Schema: schema},
			nil, mt, nil, index.NewTestVersionManager())
		return taskPrice(task.GetTaskResource())
	}

	paramtable.Get().Save(paramtable.Get().DataNodeCfg.UseMergeSort.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.UseMergeSort.Key)

	// Merge sort over sorted inputs: 4 x perSegment of maps on top of one output segment.
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: maxSegment + 4*perSegment}, mix(segments(true, false), nil, inputs))
	// Unsorted inputs take mergeSplit: only the largest map, segment 10's.
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: maxSegment + 2*perSegment}, mix(segments(false, false), nil, inputs))
	// A namespace collection merges on IsSortedByNamespace, not IsSorted.
	namespace := &schemapb.CollectionSchema{EnableNamespace: true}
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: maxSegment + 2*perSegment}, mix(segments(true, false), namespace, inputs))
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: maxSegment + 4*perSegment}, mix(segments(false, true), namespace, inputs))

	// More inputs than MaxSegmentMergeSort: mergeSplit.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.Key, "2")
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: maxSegment + 2*perSegment}, mix(segments(true, false), nil, inputs))
	paramtable.Get().Reset(paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.Key)

	// Merge sort disabled: mergeSplit.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.UseMergeSort.Key, "false")
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: maxSegment + 2*perSegment}, mix(segments(true, false), nil, inputs))
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.UseMergeSort.Key, "true")

	// A VarChar primary key: its maps also keep the strings.
	varchar := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar},
	}}
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 10, NumOfRows: 100, IsSorted: true,
		Stats: &datapb.Statistics{InsertBinlogSize: 100 * mib, DeleteNumRows: deletes, DeltaBinlogSize: 64 * mib},
	}})
	// The segment size counts the deltalog bytes too (GetSegmentSize).
	want := (100+64)*mib + taskresource.DeleteMapBytes(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		Stats: &datapb.Statistics{DeleteNumRows: deletes, DeltaBinlogSize: 64 * mib},
	}}, schemapb.DataType_VarChar)
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: want}, mix(mt, varchar, []int64{10}))
}

func TestCompactionTaskResource_MixSegmentMissing(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil).Twice()
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10}},
		nil, mt, nil, index.NewTestVersionManager())
	// Not resolvable: floor, and NOT cached so the next round retries.
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
}

func TestCompactionTaskResource_Sort(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	calls := 0
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).RunAndReturn(func(ctx context.Context, id int64) *meta.SegmentInfo {
		calls++
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 10, NumOfRows: 100,
			Stats: &datapb.Statistics{InsertBinlogSize: 3 * gib},
		}}
	})
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, mt, nil, index.NewTestVersionManager())
	assert.Equal(t, taskresource.Stats(3*gib), taskPrice(task.GetTaskResource()))
	// Cached: the second call does not walk meta again.
	assert.Equal(t, taskresource.Stats(3*gib), taskPrice(task.GetTaskResource()))
	assert.Equal(t, 1, calls)
}

func TestCompactionTaskResource_SortSegmentMissing(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil).Twice()
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, mt, nil, index.NewTestVersionManager())
	// Not resolvable: floor, and NOT cached so the next round retries.
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
}

// TestCompactionTaskResource_SortUnsizedSegment covers a sort compaction whose
// input segment is present but carries no size at all: still the floor, still
// not cached.
func TestCompactionTaskResource_SortUnsizedSegment(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).
		Return(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10}}).Twice()
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, mt, nil, index.NewTestVersionManager())
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
}

// TestCompactionTaskResource_SortNoInput covers the malformed-task shape.
func TestCompactionTaskResource_SortNoInput(t *testing.T) {
	paramtable.Init()
	task := NewMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction},
		nil, NewMockCompactionMeta(t), nil, index.NewTestVersionManager())
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
}

func TestCompactionTaskResource_L0(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 10, Stats: &datapb.Statistics{DeltaBinlogSize: 300 * mib},
	}}).Once()
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(11)).Return(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 11, Stats: &datapb.Statistics{DeltaBinlogSize: 200 * mib},
	}}).Once()
	// The targets the plan will select: their PK stats logs are loaded in full.
	mt.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.SegmentInfo{
		{SegmentInfo: &datapb.SegmentInfo{ID: 20, Level: datapb.SegmentLevel_L1, Stats: &datapb.Statistics{StatsBinlogSize: 40 * mib}}},
		{SegmentInfo: &datapb.SegmentInfo{ID: 21, Level: datapb.SegmentLevel_L1, Stats: &datapb.Statistics{StatsBinlogSize: 60 * mib}}},
	}).Once()
	task := NewL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10, 11}}, nil, mt, nil)
	assert.Equal(t, taskresource.L0Compaction(500*mib, 100*mib), taskPrice(task.GetTaskResource()))
	assert.Equal(t, taskresource.L0Compaction(500*mib, 100*mib), taskPrice(task.GetTaskResource())) // cached
	assert.Greater(t, taskPrice(task.GetTaskResource()).Memory, taskresource.L0Compaction(500*mib, 0).Memory)
}

// A Sealed target cannot be dispatched yet and its final flush still adds
// stats logs, so a price that includes one is not cached: the round that
// dispatches the task prices the flushed target the plan will carry.
func TestCompactionTaskResource_L0SealedTargetNotCached(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(&meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 10, Stats: &datapb.Statistics{DeltaBinlogSize: 300 * mib},
	}}).Times(2)
	target := func(state commonpb.SegmentState, stats int64) []*meta.SegmentInfo {
		return []*meta.SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{
			ID: 20, Level: datapb.SegmentLevel_L1, State: state, Stats: &datapb.Statistics{StatsBinlogSize: stats},
		}}}
	}
	mt.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(target(commonpb.SegmentState_Sealed, 10*mib)).Once()
	mt.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(target(commonpb.SegmentState_Flushed, 80*mib)).Once()
	task := NewL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10}}, nil, mt, nil)

	res, exact := task.GetTaskResource()
	assert.False(t, exact)
	assert.Equal(t, taskresource.L0Compaction(300*mib, 10*mib), res)

	// The target flushed: the full stats logs are priced, and now cached.
	res, exact = task.GetTaskResource()
	assert.True(t, exact)
	assert.Equal(t, taskresource.L0Compaction(300*mib, 80*mib), res)
	assert.Equal(t, res, taskPrice(task.GetTaskResource())) // cached: Times(2)/Once hold
}

func TestCompactionTaskResource_L0SegmentMissing(t *testing.T) {
	paramtable.Init()
	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil)
	task := NewL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10}}, nil, mt, nil)
	assert.Equal(t, taskresource.Default(), taskPrice(task.GetTaskResource()))
}

func TestCompactionTaskResource_ClusteringAndBump(t *testing.T) {
	paramtable.Init()
	segmentOf := func(size int64) func(ctx context.Context, id int64) *meta.SegmentInfo {
		return func(ctx context.Context, id int64) *meta.SegmentInfo {
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, NumOfRows: 100, Stats: &datapb.Statistics{InsertBinlogSize: size}}}
		}
	}

	mt := NewMockCompactionMeta(t)
	mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(segmentOf(4 * gib)).Times(3)
	clustering := NewClusteringCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_ClusteringCompaction, InputSegments: []int64{10, 11, 12}},
		nil, mt, nil, nil, nil, nil, nil, nil, nil, index.NewTestVersionManager())
	// Its input, bounded by nothing else.
	assert.Equal(t, taskresource.ClusteringCompaction(12*gib), taskPrice(clustering.GetTaskResource()))
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 12 * gib}, taskPrice(clustering.GetTaskResource()))
	assert.Equal(t, taskPrice(clustering.GetTaskResource()), taskPrice(clustering.GetTaskResource())) // cached: Times(3) holds

	missing := NewMockCompactionMeta(t)
	missing.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).Return(nil).Twice()
	orphan := NewClusteringCompactionTask(&datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_ClusteringCompaction, InputSegments: []int64{10}},
		nil, missing, nil, nil, nil, nil, nil, nil, nil, index.NewTestVersionManager())
	assert.Equal(t, taskresource.Default(), taskPrice(orphan.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskPrice(orphan.GetTaskResource())) // not cached

	bumpMeta := NewMockCompactionMeta(t)
	bumpMeta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).RunAndReturn(segmentOf(300 * mib)).Once()
	bump := NewBumpSchemaVersionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_BumpSchemaVersionCompaction, InputSegments: []int64{10}},
		nil, bumpMeta, nil, index.NewTestVersionManager())
	// One segment streamed through the mix writer: priced by that segment.
	assert.Equal(t, taskresource.MixCompaction(300*mib, 0), taskPrice(bump.GetTaskResource()))
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 300 * mib}, taskPrice(bump.GetTaskResource()))

	bumpMissing := NewMockCompactionMeta(t)
	bumpMissing.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil).Twice()
	bumpOrphan := NewBumpSchemaVersionTask(&datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_BumpSchemaVersionCompaction, InputSegments: []int64{10}},
		nil, bumpMissing, nil, index.NewTestVersionManager())
	assert.Equal(t, taskresource.Default(), taskPrice(bumpOrphan.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskPrice(bumpOrphan.GetTaskResource()))
}

// TestCompactionTaskResource_RequestCarriesEstimate proves the dispatch the
// worker receives ships exactly what GetTaskResource priced, for every
// compaction family whose plan is buildable without a full meta. The estimate
// travels in the CreateTask properties, not in the plan, so the assertion is on
// the argument handed to session.Cluster.CreateCompaction.
func TestCompactionTaskResource_RequestCarriesEstimate(t *testing.T) {
	paramtable.Init()

	newSegment := func(segID int64) *meta.SegmentInfo {
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: segID, NumOfRows: 10, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed,
			Stats: &datapb.Statistics{InsertBinlogSize: 1024, DeltaBinlogSize: 1024},
		}}
	}
	newAlloc := func(t *testing.T) allocator.Allocator {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Maybe()
		return alloc
	}

	// dispatched runs the task's real CreateTaskOnWorker against a mock cluster
	// and returns the resource it handed to CreateCompaction.
	dispatched := func(t *testing.T, task CompactionTask) taskcommon.Resource {
		var placed taskcommon.Resource
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ int64, _ *datapb.CompactionPlan, _ int64, resource taskcommon.Resource) error {
				placed = resource
				return nil
			})
		task.CreateTaskOnWorker(1, cluster)
		return placed
	}

	t.Run("mix", func(t *testing.T) {
		mt := NewMockCompactionMeta(t)
		mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *meta.SegmentInfo { return newSegment(segID) })
		task := NewMixCompactionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10},
			Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), mt, newTaskMeta(t), index.NewTestVersionManager())
		assert.Equal(t, taskPrice(task.GetTaskResource()), dispatched(t, task))
	})

	t.Run("sort", func(t *testing.T) {
		mt := NewMockCompactionMeta(t)
		mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *meta.SegmentInfo { return newSegment(segID) })
		task := NewMixCompactionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10},
			Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), mt, newTaskMeta(t), index.NewTestVersionManager())
		assert.Equal(t, taskresource.Stats(2048), taskPrice(task.GetTaskResource()))
		assert.Equal(t, taskPrice(task.GetTaskResource()), dispatched(t, task))
	})

	t.Run("l0", func(t *testing.T) {
		mt := NewMockCompactionMeta(t)
		mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *meta.SegmentInfo { return newSegment(segID) })
		// A target L1 segment, so the plan is a real compaction rather than the
		// fast-finish path that never reaches the worker.
		mt.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*meta.SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{
				ID: 11, Level: datapb.SegmentLevel_L1, InsertChannel: "ch-1",
			}}}).Maybe()
		task := NewL0CompactionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10},
			Channel: "ch-1", Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), mt, newTaskMeta(t))
		assert.Equal(t, taskPrice(task.GetTaskResource()), dispatched(t, task))
	})

	t.Run("bumpSchemaVersion", func(t *testing.T) {
		mt := NewMockCompactionMeta(t)
		mt.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *meta.SegmentInfo { return newSegment(segID) })
		task := NewBumpSchemaVersionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_BumpSchemaVersionCompaction, InputSegments: []int64{10},
			Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), mt, newTaskMeta(t), index.NewTestVersionManager())
		assert.Equal(t, taskPrice(task.GetTaskResource()), dispatched(t, task))
	})
}
