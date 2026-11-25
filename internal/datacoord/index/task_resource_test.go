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

package index

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// fixtureIndexMeta adds an HNSW index on the fixture's vector field to mt.
func fixtureIndexMeta(t *testing.T, mt *meta.Meta) (*meta.Meta, *IndexMeta) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()
	im := createIndexMetaWithSegment(catalog, taskresource.FixtureCollectionID, taskresource.FixturePartitionID,
		taskresource.FixtureSegmentID, 4, 101, 5)
	mt.IndexCatalog = im
	return mt, im
}

func bigFixtureIndexTask(mt *meta.Meta, im *IndexMeta) *indexBuildTask {
	return NewIndexBuildTask(&model.SegmentIndex{
		CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: taskresource.BigFixtureRows,
	}, 1, mt, im, nil, nil, nil)
}

func TestTaskResource_Index(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(false))
	segIndex := &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}
	it := NewIndexBuildTask(segIndex, 1, mt, im, nil, nil, nil)
	// HNSW on a 128-dim float vector over 1000 rows with no binlogs: closed form.
	assert.Equal(t, taskresource.Index(1000*128*4, true), it.GetTaskResource())
	// Cached: the value the scheduler placed on is the value the request ships.
	assert.Equal(t, taskresource.Index(1000*128*4, true), it.GetTaskResource())

	// Segment gone: floor, not cached.
	orphan := NewIndexBuildTask(&model.SegmentIndex{CollectionID: 1, SegmentID: 999, IndexID: 4, BuildID: 6}, 1, mt, im, nil, nil, nil)
	assert.Equal(t, taskresource.Default(), orphan.GetTaskResource())
	assert.Equal(t, taskresource.Default(), orphan.GetTaskResource())
}

func TestTaskResource_IndexVarCharColumnGroup(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	idx := im.Indexes[1][4]
	idx.FieldID = 102
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "INVERTED"}}
	segment := mt.Segments.GetSegment(3)
	segment.Stats.LoadResource = &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
		{GroupId: 0, FieldIds: []int64{101, 102}, MemorySize: 1 << 30},
	}}
	// The shared group's vector bytes must not dominate this varchar build.
	want := taskresource.Index(taskresource.BigFixtureRows*(64+4), false)
	it := bigFixtureIndexTask(mt, im)
	assert.Equal(t, want, it.GetTaskResource())
	assert.Equal(t, want, it.GetTaskResource()) // Cached estimate keeps the same cap.
}

func TestTaskResource_IndexScalar(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(false))
	// Repoint the index at the varchar field with an inverted (non-vector) index.
	idx := im.Indexes[1][4]
	idx.FieldID = 102
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "INVERTED"}}

	it := NewIndexBuildTask(&model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, im, nil, nil, nil)
	assert.Equal(t, taskresource.Index(1000*64, false), it.GetTaskResource())
}

// TestTaskResource_IndexFMIndex: an FM-index build is priced on its modeled
// peak -- the text, its suffix array and the sampling structures held at once --
// which is the same peak its scalar slot is derived from. The generic factor
// on the field size would under-price it several times over.
func TestTaskResource_IndexFMIndex(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	// Repoint the index at the varchar field with an FM-index.
	idx := im.Indexes[1][4]
	idx.FieldID = 102
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}

	fieldSize := taskresource.EstimateFieldSize(mt.Segments.GetSegment(3), taskresource.FixtureSchema(), 102)
	assert.Positive(t, fieldSize)
	want := taskresource.FMIndex(EstimateFMIndexBuildPeakBytes(fieldSize, taskresource.BigFixtureRows, idx.IndexParams))
	assert.Equal(t, taskresource.ClampMemory(EstimateFMIndexBuildPeakBytes(fieldSize, taskresource.BigFixtureRows, idx.IndexParams)), want.Memory)

	it := bigFixtureIndexTask(mt, im)
	assert.Equal(t, want, it.GetTaskResource())
	assert.Greater(t, want.Memory, taskresource.Index(fieldSize, false).Memory,
		"the generic factor would under-price an FM-index build")
}

// TestTaskResource_IndexFMIndexSchemaCacheMiss: while the schema is missing an
// FM-index is still bounded by its own peak model, on the field's upper bound
// without a schema, because the generic factor on the whole segment falls
// below an FM-index build whose text is a large share of the segment.
func TestTaskResource_IndexFMIndexSchemaCacheMiss(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	idx := im.Indexes[1][4]
	idx.FieldID = 102
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}
	taskresource.DropFixtureSchema(mt)

	segment := mt.Segments.GetSegment(3)
	bound := taskresource.EstimateFieldSize(segment, nil, 102)
	assert.Positive(t, bound)
	want := taskresource.FMIndex(EstimateFMIndexBuildPeakBytes(bound, taskresource.BigFixtureRows, idx.IndexParams))

	it := bigFixtureIndexTask(mt, im)
	assert.Equal(t, want, it.GetTaskResource())
	assert.Greater(t, want.Memory, taskresource.Index(taskresource.EstimateSegmentSize(segment, nil), false).Memory,
		"the generic whole-segment fallback would under-price the FM-index build")

	// Once the schema arrives the exact FM-index price replaces the bound.
	taskresource.PutFixtureSchema(mt, taskresource.FixtureSchema())
	fieldSize := taskresource.EstimateFieldSize(segment, taskresource.FixtureSchema(), 102)
	assert.Equal(t, taskresource.FMIndex(EstimateFMIndexBuildPeakBytes(fieldSize, taskresource.BigFixtureRows, idx.IndexParams)),
		it.GetTaskResource())
}

// TestTaskResource_IndexSchemaCacheMiss is the case that must never be cached:
// a V3 segment (no per-field binlogs, Stats present) whose collection schema is
// not in the cache yet. Without a schema there is no way to tell the indexed
// field from the rest of the segment, so a cached answer here would freeze a
// whole-segment over-estimate on a one-field build for the task's lifetime.
//
// The answer is that whole-segment over-estimate, not the floor. The scheduler
// places the task on it, so it must bound the build from above.
func TestTaskResource_IndexSchemaCacheMiss(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	// The segment keeps its V3 shape (Binlogs empty, Stats non-zero); only the
	// collection is missing, exactly as it is before the cache is warmed.
	taskresource.DropFixtureSchema(mt)
	assert.Greater(t, mt.Segments.GetSegment(3).GetSegmentSize(), int64(0))
	assert.Empty(t, mt.Segments.GetSegment(3).GetBinlogs())

	priced := taskresource.Index(taskresource.BigFixtureRows*128*4, true)
	// The index type is unknown too while the schema is missing, so the bound
	// takes the scalar CPU request. Only memory decides whether a worker fits.
	bound := taskresource.Index(taskresource.BigFixtureSegmentSize, false)
	assert.NotEqual(t, taskresource.Default(), bound, "the bug this test guards must be observable")

	it := bigFixtureIndexTask(mt, im)
	assert.Equal(t, bound, it.GetTaskResource())
	assert.Equal(t, bound, it.GetTaskResource())
	assert.Greater(t, bound.Memory, priced.Memory, "the fallback must err high, never low")

	// Not cached: once the schema arrives the very next call reprices from meta,
	// which proves the closure ran again instead of serving a frozen value.
	taskresource.PutFixtureSchema(mt, taskresource.FixtureSchema())
	assert.Equal(t, priced, it.GetTaskResource())
}

// TestTaskResource_IndexEmptyIndexParams covers an index whose params have not
// been read back yet: there is no index type, so "is this a vector index" has no
// answer and a vector build would otherwise be frozen at the scalar CPU request.
// Note GetIndexType answers the invalidIndex sentinel here, not "".
func TestTaskResource_IndexEmptyIndexParams(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	idx := im.Indexes[1][4]
	idx.IndexParams = nil

	priced := taskresource.Index(taskresource.BigFixtureRows*128*4, true)
	misPriced := taskresource.Index(taskresource.BigFixtureRows*128*4, false)
	assert.NotEqual(t, taskresource.Default(), misPriced, "the bug this test guards must be observable")
	// The schema is known here, but the index type is not, so the price is the
	// whole segment at the scalar CPU request until the params are read back.
	bound := taskresource.Index(taskresource.BigFixtureSegmentSize, false)

	it := bigFixtureIndexTask(mt, im)
	assert.Equal(t, bound, it.GetTaskResource())
	assert.Equal(t, bound, it.GetTaskResource())
	assert.Greater(t, bound.Memory, priced.Memory, "the fallback must err high, never low")

	// Not cached: the params arriving repairs the price on the next call.
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
	assert.Equal(t, priced, it.GetTaskResource())
}

// TestTaskResource_IndexFieldNotInSchema is the other side of the same coin: the
// schema IS known and the field is genuinely not in it. That is a real answer,
// not a transient miss, so the conservative whole-segment price is cached.
func TestTaskResource_IndexFieldNotInSchema(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(false))
	idx := im.Indexes[1][4]
	idx.FieldID = 999

	it := NewIndexBuildTask(&model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, im, nil, nil, nil)
	assert.Equal(t, taskresource.Index(taskresource.FixtureSegmentSize, true), it.GetTaskResource())

	// Liveness is checked before the cache: a dropped target must not retain
	// the old whole-segment price.
	mt.Segments.DropSegment(3)
	assert.Equal(t, taskresource.Default(), it.GetTaskResource())
}

// TestTaskResource_IndexUnpriceableField covers a known schema whose field still
// sizes to nothing (an empty segment): the floor, and not cached.
func TestTaskResource_IndexUnpriceableField(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(false))
	taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) {
		seg.Stats = nil
		seg.NumOfRows = 0
	})

	it := NewIndexBuildTask(&model.SegmentIndex{CollectionID: 1, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, im, nil, nil, nil)
	assert.Equal(t, taskresource.Default(), it.GetTaskResource())
	assert.Equal(t, taskresource.Default(), it.GetTaskResource())
}

func TestTaskResource_DroppedIndexBypassesCachedPrice(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	it := bigFixtureIndexTask(mt, im)
	assert.Greater(t, it.GetTaskResource().Memory, taskresource.Default().Memory)
	im.Indexes[1][4].IsDeleted = true
	assert.Equal(t, taskresource.Default(), it.GetTaskResource())
	// A reconstructed task after a coordinator restart must agree too.
	assert.Equal(t, taskresource.Default(), bigFixtureIndexTask(mt, im).GetTaskResource())
}

// Use real index metadata/pricing and the running global scheduler, rather than
// mocking a dropped task as already priced at the floor.
func TestIndexScheduler_CoordinatorOnlyTasksDoNotConsumeWorkerMemory(t *testing.T) {
	paramtable.Init()
	for _, name := range []string{"FLAT", "BIN_FLAT", "small", "nullable", "dropped-segment", "dropped-index", "dropped-index-reload"} {
		t.Run(name, func(t *testing.T) {
			mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
			segIndex, ok := im.GetIndexJob(5)
			require.True(t, ok)
			segIndex.NumRows = taskresource.BigFixtureRows
			// The scheduler owns its task record; metadata keeps a separate copy,
			// as in the production inspector.
			im.SegmentBuildInfo.Add(model.CloneSegmentIndex(segIndex))
			handler := meta.NewNMockHandler(t)
			collectionInfo := mt.GetCollection(1)
			handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(collectionInfo, nil).Maybe()
			it := NewIndexBuildTask(segIndex, 128, mt, im, handler, nil, nil)
			const capacity = int64(64 << 20)
			require.Greater(t, it.GetTaskResource().Memory, capacity)
			wantState := commonpb.IndexState_Finished
			switch name {
			case "FLAT", "BIN_FLAT":
				im.Indexes[1][4].IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: name}}
			case "small":
				segIndex.NumRows = 1
				im.SegmentBuildInfo.Add(model.CloneSegmentIndex(segIndex))
			case "nullable":
				collectionInfo.Schema.Fields[1].Nullable = true
				taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) {
					seg.Stats.NullCounts = map[int64]int64{101: taskresource.BigFixtureRows}
				})
			case "dropped-segment":
				mt.Segments.DropSegment(3)
				wantState = commonpb.IndexState_Failed
			case "dropped-index", "dropped-index-reload":
				im.Indexes[1][4].IsDeleted = true
				wantState = commonpb.IndexState_Failed
				if name == "dropped-index-reload" {
					it = NewIndexBuildTask(segIndex, 128, mt, im, handler, nil, nil)
				}
			}
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
				1: {NodeID: 1, TotalMemory: capacity, AvailableMemory: capacity},
			}).Maybe()
			scheduler := globalTask.NewGlobalTaskScheduler(context.Background(), cluster)
			var dispatched atomic.Bool
			follower := globalTask.NewMockTask(t)
			follower.EXPECT().GetTaskID().Return(int64(6)).Maybe()
			follower.EXPECT().GetTaskType().Return(taskcommon.Compaction).Maybe()
			follower.EXPECT().GetTaskSlot().Return(int64(128)).Maybe()
			follower.EXPECT().GetTaskResource().Return(taskcommon.Resource{CPU: 1, Memory: capacity}).Maybe()
			follower.EXPECT().GetTaskState().RunAndReturn(func() taskcommon.State {
				if dispatched.Load() {
					return taskcommon.Finished
				}
				return taskcommon.Init
			}).Maybe()
			follower.EXPECT().SetTaskTime(mock.Anything, mock.Anything).Return().Maybe()
			follower.EXPECT().CreateTaskOnWorker(int64(1), cluster, mock.Anything).Run(func(int64, session.Cluster, taskcommon.Resource) { dispatched.Store(true) }).Once()
			scheduler.Enqueue(it)
			scheduler.Enqueue(follower)
			scheduler.Start()
			// Join scheduler callbacks before the test and mock expectations finish.
			defer scheduler.Stop()
			require.Eventually(t, func() bool {
				persisted, ok := im.GetIndexJob(5)
				return ok && persisted.IndexState == wantState && dispatched.Load()
			}, 5*time.Second, 10*time.Millisecond)
			cluster.AssertNotCalled(t, "CreateIndex", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestIndexCoordinatorCompletionDeadlineReachesCatalog(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
	job, _ := im.GetIndexJob(5)
	it := NewIndexBuildTask(job, 1, mt, im, nil, nil, nil)
	mt.Segments.DropSegment(it.SegmentID)
	require.True(t, it.CanCompleteOnCoordinator())
	catalog := catalogmocks.NewDataCoordCatalog(t)
	im.Catalog = catalog
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ []*model.SegmentIndex) error {
			_, deadline := ctx.Deadline()
			require.True(t, deadline)
			<-ctx.Done()
			return ctx.Err()
		}).Once()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	it.CompleteOnCoordinator(ctx)
	assert.Equal(t, taskcommon.Init, it.GetTaskState())
	assert.Zero(t, it.GetTaskVersion())
	persisted, ok := im.GetIndexJob(it.BuildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Unissued, persisted.IndexState)
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
	it.CompleteOnCoordinator(context.Background())
	assert.Equal(t, taskcommon.Failed, it.GetTaskState())
	assert.Zero(t, it.GetTaskVersion())
	// Metadata removed between classification and persistence must stay absent.
	require.Error(t, im.UpdateIndexStateWithContext(context.Background(), 999, commonpb.IndexState_Failed, "removed"))
}

func TestIndexCoordinatorClassificationUsesCachedMetadata(t *testing.T) {
	paramtable.Init()
	for _, name := range []string{"normal", "missing schema", "missing field", "array", "nullable absent", "small", "no train", "missing task"} {
		t.Run(name, func(t *testing.T) {
			mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
			segIndex, _ := im.GetIndexJob(5)
			segIndex.NumRows = taskresource.BigFixtureRows
			im.SegmentBuildInfo.Add(segIndex)
			handler := meta.NewNMockHandler(t) // No remote lookup is allowed.
			it := NewIndexBuildTask(segIndex, 1, mt, im, handler, nil, nil)
			want := false
			switch name {
			case "missing schema":
				taskresource.DropFixtureSchema(mt)
			case "missing field":
				im.Indexes[1][4].FieldID = 999
			case "array":
				mt.GetCollection(1).Schema.Fields[1].DataType = schemapb.DataType_ArrayOfVector
				segIndex.NumRows = 1
				im.SegmentBuildInfo.Add(segIndex)
			case "nullable absent":
				mt.GetCollection(1).Schema.Fields[1].Nullable = true
				want = true
			case "small":
				segIndex.NumRows = 1
				im.SegmentBuildInfo.Add(segIndex)
				want = true
			case "no train":
				im.Indexes[1][4].IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}}
				want = true
			case "missing task":
				it.BuildID = 999
				want = true
			}
			assert.Equal(t, want, it.CanCompleteOnCoordinator())
		})
	}
}

func TestIndexCoordinatorReclassificationNeedsWorker(t *testing.T) {
	paramtable.Init()
	for _, nullable := range []bool{false, true} {
		t.Run(fmt.Sprint(nullable), func(t *testing.T) {
			mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
			job, _ := im.GetIndexJob(5)
			job.NumRows = 50
			im.SegmentBuildInfo.Add(model.CloneSegmentIndex(job))
			key := Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.Key
			require.NoError(t, Params.Save(key, "100"))
			defer Params.Reset(key)
			collection := mt.GetCollection(job.CollectionID)
			collection.Schema.Fields[1].Nullable = nullable
			if nullable {
				taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) {
					seg.Stats.NullCounts = map[int64]int64{101: 20}
				})
			}
			handler := meta.NewNMockHandler(t)
			it := NewIndexBuildTask(job, 1, mt, im, handler, nil, nil)
			require.True(t, it.CanCompleteOnCoordinator())
			require.NoError(t, Params.Save(key, "10"))
			lookup := mockey.Mock((*meta.NMockHandler).GetCollection).Return(collection, nil).Build()
			defer lookup.UnPatch()
			assert.Equal(t, globalTask.LocalNeedsWorker, it.CompleteOnCoordinator(context.Background()))
			assert.Equal(t, taskcommon.Init, it.GetTaskState())
			assert.False(t, it.CanCompleteOnCoordinator(), "the new threshold requires worker placement")
			assert.Zero(t, it.GetTaskVersion())
			mt.Segments.DropSegment(it.SegmentID)
			assert.True(t, it.CanCompleteOnCoordinator(), "a later drop still retires without a worker")
		})
	}
}

func TestIndexPreparationCollectionFailureRetries(t *testing.T) {
	paramtable.Init()
	for _, rows := range []int64{1, taskresource.BigFixtureRows} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(true))
			job, _ := im.GetIndexJob(5)
			job.NumRows = rows
			im.SegmentBuildInfo.Add(model.CloneSegmentIndex(job))
			handler := meta.NewNMockHandler(t)
			it := NewIndexBuildTask(job, 1, mt, im, handler, nil, nil)
			lookup := mockey.Mock((*meta.NMockHandler).GetCollection).Return(nil, merr.ErrServiceUnavailable).Build()
			defer lookup.UnPatch()
			assert.Nil(t, it.prepareForWorker(context.Background()), "schema lookup failure must not prepare a worker build")
			assert.Equal(t, taskcommon.Init, it.GetTaskState(), "schema lookup failure must not fake-finish a small segment")
			persisted, _ := im.GetIndexJob(it.BuildID)
			assert.Equal(t, commonpb.IndexState_Unissued, persisted.IndexState)
			assert.Zero(t, it.GetTaskVersion())
		})
	}
}
