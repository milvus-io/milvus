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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	assert.Equal(t, taskresource.Index(1000*128*4, true), taskresource.Price(it.GetTaskResource()))
	// Cached: the value the scheduler placed on is the value the request ships.
	assert.Equal(t, taskresource.Index(1000*128*4, true), taskresource.Price(it.GetTaskResource()))

	// Segment gone: floor, not cached.
	orphan := NewIndexBuildTask(&model.SegmentIndex{CollectionID: 1, SegmentID: 999, IndexID: 4, BuildID: 6}, 1, mt, im, nil, nil, nil)
	assert.Equal(t, taskresource.Default(), taskresource.Price(orphan.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskresource.Price(orphan.GetTaskResource()))
}

// TestTaskResource_IndexScalar covers a non-vector index: CPU falls back to the
// default and the field bytes are what the segment holds beyond its
// fixed-width fields -- exactly 64 bytes per row here, the varchar being the
// only variable-width field.
func TestTaskResource_IndexScalar(t *testing.T) {
	paramtable.Init()
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(false))
	// Repoint the index at the varchar field with an inverted (non-vector) index.
	idx := im.Indexes[1][4]
	idx.FieldID = 102
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "INVERTED"}}

	it := NewIndexBuildTask(&model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, im, nil, nil, nil)
	assert.Equal(t, taskresource.Index(1000*64, false), taskresource.Price(it.GetTaskResource()))
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
	assert.Equal(t, want, taskresource.Price(it.GetTaskResource()))
	assert.True(t, taskresource.Resolved(it.GetTaskResource()))
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
	assert.Equal(t, want, taskresource.Price(it.GetTaskResource()))
	assert.False(t, taskresource.Resolved(it.GetTaskResource()), "a bound without a schema is not cached")
	assert.Greater(t, want.Memory, taskresource.Index(taskresource.EstimateSegmentSize(segment, nil), false).Memory,
		"the generic whole-segment fallback would under-price the FM-index build")

	// Once the schema arrives the exact FM-index price replaces the bound.
	taskresource.PutFixtureSchema(mt, taskresource.FixtureSchema())
	fieldSize := taskresource.EstimateFieldSize(segment, taskresource.FixtureSchema(), 102)
	assert.Equal(t, taskresource.FMIndex(EstimateFMIndexBuildPeakBytes(fieldSize, taskresource.BigFixtureRows, idx.IndexParams)),
		taskresource.Price(it.GetTaskResource()))
	assert.True(t, taskresource.Resolved(it.GetTaskResource()))
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
	assert.Equal(t, bound, taskresource.Price(it.GetTaskResource()))
	assert.False(t, taskresource.Resolved(it.GetTaskResource()), "the scheduler must be told the price is not exact")
	assert.Equal(t, bound, taskresource.Price(it.GetTaskResource()))
	assert.Greater(t, bound.Memory, priced.Memory, "the fallback must err high, never low")

	// Not cached: once the schema arrives the very next call reprices from meta,
	// which proves the closure ran again instead of serving a frozen value.
	taskresource.PutFixtureSchema(mt, taskresource.FixtureSchema())
	assert.Equal(t, priced, taskresource.Price(it.GetTaskResource()))
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
	assert.Equal(t, bound, taskresource.Price(it.GetTaskResource()))
	assert.Equal(t, bound, taskresource.Price(it.GetTaskResource()))
	assert.Greater(t, bound.Memory, priced.Memory, "the fallback must err high, never low")

	// Not cached: the params arriving repairs the price on the next call.
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
	assert.Equal(t, priced, taskresource.Price(it.GetTaskResource()))
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
	assert.Equal(t, taskresource.Index(taskresource.FixtureSegmentSize, true), taskresource.Price(it.GetTaskResource()))

	// Cached: dropping the segment does not change the answer, and it is the
	// whole-segment price rather than the floor.
	mt.Segments.DropSegment(3)
	assert.Equal(t, taskresource.Index(taskresource.FixtureSegmentSize, true), taskresource.Price(it.GetTaskResource()))
	assert.NotEqual(t, taskresource.Default().CPU, taskresource.Price(it.GetTaskResource()).CPU)
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
	assert.Equal(t, taskresource.Default(), taskresource.Price(it.GetTaskResource()))
	assert.Equal(t, taskresource.Default(), taskresource.Price(it.GetTaskResource()))
}
