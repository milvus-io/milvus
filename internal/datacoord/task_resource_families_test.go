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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// familySegmentSize is the segment size the fixture below reports through
// Stats: 1000 rows of (int64 pk + 128-dim float vector + 64-byte varchar).
const familySegmentSize = int64(1000 * (8 + 512 + 64))

// familyMeta builds a meta with one collection (schema from testResourceSchema),
// one segment (V3 shape: no binlogs, Stats present) and one HNSW index on the
// vector field, which is the shape PR #52561's review found unpriced.
func familyMeta(t *testing.T) *meta {
	const collID, partID, segID, indexID, fieldID, buildID = int64(1), int64(2), int64(3), int64(4), int64(101), int64(5)
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()
	im := createIndexMetaWithSegment(catalog, collID, partID, segID, indexID, fieldID, buildID)
	mt := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments: &SegmentsInfo{segments: map[int64]*SegmentInfo{
			segID: {SegmentInfo: &datapb.SegmentInfo{
				ID: segID, CollectionID: collID, PartitionID: partID,
				NumOfRows: 1000, State: commonpb.SegmentState_Flushed, StorageVersion: 3, ManifestPath: "m",
				Stats: &datapb.Statistics{InsertBinlogSize: familySegmentSize},
			}},
		}},
		indexMeta: im,
	}
	mt.collections.Insert(collID, &collectionInfo{ID: collID, Schema: testResourceSchema()})
	return mt
}

func TestTaskResource_Index(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	segIndex := &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}
	it := newIndexBuildTask(segIndex, 1, mt, nil, nil, nil)
	// HNSW on a 128-dim float vector over 1000 rows with no binlogs: closed form.
	assert.Equal(t, indexTaskResource(1000*128*4, true), it.GetTaskResource())
	// Cached: the value the scheduler placed on is the value the request ships.
	assert.Equal(t, indexTaskResource(1000*128*4, true), it.GetTaskResource())

	// Segment gone: floor, not cached.
	orphan := newIndexBuildTask(&model.SegmentIndex{CollectionID: 1, SegmentID: 999, IndexID: 4, BuildID: 6}, 1, mt, nil, nil, nil)
	assert.Equal(t, defaultTaskResource(), orphan.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), orphan.GetTaskResource())
}

// TestTaskResource_IndexScalar covers a non-vector index: CPU falls back to the
// default and the field bytes are apportioned out of the segment size.
func TestTaskResource_IndexScalar(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	// Repoint the index at the varchar field with an inverted (non-vector) index.
	idx := mt.indexMeta.indexes[1][4]
	idx.FieldID = 102
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "INVERTED"}}

	it := newIndexBuildTask(&model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, nil, nil, nil)
	strBytes := fieldBytesPerRow(typeutil.GetFieldByID(testResourceSchema(), 102))
	perRecord, err := typeutilEstimateSizePerRecord(testResourceSchema())
	assert.NoError(t, err)
	assert.Equal(t, indexTaskResource(familySegmentSize*strBytes/perRecord, false), it.GetTaskResource())
}

// bigFamilyMeta is familyMeta scaled up so that every price in play clears the
// minTaskMemory floor. Below the floor a wrong price and the floor collapse to
// the same Resource, and a test asserting "the floor" would pass on a frozen
// over-estimate; at this size they are distinguishable.
const bigFamilyRows = int64(1000000)

func bigFamilyMeta(t *testing.T) *meta {
	mt := familyMeta(t)
	seg := mt.segments.segments[3]
	seg.NumOfRows = bigFamilyRows
	seg.Stats = &datapb.Statistics{InsertBinlogSize: bigFamilyRows * (8 + 512 + 64)}
	return mt
}

func bigFamilyIndexTask(mt *meta) *indexBuildTask {
	return newIndexBuildTask(&model.SegmentIndex{
		CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: bigFamilyRows,
	}, 1, mt, nil, nil, nil)
}

// TestTaskResource_IndexSchemaCacheMiss is the case that must never be cached:
// a V3 segment (no per-field binlogs, Stats present) whose collection schema is
// not in the cache yet. Without a schema there is no way to tell the indexed
// field from the rest of the segment, so a cached answer here would freeze a
// whole-segment over-estimate on a one-field build for the task's lifetime.
func TestTaskResource_IndexSchemaCacheMiss(t *testing.T) {
	paramtable.Init()
	mt := bigFamilyMeta(t)
	// The segment keeps its V3 shape (Binlogs empty, Stats non-zero); only the
	// collection is missing, exactly as it is before the cache is warmed.
	mt.collections = typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	assert.Greater(t, mt.segments.segments[3].getSegmentSize(), int64(0))
	assert.Empty(t, mt.segments.segments[3].GetBinlogs())

	priced := indexTaskResource(bigFamilyRows*128*4, true)
	overEstimate := indexTaskResource(bigFamilyRows*(8+512+64), true)
	assert.NotEqual(t, defaultTaskResource(), overEstimate, "the bug this test guards must be observable")

	it := bigFamilyIndexTask(mt)
	assert.Equal(t, defaultTaskResource(), it.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), it.GetTaskResource())

	// Not cached: once the schema arrives the very next call reprices from meta,
	// which proves the closure ran again instead of serving a frozen value.
	mt.collections.Insert(1, &collectionInfo{ID: 1, Schema: testResourceSchema()})
	assert.Equal(t, priced, it.GetTaskResource())
}

// TestTaskResource_IndexEmptyIndexParams covers an index whose params have not
// been read back yet: there is no index type, so "is this a vector index" has no
// answer and a vector build would otherwise be frozen at the scalar CPU request.
// Note GetIndexType answers the invalidIndex sentinel here, not "".
func TestTaskResource_IndexEmptyIndexParams(t *testing.T) {
	paramtable.Init()
	mt := bigFamilyMeta(t)
	idx := mt.indexMeta.indexes[1][4]
	idx.IndexParams = nil

	priced := indexTaskResource(bigFamilyRows*128*4, true)
	misPriced := indexTaskResource(bigFamilyRows*128*4, false)
	assert.NotEqual(t, defaultTaskResource(), misPriced, "the bug this test guards must be observable")

	it := bigFamilyIndexTask(mt)
	assert.Equal(t, defaultTaskResource(), it.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), it.GetTaskResource())

	// Not cached: the params arriving repairs the price on the next call.
	idx.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
	assert.Equal(t, priced, it.GetTaskResource())
}

// TestTaskResource_IndexFieldNotInSchema is the other side of the same coin: the
// schema IS known and the field is genuinely not in it. That is a real answer,
// not a transient miss, so the conservative whole-segment price is cached.
func TestTaskResource_IndexFieldNotInSchema(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	idx := mt.indexMeta.indexes[1][4]
	idx.FieldID = 999

	it := newIndexBuildTask(&model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, nil, nil, nil)
	assert.Equal(t, indexTaskResource(familySegmentSize, true), it.GetTaskResource())

	// Cached: dropping the segment does not change the answer, and it is the
	// whole-segment price rather than the floor.
	delete(mt.segments.segments, 3)
	assert.Equal(t, indexTaskResource(familySegmentSize, true), it.GetTaskResource())
	assert.NotEqual(t, defaultTaskResource().CPU, it.GetTaskResource().CPU)
}

// TestTaskResource_IndexUnpriceableField covers a known schema whose field still
// sizes to nothing (an empty segment): the floor, and not cached.
func TestTaskResource_IndexUnpriceableField(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	mt.segments.segments[3].Stats = nil
	mt.segments.segments[3].NumOfRows = 0

	it := newIndexBuildTask(&model.SegmentIndex{CollectionID: 1, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}, 1, mt, nil, nil, nil)
	assert.Equal(t, defaultTaskResource(), it.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), it.GetTaskResource())
}

func TestTaskResource_Stats(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7, SubJobType: indexpb.StatsSubJob_TextIndexJob}, 1, mt, nil, nil, nil)
	assert.Equal(t, statsTaskResource(familySegmentSize), st.GetTaskResource())
	assert.Equal(t, statsTaskResource(familySegmentSize), st.GetTaskResource()) // cached

	orphan := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 999, TaskID: 8}, 1, mt, nil, nil, nil)
	assert.Equal(t, defaultTaskResource(), orphan.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), orphan.GetTaskResource())
}

// TestTaskResource_StatsUnsizedSegment covers a segment with neither Stats nor
// a cached schema: the floor, and not cached.
func TestTaskResource_StatsUnsizedSegment(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	mt.segments.segments[3].Stats = nil
	mt.collections = typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()

	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7}, 1, mt, nil, nil, nil)
	assert.Equal(t, defaultTaskResource(), st.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), st.GetTaskResource())
}

func TestTaskResource_Analyze(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	at := newAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 9, FieldID: 101, FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{3}}, mt)
	assert.Equal(t, analyzeTaskResource(1000*128*4), at.GetTaskResource())
	assert.Equal(t, analyzeTaskResource(1000*128*4), at.GetTaskResource()) // cached

	missing := newAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 10, FieldID: 101, FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{999}}, mt)
	assert.Equal(t, defaultTaskResource(), missing.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), missing.GetTaskResource())
}

// TestTaskResource_AnalyzeSchemaCacheMiss is the analyze counterpart of
// TestTaskResource_IndexSchemaCacheMiss. newAnalyzeTask snapshots the schema at
// construction, so a task built before the collection is cached would keep a nil
// schema forever: priced at the floor on every round, and re-walking every input
// segment each time because the floor is never cached. Resolving the schema
// lazily lets it repair itself.
func TestTaskResource_AnalyzeSchemaCacheMiss(t *testing.T) {
	paramtable.Init()
	mt := bigFamilyMeta(t)
	// Exactly the state before the collection cache is warmed.
	mt.collections = typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()

	priced := analyzeTaskResource(bigFamilyRows * 128 * 4)
	assert.NotEqual(t, defaultTaskResource(), priced, "the bug this test guards must be observable")

	at := newAnalyzeTask(&indexpb.AnalyzeTask{
		CollectionID: 1, TaskID: 13, FieldID: 101,
		FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{3},
	}, mt)
	assert.Nil(t, at.schema, "the snapshot must be empty, or the test proves nothing")
	assert.Equal(t, defaultTaskResource(), at.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), at.GetTaskResource())

	// The schema arriving repairs the price on the very next call, without the
	// task being rebuilt.
	mt.collections.Insert(1, &collectionInfo{ID: 1, Schema: testResourceSchema()})
	assert.Equal(t, priced, at.GetTaskResource())

	// And that answer is cached: dropping the input segment does not change it.
	delete(mt.segments.segments, 3)
	assert.Equal(t, priced, at.GetTaskResource())
}

// TestTaskResource_AnalyzeUnknownField covers a clustering key that is not in
// the cached schema, and a field the vector estimator cannot size.
func TestTaskResource_AnalyzeUnknownField(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)

	unknown := newAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 11, FieldID: 999, SegmentIDs: []int64{3}}, mt)
	assert.Equal(t, defaultTaskResource(), unknown.GetTaskResource())

	// A scalar clustering key has no raw-vector footprint to price.
	scalar := newAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 12, FieldID: 100, SegmentIDs: []int64{3}}, mt)
	assert.Equal(t, defaultTaskResource(), scalar.GetTaskResource())
}

func TestTaskResource_CopySegmentAndRefresh(t *testing.T) {
	paramtable.Init()
	copyTask := &copySegmentTask{times: taskcommon.NewTimes()}
	copyTask.task.Store(&datapb.CopySegmentTask{TaskId: 1, JobId: 2})
	assert.Equal(t, lightweightTaskResource(), copyTask.GetTaskResource())

	refresh := &refreshExternalCollectionTask{}
	assert.Equal(t, lightweightTaskResource(), refresh.GetTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 64 * testMiB}, refresh.GetTaskResource())
}
