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

// TestTaskResource_IndexUnpriceableField covers a collection whose schema is
// not cached yet: nothing to apportion, so the floor and no caching.
func TestTaskResource_IndexUnpriceableField(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	mt.segments.segments[3].Stats = nil
	mt.collections = typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()

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
