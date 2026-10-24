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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const readinessCollID int64 = 100

// readinessVectorSchema has a vector field only (no text / JSON-key
// requirement, so the stats term is exempted and the index term can be
// exercised in isolation).
func readinessVectorSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "readiness_vector",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}
}

// readinessStatsSchema has a text field (enable_match) and a JSON field, so the
// stats term is active, with no vector field (index term exempt unless an index
// is explicitly added).
func readinessStatsSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "readiness_stats",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 2, Name: "title", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}},
			},
			{FieldID: 3, Name: "meta", DataType: schemapb.DataType_JSON},
		},
	}
}

// readinessExternalSchema marks a scalar field with ExternalField so the
// collection is treated as external (sort term exempt; JSON-key stats only
// buildable on V3 manifest segments).
func readinessExternalSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "readiness_external",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
			{FieldID: 2, Name: "meta", DataType: schemapb.DataType_JSON, ExternalField: "meta"},
		},
	}
}

// newReadinessIndexMeta builds an indexMeta over the given per-field indexes and
// per-segment index states. Field indexes carry the given fieldID keyed by
// IndexID; segment indexes are keyed by SegmentID.
func newReadinessIndexMeta(collectionID int64, fieldIndexes map[int64]*model.Index, segIndexes map[int64][]*model.SegmentIndex) *indexMeta {
	im := &indexMeta{
		catalog:          datacoord.NewCatalog(NewMetaMemoryKV(), "", ""),
		keyLock:          lock.NewKeyLock[UniqueID](),
		indexes:          map[UniqueID]map[UniqueID]*model.Index{collectionID: fieldIndexes},
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
	}
	for segID, segs := range segIndexes {
		mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		for _, s := range segs {
			mm.Insert(s.IndexID, s)
		}
		im.segmentIndexes.Insert(segID, mm)
	}
	return im
}

// readinessMeta builds a meta whose collection cache holds the given collection
// and whose indexMeta is the given one.
func readinessMeta(coll *collectionInfo, im *indexMeta) *meta {
	mt := createMeta(datacoord.NewCatalog(NewMetaMemoryKV(), "", ""), withIndexMeta(im))
	mt.AddCollection(coll)
	return mt
}

func readinessCollection(id int64, schema *schemapb.CollectionSchema) *collectionInfo {
	return &collectionInfo{ID: id, Schema: schema}
}

func readyFlushedSegment(id int64) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           id,
			CollectionID: readinessCollID,
			State:        commonpb.SegmentState_Flushed,
			Binlogs:      []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
		},
	}
}

func vectorIndex(fieldID int64, indexID int64) *model.Index {
	return &model.Index{
		CollectionID: readinessCollID,
		FieldID:      fieldID,
		IndexID:      indexID,
		IndexName:    "readiness_index",
		IsDeleted:    false,
		CreateTime:   1,
		IndexParams:  []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}},
		TypeParams:   []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
	}
}

func segmentIndex(segID, indexID int64, state commonpb.IndexState) *model.SegmentIndex {
	return &model.SegmentIndex{
		CollectionID: readinessCollID,
		SegmentID:    segID,
		IndexID:      indexID,
		IndexState:   state,
		IsDeleted:    false,
	}
}

func TestSegmentReadiness_DataReady(t *testing.T) {
	coll := readinessCollection(readinessCollID, readinessVectorSchema())
	// No index on the collection: isolate the data term.
	r := readinessMeta(coll, newReadinessIndexMeta(readinessCollID, map[int64]*model.Index{}, nil)).prepareReadiness(coll)
	require.NotNil(t, r)

	cases := []struct {
		name    string
		segment *SegmentInfo
		ready   bool
	}{
		{
			name: "growing not ready",
			segment: &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, CollectionID: readinessCollID, State: commonpb.SegmentState_Growing, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}},
			}},
			ready: false,
		},
		{
			name: "flushed with binlogs ready",
			segment: &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, CollectionID: readinessCollID, State: commonpb.SegmentState_Flushed, Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
			}},
			ready: true,
		},
		{
			name: "flushing with binlogs ready",
			segment: &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, CollectionID: readinessCollID, State: commonpb.SegmentState_Flushing, Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
			}},
			ready: true,
		},
		{
			name: "flushed with manifest ready",
			segment: &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, CollectionID: readinessCollID, State: commonpb.SegmentState_Flushed, ManifestPath: "s3://segments/1/manifest",
			}},
			ready: true,
		},
		{
			name: "flushed with no footprint not ready",
			segment: &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, CollectionID: readinessCollID, State: commonpb.SegmentState_Flushed,
			}},
			ready: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.ready, r.dataReady(tc.segment))
		})
	}
}

func TestSegmentReadiness_IndexTerm_NoIndexCollection(t *testing.T) {
	// The behavior fix: on a no-index collection a segment is index-ready as
	// soon as its data is complete, even with no segment-index record (design
	// §5.3 "skipNoIndexCollection=true" semantics).
	coll := readinessCollection(readinessCollID, readinessVectorSchema())
	im := newReadinessIndexMeta(readinessCollID, map[int64]*model.Index{}, nil)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.False(t, r.hasIndex)

	seg := readyFlushedSegment(1)
	require.True(t, r.indexReady(seg), "no-index collection must be index-ready without any index")
}

func TestSegmentReadiness_IndexTerm_VectorIndex(t *testing.T) {
	coll := readinessCollection(readinessCollID, readinessVectorSchema())
	const (
		fieldID = int64(2)
		indexID = int64(10)
	)
	im := newReadinessIndexMeta(readinessCollID,
		map[int64]*model.Index{indexID: vectorIndex(fieldID, indexID)},
		nil,
	)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.True(t, r.hasIndex)

	t.Run("finished index ready", func(t *testing.T) {
		seg := readyFlushedSegment(1)
		im.segmentIndexes.Insert(seg.GetID(), func() *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex] {
			mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
			mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_Finished))
			return mm
		}())
		require.True(t, r.indexReady(seg))
	})

	t.Run("unissued index not ready", func(t *testing.T) {
		seg := readyFlushedSegment(2)
		im.segmentIndexes.Insert(seg.GetID(), func() *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex] {
			mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
			mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_Unissued))
			return mm
		}())
		require.False(t, r.indexReady(seg))
	})

	t.Run("in-progress index not ready", func(t *testing.T) {
		seg := readyFlushedSegment(3)
		im.segmentIndexes.Insert(seg.GetID(), func() *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex] {
			mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
			mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_InProgress))
			return mm
		}())
		require.False(t, r.indexReady(seg))
	})

	t.Run("missing segment index record not ready", func(t *testing.T) {
		seg := readyFlushedSegment(4)
		require.False(t, r.indexReady(seg))
	})

	t.Run("small sorted leaf exempt", func(t *testing.T) {
		seg := readyFlushedSegment(5)
		seg.NumOfRows = 100
		seg.IsSorted = true
		require.True(t, r.indexReady(seg), "small sorted leaf below MinSegmentNumRowsToEnableIndex is exempt")

		large := readyFlushedSegment(6)
		large.NumOfRows = 5000
		large.IsSorted = true
		require.False(t, r.indexReady(large), "large sorted segment is not exempt")
	})
}

func TestSegmentReadiness_IndexTerm_PureScalarDegradation(t *testing.T) {
	// Pure-scalar collection: no vector field, so targetFieldIDs would be empty
	// and the term vacuously satisfied; the predicate must degrade to "every
	// scalar index Finished" instead (design §5.3, O3).
	coll := readinessCollection(readinessCollID, &schemapb.CollectionSchema{
		Name: "readiness_scalar",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "title", DataType: schemapb.DataType_VarChar},
		},
	})
	const (
		fieldID = int64(2)
		indexID = int64(10)
	)
	im := newReadinessIndexMeta(readinessCollID,
		map[int64]*model.Index{indexID: {CollectionID: readinessCollID, FieldID: fieldID, IndexID: indexID, IndexName: "scalar_index"}},
		nil,
	)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.True(t, r.hasIndex)
	require.Equal(t, []int64{fieldID}, r.targetFieldIDs)

	t.Run("scalar index finished ready", func(t *testing.T) {
		seg := readyFlushedSegment(1)
		mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_Finished))
		im.segmentIndexes.Insert(seg.GetID(), mm)
		require.True(t, r.indexReady(seg))
	})

	t.Run("scalar index unissued not ready", func(t *testing.T) {
		seg := readyFlushedSegment(2)
		mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_Unissued))
		im.segmentIndexes.Insert(seg.GetID(), mm)
		require.False(t, r.indexReady(seg))
	})
}

func TestSegmentReadiness_IndexTerm_DVForceAllIndexReady(t *testing.T) {
	Params.Save(Params.DataCoordCfg.DVForceAllIndexReady.Key, "true")
	defer Params.Reset(Params.DataCoordCfg.DVForceAllIndexReady.Key)

	coll := readinessCollection(readinessCollID, &schemapb.CollectionSchema{
		Name: "readiness_dv",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 3, Name: "scalar", DataType: schemapb.DataType_VarChar},
		},
	})
	const (
		vecFieldID    = int64(2)
		scalarFieldID = int64(3)
		vecIndexID    = int64(10)
		scalarIndexID = int64(11)
	)
	im := newReadinessIndexMeta(readinessCollID,
		map[int64]*model.Index{
			vecIndexID:    vectorIndex(vecFieldID, vecIndexID),
			scalarIndexID: {CollectionID: readinessCollID, FieldID: scalarFieldID, IndexID: scalarIndexID, IndexName: "scalar_index"},
		},
		nil,
	)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.Contains(t, r.targetFieldIDs, vecFieldID)
	require.Contains(t, r.targetFieldIDs, scalarFieldID)

	t.Run("vector only finished not ready", func(t *testing.T) {
		seg := readyFlushedSegment(1)
		mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		mm.Insert(vecIndexID, segmentIndex(seg.GetID(), vecIndexID, commonpb.IndexState_Finished))
		im.segmentIndexes.Insert(seg.GetID(), mm)
		require.False(t, r.indexReady(seg), "scalar-indexed field must also be finished when DVForceAllIndexReady")
	})

	t.Run("all indexes finished ready", func(t *testing.T) {
		seg := readyFlushedSegment(2)
		mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		mm.Insert(vecIndexID, segmentIndex(seg.GetID(), vecIndexID, commonpb.IndexState_Finished))
		mm.Insert(scalarIndexID, segmentIndex(seg.GetID(), scalarIndexID, commonpb.IndexState_Finished))
		im.segmentIndexes.Insert(seg.GetID(), mm)
		require.True(t, r.indexReady(seg))
	})
}

func TestSegmentReadiness_SortTerm(t *testing.T) {
	// EnableSortCompaction defaults to true; keep EnableCompaction at its
	// default true so enableSortCompaction() is true.
	coll := readinessCollection(readinessCollID, readinessVectorSchema())
	im := newReadinessIndexMeta(readinessCollID, map[int64]*model.Index{}, nil)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.True(t, r.requireSorted)

	seg := readyFlushedSegment(1)
	require.False(t, r.sortReady(seg), "unsorted segment is not sort-ready when sort compaction is enabled")

	seg.IsSorted = true
	require.True(t, r.sortReady(seg))
	seg.IsSorted = false
	seg.IsSortedByNamespace = true
	require.True(t, r.sortReady(seg), "IsSortedByNamespace satisfies the sort term")

	t.Run("sort compaction disabled exempts", func(t *testing.T) {
		Params.Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
		defer Params.Reset(Params.DataCoordCfg.EnableSortCompaction.Key)
		r2 := readinessMeta(coll, im).prepareReadiness(coll)
		require.False(t, r2.requireSorted)
		require.True(t, r2.sortReady(readyFlushedSegment(2)))
	})

	t.Run("external collection exempts", func(t *testing.T) {
		ext := readinessCollection(readinessCollID+1, readinessExternalSchema())
		require.True(t, ext.IsExternal())
		r2 := readinessMeta(ext, im).prepareReadiness(ext)
		require.False(t, r2.requireSorted)
		require.True(t, r2.sortReady(readyFlushedSegment(3)))
	})
}

func TestSegmentReadiness_StatsTerm(t *testing.T) {
	// No vector field and no index on the collection: index and sort terms are
	// isolated; disable sort compaction so the sorted flag only gates the stats
	// needDo* predicates.
	Params.Save(Params.DataCoordCfg.EnableSortCompaction.Key, "false")
	defer Params.Reset(Params.DataCoordCfg.EnableSortCompaction.Key)

	coll := readinessCollection(readinessCollID, readinessStatsSchema())
	im := newReadinessIndexMeta(readinessCollID, map[int64]*model.Index{}, nil)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.True(t, r.requireTextStats)
	require.True(t, r.requireJSONKeyStats)

	// Segments are marked sorted so the text/JSON-key needDo* predicates
	// actually evaluate the stats-presence branch.
	seg := readyFlushedSegment(1)
	seg.IsSorted = true

	t.Run("no stats written not ready", func(t *testing.T) {
		require.False(t, r.statsReady(seg))
	})

	t.Run("text stats only not ready", func(t *testing.T) {
		withText := seg.Clone()
		withText.TextStatsLogs = map[int64]*datapb.TextIndexStats{2: {}}
		require.False(t, r.statsReady(withText), "JSON-key stats still pending")
	})

	t.Run("both stats written ready", func(t *testing.T) {
		withStats := seg.Clone()
		withStats.TextStatsLogs = map[int64]*datapb.TextIndexStats{2: {}}
		withStats.JsonKeyStats = map[int64]*datapb.JsonKeyStats{3: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion}}
		require.True(t, r.statsReady(withStats))
	})

	t.Run("stale json key format not ready", func(t *testing.T) {
		withStats := seg.Clone()
		withStats.TextStatsLogs = map[int64]*datapb.TextIndexStats{2: {}}
		withStats.JsonKeyStats = map[int64]*datapb.JsonKeyStats{3: {JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion - 1}}
		require.False(t, r.statsReady(withStats), "an outdated JSON-key stats format must be rebuilt")
	})

	t.Run("external v2 segment json exempt", func(t *testing.T) {
		ext := readinessCollection(readinessCollID+1, readinessExternalSchema())
		r2 := readinessMeta(ext, im).prepareReadiness(ext)
		require.True(t, r2.requireJSONKeyStats)
		v2 := readyFlushedSegment(4)
		v2.StorageVersion = storage.StorageV1
		require.True(t, r2.statsReady(v2), "external V2 segment can never build JSON-key stats and is exempt")
	})
}

func TestSegmentReadiness_SchemaTerm(t *testing.T) {
	coll := readinessCollection(readinessCollID, readinessVectorSchema())
	coll.Schema.Version = 5
	im := newReadinessIndexMeta(readinessCollID, map[int64]*model.Index{}, nil)
	r := readinessMeta(coll, im).prepareReadiness(coll)
	require.NotNil(t, r)
	require.Equal(t, int32(5), r.schemaVersion)

	seg := readyFlushedSegment(1)
	require.False(t, r.schemaReady(seg), "segment behind the collection schema version is not ready")

	seg.SchemaVersion = 5
	require.True(t, r.schemaReady(seg))

	seg.SchemaVersion = 6
	require.True(t, r.schemaReady(seg), "ahead-of-collection schema version is ready")
}

func TestSegmentReadiness_Ready(t *testing.T) {
	coll := readinessCollection(readinessCollID, readinessVectorSchema())
	const (
		fieldID = int64(2)
		indexID = int64(10)
	)
	im := newReadinessIndexMeta(readinessCollID, map[int64]*model.Index{indexID: vectorIndex(fieldID, indexID)}, nil)
	r := readinessMeta(coll, im).prepareReadiness(coll)

	seg := readyFlushedSegment(1)
	seg.NumOfRows = 4096
	seg.IsSorted = true
	mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_Finished))
	im.segmentIndexes.Insert(seg.GetID(), mm)

	require.True(t, r.Ready(seg), "all terms satisfied")
	require.True(t, r.Predicate()(seg), "Predicate() must agree with Ready()")

	t.Run("nil segment never ready", func(t *testing.T) {
		require.False(t, r.Ready(nil))
	})

	t.Run("still invisible is ready", func(t *testing.T) {
		// Members stay IsInvisible=true through STAGED→READY; readiness must
		// not gate on visibility.
		seg.IsInvisible = true
		require.True(t, r.Ready(seg))
	})

	t.Run("index regression makes it not ready", func(t *testing.T) {
		mm := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		mm.Insert(indexID, segmentIndex(seg.GetID(), indexID, commonpb.IndexState_Unissued))
		im.segmentIndexes.Insert(seg.GetID(), mm)
		seg.IsInvisible = true
		require.False(t, r.Ready(seg))
	})
}

func TestPrepareReadiness_NilCollection(t *testing.T) {
	mt := createMeta(datacoord.NewCatalog(NewMetaMemoryKV(), "", ""))
	require.Nil(t, mt.prepareReadiness(nil))
	require.Nil(t, mt.prepareReadiness(&collectionInfo{ID: 1}))
}
