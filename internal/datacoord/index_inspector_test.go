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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestIndexInspector_inspect(t *testing.T) {
	t.Run("normal test", func(t *testing.T) {
		ctx := context.Background()
		notifyChan := make(chan int64, 1)
		scheduler := task.NewMockGlobalScheduler(t)
		alloc := allocator.NewMockAllocator(t)
		handler := NewNMockHandler(t)
		storage := mocks.NewChunkManager(t)
		versionManager := newIndexEngineVersionManager()
		catalog := mocks2.NewDataCoordCatalog(t)

		meta := &meta{
			segments:    NewSegmentsInfo(),
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
			indexMeta: &indexMeta{
				keyLock:          lock.NewKeyLock[UniqueID](),
				catalog:          catalog,
				segmentBuildInfo: newSegmentIndexBuildInfo(),
				indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
				segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},
		}

		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 2,
				PartitionID:  3,
				NumOfRows:    3000,
				State:        commonpb.SegmentState_Flushed,
				IsSorted:     true,
			},
		}
		meta.segments.SetSegment(segment.GetID(), segment)

		meta.indexMeta.indexes[2] = map[UniqueID]*model.Index{
			5: {
				CollectionID: 2,
				FieldID:      101,
				IndexID:      5,
				IndexName:    indexName,
			},
		}

		inspector := newIndexInspector(ctx, notifyChan, meta, scheduler, alloc, handler, storage, versionManager)

		inspector.Start()
		defer inspector.Stop()

		notifyChan <- segment.GetCollectionID()

		alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil)
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		scheduler.EXPECT().Enqueue(mock.Anything).Run(func(_a0 task.Task) {
			err := meta.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
				SegmentID: segment.GetID(),
				BuildID:   segment.GetID(),
			})
			assert.NoError(t, err)
			err = meta.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
				BuildID: segment.GetID(),
				State:   commonpb.IndexState_Finished,
			})
			assert.NoError(t, err)
		})

		assert.Eventually(t, func() bool {
			return !meta.indexMeta.IsUnIndexedSegment(segment.GetCollectionID(), segment.GetID())
		}, time.Second*10, time.Millisecond*10)
	})
}

func TestIndexInspector_ReloadFromMeta(t *testing.T) {
	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storage := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			catalog:          catalog,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	inspector := newIndexInspector(ctx, notifyChan, meta, scheduler, alloc, handler, storage, versionManager)

	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)

	seg1 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 2,
			State:        commonpb.SegmentState_Flushed,
		},
	}
	meta.segments.SetSegment(seg1.ID, seg1)

	segIndex1 := &model.SegmentIndex{
		SegmentID:  seg1.ID,
		IndexID:    3,
		BuildID:    4,
		IndexState: commonpb.IndexState_Unissued,
	}
	meta.indexMeta.AddSegmentIndex(ctx, segIndex1)

	meta.indexMeta.indexes[2] = map[UniqueID]*model.Index{
		3: {
			CollectionID: 2,
			FieldID:      100,
			IndexID:      3,
			IndexName:    indexName,
		},
	}

	scheduler.EXPECT().Enqueue(mock.Anything).Return()
	inspector.reloadFromMeta()
}

func TestIndexInspector_isExternalCollection(t *testing.T) {
	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()

	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	inspector := newIndexInspector(ctx, notifyChan, m, scheduler, alloc, handler, storageCli, versionManager)

	t.Run("collection not found", func(t *testing.T) {
		assert.False(t, inspector.isExternalCollection(999))
	})

	t.Run("normal collection is not external", func(t *testing.T) {
		m.collections.Insert(10, &collectionInfo{
			ID: 10,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{Name: "pk", FieldID: 100, DataType: schemapb.DataType_Int64},
				},
			},
		})
		assert.False(t, inspector.isExternalCollection(10))
	})

	t.Run("external collection is external", func(t *testing.T) {
		m.collections.Insert(20, &collectionInfo{
			ID: 20,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{Name: "id", FieldID: 101, DataType: schemapb.DataType_Int64, ExternalField: "id"},
				},
			},
		})
		assert.True(t, inspector.isExternalCollection(20))
	})
}

func TestIndexInspector_CreateIndexesForSegment_ExternalUnsorted(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableCompaction.Key)
	}()

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			catalog:          catalog,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	m.indexMeta.indexes[2] = map[UniqueID]*model.Index{
		5: {
			CollectionID: 2,
			FieldID:      101,
			IndexID:      5,
			IndexName:    indexName,
		},
	}

	inspector := newIndexInspector(ctx, notifyChan, m, scheduler, alloc, handler, storageCli, versionManager)

	t.Run("normal unsorted segment is skipped", func(t *testing.T) {
		m.collections.Insert(2, &collectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{Name: "pk", FieldID: 100, DataType: schemapb.DataType_Int64},
				},
			},
		})

		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 2,
				State:        commonpb.SegmentState_Flushed,
				IsSorted:     false,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		// No index should be created because segment is unsorted and collection is not external
		assert.True(t, m.indexMeta.IsUnIndexedSegment(2, 1))
	})

	t.Run("external unsorted segment is not skipped", func(t *testing.T) {
		m.collections.Insert(2, &collectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{Name: "id", FieldID: 101, DataType: schemapb.DataType_Int64, ExternalField: "id"},
				},
			},
		})

		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 2,
				State:        commonpb.SegmentState_Flushed,
				IsSorted:     false,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		alloc.EXPECT().AllocID(mock.Anything).Return(int64(12345), nil)
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
		scheduler.EXPECT().Enqueue(mock.Anything).Return()

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
	})
}

func TestIndexInspector_CreateIndexForSegment_OverrideIndexType(t *testing.T) {
	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storage := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			catalog:          catalog,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 2,
			PartitionID:  3,
			NumOfRows:    100,
			State:        commonpb.SegmentState_Flushed,
			IsSorted:     true,
			Level:        datapb.SegmentLevel_L1,
		},
	}
	meta.segments.SetSegment(segment.GetID(), segment)

	meta.indexMeta.indexes[2] = map[UniqueID]*model.Index{
		5: {
			CollectionID: 2,
			FieldID:      101,
			IndexID:      5,
			IndexName:    indexName,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
				{Key: paramtable.OverrideIndexTypeKey, Value: "DISKANN"},
			},
		},
	}

	inspector := newIndexInspector(ctx, notifyChan, meta, scheduler, alloc, handler, storage, versionManager)

	alloc.EXPECT().AllocID(mock.Anything).Return(int64(12345), nil)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
	scheduler.EXPECT().Enqueue(mock.Anything).Return()

	err := inspector.createIndexForSegment(ctx, segment, 5)
	assert.NoError(t, err)

	segIndexes := meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
	segIdx, ok := segIndexes[5]
	assert.True(t, ok)
	assert.Equal(t, "DISKANN", segIdx.IndexType)
}

func TestGetSegmentBinlogFields(t *testing.T) {
	t.Run("uses ChildFields not FieldID", func(t *testing.T) {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID:     0, // columnGroupID
						ChildFields: []int64{100, 101, 102},
					},
					{
						FieldID:     1, // another columnGroupID
						ChildFields: []int64{200},
					},
				},
			},
		}
		fields := getSegmentBinlogFields(segment)
		assert.Contains(t, fields, int64(100))
		assert.Contains(t, fields, int64(101))
		assert.Contains(t, fields, int64(102))
		assert.Contains(t, fields, int64(200))
		// columnGroupIDs should NOT be in the result
		assert.NotContains(t, fields, int64(0))
		assert.NotContains(t, fields, int64(1))
	})

	t.Run("empty binlogs", func(t *testing.T) {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{Binlogs: nil},
		}
		fields := getSegmentBinlogFields(segment)
		assert.Empty(t, fields)
	})
}

func TestIndexInspector_MinSchemaVersionEnforcement(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key)

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			catalog:          catalog,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	inspector := newIndexInspector(ctx, notifyChan, m, scheduler, alloc, handler, storageCli, versionManager)

	collID := int64(10)

	// Index on field 102 (function output) with MinSchemaVersion=2
	m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
		5: {
			CollectionID:     collID,
			FieldID:          102,
			IndexID:          5,
			IndexName:        "bm25_idx",
			MinSchemaVersion: 2,
		},
	}

	m.collections.Insert(collID, &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			// DoPhysicalBackfill=true is required for the MinSchemaVersion gate to engage.
			// Without it the gate is bypassed and indexes are built immediately (the
			// metadata-only schema-change path).
			DoPhysicalBackfill: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{Name: "bm25_fn", OutputFieldIds: []int64{102}},
			},
		},
	})

	t.Run("skip index when field data missing from segment", func(t *testing.T) {
		// Segment with SchemaVersion=1, binlogs have fields 100+101 but NOT 102.
		// In physical backfill mode, the gate skips index creation until the field's
		// binlog data is written by backfill.
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  collID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 1,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		// Index should NOT have been created — field 102 data is missing
		assert.True(t, m.indexMeta.IsUnIndexedSegment(collID, segment.GetID()))
	})

	t.Run("return error when field binlogs present but segment schema version still behind", func(t *testing.T) {
		// Segment with SchemaVersion=1 (behind index.MinSchemaVersion=2) BUT field 102
		// already exists in binlogs. This is a transient inconsistency window: backfill
		// has written the field data but the metadata-update tick has not yet bumped
		// segment.SchemaVersion. The inspector must NOT proceed to build the index in
		// this window — it returns an error and retries on the next tick by which time
		// the metadata is expected to have caught up.
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  collID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 1,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101, 102}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "segment schema version")
		// Index should NOT have been created
		assert.True(t, m.indexMeta.IsUnIndexedSegment(collID, segment.GetID()))
	})

	t.Run("create index normally when MinSchemaVersion matches", func(t *testing.T) {
		// Segment with SchemaVersion=2, matches MinSchemaVersion — should proceed normally
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            3,
				CollectionID:  collID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 2,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101, 102}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		alloc.EXPECT().AllocID(mock.Anything).Return(int64(12346), nil).Once()
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
		scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
	})
}
