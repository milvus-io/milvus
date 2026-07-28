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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type (
	indexInspectorTestScheduler struct{ task.GlobalScheduler }
	indexInspectorTestAllocator struct{ allocator.Allocator }
	indexInspectorTestHandler   struct{ Handler }
	indexInspectorTestCatalog   struct{ metastore.DataCoordCatalog }
	indexInspectorTestStorage   struct{ storage.ChunkManager }
)

func TestIndexInspector_inspect(t *testing.T) {
	t.Run("normal test", func(t *testing.T) {
		ctx := context.Background()
		notifyChan := make(chan int64, 1)
		scheduler := &indexInspectorTestScheduler{}
		alloc := &indexInspectorTestAllocator{}
		handler := &indexInspectorTestHandler{}
		storageCli := &indexInspectorTestStorage{}
		versionManager := newIndexEngineVersionManager()
		catalog := &indexInspectorTestCatalog{}

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
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 101},
				},
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
		collection := &collectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, Name: "field", DataType: schemapb.DataType_Int64},
				},
			},
		}
		meta.collections.Insert(collection.ID, collection)

		inspector := newIndexInspector(ctx, notifyChan, meta, scheduler, alloc, handler, storageCli, versionManager)

		// Register all patches before Start(): the inspector goroutine
		// (reloadFromMeta, the ticker, and the notify channel) may invoke the
		// mocks immediately, and a call racing with patch registration can
		// silently abort the indexing flow.
		mockAllocID := mockey.Mock((*indexInspectorTestAllocator).AllocID).Return(int64(1001), nil).Build()
		defer mockAllocID.UnPatch()
		mockCreateSegmentIndex := mockey.Mock((*indexInspectorTestCatalog).CreateSegmentIndex).Return(nil).Build()
		defer mockCreateSegmentIndex.UnPatch()
		mockAlterSegmentIndexes := mockey.Mock((*indexInspectorTestCatalog).AlterSegmentIndexes).Return(nil).Build()
		defer mockAlterSegmentIndexes.UnPatch()
		mockGetCollection := mockey.Mock((*indexInspectorTestHandler).GetCollection).Return(collection, nil).Build()
		defer mockGetCollection.UnPatch()
		mockEnqueue := mockey.Mock((*indexInspectorTestScheduler).Enqueue).To(func(_ *indexInspectorTestScheduler, _ task.Task) {
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
		}).Build()
		defer mockEnqueue.UnPatch()

		inspector.Start()
		defer inspector.Stop()

		notifyChan <- segment.GetCollectionID()

		assert.Eventually(t, func() bool {
			return !meta.indexMeta.IsUnIndexedSegment(segment.GetCollectionID(), segment.GetID())
		}, time.Second*10, time.Millisecond*10)
	})
}

func TestIndexInspector_ReloadFromMeta(t *testing.T) {
	pt := paramtable.Get()
	heavyKey := pt.DataCoordCfg.IndexTaskSlotUsage.Key
	scalarKey := pt.DataCoordCfg.ScalarIndexTaskSlotUsage.Key
	assert.NoError(t, pt.Save(heavyKey, "64"))
	assert.NoError(t, pt.Save(scalarKey, "16"))
	defer pt.Reset(heavyKey)
	defer pt.Reset(scalarKey)

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
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{MemorySize: 200 * 1024 * 1024}},
			}},
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
			IndexParams: []*commonpb.KeyValuePair{{
				Key: common.IndexTypeKey, Value: "FMINDEX",
			}},
		},
	}

	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(scheduled task.Task) {
		assert.Equal(t, int64(4), scheduled.GetTaskSlot())
	}).Return()
	inspector.reloadFromMeta()
}

func TestIndexInspector_CreateIndexForSegment_FMIndexUsesMemoryBasedSlots(t *testing.T) {
	pt := paramtable.Get()
	heavyKey := pt.DataCoordCfg.IndexTaskSlotUsage.Key
	scalarKey := pt.DataCoordCfg.ScalarIndexTaskSlotUsage.Key
	assert.NoError(t, pt.Save(heavyKey, "64"))
	assert.NoError(t, pt.Save(scalarKey, "16"))
	defer pt.Reset(heavyKey)
	defer pt.Reset(scalarKey)

	ctx := context.Background()
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

	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 2,
		PartitionID:  3,
		State:        commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 101,
			Binlogs: []*datapb.Binlog{{MemorySize: 200 * 1024 * 1024}},
		}},
	}}
	meta.segments.SetSegment(segment.GetID(), segment)
	meta.indexMeta.indexes[2] = map[UniqueID]*model.Index{
		5: {
			CollectionID: 2,
			FieldID:      101,
			IndexID:      5,
			IndexName:    indexName,
			IndexParams: []*commonpb.KeyValuePair{{
				Key: common.IndexTypeKey, Value: "FMINDEX",
			}},
		},
	}

	inspector := newIndexInspector(ctx, nil, meta, scheduler, alloc, handler, storage, versionManager)
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(12345), nil)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(scheduled task.Task) {
		assert.Equal(t, int64(4), scheduled.GetTaskSlot())
	}).Return()

	assert.NoError(t, inspector.createIndexForSegment(ctx, segment, 5))
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
	scheduler := &indexInspectorTestScheduler{}
	alloc := &indexInspectorTestAllocator{}
	handler := &indexInspectorTestHandler{}
	storageCli := &indexInspectorTestStorage{}
	versionManager := newIndexEngineVersionManager()
	catalog := &indexInspectorTestCatalog{}

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
	mockAllocID := mockey.Mock((*indexInspectorTestAllocator).AllocID).Return(int64(12345), nil).Build()
	defer mockAllocID.UnPatch()
	mockCreateSegmentIndex := mockey.Mock((*indexInspectorTestCatalog).CreateSegmentIndex).Return(nil).Build()
	defer mockCreateSegmentIndex.UnPatch()
	mockEnqueue := mockey.Mock((*indexInspectorTestScheduler).Enqueue).Return().Build()
	defer mockEnqueue.UnPatch()
	mockGetCollection := mockey.Mock((*indexInspectorTestHandler).GetCollection).
		To(func(_ *indexInspectorTestHandler, _ context.Context, collectionID UniqueID) (*collectionInfo, error) {
			return m.GetCollection(collectionID), nil
		}).Build()
	defer mockGetCollection.UnPatch()

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
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 101},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

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

func TestIndexInspector_SchemaVersionGate(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key)

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := &indexInspectorTestScheduler{}
	alloc := &indexInspectorTestAllocator{}
	handler := &indexInspectorTestHandler{}
	storageCli := &indexInspectorTestStorage{}
	versionManager := newIndexEngineVersionManager()
	catalog := &indexInspectorTestCatalog{}

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
	nextBuildID := int64(12345)
	mockAllocID := mockey.Mock((*indexInspectorTestAllocator).AllocID).
		To(func(_ *indexInspectorTestAllocator, _ context.Context) (int64, error) {
			nextBuildID++
			return nextBuildID, nil
		}).Build()
	defer mockAllocID.UnPatch()
	mockCreateSegmentIndex := mockey.Mock((*indexInspectorTestCatalog).CreateSegmentIndex).Return(nil).Build()
	defer mockCreateSegmentIndex.UnPatch()
	mockEnqueue := mockey.Mock((*indexInspectorTestScheduler).Enqueue).Return().Build()
	defer mockEnqueue.UnPatch()
	mockGetCollection := mockey.Mock((*indexInspectorTestHandler).GetCollection).
		To(func(_ *indexInspectorTestHandler, _ context.Context, collectionID UniqueID) (*collectionInfo, error) {
			if collectionID == 11 {
				return nil, errors.New("mock rootcoord unreachable")
			}
			return m.GetCollection(collectionID), nil
		}).Build()
	defer mockGetCollection.UnPatch()

	inspector := newIndexInspector(ctx, notifyChan, m, scheduler, alloc, handler, storageCli, versionManager)
	collID := int64(10)
	collInfo := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Version: 5,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
				{FieldID: 103, Name: "new_vector", DataType: schemapb.DataType_FloatVector},
			},
			Functions: []*schemapb.FunctionSchema{
				{Name: "bm25_fn", OutputFieldIds: []int64{102}},
			},
		},
	}
	m.collections.Insert(collID, collInfo)

	t.Run("create function output index when schema is caught up", func(t *testing.T) {
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			5: {CollectionID: collID, FieldID: 102, IndexID: 5, IndexName: "bm25_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  5,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(5))
	})

	t.Run("create non function output index when schema is caught up", func(t *testing.T) {
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			6: {CollectionID: collID, FieldID: 103, IndexID: 6, IndexName: "new_vector_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             2,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  5,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(6))
	})

	t.Run("skip whole segment when schema is behind", func(t *testing.T) {
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			7: {CollectionID: collID, FieldID: 101, IndexID: 7, IndexName: "text_idx"},
			8: {CollectionID: collID, FieldID: 102, IndexID: 8, IndexName: "bm25_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             3,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  4,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		segIndexes := m.indexMeta.GetSegmentIndexes(collID, segment.GetID())
		assert.NotContains(t, segIndexes, UniqueID(7))
		assert.NotContains(t, segIndexes, UniqueID(8))
	})

	t.Run("create function output index when segment schema is ahead", func(t *testing.T) {
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			9: {CollectionID: collID, FieldID: 102, IndexID: 9, IndexName: "bm25_idx_ahead"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             4,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  6,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(9))
	})

	t.Run("defer index build when schema is unresolvable", func(t *testing.T) {
		unresolvableCollID := int64(11)
		m.indexMeta.indexes[unresolvableCollID] = map[UniqueID]*model.Index{
			10: {CollectionID: unresolvableCollID, FieldID: 102, IndexID: 10, IndexName: "unresolvable_schema_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           5,
				CollectionID: unresolvableCollID,
				State:        commonpb.SegmentState_Flushed,
				IsSorted:     true,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.NotContains(t, m.indexMeta.GetSegmentIndexes(unresolvableCollID, segment.GetID()), UniqueID(10))
	})

	t.Run("defer index build when indexed field is unknown to schema", func(t *testing.T) {
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			11: {CollectionID: collID, FieldID: 104, IndexID: 11, IndexName: "stale_schema_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            6,
				CollectionID:  collID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 5,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.NotContains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(11))
	})

	externalCollID := int64(12)
	externalCollInfo := &collectionInfo{
		ID: externalCollID,
		Schema: &schemapb.CollectionSchema{
			Version: 5,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 200, Name: "pk", DataType: schemapb.DataType_Int64, ExternalField: "pk"},
				{FieldID: 201, Name: "vector", DataType: schemapb.DataType_FloatVector},
			},
		},
	}
	m.collections.Insert(externalCollID, externalCollInfo)

	t.Run("create external index when schema is caught up without binlogs", func(t *testing.T) {
		m.indexMeta.indexes[externalCollID] = map[UniqueID]*model.Index{
			12: {CollectionID: externalCollID, FieldID: 201, IndexID: 12, IndexName: "external_vector_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            7,
				CollectionID:  externalCollID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 5,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(externalCollID, segment.GetID()), UniqueID(12))
	})

	t.Run("skip external index when schema is behind despite field coverage", func(t *testing.T) {
		m.indexMeta.indexes[externalCollID] = map[UniqueID]*model.Index{
			13: {CollectionID: externalCollID, FieldID: 201, IndexID: 13, IndexName: "external_vector_idx_behind"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            8,
				CollectionID:  externalCollID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 4,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{200, 201}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.NotContains(t, m.indexMeta.GetSegmentIndexes(externalCollID, segment.GetID()), UniqueID(13))
	})

	t.Run("skip external index when segment schema is ahead", func(t *testing.T) {
		m.indexMeta.indexes[externalCollID] = map[UniqueID]*model.Index{
			14: {CollectionID: externalCollID, FieldID: 201, IndexID: 14, IndexName: "external_vector_idx_ahead"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            9,
				CollectionID:  externalCollID,
				State:         commonpb.SegmentState_Flushed,
				IsSorted:      true,
				SchemaVersion: 6,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.NotContains(t, m.indexMeta.GetSegmentIndexes(externalCollID, segment.GetID()), UniqueID(14))
	})
}
