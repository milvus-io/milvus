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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// nullablePadPerRow mirrors the nullable fixed width encoding overhead the
// meta package charges per row on top of the schema width.
const nullablePadPerRow = int64(8)

func newSlotTestMeta(catalog *mocks2.DataCoordCatalog) (*meta.Meta, *IndexMeta) {
	im := &IndexMeta{
		KeyLock:          lock.NewKeyLock[UniqueID](),
		Catalog:          catalog,
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		Indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	mt := &meta.Meta{
		Segments:     meta.NewSegmentsInfo(),
		Collections:  typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](),
		IndexCatalog: im,
	}
	return mt, im
}

// externalSlotTestSchema is the production shape of a non milvus-table
// external collection: create time forces every user field nullable (Pass 2
// of NormalizeAndValidateExternalCollectionSchema).
func externalSlotTestSchema(vecField int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
			{
				FieldID:       vecField,
				Name:          "vec",
				DataType:      schemapb.DataType_FloatVector,
				Nullable:      true,
				ExternalField: "vec_col",
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}

func TestIndexInspector_ReloadFromMetaNeverResolvesThroughHandler(t *testing.T) {
	// Recovery runs synchronously in Start(), so it must never load a
	// collection through the handler (rootcoord). It also never narrows the
	// estimate: a StorageV2 segment keeps its per-field binlog size, and a
	// manifest-backed V3 segment is recovered without FieldBinlog arrays (the
	// catalog does not persist them), so it keeps the GetFieldBinlogSize
	// fallback.
	const (
		collID    = UniqueID(2)
		vecField  = UniqueID(101)
		numRows   = int64(3000)
		fieldSize = int64(600 * 1024 * 1024)
	)

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	// no GetCollection expectation: any resolution attempt fails the test
	handler := meta.NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := NewIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	mt, im := newSlotTestMeta(catalog)
	im.Indexes[collID] = map[UniqueID]*model.Index{
		3: {
			CollectionID: collID,
			FieldID:      vecField,
			IndexID:      3,
			IndexName:    meta.IndexName,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
			},
		},
	}
	// the collection is cached: recovery may use the local cache, but the
	// recovered segments carry nothing to attribute
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: externalSlotTestSchema(vecField)})
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)

	segments := []*meta.SegmentInfo{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				CollectionID:   collID,
				NumOfRows:      numRows,
				State:          commonpb.SegmentState_Flushed,
				StorageVersion: storage.StorageV2,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: vecField,
						Binlogs: []*datapb.Binlog{
							{EntriesNum: numRows, MemorySize: fieldSize},
						},
					},
				},
			},
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             2,
				CollectionID:   collID,
				NumOfRows:      numRows,
				State:          commonpb.SegmentState_Flushed,
				StorageVersion: storage.StorageV3,
				ManifestPath:   "manifest.json",
				Stats:          &datapb.Statistics{InsertBinlogSize: fieldSize},
			},
		},
	}
	for _, segment := range segments {
		mt.Segments.SetSegment(segment.GetID(), segment)
		err := im.AddSegmentIndex(ctx, &model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segment.GetID(),
			IndexID:      3,
			BuildID:      100 + segment.GetID(),
			NumRows:      numRows,
			IndexState:   commonpb.IndexState_Unissued,
		})
		assert.NoError(t, err)
	}

	slots := make([]int64, 0, 2)
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		slots = append(slots, t.GetTaskSlot())
	}).Return()

	inspector := NewIndexInspector(ctx, notifyChan, mt, im, scheduler, alloc, handler, storageCli, versionManager)
	inspector.reloadFromMeta()

	assert.Len(t, slots, 2)
	for _, slot := range slots {
		assert.Equal(t, calculateIndexTaskSlot(fieldSize, true), slot)
	}
}

func TestIndexInspector_estimateIndexFieldSize(t *testing.T) {
	const (
		collID   = UniqueID(2)
		vecField = UniqueID(101)
		numRows  = int64(3000)
		// The synthetic column group of an external segment covers every
		// column at once, so it is much larger than the indexed field alone.
		groupSize = int64(2 * 1024 * 1024 * 1024)
	)

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := meta.NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := NewIndexEngineVersionManager()

	mt, im := newSlotTestMeta(nil)
	inspector := NewIndexInspector(ctx, notifyChan, mt, im, scheduler, alloc, handler, storageCli, versionManager)

	externalSchema := externalSlotTestSchema(vecField)
	// An external segment carries a single fake column group listing every
	// field in ChildFields, so GetFieldBinlogSize reports the whole group.
	externalSegment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   collID,
			NumOfRows:      numRows,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest.json",
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{100, vecField},
					Binlogs: []*datapb.Binlog{
						{EntriesNum: numRows, MemorySize: groupSize},
					},
				},
			},
		},
	}

	t.Run("collection not found falls back to binlog size", func(t *testing.T) {
		assert.Equal(t, groupSize, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), externalSegment, vecField))
	})

	t.Run("collection without schema falls back to binlog size", func(t *testing.T) {
		assert.Equal(t, groupSize, inspector.estimateIndexFieldSize(ctx, &meta.CollectionInfo{ID: collID}, externalSegment, vecField))
	})

	t.Run("shared column group is attributed per field", func(t *testing.T) {
		// a normal storage v3 segment sharing one column group behaves the
		// same way: only the indexed column is read out of the group
		mt.AddCollection(&meta.CollectionInfo{
			ID: collID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
					{
						FieldID:  vecField,
						Name:     "vec",
						DataType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: "128"},
						},
					},
				},
			},
		})
		assert.Equal(t, int64(128*4)*numRows, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), externalSegment, vecField))
	})

	t.Run("single field column group keeps the measured size", func(t *testing.T) {
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: externalSchema})
		segment := &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             2,
				CollectionID:   collID,
				NumOfRows:      numRows,
				StorageVersion: storage.StorageV3,
				ManifestPath:   "manifest.json",
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: vecField,
						Binlogs: []*datapb.Binlog{
							{EntriesNum: numRows, MemorySize: groupSize},
						},
					},
				},
			},
		}
		assert.Equal(t, groupSize, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), segment, vecField))
	})

	t.Run("storage v2 shared group falls back to the group size", func(t *testing.T) {
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: externalSchema})
		segment := &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             3,
				CollectionID:   collID,
				NumOfRows:      numRows,
				StorageVersion: storage.StorageV2,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID:     0,
						ChildFields: []int64{100, vecField},
						Binlogs: []*datapb.Binlog{
							{EntriesNum: numRows, MemorySize: groupSize},
						},
					},
				},
			},
		}
		assert.Equal(t, groupSize, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), segment, vecField))
	})

	t.Run("recovered manifest segment without binlogs keeps the fallback size", func(t *testing.T) {
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: externalSchema})
		segment := &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             4,
				CollectionID:   collID,
				NumOfRows:      numRows,
				StorageVersion: storage.StorageV3,
				ManifestPath:   "manifest.json",
				Stats:          &datapb.Statistics{InsertBinlogSize: groupSize},
			},
		}
		assert.Equal(t, groupSize, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), segment, vecField))
	})

	t.Run("external collection estimates from schema", func(t *testing.T) {
		// all user fields are nullable, so the vector is charged its width
		// plus the nullable encoding overhead instead of the whole group
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: externalSchema})
		assert.Equal(t, (int64(128*4)+nullablePadPerRow)*numRows, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), externalSegment, vecField))
	})

	t.Run("external collection falls back on unknown field", func(t *testing.T) {
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: externalSchema})
		assert.Equal(t, groupSize, inspector.estimateIndexFieldSize(ctx, mt.GetCollection(collID), externalSegment, 999))
	})
}

func TestIndexInspector_CreateIndexForSegment_ExternalTaskSlot(t *testing.T) {
	paramtable.Init()

	const (
		collID   = UniqueID(2)
		vecField = UniqueID(101)
		indexID  = UniqueID(5)
		numRows  = int64(3000)
		// 2GB column group: the whole-segment size would ask for
		// (2GB / 512MB) * indexTaskSlotUsage slots.
		groupSize = int64(2 * 1024 * 1024 * 1024)
	)

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := meta.NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := NewIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	mt, im := newSlotTestMeta(catalog)
	im.Indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			FieldID:      vecField,
			IndexID:      indexID,
			IndexName:    meta.IndexName,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
			},
		},
	}
	collection := &meta.CollectionInfo{ID: collID, Schema: externalSlotTestSchema(vecField)}
	mt.AddCollection(collection)

	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   collID,
			PartitionID:    3,
			NumOfRows:      numRows,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L1,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest.json",
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{100, vecField},
					Binlogs: []*datapb.Binlog{
						{EntriesNum: numRows, MemorySize: groupSize},
					},
				},
			},
		},
	}
	mt.Segments.SetSegment(segment.GetID(), segment)

	inspector := NewIndexInspector(ctx, notifyChan, mt, im, scheduler, alloc, handler, storageCli, versionManager)

	var enqueuedSlot int64
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(12345), nil)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		enqueuedSlot = t.GetTaskSlot()
	}).Return()

	err := inspector.createIndexForSegment(ctx, collection, segment, indexID)
	assert.NoError(t, err)

	// A 128 dim nullable float vector over 3000 rows is bounded by its width
	// plus the nullable encoding overhead — ~1.5MB, the smallest slot bucket,
	// instead of the whole 2GB column group.
	expected := calculateIndexTaskSlot((int64(128*4)+nullablePadPerRow)*numRows, true)
	assert.Equal(t, expected, enqueuedSlot)
	assert.Less(t, enqueuedSlot, calculateIndexTaskSlot(groupSize, true))
}

func TestIndexInspector_CreateIndexesForSegment_PassesResolvedCollection(t *testing.T) {
	paramtable.Init()

	const (
		collID    = UniqueID(2)
		vecField  = UniqueID(101)
		indexID   = UniqueID(5)
		numRows   = int64(3000)
		groupSize = int64(2 * 1024 * 1024 * 1024)
	)

	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := meta.NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := NewIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)

	mt, im := newSlotTestMeta(catalog)
	im.Indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			FieldID:      vecField,
			IndexID:      indexID,
			IndexName:    meta.IndexName,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
			},
		},
	}
	// The collection is only resolvable through the handler (not in the local
	// cache), so a narrowed slot proves createIndexesForSegment hands the
	// resolved collection to the estimation.
	handler.EXPECT().GetCollection(mock.Anything, collID).Return(
		&meta.CollectionInfo{ID: collID, Schema: externalSlotTestSchema(vecField)}, nil)

	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   collID,
			PartitionID:    3,
			NumOfRows:      numRows,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L1,
			IsSorted:       true,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest.json",
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{100, vecField},
					Binlogs: []*datapb.Binlog{
						{EntriesNum: numRows, MemorySize: groupSize},
					},
				},
			},
		},
	}
	mt.Segments.SetSegment(segment.GetID(), segment)

	inspector := NewIndexInspector(ctx, notifyChan, mt, im, scheduler, alloc, handler, storageCli, versionManager)

	var enqueuedSlot int64
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(12345), nil)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil)
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		enqueuedSlot = t.GetTaskSlot()
	}).Return()

	assert.NoError(t, inspector.createIndexesForSegment(ctx, segment))
	assert.Equal(t, calculateIndexTaskSlot((int64(128*4)+nullablePadPerRow)*numRows, true), enqueuedSlot)
}
