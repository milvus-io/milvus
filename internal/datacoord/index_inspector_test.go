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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
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

		store := metacache.NewMetaStore(nil)
		meta := &meta{
			segments:  NewSegmentsInfo(store),
			metaStore: store,
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

		inspector := newIndexInspector(ctx, notifyChan, meta, scheduler, alloc, handler, storage, versionManager)

		// Register all expectations before Start(): the inspector goroutine
		// (reloadFromMeta, the ticker, and the notify channel) may invoke the
		// mocks immediately, and a call racing with EXPECT registration hits
		// a no-expectation mock and silently aborts the indexing flow.
		handler.EXPECT().GetCollection(mock.Anything, int64(2)).Return(&collectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "f101"}},
			},
		}, nil).Maybe()
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

	store := metacache.NewMetaStore(nil)
	meta := &meta{
		segments:  NewSegmentsInfo(store),
		metaStore: store,
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

// A notification round takes only the notified segments, deduplicated; the
// ticker also takes the pending ones, and a notification supersedes a pending
// retry.
func TestIndexInspector_PopIndexSegments(t *testing.T) {
	inspector := newIndexInspector(context.Background(), nil, nil, nil, nil, nil, nil, nil)
	inspector.requeuePendingIndexSegments(1, 2)
	inspector.notifyIndexSegments(2, 3, 2)

	assert.ElementsMatch(t, []UniqueID{2, 3}, inspector.popIndexSegments(false))
	assert.ElementsMatch(t, []UniqueID{1}, inspector.popIndexSegments(true))
	assert.Empty(t, inspector.popIndexSegments(true))
}

// Senders must not depend on the create-index loop: with only the forwarder
// running, more segments than the channel holds still go through.
func TestIndexInspector_ForwardBuildIndexChWithoutCreateIndexLoop(t *testing.T) {
	drainBuildIndexChForTest()
	defer drainBuildIndexChForTest()

	// The deadline bounds the senders' wait if forwarding regresses.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inspector := newIndexInspector(ctx, nil, nil, nil, nil, nil, nil, nil)
	defer inspector.Stop()
	setBuildIndexConsumerForTest(t, ctx.Done())
	inspector.wg.Add(1)
	go inspector.forwardBuildIndexCh(inspector.ctx)

	total := 2 * cap(getBuildIndexChSingleton())
	notifySegmentIndexBuild(lo.RangeFrom(UniqueID(0), total)...)

	got := typeutil.NewUniqueSet()
	assert.Eventually(t, func() bool {
		got.Insert(inspector.popIndexSegments(false)...)
		return got.Len() == total
	}, 5*time.Second, 10*time.Millisecond)
	assert.Len(t, inspector.notifiedIndexSegsCh, 1, "forwarding must wake the create-index loop")
}

// A segment that appears after Start is indexed through its notification alone,
// while the segment the startup scan left pending waits for the ticker: the
// tick/reconcile intervals are far beyond the wait below, and the mocks allow a
// single index task.
func TestIndexInspector_StartIndexesNotifiedSegment(t *testing.T) {
	drainBuildIndexChForTest()
	defer drainBuildIndexChForTest()
	interval := &Params.DataCoordCfg.TaskCheckInterval
	defer interval.SwapTempValue(interval.SwapTempValue("60"))

	ctx := context.Background()
	const (
		collID       = UniqueID(12)
		segID        = UniqueID(32)
		pendingSegID = UniqueID(33)
		fieldID      = UniqueID(101)
		indexID      = UniqueID(42)
	)
	newFlushedSegment := func(id UniqueID) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:           id,
			CollectionID: collID,
			NumOfRows:    1024,
			State:        commonpb.SegmentState_Flushed,
			IsSorted:     true,
			Binlogs:      []*datapb.FieldBinlog{{FieldID: fieldID}},
		}}
	}
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	catalog := mocks2.NewDataCoordCatalog(t)
	ms := metacache.NewMetaStore(nil)
	m := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
		indexMeta: createTestIndexMeta(t, collID, map[int64]*model.Index{
			indexID: {CollectionID: collID, FieldID: fieldID, IndexID: indexID, IndexName: "default_idx"},
		}, catalog),
	}
	m.segments.SetSegment(pendingSegID, newFlushedSegment(pendingSegID))
	handler.EXPECT().GetCollection(mock.Anything, collID).Return(&collectionInfo{
		ID:     collID,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: fieldID, Name: "f101"}}},
	}, nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(1, nil).Once()
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
	scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

	inspector := newIndexInspector(ctx, make(chan int64, 1), m, scheduler, alloc, handler, mocks.NewChunkManager(t), newIndexEngineVersionManager())
	inspector.Start()
	defer inspector.Stop()

	m.segments.SetSegment(segID, newFlushedSegment(segID))
	notifySegmentIndexBuild(segID)

	assert.Eventually(t, func() bool {
		return !m.indexMeta.IsUnIndexedSegment(collID, segID)
	}, 5*time.Second, 10*time.Millisecond)
	assert.True(t, m.indexMeta.IsUnIndexedSegment(collID, pendingSegID))
	assert.ElementsMatch(t, []UniqueID{pendingSegID}, inspector.popIndexSegments(true))
}

func TestIndexInspector_ProcessIndexSegmentsRequeuesStillUnindexedSegment(t *testing.T) {
	ctx := context.Background()
	const (
		collID  = UniqueID(10)
		partID  = UniqueID(20)
		segID   = UniqueID(30)
		indexID = UniqueID(40)
	)

	ms := metacache.NewMetaStore(nil)
	m := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
		indexMeta: &indexMeta{
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						CollectionID: collID,
						FieldID:      102,
						IndexID:      indexID,
						IndexName:    "bm25_idx",
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}
	ms.PutCollection(&collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
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
	m.segments.SetSegment(segID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           segID,
		CollectionID: collID,
		PartitionID:  partID,
		State:        commonpb.SegmentState_Flushed,
		IsSorted:     true,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 0, ChildFields: []int64{100, 101}},
		},
	}})

	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, collID).
		Return(nil, errors.New("mock rootcoord unreachable")).Once()
	inspector := newIndexInspector(ctx, nil, m, nil, nil, handler, nil, nil)
	inspector.notifyIndexSegments(segID)

	inspector.processIndexSegments(ctx, false)

	assert.Empty(t, inspector.popIndexSegments(false), "a deferred segment must not ride on notifications")
	assert.ElementsMatch(t, []UniqueID{segID}, inspector.popIndexSegments(true))
}

func TestIndexInspector_ProcessIndexSegmentsCreatesIndex(t *testing.T) {
	ctx := context.Background()
	const (
		collID  = UniqueID(11)
		partID  = UniqueID(21)
		segID   = UniqueID(31)
		fieldID = UniqueID(101)
		indexID = UniqueID(41)
		buildID = UniqueID(51)
	)

	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()
	catalog := mocks2.NewDataCoordCatalog(t)
	ms := metacache.NewMetaStore(nil)
	m := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
		indexMeta: &indexMeta{
			keyLock:          lock.NewKeyLock[UniqueID](),
			catalog:          catalog,
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						CollectionID: collID,
						FieldID:      fieldID,
						IndexID:      indexID,
						IndexName:    "default_idx",
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}
	m.segments.SetSegment(segID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           segID,
		CollectionID: collID,
		PartitionID:  partID,
		NumOfRows:    1024,
		State:        commonpb.SegmentState_Flushed,
		IsSorted:     true,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: fieldID},
		},
	}})

	inspector := newIndexInspector(ctx, make(chan int64, 1), m, scheduler, alloc, handler, storageCli, versionManager)
	inspector.notifyIndexSegments(segID)
	handler.EXPECT().GetCollection(mock.Anything, collID).Return(&collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: fieldID, Name: "f101"},
		}},
	}, nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(buildID, nil).Once()
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
	scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

	inspector.processIndexSegments(ctx, false)

	assert.Empty(t, inspector.popIndexSegments(true))
	assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segID), indexID)
}

func TestIndexInspector_CreateIndexForSegment_FMIndexUsesMemoryBasedSlots(t *testing.T) {
	for _, tc := range []struct {
		name, enabled  string
		storageVersion int64
		expected       int32
	}{
		{"v3 manifest", "true", 3, 2},
		{"v3 legacy", "false", 3, 0},
		{"v2 manifest switch", "true", 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := paramtable.Get()
			assert.NoError(t, pt.Save(pt.DataCoordCfg.WriteSegmentIndexToManifest.Key, tc.enabled))
			defer pt.Reset(pt.DataCoordCfg.WriteSegmentIndexToManifest.Key)

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

			ms := metacache.NewMetaStore(nil)
			meta := &meta{
				segments:  NewSegmentsInfo(ms),
				metaStore: ms,
				indexMeta: &indexMeta{
					keyLock:          lock.NewKeyLock[UniqueID](),
					catalog:          catalog,
					segmentBuildInfo: newSegmentIndexBuildInfo(),
					indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
					segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
				},
			}

			segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				StorageVersion: tc.storageVersion,
				CollectionID:   2,
				PartitionID:    3,
				State:          commonpb.SegmentState_Flushed,
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
			catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Run(func(_ context.Context, index *model.SegmentIndex) {
				assert.Equal(t, tc.expected, int32(index.IndexStorePathVersion))
			}).Return(nil)
			scheduler.EXPECT().Enqueue(mock.Anything).Run(func(scheduled task.Task) {
				assert.Equal(t, int64(4), scheduled.GetTaskSlot())
			}).Return()

			assert.NoError(t, inspector.createIndexForSegment(ctx, segment, 5))
		})
	}
}

func TestIndexInspector_isExternalCollection(t *testing.T) {
	ctx := context.Background()
	notifyChan := make(chan int64, 1)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storageCli := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()

	ms := metacache.NewMetaStore(nil)
	m := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
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
		m.AddCollection(&collectionInfo{
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
		m.AddCollection(&collectionInfo{
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

	ms := metacache.NewMetaStore(nil)
	m := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
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
		m.AddCollection(&collectionInfo{
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
		m.AddCollection(&collectionInfo{
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

		handler.EXPECT().GetCollection(mock.Anything, int64(2)).Return(&collectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{Name: "id", FieldID: 101, DataType: schemapb.DataType_Int64, ExternalField: "id"}},
			},
		}, nil).Maybe()
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

	ms := metacache.NewMetaStore(nil)
	meta := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
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

func TestIndexInspector_FunctionOutputSchemaVersionGate(t *testing.T) {
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

	ms := metacache.NewMetaStore(nil)
	m := &meta{
		segments:  NewSegmentsInfo(ms),
		metaStore: ms,
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
	collInfo := &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
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
	m.AddCollection(collInfo)
	handler.EXPECT().GetCollection(mock.Anything, collID).Return(collInfo, nil).Maybe()

	t.Run("build function output index when collection has no schema change", func(t *testing.T) {
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
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		alloc.EXPECT().AllocID(mock.Anything).Return(int64(12346), nil).Once()
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
		scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(5))
	})

	t.Run("build non function output index", func(t *testing.T) {
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			7: {CollectionID: collID, FieldID: 103, IndexID: 7, IndexName: "new_vector_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             3,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		alloc.EXPECT().AllocID(mock.Anything).Return(int64(12347), nil).Once()
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
		scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(7))
	})

	// A segment behind the collection schema version defers every index,
	// including non-function fields.
	t.Run("skip whole segment when schema behind", func(t *testing.T) {
		collInfo.Schema.Version = 5
		defer func() { collInfo.Schema.Version = 0 }()
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			8: {CollectionID: collID, FieldID: 101, IndexID: 8, IndexName: "text_idx"},
			9: {CollectionID: collID, FieldID: 102, IndexID: 9, IndexName: "bm25_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             4,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  3,
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		segIndexes := m.indexMeta.GetSegmentIndexes(collID, segment.GetID())
		assert.NotContains(t, segIndexes, UniqueID(8))
		assert.NotContains(t, segIndexes, UniqueID(9))
	})

	t.Run("defer index build when schema is unresolvable", func(t *testing.T) {
		unresolvableCollID := int64(11)
		handler.EXPECT().GetCollection(mock.Anything, unresolvableCollID).
			Return(nil, errors.New("mock rootcoord unreachable")).Once()
		m.indexMeta.indexes[unresolvableCollID] = map[UniqueID]*model.Index{
			10: {CollectionID: unresolvableCollID, FieldID: 102, IndexID: 10, IndexName: "unresolvable_schema_idx"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           5,
				CollectionID: unresolvableCollID,
				State:        commonpb.SegmentState_Flushed,
				IsSorted:     true,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101}},
				},
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
				ID:           6,
				CollectionID: collID,
				State:        commonpb.SegmentState_Flushed,
				IsSorted:     true,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.NotContains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(11))
	})

	t.Run("create V3 function output index when segment schema is caught up", func(t *testing.T) {
		collInfo.Schema.Version = 5
		defer func() { collInfo.Schema.Version = 0 }()
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			12: {CollectionID: collID, FieldID: 102, IndexID: 12, IndexName: "bm25_idx_v3_caughtup"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             7,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  5,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		alloc.EXPECT().AllocID(mock.Anything).Return(int64(12349), nil).Once()
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
		scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(12))
	})

	t.Run("skip V3 function output index when segment schema is behind", func(t *testing.T) {
		collInfo.Schema.Version = 5
		defer func() { collInfo.Schema.Version = 0 }()
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			13: {CollectionID: collID, FieldID: 102, IndexID: 13, IndexName: "bm25_idx_v3_behind"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             8,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  3,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.NotContains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(13))
	})

	t.Run("create V3 function output index when segment schema is ahead", func(t *testing.T) {
		collInfo.Schema.Version = 5
		defer func() { collInfo.Schema.Version = 0 }()
		m.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			14: {CollectionID: collID, FieldID: 102, IndexID: 14, IndexName: "bm25_idx_v3_ahead"},
		}
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             9,
				CollectionID:   collID,
				State:          commonpb.SegmentState_Flushed,
				IsSorted:       true,
				StorageVersion: storage.StorageV3,
				SchemaVersion:  6,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 101}},
				},
			},
		}
		m.segments.SetSegment(segment.GetID(), segment)

		alloc.EXPECT().AllocID(mock.Anything).Return(int64(12350), nil).Once()
		catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
		scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()

		err := inspector.createIndexesForSegment(ctx, segment)
		assert.NoError(t, err)
		assert.Contains(t, m.indexMeta.GetSegmentIndexes(collID, segment.GetID()), UniqueID(14))
	})
}
