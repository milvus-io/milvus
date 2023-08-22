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
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func createMetaForRecycleUnusedIndexes(catalog metastore.DataCoordCatalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		//partID = UniqueID(200)
		fieldID = UniqueID(300)
		indexID = UniqueID(400)
	)
	return &meta{
		RWMutex:      sync.RWMutex{},
		ctx:          ctx,
		catalog:      catalog,
		collections:  nil,
		segments:     nil,
		channelCPs:   nil,
		chunkManager: nil,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       "_default_idx",
					IsDeleted:       false,
					CreateTime:      10,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				indexID + 1: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 1,
					IndexID:         indexID + 1,
					IndexName:       "_default_idx_101",
					IsDeleted:       true,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
			collID + 1: {
				indexID + 10: {
					TenantID:        "",
					CollectionID:    collID + 1,
					FieldID:         fieldID + 10,
					IndexID:         indexID + 10,
					IndexName:       "index",
					IsDeleted:       true,
					CreateTime:      10,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		buildID2SegmentIndex: nil,
	}
}

func TestGarbageCollector_recycleUnusedIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedIndexes(catalog),
		}
		gc.recycleUnusedIndexes()
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedIndexes(catalog),
		}
		gc.recycleUnusedIndexes()
	})
}

func createMetaForRecycleUnusedSegIndexes(catalog metastore.DataCoordCatalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		//fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
	)
	return &meta{
		RWMutex:     sync.RWMutex{},
		ctx:         ctx,
		catalog:     catalog,
		collections: nil,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1026,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
				segID + 1: {
					SegmentInfo: nil,
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
			},
		},
		channelCPs:   nil,
		chunkManager: nil,
		indexes:      map[UniqueID]map[UniqueID]*model.Index{},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     0,
				WriteHandoff:  false,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}
}

func TestGarbageCollector_recycleUnusedSegIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropSegmentIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedSegIndexes(catalog),
		}
		gc.recycleUnusedSegIndexes()
	})

	t.Run("fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("DropSegmentIndex",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		gc := &garbageCollector{
			meta: createMetaForRecycleUnusedSegIndexes(catalog),
		}
		gc.recycleUnusedSegIndexes()
	})
}

func createMetaTableForRecycleUnusedIndexFiles(catalog *datacoord.Catalog) *meta {
	var (
		ctx    = context.Background()
		collID = UniqueID(100)
		partID = UniqueID(200)
		//fieldID = UniqueID(300)
		indexID = UniqueID(400)
		segID   = UniqueID(500)
		buildID = UniqueID(600)
	)
	return &meta{
		RWMutex:     sync.RWMutex{},
		ctx:         ctx,
		catalog:     catalog,
		collections: nil,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1026,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 1,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "",
						NumOfRows:     1026,
						State:         commonpb.SegmentState_Flushed,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_InProgress,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    10,
							IndexFileKeys: nil,
							IndexSize:     0,
							WriteHandoff:  false,
						},
					},
				},
			},
		},
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       "_default_idx",
					IsDeleted:       false,
					CreateTime:      10,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     0,
				WriteHandoff:  false,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    10,
				IndexFileKeys: nil,
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}
}

func TestGarbageCollector_recycleUnusedIndexFiles(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return([]string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"}, nil, nil)
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(nil)
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})

	t.Run("list fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.New("error"))
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})

	t.Run("remove fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("error"))
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return([]string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"}, nil, nil)
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(nil)
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})

	t.Run("remove with prefix fail", func(t *testing.T) {
		cm := &mocks.ChunkManager{}
		cm.EXPECT().RootPath().Return("root")
		cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(errors.New("error"))
		cm.EXPECT().ListWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return([]string{"a/b/c/", "a/b/600/", "a/b/601/", "a/b/602/"}, nil, nil)
		cm.EXPECT().RemoveWithPrefix(mock.Anything, mock.Anything).Return(errors.New("error"))
		gc := &garbageCollector{
			meta: createMetaTableForRecycleUnusedIndexFiles(&datacoord.Catalog{MetaKv: kvmocks.NewMetaKv(t)}),
			option: GcOption{
				cli: cm,
			},
		}
		gc.recycleUnusedIndexFiles()
	})
}

func TestGarbageCollector_clearETCD(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("ChannelExists",
		mock.Anything,
		mock.Anything,
	).Return(true)
	catalog.On("DropChannelCheckpoint",
		mock.Anything,
		mock.Anything,
	).Return(nil).Maybe()
	catalog.On("CreateSegmentIndex",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("DropSegment",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	m := &meta{
		catalog: catalog,
		channelCPs: map[string]*msgpb.MsgPosition{
			"dmlChannel": {
				Timestamp: 1000,
			},
		},
		segments: &SegmentsInfo{
			map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "dmlChannel",
						NumOfRows:     5000,
						State:         commonpb.SegmentState_Dropped,
						MaxRowNum:     65536,
						DroppedAt:     0,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 900,
						},
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       5000,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    0,
							IndexFileKeys: []string{"file1", "file2"},
							IndexSize:     1024,
							WriteHandoff:  false,
						},
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 1,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "dmlChannel",
						NumOfRows:     5000,
						State:         commonpb.SegmentState_Dropped,
						MaxRowNum:     65536,
						DroppedAt:     0,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 900,
						},
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       5000,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    0,
							IndexFileKeys: []string{"file3", "file4"},
							IndexSize:     1024,
							WriteHandoff:  false,
						},
					},
				},
				segID + 2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 2,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "dmlChannel",
						NumOfRows:     10000,
						State:         commonpb.SegmentState_Dropped,
						MaxRowNum:     65536,
						DroppedAt:     10,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 900,
						},
						CompactionFrom: []int64{segID, segID + 1},
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{},
				},
				segID + 3: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 3,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "dmlChannel",
						NumOfRows:     2000,
						State:         commonpb.SegmentState_Dropped,
						MaxRowNum:     65536,
						DroppedAt:     10,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 900,
						},
						CompactionFrom: nil,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{},
				},
				segID + 4: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            segID + 4,
						CollectionID:  collID,
						PartitionID:   partID,
						InsertChannel: "dmlChannel",
						NumOfRows:     12000,
						State:         commonpb.SegmentState_Flushed,
						MaxRowNum:     65536,
						DroppedAt:     10,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 900,
						},
						CompactionFrom: []int64{segID + 2, segID + 3},
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{},
				},
				segID + 5: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 5,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "dmlChannel",
						NumOfRows:      2000,
						State:          commonpb.SegmentState_Dropped,
						MaxRowNum:      65535,
						DroppedAt:      0,
						CompactionFrom: nil,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 1200,
						},
					},
				},
				segID + 6: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 6,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "dmlChannel",
						NumOfRows:      2000,
						State:          commonpb.SegmentState_Dropped,
						MaxRowNum:      65535,
						DroppedAt:      uint64(time.Now().Add(time.Hour).UnixNano()),
						CompactionFrom: nil,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 900,
						},
						Compacted: true,
					},
				},
				// compacted and child is GCed, dml pos is big than channel cp
				segID + 7: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 7,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "dmlChannel",
						NumOfRows:      2000,
						State:          commonpb.SegmentState_Dropped,
						MaxRowNum:      65535,
						DroppedAt:      0,
						CompactionFrom: nil,
						DmlPosition: &msgpb.MsgPosition{
							Timestamp: 1200,
						},
						Compacted: true,
					},
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       5000,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     1024,
				WriteHandoff:  false,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       5000,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: []string{"file3", "file4"},
				IndexSize:     1024,
				WriteHandoff:  false,
			},
		},
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      0,
					TypeParams:      nil,
					IndexParams:     nil,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		collections: map[UniqueID]*collectionInfo{
			collID: {
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Name:        "",
					Description: "",
					AutoID:      false,
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:      fieldID,
							Name:         "",
							IsPrimaryKey: false,
							Description:  "",
							DataType:     schemapb.DataType_FloatVector,
							TypeParams:   nil,
							IndexParams:  nil,
							AutoID:       false,
							State:        0,
						},
					},
				},
				Partitions:     nil,
				StartPositions: nil,
				Properties:     nil,
			},
		},
	}
	cm := &mocks.ChunkManager{}
	cm.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil)
	gc := &garbageCollector{
		option: GcOption{
			cli:           &mocks.ChunkManager{},
			dropTolerance: 1,
		},
		meta:    m,
		handler: newMockHandlerWithMeta(m),
	}
	gc.clearEtcd()

	/*
		A    B
		 \   /
		   C	D
		    \  /
		      E

		E: flushed, not indexed, should not be GCed
		D: dropped, not indexed, should not be GCed, since E is not GCed
		C: dropped, not indexed, should not be GCed, since E is not GCed
		A: dropped, indexed, should not be GCed, since C is not indexed
		B: dropped, indexed, should not be GCed, since C is not indexed

		F: dropped, compcated is false, should not be GCed, since dml position is larger than channel cp
		G: dropped, compacted is true, missing child info, should be GCed since dml pos is less than channel cp, FAST GC do not wait drop tolerance
		H: dropped, compacted is true, missing child info, should not be GCed since dml pos is larger than channel cp

		conclusion: only G is GCed.
	*/
	segA := gc.meta.GetSegment(segID)
	assert.NotNil(t, segA)
	segB := gc.meta.GetSegment(segID + 1)
	assert.NotNil(t, segB)
	segC := gc.meta.GetSegment(segID + 2)
	assert.NotNil(t, segC)
	segD := gc.meta.GetSegment(segID + 3)
	assert.NotNil(t, segD)
	segE := gc.meta.GetSegment(segID + 4)
	assert.NotNil(t, segE)
	segF := gc.meta.GetSegment(segID + 5)
	assert.NotNil(t, segF)
	segG := gc.meta.GetSegment(segID + 6)
	assert.Nil(t, segG)
	segH := gc.meta.GetSegment(segID + 7)
	assert.NotNil(t, segH)
	err := gc.meta.AddSegmentIndex(&model.SegmentIndex{
		SegmentID:    segID + 4,
		CollectionID: collID,
		PartitionID:  partID,
		NumRows:      12000,
		IndexID:      indexID,
		BuildID:      buildID + 4,
	})
	assert.NoError(t, err)

	err = gc.meta.FinishTask(&indexpb.IndexTaskInfo{
		BuildID:        buildID + 4,
		State:          commonpb.IndexState_Finished,
		IndexFileKeys:  []string{"file1", "file2", "file3", "file4"},
		SerializedSize: 10240,
		FailReason:     "",
	})
	assert.NoError(t, err)

	gc.clearEtcd()
	/*

		A: processed prior to C, C is not GCed yet and C is not indexed, A is not GCed in this turn
		B: processed prior to C, C is not GCed yet and C is not indexed, B is not GCed in this turn

		E: flushed, indexed, should not be GCed
		C: dropped, not indexed, should be GCed since E is indexed
		D: dropped, not indexed, should be GCed since E is indexed
	*/

	segC = gc.meta.GetSegment(segID + 2)
	assert.Nil(t, segC)
	segD = gc.meta.GetSegment(segID + 3)
	assert.Nil(t, segD)

	gc.clearEtcd()
	/*
		A: compacted became false due to C is GCed already, A should be GCed since dropTolernace is meet
		B: compacted became false due to C is GCed already, B should be GCed since dropTolerance is meet
	*/
	segA = gc.meta.GetSegment(segID)
	assert.Nil(t, segA)
	segB = gc.meta.GetSegment(segID + 1)
	assert.Nil(t, segB)
}

type GarbageCollectorConstructSuite struct {
	suite.Suite

	meta         *meta
	hander       *mockHandler
	chunkManager *mocks.ChunkManager
}

func (suite *GarbageCollectorConstructSuite) SetupTest() {
	meta, err := newMemoryMeta()
	suite.Require().NoError(err)

	suite.meta = meta

	handler := newMockHandler()
	suite.hander = handler

	cm := mocks.NewChunkManager(suite.T())
	suite.chunkManager = cm
}

func (suite *GarbageCollectorConstructSuite) TestBasic() {
	suite.Run("normal_construct", func() {
		gc := newGarbageCollector(suite.meta, suite.hander, GcOption{
			cli:              suite.chunkManager,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		gc.start()

		suite.NotPanics(func() {
			gc.close()
		})
	})

	suite.Run("with_nil_cli", func() {
		gc := newGarbageCollector(suite.meta, suite.hander, GcOption{
			cli:              nil,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
		})

		gc.start()

		suite.NotPanics(func() {
			gc.close()
		})
	})
}

type GarbageCollectionSuite struct {
	suite.Suite

	meta         *meta
	hander       *mockHandler
	chunkManager *mocks.ChunkManager

	gc *garbageCollector
}

func (suite *GarbageCollectionSuite) SetupTest() {
	meta, err := newMemoryMeta()
	suite.Require().NoError(err)

	suite.meta = meta

	handler := newMockHandler()
	suite.hander = handler

	cm := mocks.NewChunkManager(suite.T())
	suite.chunkManager = cm

	suite.gc = newGarbageCollector(suite.meta, suite.hander, GcOption{
		cli:              suite.chunkManager,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		missingTolerance: time.Hour * 24,
		dropTolerance:    time.Hour * 24,
	})
}

func (suite *GarbageCollectionSuite) setupMeta(segments []*SegmentInfo) {
	meta := suite.meta
	meta.Lock()
	defer meta.Unlock()

	meta.segments = NewSegmentsInfo()
	lo.ForEach(segments, func(info *SegmentInfo, _ int) {
		meta.segments.SetSegment(info.GetID(), info)
	})
}

func (suite *GarbageCollectionSuite) composeBinlogPath(collID, partID, segmentID, fieldID, logID int64) string {
	return fmt.Sprintf("%d/%d/%d/%d/%d", collID, partID, segmentID, fieldID, logID)
}

func (suite *GarbageCollectionSuite) composeDeltalogPath(collID, partID, segmentID, logID int64) string {
	return fmt.Sprintf("%d/%d/%d/%d", collID, partID, segmentID, logID)
}

func (suite *GarbageCollectionSuite) TestScan() {
	type testCase struct {
		tag       string
		segmentID int64
		binlogs   []int64
		statslogs []int64
		deltalogs []int64

		storageBinlogs   []string
		storageStatlogs  []string
		storageDeltalogs []string

		expectRemoves []string
		lastModified  time.Duration
	}

	rootPath := fmt.Sprintf(`gc_%s`, funcutil.RandomString(8))
	cases := []testCase{
		{
			tag:       "all_valid_files",
			segmentID: 1000,
			binlogs:   []int64{1234},
			statslogs: []int64{1235},
			deltalogs: []int64{1236},
			storageBinlogs: []string{
				suite.composeBinlogPath(100, 101, 1000, 100, 1234),
			},
			storageStatlogs: []string{
				suite.composeBinlogPath(100, 101, 1000, 100, 1235),
			},
			storageDeltalogs: []string{
				suite.composeDeltalogPath(100, 101, 1000, 1236),
			},
			expectRemoves: nil,
			lastModified:  0,
		},
		{
			tag:       "miss_within_tolerance",
			segmentID: 1001,
			storageBinlogs: []string{
				suite.composeBinlogPath(100, 101, 1001, 100, 1234),
			},
			storageStatlogs: []string{
				suite.composeBinlogPath(100, 101, 1001, 100, 1235),
			},
			storageDeltalogs: []string{
				suite.composeDeltalogPath(100, 101, 1001, 1236),
			},
			expectRemoves: nil,
			lastModified:  0,
		},
		{
			tag:       "missing_files_gc",
			segmentID: 1002,
			storageBinlogs: []string{
				suite.composeBinlogPath(100, 101, 1001, 100, 1234),
			},
			storageStatlogs: []string{
				suite.composeBinlogPath(100, 101, 1001, 100, 1235),
			},
			storageDeltalogs: []string{
				suite.composeDeltalogPath(100, 101, 1001, 1236),
			},
			expectRemoves: []string{
				path.Join(rootPath, insertLogPrefix, suite.composeBinlogPath(100, 101, 1001, 100, 1234)),
				path.Join(rootPath, statsLogPrefix, suite.composeBinlogPath(100, 101, 1001, 100, 1235)),
				path.Join(rootPath, deltaLogPrefix, suite.composeDeltalogPath(100, 101, 1001, 1236)),
			},
			lastModified: -suite.gc.option.missingTolerance - time.Second,
		},
		{
			tag:       "noise_files",
			segmentID: 1003,
			storageBinlogs: []string{
				"other_path/shall/not/gc",
			},
			expectRemoves: nil,
			lastModified:  -suite.gc.option.missingTolerance - time.Second,
		},
	}

	for _, tc := range cases {
		suite.Run(tc.tag, func() {
			defer func() {
				suite.chunkManager.AssertExpectations(suite.T())
				suite.chunkManager.ExpectedCalls = nil
				suite.chunkManager.Calls = nil
				meta := suite.meta
				meta.Lock()
				defer meta.Unlock()
				meta.segments.DropSegment(tc.segmentID)
			}()
			suite.setupMeta([]*SegmentInfo{
				NewSegmentInfo(&datapb.SegmentInfo{
					ID: tc.segmentID,
					Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: lo.Map(tc.binlogs, func(logID int64, _ int) *datapb.Binlog {
								return &datapb.Binlog{LogID: logID, LogPath: path.Join(rootPath, insertLogPrefix, suite.composeBinlogPath(100, 101, tc.segmentID, 100, logID))}
							}),
						},
					},
					Statslogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: lo.Map(tc.statslogs, func(logID int64, _ int) *datapb.Binlog {
								return &datapb.Binlog{LogID: logID, LogPath: path.Join(rootPath, statsLogPrefix, suite.composeBinlogPath(100, 101, tc.segmentID, 100, logID))}
							}),
						},
					},
					Deltalogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: lo.Map(tc.deltalogs, func(logID int64, _ int) *datapb.Binlog {
								return &datapb.Binlog{LogID: logID, LogPath: path.Join(rootPath, deltaLogPrefix, suite.composeDeltalogPath(100, 101, tc.segmentID, logID))}
							}),
						},
					},
				}),
			})

			suite.chunkManager.EXPECT().RootPath().Return(rootPath)

			setupList := func(prefix string, logs []string) {
				suite.chunkManager.EXPECT().ListWithPrefix(
					mock.Anything,
					fmt.Sprintf("%s/%s", rootPath, prefix),
					true,
				).Return(lo.Map(logs, func(p string, _ int) string {
					return path.Join(rootPath, prefix, p)
				}), lo.Map(logs, func(_ string, _ int) time.Time {
					return time.Now().Add(tc.lastModified)
				}), nil)
			}
			setupList(insertLogPrefix, tc.storageBinlogs)
			setupList(statsLogPrefix, tc.storageStatlogs)
			setupList(deltaLogPrefix, tc.storageDeltalogs)
			lo.ForEach(tc.expectRemoves, func(p string, _ int) {
				suite.chunkManager.EXPECT().Remove(mock.Anything, p).Return(nil)
			})

			suite.NotPanics(func() {
				suite.gc.scan()
			}, "garbage collection shall not panic during scan")

			if len(tc.expectRemoves) == 0 {
				suite.chunkManager.AssertNotCalled(suite.T(), "Remove", mock.Anything, mock.Anything)
			}
		})
	}
}

func (suite *GarbageCollectionSuite) TestScan_Failures() {

	rootPath := fmt.Sprintf(`gc_%s`, funcutil.RandomString(8))
	suite.Run("list_fails", func() {
		defer func() {
			suite.chunkManager.AssertExpectations(suite.T())
			suite.chunkManager.ExpectedCalls = nil
			suite.chunkManager.Calls = nil
		}()

		suite.chunkManager.EXPECT().RootPath().Return(rootPath)
		suite.chunkManager.EXPECT().ListWithPrefix(mock.Anything, mock.AnythingOfType("string"), true).Return(nil, nil, errors.New("mocked"))

		suite.NotPanics(func() {
			suite.gc.scan()
		}, "garbage collection shall not panic during scan")

		suite.chunkManager.AssertNotCalled(suite.T(), "Remove", mock.Anything, mock.Anything)
	})

	suite.Run("remove_fails", func() {
		defer func() {
			suite.chunkManager.AssertExpectations(suite.T())
			suite.chunkManager.ExpectedCalls = nil
			suite.chunkManager.Calls = nil
		}()

		suite.chunkManager.EXPECT().RootPath().Return(rootPath)
		suite.chunkManager.EXPECT().ListWithPrefix(mock.Anything, mock.AnythingOfType("string"), true).Return([]string{
			path.Join(rootPath, "wild_cast", "100/101/1000/100/1234"),
		}, []time.Time{{}}, nil)

		suite.chunkManager.EXPECT().Remove(mock.Anything, path.Join(rootPath, "wild_cast", "100/101/1000/100/1234")).Return(errors.New("mocked"))

		suite.NotPanics(func() {
			suite.gc.scan()
		}, "garbage collection shall not panic during scan")
	})
}

func (suite *GarbageCollectionSuite) TestCleanEtcd() {

}

func TestGarbageCollector(t *testing.T) {
	suite.Run(t, new(GarbageCollectorConstructSuite))
	suite.Run(t, new(GarbageCollectionSuite))
}
