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

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	collID    = UniqueID(100)
	partID    = UniqueID(200)
	indexID   = UniqueID(300)
	fieldID   = UniqueID(400)
	indexName = "_default_idx"
	segID     = UniqueID(500)
	buildID   = UniqueID(600)
	nodeID    = UniqueID(700)
)

func createMetaTable(catalog metastore.DataCoordCatalog) *meta {
	return &meta{
		catalog: catalog,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   1,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MetricTypeKey,
							Value: "L2",
						},
					},
				},
			},
		},
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1025,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1025,
							IndexID:       indexID,
							BuildID:       buildID,
							NodeID:        0,
							IndexVersion:  0,
							IndexState:    commonpb.IndexState_Unissued,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    0,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 1,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 1,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 1,
							NodeID:        nodeID,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_InProgress,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 2,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 2,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 2,
							NodeID:        nodeID,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_InProgress,
							FailReason:    "",
							IsDeleted:     true,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 3: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 3,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 3,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       500,
							IndexID:       indexID,
							BuildID:       buildID + 3,
							NodeID:        0,
							IndexVersion:  0,
							IndexState:    commonpb.IndexState_Unissued,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 4: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 4,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 4,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 4,
							NodeID:        nodeID,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 5: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 5,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 5,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 5,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 6: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 6,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 6,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 6,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Finished,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 7: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 7,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 7,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 7,
							NodeID:        0,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_Failed,
							FailReason:    "error",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 8: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 8,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 8,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       1026,
							IndexID:       indexID,
							BuildID:       buildID + 8,
							NodeID:        nodeID + 1,
							IndexVersion:  1,
							IndexState:    commonpb.IndexState_InProgress,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 9: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 9,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 9,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       500,
							IndexID:       indexID,
							BuildID:       buildID + 9,
							NodeID:        0,
							IndexVersion:  0,
							IndexState:    commonpb.IndexState_Unissued,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
				segID + 10: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 10,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
					segmentIndexes: map[UniqueID]*model.SegmentIndex{
						indexID: {
							SegmentID:     segID + 10,
							CollectionID:  collID,
							PartitionID:   partID,
							NumRows:       500,
							IndexID:       indexID,
							BuildID:       buildID + 10,
							NodeID:        nodeID,
							IndexVersion:  0,
							IndexState:    commonpb.IndexState_Unissued,
							FailReason:    "",
							IsDeleted:     false,
							CreateTime:    1111,
							IndexFileKeys: nil,
							IndexSize:     0,
						},
					},
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1025,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 1: {
				SegmentID:     segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 1,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 2: {
				SegmentID:     segID + 2,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 2,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     true,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 3: {
				SegmentID:     segID + 3,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       500,
				IndexID:       indexID,
				BuildID:       buildID + 3,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 4: {
				SegmentID:     segID + 4,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 4,
				NodeID:        nodeID,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 5: {
				SegmentID:     segID + 5,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 5,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 6: {
				SegmentID:     segID + 6,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 6,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 7: {
				SegmentID:     segID + 7,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 7,
				NodeID:        0,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_Failed,
				FailReason:    "error",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 8: {
				SegmentID:     segID + 8,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1026,
				IndexID:       indexID,
				BuildID:       buildID + 8,
				NodeID:        nodeID + 1,
				IndexVersion:  1,
				IndexState:    commonpb.IndexState_InProgress,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 9: {
				SegmentID:     segID + 9,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       500,
				IndexID:       indexID,
				BuildID:       buildID + 9,
				NodeID:        0,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
			buildID + 10: {
				SegmentID:     segID + 10,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       500,
				IndexID:       indexID,
				BuildID:       buildID + 10,
				NodeID:        nodeID,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    1111,
				IndexFileKeys: nil,
				IndexSize:     0,
			},
		},
	}
}

func TestIndexBuilder(t *testing.T) {
	var (
		collID  = UniqueID(100)
		partID  = UniqueID(200)
		indexID = UniqueID(300)
		segID   = UniqueID(500)
		buildID = UniqueID(600)
		nodeID  = UniqueID(700)
	)

	Params.Init()
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("CreateSegmentIndex",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	catalog.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	mt := createMetaTable(catalog)
	nodeManager := &IndexNodeManager{
		ctx: ctx,
		nodeClients: map[UniqueID]types.IndexNode{
			4: indexnode.NewIndexNodeMock(),
		},
	}
	chunkManager := &mocks.ChunkManager{}
	chunkManager.EXPECT().RootPath().Return("root")

	ib := newIndexBuilder(ctx, mt, nodeManager, chunkManager)

	assert.Equal(t, 7, len(ib.tasks))
	assert.Equal(t, indexTaskInit, ib.tasks[buildID])
	assert.Equal(t, indexTaskInProgress, ib.tasks[buildID+1])
	assert.Equal(t, indexTaskInProgress, ib.tasks[buildID+2])
	assert.Equal(t, indexTaskInit, ib.tasks[buildID+3])
	assert.Equal(t, indexTaskInProgress, ib.tasks[buildID+8])
	assert.Equal(t, indexTaskInit, ib.tasks[buildID+9])
	assert.Equal(t, indexTaskInit, ib.tasks[buildID+10])

	ib.scheduleDuration = time.Millisecond * 500
	ib.Start()

	t.Run("enqueue", func(t *testing.T) {
		segIdx := &model.SegmentIndex{
			SegmentID:     segID + 10,
			CollectionID:  collID,
			PartitionID:   partID,
			NumRows:       1026,
			IndexID:       indexID,
			BuildID:       buildID + 10,
			NodeID:        0,
			IndexVersion:  0,
			IndexState:    0,
			FailReason:    "",
			IsDeleted:     false,
			CreateTime:    0,
			IndexFileKeys: nil,
			IndexSize:     0,
		}
		err := ib.meta.AddSegmentIndex(segIdx)
		assert.NoError(t, err)
		ib.enqueue(buildID + 10)
	})

	t.Run("node down", func(t *testing.T) {
		ib.nodeDown(nodeID)
	})

	for {
		ib.taskMutex.RLock()
		if len(ib.tasks) == 0 {
			break
		}
		ib.taskMutex.RUnlock()
	}
	ib.Stop()
}

func TestIndexBuilder_Error(t *testing.T) {
	Params.Init()

	sc := catalogmocks.NewDataCoordCatalog(t)
	sc.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)
	ec := catalogmocks.NewDataCoordCatalog(t)
	ec.On("AlterSegmentIndexes",
		mock.Anything,
		mock.Anything,
	).Return(errors.New("fail"))

	chunkManager := &mocks.ChunkManager{}
	chunkManager.EXPECT().RootPath().Return("root")
	ib := &indexBuilder{
		ctx: context.Background(),
		tasks: map[int64]indexTaskState{
			buildID: indexTaskInit,
		},
		meta:         createMetaTable(ec),
		chunkManager: chunkManager,
	}

	t.Run("meta not exist", func(t *testing.T) {
		ib.tasks[buildID+100] = indexTaskInit
		ib.process(buildID + 100)

		_, ok := ib.tasks[buildID+100]
		assert.False(t, ok)
	})

	t.Run("init no need to build index", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta.indexes[collID][indexID].IsDeleted = true
		ib.process(buildID)

		_, ok := ib.tasks[buildID]
		assert.False(t, ok)
		ib.meta.indexes[collID][indexID].IsDeleted = false
	})

	t.Run("finish few rows task fail", func(t *testing.T) {
		ib.tasks[buildID+9] = indexTaskInit
		ib.process(buildID + 9)

		state, ok := ib.tasks[buildID+9]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInit, state)
	})

	t.Run("peek client fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.nodeManager = &IndexNodeManager{nodeClients: map[UniqueID]types.IndexNode{}}
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInit, state)
	})

	t.Run("update version fail", func(t *testing.T) {
		ib.nodeManager = &IndexNodeManager{
			ctx:         context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{1: indexnode.NewIndexNodeMock()},
		}
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInit, state)
	})

	t.Run("assign task error", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "local")
		ib.tasks[buildID] = indexTaskInit
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallCreateJob: func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
						return nil, errors.New("error")
					},
					CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							TaskSlots: 1,
						}, nil
					},
				},
			},
		}
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskRetry, state)
	})
	t.Run("assign task fail", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "local")
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallCreateJob: func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
							Reason:    "mock fail",
						}, nil
					},
					CallGetJobStats: func(ctx context.Context, in *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
						return &indexpb.GetJobStatsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							TaskSlots: 1,
						}, nil
					},
				},
			},
		}
		ib.tasks[buildID] = indexTaskInit
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskRetry, state)
	})

	t.Run("drop job error", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
						}, errors.New("error")
					},
				},
			},
		}
		ib.tasks[buildID] = indexTaskDone
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskDone, state)

		ib.tasks[buildID] = indexTaskRetry
		ib.process(buildID)

		state, ok = ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskRetry, state)
	})

	t.Run("drop job fail", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
							Reason:    "mock fail",
						}, nil
					},
				},
			},
		}
		ib.tasks[buildID] = indexTaskDone
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskDone, state)

		ib.tasks[buildID] = indexTaskRetry
		ib.process(buildID)

		state, ok = ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskRetry, state)
	})

	t.Run("get state error", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return nil, errors.New("error")
					},
				},
			},
		}

		ib.tasks[buildID] = indexTaskInProgress
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInProgress, state)
	})

	t.Run("get state fail", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return &indexpb.QueryJobsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_BuildIndexError,
								Reason:    "mock fail",
							},
						}, nil
					},
				},
			},
		}

		ib.tasks[buildID] = indexTaskInProgress
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInProgress, state)
	})

	t.Run("finish task fail", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = ec
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return &indexpb.QueryJobsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							IndexInfos: []*indexpb.IndexTaskInfo{
								{
									BuildID:        buildID,
									State:          commonpb.IndexState_Finished,
									IndexFileKeys:  []string{"file1", "file2"},
									SerializedSize: 1024,
									FailReason:     "",
								},
							},
						}, nil
					},
				},
			},
		}

		ib.tasks[buildID] = indexTaskInProgress
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInProgress, state)
	})

	t.Run("task still in progress", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = ec
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return &indexpb.QueryJobsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							IndexInfos: []*indexpb.IndexTaskInfo{
								{
									BuildID:        buildID,
									State:          commonpb.IndexState_InProgress,
									IndexFileKeys:  nil,
									SerializedSize: 0,
									FailReason:     "",
								},
							},
						}, nil
					},
				},
			},
		}

		ib.tasks[buildID] = indexTaskInProgress
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskInProgress, state)
	})

	t.Run("indexNode has no task", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return &indexpb.QueryJobsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
								Reason:    "",
							},
							IndexInfos: nil,
						}, nil
					},
				},
			},
		}

		ib.tasks[buildID] = indexTaskInProgress
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskRetry, state)
	})

	t.Run("node not exist", func(t *testing.T) {
		ib.meta.buildID2SegmentIndex[buildID].NodeID = nodeID
		ib.meta.catalog = sc
		ib.nodeManager = &IndexNodeManager{
			ctx:         context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{},
		}

		ib.tasks[buildID] = indexTaskInProgress
		ib.process(buildID)

		state, ok := ib.tasks[buildID]
		assert.True(t, ok)
		assert.Equal(t, indexTaskRetry, state)
	})
}
