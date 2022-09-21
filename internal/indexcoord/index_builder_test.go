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

package indexcoord

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/assert"
)

func createMetaTable(catalog metastore.IndexCoordCatalog) *metaTable {
	return &metaTable{
		catalog: catalog,
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
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
							Key:   "dim",
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "metrics_type",
							Value: "L2",
						},
					},
				},
			},
		},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: {
					SegmentID:      segID,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1025,
					IndexID:        indexID,
					BuildID:        buildID,
					NodeID:         0,
					IndexVersion:   0,
					IndexState:     commonpb.IndexState_Unissued,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     0,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 1: {
				indexID: {
					SegmentID:      segID + 1,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 1,
					NodeID:         nodeID,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_InProgress,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 2: {
				indexID: {
					SegmentID:      segID + 2,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 2,
					NodeID:         nodeID,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_InProgress,
					FailReason:     "",
					IsDeleted:      true,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 3: {
				indexID: {
					SegmentID:      segID + 3,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        500,
					IndexID:        indexID,
					BuildID:        buildID + 3,
					NodeID:         0,
					IndexVersion:   0,
					IndexState:     commonpb.IndexState_Unissued,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 4: {
				indexID: {
					SegmentID:      segID + 4,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 4,
					NodeID:         nodeID,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_Finished,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 5: {
				indexID: {
					SegmentID:      segID + 5,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 5,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_Finished,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 6: {
				indexID: {
					SegmentID:      segID + 6,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 6,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_Finished,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 7: {
				indexID: {
					SegmentID:      segID + 7,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 7,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_Failed,
					FailReason:     "error",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 8: {
				indexID: {
					SegmentID:      segID + 8,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1026,
					IndexID:        indexID,
					BuildID:        buildID + 8,
					NodeID:         nodeID + 1,
					IndexVersion:   1,
					IndexState:     commonpb.IndexState_InProgress,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 9: {
				indexID: {
					SegmentID:      segID + 9,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        500,
					IndexID:        indexID,
					BuildID:        buildID + 9,
					NodeID:         0,
					IndexVersion:   0,
					IndexState:     commonpb.IndexState_Unissued,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
			segID + 10: {
				indexID: {
					SegmentID:      segID + 10,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        500,
					IndexID:        indexID,
					BuildID:        buildID + 10,
					NodeID:         nodeID,
					IndexVersion:   0,
					IndexState:     commonpb.IndexState_Unissued,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1111,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:      segID,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1025,
				IndexID:        indexID,
				BuildID:        buildID,
				NodeID:         0,
				IndexVersion:   0,
				IndexState:     commonpb.IndexState_Unissued,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     0,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 1: {
				SegmentID:      segID + 1,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 1,
				NodeID:         nodeID,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_InProgress,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 2: {
				SegmentID:      segID + 2,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 2,
				NodeID:         nodeID,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_InProgress,
				FailReason:     "",
				IsDeleted:      true,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 3: {
				SegmentID:      segID + 3,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        500,
				IndexID:        indexID,
				BuildID:        buildID + 3,
				NodeID:         0,
				IndexVersion:   0,
				IndexState:     commonpb.IndexState_Unissued,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 4: {
				SegmentID:      segID + 4,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 4,
				NodeID:         nodeID,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_Finished,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 5: {
				SegmentID:      segID + 5,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 5,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_Finished,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 6: {
				SegmentID:      segID + 6,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 6,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_Finished,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 7: {
				SegmentID:      segID + 7,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 7,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_Failed,
				FailReason:     "error",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 8: {
				SegmentID:      segID + 8,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1026,
				IndexID:        indexID,
				BuildID:        buildID + 8,
				NodeID:         nodeID + 1,
				IndexVersion:   1,
				IndexState:     commonpb.IndexState_InProgress,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 9: {
				SegmentID:      segID + 9,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        500,
				IndexID:        indexID,
				BuildID:        buildID + 9,
				NodeID:         0,
				IndexVersion:   0,
				IndexState:     commonpb.IndexState_Unissued,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
			buildID + 10: {
				SegmentID:      segID + 10,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        500,
				IndexID:        indexID,
				BuildID:        buildID + 10,
				NodeID:         nodeID,
				IndexVersion:   0,
				IndexState:     commonpb.IndexState_Unissued,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1111,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
		},
	}
}

func TestIndexBuilder(t *testing.T) {
	Params.Init()
	ctx := context.Background()

	ic := &IndexCoord{
		loopCtx:            ctx,
		reqTimeoutInterval: time.Second * 5,
		dataCoordClient:    NewDataCoordMock(),
		nodeManager: &NodeManager{
			ctx: ctx,
			nodeClients: map[UniqueID]types.IndexNode{
				4: indexnode.NewIndexNodeMock(),
			},
		},
		chunkManager: &chunkManagerMock{},
		etcdKV: &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
		},
	}

	ib := newIndexBuilder(ctx, ic, createMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
			multiSave: func(m map[string]string) error {
				return nil
			},
		},
	}), []UniqueID{nodeID})

	assert.Equal(t, 8, len(ib.tasks))
	assert.Equal(t, indexTaskInit, ib.tasks[buildID])
	assert.Equal(t, indexTaskInProgress, ib.tasks[buildID+1])
	assert.Equal(t, indexTaskDeleted, ib.tasks[buildID+2])
	assert.Equal(t, indexTaskInit, ib.tasks[buildID+3])
	assert.Equal(t, indexTaskDone, ib.tasks[buildID+4])
	assert.Equal(t, indexTaskRetry, ib.tasks[buildID+8])
	assert.Equal(t, indexTaskInit, ib.tasks[buildID+9])
	assert.Equal(t, indexTaskRetry, ib.tasks[buildID+10])

	ib.scheduleDuration = time.Millisecond * 500
	ib.Start()

	t.Run("enqueue", func(t *testing.T) {
		segIdx := &model.SegmentIndex{
			SegmentID:      segID + 10,
			CollectionID:   collID,
			PartitionID:    partID,
			NumRows:        1026,
			IndexID:        indexID,
			BuildID:        buildID + 10,
			NodeID:         0,
			IndexVersion:   0,
			IndexState:     0,
			FailReason:     "",
			IsDeleted:      false,
			CreateTime:     0,
			IndexFilePaths: nil,
			IndexSize:      0,
		}
		err := ib.meta.AddIndex(segIdx)
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
	ib := &indexBuilder{
		tasks: map[int64]indexTaskState{
			buildID: indexTaskInit,
		},
		meta: createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				save: func(s string, s2 string) error {
					return errors.New("error")
				},
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			}}),
		ic: &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{}, errors.New("error")
				},
			},
		},
	}

	t.Run("meta not exist", func(t *testing.T) {
		ib.tasks[buildID+100] = indexTaskInit
		ib.process(buildID + 100)
	})

	t.Run("init no need to build index", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta.collectionIndexes[collID][indexID].IsDeleted = true
		ib.process(buildID)
		ib.meta.collectionIndexes[collID][indexID].IsDeleted = false
	})

	t.Run("finish few rows task fail", func(t *testing.T) {
		ib.tasks[buildID+9] = indexTaskInit
		ib.process(buildID + 9)
	})

	t.Run("peek client fail", func(t *testing.T) {
		ib.ic.nodeManager = &NodeManager{nodeClients: map[UniqueID]types.IndexNode{}}
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.process(buildID)
	})

	t.Run("update version fail", func(t *testing.T) {
		ib.ic.nodeManager = &NodeManager{
			ctx:         context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{1: indexnode.NewIndexNodeMock()},
		}
		ib.process(buildID)
	})

	t.Run("acquire lock fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		dataMock := NewDataCoordMock()
		dataMock.CallAcquireSegmentLock = func(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
			return nil, errors.New("error")
		}
		ib.ic.dataCoordClient = dataMock
		ib.process(buildID)
	})

	t.Run("get segment info error", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		dataMock := NewDataCoordMock()
		dataMock.CallGetSegmentInfo = func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
			return nil, errors.New("error")
		}
		ib.ic.dataCoordClient = dataMock
		ib.process(buildID)
	})

	t.Run("get segment info fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		dataMock := NewDataCoordMock()
		dataMock.CallGetSegmentInfo = func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
			return &datapb.GetSegmentInfoResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
			}, nil
		}
		ib.ic.dataCoordClient = dataMock
		ib.process(buildID)
	})

	t.Run("assign task fail", func(t *testing.T) {
		Params.CommonCfg.StorageType = "local"
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		ib.ic = &IndexCoord{
			loopCtx:            context.Background(),
			reqTimeoutInterval: time.Second,
			nodeManager: &NodeManager{
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
			},
			chunkManager:    &chunkManagerMock{},
			dataCoordClient: NewDataCoordMock(),
		}
		ib.process(buildID)
	})

	t.Run("update index state inProgress fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallCreateJob: func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
						err := ib.meta.MarkSegmentsIndexAsDeletedByBuildID([]UniqueID{buildID})
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
						}, err
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
	})

	t.Run("update index state inProgress error", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
					CallCreateJob: func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
						ib.meta = createMetaTable(&indexcoord.Catalog{
							Txn: &mockETCDKV{
								multiSave: func(m map[string]string) error {
									return errors.New("error")
								},
							},
						})
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
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
		ib.process(buildID)
	})

	t.Run("no need to build index", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDone
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		ib.meta.collectionIndexes[collID][indexID].IsDeleted = true
		ib.process(buildID)
	})

	t.Run("drop index job error", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDone
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		err := ib.meta.UpdateVersion(buildID, nodeID)
		assert.NoError(t, err)
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						return nil, errors.New("error")
					},
				},
			},
		}
		ib.process(buildID)
	})

	t.Run("drop index job fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDone
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		err := ib.meta.UpdateVersion(buildID, nodeID)
		assert.NoError(t, err)
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
							Reason:    "fail reason",
						}, nil
					},
				},
			},
		}
		ib.process(buildID)
	})

	t.Run("release lock fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDone
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						}, nil
					},
				},
			},
		}
		ib.process(buildID)
	})

	t.Run("retry no need to build index", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskRetry
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		err := ib.meta.MarkIndexAsDeleted(collID, []UniqueID{indexID})
		assert.NoError(t, err)
		ib.process(buildID)
	})

	t.Run("retry release lock fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskRetry
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		ib.process(buildID)
	})

	t.Run("delete mark fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDeleted
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		ib.process(buildID)
	})

	t.Run("delete drop index job fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDeleted
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})
		err := ib.meta.UpdateVersion(buildID, nodeID)
		assert.NoError(t, err)
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						}, errors.New("error")
					},
				},
			},
		}
		ib.process(buildID)
	})

	t.Run("delete release lock fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDeleted
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})
		err := ib.meta.UpdateVersion(buildID, nodeID)
		assert.NoError(t, err)
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallDropJobs: func(ctx context.Context, in *indexpb.DropJobsRequest) (*commonpb.Status, error) {
						ib.meta.catalog = &indexcoord.Catalog{
							Txn: &mockETCDKV{
								multiSave: func(m map[string]string) error {
									return errors.New("error")
								},
							},
						}
						return &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						}, nil
					},
				},
			},
		}
		ib.process(buildID)
	})

	t.Run("deleted remove task", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskDeleted
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: NewMockEtcdKV(),
		})
		ib.process(buildID)
	})

	t.Run("finish task fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInProgress
		ib.meta = createMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				1: &indexnode.Mock{
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
									IndexFiles:     nil,
									SerializedSize: 0,
									FailReason:     "",
								},
							},
						}, nil
					},
				},
			},
		}
		ib.getTaskState(buildID, 1)
	})

	t.Run("inProgress no need to build index", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInProgress
		ib.meta = createMetaTable(&indexcoord.Catalog{Txn: NewMockEtcdKV()})
		err := ib.meta.MarkIndexAsDeleted(collID, []UniqueID{indexID})
		assert.NoError(t, err)
		ib.process(buildID)
	})
}

func Test_indexBuilder_getTaskState(t *testing.T) {
	Params.Init()
	ib := &indexBuilder{
		tasks: map[int64]indexTaskState{
			buildID: indexTaskInit,
		},
		meta: createMetaTable(&indexcoord.Catalog{Txn: NewMockEtcdKV()}),
		ic: &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{}, errors.New("error")
				},
			},
		},
	}

	t.Run("node not exist", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
			ctx:         context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{},
		}

		ib.getTaskState(buildID, nodeID)
	})

	t.Run("get state retry", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
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
									State:          commonpb.IndexState_Retry,
									IndexFiles:     nil,
									SerializedSize: 0,
									FailReason:     "create index fail",
								},
							},
						}, nil
					},
				},
			},
		}
		ib.getTaskState(buildID, nodeID)
	})

	t.Run("get state not exist", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
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
		ib.getTaskState(buildID, nodeID)
	})

	t.Run("query jobs error", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return nil, errors.New("error")
					},
				},
			},
		}
		ib.getTaskState(buildID, nodeID)
	})

	t.Run("query jobs fail", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
			ctx: context.Background(),
			nodeClients: map[UniqueID]types.IndexNode{
				nodeID: &indexnode.Mock{
					CallQueryJobs: func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
						return &indexpb.QueryJobsResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UnexpectedError,
								Reason:    "fail reason",
							},
						}, nil
					},
				},
			},
		}
		ib.getTaskState(buildID, nodeID)
	})

	t.Run("job is InProgress", func(t *testing.T) {
		ib.tasks[buildID] = indexTaskInit
		ib.ic.dataCoordClient = NewDataCoordMock()
		ib.ic.nodeManager = &NodeManager{
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
									IndexFiles:     nil,
									SerializedSize: 0,
									FailReason:     "",
								},
							},
						}, nil
					},
				},
			},
		}
		ib.getTaskState(buildID, nodeID)
	})
}

func Test_indexBuilder_releaseLockAndResetNode_error(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	ib := &indexBuilder{
		ctx:    ctx,
		cancel: cancel,
		tasks: map[int64]indexTaskState{
			buildID: indexTaskInit,
		},
		meta: createMetaTable(&indexcoord.Catalog{Txn: NewMockEtcdKV()}),
		ic: &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallReleaseSegmentLock: func(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
					return nil, errors.New("error")
				},
			},
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ib.releaseLockAndResetNode(buildID, nodeID)
		assert.Error(t, err)
	}()
	time.Sleep(time.Second)
	ib.cancel()
	wg.Wait()
}

func Test_indexBuilder_releaseLockAndResetTask_error(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	ib := &indexBuilder{
		ctx:    ctx,
		cancel: cancel,
		tasks: map[int64]indexTaskState{
			buildID: indexTaskInit,
		},
		meta: createMetaTable(&indexcoord.Catalog{Txn: NewMockEtcdKV()}),
		ic: &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallReleaseSegmentLock: func(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
					return nil, errors.New("error")
				},
			},
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ib.releaseLockAndResetTask(buildID, nodeID)
		assert.Error(t, err)
	}()
	time.Sleep(time.Second)
	ib.cancel()
	wg.Wait()
}
