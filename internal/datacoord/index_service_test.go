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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestServerId(t *testing.T) {
	s := &Server{session: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 0}}}
	assert.Equal(t, int64(0), s.serverID())
}

func TestServer_CreateIndex(t *testing.T) {
	var (
		collID  = UniqueID(1)
		fieldID = UniqueID(10)
		// indexID    = UniqueID(100)
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		req = &indexpb.CreateIndexRequest{
			CollectionID:    collID,
			FieldID:         fieldID,
			IndexName:       "",
			TypeParams:      typeParams,
			IndexParams:     indexParams,
			Timestamp:       100,
			IsAutoIndex:     false,
			UserIndexParams: indexParams,
		}
		ctx = context.Background()
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil).Maybe()

	mock0Allocator := newMockAllocator(t)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, &collectionInfo{
		ID:             collID,
		Partitions:     nil,
		StartPositions: nil,
		Properties:     nil,
		CreatedAt:      0,
	})

	indexMeta := newSegmentIndexMeta(catalog)
	s := &Server{
		meta: &meta{
			catalog:     catalog,
			collections: collections,
			indexMeta:   indexMeta,
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}

	s.stateCode.Store(commonpb.StateCode_Healthy)

	b := mocks.NewMixCoord(t)

	t.Run("get field name failed", func(t *testing.T) {
		b.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))

		s.broker = broker.NewCoordinatorBroker(b)
		resp, err := s.CreateIndex(ctx, req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
		assert.Equal(t, "mock error", resp.GetReason())
	})

	b.ExpectedCalls = nil
	b.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Reason:    "",
			Code:      0,
			Retriable: false,
			Detail:    "",
		},
		Schema: &schemapb.CollectionSchema{
			Name:        "test_index",
			Description: "test index",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:        0,
					Name:           "pk",
					IsPrimaryKey:   false,
					Description:    "",
					DataType:       schemapb.DataType_Int64,
					TypeParams:     nil,
					IndexParams:    nil,
					AutoID:         false,
					State:          0,
					ElementType:    0,
					DefaultValue:   nil,
					IsDynamic:      false,
					IsPartitionKey: false,
				},
				{
					FieldID:        fieldID,
					Name:           "FieldFloatVector",
					IsPrimaryKey:   false,
					Description:    "",
					DataType:       schemapb.DataType_FloatVector,
					TypeParams:     nil,
					IndexParams:    nil,
					AutoID:         false,
					State:          0,
					ElementType:    0,
					DefaultValue:   nil,
					IsDynamic:      false,
					IsPartitionKey: false,
				},
				{
					FieldID:        fieldID + 1,
					Name:           "json",
					IsPrimaryKey:   false,
					Description:    "",
					DataType:       schemapb.DataType_JSON,
					TypeParams:     nil,
					IndexParams:    nil,
					AutoID:         false,
					State:          0,
					ElementType:    0,
					DefaultValue:   nil,
					IsDynamic:      false,
					IsPartitionKey: false,
				},
			},
			EnableDynamicField: false,
		},
		CollectionID: collID,
	}, nil)

	t.Run("success", func(t *testing.T) {
		resp, err := s.CreateIndex(ctx, req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("test json path", func(t *testing.T) {
		req := &indexpb.CreateIndexRequest{
			CollectionID:    collID,
			FieldID:         fieldID + 1,
			IndexName:       "",
			TypeParams:      typeParams,
			IndexParams:     indexParams,
			Timestamp:       100,
			IsAutoIndex:     false,
			UserIndexParams: indexParams,
		}
		req.IndexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.JSONPathKey,
				Value: "json",
			},
			{
				Key:   common.JSONCastTypeKey,
				Value: "double",
			},
			{
				Key:   common.IndexTypeKey,
				Value: "INVERTED",
			},
		}
		resp, err := s.CreateIndex(ctx, req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))

		indexes := s.meta.indexMeta.GetFieldIndexes(req.GetCollectionID(), req.GetFieldID(), req.GetIndexName())
		assert.Equal(t, 1, len(indexes))
		jsonPath, err := funcutil.GetAttrByKeyFromRepeatedKV(common.JSONPathKey, indexes[0].IndexParams)
		assert.NoError(t, err)
		assert.Equal(t, "", jsonPath)
	})

	t.Run("success with index exist", func(t *testing.T) {
		req.IndexName = ""
		resp, err := s.CreateIndex(ctx, req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("server not healthy", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := s.CreateIndex(ctx, req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
	})

	req.IndexName = "FieldFloatVector"
	t.Run("index not consistent", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Healthy)
		req.FieldID++
		resp, err := s.CreateIndex(ctx, req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("alloc ID fail", func(t *testing.T) {
		req.FieldID = fieldID
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocID(mock.Anything).Return(0, errors.New("mock")).Maybe()
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(0, nil).Maybe()
		s.allocator = alloc
		s.meta.indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{}
		resp, err := s.CreateIndex(ctx, req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("disk index", func(t *testing.T) {
		s.allocator = mock0Allocator
		s.meta.indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{}
		req.IndexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "DISKANN",
			},
		}
		s.indexNodeManager = session.NewNodeManager(ctx, defaultDataNodeCreatorFunc)
		resp, err := s.CreateIndex(ctx, req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("disk index with mmap", func(t *testing.T) {
		s.allocator = mock0Allocator
		s.meta.indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{}
		req.IndexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "DISKANN",
			},
			{
				Key:   common.MmapEnabledKey,
				Value: "true",
			},
		}
		nodeManager := session.NewNodeManager(ctx, defaultDataNodeCreatorFunc)
		s.indexNodeManager = nodeManager
		mockNode := mocks.NewMockDataNodeClient(t)
		nodeManager.SetClient(1001, mockNode)

		resp, err := s.CreateIndex(ctx, req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
	})

	t.Run("save index fail", func(t *testing.T) {
		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		s.meta.indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{}
		s.meta.catalog = &datacoord.Catalog{MetaKv: metakv}
		s.meta.indexMeta.catalog = s.meta.catalog
		req.IndexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		resp, err := s.CreateIndex(ctx, req)
		assert.Error(t, merr.CheckRPCCall(resp, err))
	})
}

func TestServer_AlterIndex(t *testing.T) {
	var (
		collID       = UniqueID(1)
		partID       = UniqueID(2)
		fieldID      = UniqueID(10)
		indexID      = UniqueID(100)
		segID        = UniqueID(1000)
		invalidSegID = UniqueID(1001)
		buildID      = UniqueID(10000)
		indexName    = "default_idx"
		typeParams   = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.AlterIndexRequest{
			CollectionID: collID,
			IndexName:    "default_idx",
			Params: []*commonpb.KeyValuePair{{
				Key:   common.MmapEnabledKey,
				Value: "true",
			}},
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("AlterIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	mock0Allocator := newMockAllocator(t)

	indexMeta := &indexMeta{
		catalog: catalog,
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				// finished
				indexID: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID,
					IndexID:         indexID,
					IndexName:       indexName,
					IsDeleted:       false,
					CreateTime:      createTS,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				// deleted
				indexID + 1: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 1,
					IndexID:         indexID + 1,
					IndexName:       indexName + "_1",
					IsDeleted:       true,
					CreateTime:      createTS,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				// unissued
				indexID + 2: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 2,
					IndexID:         indexID + 2,
					IndexName:       indexName + "_2",
					IsDeleted:       false,
					CreateTime:      createTS,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				// inProgress
				indexID + 3: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 3,
					IndexID:         indexID + 3,
					IndexName:       indexName + "_3",
					IsDeleted:       false,
					CreateTime:      createTS,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				// failed
				indexID + 4: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 4,
					IndexID:         indexID + 4,
					IndexName:       indexName + "_4",
					IsDeleted:       false,
					CreateTime:      createTS,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
				// unissued
				indexID + 5: {
					TenantID:        "",
					CollectionID:    collID,
					FieldID:         fieldID + 5,
					IndexID:         indexID + 5,
					IndexName:       indexName + "_5",
					IsDeleted:       false,
					CreateTime:      createTS,
					TypeParams:      typeParams,
					IndexParams:     indexParams,
					IsAutoIndex:     false,
					UserIndexParams: nil,
				},
			},
		},
		segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+1, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 1,
		BuildID:             buildID + 1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+3, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 3,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+4, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 4,
		BuildID:             buildID + 4,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "mock failed",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+5, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 5,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	indexMeta.segmentIndexes.Insert(segID, segIdx1)

	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID,
		BuildID:        buildID,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_Finished,
		CreatedUTCTime: createTS,
	})
	segIdx2.Insert(indexID+1, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID + 1,
		BuildID:        buildID + 1,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_Finished,
		CreatedUTCTime: createTS,
	})
	segIdx2.Insert(indexID+3, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID + 3,
		BuildID:        buildID + 3,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_InProgress,
		CreatedUTCTime: createTS,
	})
	segIdx2.Insert(indexID+4, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID + 4,
		BuildID:        buildID + 4,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_Failed,
		FailReason:     "mock failed",
		CreatedUTCTime: createTS,
	})
	segIdx2.Insert(indexID+5, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID + 5,
		BuildID:        buildID + 5,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_Finished,
		CreatedUTCTime: createTS,
	})
	indexMeta.segmentIndexes.Insert(segID-1, segIdx2)

	mockHandler := NewNMockHandler(t)

	mockGetCollectionInfo := func() {
		mockHandler.EXPECT().GetCollection(mock.Anything, collID).Return(&collectionInfo{
			ID: collID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  fieldID,
						Name:     "FieldFloatVector",
						DataType: schemapb.DataType_FloatVector,
					},
				},
			},
		}, nil).Once()
	}

	s := &Server{
		meta: &meta{
			catalog:   catalog,
			indexMeta: indexMeta,
			segments: &SegmentsInfo{
				compactionTo: make(map[int64][]int64),
				segments: map[UniqueID]*SegmentInfo{
					invalidSegID: {
						SegmentInfo: &datapb.SegmentInfo{
							ID:             invalidSegID,
							CollectionID:   collID,
							PartitionID:    partID,
							NumOfRows:      10000,
							State:          commonpb.SegmentState_Flushed,
							MaxRowNum:      65536,
							LastExpireTime: createTS,
							StartPosition: &msgpb.MsgPosition{
								// timesamp > index start time, will be filtered out
								Timestamp: createTS + 1,
							},
						},
					},
					segID: {
						SegmentInfo: &datapb.SegmentInfo{
							ID:             segID,
							CollectionID:   collID,
							PartitionID:    partID,
							NumOfRows:      10000,
							State:          commonpb.SegmentState_Flushed,
							MaxRowNum:      65536,
							LastExpireTime: createTS,
							StartPosition: &msgpb.MsgPosition{
								Timestamp: createTS,
							},
							CreatedByCompaction: true,
							CompactionFrom:      []int64{segID - 1},
						},
					},
					segID - 1: {
						SegmentInfo: &datapb.SegmentInfo{
							ID:             segID,
							CollectionID:   collID,
							PartitionID:    partID,
							NumOfRows:      10000,
							State:          commonpb.SegmentState_Dropped,
							MaxRowNum:      65536,
							LastExpireTime: createTS,
							StartPosition: &msgpb.MsgPosition{
								Timestamp: createTS,
							},
						},
					},
				},
			},
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
		handler:         mockHandler,
	}

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.AlterIndex(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrServiceNotReady)
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)

	t.Run("mmap_unsupported", func(t *testing.T) {
		mockGetCollectionInfo()
		indexParams[0].Value = "GPU_CAGRA"

		resp, err := s.AlterIndex(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

		indexParams[0].Value = "IVF_FLAT"
	})

	t.Run("param_value_invalied", func(t *testing.T) {
		mockGetCollectionInfo()
		req.Params[0].Value = "abc"
		resp, err := s.AlterIndex(ctx, req)
		assert.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

		req.Params[0].Value = "true"
	})

	t.Run("delete_params", func(t *testing.T) {
		mockGetCollectionInfo()
		deleteReq := &indexpb.AlterIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
			DeleteKeys:   []string{common.MmapEnabledKey},
		}
		resp, err := s.AlterIndex(ctx, deleteReq)
		assert.NoError(t, merr.CheckRPCCall(resp, err))

		describeResp, err := s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
			Timestamp:    createTS,
		})
		assert.NoError(t, merr.CheckRPCCall(describeResp, err))
		for _, param := range describeResp.IndexInfos[0].GetUserIndexParams() {
			assert.NotEqual(t, common.MmapEnabledKey, param.GetKey())
		}
	})
	t.Run("update_and_delete_params", func(t *testing.T) {
		mockGetCollectionInfo()
		updateAndDeleteReq := &indexpb.AlterIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
			Params: []*commonpb.KeyValuePair{
				{
					Key:   common.MmapEnabledKey,
					Value: "true",
				},
			},
			DeleteKeys: []string{common.MmapEnabledKey},
		}
		resp, err := s.AlterIndex(ctx, updateAndDeleteReq)
		assert.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	})

	t.Run("success", func(t *testing.T) {
		resp, err := s.AlterIndex(ctx, req)
		assert.NoError(t, merr.CheckRPCCall(resp, err))

		describeResp, err := s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    "default_idx",
			Timestamp:    createTS,
		})
		assert.NoError(t, merr.CheckRPCCall(describeResp, err))
		enableMmap, ok := common.IsMmapDataEnabled(describeResp.IndexInfos[0].GetUserIndexParams()...)
		assert.True(t, enableMmap, "indexInfo: %+v", describeResp.IndexInfos[0])
		assert.True(t, ok)
	})
}

func TestServer_GetIndexState(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		fieldID    = UniqueID(10)
		indexID    = UniqueID(100)
		segID      = UniqueID(1000)
		indexName  = "default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.GetIndexStateRequest{
			CollectionID: collID,
			IndexName:    "",
		}
	)
	mock0Allocator := newMockAllocator(t)
	s := &Server{
		meta: &meta{
			catalog:   &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
			indexMeta: newSegmentIndexMeta(&datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)
	t.Run("index not found", func(t *testing.T) {
		resp, err := s.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, resp.GetStatus().GetErrorCode())
	})

	segments := map[UniqueID]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "",
				NumOfRows:      10250,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS - 1,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS - 1,
				},
			},
			allocations:     nil,
			lastFlushTime:   time.Time{},
			isCompacting:    false,
			lastWrittenTime: time.Time{},
		},
	}
	s.meta = &meta{
		catalog: &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
		indexMeta: &indexMeta{
			catalog: &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						TenantID:        "",
						CollectionID:    collID,
						FieldID:         fieldID,
						IndexID:         indexID,
						IndexName:       indexName,
						IsDeleted:       false,
						CreateTime:      createTS,
						TypeParams:      typeParams,
						IndexParams:     indexParams,
						IsAutoIndex:     false,
						UserIndexParams: nil,
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},

		segments: NewSegmentsInfo(),
	}
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	t.Run("index state is unissued", func(t *testing.T) {
		resp, err := s.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.IndexState_InProgress, resp.GetState())
	})

	segments = map[UniqueID]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "",
				NumOfRows:      10250,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS - 1,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS - 1,
				},
			},
			allocations:     nil,
			lastFlushTime:   time.Time{},
			isCompacting:    false,
			lastWrittenTime: time.Time{},
		},
	}
	s.meta = &meta{
		catalog: &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
		indexMeta: &indexMeta{
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						TenantID:        "",
						CollectionID:    collID,
						FieldID:         fieldID,
						IndexID:         indexID,
						IndexName:       indexName,
						IsDeleted:       false,
						CreateTime:      createTS,
						TypeParams:      typeParams,
						IndexParams:     indexParams,
						IsAutoIndex:     false,
						UserIndexParams: nil,
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
		segments: NewSegmentsInfo(),
	}
	segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             3000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_IndexStateNone,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	s.meta.indexMeta.segmentIndexes.Insert(segID, segIdx)
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	t.Run("index state is none", func(t *testing.T) {
		resp, err := s.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, commonpb.IndexState_IndexStateNone, resp.GetState())
	})

	t.Run("ambiguous index name", func(t *testing.T) {
		s.meta.indexMeta.indexes[collID][indexID+1] = &model.Index{
			TenantID:        "",
			CollectionID:    collID,
			IndexID:         indexID + 1,
			IndexName:       "default_idx_1",
			IsDeleted:       false,
			CreateTime:      createTS,
			TypeParams:      typeParams,
			IndexParams:     indexParams,
			IsAutoIndex:     false,
			UserIndexParams: nil,
		}
		resp, err := s.GetIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func TestServer_GetSegmentIndexState(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		fieldID    = UniqueID(10)
		indexID    = UniqueID(100)
		segID      = UniqueID(1000)
		indexName  = "default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.GetSegmentIndexStateRequest{
			CollectionID: collID,
			IndexName:    "",
			SegmentIDs:   []UniqueID{segID},
		}
	)

	mock0Allocator := newMockAllocator(t)
	indexMeta := newSegmentIndexMeta(&datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)})

	s := &Server{
		meta: &meta{
			catalog:   indexMeta.catalog,
			indexMeta: indexMeta,
			segments:  NewSegmentsInfo(),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}

	t.Run("server is not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Abnormal)
		resp, err := s.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	t.Run("no indexes", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := s.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, resp.GetStatus().GetErrorCode())
	})

	t.Run("unfinished", func(t *testing.T) {
		s.meta.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      createTS,
				TypeParams:      typeParams,
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		}
		s.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			NumRows:             10250,
			IndexID:             indexID,
			BuildID:             10,
			NodeID:              0,
			IndexVersion:        1,
			IndexState:          commonpb.IndexState_InProgress,
			FailReason:          "",
			IsDeleted:           false,
			CreatedUTCTime:      createTS,
			IndexFileKeys:       []string{"file1", "file2"},
			IndexSerializedSize: 1025,
			WriteHandoff:        false,
		})
		s.meta.segments.SetSegment(segID, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "ch",
			},
			allocations:     nil,
			lastFlushTime:   time.Time{},
			isCompacting:    false,
			lastWrittenTime: time.Time{},
		})

		resp, err := s.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("finish", func(t *testing.T) {
		s.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			NumRows:             10250,
			IndexID:             indexID,
			BuildID:             10,
			NodeID:              0,
			IndexVersion:        1,
			IndexState:          commonpb.IndexState_Finished,
			FailReason:          "",
			IsDeleted:           false,
			CreatedUTCTime:      createTS,
			IndexFileKeys:       []string{"file1", "file2"},
			IndexSerializedSize: 1025,
			WriteHandoff:        false,
		})
		resp, err := s.GetSegmentIndexState(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestServer_GetIndexBuildProgress(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		fieldID    = UniqueID(10)
		indexID    = UniqueID(100)
		segID      = UniqueID(1000)
		indexName  = "default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.GetIndexBuildProgressRequest{
			CollectionID: collID,
			IndexName:    "",
		}
	)

	mock0Allocator := newMockAllocator(t)

	s := &Server{
		meta: &meta{
			catalog:   &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
			indexMeta: newSegmentIndexMeta(&datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)}),
			segments:  NewSegmentsInfo(),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}
	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	t.Run("no indexes", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Healthy)
		resp, err := s.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, resp.GetStatus().GetErrorCode())
	})

	t.Run("unissued", func(t *testing.T) {
		s.meta.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      createTS,
				TypeParams:      typeParams,
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		}
		s.meta.segments = NewSegmentsInfo()
		s.meta.segments.SetSegment(segID, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "",
				NumOfRows:      10250,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS,
				},
			},
			allocations:     nil,
			lastFlushTime:   time.Time{},
			isCompacting:    false,
			lastWrittenTime: time.Time{},
		})

		resp, err := s.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, int64(10250), resp.GetTotalRows())
		assert.Equal(t, int64(0), resp.GetIndexedRows())
	})

	t.Run("finish", func(t *testing.T) {
		s.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			PartitionID:         partID,
			NumRows:             10250,
			IndexID:             indexID,
			BuildID:             10,
			NodeID:              0,
			IndexVersion:        1,
			IndexState:          commonpb.IndexState_Finished,
			FailReason:          "",
			IsDeleted:           false,
			CreatedUTCTime:      createTS,
			IndexFileKeys:       []string{"file1", "file2"},
			IndexSerializedSize: 0,
			WriteHandoff:        false,
		})
		s.meta.segments = NewSegmentsInfo()
		s.meta.segments.SetSegment(segID, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				InsertChannel:  "",
				NumOfRows:      10250,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS,
				},
			},
			allocations:     nil,
			lastFlushTime:   time.Time{},
			isCompacting:    false,
			lastWrittenTime: time.Time{},
		})

		resp, err := s.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, int64(10250), resp.GetTotalRows())
		assert.Equal(t, int64(10250), resp.GetIndexedRows())
	})

	t.Run("multiple index", func(t *testing.T) {
		s.meta.indexMeta.indexes[collID] = map[UniqueID]*model.Index{
			indexID: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID,
				IndexID:         indexID,
				IndexName:       indexName,
				IsDeleted:       false,
				CreateTime:      createTS,
				TypeParams:      typeParams,
				IndexParams:     indexParams,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
			indexID + 1: {
				TenantID:        "",
				CollectionID:    collID,
				FieldID:         fieldID + 1,
				IndexID:         indexID + 1,
				IndexName:       "_default_idx_102",
				IsDeleted:       false,
				CreateTime:      0,
				TypeParams:      nil,
				IndexParams:     nil,
				IsAutoIndex:     false,
				UserIndexParams: nil,
			},
		}
		resp, err := s.GetIndexBuildProgress(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func TestServer_DescribeIndex(t *testing.T) {
	var (
		collID       = UniqueID(1)
		partID       = UniqueID(2)
		fieldID      = UniqueID(10)
		indexID      = UniqueID(100)
		segID        = UniqueID(1000)
		invalidSegID = UniqueID(1001)
		buildID      = UniqueID(10000)
		indexName    = "default_idx"
		typeParams   = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.DescribeIndexRequest{
			CollectionID: collID,
			IndexName:    "",
			Timestamp:    createTS,
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("AlterIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	mock0Allocator := newMockAllocator(t)

	segments := map[UniqueID]*SegmentInfo{
		invalidSegID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             invalidSegID,
				CollectionID:   collID,
				PartitionID:    partID,
				NumOfRows:      10000,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					// timesamp > index start time, will be filtered out
					Timestamp: createTS + 1,
				},
			},
		},
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				NumOfRows:      10000,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS,
				},
				CreatedByCompaction: true,
				CompactionFrom:      []int64{segID - 1},
			},
		},
		segID - 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID - 1,
				CollectionID:   collID,
				PartitionID:    partID,
				NumOfRows:      10000,
				State:          commonpb.SegmentState_Dropped,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS,
				},
			},
		},
	}
	s := &Server{
		meta: &meta{
			catalog: catalog,
			indexMeta: &indexMeta{
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						// finished
						indexID: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID,
							IndexName:       indexName,
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// deleted
						indexID + 1: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 1,
							IndexID:         indexID + 1,
							IndexName:       indexName + "_1",
							IsDeleted:       true,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 2: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 2,
							IndexID:         indexID + 2,
							IndexName:       indexName + "_2",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// inProgress
						indexID + 3: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 3,
							IndexID:         indexID + 3,
							IndexName:       indexName + "_3",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// failed
						indexID + 4: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 4,
							IndexID:         indexID + 4,
							IndexName:       indexName + "_4",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 5: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 5,
							IndexID:         indexID + 5,
							IndexName:       indexName + "_5",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},

			segments: NewSegmentsInfo(),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
		CurrentIndexVersion: 7,
	})
	segIdx1.Insert(indexID+1, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 1,
		BuildID:             buildID + 1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
		// deleted index
		CurrentIndexVersion: 6,
	})
	segIdx1.Insert(indexID+3, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 3,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+4, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 4,
		BuildID:             buildID + 4,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "mock failed",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+5, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 5,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	s.meta.indexMeta.segmentIndexes.Insert(segID, segIdx1)

	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID - 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		CreatedUTCTime:      createTS,
		CurrentIndexVersion: 6,
	})
	segIdx2.Insert(indexID+1, &model.SegmentIndex{
		SegmentID:           segID - 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 1,
		BuildID:             buildID + 1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		CreatedUTCTime:      createTS,
		CurrentIndexVersion: 6,
	})
	segIdx2.Insert(indexID+3, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID + 3,
		BuildID:        buildID + 3,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_InProgress,
		CreatedUTCTime: createTS,
	})
	segIdx2.Insert(indexID+4, &model.SegmentIndex{
		SegmentID:      segID - 1,
		CollectionID:   collID,
		PartitionID:    partID,
		NumRows:        10000,
		IndexID:        indexID + 4,
		BuildID:        buildID + 4,
		NodeID:         0,
		IndexVersion:   1,
		IndexState:     commonpb.IndexState_Failed,
		FailReason:     "mock failed",
		CreatedUTCTime: createTS,
	})
	segIdx2.Insert(indexID+5, &model.SegmentIndex{
		SegmentID:           segID - 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 5,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		CreatedUTCTime:      createTS,
		CurrentIndexVersion: 6,
	})
	s.meta.indexMeta.segmentIndexes.Insert(segID-1, segIdx2)

	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)

	t.Run("success", func(t *testing.T) {
		resp, err := s.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 5, len(resp.GetIndexInfos()))
		minIndexVersion := int32(math.MaxInt32)
		maxIndexVersion := int32(math.MinInt32)
		for _, indexInfo := range resp.GetIndexInfos() {
			if indexInfo.GetMinIndexVersion() < minIndexVersion {
				minIndexVersion = indexInfo.GetMinIndexVersion()
			}
			if indexInfo.GetMaxIndexVersion() > maxIndexVersion {
				maxIndexVersion = indexInfo.GetMaxIndexVersion()
			}
		}
		assert.Equal(t, int32(7), minIndexVersion)
		assert.Equal(t, int32(7), maxIndexVersion)
	})

	t.Run("describe after drop index", func(t *testing.T) {
		status, err := s.DropIndex(ctx, &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    "",
			DropAll:      true,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

		resp, err := s.DescribeIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, resp.GetStatus().GetErrorCode())
	})
}

func TestServer_ListIndexes(t *testing.T) {
	var (
		collID     = UniqueID(1)
		fieldID    = UniqueID(10)
		indexID    = UniqueID(100)
		indexName  = "default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.ListIndexesRequest{
			CollectionID: collID,
		}
	)

	mock0Allocator := newMockAllocator(t)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	s := &Server{
		meta: &meta{
			catalog: catalog,
			indexMeta: &indexMeta{
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						// finished
						indexID: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID,
							IndexName:       indexName,
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// deleted
						indexID + 1: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 1,
							IndexID:         indexID + 1,
							IndexName:       indexName + "_1",
							IsDeleted:       true,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 2: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 2,
							IndexID:         indexID + 2,
							IndexName:       indexName + "_2",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// inProgress
						indexID + 3: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 3,
							IndexID:         indexID + 3,
							IndexName:       indexName + "_3",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// failed
						indexID + 4: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 4,
							IndexID:         indexID + 4,
							IndexName:       indexName + "_4",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 5: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 5,
							IndexID:         indexID + 5,
							IndexName:       indexName + "_5",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},

			segments: NewSegmentsInfo(),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.ListIndexes(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)

	t.Run("success", func(t *testing.T) {
		resp, err := s.ListIndexes(ctx, req)
		assert.NoError(t, err)

		// assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 5, len(resp.GetIndexInfos()))
	})
}

func TestServer_GetIndexStatistics(t *testing.T) {
	var (
		collID       = UniqueID(1)
		partID       = UniqueID(2)
		fieldID      = UniqueID(10)
		indexID      = UniqueID(100)
		segID        = UniqueID(1000)
		invalidSegID = UniqueID(1001)
		buildID      = UniqueID(10000)
		indexName    = "default_idx"
		typeParams   = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.GetIndexStatisticsRequest{
			CollectionID: collID,
			IndexName:    "",
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("AlterIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	mock0Allocator := newMockAllocator(t)

	segments := map[UniqueID]*SegmentInfo{
		invalidSegID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				NumOfRows:      10000,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					// timesamp > index start time, will be filtered out
					Timestamp: createTS + 1,
				},
			},
		},
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   collID,
				PartitionID:    partID,
				NumOfRows:      10000,
				State:          commonpb.SegmentState_Flushed,
				MaxRowNum:      65536,
				LastExpireTime: createTS,
				StartPosition: &msgpb.MsgPosition{
					Timestamp: createTS,
				},
			},
		},
	}
	s := &Server{
		meta: &meta{
			catalog: catalog,
			indexMeta: &indexMeta{
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						// finished
						indexID: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID,
							IndexName:       indexName,
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// deleted
						indexID + 1: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 1,
							IndexID:         indexID + 1,
							IndexName:       indexName + "_1",
							IsDeleted:       true,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 2: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 2,
							IndexID:         indexID + 2,
							IndexName:       indexName + "_2",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// inProgress
						indexID + 3: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 3,
							IndexID:         indexID + 3,
							IndexName:       indexName + "_3",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// failed
						indexID + 4: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 4,
							IndexID:         indexID + 4,
							IndexName:       indexName + "_4",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 5: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 5,
							IndexID:         indexID + 5,
							IndexName:       indexName + "_5",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},

			segments: NewSegmentsInfo(),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+1, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 1,
		BuildID:             buildID + 1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+3, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 3,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+4, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 4,
		BuildID:             buildID + 4,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "mock failed",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx1.Insert(indexID+5, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID + 5,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	s.meta.indexMeta.segmentIndexes.Insert(segID, segIdx1)
	for id, segment := range segments {
		s.meta.segments.SetSegment(id, segment)
	}

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GetIndexStatistics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)

	t.Run("success", func(t *testing.T) {
		resp, err := s.GetIndexStatistics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 5, len(resp.GetIndexInfos()))
	})

	t.Run("describe after drop index", func(t *testing.T) {
		status, err := s.DropIndex(ctx, &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    "",
			DropAll:      true,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())

		resp, err := s.GetIndexStatistics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_IndexNotExist, resp.GetStatus().GetErrorCode())
	})
}

func TestServer_DropIndex(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		fieldID    = UniqueID(10)
		indexID    = UniqueID(100)
		segID      = UniqueID(1000)
		indexName  = "default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.DropIndexRequest{
			CollectionID: collID,
			IndexName:    indexName,
		}
	)

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.On("AlterIndexes",
		mock.Anything,
		mock.Anything,
	).Return(nil)

	mock0Allocator := newMockAllocator(t)

	s := &Server{
		meta: &meta{
			catalog: catalog,
			indexMeta: &indexMeta{
				catalog: catalog,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						// finished
						indexID: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID,
							IndexName:       indexName,
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// deleted
						indexID + 1: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 1,
							IndexID:         indexID + 1,
							IndexName:       indexName + "_1",
							IsDeleted:       true,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// unissued
						indexID + 2: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID + 2,
							IndexID:         indexID + 2,
							IndexName:       indexName + "_2",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// inProgress
						indexID + 3: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID + 3,
							IndexName:       indexName + "_3",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
						// failed
						indexID + 4: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID + 4,
							IndexName:       indexName + "_4",
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},

			segments: NewSegmentsInfo(),
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}

	s.meta.segments.SetSegment(segID, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   collID,
			PartitionID:    partID,
			NumOfRows:      10000,
			State:          commonpb.SegmentState_Flushed,
			MaxRowNum:      65536,
			LastExpireTime: createTS,
		},
	})

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp), merr.ErrServiceNotReady)
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)

	t.Run("drop fail", func(t *testing.T) {
		catalog := catalogmocks.NewDataCoordCatalog(t)
		catalog.On("AlterIndexes",
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))
		s.meta.indexMeta.catalog = catalog
		resp, err := s.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("drop one index", func(t *testing.T) {
		s.meta.indexMeta.catalog = catalog
		resp, err := s.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("drop one without indexName", func(t *testing.T) {
		req = &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    "",
			DropAll:      false,
		}
		resp, err := s.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetErrorCode())
	})

	t.Run("drop all indexes", func(t *testing.T) {
		req = &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    "",
			DropAll:      true,
		}
		resp, err := s.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("drop not exist index", func(t *testing.T) {
		req = &indexpb.DropIndexRequest{
			CollectionID: collID,
			PartitionIDs: nil,
			IndexName:    "",
			DropAll:      true,
		}
		resp, err := s.DropIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestServer_GetIndexInfos(t *testing.T) {
	var (
		collID     = UniqueID(1)
		partID     = UniqueID(2)
		fieldID    = UniqueID(10)
		indexID    = UniqueID(100)
		segID      = UniqueID(1000)
		buildID    = UniqueID(10000)
		indexName  = "default_idx"
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "128",
			},
		}
		indexParams = []*commonpb.KeyValuePair{
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
		}
		createTS = uint64(1000)
		ctx      = context.Background()
		req      = &indexpb.GetIndexInfoRequest{
			CollectionID: collID,
			SegmentIDs:   []UniqueID{segID},
			IndexName:    indexName,
		}
	)

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	mock0Allocator := newMockAllocator(t)

	s := &Server{
		meta: &meta{
			catalog: &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
			indexMeta: &indexMeta{
				catalog: &datacoord.Catalog{MetaKv: mockkv.NewMetaKv(t)},
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collID: {
						// finished
						indexID: {
							TenantID:        "",
							CollectionID:    collID,
							FieldID:         fieldID,
							IndexID:         indexID,
							IndexName:       indexName,
							IsDeleted:       false,
							CreateTime:      createTS,
							TypeParams:      typeParams,
							IndexParams:     indexParams,
							IsAutoIndex:     false,
							UserIndexParams: nil,
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			},

			segments:     NewSegmentsInfo(),
			chunkManager: cli,
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
	}
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             10000,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      createTS,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	s.meta.indexMeta.segmentIndexes.Insert(segID, segIdx1)
	s.meta.segments.SetSegment(segID, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   collID,
			PartitionID:    partID,
			NumOfRows:      10000,
			State:          commonpb.SegmentState_Flushed,
			MaxRowNum:      65536,
			LastExpireTime: createTS,
		},
	})

	t.Run("server not available", func(t *testing.T) {
		s.stateCode.Store(commonpb.StateCode_Initializing)
		resp, err := s.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
	})

	s.stateCode.Store(commonpb.StateCode_Healthy)
	t.Run("get segment index infos", func(t *testing.T) {
		resp, err := s.GetIndexInfos(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 1, len(resp.GetSegmentInfo()))
	})
}

func TestMeta_GetHasUnindexTaskSegments(t *testing.T) {
	segments := map[UniqueID]*SegmentInfo{
		segID: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1025,
				State:         commonpb.SegmentState_Flushed,
			},
		},
		segID + 1: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 1,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1025,
				State:         commonpb.SegmentState_Growing,
			},
		},
		segID + 2: {
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segID + 2,
				CollectionID:  collID,
				PartitionID:   partID,
				InsertChannel: "",
				NumOfRows:     1025,
				State:         commonpb.SegmentState_Dropped,
			},
		},
	}
	m := &meta{
		segments: NewSegmentsInfo(),
		indexMeta: &indexMeta{
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
					indexID + 1: {
						TenantID:        "",
						CollectionID:    collID,
						FieldID:         fieldID + 1,
						IndexID:         indexID + 1,
						IndexName:       indexName + "_1",
						IsDeleted:       false,
						CreateTime:      0,
						TypeParams:      nil,
						IndexParams:     nil,
						IsAutoIndex:     false,
						UserIndexParams: nil,
					},
				},
			},
		},
	}
	for id, segment := range segments {
		m.segments.SetSegment(id, segment)
	}
	s := &Server{meta: m}

	t.Run("normal", func(t *testing.T) {
		segments := s.getUnIndexTaskSegments(context.TODO())
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)

		m.indexMeta.segmentIndexes.Insert(segID, typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]())
		m.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID + 2,
			IndexState:   commonpb.IndexState_Finished,
		})
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)
	})

	t.Run("segment partial field with index", func(t *testing.T) {
		m.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID,
			IndexState:   commonpb.IndexState_Finished,
		})

		segments := s.getUnIndexTaskSegments(context.TODO())
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, segID, segments[0].ID)
	})

	t.Run("segment all vector field with index", func(t *testing.T) {
		m.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			CollectionID: collID,
			SegmentID:    segID,
			IndexID:      indexID + 1,
			IndexState:   commonpb.IndexState_Finished,
		})

		segments := s.getUnIndexTaskSegments(context.TODO())
		assert.Equal(t, 0, len(segments))
	})
}

func TestValidateIndexParams(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
				{
					Key:   common.MmapEnabledKey,
					Value: "true",
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.NoError(t, err)
	})

	t.Run("invalid index param", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
				{
					Key:   common.MmapEnabledKey,
					Value: "h",
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("invalid index user param", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MmapEnabledKey,
					Value: "h",
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("duplicated_index_params", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("duplicated_user_index_params", func(t *testing.T) {
		index := &model.Index{
			UserIndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("duplicated_user_index_params", func(t *testing.T) {
		index := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
				{
					Key:   common.IndexTypeKey,
					Value: indexparamcheck.AutoIndex,
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})
}

func TestJsonIndex(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
	mock0Allocator := newMockAllocator(t)
	indexMeta := newSegmentIndexMeta(catalog)
	b := mocks.NewMixCoord(t)
	b.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: 0,
			Code:      0,
		},
		Schema: &schemapb.CollectionSchema{
			Name: "test_index",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  0,
					Name:     "json",
					DataType: schemapb.DataType_JSON,
				},
				{
					FieldID:  1,
					Name:     "json2",
					DataType: schemapb.DataType_JSON,
				},
				{
					FieldID:   2,
					Name:      "dynamic",
					DataType:  schemapb.DataType_JSON,
					IsDynamic: true,
				},
			},
		},
	}, nil)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, &collectionInfo{
		ID: collID,
	})

	s := &Server{
		meta: &meta{
			catalog:     catalog,
			collections: collections,
			indexMeta:   indexMeta,
		},
		allocator:       mock0Allocator,
		notifyIndexChan: make(chan UniqueID, 1),
		broker:          broker.NewCoordinatorBroker(b),
	}
	s.stateCode.Store(commonpb.StateCode_Healthy)

	req := &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "a",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "varchar"}, {Key: common.JSONPathKey, Value: "json[\"a\"]"}},
	}
	resp, err := s.CreateIndex(context.Background(), req)
	assert.NoError(t, merr.CheckRPCCall(resp, err))

	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "varchar"}, {Key: common.JSONPathKey, Value: "json[\"c\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.NoError(t, merr.CheckRPCCall(resp, err))

	// different json field with same json path
	req = &indexpb.CreateIndexRequest{
		FieldID:     1,
		IndexName:   "",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "varchar"}, {Key: common.JSONPathKey, Value: "json2[\"c\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.NoError(t, merr.CheckRPCCall(resp, err))

	// duplicated index with same params
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "a",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "varchar"}, {Key: common.JSONPathKey, Value: "json[\"a\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.NoError(t, merr.CheckRPCCall(resp, err))

	// duplicated index with different cast type
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "a",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[\"a\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// duplicated index with different index name
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "b",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[\"a\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// another field json index with same index name
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "a",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[\"b\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// lack of json params
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "a",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONPathKey, Value: "json[\"a\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// incorrect field name in json path
	req = &indexpb.CreateIndexRequest{
		FieldID:     1,
		IndexName:   "c",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "bad_json[\"a\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// dynamic field
	req = &indexpb.CreateIndexRequest{
		FieldID:     2,
		IndexName:   "",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "dynamic_a_field"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.NoError(t, merr.CheckRPCCall(resp, err))

	// wrong path: missing quotes
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "d",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[a][\"b\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// wrong path: missing closing quote
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "e",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[\"a\"][\"b"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// wrong path: malformed brackets
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "f",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[\"a\"[\"b]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.Error(t, merr.CheckRPCCall(resp, err))

	// valid path with array index
	req = &indexpb.CreateIndexRequest{
		FieldID:     0,
		IndexName:   "g",
		IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "double"}, {Key: common.JSONPathKey, Value: "json[\"a\"][0][\"b\"]"}},
	}
	resp, err = s.CreateIndex(context.Background(), req)
	assert.NoError(t, merr.CheckRPCCall(resp, err))
}
