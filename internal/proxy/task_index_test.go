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

package proxy

import (
	"context"
	"os"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	gin.SetMode(gin.TestMode)
	code := m.Run()
	os.Exit(code)
}

func sortKeyValuePairs(pairs []*commonpb.KeyValuePair) {
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key > pairs[j].Key
	})
}

func TestGetIndexStateTask_Execute(t *testing.T) {
	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	fieldName := funcutil.GenRandomStr()
	indexName := ""
	ctx := context.Background()

	rootCoord := newMockRootCoord()
	queryCoord := getMockQueryCoord()
	datacoord := NewDataCoordMock()

	gist := &getIndexStateTask{
		GetIndexStateRequest: &milvuspb.GetIndexStateRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      fieldName,
			IndexName:      indexName,
		},
		ctx:       ctx,
		rootCoord: rootCoord,
		dataCoord: datacoord,
		result: &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock-1"},
			State:  commonpb.IndexState_Unissued,
		},
		collectionID: collectionID,
	}

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	err := InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.NoError(t, err)
	assert.Error(t, gist.Execute(ctx))

	rootCoord.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:         merr.Success(),
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	rootCoord.ShowPartitionsFunc = func(ctx context.Context, request *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Success(),
		}, nil
	}

	datacoord.GetIndexStateFunc = func(ctx context.Context, request *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
		return &indexpb.GetIndexStateResponse{
			Status:     merr.Success(),
			State:      commonpb.IndexState_Finished,
			FailReason: "",
		}, nil
	}

	assert.NoError(t, gist.Execute(ctx))
	assert.Equal(t, commonpb.IndexState_Finished, gist.result.GetState())
}

func TestDropIndexTask_PreExecute(t *testing.T) {
	collectionName := "collection1"
	collectionID := UniqueID(1)
	fieldName := "field1"
	indexName := "_default_idx_101"

	paramtable.Init()
	qc := getMockQueryCoord()
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status:        merr.Success(),
		CollectionIDs: []int64{},
	}, nil)
	dc := NewDataCoordMock()
	ctx := context.Background()

	mockCache := NewMockCache(t)
	mockCache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)
	globalMetaCache = mockCache

	dit := dropIndexTask{
		ctx: ctx,
		DropIndexRequest: &milvuspb.DropIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
				TargetID:  0,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
			IndexName:      indexName,
		},
		dataCoord:    dc,
		queryCoord:   qc,
		result:       nil,
		collectionID: collectionID,
	}

	t.Run("normal", func(t *testing.T) {
		err := dit.PreExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("get collectionID error", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(UniqueID(0), errors.New("error"))
		globalMetaCache = mockCache
		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	mockCache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)
	globalMetaCache = mockCache

	t.Run("coll has been loaded", func(t *testing.T) {
		qc := getMockQueryCoord()
		qc.ExpectedCalls = nil
		qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        merr.Success(),
			CollectionIDs: []int64{collectionID},
		}, nil)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection error", func(t *testing.T) {
		qc := getMockQueryCoord()
		qc.ExpectedCalls = nil
		qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        merr.Success(),
			CollectionIDs: []int64{collectionID},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection fail", func(t *testing.T) {
		qc := getMockQueryCoord()
		qc.ExpectedCalls = nil
		qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status:        merr.Success(),
			CollectionIDs: []int64{collectionID},
		}, nil)
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "fail reason",
			},
		}, nil)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func getMockQueryCoord() *mocks.MockQueryCoordClient {
	qc := &mocks.MockQueryCoordClient{}
	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(successStatus, nil)
	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: "channel-1",
				NodeIds:     []int64{1, 2, 3},
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
			},
		},
	}, nil)
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	return qc
}

func TestCreateIndexTask_PreExecute(t *testing.T) {
	collectionName := "collection1"
	collectionID := UniqueID(1)
	fieldName := newTestSchema().Fields[0].Name

	dc := NewDataCoordMock()
	ctx := context.Background()

	mockCache := NewMockCache(t)
	mockCache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)
	mockCache.On("GetCollectionSchema",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(newSchemaInfo(newTestSchema()), nil)

	globalMetaCache = mockCache

	cit := createIndexTask{
		ctx: ctx,
		req: &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_CreateIndex,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
		datacoord:    dc,
		result:       nil,
		collectionID: collectionID,
	}

	t.Run("normal", func(t *testing.T) {
		err := cit.PreExecute(ctx)
		assert.NoError(t, err)
	})
}

func Test_sparse_parseIndexParams(t *testing.T) {
	cit := &createIndexTask{
		Condition: nil,
		req: &milvuspb.CreateIndexRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: "",
			FieldName:      "",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "SPARSE_INVERTED_INDEX",
				},
				{
					Key:   MetricTypeKey,
					Value: "IP",
				},
				{
					Key:   common.IndexParamsKey,
					Value: "{\"drop_ratio_build\": 0.3}",
				},
			},
			IndexName: "",
		},
		ctx:            nil,
		rootCoord:      nil,
		result:         nil,
		isAutoIndex:    false,
		newIndexParams: nil,
		newTypeParams:  nil,
		collectionID:   0,
		fieldSchema: &schemapb.FieldSchema{
			FieldID:      101,
			Name:         "FieldID",
			IsPrimaryKey: false,
			Description:  "field no.1",
			DataType:     schemapb.DataType_SparseFloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   MetricTypeKey,
					Value: "IP",
				},
			},
		},
	}

	t.Run("parse index params", func(t *testing.T) {
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)

		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "SPARSE_INVERTED_INDEX",
				},
				{
					Key:   MetricTypeKey,
					Value: "IP",
				},
				{
					Key:   "drop_ratio_build",
					Value: "0.3",
				},
			}, cit.newIndexParams)
		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{}, cit.newTypeParams)
	})
}

func Test_parseIndexParams(t *testing.T) {
	cit := &createIndexTask{
		Condition: nil,
		req: &milvuspb.CreateIndexRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: "",
			FieldName:      "",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "HNSW",
				},
				{
					Key:   MetricTypeKey,
					Value: "IP",
				},
				{
					Key:   common.IndexParamsKey,
					Value: "{\"M\": 48, \"efConstruction\": 64}",
				},
				{
					Key:   DimKey,
					Value: "128",
				},
			},
			IndexName: "",
		},
		ctx:            nil,
		rootCoord:      nil,
		result:         nil,
		isAutoIndex:    false,
		newIndexParams: nil,
		newTypeParams:  nil,
		collectionID:   0,
		fieldSchema: &schemapb.FieldSchema{
			FieldID:      101,
			Name:         "FieldID",
			IsPrimaryKey: false,
			Description:  "field no.1",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   DimKey,
					Value: "128",
				},
				{
					Key:   MetricTypeKey,
					Value: "L2",
				},
			},
		},
	}

	t.Run("parse index params", func(t *testing.T) {
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)

		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "HNSW",
				},
				{
					Key:   MetricTypeKey,
					Value: "IP",
				},
				{
					Key:   "M",
					Value: "48",
				},
				{
					Key:   "efConstruction",
					Value: "64",
				},
			}, cit.newIndexParams)
		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{
				{
					Key:   DimKey,
					Value: "128",
				},
			}, cit.newTypeParams)
	})

	cit2 := &createIndexTask{
		Condition: nil,
		req: &milvuspb.CreateIndexRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: "",
			FieldName:      "",
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "IVF_FLAT",
				},
				{
					Key:   MetricTypeKey,
					Value: "L2",
				},
				{
					Key:   common.IndexParamsKey,
					Value: "{\"nlist\": 100}",
				},
				{
					Key:   DimKey,
					Value: "128",
				},
			},
			IndexName: "",
		},
		ctx:            nil,
		rootCoord:      nil,
		result:         nil,
		isAutoIndex:    false,
		newIndexParams: nil,
		newTypeParams:  nil,
		collectionID:   0,
		fieldSchema: &schemapb.FieldSchema{
			FieldID:      101,
			Name:         "FieldID",
			IsPrimaryKey: false,
			Description:  "field no.1",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   DimKey,
					Value: "128",
				},
				{
					Key:   MetricTypeKey,
					Value: "L2",
				},
			},
		},
	}
	t.Run("parse index params 2", func(t *testing.T) {
		Params.Save(Params.AutoIndexConfig.Enable.Key, "true")
		indexParams := map[string]any{
			common.IndexTypeKey: "HNSW",
			"M":                 10,
			"efConstruction":    100,
		}
		indexParamsStr, err := json.Marshal(indexParams)
		assert.NoError(t, err)
		Params.Save(Params.AutoIndexConfig.IndexParams.Key, string(indexParamsStr))
		err = cit2.parseIndexParams(context.TODO())
		assert.NoError(t, err)

		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: "HNSW",
				},
				{
					Key:   MetricTypeKey,
					Value: "L2",
				},
				{
					Key:   "M",
					Value: "10",
				},
				{
					Key:   "efConstruction",
					Value: "100",
				},
				{
					Key:   "nlist",
					Value: "100",
				},
			}, cit2.newIndexParams)
		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{
				{
					Key:   DimKey,
					Value: "128",
				},
			}, cit2.newTypeParams)
	})
	t.Run("create index on json field", func(t *testing.T) {
		cit3 := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				Base:           nil,
				DbName:         "",
				CollectionName: "",
				FieldName:      "",
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "HNSW",
					},
					{
						Key:   MetricTypeKey,
						Value: "IP",
					},
					{
						Key:   common.IndexParamsKey,
						Value: "{\"M\": 48, \"efConstruction\": 64}",
					},
					{
						Key:   DimKey,
						Value: "128",
					},
				},
				IndexName: "",
			},
			ctx:            nil,
			rootCoord:      nil,
			result:         nil,
			isAutoIndex:    false,
			newIndexParams: nil,
			newTypeParams:  nil,
			collectionID:   0,
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_JSON,
			},
		}
		err := cit3.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("create index on VarChar field", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: indexparamcheck.IndexINVERTED,
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_VarChar,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("create index on VarChar field without index type", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{},
				IndexName:   "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_VarChar,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		sortKeyValuePairs(cit.newIndexParams)
		assert.Equal(t, cit.newIndexParams, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED},
		})
	})

	t.Run("create index on Arithmetic field", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: DefaultArithmeticIndexType,
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("create index on Arithmetic field without index type", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{},
				IndexName:   "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		sortKeyValuePairs(cit.newIndexParams)
		assert.Equal(t, cit.newIndexParams, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED},
		})
	})

	// Compatible with the old version <= 2.3.0
	t.Run("create marisa-trie index on VarChar field", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "marisa-trie",
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_VarChar,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
	})

	// Compatible with the old version <= 2.3.0
	t.Run("create Asceneding index on Arithmetic field", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "Asceneding",
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("create unsupported index on Arithmetic field", func(t *testing.T) {
		cit := &createIndexTask{
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "invalid_type",
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		}
		err := cit.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("create index on array field", func(t *testing.T) {
		cit3 := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				Base:           nil,
				DbName:         "",
				CollectionName: "",
				FieldName:      "",
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "STL_SORT",
					},
				},
				IndexName: "",
			},
			ctx:            nil,
			rootCoord:      nil,
			result:         nil,
			isAutoIndex:    false,
			newIndexParams: nil,
			newTypeParams:  nil,
			collectionID:   0,
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int64,
			},
		}
		err := cit3.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("pass vector index type on scalar field", func(t *testing.T) {
		cit4 := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				Base:           nil,
				DbName:         "",
				CollectionName: "",
				FieldName:      "",
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "HNSW",
					},
					{
						Key:   MetricTypeKey,
						Value: "IP",
					},
					{
						Key:   common.IndexParamsKey,
						Value: "{\"M\": 48, \"efConstruction\": 64}",
					},
					{
						Key:   DimKey,
						Value: "128",
					},
				},
				IndexName: "",
			},
			ctx:            nil,
			rootCoord:      nil,
			result:         nil,
			isAutoIndex:    false,
			newIndexParams: nil,
			newTypeParams:  nil,
			collectionID:   0,
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_VarChar,
			},
		}
		err := cit4.parseIndexParams(context.TODO())
		assert.Error(t, err)

		cit5 := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				Base:           nil,
				DbName:         "",
				CollectionName: "",
				FieldName:      "",
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "HNSW",
					},
					{
						Key:   MetricTypeKey,
						Value: "IP",
					},
					{
						Key:   common.IndexParamsKey,
						Value: "{\"M\": 48, \"efConstruction\": 64}",
					},
					{
						Key:   DimKey,
						Value: "128",
					},
				},
				IndexName: "",
			},
			ctx:            nil,
			rootCoord:      nil,
			result:         nil,
			isAutoIndex:    false,
			newIndexParams: nil,
			newTypeParams:  nil,
			collectionID:   0,
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_Int64,
			},
		}
		err = cit5.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("enable scalar auto index", func(t *testing.T) {
		err := Params.Save(Params.AutoIndexConfig.ScalarAutoIndexEnable.Key, "true")
		assert.NoError(t, err)

		cit := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "",
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_Int64,
			},
		}

		err = cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		sortKeyValuePairs(cit.newIndexParams)
		assert.Equal(t, cit.newIndexParams, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED},
		})
	})

	t.Run("create auto index on numeric field", func(t *testing.T) {
		cit := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: AutoIndexName,
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_Int64,
			},
		}

		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		sortKeyValuePairs(cit.newIndexParams)
		assert.Equal(t, cit.newIndexParams, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED},
		})
	})

	t.Run("create auto index on varchar field", func(t *testing.T) {
		cit := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: AutoIndexName,
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_VarChar,
			},
		}

		err := cit.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		sortKeyValuePairs(cit.newIndexParams)
		assert.Equal(t, cit.newIndexParams, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED},
		})
	})

	t.Run("create auto index on json field", func(t *testing.T) {
		cit := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: AutoIndexName,
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldID",
				IsPrimaryKey: false,
				Description:  "field no.1",
				DataType:     schemapb.DataType_JSON,
			},
		}

		err := cit.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("create auto index and mmap enable", func(t *testing.T) {
		paramtable.Init()
		Params.Save(Params.AutoIndexConfig.Enable.Key, "true")
		defer Params.Reset(Params.AutoIndexConfig.Enable.Key)

		cit := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: AutoIndexName,
					},
					{
						Key:   common.MmapEnabledKey,
						Value: "true",
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldVector",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
			},
		}

		err := cit.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("verify merge params with yaml", func(t *testing.T) {
		paramtable.Init()
		Params.Save("knowhere.HNSW.build.M", "3000")
		Params.Save("knowhere.HNSW.build.efConstruction", "120")
		defer Params.Reset("knowhere.HNSW.build.M")
		defer Params.Reset("knowhere.HNSW.build.efConstruction")

		cit := &createIndexTask{
			Condition: nil,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: "HNSW",
					},
					{
						Key:   common.MetricTypeKey,
						Value: metric.L2,
					},
				},
				IndexName: "",
			},
			fieldSchema: &schemapb.FieldSchema{
				FieldID:      101,
				Name:         "FieldVector",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "768"},
				},
			},
		}
		err := cit.parseIndexParams(context.TODO())
		// Out of range in json: param 'M' (3000) should be in range [2, 2048]
		assert.Error(t, err)
	})
}

func Test_wrapUserIndexParams(t *testing.T) {
	params := wrapUserIndexParams("L2")
	assert.Equal(t, 2, len(params))
	assert.Equal(t, "index_type", params[0].Key)
	assert.Equal(t, AutoIndexName, params[0].Value)
	assert.Equal(t, "metric_type", params[1].Key)
	assert.Equal(t, "L2", params[1].Value)
}

func Test_parseIndexParams_AutoIndex_WithType(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.AutoIndexConfig.Enable.Key, "true")
	Params.Save(Params.AutoIndexConfig.IndexParams.Key, `{"M": 30,"efConstruction": 360,"index_type": "HNSW"}`)
	Params.Save(Params.AutoIndexConfig.SparseIndexParams.Key, `{"drop_ratio_build": 0.2, "index_type": "SPARSE_INVERTED_INDEX"}`)
	Params.Save(Params.AutoIndexConfig.BinaryIndexParams.Key, `{"nlist": 1024, "index_type": "BIN_IVF_FLAT"}`)

	defer Params.Reset(Params.AutoIndexConfig.Enable.Key)
	defer Params.Reset(Params.AutoIndexConfig.IndexParams.Key)
	defer Params.Reset(Params.AutoIndexConfig.SparseIndexParams.Key)
	defer Params.Reset(Params.AutoIndexConfig.BinaryIndexParams.Key)

	floatFieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	sparseFloatFieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_SparseFloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "64"},
		},
	}
	binaryFieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4096"},
		},
	}

	t.Run("case 1, float vector parameters", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: floatFieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "L2"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.True(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
			{Key: "M", Value: "30"},
			{Key: "efConstruction", Value: "360"},
		}, task.newIndexParams)
	})

	t.Run("case 2, sparse vector parameters", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: sparseFloatFieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "IP"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.True(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"},
			{Key: common.MetricTypeKey, Value: "IP"},
			{Key: "drop_ratio_build", Value: "0.2"},
		}, task.newIndexParams)
	})

	t.Run("case 3, binary vector parameters", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: binaryFieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "JACCARD"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.True(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "BIN_IVF_FLAT"},
			{Key: common.MetricTypeKey, Value: "JACCARD"},
			{Key: "nlist", Value: "1024"},
		}, task.newIndexParams)
	})
}

func Test_parseIndexParams_AutoIndex(t *testing.T) {
	paramtable.Init()

	Params.Save(Params.AutoIndexConfig.Enable.Key, "false")
	Params.Save(Params.AutoIndexConfig.IndexParams.Key, `{"M": 30,"efConstruction": 360,"index_type": "HNSW", "metric_type": "IP"}`)
	Params.Save(Params.AutoIndexConfig.BinaryIndexParams.Key, `{"nlist": 1024, "index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD"}`)
	Params.Save(Params.AutoIndexConfig.SparseIndexParams.Key, `{"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"}`)
	defer Params.Reset(Params.AutoIndexConfig.Enable.Key)
	defer Params.Reset(Params.AutoIndexConfig.IndexParams.Key)
	defer Params.Reset(Params.AutoIndexConfig.BinaryIndexParams.Key)
	defer Params.Reset(Params.AutoIndexConfig.SparseIndexParams.Key)

	autoIndexConfig := Params.AutoIndexConfig.IndexParams.GetAsJSONMap()
	autoIndexConfigBinary := Params.AutoIndexConfig.BinaryIndexParams.GetAsJSONMap()
	autoIndexConfigSparse := Params.AutoIndexConfig.SparseIndexParams.GetAsJSONMap()
	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		},
	}

	fieldSchemaBinary := &schemapb.FieldSchema{
		DataType: schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		},
	}

	fieldSchemaSparse := &schemapb.FieldSchema{
		DataType: schemapb.DataType_SparseFloatVector,
	}

	t.Run("case 1, empty parameters binary", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchemaBinary,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: make([]*commonpb.KeyValuePair, 0),
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.False(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: AutoIndexName},
			{Key: common.MetricTypeKey, Value: autoIndexConfigBinary[common.MetricTypeKey]},
		}, task.newExtraParams)
	})

	t.Run("case 1, empty parameters sparse", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchemaSparse,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: make([]*commonpb.KeyValuePair, 0),
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.False(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: AutoIndexName},
			{Key: common.MetricTypeKey, Value: autoIndexConfigSparse[common.MetricTypeKey]},
		}, task.newExtraParams)
	})

	t.Run("case 1, empty parameters float vector", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: make([]*commonpb.KeyValuePair, 0),
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.False(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: AutoIndexName},
			{Key: common.MetricTypeKey, Value: autoIndexConfig[common.MetricTypeKey]},
		}, task.newExtraParams)
	})

	t.Run("case 2, only metric type passed", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "L2"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.True(t, task.userAutoIndexMetricTypeSpecified)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: AutoIndexName},
			{Key: common.MetricTypeKey, Value: "L2"},
		}, task.newExtraParams)
	})

	t.Run("case 3, AutoIndex & metric_type passed", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "L2"},
					{Key: common.IndexTypeKey, Value: AutoIndexName},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: AutoIndexName},
			{Key: common.MetricTypeKey, Value: "L2"},
		}, task.newExtraParams)
	})

	t.Run("case 4, duplicate and useless parameters passed", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: "not important", Value: "L2"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("case 5, duplicate and useless parameters passed", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.MetricTypeKey, Value: "L2"},
					{Key: "not important", Value: "L2"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})

	t.Run("case 6, autoindex & duplicate", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: AutoIndexName},
					{Key: common.MetricTypeKey, Value: "L2"},
					{Key: "not important", Value: "L2"},
				},
			},
		}
		err := task.parseIndexParams(context.TODO())
		assert.Error(t, err)
	})
}

func newTestSchema() *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
	}

	for name, value := range schemapb.DataType_value {
		dataType := schemapb.DataType(value)
		if !typeutil.IsIntegerType(dataType) && !typeutil.IsFloatingType(dataType) && !typeutil.IsVectorType(dataType) && !typeutil.IsStringType(dataType) {
			continue
		}
		newField := &schemapb.FieldSchema{
			FieldID: int64(100 + value), Name: name + "Field", IsPrimaryKey: false, Description: "", DataType: dataType,
		}
		fields = append(fields, newField)
	}

	return &schemapb.CollectionSchema{
		Name:               "test",
		Description:        "schema for test used",
		AutoID:             true,
		Fields:             fields,
		EnableDynamicField: true,
	}
}
