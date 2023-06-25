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
	"encoding/json"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
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
	queryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs: []int64{},
	}, nil)
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
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "mock"},
			State:  commonpb.IndexState_Unissued,
		},
		collectionID: collectionID,
	}

	shardMgr := newShardClientMgr()
	// failed to get collection id.
	_ = InitMetaCache(ctx, rootCoord, queryCoord, shardMgr)
	assert.Error(t, gist.Execute(ctx))

	rootCoord.DescribeCollectionFunc = func(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Schema:         newTestSchema(),
			CollectionID:   collectionID,
			CollectionName: request.CollectionName,
		}, nil
	}

	datacoord.GetIndexStateFunc = func(ctx context.Context, request *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
		return &indexpb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
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

	Params.Init()
	qc := getMockQueryCoord()
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
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
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionIDs: []int64{collectionID},
		}, nil)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection error", func(t *testing.T) {
		qc := getMockQueryCoord()
		qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection fail", func(t *testing.T) {
		qc := getMockQueryCoord()
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

func getMockQueryCoord() *types.MockQueryCoord {
	qc := &types.MockQueryCoord{}
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
	).Return(newTestSchema(), nil)

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
			}},
	}

	t.Run("parse index params", func(t *testing.T) {
		err := cit.parseIndexParams()
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
			}},
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
		err = cit2.parseIndexParams()
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
		err := cit3.parseIndexParams()
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

func Test_parseIndexParams_AutoIndex(t *testing.T) {
	Params.Init()
	mgr := config.NewManager()
	mgr.SetConfig("autoIndex.enable", "false")
	mgr.SetConfig("autoIndex.params.build", `{"M": 30,"efConstruction": 360,"index_type": "HNSW", "metric_type": "IP"}`)
	Params.AutoIndexConfig.Enable.Init(mgr)
	Params.AutoIndexConfig.IndexParams.Init(mgr)
	autoIndexConfig := Params.AutoIndexConfig.IndexParams.GetAsJSONMap()
	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		},
	}

	t.Run("case 1, empty parameters", func(t *testing.T) {
		task := &createIndexTask{
			fieldSchema: fieldSchema,
			req: &milvuspb.CreateIndexRequest{
				ExtraParams: make([]*commonpb.KeyValuePair, 0),
			},
		}
		err := task.parseIndexParams()
		assert.NoError(t, err)
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
		err := task.parseIndexParams()
		assert.NoError(t, err)
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
		err := task.parseIndexParams()
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
		err := task.parseIndexParams()
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
		err := task.parseIndexParams()
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
		err := task.parseIndexParams()
		assert.Error(t, err)
	})
}
