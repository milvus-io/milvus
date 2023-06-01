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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func TestGetIndexStateTask_Execute(t *testing.T) {
	dbName := funcutil.GenRandomStr()
	collectionName := funcutil.GenRandomStr()
	collectionID := UniqueID(1)
	fieldName := funcutil.GenRandomStr()
	indexName := ""
	ctx := context.Background()

	rootCoord := newMockRootCoord()
	indexCoord := newMockIndexCoord()
	queryCoord := NewQueryCoordMock()

	gist := &getIndexStateTask{
		GetIndexStateRequest: &milvuspb.GetIndexStateRequest{
			Base:           &commonpb.MsgBase{},
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      fieldName,
			IndexName:      indexName,
		},
		ctx:        ctx,
		indexCoord: indexCoord,
		rootCoord:  rootCoord,
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

	indexCoord.GetIndexStateFunc = func(ctx context.Context, request *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
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

	Params.InitOnce()
	showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			CollectionIDs: nil,
		}, nil
	}
	qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
	ic := newMockIndexCoord()
	ctx := context.Background()
	qc.updateState(commonpb.StateCode_Healthy)

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
		indexCoord:   ic,
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
		showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				CollectionIDs: []int64{collectionID},
			}, nil
		}
		qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
		qc.updateState(commonpb.StateCode_Healthy)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection error", func(t *testing.T) {
		showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
			return nil, errors.New("error")
		}
		qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
		qc.updateState(commonpb.StateCode_Healthy)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("show collection fail", func(t *testing.T) {
		showCollectionMock := func(ctx context.Context, request *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "fail reason",
				},
			}, nil
		}
		qc := NewQueryCoordMock(withValidShardLeaders(), SetQueryCoordShowCollectionsFunc(showCollectionMock))
		qc.updateState(commonpb.StateCode_Healthy)
		dit.queryCoord = qc

		err := dit.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestCreateIndexTask_PreExecute(t *testing.T) {
	collectionName := "collection1"
	collectionID := UniqueID(1)
	fieldName := newTestSchema().Fields[0].Name

	Params.InitOnce()
	ic := newMockIndexCoord()
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
		indexCoord:   ic,
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
					Key:   "index_type",
					Value: "HNSW",
				},
				{
					Key:   MetricTypeKey,
					Value: "IP",
				},
				{
					Key:   "params",
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
		indexCoord:     nil,
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
					Key:   "index_type",
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
					Key:   "index_type",
					Value: "IVF_FLAT",
				},
				{
					Key:   MetricTypeKey,
					Value: "L2",
				},
				{
					Key:   "params",
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
		indexCoord:     nil,
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
		Params.AutoIndexConfig.Enable = true
		Params.AutoIndexConfig.IndexParams = map[string]string{
			"index_type":     "HNSW",
			"M":              "10",
			"efConstruction": "100",
		}
		err := cit2.parseIndexParams()
		assert.NoError(t, err)

		assert.ElementsMatch(t,
			[]*commonpb.KeyValuePair{
				{
					Key:   "index_type",
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
	Params.AutoIndexConfig.Enable = false
	Params.AutoIndexConfig.IndexParams = map[string]string{
		"M":              "30",
		"efConstruction": "360",
		"index_type":     "HNSW",
		"metric_type":    "IP",
	}
	autoIndexConfig := Params.AutoIndexConfig.IndexParams
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

func TestGetIndexStatisticsTask(t *testing.T) {
	collectionName := "collection1"
	collectionID := UniqueID(1)
	//fieldName := newTestSchema().Fields[0].Name
	indexName := "_default"
	Params.InitOnce()
	ic := newMockIndexCoord()
	ctx := context.Background()

	git := getIndexStatisticsTask{
		ctx: ctx,
		GetIndexStatisticsRequest: &milvuspb.GetIndexStatisticsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_GetIndexStatistics,
			},
			CollectionName: collectionName,
			IndexName:      indexName,
		},
		indexCoord:   ic,
		result:       nil,
		collectionID: collectionID,
	}

	t.Run("pre execute get collection id fail", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(collectionID, errors.New("mock error"))
		globalMetaCache = mockCache

		err := git.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("execute GetCollectionSchema fail", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newTestSchema(), errors.New("mock error"))
		globalMetaCache = mockCache
		err := git.Execute(ctx)
		assert.Error(t, err)
	})

	t.Run("execute indexcoord GetIndexStatistics fail", func(t *testing.T) {
		mockCache := NewMockCache(t)
		mockCache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(newTestSchema(), nil)
		globalMetaCache = mockCache
		err := git.Execute(ctx)
		assert.Error(t, err)
	})
}
