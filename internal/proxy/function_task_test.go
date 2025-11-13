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
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
)

type FunctionTaskSuite struct {
	suite.Suite
}

func TestFunctionTask(t *testing.T) {
	suite.Run(t, new(FunctionTaskSuite))
}

func (f *FunctionTaskSuite) TestFunctionOnType() {
	{
		task := &addCollectionFunctionTask{
			AddCollectionFunctionRequest: &milvuspb.AddCollectionFunctionRequest{},
		}
		err := task.OnEnqueue()
		f.NoError(err)
		f.Equal(commonpb.MsgType_AddCollectionFunction, task.Type())
		f.Equal(task.TraceCtx(), task.ctx)
		task.SetID(1)
		f.Equal(task.ID(), int64(1))
		task.SetTs(2)
		f.Equal(task.EndTs(), uint64(2))
		f.Equal(task.Name(), AddCollectionFunctionTask)
	}
	{
		task := &dropCollectionFunctionTask{
			DropCollectionFunctionRequest: &milvuspb.DropCollectionFunctionRequest{},
		}
		err := task.OnEnqueue()
		f.NoError(err)
		f.Equal(commonpb.MsgType_DropCollectionFunction, task.Type())
		f.Equal(task.TraceCtx(), task.ctx)
		task.SetID(1)
		f.Equal(task.ID(), int64(1))
		task.SetTs(2)
		f.Equal(task.EndTs(), uint64(2))
		f.Equal(task.Name(), DropCollectionFunctionTask)
	}
	{
		task := &alterCollectionFunctionTask{
			AlterCollectionFunctionRequest: &milvuspb.AlterCollectionFunctionRequest{},
		}
		err := task.OnEnqueue()
		f.NoError(err)
		f.Equal(commonpb.MsgType_AlterCollectionFunction, task.Type())
		f.Equal(task.TraceCtx(), task.ctx)
		task.SetID(1)
		f.Equal(task.ID(), int64(1))
		task.SetTs(2)
		f.Equal(task.EndTs(), uint64(2))
		f.Equal(task.Name(), AlterCollectionFunctionTask)
	}
}

func (f *FunctionTaskSuite) TestAddCollectionFunctionTaskPreExecute() {
	ctx := context.Background()
	{
		mixc := mocks.NewMockMixCoordClient(f.T())
		req := &milvuspb.AddCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			DbName:         "db",
			CollectionName: "NotExist",
			CollectionID:   1,
			FunctionSchema: &schemapb.FunctionSchema{},
		}

		task := &addCollectionFunctionTask{
			Condition:                    NewTaskCondition(ctx),
			AddCollectionFunctionRequest: req,
			mixCoord:                     mixc,
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(0, fmt.Errorf("Mock Error")).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.ErrorContains(err, "Mock Error")
	}
	{
		// Test with invalid function schema
		mixc := mocks.NewMockMixCoordClient(f.T())
		req := &milvuspb.AddCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionSchema: &schemapb.FunctionSchema{},
		}

		task := &addCollectionFunctionTask{
			Condition:                    NewTaskCondition(ctx),
			AddCollectionFunctionRequest: req,
			mixCoord:                     mixc,
		}

		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(nil, fmt.Errorf("Mock info error")).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.ErrorContains(err, "Mock info error")
	}

	{
		// Test with valid request
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"sparse_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AddCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionSchema: functionSchema,
		}

		task := &addCollectionFunctionTask{
			Condition:                    NewTaskCondition(ctx),
			AddCollectionFunctionRequest: req,
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(nil, nil).Maybe()
		globalMetaCache = cache
		err := task.PreExecute(ctx)
		f.ErrorContains(err, "not support adding BM25")
	}
	{
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vec"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AddCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionSchema: functionSchema,
		}

		task := &addCollectionFunctionTask{
			Condition:                    NewTaskCondition(ctx),
			AddCollectionFunctionRequest: req,
		}
		coll := &collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Functions: []*schemapb.FunctionSchema{},
				},
			},
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(coll, nil).Maybe()
		m := mockey.Mock(validateFunction).Return(nil).Build()
		defer m.UnPatch()
		globalMetaCache = cache
		err := task.PreExecute(ctx)
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestAlterCollectionFunctionTaskPreExecute() {
	ctx := context.Background()

	{
		mixc := mocks.NewMockMixCoordClient(f.T())
		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			DbName:         "db",
			CollectionName: "NotExist",
			FunctionName:   "test",
			CollectionID:   1,
			FunctionSchema: &schemapb.FunctionSchema{Name: "test"},
		}

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
			mixCoord:                       mixc,
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(0, fmt.Errorf("Mock Error")).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.ErrorContains(err, "Mock Error")
	}
	{
		// Test with invalid function schema
		mixc := mocks.NewMockMixCoordClient(f.T())
		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test",
			FunctionSchema: &schemapb.FunctionSchema{Name: "test"},
		}

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
			mixCoord:                       mixc,
		}

		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(nil, fmt.Errorf("Mock info error")).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.ErrorContains(err, "Mock info error")
	}
	{
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"sparse_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
			FunctionSchema: functionSchema,
		}

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(nil, nil).Maybe()
		globalMetaCache = cache
		err := task.PreExecute(ctx)
		f.ErrorContains(err, "not support alter BM25")
	}
	{
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"vector_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
			FunctionSchema: functionSchema,
		}

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		coll := &collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Functions: []*schemapb.FunctionSchema{
						{Name: req.FunctionName, Type: schemapb.FunctionType_BM25},
					},
				},
			},
		}

		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(coll, nil).Maybe()
		globalMetaCache = cache
		err := task.PreExecute(ctx)
		f.ErrorContains(err, "not support alter BM25")
	}
	{
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"dense_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "NotEqual",
			FunctionSchema: functionSchema,
		}

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(nil, nil).Maybe()
		globalMetaCache = cache
		err := task.PreExecute(ctx)
		f.ErrorContains(err, "Invalid function config, name not match")
	}
	{
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vec"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
			FunctionSchema: functionSchema,
		}

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
		}
		coll := &collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Functions: []*schemapb.FunctionSchema{
						{Name: "test_function", Type: schemapb.FunctionType_TextEmbedding},
						{Name: "f2", Type: schemapb.FunctionType_TextEmbedding},
					},
				},
			},
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(coll, nil).Maybe()
		m := mockey.Mock(validateFunction).Return(nil).Build()
		defer m.UnPatch()
		globalMetaCache = cache
		err := task.PreExecute(ctx)
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestDropCollectionFunctionTaskPreExecute() {
	ctx := context.Background()
	{
		// Test with valid request
		req := &milvuspb.DropCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropCollectionFunction,
			},
			CollectionName: "test_collection",
			FunctionName:   "test_function",
		}

		task := &dropCollectionFunctionTask{
			Condition:                     NewTaskCondition(ctx),
			DropCollectionFunctionRequest: req,
		}

		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(nil, fmt.Errorf("mock error")).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.ErrorContains(err, "mock error")
	}
	{
		req := &milvuspb.DropCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropCollectionFunction,
			},
			CollectionName: "test_collection",
			FunctionName:   "test_function",
		}

		task := &dropCollectionFunctionTask{
			Condition:                     NewTaskCondition(ctx),
			DropCollectionFunctionRequest: req,
		}

		coll := &collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Functions: []*schemapb.FunctionSchema{},
				},
			},
		}

		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(coll, nil).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.NoError(err)
	}
	{
		req := &milvuspb.DropCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropCollectionFunction,
			},
			CollectionName: "test_collection",
			FunctionName:   "test_function",
		}

		task := &dropCollectionFunctionTask{
			Condition:                     NewTaskCondition(ctx),
			DropCollectionFunctionRequest: req,
		}

		coll := &collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Functions: []*schemapb.FunctionSchema{
						{Name: req.FunctionName},
					},
				},
			},
		}

		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(coll, nil).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.NoError(err)
	}
}

func (f *FunctionTaskSuite) TestGetCollectionInfo() {
	ctx := context.Background()
	{
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, "db", "collection").Return(0, fmt.Errorf("Mock Error")).Maybe()
		globalMetaCache = cache

		_, err := getCollectionInfo(ctx, "db", "collection")
		f.ErrorContains(err, "Mock Error")
	}
	{
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, "db", "collection").Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, "db", "collection", int64(1)).Return(nil, fmt.Errorf("Mock info error")).Maybe()
		globalMetaCache = cache

		_, err := getCollectionInfo(ctx, "db", "collection")
		f.ErrorContains(err, "Mock info error")
	}
}

func (f *FunctionTaskSuite) TestAlterCollectionFunctionTaskExecute() {
	ctx := context.Background()
	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"sparse_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
			FunctionSchema: functionSchema,
		}

		mockRootCoord.EXPECT().AlterCollectionFunction(mock.Anything, req).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil)

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
			mixCoord:                       mockRootCoord,
		}

		err := task.Execute(ctx)
		f.NoError(err)
	}

	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"sparse_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AlterCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AlterCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
			FunctionSchema: functionSchema,
		}

		mockRootCoord.EXPECT().AlterCollectionFunction(mock.Anything, req).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "test error",
		}, nil)

		task := &alterCollectionFunctionTask{
			Condition:                      NewTaskCondition(ctx),
			AlterCollectionFunctionRequest: req,
			mixCoord:                       mockRootCoord,
		}
		err := task.Execute(ctx)
		f.Error(err)
	}
}

func (f *FunctionTaskSuite) TestAddCollectionFunctionTaskExecute() {
	ctx := context.Background()

	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"sparse_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AddCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionSchema: functionSchema,
		}

		mockRootCoord.EXPECT().AddCollectionFunction(mock.Anything, req).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil)

		task := &addCollectionFunctionTask{
			Condition:                    NewTaskCondition(ctx),
			AddCollectionFunctionRequest: req,
			mixCoord:                     mockRootCoord,
		}

		err := task.Execute(ctx)
		f.NoError(err)
	}

	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		functionSchema := &schemapb.FunctionSchema{
			Name:             "test_function",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text_field"},
			OutputFieldNames: []string{"sparse_field"},
			Params:           []*commonpb.KeyValuePair{},
		}

		req := &milvuspb.AddCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_AddCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionSchema: functionSchema,
		}

		mockRootCoord.EXPECT().AddCollectionFunction(mock.Anything, req).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "test error",
		}, nil)

		task := &addCollectionFunctionTask{
			Condition:                    NewTaskCondition(ctx),
			AddCollectionFunctionRequest: req,
			mixCoord:                     mockRootCoord,
		}
		err := task.Execute(ctx)
		f.Error(err)
	}
}

func (f *FunctionTaskSuite) TestDropCollectionFunctionTaskExecute() {
	ctx := context.Background()

	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		req := &milvuspb.DropCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
		}

		mockRootCoord.EXPECT().DropCollectionFunction(mock.Anything, req).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil)

		task := &dropCollectionFunctionTask{
			Condition:                     NewTaskCondition(ctx),
			DropCollectionFunctionRequest: req,
			mixCoord:                      mockRootCoord,
			fSchema: &schemapb.FunctionSchema{
				Type: schemapb.FunctionType_TextEmbedding,
			},
		}

		err := task.Execute(ctx)
		f.NoError(err)
	}

	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		req := &milvuspb.DropCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
		}

		mockRootCoord.EXPECT().DropCollectionFunction(mock.Anything, req).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "test error",
		}, nil)

		task := &dropCollectionFunctionTask{
			Condition:                     NewTaskCondition(ctx),
			DropCollectionFunctionRequest: req,
			mixCoord:                      mockRootCoord,
			fSchema: &schemapb.FunctionSchema{
				Type: schemapb.FunctionType_TextEmbedding,
			},
		}

		err := task.Execute(ctx)
		f.Error(err)
	}

	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())

		req := &milvuspb.DropCollectionFunctionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropCollectionFunction,
			},
			CollectionName: "test_collection",
			CollectionID:   1,
			FunctionName:   "test_function",
		}

		task := &dropCollectionFunctionTask{
			Condition:                     NewTaskCondition(ctx),
			DropCollectionFunctionRequest: req,
			mixCoord:                      mockRootCoord,
			fSchema: &schemapb.FunctionSchema{
				Type: schemapb.FunctionType_BM25,
			},
		}

		err := task.Execute(ctx)
		f.ErrorContains(err, "Currently does not support droping BM25 function")
	}
}
