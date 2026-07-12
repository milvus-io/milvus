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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/function/validator"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type FunctionTaskSuite struct {
	suite.Suite
}

func TestFunctionTask(t *testing.T) {
	suite.Run(t, new(FunctionTaskSuite))
}

// TestAddFunctionRequiresStorageV3Gate guards the add_function_field V3 gate (issue #51167):
// adding a function must be rejected unless StorageV3 (useLoonFFI), the schema-bump compaction
// (bumpSchemaVersion.enabled), and the storage-version upgrade compaction (storageVersion.enabled)
// are all on, so the new function output is actually backfilled into pre-existing segments.
func (f *FunctionTaskSuite) TestAddFunctionRequiresStorageV3Gate() {
	useLoon := paramtable.Get().CommonCfg.UseLoonFFI.Key
	bumpEnabled := paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key
	svEnabled := paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key
	defer paramtable.Get().Reset(useLoon)
	defer paramtable.Get().Reset(bumpEnabled)
	defer paramtable.Get().Reset(svEnabled)

	// useLoonFFI off -> reject
	paramtable.Get().Save(useLoon, "false")
	f.ErrorContains(validateAddFunctionRequiresStorageV3(), "StorageV3")

	// useLoonFFI on but bumpSchemaVersion.enabled off -> reject
	paramtable.Get().Save(useLoon, "true")
	paramtable.Get().Save(bumpEnabled, "false")
	f.ErrorContains(validateAddFunctionRequiresStorageV3(), "bumpSchemaVersion.enabled")

	// bumpSchemaVersion on but storageVersion.enabled off -> reject
	paramtable.Get().Save(bumpEnabled, "true")
	paramtable.Get().Save(svEnabled, "false")
	f.ErrorContains(validateAddFunctionRequiresStorageV3(), "storageVersion.enabled")

	// all on -> pass
	paramtable.Get().Save(svEnabled, "true")
	f.NoError(validateAddFunctionRequiresStorageV3())
}

// TestValidateAddFunctionInputNotText guards the reject of a BM25/MinHash function whose
// input is a TEXT field (issue #51167): its output cannot be backfilled into existing
// segments (stringInputsFromRecord hard-fails on the binary LOB column), so add-function
// must fail fast. VarChar input stays allowed; non-materialized function types are ignored.
func (f *FunctionTaskSuite) TestValidateAddFunctionInputNotText() {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "varchar_in", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "text_in", DataType: schemapb.DataType_Text},
	}}
	fn := func(t schemapb.FunctionType, input string) *schemapb.FunctionSchema {
		return &schemapb.FunctionSchema{Name: "fn", Type: t, InputFieldNames: []string{input}}
	}

	// TEXT input rejected for BM25 and MinHash
	f.ErrorContains(validateAddFunctionInputNotText(schema, fn(schemapb.FunctionType_BM25, "text_in")), "TEXT input field")
	f.ErrorContains(validateAddFunctionInputNotText(schema, fn(schemapb.FunctionType_MinHash, "text_in")), "TEXT input field")
	// VarChar input allowed
	f.NoError(validateAddFunctionInputNotText(schema, fn(schemapb.FunctionType_BM25, "varchar_in")))
	f.NoError(validateAddFunctionInputNotText(schema, fn(schemapb.FunctionType_MinHash, "varchar_in")))
	// non-materialized function type (e.g. TextEmbedding) is out of scope -> allowed even with TEXT input
	f.NoError(validateAddFunctionInputNotText(schema, fn(schemapb.FunctionType_TextEmbedding, "text_in")))
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
	// Adding a function requires StorageV3 + schema-bump/storage-version compaction enabled
	// (see validateAddFunctionRequiresStorageV3). storageVersion.enabled defaults true;
	// bumpSchemaVersion.enabled defaults false, so it must be set explicitly.
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.Key)
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
		m := mockey.Mock(validator.ValidateFunction).Return(nil).Build()
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
		f.ErrorContains(err, "invalid function config, name not match")
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
		coll := &collectionInfo{
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
						{Name: "vec", DataType: schemapb.DataType_FloatVector, IsFunctionOutput: true},
					},
					Functions: []*schemapb.FunctionSchema{
						{Name: "test_function", Type: schemapb.FunctionType_TextEmbedding},
					},
				},
			},
		}
		cache := NewMockCache(f.T())
		cache.EXPECT().GetCollectionID(ctx, req.DbName, req.CollectionName).Return(int64(1), nil).Maybe()
		cache.EXPECT().GetCollectionInfo(ctx, req.DbName, req.CollectionName, int64(1)).Return(coll, nil).Maybe()
		globalMetaCache = cache

		err := task.PreExecute(ctx)
		f.ErrorContains(err, externalCollectionFunctionMutationUnsupportedMsg)
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
		m := mockey.Mock(validator.ValidateFunction).Return(nil).Build()
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
					Fields: []*schemapb.FieldSchema{
						{Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
						{Name: "vec", DataType: schemapb.DataType_FloatVector, IsFunctionOutput: true},
					},
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
		f.ErrorContains(err, externalCollectionFunctionMutationUnsupportedMsg)
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
		f.ErrorContains(err, "currently does not support dropping BM25 function")
	}

	// MinHash mirrors BM25: dropping only the function would strand output
	// data generated under its signature.
	{
		mockRootCoord := mocks.NewMockMixCoordClient(f.T())
		task := &dropCollectionFunctionTask{
			Condition: NewTaskCondition(ctx),
			DropCollectionFunctionRequest: &milvuspb.DropCollectionFunctionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollectionFunction},
				CollectionName: "test_collection",
				CollectionID:   1,
				FunctionName:   "minhash_function",
			},
			mixCoord: mockRootCoord,
			fSchema: &schemapb.FunctionSchema{
				Type: schemapb.FunctionType_MinHash,
			},
		}
		err := task.Execute(ctx)
		f.ErrorContains(err, "currently does not support dropping MinHash function")
	}
}

// The legacy function APIs must reject search-time runner functions (BM25 and
// MinHash) symmetrically: altering their parameters in place, or adding them
// detached from their output fields, silently diverges query-time
// hashing/scoring from the indexed corpus.
func (f *FunctionTaskSuite) TestLegacyAPIRejectsMinHash() {
	ctx := context.Background()

	addTask := &addCollectionFunctionTask{
		Condition: NewTaskCondition(ctx),
		AddCollectionFunctionRequest: &milvuspb.AddCollectionFunctionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AddCollectionFunction},
			CollectionName: "test_collection",
			FunctionSchema: &schemapb.FunctionSchema{
				Name: "minhash_function",
				Type: schemapb.FunctionType_MinHash,
			},
		},
	}
	err := addTask.PreExecute(ctx)
	f.ErrorContains(err, "currently does not support adding MinHash function")

	alterTask := &alterCollectionFunctionTask{
		Condition: NewTaskCondition(ctx),
		AlterCollectionFunctionRequest: &milvuspb.AlterCollectionFunctionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollectionFunction},
			CollectionName: "test_collection",
			FunctionName:   "minhash_function",
			FunctionSchema: &schemapb.FunctionSchema{
				Name: "minhash_function",
				Type: schemapb.FunctionType_MinHash,
			},
		},
	}
	err = alterTask.PreExecute(ctx)
	f.ErrorContains(err, "currently does not support alter MinHash function")
}
