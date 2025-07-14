// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

func TestUpsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := upsertTask{
		req: &milvuspb.UpsertRequest{
			NumRows: 0,
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{},
			},
		},
	}
	case1.upsertMsg.InsertMsg.InsertRequest = &msgpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
		),
		CollectionName: case1.req.CollectionName,
		PartitionName:  case1.req.PartitionName,
		FieldsData:     case1.req.FieldsData,
		NumRows:        uint64(case1.req.NumRows),
		Version:        msgpb.InsertDataVersion_ColumnBased,
	}

	err = case1.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// checkFieldsDataBySchema was already checked by TestUpsertTask_checkFieldsDataBySchema

	boolFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}
	int8FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}
	int16FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}
	int32FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}
	int64FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}
	floatFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Float}
	doubleFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Double}
	floatVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
	binaryVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector}
	varCharFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}

	numRows := 20
	dim := 128
	collSchema := &schemapb.CollectionSchema{
		Name:        "TestUpsertTask_checkRowNums",
		Description: "TestUpsertTask_checkRowNums",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			boolFieldSchema,
			int8FieldSchema,
			int16FieldSchema,
			int32FieldSchema,
			int64FieldSchema,
			floatFieldSchema,
			doubleFieldSchema,
			floatVectorFieldSchema,
			binaryVectorFieldSchema,
			varCharFieldSchema,
		},
	}
	schema := newSchemaInfo(collSchema)
	case2 := upsertTask{
		req: &milvuspb.UpsertRequest{
			NumRows:    uint32(numRows),
			FieldsData: []*schemapb.FieldData{},
		},
		rowIDs:     testutils.GenerateInt64Array(numRows),
		timestamps: testutils.GenerateUint64Array(numRows),
		schema:     schema,
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{},
			},
		},
	}

	// satisfied
	case2.req.FieldsData = []*schemapb.FieldData{
		newScalarFieldData(boolFieldSchema, "Bool", numRows),
		newScalarFieldData(int8FieldSchema, "Int8", numRows),
		newScalarFieldData(int16FieldSchema, "Int16", numRows),
		newScalarFieldData(int32FieldSchema, "Int32", numRows),
		newScalarFieldData(int64FieldSchema, "Int64", numRows),
		newScalarFieldData(floatFieldSchema, "Float", numRows),
		newScalarFieldData(doubleFieldSchema, "Double", numRows),
		newFloatVectorFieldData("FloatVector", numRows, dim),
		newBinaryVectorFieldData("BinaryVector", numRows, dim),
		newScalarFieldData(varCharFieldSchema, "VarChar", numRows),
	}
	case2.upsertMsg.InsertMsg.InsertRequest = &msgpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
		),
		CollectionName: case2.req.CollectionName,
		PartitionName:  case2.req.PartitionName,
		FieldsData:     case2.req.FieldsData,
		NumRows:        uint64(case2.req.NumRows),
		RowIDs:         case2.rowIDs,
		Timestamps:     case2.timestamps,
		Version:        msgpb.InsertDataVersion_ColumnBased,
	}
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less bool data
	case2.req.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more bool data
	case2.req.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less int8 data
	case2.req.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int8 data
	case2.req.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int16 data
	case2.req.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int16 data
	case2.req.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int32 data
	case2.req.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int32 data
	case2.req.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int64 data
	case2.req.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int64 data
	case2.req.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less float data
	case2.req.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more float data
	case2.req.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less double data
	case2.req.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.req.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less float vectors
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more float vectors
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less binary vectors
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more binary vectors
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less double data
	case2.req.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.req.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)
}

func TestUpsertTask(t *testing.T) {
	t.Run("test getChannels", func(t *testing.T) {
		collectionID := UniqueID(0)
		collectionName := "col-0"
		channels := []pChan{"mock-chan-0", "mock-chan-1"}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(collectionID, nil)
		globalMetaCache = cache

		chMgr := NewMockChannelsMgr(t)
		chMgr.EXPECT().getChannels(mock.Anything).Return(channels, nil)
		ut := upsertTask{
			ctx: context.Background(),
			req: &milvuspb.UpsertRequest{
				CollectionName: collectionName,
			},
			chMgr: chMgr,
		}
		err := ut.setChannels()
		assert.NoError(t, err)
		resChannels := ut.getChannels()
		assert.ElementsMatch(t, channels, resChannels)
		assert.ElementsMatch(t, channels, ut.pChannels)
	})
}

func TestUpsertTaskForReplicate(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	globalMetaCache = mockCache
	ctx := context.Background()

	t.Run("fail to get collection info", func(t *testing.T) {
		ut := upsertTask{
			ctx: ctx,
			req: &milvuspb.UpsertRequest{
				CollectionName: "col-0",
			},
		}
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("foo")).Once()
		err := ut.PreExecute(ctx)
		assert.Error(t, err)
	})

	t.Run("replicate mode", func(t *testing.T) {
		ut := upsertTask{
			ctx: ctx,
			req: &milvuspb.UpsertRequest{
				CollectionName: "col-0",
			},
		}
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			replicateID: "local-mac",
		}, nil).Once()
		err := ut.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestUpsertTask_Function(t *testing.T) {
	paramtable.Init()
	paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
		return map[string]string{
			"mock.apikey": "mock",
		}
	}
	ts := function.CreateOpenAIEmbeddingServer()
	defer ts.Close()
	paramtable.Get().FunctionCfg.TextEmbeddingProviders.GetFunc = func() map[string]string {
		return map[string]string{
			"openai.url": ts.URL,
		}
	}

	data := []*schemapb.FieldData{}
	f1 := schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldId:   100,
		FieldName: "id",
		IsDynamic: false,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{0, 1},
					},
				},
			},
		},
	}
	data = append(data, &f1)
	f2 := schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldId:   101,
		FieldName: "text",
		IsDynamic: false,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"sentence", "sentence"},
					},
				},
			},
		},
	}
	data = append(data, &f2)
	collectionName := "TestUpsertTask_function"
	schema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "TestUpsertTask_function",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "200"},
				},
			},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "test_function",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldIds:    []int64{101},
				InputFieldNames:  []string{"text"},
				OutputFieldIds:   []int64{102},
				OutputFieldNames: []string{"vector"},
				Params: []*commonpb.KeyValuePair{
					{Key: "provider", Value: "openai"},
					{Key: "model_name", Value: "text-embedding-ada-002"},
					{Key: "credential", Value: "mock"},
					{Key: "dim", Value: "4"},
				},
			},
		},
	}

	info := newSchemaInfo(schema)
	collectionID := UniqueID(0)
	cache := NewMockCache(t)
	globalMetaCache = cache

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Status(nil),
		ID:     collectionID,
		Count:  10,
	}, nil)
	idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
	idAllocator.Start()
	defer idAllocator.Close()
	assert.NoError(t, err)
	task := upsertTask{
		ctx: context.Background(),
		req: &milvuspb.UpsertRequest{
			CollectionName: collectionName,
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					),
					CollectionName: collectionName,
					DbName:         "hooooooo",
					Version:        msgpb.InsertDataVersion_ColumnBased,
					FieldsData:     data,
					NumRows:        2,
					PartitionName:  Params.CommonCfg.DefaultPartitionName.GetValue(),
				},
			},
		},
		idAllocator: idAllocator,
		schema:      info,
		result:      &milvuspb.MutationResult{},
	}
	err = task.insertPreExecute(ctx)
	assert.NoError(t, err)

	// process failed
	{
		oldRows := task.upsertMsg.InsertMsg.InsertRequest.NumRows
		task.upsertMsg.InsertMsg.InsertRequest.NumRows = 10000
		err = task.insertPreExecute(ctx)
		assert.Error(t, err)
		task.upsertMsg.InsertMsg.InsertRequest.NumRows = oldRows
	}
}

func TestUpsertTaskForSchemaMismatch(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	globalMetaCache = mockCache
	ctx := context.Background()

	t.Run("schema ts mismatch", func(t *testing.T) {
		ut := upsertTask{
			ctx: ctx,
			req: &milvuspb.UpsertRequest{
				CollectionName: "col-0",
				NumRows:        10,
			},
			schemaTimestamp: 99,
		}
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			updateTimestamp: 100,
		}, nil)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		err := ut.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
	})
}

func TestUpsertTask_QueryPreExecute(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	// Create test schema
	primaryFieldSchema := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "id",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
		AutoID:       false,
	}

	vectorFieldSchema := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "128"},
		},
	}

	collectionSchema := &schemapb.CollectionSchema{
		Name:   "test_collection",
		Fields: []*schemapb.FieldSchema{primaryFieldSchema, vectorFieldSchema},
	}

	schema := newSchemaInfo(collectionSchema)

	// Test data
	primaryKeyData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "id",
		FieldId:   100,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{},
					},
				},
			},
		},
	}

	vectorData := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "vector",
		FieldId:   101,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 128,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: make([]float32, 0),
					},
				},
			},
		},
	}

	mockey.PatchConvey("TestUpsertTask_QueryPreExecute", t, func() {
		t.Run("success with no existing data - pure insert", func(t *testing.T) {
			// Mock globalMetaCache.GetShardLeaderList using mockey with correct signature
			mockGetShardLeaderList := mockey.Mock((*MetaCache).GetShardLeaderList).To(func(
				cache *MetaCache, ctx context.Context, database, collectionName string, collectionID int64, withCache bool,
			) ([]string, error) {
				return []string{}, nil
			}).Build()
			defer mockGetShardLeaderList.UnPatch()

			// Create upsert task
			task := &upsertTask{
				ctx:          ctx,
				collectionID: 1,
				schema:       schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_collection",
					DbName:         "default",
					FieldsData:     []*schemapb.FieldData{primaryKeyData},
					NumRows:        0,
				},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{
						InsertRequest: &msgpb.InsertRequest{
							FieldsData: []*schemapb.FieldData{primaryKeyData},
						},
					},
				},
			}

			// Execute test - this should pass because primary key data is empty (no existing records)
			err := task.queryPreExecute(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, task.deletePKs)
			assert.Equal(t, []*schemapb.FieldData{primaryKeyData}, task.insertFieldData)
		})

		t.Run("error - collection not loaded", func(t *testing.T) {
			// Mock globalMetaCache.GetShardLeaderList to return error for collection not loaded
			mockGetShardLeaderList := mockey.Mock((*MetaCache).GetShardLeaderList).To(func(
				cache *MetaCache, ctx context.Context, database, collectionName string, collectionID int64, withCache bool,
			) ([]string, error) {
				return nil, errors.New("collection not loaded")
			}).Build()
			defer mockGetShardLeaderList.UnPatch()

			// Mock retrieveByPKs to trigger the GetShardLeaderList call
			mockRetrieveByPKs := mockey.Mock(retrieveByPKs).To(func(
				ctx context.Context, t *upsertTask, ids *schemapb.IDs, outputFields []string,
			) (*milvuspb.QueryResults, error) {
				// This will internally call GetShardLeaderList which returns our mocked error
				return nil, errors.New("collection not loaded")
			}).Build()
			defer mockRetrieveByPKs.UnPatch()

			// Create primary key data with actual data to trigger retrieve call
			primaryKeyDataWithData := &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "id",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1}, // Non-empty data to trigger retrieve
							},
						},
					},
				},
			}

			// Create upsert task
			task := &upsertTask{
				ctx:          ctx,
				collectionID: 1,
				schema:       schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_collection",
					DbName:         "default",
					FieldsData:     []*schemapb.FieldData{primaryKeyDataWithData},
					NumRows:        1,
				},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{
						InsertRequest: &msgpb.InsertRequest{
							FieldsData: []*schemapb.FieldData{primaryKeyDataWithData},
						},
					},
				},
			}

			// Execute test
			err := task.queryPreExecute(ctx)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "collection not loaded")
		})

		t.Run("error - no primary key field in schema", func(t *testing.T) {
			// Create schema without primary key
			noPKSchema := &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  101,
						Name:     "vector",
						DataType: schemapb.DataType_FloatVector,
					},
				},
			}

			schemaInfo := newSchemaInfo(noPKSchema)

			// Create upsert task
			task := &upsertTask{
				ctx:          ctx,
				collectionID: 1,
				schema:       schemaInfo,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_collection",
					DbName:         "default",
					FieldsData:     []*schemapb.FieldData{primaryKeyData},
					NumRows:        0,
				},
			}

			// Execute test
			err := task.queryPreExecute(ctx)
			assert.Error(t, err)
		})

		t.Run("error - primary key field not found in request", func(t *testing.T) {
			// Create upsert task without primary key field data
			task := &upsertTask{
				ctx:          ctx,
				collectionID: 1,
				schema:       schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_collection",
					DbName:         "default",
					FieldsData:     []*schemapb.FieldData{vectorData}, // Missing primary key
					NumRows:        0,
				},
			}

			// Execute test
			err := task.queryPreExecute(ctx)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "must assign pk when upsert")
		})

		t.Run("success with existing data - update scenario", func(t *testing.T) {
			// Create primary key data with existing data
			existingPrimaryKeyData := &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "id",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2},
							},
						},
					},
				},
			}

			// Mock successful query result with existing data
			existingVectorData := &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				FieldId:   101,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 128,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: make([]float32, 256), // 2 records * 128 dim
							},
						},
					},
				},
			}

			mockQueryResults := &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				FieldsData: []*schemapb.FieldData{existingPrimaryKeyData, existingVectorData},
			}

			// Mock retrieveByPKs to return existing data
			mockRetrieveByPKs := mockey.Mock(retrieveByPKs).To(func(
				ctx context.Context, t *upsertTask, ids *schemapb.IDs, outputFields []string,
			) (*milvuspb.QueryResults, error) {
				return mockQueryResults, nil
			}).Build()
			defer mockRetrieveByPKs.UnPatch()

			// Create upsert task with existing data
			task := &upsertTask{
				ctx:          ctx,
				collectionID: 1,
				schema:       schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_collection",
					DbName:         "default",
					FieldsData:     []*schemapb.FieldData{existingPrimaryKeyData, existingVectorData},
					NumRows:        2,
				},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{
						InsertRequest: &msgpb.InsertRequest{
							FieldsData: []*schemapb.FieldData{existingPrimaryKeyData, existingVectorData},
						},
					},
				},
			}

			// Execute test
			err := task.queryPreExecute(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, task.deletePKs)
			assert.NotNil(t, task.insertFieldData)
		})

		t.Run("mixed scenario - 200 records with 100 existing and 100 new", func(t *testing.T) {
			// Create primary key data with 200 records (IDs 1-200)
			allPrimaryKeyIDs := make([]int64, 200)
			for i := 0; i < 200; i++ {
				allPrimaryKeyIDs[i] = int64(i + 1)
			}

			allPrimaryKeyData := &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "id",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: allPrimaryKeyIDs,
							},
						},
					},
				},
			}

			// Create vector data for all 200 records
			allVectorData := &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				FieldId:   101,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 128,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: make([]float32, 200*128), // 200 records * 128 dim
							},
						},
					},
				},
			}

			// Mock query results: only return first 100 records as "existing"
			existingPrimaryKeyIDs := make([]int64, 100)
			for i := 0; i < 100; i++ {
				existingPrimaryKeyIDs[i] = int64(i + 1)
			}

			existingPrimaryKeyData := &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "id",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: existingPrimaryKeyIDs,
							},
						},
					},
				},
			}

			existingVectorData := &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				FieldId:   101,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 128,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: make([]float32, 100*128), // 100 existing records * 128 dim
							},
						},
					},
				},
			}

			// Mock retrieveByPKs to return only the first 100 records (existing ones)
			mockQueryResults := &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				FieldsData: []*schemapb.FieldData{existingPrimaryKeyData, existingVectorData},
			}

			mockRetrieveByPKs := mockey.Mock(retrieveByPKs).To(func(
				ctx context.Context, t *upsertTask, ids *schemapb.IDs, outputFields []string,
			) (*milvuspb.QueryResults, error) {
				// Simulate that only first 100 records exist in the system
				return mockQueryResults, nil
			}).Build()
			defer mockRetrieveByPKs.UnPatch()

			// Create upsert task with all 200 records
			task := &upsertTask{
				ctx:          ctx,
				collectionID: 1,
				schema:       schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_collection",
					DbName:         "default",
					FieldsData:     []*schemapb.FieldData{allPrimaryKeyData, allVectorData},
					NumRows:        200,
				},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{
						InsertRequest: &msgpb.InsertRequest{
							FieldsData: []*schemapb.FieldData{allPrimaryKeyData, allVectorData},
						},
					},
				},
			}

			// Execute test
			err := task.queryPreExecute(ctx)
			assert.NoError(t, err)

			// Verify results
			assert.NotNil(t, task.deletePKs, "deletePKs should not be nil")
			assert.NotNil(t, task.insertFieldData, "insertFieldData should not be nil")

			// Verify deletePKs contains exactly the 100 existing primary keys
			if task.deletePKs != nil {
				switch pkData := task.deletePKs.GetIdField().(type) {
				case *schemapb.IDs_IntId:
					assert.Equal(t, 100, len(pkData.IntId.Data), "deletePKs should contain 100 existing primary keys")
					// Verify the IDs are 1-100 (the existing ones)
					for i, id := range pkData.IntId.Data {
						assert.Equal(t, int64(i+1), id, "deletePKs should contain primary key %d", i+1)
					}
				default:
					t.Errorf("Expected IntId type for deletePKs, got %T", pkData)
				}
			}

			// Verify insertFieldData contains all 200 records
			assert.Equal(t, 2, len(task.insertFieldData), "insertFieldData should contain 2 fields (primary key + vector)")

			// Find primary key field in insertFieldData
			var insertPKField *schemapb.FieldData
			for _, field := range task.insertFieldData {
				if field.FieldName == "id" {
					insertPKField = field
					break
				}
			}
			assert.NotNil(t, insertPKField, "insertFieldData should contain primary key field")

			if insertPKField != nil {
				pkScalars := insertPKField.GetScalars()
				assert.NotNil(t, pkScalars, "primary key field should have scalars")
				if pkScalars != nil {
					longData := pkScalars.GetLongData()
					assert.NotNil(t, longData, "primary key field should have long data")
					if longData != nil {
						assert.Equal(t, 200, len(longData.Data), "insertFieldData should contain all 200 primary keys")
						// Verify all IDs 1-200 are present
						for i, id := range longData.Data {
							assert.Equal(t, int64(i+1), id, "insertFieldData should contain primary key %d", i+1)
						}
					}
				}
			}
		})
	})
}
