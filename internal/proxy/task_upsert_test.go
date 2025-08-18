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
	grpcmixcoordclient "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

// Helper function to create test updateTask
func createTestUpdateTask() *upsertTask {
	mcClient := &grpcmixcoordclient.Client{}

	upsertTask := &upsertTask{
		baseTask:  baseTask{},
		Condition: NewTaskCondition(context.Background()),
		req: &milvuspb.UpsertRequest{
			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "_default",
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "id",
					FieldId:   100,
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
							},
						},
					},
				},
				{
					FieldName: "name",
					FieldId:   102,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"test1", "test2", "test3"}},
							},
						},
					},
				},
				{
					FieldName: "vector",
					FieldId:   101,
					Type:      schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 128,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: make([]float32, 384)}, // 3 * 128
							},
						},
					},
				},
			},
			NumRows: 3,
		},
		ctx:          context.Background(),
		schema:       createTestSchema(),
		collectionID: 1001,
		node: &Proxy{
			mixCoord: mcClient,
			lbPolicy: NewLBPolicyImpl(nil),
		},
	}

	return upsertTask
}

// Helper function to create test schema
func createTestSchema() *schemaInfo {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
			{
				FieldID:  102,
				Name:     "name",
				DataType: schemapb.DataType_VarChar,
			},
		},
	}
	return newSchemaInfo(schema)
}

func TestRetrieveByPKs_Success(t *testing.T) {
	mockey.PatchConvey("TestRetrieveByPKs_Success", t, func() {
		// Setup mocks
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(&schemapb.FieldSchema{
			FieldID:      100,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}, nil).Build()

		mockey.Mock(validatePartitionTag).Return(nil).Build()

		mockey.Mock((*MetaCache).GetPartitionID).Return(int64(1002), nil).Build()

		mockey.Mock(planparserv2.CreateRequeryPlan).Return(&planpb.PlanNode{}).Build()

		mockey.Mock((*Proxy).query).Return(&milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "id",
					FieldId:   100,
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2}},
							},
						},
					},
				},
			},
		}, nil).Build()

		globalMetaCache = &MetaCache{}
		mockey.Mock(globalMetaCache.GetPartitionID).Return(int64(1002), nil).Build()

		// Execute test
		task := createTestUpdateTask()
		task.partitionKeyMode = false
		task.upsertMsg = &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					PartitionName: "_default",
				},
			},
			DeleteMsg: &msgstream.DeleteMsg{
				DeleteRequest: &msgpb.DeleteRequest{
					PartitionName: "_default",
				},
			},
		}

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		}

		result, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, commonpb.ErrorCode_Success, result.Status.ErrorCode)
		assert.Len(t, result.FieldsData, 1)
	})
}

func TestRetrieveByPKs_GetPrimaryFieldSchemaError(t *testing.T) {
	mockey.PatchConvey("TestRetrieveByPKs_GetPrimaryFieldSchemaError", t, func() {
		expectedErr := merr.WrapErrParameterInvalidMsg("primary field not found")
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(nil, expectedErr).Build()

		task := createTestUpdateTask()
		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		}

		result, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "primary field not found")
	})
}

func TestRetrieveByPKs_PartitionKeyMode(t *testing.T) {
	mockey.PatchConvey("TestRetrieveByPKs_PartitionKeyMode", t, func() {
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(&schemapb.FieldSchema{
			FieldID:      100,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}, nil).Build()

		mockey.Mock(planparserv2.CreateRequeryPlan).Return(&planpb.PlanNode{}).Build()

		mockey.Mock((*Proxy).query).Return(&milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{},
		}, nil).Build()

		task := createTestUpdateTask()
		task.partitionKeyMode = true

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		}

		result, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestUpdateTask_queryPreExecute_Success(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_queryPreExecute_Success", t, func() {
		// Setup mocks
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(&schemapb.FieldSchema{
			FieldID:      100,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}, nil).Build()

		mockey.Mock(typeutil.GetPrimaryFieldData).Return(&schemapb.FieldData{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		}, nil).Build()

		mockey.Mock(parsePrimaryFieldData2IDs).Return(&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		}, nil).Build()

		mockey.Mock(typeutil.GetSizeOfIDs).Return(3).Build()

		mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "id",
					FieldId:   100,
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2}},
							},
						},
					},
				},
				{
					FieldName: "name",
					FieldId:   102,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"old1", "old2"}},
							},
						},
					},
				},
				{
					FieldName: "vector",
					FieldId:   101,
					Type:      schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 128,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: make([]float32, 256)}, // 2 * 128
							},
						},
					},
				},
			},
		}, nil).Build()

		mockey.Mock(typeutil.NewIDsChecker).Return(&typeutil.IDsChecker{}, nil).Build()

		// Execute test
		task := createTestUpdateTask()
		task.schema = createTestSchema()
		task.upsertMsg = &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "id",
							FieldId:   100,
							Type:      schemapb.DataType_Int64,
						},
						{
							FieldName: "name",
							FieldId:   102,
							Type:      schemapb.DataType_VarChar,
						},
						{
							FieldName: "vector",
							FieldId:   101,
							Type:      schemapb.DataType_FloatVector,
						},
					},
				},
			},
		}

		err := task.queryPreExecute(context.Background())

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, task.deletePKs)
		assert.NotNil(t, task.insertFieldData)
	})
}

func TestUpdateTask_queryPreExecute_GetPrimaryFieldSchemaError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_queryPreExecute_GetPrimaryFieldSchemaError", t, func() {
		expectedErr := merr.WrapErrParameterInvalidMsg("primary field not found")
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(nil, expectedErr).Build()

		task := createTestUpdateTask()
		task.schema = createTestSchema()

		err := task.queryPreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "primary field not found")
	})
}

func TestUpdateTask_queryPreExecute_GetPrimaryFieldDataError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_queryPreExecute_GetPrimaryFieldDataError", t, func() {
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(&schemapb.FieldSchema{
			FieldID:      100,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}, nil).Build()

		expectedErr := merr.WrapErrParameterInvalidMsg("primary field data not found")
		mockey.Mock(typeutil.GetPrimaryFieldData).Return(nil, expectedErr).Build()

		task := createTestUpdateTask()
		task.schema = createTestSchema()

		err := task.queryPreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must assign pk when upsert")
	})
}

func TestUpdateTask_queryPreExecute_EmptyOldIDs(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_queryPreExecute_EmptyOldIDs", t, func() {
		mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(&schemapb.FieldSchema{
			FieldID:      100,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}, nil).Build()

		mockey.Mock(typeutil.GetPrimaryFieldData).Return(&schemapb.FieldData{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
		}, nil).Build()

		mockey.Mock(parsePrimaryFieldData2IDs).Return(&schemapb.IDs{}, nil).Build()

		mockey.Mock(typeutil.GetSizeOfIDs).Return(0).Build()

		task := createTestUpdateTask()
		task.schema = createTestSchema()

		err := task.queryPreExecute(context.Background())

		assert.NoError(t, err)
		assert.NotNil(t, task.deletePKs)
		assert.Equal(t, task.req.GetFieldsData(), task.insertFieldData)
	})
}

func TestUpdateTask_PreExecute_Success(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_Success", t, func() {
		// Setup mocks
		globalMetaCache = &MetaCache{}

		mockey.Mock(GetReplicateID).Return("", nil).Build()

		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()

		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
		}, nil).Build()

		mockey.Mock((*MetaCache).GetCollectionSchema).Return(createTestSchema(), nil).Build()

		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()

		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			name: "_default",
		}, nil).Build()

		mockey.Mock((*upsertTask).queryPreExecute).Return(nil).Build()

		mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()

		mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()

		// Execute test
		task := createTestUpdateTask()
		task.req.PartialUpdate = true

		err := task.PreExecute(context.Background())

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, int64(1001), task.collectionID)
		assert.NotNil(t, task.schema)
		assert.NotNil(t, task.upsertMsg)
	})
}

func TestUpdateTask_PreExecute_ReplicateIDError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_ReplicateIDError", t, func() {
		globalMetaCache = &MetaCache{}

		mockey.Mock(GetReplicateID).Return("replica1", nil).Build()

		task := createTestUpdateTask()

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can't operate on the collection under standby mode")
	})
}

func TestUpdateTask_PreExecute_GetCollectionIDError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_GetCollectionIDError", t, func() {
		globalMetaCache = &MetaCache{}

		mockey.Mock(GetReplicateID).Return("", nil).Build()

		expectedErr := merr.WrapErrCollectionNotFound("test_collection")
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(0), expectedErr).Build()

		task := createTestUpdateTask()

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
	})
}

func TestUpdateTask_PreExecute_PartitionKeyModeError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_PartitionKeyModeError", t, func() {
		globalMetaCache = &MetaCache{}

		mockey.Mock(GetReplicateID).Return("", nil).Build()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(createTestSchema(), nil).Build()

		mockey.Mock(isPartitionKeyMode).Return(true, nil).Build()

		task := createTestUpdateTask()
		task.req.PartitionName = "custom_partition" // This should cause error in partition key mode

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not support manually specifying the partition names if partition key mode is used")
	})
}

func TestUpdateTask_PreExecute_InvalidNumRows(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_InvalidNumRows", t, func() {
		globalMetaCache = &MetaCache{}

		mockey.Mock(GetReplicateID).Return("", nil).Build()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(createTestSchema(), nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			name: "_default",
		}, nil).Build()

		task := createTestUpdateTask()
		task.req.NumRows = 0 // Invalid num_rows

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid num_rows")
	})
}

func TestUpdateTask_PreExecute_QueryPreExecuteError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_QueryPreExecuteError", t, func() {
		globalMetaCache = &MetaCache{}

		mockey.Mock(GetReplicateID).Return("", nil).Build()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(createTestSchema(), nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			name: "_default",
		}, nil).Build()

		expectedErr := merr.WrapErrParameterInvalidMsg("query pre-execute failed")
		mockey.Mock((*upsertTask).queryPreExecute).Return(expectedErr).Build()

		task := createTestUpdateTask()
		task.req.PartialUpdate = true

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "query pre-execute failed")
	})
}
