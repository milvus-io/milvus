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
	"encoding/json"
	"testing"

	"github.com/bytedance/mockey"
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
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
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

func TestUpsertTask_Function(t *testing.T) {
	paramtable.Init()
	paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
		return map[string]string{
			"mock.apikey": "mock",
		}
	}
	ts := embedding.CreateOpenAIEmbeddingServer()
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
	err = genFunctionFields(task.ctx, task.upsertMsg.InsertMsg, task.schema, task.req.GetPartialUpdate())
	assert.NoError(t, err)
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
			schema: newSchemaInfo(&schemapb.CollectionSchema{
				Name: "col-0",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			}),
		}, nil)
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
			lbPolicy: shardclient.NewLBPolicyImpl(nil),
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
		}, segcore.StorageCost{}, nil).Build()

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

		result, _, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

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

		result, _, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

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
		}, segcore.StorageCost{}, nil).Build()

		task := createTestUpdateTask()
		task.partitionKeyMode = true

		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		}

		result, _, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

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
		}, segcore.StorageCost{}, nil).Build()

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

		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()

		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
			schema:          schema,
		}, nil).Build()

		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()

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

func TestUpdateTask_PreExecute_GetCollectionIDError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_GetCollectionIDError", t, func() {
		globalMetaCache = &MetaCache{}

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

		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
			schema:          schema,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()

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

		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
			schema:          schema,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			name: "_default",
		}, nil).Build()

		task := createTestUpdateTask()
		task.req.FieldsData = []*schemapb.FieldData{}
		task.req.NumRows = 0 // Invalid num_rows

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid num_rows")
	})
}

func TestUpdateTask_PreExecute_QueryPreExecuteError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_QueryPreExecuteError", t, func() {
		globalMetaCache = &MetaCache{}

		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			updateTimestamp: 12345,
			schema:          schema,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
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

func TestUpsertTask_queryPreExecute_MixLogic(t *testing.T) {
	// Schema for the test collection
	schema := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_merge_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{FieldID: 102, Name: "extra", DataType: schemapb.DataType_VarChar, Nullable: true},
		},
	})

	// Upsert IDs: 1 (update), 2 (update), 3 (insert)
	upsertData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}}}}},
		},
	}
	numRows := uint64(len(upsertData[0].GetScalars().GetLongData().GetData()))

	// Query result for existing PKs: 1, 2
	mockQueryResult := &milvuspb.QueryResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
			},
			{
				FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10, 20}}}}},
			},
			{
				FieldName: "extra", FieldId: 102, Type: schemapb.DataType_VarChar,
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"old1", "old2"}}}}},
				ValidData: []bool{true, true},
			},
		},
	}

	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData: upsertData,
			NumRows:    uint32(numRows),
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: upsertData,
					NumRows:    numRows,
				},
			},
		},
		node: &Proxy{},
	}

	mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
	defer mockRetrieve.UnPatch()

	err := task.queryPreExecute(context.Background())
	assert.NoError(t, err)

	// Verify delete PKs
	deletePks := task.deletePKs.GetIntId().GetData()
	assert.ElementsMatch(t, []int64{1, 2}, deletePks)

	// Verify merged insert data
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema.CollectionSchema)
	assert.NoError(t, err)
	idField, err := typeutil.GetPrimaryFieldData(task.insertFieldData, primaryFieldSchema)
	assert.NoError(t, err)
	ids, err := parsePrimaryFieldData2IDs(idField)
	assert.NoError(t, err)
	insertPKs := ids.GetIntId().GetData()
	assert.Equal(t, []int64{1, 2, 3}, insertPKs)

	var valueField *schemapb.FieldData
	for _, f := range task.insertFieldData {
		if f.GetFieldName() == "value" {
			valueField = f
			break
		}
	}
	assert.NotNil(t, valueField)
	assert.Equal(t, []int32{100, 200, 300}, valueField.GetScalars().GetIntData().GetData())
}

func TestUpsertTask_queryPreExecute_PureInsert(t *testing.T) {
	// Schema for the test collection
	schema := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_merge_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{FieldID: 102, Name: "extra", DataType: schemapb.DataType_VarChar, Nullable: true},
		},
	})

	// Upsert IDs: 4, 5
	upsertData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{4, 5}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{400, 500}}}}},
		},
	}
	numRows := uint64(len(upsertData[0].GetScalars().GetLongData().GetData()))

	// Query result is empty, but schema is preserved
	mockQueryResult := &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}}},
		},
		{
			FieldName: "extra", FieldId: 102, Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{}}}}},
		},
	}}

	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData: upsertData,
			NumRows:    uint32(numRows),
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: upsertData,
					NumRows:    numRows,
				},
			},
		},
		node: &Proxy{},
	}

	mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
	defer mockRetrieve.UnPatch()

	err := task.queryPreExecute(context.Background())
	assert.NoError(t, err)

	// Verify delete PKs
	deletePks := task.deletePKs.GetIntId().GetData()
	assert.Empty(t, deletePks)

	// Verify merged insert data
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema.CollectionSchema)
	assert.NoError(t, err)
	idField, err := typeutil.GetPrimaryFieldData(task.insertFieldData, primaryFieldSchema)
	assert.NoError(t, err)
	ids, err := parsePrimaryFieldData2IDs(idField)
	assert.NoError(t, err)
	insertPKs := ids.GetIntId().GetData()
	assert.Equal(t, []int64{4, 5}, insertPKs)

	var valueField *schemapb.FieldData
	for _, f := range task.insertFieldData {
		if f.GetFieldName() == "value" {
			valueField = f
			break
		}
	}
	assert.NotNil(t, valueField)
	assert.Equal(t, []int32{400, 500}, valueField.GetScalars().GetIntData().GetData())
}

func TestUpsertTask_queryPreExecute_PureUpdate(t *testing.T) {
	// Schema for the test collection
	schema := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_merge_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{FieldID: 102, Name: "extra", DataType: schemapb.DataType_VarChar, Nullable: true},
		},
	})

	// Upsert IDs: 6, 7
	upsertData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{6, 7}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{600, 700}}}}},
		},
	}
	numRows := uint64(len(upsertData[0].GetScalars().GetLongData().GetData()))

	// Query result for existing PKs: 6, 7
	mockQueryResult := &milvuspb.QueryResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{6, 7}}}}},
			},
			{
				FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{60, 70}}}}},
			},
		},
	}

	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData: upsertData,
			NumRows:    uint32(numRows),
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: upsertData,
					NumRows:    numRows,
				},
			},
		},
		node: &Proxy{},
	}

	mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
	defer mockRetrieve.UnPatch()

	err := task.queryPreExecute(context.Background())
	assert.NoError(t, err)

	// Verify delete PKs
	deletePks := task.deletePKs.GetIntId().GetData()
	assert.ElementsMatch(t, []int64{6, 7}, deletePks)

	// Verify merged insert data
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema.CollectionSchema)
	assert.NoError(t, err)
	idField, err := typeutil.GetPrimaryFieldData(task.insertFieldData, primaryFieldSchema)
	assert.NoError(t, err)
	ids, err := parsePrimaryFieldData2IDs(idField)
	assert.NoError(t, err)
	insertPKs := ids.GetIntId().GetData()
	assert.Equal(t, []int64{6, 7}, insertPKs)

	var valueField *schemapb.FieldData
	for _, f := range task.insertFieldData {
		if f.GetFieldName() == "value" {
			valueField = f
			break
		}
	}
	assert.NotNil(t, valueField)
	assert.Equal(t, []int32{600, 700}, valueField.GetScalars().GetIntData().GetData())
}

func TestCheckDynamicFieldDataForPartialUpdate(t *testing.T) {
	t.Run("preserves $meta keys matching static field names after schema evolution", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "dfA", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		// $meta contains {"dfA": 111, "dfB": "keep_me", "dfC": 999}
		// All keys must be preserved — including "dfA" which matches a static field name.
		metaJSON, _ := json.Marshal(map[string]interface{}{"dfA": 111, "dfB": "keep_me", "dfC": 999})
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.NoError(t, err)

		jsonData := insertMsg.FieldsData[0].GetScalars().GetJsonData().GetData()
		assert.Len(t, jsonData, 1)

		var m map[string]interface{}
		err = json.Unmarshal(jsonData[0], &m)
		assert.NoError(t, err)
		assert.Contains(t, m, "dfA", "key matching static field name must be preserved")
		assert.Contains(t, m, "dfB", "non-conflicting key must be preserved")
		assert.Equal(t, "keep_me", m["dfB"])
		assert.Contains(t, m, "dfC", "non-conflicting key must be preserved")
	})

	t.Run("rejects $meta key in dynamic field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		metaJSON := []byte(`{"$meta": "bad_value"}`)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "$meta")
	})

	t.Run("rejects malformed JSON", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{invalid json`)}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.Error(t, err)
	})

	t.Run("rejects dynamic field when dynamic schema is disabled", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: false,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			},
		}

		metaJSON := []byte(`{"key": "value"}`)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "without dynamic schema enabled")
	})

	t.Run("auto-generates empty dynamic field when none present", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			},
		}

		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				NumRows: 2,
				Version: msgpb.InsertDataVersion_ColumnBased,
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.NoError(t, err)
		// Should have appended a dynamic field
		assert.Len(t, insertMsg.FieldsData, 2)
		assert.True(t, insertMsg.FieldsData[1].IsDynamic)
		assert.Len(t, insertMsg.FieldsData[1].GetScalars().GetJsonData().GetData(), 2)
	})

	t.Run("strict checkDynamicFieldData rejects what partial update allows", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "end_timestamp", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		makeMsg := func() *msgstream.InsertMsg {
			metaJSON := []byte(`{"end_timestamp": 1234, "color": "red"}`)
			return &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
							}}},
						},
					},
				},
			}
		}

		// Strict path must reject: $meta contains "end_timestamp" which is a static field
		err := checkDynamicFieldData(schema, makeMsg())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "end_timestamp")

		// Partial update path must allow the same data
		err = checkDynamicFieldDataForPartialUpdate(schema, makeMsg())
		assert.NoError(t, err)
	})

	t.Run("multiple rows with mixed dynamic keys", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "status", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		row1 := []byte(`{"status": "active", "color": "red"}`)   // "status" matches static field
		row2 := []byte(`{"color": "blue", "size": 42}`)          // no conflict
		row3 := []byte(`{"status": "done", "tag": "important"}`) // "status" matches static field

		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{row1, row2, row3}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.NoError(t, err)

		// Verify all 3 rows preserved intact
		jsonRows := insertMsg.FieldsData[0].GetScalars().GetJsonData().GetData()
		assert.Len(t, jsonRows, 3)
		for i, row := range jsonRows {
			var m map[string]interface{}
			assert.NoError(t, json.Unmarshal(row, &m), "row %d must be valid JSON", i)
		}
	})

	t.Run("multiple static fields with overlapping keys in $meta", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "fieldA", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "fieldB", DataType: schemapb.DataType_VarChar},
				{FieldID: 103, Name: "fieldC", DataType: schemapb.DataType_Float},
				{FieldID: 104, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		// $meta contains keys matching ALL 3 static fields plus an extra dynamic key
		metaJSON := []byte(`{"fieldA": 1, "fieldB": "val", "fieldC": 3.14, "extra": true}`)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 104, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.NoError(t, err)

		var m map[string]interface{}
		err = json.Unmarshal(insertMsg.FieldsData[0].GetScalars().GetJsonData().GetData()[0], &m)
		assert.NoError(t, err)
		assert.Contains(t, m, "fieldA")
		assert.Contains(t, m, "fieldB")
		assert.Contains(t, m, "fieldC")
		assert.Contains(t, m, "extra")
	})

	t.Run("sets FieldName to $meta for IsDynamic field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		metaJSON := []byte(`{"color": "green"}`)
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "original_name", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.NoError(t, err)
		// The function must normalize FieldName to "$meta"
		assert.Equal(t, "$meta", insertMsg.FieldsData[0].GetFieldName())
	})

	t.Run("non-conflicting keys pass both strict and partial update", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "status", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		makeMsg := func() *msgstream.InsertMsg {
			metaJSON := []byte(`{"color": "blue", "size": 42}`)
			return &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: []*schemapb.FieldData{
						{
							FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{metaJSON}},
							}}},
						},
					},
				},
			}
		}

		// Both paths must accept $meta with no static field conflicts
		err := checkDynamicFieldData(schema, makeMsg())
		assert.NoError(t, err)

		err = checkDynamicFieldDataForPartialUpdate(schema, makeMsg())
		assert.NoError(t, err)
	})

	t.Run("empty JSON object in $meta", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:               "test_collection",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}

		insertMsg := &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: []*schemapb.FieldData{
					{
						FieldName: "$meta", FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{}`)}},
						}}},
					},
				},
			},
		}

		err := checkDynamicFieldDataForPartialUpdate(schema, insertMsg)
		assert.NoError(t, err)
	})
}

// Test ToCompressedFormatNullable for Geometry and Timestamptz types
func TestToCompressedFormatNullable_GeometryAndTimestamptz(t *testing.T) {
	t.Run("timestamptz with null values", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_Timestamptz,
			FieldName: "timestamp_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_TimestamptzData{
						TimestamptzData: &schemapb.TimestamptzArray{
							Data: []int64{1000, 0, 3000, 0},
						},
					},
				},
			},
			ValidData: []bool{true, false, true, false},
		}

		err := ToCompressedFormatNullable(field)
		assert.NoError(t, err)
		assert.Equal(t, []int64{1000, 3000}, field.GetScalars().GetTimestamptzData().GetData())
		assert.Equal(t, []bool{true, false, true, false}, field.ValidData)
	})

	t.Run("geometry WKT with null values", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_Geometry,
			FieldName: "geometry_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{
						GeometryWktData: &schemapb.GeometryWktArray{
							Data: []string{"POINT (1 2)", "", "POINT (5 6)"},
						},
					},
				},
			},
			ValidData: []bool{true, false, true},
		}

		err := ToCompressedFormatNullable(field)
		assert.NoError(t, err)
		assert.Equal(t, []string{"POINT (1 2)", "POINT (5 6)"}, field.GetScalars().GetGeometryWktData().GetData())
	})

	t.Run("geometry WKB with null values", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_Geometry,
			FieldName: "geometry_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryData{
						GeometryData: &schemapb.GeometryArray{
							Data: [][]byte{{0x01, 0x02}, nil, {0x05, 0x06}},
						},
					},
				},
			},
			ValidData: []bool{true, false, true},
		}

		err := ToCompressedFormatNullable(field)
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{0x01, 0x02}, {0x05, 0x06}}, field.GetScalars().GetGeometryData().GetData())
	})
}

// Test GenNullableFieldData for Geometry and Timestamptz types
func TestGenNullableFieldData_GeometryAndTimestamptz(t *testing.T) {
	t.Run("generate timestamptz nullable field", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID:   100,
			Name:      "timestamp_field",
			DataType:  schemapb.DataType_Timestamptz,
			IsDynamic: false,
		}

		upsertIDSize := 5
		fieldData, err := GenNullableFieldData(field, upsertIDSize)

		assert.NoError(t, err)
		assert.NotNil(t, fieldData)
		assert.Equal(t, int64(100), fieldData.FieldId)
		assert.Equal(t, "timestamp_field", fieldData.FieldName)
		assert.Len(t, fieldData.ValidData, upsertIDSize)
		assert.Len(t, fieldData.GetScalars().GetTimestamptzData().GetData(), upsertIDSize)
	})

	t.Run("generate geometry nullable field", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID:   101,
			Name:      "geometry_field",
			DataType:  schemapb.DataType_Geometry,
			IsDynamic: false,
		}

		upsertIDSize := 3
		fieldData, err := GenNullableFieldData(field, upsertIDSize)

		assert.NoError(t, err)
		assert.NotNil(t, fieldData)
		assert.Equal(t, int64(101), fieldData.FieldId)
		assert.Equal(t, "geometry_field", fieldData.FieldName)
		assert.Len(t, fieldData.ValidData, upsertIDSize)
		assert.Len(t, fieldData.GetScalars().GetGeometryWktData().GetData(), upsertIDSize)
	})
}

func TestUpsertTask_DuplicatePK_Int64(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_duplicate_pk",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
		},
	}

	// Data with duplicate primary keys: 1, 2, 1 (duplicate)
	fieldsData := []*schemapb.FieldData{
		{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 1}},
					},
				},
			},
		},
		{
			FieldName: "value",
			FieldId:   101,
			Type:      schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}},
					},
				},
			},
		},
	}

	// Test CheckDuplicatePkExist directly
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	assert.NoError(t, err)
	hasDuplicate, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
	assert.NoError(t, err)
	assert.True(t, hasDuplicate, "should detect duplicate primary keys")
}

func TestUpsertTask_DuplicatePK_VarChar(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_duplicate_pk_varchar",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "100"}}},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
		},
	}

	// Data with duplicate primary keys: "a", "b", "a" (duplicate)
	fieldsData := []*schemapb.FieldData{
		{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"a", "b", "a"}},
					},
				},
			},
		},
		{
			FieldName: "value",
			FieldId:   101,
			Type:      schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}},
					},
				},
			},
		},
	}

	// Test CheckDuplicatePkExist directly
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	assert.NoError(t, err)
	hasDuplicate, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
	assert.NoError(t, err)
	assert.True(t, hasDuplicate, "should detect duplicate primary keys")
}

func TestUpsertTask_NoDuplicatePK(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_no_duplicate_pk",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
		},
	}

	// Data with unique primary keys: 1, 2, 3
	fieldsData := []*schemapb.FieldData{
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
			FieldName: "value",
			FieldId:   101,
			Type:      schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}},
					},
				},
			},
		},
	}

	// Call CheckDuplicatePkExist directly to verify no duplicate error
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	assert.NoError(t, err)
	hasDuplicate, err := CheckDuplicatePkExist(primaryFieldSchema, fieldsData)
	assert.NoError(t, err)
	assert.False(t, hasDuplicate, "should not have duplicate primary keys")
}

// TestUpsertTask_queryPreExecute_EmptyDataArray tests the scenario where:
// 1. Partial update is enabled
// 2. Three columns are passed: pk (a), vector (b), scalar (c)
// 3. Columns a and b have 10 rows of data, column c has FieldData but empty data array
// 4. Verifies both nullable and non-nullable scenarios for column c
func TestUpsertTask_queryPreExecute_EmptyDataArray(t *testing.T) {
	numRows := 10
	dim := 128

	t.Run("scalar field with empty data array nullable field", func(t *testing.T) {
		// Schema with nullable scalar field c
		schema := newSchemaInfo(&schemapb.CollectionSchema{
			Name: "test_empty_data_array",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "a", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{
					FieldID:  101,
					Name:     "b",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					},
				},
				{FieldID: 102, Name: "c", DataType: schemapb.DataType_Int32, Nullable: true},
			},
		})

		// Upsert data: a (pk, 10 rows), b (vector, 10 rows), c (scalar, FieldData exists but data array is empty)
		pkData := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			pkData[i] = int64(i + 1)
		}
		vectorData := make([]float32, numRows*dim)

		upsertData := []*schemapb.FieldData{
			{
				FieldName: "a", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pkData}}}},
			},
			{
				FieldName: "b", FieldId: 101, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectorData}}}},
			},
			{
				// c has FieldData but empty data array
				FieldName: "c", FieldId: 102, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}}},
			},
		}

		// Query result returns empty (all are new inserts)
		mockQueryResult := &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "a", FieldId: 100, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{}}}}},
				},
				{
					FieldName: "b", FieldId: 101, Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{}}}}},
				},
				{
					FieldName: "c", FieldId: 102, Type: schemapb.DataType_Int32,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}}},
				},
			},
		}

		mockey.PatchConvey("test nullable field", t, func() {
			// Setup mocks using mockey
			mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
			mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{updateTimestamp: 12345, schema: schema}, nil).Build()
			mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
			mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
			mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{name: "_default"}, nil).Build()
			mockey.Mock((*MetaCache).GetDatabaseInfo).Return(&databaseInfo{dbID: 0}, nil).Build()
			mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()

			globalMetaCache = &MetaCache{}

			// Setup idAllocator
			ctx := context.Background()
			rc := mocks.NewMockRootCoordClient(t)
			rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
				Status: merr.Status(nil),
				ID:     1000,
				Count:  uint32(numRows),
			}, nil).Maybe()
			idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
			assert.NoError(t, err)
			idAllocator.Start()
			defer idAllocator.Close()

			task := &upsertTask{
				ctx:    ctx,
				schema: schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_empty_data_array",
					FieldsData:     upsertData,
					NumRows:        uint32(numRows),
				},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{
						InsertRequest: &msgpb.InsertRequest{
							CollectionName: "test_empty_data_array",
							FieldsData:     upsertData,
							NumRows:        uint64(numRows),
						},
					},
				},
				idAllocator: idAllocator,
				result:      &milvuspb.MutationResult{},
				node:        &Proxy{},
			}

			// case1: test upsert
			err = task.PreExecute(ctx)
			assert.Error(t, err)

			// case2: test partial update
			task.req.PartialUpdate = true
			err = task.PreExecute(ctx)
			assert.Error(t, err)
		})
	})

	t.Run("scalar field with empty data array - non-nullable field", func(t *testing.T) {
		// Schema with non-nullable scalar field c
		schema := newSchemaInfo(&schemapb.CollectionSchema{
			Name: "test_empty_data_array_non_nullable",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "a", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{
					FieldID:  101,
					Name:     "b",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					},
				},
				{FieldID: 102, Name: "c", DataType: schemapb.DataType_Int32, Nullable: false},
			},
		})

		// Upsert data: a (pk, 10 rows), b (vector, 10 rows), c (scalar, FieldData exists but data array is empty)
		pkData := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			pkData[i] = int64(i + 1)
		}
		vectorData := make([]float32, numRows*dim)

		upsertData := []*schemapb.FieldData{
			{
				FieldName: "a", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pkData}}}},
			},
			{
				FieldName: "b", FieldId: 101, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectorData}}}},
			},
			{
				// c has FieldData but empty data array - this should cause validation error for non-nullable field
				FieldName: "c", FieldId: 102, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}}},
			},
		}

		// Query result returns empty (all are new inserts)
		mockQueryResult := &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "a", FieldId: 100, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{}}}}},
				},
				{
					FieldName: "b", FieldId: 101, Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{}}}}},
				},
				{
					FieldName: "c", FieldId: 102, Type: schemapb.DataType_Int32,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{}}}}},
				},
			},
		}

		mockey.PatchConvey("test non-nullable field", t, func() {
			// Setup mocks using mockey
			mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
			mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{updateTimestamp: 12345, schema: schema}, nil).Build()
			mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
			mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
			mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{name: "_default"}, nil).Build()
			mockey.Mock((*MetaCache).GetDatabaseInfo).Return(&databaseInfo{dbID: 0}, nil).Build()
			mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()

			globalMetaCache = &MetaCache{}

			// Setup idAllocator
			ctx := context.Background()
			rc := mocks.NewMockRootCoordClient(t)
			rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
				Status: merr.Status(nil),
				ID:     1000,
				Count:  uint32(numRows),
			}, nil).Maybe()
			idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
			assert.NoError(t, err)
			idAllocator.Start()
			defer idAllocator.Close()

			task := &upsertTask{
				ctx:    ctx,
				schema: schema,
				req: &milvuspb.UpsertRequest{
					CollectionName: "test_empty_data_array_non_nullable",
					FieldsData:     upsertData,
					NumRows:        uint32(numRows),
				},
				upsertMsg: &msgstream.UpsertMsg{
					InsertMsg: &msgstream.InsertMsg{
						InsertRequest: &msgpb.InsertRequest{
							CollectionName: "test_empty_data_array_non_nullable",
							FieldsData:     upsertData,
							NumRows:        uint64(numRows),
						},
					},
				},
				idAllocator: idAllocator,
				result:      &milvuspb.MutationResult{},
				node:        &Proxy{},
			}

			// case1: test upsert
			err = task.PreExecute(ctx)
			assert.Error(t, err)

			// case2: test partial update
			task.req.PartialUpdate = true
			err = task.PreExecute(ctx)
			assert.Error(t, err)
		})
	})
}

func TestUpsertTask_queryPreExecute_NullableFields(t *testing.T) {
	dim := int64(4)

	schema := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_nullable_vec",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
			{FieldID: 102, Name: "nullable_vec", DataType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
		},
	})

	// Generate vector data: [pk, pk, pk, pk]
	genVec := func(pk int64) []float32 {
		return []float32{float32(pk), float32(pk), float32(pk), float32(pk)}
	}

	// Create all_columns upsert data (includes nullable_vec)
	// nullable_vec = [pk+100, pk+100, pk+100, pk+100], ValidData = all true
	createAllCols := func(pks []int64) []*schemapb.FieldData {
		var ids []int64
		var vecData, nullableData []float32
		var validData []bool
		for _, pk := range pks {
			ids = append(ids, pk)
			vecData = append(vecData, genVec(pk)...)
			nullableData = append(nullableData, genVec(pk+100)...)
			validData = append(validData, true)
		}
		return []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}},
			},
			{
				FieldName: "vector", FieldId: 101, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vecData}}}},
			},
			{
				FieldName: "nullable_vec", FieldId: 102, Type: schemapb.DataType_FloatVector, ValidData: validData,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: nullableData}}}},
			},
		}
	}

	// Create partial_columns upsert data (excludes nullable_vec)
	createPartialCols := func(pks []int64) []*schemapb.FieldData {
		var ids []int64
		var vecData []float32
		for _, pk := range pks {
			ids = append(ids, pk)
			vecData = append(vecData, genVec(pk)...)
		}
		return []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}},
			},
			{
				FieldName: "vector", FieldId: 101, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vecData}}}},
			},
		}
	}

	// Create mock query result
	// existing nullable_vec = [pk+300, pk+300, pk+300, pk+300], ValidData = all true
	queryResult := func(pks []int64) *milvuspb.QueryResults {
		var ids []int64
		var vecData, nullableData []float32
		var validData []bool
		for _, pk := range pks {
			ids = append(ids, pk)
			vecData = append(vecData, genVec(pk+200)...)
			nullableData = append(nullableData, genVec(pk+300)...)
			validData = append(validData, true)
		}
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}},
				},
				{
					FieldName: "vector", FieldId: 101, Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vecData}}}},
				},
				{
					FieldName: "nullable_vec", FieldId: 102, Type: schemapb.DataType_FloatVector, ValidData: validData,
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: nullableData}}}},
				},
			},
		}
	}

	runUpsert := func(upsertData []*schemapb.FieldData, mockResult *milvuspb.QueryResults) *upsertTask {
		numRows := uint32(len(upsertData[0].GetScalars().GetLongData().GetData()))
		task := &upsertTask{
			ctx:    context.Background(),
			schema: schema,
			req:    &milvuspb.UpsertRequest{FieldsData: upsertData, NumRows: numRows},
			upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: upsertData,
					NumRows:    uint64(numRows),
					Version:    msgpb.InsertDataVersion_ColumnBased, // Required, otherwise NRows() returns 0
				},
			}},
			node: &Proxy{},
		}
		mock := mockey.Mock(retrieveByPKs).Return(mockResult, segcore.StorageCost{}, nil).Build()
		defer mock.UnPatch()
		err := task.queryPreExecute(context.Background())
		assert.NoError(t, err)
		return task
	}

	// Step 1a: Empty data, upsert pk1(partial) -> insert, nullable_vec=null
	task1a := runUpsert(createPartialCols([]int64{1}), queryResult(nil))
	assert.Empty(t, task1a.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{1}, task1a.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{1, 1, 1, 1}, task1a.insertFieldData[1].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []bool{false}, task1a.insertFieldData[2].ValidData)
	assert.Empty(t, task1a.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 1b: Empty data, upsert pk2(all) -> insert, nullable_vec=[102,...]
	task1b := runUpsert(createAllCols([]int64{2}), queryResult(nil))
	assert.Empty(t, task1b.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{2}, task1b.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{2, 2, 2, 2}, task1b.insertFieldData[1].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []float32{102, 102, 102, 102}, task1b.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 2a: pk1 exists, upsert pk1(all) -> update, nullable_vec=[101,...] (from upsert)
	task2a := runUpsert(createAllCols([]int64{1}), queryResult([]int64{1}))
	assert.Equal(t, []int64{1}, task2a.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{1}, task2a.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{1, 1, 1, 1}, task2a.insertFieldData[1].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []float32{101, 101, 101, 101}, task2a.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 2b: pk2 exists, upsert pk2(partial) -> update, nullable_vec=[302,...] (from existing)
	task2b := runUpsert(createPartialCols([]int64{2}), queryResult([]int64{2}))
	assert.Equal(t, []int64{2}, task2b.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{2}, task2b.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{2, 2, 2, 2}, task2b.insertFieldData[1].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []float32{302, 302, 302, 302}, task2b.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 3a: Empty data, upsert pk3(partial) -> insert, nullable_vec=null
	task3a := runUpsert(createPartialCols([]int64{3}), queryResult(nil))
	assert.Empty(t, task3a.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{3}, task3a.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []bool{false}, task3a.insertFieldData[2].ValidData)
	assert.Empty(t, task3a.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 3b: Empty data, upsert pk4(all) -> insert, nullable_vec=[104,...]
	task3b := runUpsert(createAllCols([]int64{4}), queryResult(nil))
	assert.Empty(t, task3b.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{4}, task3b.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{104, 104, 104, 104}, task3b.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 4a: pk3,pk4 exist, upsert pk3,pk4,pk5,pk6(all) -> pk3,pk4 update, pk5,pk6 insert
	task4a := runUpsert(createAllCols([]int64{3, 4, 5, 6}), queryResult([]int64{3, 4}))
	assert.Equal(t, []int64{3, 4}, task4a.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{3, 4, 5, 6}, task4a.insertFieldData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6}, task4a.insertFieldData[1].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []float32{103, 103, 103, 103, 104, 104, 104, 104, 105, 105, 105, 105, 106, 106, 106, 106}, task4a.insertFieldData[2].GetVectors().GetFloatVector().GetData())

	// Step 4b: pk3,pk4 exist, upsert pk3,pk4,pk5,pk6(partial) -> pk3,pk4 update (use existing), pk5,pk6 insert (null)
	task4b := runUpsert(createPartialCols([]int64{3, 4, 5, 6}), queryResult([]int64{3, 4}))
	assert.Equal(t, []int64{3, 4}, task4b.deletePKs.GetIntId().GetData())
	assert.Equal(t, []int64{3, 4, 5, 6}, task4b.insertFieldData[0].GetScalars().GetLongData().GetData())
	// Update rows pk3,pk4: nullable_vec from existing data (ValidData=true)
	// Insert rows pk5,pk6: nullable_vec generated by GenNullableFieldData (null, ValidData=false)
	// ValidData has 4 elements, FloatVector only contains data for ValidData=true rows
	assert.Equal(t, []bool{true, true, false, false}, task4b.insertFieldData[2].ValidData)
	assert.Equal(t, []float32{303, 303, 303, 303, 304, 304, 304, 304}, task4b.insertFieldData[2].GetVectors().GetFloatVector().GetData())
}

func TestUpsertTask_GenNullableFieldData(t *testing.T) {
	upsertIDSize := 5

	t.Run("scalar_types", func(t *testing.T) {
		testCases := []struct {
			name     string
			dataType schemapb.DataType
		}{
			{"Bool", schemapb.DataType_Bool},
			{"Int32", schemapb.DataType_Int32},
			{"Int64", schemapb.DataType_Int64},
			{"Float", schemapb.DataType_Float},
			{"Double", schemapb.DataType_Double},
			{"VarChar", schemapb.DataType_VarChar},
			{"JSON", schemapb.DataType_JSON},
			{"Array", schemapb.DataType_Array},
			{"Timestamptz", schemapb.DataType_Timestamptz},
			{"Geometry", schemapb.DataType_Geometry},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				field := &schemapb.FieldSchema{
					FieldID:  100,
					Name:     "test_field",
					DataType: tc.dataType,
					Nullable: true,
				}
				result, err := GenNullableFieldData(field, upsertIDSize)
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, field.FieldID, result.FieldId)
				assert.Equal(t, field.Name, result.FieldName)
				assert.Equal(t, tc.dataType, result.Type)
				assert.Equal(t, upsertIDSize, len(result.ValidData))
				// All ValidData should be false (null)
				for _, v := range result.ValidData {
					assert.False(t, v)
				}
			})
		}
	})

	t.Run("vector_types", func(t *testing.T) {
		testCases := []struct {
			name     string
			dataType schemapb.DataType
		}{
			{"FloatVector", schemapb.DataType_FloatVector},
			{"Float16Vector", schemapb.DataType_Float16Vector},
			{"BFloat16Vector", schemapb.DataType_BFloat16Vector},
			{"BinaryVector", schemapb.DataType_BinaryVector},
			{"Int8Vector", schemapb.DataType_Int8Vector},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				field := &schemapb.FieldSchema{
					FieldID:    100,
					Name:       "test_vector",
					DataType:   tc.dataType,
					Nullable:   true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
				}
				result, err := GenNullableFieldData(field, upsertIDSize)
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, field.FieldID, result.FieldId)
				assert.Equal(t, field.Name, result.FieldName)
				assert.Equal(t, tc.dataType, result.Type)
				assert.Equal(t, upsertIDSize, len(result.ValidData))
				// All ValidData should be false (null)
				for _, v := range result.ValidData {
					assert.False(t, v)
				}
				assert.NotNil(t, result.GetVectors())
			})
		}
	})

	t.Run("sparse_float_vector", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID:  100,
			Name:     "test_sparse",
			DataType: schemapb.DataType_SparseFloatVector,
			Nullable: true,
		}
		result, err := GenNullableFieldData(field, upsertIDSize)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, upsertIDSize, len(result.ValidData))
		assert.NotNil(t, result.GetVectors().GetSparseFloatVector())
	})

	t.Run("unsupported_type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			FieldID:  100,
			Name:     "test_unsupported",
			DataType: schemapb.DataType_None,
			Nullable: true,
		}
		result, err := GenNullableFieldData(field, upsertIDSize)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}
