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
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	grpcmixcoordclient "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/segcore"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	streamingmessage "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	boolFieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID, Name: "Bool", DataType: schemapb.DataType_Bool}
	int8FieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 1, Name: "Int8", DataType: schemapb.DataType_Int8}
	int16FieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 2, Name: "Int16", DataType: schemapb.DataType_Int16}
	int32FieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 3, Name: "Int32", DataType: schemapb.DataType_Int32}
	int64FieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 4, Name: "Int64", DataType: schemapb.DataType_Int64}
	floatFieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 5, Name: "Float", DataType: schemapb.DataType_Float}
	doubleFieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 6, Name: "Double", DataType: schemapb.DataType_Double}
	floatVectorFieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 7, Name: "FloatVector", DataType: schemapb.DataType_FloatVector}
	binaryVectorFieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 8, Name: "BinaryVector", DataType: schemapb.DataType_BinaryVector}
	varCharFieldSchema := &schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 9, Name: "VarChar", DataType: schemapb.DataType_VarChar}

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
	schema := mustNewSchemaInfo(collSchema)
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

		chMgr := channelmgr.NewMockChannelsMgr(t)
		chMgr.EXPECT().GetChannels(mock.Anything).Return(channels, nil)
		ut := upsertTask{
			baseTask: baseTask{metaCache: cache},
			ctx:      context.Background(),
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

	info := mustNewSchemaInfo(schema)
	collectionID := UniqueID(0)

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
		baseTask: baseTask{metaCache: &MetaCache{}},
		ctx:      context.Background(),
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
		oldRows := task.upsertMsg.InsertMsg.NumRows
		task.upsertMsg.InsertMsg.NumRows = 10000
		err = task.insertPreExecute(ctx)
		assert.Error(t, err)
		task.upsertMsg.InsertMsg.NumRows = oldRows
	}
}

func TestUpsertTaskForSchemaMismatch(t *testing.T) {
	mockCache := NewMockCache(t)
	ctx := context.Background()

	t.Run("schema ts mismatch", func(t *testing.T) {
		ut := upsertTask{
			baseTask: baseTask{metaCache: mockCache},
			ctx:      ctx,
			req: &milvuspb.UpsertRequest{
				CollectionName: "col-0",
				NumRows:        10,
			},
			schemaTimestamp: 99,
		}
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			UpdateTimestamp: 100,
			Schema: mustNewSchemaInfo(&schemapb.CollectionSchema{
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
		baseTask:  baseTask{metaCache: &MetaCache{}},
		Condition: NewTaskCondition(context.Background()),
		req: &milvuspb.UpsertRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
			),
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
	return mustNewSchemaInfo(schema)
}

type partialUpdateCASTestWAL struct {
	streaming.WALAccesser
	term            int64
	resolveErr      error
	resolveHook     func(string)
	resolveCalls    int
	appendCalls     int
	appendHook      func(context.Context, ...streamingmessage.MutableMessage) streaming.AppendResponses
	appended        []streamingmessage.MutableMessage
	appendedBatches [][]streamingmessage.MutableMessage
}

func newPartialUpdateCASTestWAL(t *testing.T, term int64) *partialUpdateCASTestWAL {
	t.Helper()

	w := &partialUpdateCASTestWAL{term: term}
	appendMock := mockey.Mock((*partialUpdateCASTestWAL).AppendMessages).To(
		func(w *partialUpdateCASTestWAL, ctx context.Context, msgs ...streamingmessage.MutableMessage) streaming.AppendResponses {
			w.appendCalls++
			w.appended = append([]streamingmessage.MutableMessage(nil), msgs...)
			w.appendedBatches = append(w.appendedBatches, append([]streamingmessage.MutableMessage(nil), msgs...))
			if w.appendHook != nil {
				return w.appendHook(ctx, msgs...)
			}
			responses := make([]streaming.AppendResponse, len(msgs))
			for idx := range responses {
				responses[idx].AppendResult = &streamingtypes.AppendResult{TimeTick: uint64(idx + 1)}
			}
			return streaming.AppendResponses{Responses: responses}
		},
	).Build()
	t.Cleanup(func() { appendMock.UnPatch() })
	return w
}

func (w *partialUpdateCASTestWAL) ResolvePChannelInfo(_ context.Context, vchannel string) (streamingtypes.PChannelInfo, error) {
	w.resolveCalls++
	if w.resolveHook != nil {
		w.resolveHook(vchannel)
	}
	if w.resolveErr != nil {
		return streamingtypes.PChannelInfo{}, w.resolveErr
	}
	term := w.term
	if term == 0 {
		term = 9
	}
	return streamingtypes.PChannelInfo{
		Name:       funcutil.ToPhysicalChannel(vchannel),
		Term:       term,
		AccessMode: streamingtypes.AccessModeRW,
	}, nil
}

type partialUpdateCASExpected struct{}

func expectedPartialUpdateCASGroups(t *testing.T, ids *schemapb.IDs, vchannels []string) map[string]partialUpdateCASExpected {
	t.Helper()

	channelIndexes, err := typeutil.HashPK2Channels(ids, vchannels)
	require.NoError(t, err)
	require.Equal(t, typeutil.GetSizeOfIDs(ids), len(channelIndexes))

	groups := make(map[string]partialUpdateCASExpected, len(vchannels))
	for _, channelIndex := range channelIndexes {
		require.Less(t, int(channelIndex), len(vchannels))
		vchannel := vchannels[channelIndex]
		groups[vchannel] = partialUpdateCASExpected{}
	}
	return groups
}

func requireAppendedPartialUpdateCASGroups(
	t *testing.T,
	msgs []streamingmessage.MutableMessage,
	expected map[string]partialUpdateCASExpected,
	readTS uint64,
	term int64,
) {
	t.Helper()

	seen := make(map[string]struct{}, len(expected))
	for _, msg := range msgs {
		meta, err := streamingmessage.ExtractPartialUpdateCAS(msg)
		require.NoError(t, err)
		if meta == nil {
			continue
		}
		_, ok := expected[msg.VChannel()]
		require.True(t, ok, "unexpected partial update CAS for vchannel %s", msg.VChannel())
		require.Equal(t, streamingmessage.MessageTypeInsert, msg.MessageType())
		require.Equal(t, readTS, meta.GetReadTs())
		require.EqualValues(t, term, meta.GetObservedPchannelTerm())
		seen[msg.VChannel()] = struct{}{}
	}
	require.Len(t, seen, len(expected))
}

func requireFirstPartialUpdateCAS(t *testing.T, msgs []streamingmessage.MutableMessage) *messagespb.PartialUpdateCAS {
	t.Helper()

	for _, msg := range msgs {
		meta, err := streamingmessage.ExtractPartialUpdateCAS(msg)
		require.NoError(t, err)
		if meta != nil {
			return meta
		}
	}
	require.Fail(t, "partial update CAS metadata not found")
	return nil
}

func partialUpdateCASIDs(data []int64) *schemapb.IDs {
	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: data},
		},
	}
}

func partialUpdateCASPKFieldData(data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: "id",
		FieldId:   100,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: data},
				},
			},
		},
	}
}

func partialUpdateCASStringIDs(data []string) *schemapb.IDs {
	return &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{Data: data},
		},
	}
}

func partialUpdateCASStringPKFieldData(data []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: "id",
		FieldId:   100,
		Type:      schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: data},
				},
			},
		},
	}
}

var partialUpdateCASTestVChannels = []string{
	"by-dev-rootcoord-dml_0_1001v0",
	"by-dev-rootcoord-dml_1_1001v1",
}

func setPartialUpdateCASTestChannels(task *upsertTask, vchannels []string) {
	task.chMgr = channelmgr.NewChannelsMgr(func(collectionID typeutil.UniqueID) (channelmgr.ChannelInfo, error) {
		vchans := make([]string, 0, len(vchannels))
		pchans := make([]string, 0, len(vchannels))
		for _, vchannel := range vchannels {
			vchans = append(vchans, vchannel)
			pchans = append(pchans, funcutil.ToPhysicalChannel(vchannel))
		}
		return channelmgr.ChannelInfo{VChans: vchans, PChans: pchans}, nil
	})
	proxy := task.node.(*Proxy)
	if proxy.tsoAllocator == nil {
		proxy.tsoAllocator = &timestampAllocator{
			tso:    newMockTimestampAllocatorInterface(),
			peerID: paramtable.GetNodeID(),
		}
	}
}

func preparePartialUpdateCASTestGroups(t *testing.T, task *upsertTask) {
	t.Helper()
	require.NoError(t, task.preparePartialUpdateCASGroups(context.Background()))
}

func buildPartialUpdateCASTestMessages(
	t *testing.T,
	collectionID int64,
	vchannels []string,
	insertPKs []int64,
	deletePKs []int64,
	casGroups map[string]*messagespb.PartialUpdateCAS,
) ([]streamingmessage.MutableMessage, []streamingmessage.MutableMessage) {
	insertGroups := groupPartialUpdateCASIntPKs(t, insertPKs, vchannels)
	insertMsgs := make([]streamingmessage.MutableMessage, 0, len(insertGroups))
	for vchannel, pks := range insertGroups {
		builder := streamingmessage.NewInsertMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&streamingmessage.InsertMessageHeader{
				CollectionId: collectionID,
				Partitions: []*streamingmessage.PartitionSegmentAssignment{{
					PartitionId: 100,
					Rows:        uint64(len(pks)),
				}},
			}).
			WithBody(&msgpb.InsertRequest{
				CollectionID: collectionID,
				NumRows:      uint64(len(pks)),
				FieldsData:   []*schemapb.FieldData{partialUpdateCASPKFieldData(pks)},
			})
		if casGroups != nil {
			meta, ok := casGroups[vchannel]
			require.True(t, ok)
			require.NoError(t, builder.AddPartialUpdateCAS(meta))
		}
		insertMsg, err := builder.BuildMutable()
		require.NoError(t, err)
		insertMsgs = append(insertMsgs, insertMsg)
	}

	deleteGroups := groupPartialUpdateCASIntPKs(t, deletePKs, vchannels)
	deleteMsgs := make([]streamingmessage.MutableMessage, 0, len(deleteGroups))
	for vchannel, pks := range deleteGroups {
		deleteMsg, err := streamingmessage.NewDeleteMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&streamingmessage.DeleteMessageHeader{
				CollectionId: collectionID,
				Rows:         uint64(len(pks)),
			}).
			WithBody(&msgpb.DeleteRequest{
				CollectionID: collectionID,
				NumRows:      int64(len(pks)),
				PrimaryKeys:  partialUpdateCASIDs(pks),
			}).
			BuildMutable()
		require.NoError(t, err)
		deleteMsgs = append(deleteMsgs, deleteMsg)
	}
	return insertMsgs, deleteMsgs
}

func buildPartialUpdateCASStringTestMessages(
	t *testing.T,
	collectionID int64,
	vchannels []string,
	insertPKs []string,
	deletePKs []string,
	casGroups map[string]*messagespb.PartialUpdateCAS,
) ([]streamingmessage.MutableMessage, []streamingmessage.MutableMessage) {
	insertGroups := groupPartialUpdateCASStringPKs(t, insertPKs, vchannels)
	insertMsgs := make([]streamingmessage.MutableMessage, 0, len(insertGroups))
	for vchannel, pks := range insertGroups {
		builder := streamingmessage.NewInsertMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&streamingmessage.InsertMessageHeader{
				CollectionId: collectionID,
				Partitions: []*streamingmessage.PartitionSegmentAssignment{{
					PartitionId: 100,
					Rows:        uint64(len(pks)),
				}},
			}).
			WithBody(&msgpb.InsertRequest{
				CollectionID: collectionID,
				NumRows:      uint64(len(pks)),
				FieldsData:   []*schemapb.FieldData{partialUpdateCASStringPKFieldData(pks)},
			})
		if casGroups != nil {
			meta, ok := casGroups[vchannel]
			require.True(t, ok)
			require.NoError(t, builder.AddPartialUpdateCAS(meta))
		}
		insertMsg, err := builder.BuildMutable()
		require.NoError(t, err)
		insertMsgs = append(insertMsgs, insertMsg)
	}

	deleteGroups := groupPartialUpdateCASStringPKs(t, deletePKs, vchannels)
	deleteMsgs := make([]streamingmessage.MutableMessage, 0, len(deleteGroups))
	for vchannel, pks := range deleteGroups {
		deleteMsg, err := streamingmessage.NewDeleteMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&streamingmessage.DeleteMessageHeader{
				CollectionId: collectionID,
				Rows:         uint64(len(pks)),
			}).
			WithBody(&msgpb.DeleteRequest{
				CollectionID: collectionID,
				NumRows:      int64(len(pks)),
				PrimaryKeys:  partialUpdateCASStringIDs(pks),
			}).
			BuildMutable()
		require.NoError(t, err)
		deleteMsgs = append(deleteMsgs, deleteMsg)
	}
	return insertMsgs, deleteMsgs
}

func groupPartialUpdateCASIntPKs(t *testing.T, pks []int64, vchannels []string) map[string][]int64 {
	t.Helper()
	groups := make(map[string][]int64)
	if len(pks) == 0 {
		return groups
	}
	indexes, err := typeutil.HashPK2Channels(partialUpdateCASIDs(pks), vchannels)
	require.NoError(t, err)
	for idx, channelIndex := range indexes {
		vchannel := vchannels[channelIndex]
		groups[vchannel] = append(groups[vchannel], pks[idx])
	}
	return groups
}

func groupPartialUpdateCASStringPKs(t *testing.T, pks []string, vchannels []string) map[string][]string {
	t.Helper()
	groups := make(map[string][]string)
	if len(pks) == 0 {
		return groups
	}
	indexes, err := typeutil.HashPK2Channels(partialUpdateCASStringIDs(pks), vchannels)
	require.NoError(t, err)
	for idx, channelIndex := range indexes {
		vchannel := vchannels[channelIndex]
		groups[vchannel] = append(groups[vchannel], pks[idx])
	}
	return groups
}

func partialUpdateCASTestTask(
	t *testing.T,
	partial bool,
	originalPKs []int64,
	finalInsertPKs []int64,
	deletePKs []int64,
) (*upsertTask, []streamingmessage.MutableMessage, []streamingmessage.MutableMessage) {
	task := createTestUpdateTask()
	task.SetTs(12345)
	task.req.PartialUpdate = partial
	task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{
		FieldName: "name",
		Op:        schemapb.FieldPartialUpdateOp_ARRAY_APPEND,
	}}
	task.req.NumRows = uint32(len(originalPKs))
	task.req.FieldsData[0] = partialUpdateCASPKFieldData(originalPKs)
	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	task.result = &milvuspb.MutationResult{
		IDs: partialUpdateCASIDs(finalInsertPKs),
	}
	task.deletePKs = partialUpdateCASIDs(deletePKs)
	task.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{}},
		DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{}},
	}
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	insertMsgs, deleteMsgs := buildPartialUpdateCASTestMessages(t, task.collectionID, partialUpdateCASTestVChannels, finalInsertPKs, deletePKs, nil)
	return task, insertMsgs, deleteMsgs
}

func partialUpdateCASRealPackTestTask(
	t *testing.T,
	originalPKs []int64,
	finalInsertPKs []int64,
	deletePKs []int64,
) *upsertTask {
	require.Len(t, finalInsertPKs, len(originalPKs))
	rowIDs := make([]int64, len(finalInsertPKs))
	timestamps := make([]uint64, len(finalInsertPKs))
	for i := range finalInsertPKs {
		rowIDs[i] = int64(101 + i)
	}

	task := createTestUpdateTask()
	task.SetTs(12345)
	for i := range timestamps {
		timestamps[i] = task.BeginTs()
	}
	task.req.PartialUpdate = true
	task.req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
		commonpbutil.WithMsgID(10000),
		commonpbutil.WithTimeStamp(task.BeginTs()),
	)
	task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{
		FieldName: "name",
		Op:        schemapb.FieldPartialUpdateOp_ARRAY_APPEND,
	}}
	task.req.NumRows = uint32(len(originalPKs))
	task.req.FieldsData[0] = partialUpdateCASPKFieldData(originalPKs)
	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	task.result = &milvuspb.MutationResult{
		IDs: partialUpdateCASIDs(finalInsertPKs),
	}
	task.deletePKs = partialUpdateCASIDs(deletePKs)
	task.idAllocator = &allocator.IDAllocator{}
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels[:1])
	task.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx:            context.Background(),
				BeginTimestamp: task.BeginTs(),
				EndTimestamp:   task.BeginTs(),
			},
			InsertRequest: &msgpb.InsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					commonpbutil.WithTimeStamp(task.BeginTs()),
				),
				CollectionName: task.req.GetCollectionName(),
				CollectionID:   task.collectionID,
				PartitionName:  task.req.GetPartitionName(),
				FieldsData:     []*schemapb.FieldData{partialUpdateCASPKFieldData(finalInsertPKs)},
				NumRows:        uint64(len(finalInsertPKs)),
				Version:        msgpb.InsertDataVersion_ColumnBased,
				DbName:         task.req.GetDbName(),
				RowIDs:         rowIDs,
				Timestamps:     timestamps,
			},
		},
		DeleteMsg: &msgstream.DeleteMsg{
			DeleteRequest: &msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithTimeStamp(task.BeginTs()),
				),
				DbName:         task.req.GetDbName(),
				CollectionName: task.req.GetCollectionName(),
				CollectionID:   task.collectionID,
				PrimaryKeys:    partialUpdateCASIDs(deletePKs),
				NumRows:        int64(len(deletePKs)),
				PartitionName:  task.req.GetPartitionName(),
			},
		},
	}
	return task
}

func TestRepackInsertDataForStreamingServiceCASMetadata(t *testing.T) {
	mockCache := &MetaCache{}
	partitionPatch := mockey.Mock((*MetaCache).GetPartitionID).Return(int64(200), nil).Build()
	defer partitionPatch.UnPatch()

	newInput := func() (*upsertTask, string, map[string]*messagespb.PartialUpdateCAS) {
		task := partialUpdateCASRealPackTestTask(t, []int64{10}, []int64{10}, nil)
		vchannel := partialUpdateCASTestVChannels[0]
		return task, vchannel, map[string]*messagespb.PartialUpdateCAS{
			vchannel: {
				ReadTs:               100,
				ObservedPchannelTerm: 1,
			},
		}
	}

	task, vchannel, groups := newInput()
	msgs, err := repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.True(t, streamingmessage.HasPartialUpdateCAS(msgs[0]))

	task, vchannel, _ = newInput()
	_, err = repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		map[string]*messagespb.PartialUpdateCAS{},
	)
	require.Error(t, err)

	task, vchannel, _ = newInput()
	_, err = repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		map[string]*messagespb.PartialUpdateCAS{vchannel: nil},
	)
	require.Error(t, err)

	task, vchannel, groups = newInput()
	groups[vchannel].ReadTs = 0
	_, err = repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		groups,
	)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestRepackInsertDataForStreamingServiceSplitsOversizedCASMessage(t *testing.T) {
	mockCache := &MetaCache{}
	partitionPatch := mockey.Mock((*MetaCache).GetPartitionID).Return(int64(200), nil).Build()
	defer partitionPatch.UnPatch()

	pks := []int64{10, 20}
	task := partialUpdateCASRealPackTestTask(t, pks, pks, nil)
	vchannel := partialUpdateCASTestVChannels[0]
	groups := map[string]*messagespb.PartialUpdateCAS{
		vchannel: {
			ReadTs:               100,
			ObservedPchannelTerm: 1,
		},
	}

	unsplit, err := repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, unsplit, 1)
	maxMessageSize := unsplit[0].EstimateSize() - 1
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(maxMessageSize)))
	t.Cleanup(func() { Params.Reset(Params.PulsarCfg.MaxMessageSize.Key) })

	msgs, err := repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	rowIDs := make([]int64, 0, len(pks))
	for _, msg := range msgs {
		require.LessOrEqual(t, msg.EstimateSize(), maxMessageSize)
		require.True(t, streamingmessage.HasPartialUpdateCAS(msg))
		meta, err := streamingmessage.ExtractPartialUpdateCAS(msg)
		require.NoError(t, err)
		require.True(t, proto.Equal(groups[vchannel], meta))
		body := streamingmessage.MustAsMutableInsertMessageV1(msg).MustBody()
		rowIDs = append(rowIDs, body.GetRowIDs()...)
	}
	require.Equal(t, []int64{101, 102}, rowIDs)
}

func TestRepackInsertDataForStreamingServiceRejectsOversizedSingleRow(t *testing.T) {
	mockCache := &MetaCache{}
	partitionPatch := mockey.Mock((*MetaCache).GetPartitionID).Return(int64(200), nil).Build()
	defer partitionPatch.UnPatch()

	task := partialUpdateCASRealPackTestTask(t, []int64{10}, []int64{10}, nil)
	vchannel := partialUpdateCASTestVChannels[0]
	groups := map[string]*messagespb.PartialUpdateCAS{
		vchannel: {
			ReadTs:               100,
			ObservedPchannelTerm: 1,
		},
	}

	baseline, err := repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, baseline, 1)
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(baseline[0].EstimateSize()-1)))
	t.Cleanup(func() { Params.Reset(Params.PulsarCfg.MaxMessageSize.Key) })

	_, err = repackInsertDataForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		nil,
		1,
		groups,
	)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func TestRepackInsertDataByPartitionForStreamingServicePropagatesPackingError(t *testing.T) {
	task := partialUpdateCASRealPackTestTask(t, []int64{10}, []int64{10}, nil)
	task.upsertMsg.InsertMsg.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{},
					},
				},
			},
		},
	}

	_, err := repackInsertDataByPartitionForStreamingService(
		context.Background(),
		200,
		"_default",
		[]int{0},
		partialUpdateCASTestVChannels[0],
		task.upsertMsg.InsertMsg,
		nil,
		1,
		nil,
		streamingmessage.WALNamePulsar,
	)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestRepackInsertDataByPartitionForStreamingServicePreservesEntityPackingOrder(t *testing.T) {
	pks := []int64{10, 20}
	task := partialUpdateCASRealPackTestTask(t, pks, pks, nil)
	task.upsertMsg.InsertMsg.HashValues = []uint32{0, 0}
	task.upsertMsg.InsertMsg.FieldsData = []*schemapb.FieldData{
		{
			FieldId: 100,
			Type:    schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{
							strings.Repeat("a", 4096),
							strings.Repeat("b", 4096),
						}},
					},
				},
			},
		},
	}
	meta := &messagespb.PartialUpdateCAS{
		ReadTs:               100,
		ObservedPchannelTerm: 1,
	}

	baseline, err := repackInsertDataByPartitionForStreamingService(
		context.Background(),
		200,
		"_default",
		[]int{0},
		partialUpdateCASTestVChannels[0],
		task.upsertMsg.InsertMsg,
		nil,
		1,
		meta,
		streamingmessage.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, baseline, 1)
	maxMessageSize := baseline[0].EstimateSize()
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(maxMessageSize)))
	t.Cleanup(func() { Params.Reset(Params.PulsarCfg.MaxMessageSize.Key) })

	entityPacked, err := channelmgr.GenInsertMsgsByPartition(
		context.Background(),
		0,
		200,
		"_default",
		[]int{0, 1},
		partialUpdateCASTestVChannels[0],
		task.upsertMsg.InsertMsg,
		streamingmessage.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, entityPacked, 2)

	msgs, err := repackInsertDataByPartitionForStreamingService(
		context.Background(),
		200,
		"_default",
		[]int{0, 1},
		partialUpdateCASTestVChannels[0],
		task.upsertMsg.InsertMsg,
		nil,
		1,
		meta,
		streamingmessage.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	rowIDs := make([]int64, 0, len(pks))
	for _, msg := range msgs {
		require.LessOrEqual(t, msg.EstimateSize(), maxMessageSize)
		body := streamingmessage.MustAsMutableInsertMessageV1(msg).MustBody()
		rowIDs = append(rowIDs, body.GetRowIDs()...)
	}
	require.Equal(t, []int64{101, 102}, rowIDs)
}

func TestRepackInsertDataWithPartitionKeyForStreamingServiceCASMetadata(t *testing.T) {
	mockCache := NewMockCache(t)
	mockCache.EXPECT().
		GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"_default_0": 200}, nil).
		Maybe()
	mockCache.EXPECT().
		GetPartitionID(mock.Anything, mock.Anything, mock.Anything, "_default_0").
		Return(int64(200), nil).
		Maybe()

	newInput := func() (*upsertTask, string, map[string]*messagespb.PartialUpdateCAS) {
		task := partialUpdateCASRealPackTestTask(t, []int64{10}, []int64{10}, nil)
		task.partitionKeys = partialUpdateCASPKFieldData([]int64{10})
		vchannel := partialUpdateCASTestVChannels[0]
		return task, vchannel, map[string]*messagespb.PartialUpdateCAS{
			vchannel: {
				ReadTs:               100,
				ObservedPchannelTerm: 1,
			},
		}
	}

	task, vchannel, groups := newInput()
	msgs, err := repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		task.partitionKeys,
		nil,
		task.schema.CollectionSchema,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.True(t, streamingmessage.HasPartialUpdateCAS(msgs[0]))

	task, vchannel, _ = newInput()
	_, err = repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		task.partitionKeys,
		nil,
		task.schema.CollectionSchema,
		1,
		map[string]*messagespb.PartialUpdateCAS{},
	)
	require.Error(t, err)

	task, vchannel, _ = newInput()
	_, err = repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		task.partitionKeys,
		nil,
		task.schema.CollectionSchema,
		1,
		map[string]*messagespb.PartialUpdateCAS{vchannel: nil},
	)
	require.Error(t, err)

	task, vchannel, groups = newInput()
	groups[vchannel].ReadTs = 0
	_, err = repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		task.partitionKeys,
		nil,
		task.schema.CollectionSchema,
		1,
		groups,
	)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestRepackInsertDataWithPartitionKeyForStreamingServiceSplitsOversizedCASMessage(t *testing.T) {
	mockCache := NewMockCache(t)
	mockCache.EXPECT().
		GetPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]int64{"_default_0": 200}, nil).
		Maybe()
	mockCache.EXPECT().
		GetPartitionID(mock.Anything, mock.Anything, mock.Anything, "_default_0").
		Return(int64(200), nil).
		Maybe()

	pks := []int64{10, 20}
	task := partialUpdateCASRealPackTestTask(t, pks, pks, nil)
	task.partitionKeys = partialUpdateCASPKFieldData(pks)
	vchannel := partialUpdateCASTestVChannels[0]
	groups := map[string]*messagespb.PartialUpdateCAS{
		vchannel: {
			ReadTs:               100,
			ObservedPchannelTerm: 1,
		},
	}

	unsplit, err := repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		task.partitionKeys,
		nil,
		task.schema.CollectionSchema,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, unsplit, 1)
	maxMessageSize := unsplit[0].EstimateSize() - 1
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(maxMessageSize)))
	t.Cleanup(func() { Params.Reset(Params.PulsarCfg.MaxMessageSize.Key) })

	msgs, err := repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		mockCache,
		[]string{vchannel},
		task.upsertMsg.InsertMsg,
		task.result,
		task.partitionKeys,
		nil,
		task.schema.CollectionSchema,
		1,
		groups,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	rowIDs := make([]int64, 0, len(pks))
	for _, msg := range msgs {
		require.LessOrEqual(t, msg.EstimateSize(), maxMessageSize)
		require.True(t, streamingmessage.HasPartialUpdateCAS(msg))
		meta, err := streamingmessage.ExtractPartialUpdateCAS(msg)
		require.NoError(t, err)
		require.True(t, proto.Equal(groups[vchannel], meta))
		body := streamingmessage.MustAsMutableInsertMessageV1(msg).MustBody()
		rowIDs = append(rowIDs, body.GetRowIDs()...)
	}
	require.Equal(t, []int64{101, 102}, rowIDs)
}

func TestInsertTaskExecuteSelectsPartitionRouting(t *testing.T) {
	for _, partitionKey := range []bool{false, true} {
		t.Run(map[bool]string{false: "primary key", true: "partition key"}[partitionKey], func(t *testing.T) {
			collectionPatch := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
			defer collectionPatch.UnPatch()

			primaryPatch := mockey.Mock(repackInsertDataForStreamingService).
				Return([]streamingmessage.MutableMessage{}, nil).
				Build()
			defer primaryPatch.UnPatch()
			partitionPatch := mockey.Mock(repackInsertDataWithPartitionKeyForStreamingService).
				Return([]streamingmessage.MutableMessage{}, nil).
				Build()
			defer partitionPatch.UnPatch()

			fakeWAL := newPartialUpdateCASTestWAL(t, 1)
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(fakeWAL)
			t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })

			task := &insertTask{
				baseTask: baseTask{metaCache: &MetaCache{}},
				ctx:      context.Background(),
				insertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
					Base:           &commonpb.MsgBase{},
					DbName:         "test_db",
					CollectionName: "test_collection",
				}},
				result: &milvuspb.MutationResult{},
				chMgr: channelmgr.NewChannelsMgr(func(typeutil.UniqueID) (channelmgr.ChannelInfo, error) {
					return channelmgr.ChannelInfo{
						VChans: []string{partialUpdateCASTestVChannels[0]},
						PChans: []string{funcutil.ToPhysicalChannel(partialUpdateCASTestVChannels[0])},
					}, nil
				}),
				schema: &schemapb.CollectionSchema{},
			}
			if partitionKey {
				task.partitionKeys = partialUpdateCASPKFieldData([]int64{10})
			}

			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, 1, fakeWAL.appendCalls)
		})
	}
}

func TestPackInsertMessageUsesPartitionKeyRouting(t *testing.T) {
	task := partialUpdateCASRealPackTestTask(t, []int64{10}, []int64{10}, nil)
	task.partitionKeys = partialUpdateCASPKFieldData([]int64{10})
	collectionPatch := mockey.Mock((*MetaCache).GetCollectionID).Return(task.collectionID, nil).Build()
	defer collectionPatch.UnPatch()
	partitionPatch := mockey.Mock(repackInsertDataWithPartitionKeyForStreamingService).
		Return([]streamingmessage.MutableMessage{}, nil).
		Build()
	defer partitionPatch.UnPatch()

	msgs, err := task.packInsertMessage(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

func TestAppendUpsertAttemptMapsSchemaVersionMismatch(t *testing.T) {
	task, insertMsgs, _ := partialUpdateCASTestTask(t, false, []int64{10}, []int64{10}, nil)
	fakeWAL := newPartialUpdateCASTestWAL(t, 1)
	fakeWAL.appendHook = func(context.Context, ...streamingmessage.MutableMessage) streaming.AppendResponses {
		return streaming.AppendResponses{Responses: []streaming.AppendResponse{{
			Error: streamingstatus.NewSchemaVersionMismatch("schema changed"),
		}}}
	}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
	insertPatch := mockey.Mock((*upsertTask).packInsertMessage).Return(insertMsgs, nil).Build()
	defer insertPatch.UnPatch()
	deletePatch := mockey.Mock((*upsertTask).packDeleteMessage).Return(nil, nil).Build()
	defer deletePatch.UnPatch()

	err := task.appendUpsertAttempt(context.Background(), nil)
	require.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
}

func TestAttachPartialUpdateCASRejectsMissingMarker(t *testing.T) {
	task, insertMsgs, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, nil)
	require.NotEmpty(t, insertMsgs)
	task.partialUpdateCASGroups = map[string]*messagespb.PartialUpdateCAS{
		insertMsgs[0].VChannel(): {
			ReadTs:               100,
			ObservedPchannelTerm: 1,
		},
	}
	err := task.attachPartialUpdateCAS(insertMsgs[:1])
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Contains(t, err.Error(), "missing CAS metadata")
}

func partialUpdateCASStringTestTask(
	t *testing.T,
	originalPKs []string,
	finalInsertPKs []string,
	deletePKs []string,
) (*upsertTask, []streamingmessage.MutableMessage, []streamingmessage.MutableMessage) {
	task := createTestUpdateTask()
	task.SetTs(12345)
	task.schema.CollectionSchema.Fields[0].DataType = schemapb.DataType_VarChar
	task.req.PartialUpdate = true
	task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{
		FieldName: "name",
		Op:        schemapb.FieldPartialUpdateOp_ARRAY_APPEND,
	}}
	task.req.NumRows = uint32(len(originalPKs))
	task.req.FieldsData[0] = partialUpdateCASStringPKFieldData(originalPKs)
	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	task.result = &milvuspb.MutationResult{
		IDs: partialUpdateCASStringIDs(finalInsertPKs),
	}
	task.deletePKs = partialUpdateCASStringIDs(deletePKs)
	task.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{}},
		DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{}},
	}
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	insertMsgs, deleteMsgs := buildPartialUpdateCASStringTestMessages(t, task.collectionID, partialUpdateCASTestVChannels, finalInsertPKs, deletePKs, nil)
	return task, insertMsgs, deleteMsgs
}

func TestPartialUpdateAppendAcceptsBuilderCASMetadata(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)
	insertMsgs, deleteMsgs := buildPartialUpdateCASTestMessages(
		t,
		task.collectionID,
		partialUpdateCASTestVChannels,
		[]int64{20, 10, 30},
		[]int64{20},
		task.partialUpdateCASGroups,
	)

	m := mockey.Mock((*upsertTask).packInsertMessage).Return(insertMsgs, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).packDeleteMessage).Return(deleteMsgs, nil).Build()
	defer m.UnPatch()

	err := task.Execute(context.Background())
	require.NoError(t, err)
	expected := expectedPartialUpdateCASGroups(t, partialUpdateCASIDs([]int64{10, 20, 30}), partialUpdateCASTestVChannels)
	require.Equal(t, len(expected), fakeWAL.resolveCalls)
	require.Equal(t, 1, fakeWAL.appendCalls)
	require.Len(t, fakeWAL.appended, len(insertMsgs)+len(deleteMsgs))
	requireAppendedPartialUpdateCASGroups(t, fakeWAL.appended, expected, task.partialUpdateReadTs, 9)
}

func TestPartialUpdateAppendPacksMessagesAndAttachesCASMetadata(t *testing.T) {
	task := partialUpdateCASRealPackTestTask(t, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)

	m := mockey.Mock((*MetaCache).GetCollectionID).Return(task.collectionID, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*MetaCache).GetPartitionID).Return(UniqueID(100), nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*allocator.IDAllocator).Alloc).Return(UniqueID(1000), UniqueID(1001), nil).Build()
	defer m.UnPatch()

	err := task.Execute(context.Background())
	require.NoError(t, err)
	expected := expectedPartialUpdateCASGroups(t, partialUpdateCASIDs([]int64{10, 20, 30}), partialUpdateCASTestVChannels[:1])
	require.Equal(t, 1, fakeWAL.resolveCalls)
	require.Equal(t, 1, fakeWAL.appendCalls)
	require.Len(t, fakeWAL.appended, 2)
	requireAppendedPartialUpdateCASGroups(t, fakeWAL.appended, expected, task.partialUpdateReadTs, 9)
}

func TestPartialUpdateRetriesAfterCASConflict(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	task.req.FieldOps[0].Op = schemapb.FieldPartialUpdateOp_REPLACE
	task.node.(*Proxy).tsoAllocator = &timestampAllocator{
		tso:    newMockTimestampAllocatorInterface(),
		peerID: paramtable.GetNodeID(),
	}
	initialTs := task.BeginTs()
	initialID := task.ID()

	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	fakeWAL.appendHook = func(ctx context.Context, msgs ...streamingmessage.MutableMessage) streaming.AppendResponses {
		resp := streamingtypes.NewAppendResponseN(len(msgs))
		if fakeWAL.appendCalls == 1 {
			fakeWAL.term = 10
			resp.FillAllError(streamingstatus.NewPartialUpdateRetryable("conflict"))
			return resp
		}
		resp.FillAllResponse(streaming.AppendResponse{
			AppendResult: &streamingtypes.AppendResult{TimeTick: 200},
		})
		return resp
	}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)
	firstAttemptReadTS := task.partialUpdateReadTs

	m := mockey.Mock((*upsertTask).packInsertMessage).To(
		func(task *upsertTask, ctx context.Context, ez *streamingmessage.CipherConfig) ([]streamingmessage.MutableMessage, error) {
			insertMsgs, _ := buildPartialUpdateCASTestMessages(
				t,
				task.collectionID,
				partialUpdateCASTestVChannels,
				[]int64{20, 10, 30},
				[]int64{20},
				task.partialUpdateCASGroups,
			)
			return insertMsgs, nil
		},
	).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).packDeleteMessage).To(
		func(task *upsertTask, ctx context.Context, ez *streamingmessage.CipherConfig) ([]streamingmessage.MutableMessage, error) {
			_, deleteMsgs := buildPartialUpdateCASTestMessages(
				t,
				task.collectionID,
				partialUpdateCASTestVChannels,
				[]int64{20, 10, 30},
				[]int64{20},
				nil,
			)
			return deleteMsgs, nil
		},
	).Build()
	defer m.UnPatch()

	requeryCalls := 0
	m = mockey.Mock((*upsertTask).queryPreExecute).To(func(task *upsertTask, ctx context.Context) error {
		requeryCalls++
		require.Equal(t, initialTs, task.BeginTs())
		require.Equal(t, initialID, task.ID())
		for _, meta := range task.partialUpdateCASGroups {
			require.Greater(t, meta.GetReadTs(), initialTs)
		}
		return nil
	}).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()
	defer m.UnPatch()

	err := task.Execute(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, fakeWAL.appendCalls)
	require.Equal(t, 1, requeryCalls)
	require.Len(t, fakeWAL.appendedBatches, 2)

	firstCAS := requireFirstPartialUpdateCAS(t, fakeWAL.appendedBatches[0])
	secondCAS := requireFirstPartialUpdateCAS(t, fakeWAL.appendedBatches[1])
	require.Equal(t, firstAttemptReadTS, firstCAS.GetReadTs())
	require.Greater(t, secondCAS.GetReadTs(), firstCAS.GetReadTs())
	require.EqualValues(t, 9, firstCAS.GetObservedPchannelTerm())
	require.EqualValues(t, 10, secondCAS.GetObservedPchannelTerm())
	require.Equal(t, initialTs, task.BeginTs())
	require.Equal(t, initialID, task.ID())
	require.Equal(t, uint64(200), task.result.GetTimestamp())
}

func TestPartialUpdateCASConflictProjection(t *testing.T) {
	for _, tc := range []struct {
		name             string
		canRetryConflict bool
		expectedError    error
		expectedRetry    bool
	}{
		{
			name:          "relative update",
			expectedError: merr.ErrCollectionPartialUpdateConflict,
		},
		{
			name:             "replace retry exhausted",
			canRetryConflict: true,
			expectedError:    merr.ErrServiceUnavailable,
			expectedRetry:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, []int64{10})
			if tc.canRetryConflict {
				task.req.FieldOps[0].Op = schemapb.FieldPartialUpdateOp_REPLACE
			}

			casErr := streamingstatus.NewPartialUpdateRetryable("conflict")
			appendPatch := mockey.Mock((*upsertTask).appendUpsertAttempt).
				Return(casErr).Build()
			defer appendPatch.UnPatch()
			preparePatch := mockey.Mock((*upsertTask).preparePartialUpdateRetryAttempt).Return(nil).Build()
			defer preparePatch.UnPatch()

			err := task.executePartialUpdateWithCASRetry(context.Background(), nil)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.expectedError)
			require.ErrorIs(t, err, casErr)
			resultStatus := merr.Status(err)
			require.Equal(t, merr.Code(tc.expectedError), resultStatus.GetCode())
			require.Equal(t, tc.expectedRetry, resultStatus.GetRetriable())
		})
	}
}

func TestPartialUpdateCASRetryRequiresOriginalFields(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, []int64{10})
	task.req.FieldOps[0].Op = schemapb.FieldPartialUpdateOp_REPLACE
	task.partialUpdateOriginalFields = nil

	err := task.executePartialUpdateWithCASRetry(context.Background(), nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestPartialUpdateCASRetryStopsWhenAttemptPreparationFails(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, []int64{10})
	task.req.FieldOps[0].Op = schemapb.FieldPartialUpdateOp_REPLACE
	casErr := streamingstatus.NewPartialUpdateRetryable("conflict")
	prepareErr := merr.WrapErrServiceInternalMsg("prepare retry attempt failed")

	appendCalls := 0
	appendPatch := mockey.Mock((*upsertTask).appendUpsertAttempt).To(
		func(task *upsertTask, ctx context.Context, ez *streamingmessage.CipherConfig) error {
			appendCalls++
			return casErr
		},
	).Build()
	defer appendPatch.UnPatch()
	preparePatch := mockey.Mock((*upsertTask).preparePartialUpdateRetryAttempt).Return(prepareErr).Build()
	defer preparePatch.UnPatch()

	err := task.executePartialUpdateWithCASRetry(context.Background(), nil)
	require.ErrorIs(t, err, prepareErr)
	require.Equal(t, 1, appendCalls)
}

func TestUnwrapPartialUpdateAppendErrorRejectsMixedOutcome(t *testing.T) {
	casErr := streamingstatus.NewPartialUpdateRetryable("conflict")
	unknownErr := context.DeadlineExceeded

	err := unwrapPartialUpdateAppendError(streaming.AppendResponses{
		Responses: []streaming.AppendResponse{
			{Error: casErr},
			{Error: unknownErr},
		},
	})
	require.ErrorIs(t, err, unknownErr)

	err = unwrapPartialUpdateAppendError(streaming.AppendResponses{
		Responses: []streaming.AppendResponse{
			{Error: casErr},
			{AppendResult: &streamingtypes.AppendResult{TimeTick: 100}},
		},
	})
	require.ErrorIs(t, err, casErr)
}

func TestPartialUpdateQueryAccumulatesStorageCost(t *testing.T) {
	schema := createTestSchema()
	upsertData := []*schemapb.FieldData{
		partialUpdateCASPKFieldData([]int64{1}),
		{
			FieldName: "name",
			FieldId:   102,
			Type:      schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"new"}}},
			}},
		},
	}
	queryResult := &milvuspb.QueryResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{
			partialUpdateCASPKFieldData([]int64{1}),
			{
				FieldName: "name",
				FieldId:   102,
				Type:      schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"old"}}},
				}},
			},
		},
	}
	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData: upsertData,
			NumRows:    1,
		},
		upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{FieldsData: upsertData, NumRows: 1},
		}},
		node:        &Proxy{},
		storageCost: segcore.StorageCost{ScannedRemoteBytes: 5, ScannedTotalBytes: 10},
	}
	retrievePatch := mockey.Mock(retrieveByPKs).Return(queryResult, segcore.StorageCost{
		ScannedRemoteBytes: 7,
		ScannedTotalBytes:  20,
	}, nil).Build()
	defer retrievePatch.UnPatch()

	require.NoError(t, task.queryPreExecute(context.Background()))
	require.EqualValues(t, 12, task.storageCost.ScannedRemoteBytes)
	require.EqualValues(t, 30, task.storageCost.ScannedTotalBytes)
}

func TestPartialUpdateRetryRestoresOriginalFieldsBeforeQuery(t *testing.T) {
	task := createTestUpdateTask()
	task.req.PartialUpdate = true
	task.result = &milvuspb.MutationResult{}
	task.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{}},
		DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{}},
	}
	task.upsertMsg.InsertMsg.FieldsData = []*schemapb.FieldData{
		partialUpdateCASPKFieldData([]int64{10}),
		partialUpdateCASPKFieldData([]int64{20}),
		partialUpdateCASPKFieldData([]int64{30}),
		partialUpdateCASPKFieldData([]int64{40}),
	}
	task.node.(*Proxy).tsoAllocator = &timestampAllocator{
		tso:    newMockTimestampAllocatorInterface(),
		peerID: paramtable.GetNodeID(),
	}
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	m := mockey.Mock((*upsertTask).queryPreExecute).To(func(task *upsertTask, ctx context.Context) error {
		require.Len(t, task.upsertMsg.InsertMsg.GetFieldsData(), len(task.req.GetFieldsData()))
		for i, field := range task.req.GetFieldsData() {
			require.Same(t, field, task.upsertMsg.InsertMsg.GetFieldsData()[i])
		}
		return nil
	}).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()
	defer m.UnPatch()

	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	require.NoError(t, task.preparePartialUpdateRetryAttempt(context.Background()))
}

func TestPartialUpdateRetryRefreshesMutationResultCounts(t *testing.T) {
	task := createTestUpdateTask()
	task.req.PartialUpdate = true
	task.result = &milvuspb.MutationResult{
		DeleteCnt: 1,
		InsertCnt: 1,
		UpsertCnt: 1,
	}
	task.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
			FieldsData: task.req.GetFieldsData(),
			NumRows:    uint64(task.req.GetNumRows()),
		}},
		DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{}},
	}
	task.node.(*Proxy).tsoAllocator = &timestampAllocator{
		tso:    newMockTimestampAllocatorInterface(),
		peerID: paramtable.GetNodeID(),
	}
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	m := mockey.Mock((*upsertTask).queryPreExecute).To(func(task *upsertTask, ctx context.Context) error {
		task.insertFieldData = cloneFieldDataList(task.req.GetFieldsData())
		task.deletePKs = partialUpdateCASIDs([]int64{1, 2})
		return nil
	}).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()
	defer m.UnPatch()

	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	err := task.preparePartialUpdateRetryAttempt(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, task.result.GetDeleteCnt())
	require.EqualValues(t, 3, task.result.GetInsertCnt())
	require.EqualValues(t, 3, task.result.GetUpsertCnt())
}

func TestPartialUpdateRetryResolvesTermBeforeQuery(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	task.result = &milvuspb.MutationResult{}
	task.node.(*Proxy).tsoAllocator = &timestampAllocator{
		tso:    newMockTimestampAllocatorInterface(),
		peerID: paramtable.GetNodeID(),
	}

	events := make([]string, 0, len(partialUpdateCASTestVChannels)+1)
	fakeWAL := newPartialUpdateCASTestWAL(t, 11)
	fakeWAL.resolveHook = func(string) {
		events = append(events, "resolve")
	}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	m := mockey.Mock((*timestampAllocator).AllocOne).To(
		func(_ *timestampAllocator, _ context.Context) (Timestamp, error) {
			events = append(events, "readTS")
			return 1000, nil
		},
	).Build()
	defer m.UnPatch()

	generatedField := &schemapb.FieldData{FieldName: "generated", FieldId: 999}
	m = mockey.Mock(genFunctionFields).To(
		func(ctx context.Context, insertMsg *msgstream.InsertMsg, schema *schemaInfo, partialUpdate bool) error {
			events = append(events, "function")
			require.Len(t, insertMsg.GetFieldsData(), len(task.req.GetFieldsData()))
			insertMsg.FieldsData = append(insertMsg.FieldsData, generatedField)
			return nil
		},
	).Build()
	defer m.UnPatch()

	m = mockey.Mock((*upsertTask).queryPreExecute).To(func(task *upsertTask, ctx context.Context) error {
		events = append(events, "query")
		require.Contains(t, task.upsertMsg.InsertMsg.GetFieldsData(), generatedField)
		return nil
	}).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()
	defer m.UnPatch()

	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	err := task.preparePartialUpdateRetryAttempt(context.Background())
	require.NoError(t, err)
	require.Len(t, events, len(partialUpdateCASTestVChannels)+3)
	require.Equal(t, "function", events[0])
	for _, event := range events[1 : len(events)-2] {
		require.Equal(t, "resolve", event)
	}
	require.Equal(t, "readTS", events[len(events)-2])
	require.Equal(t, "query", events[len(events)-1])
}

func TestPartialUpdateAppendAcceptsBuilderCASMetadataForVarCharPK(t *testing.T) {
	task, _, _ := partialUpdateCASStringTestTask(
		t,
		[]string{"pk10", "pk20", "pk30"},
		[]string{"pk20", "pk10", "pk30"},
		[]string{"pk20"},
	)
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)
	insertMsgs, deleteMsgs := buildPartialUpdateCASStringTestMessages(
		t,
		task.collectionID,
		partialUpdateCASTestVChannels,
		[]string{"pk20", "pk10", "pk30"},
		[]string{"pk20"},
		task.partialUpdateCASGroups,
	)

	m := mockey.Mock((*upsertTask).packInsertMessage).Return(insertMsgs, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).packDeleteMessage).Return(deleteMsgs, nil).Build()
	defer m.UnPatch()

	err := task.Execute(context.Background())
	require.NoError(t, err)
	expected := expectedPartialUpdateCASGroups(t, partialUpdateCASStringIDs([]string{"pk10", "pk20", "pk30"}), partialUpdateCASTestVChannels)
	require.Equal(t, len(expected), fakeWAL.resolveCalls)
	require.Equal(t, 1, fakeWAL.appendCalls)
	require.Len(t, fakeWAL.appended, len(insertMsgs)+len(deleteMsgs))
	requireAppendedPartialUpdateCASGroups(t, fakeWAL.appended, expected, task.partialUpdateReadTs, 9)
}

func TestPartialUpdateAutoIDBuildsCASGroupsFromOriginalPKs(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{10, 20, 30}, []int64{20})
	task.schema.CollectionSchema.Fields[0].AutoID = true
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	preparePartialUpdateCASTestGroups(t, task)
	expected := expectedPartialUpdateCASGroups(t, partialUpdateCASIDs([]int64{10, 20, 30}), partialUpdateCASTestVChannels)
	require.Len(t, task.partialUpdateCASGroups, len(expected))
	for vchannel := range expected {
		require.Contains(t, task.partialUpdateCASGroups, vchannel)
	}
	require.Equal(t, len(expected), fakeWAL.resolveCalls)
	require.Zero(t, fakeWAL.appendCalls)
}

func TestNonPartialUpsertDoesNotAttachCASMetadata(t *testing.T) {
	task, insertMsgs, deleteMsgs := partialUpdateCASTestTask(t, false, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	m := mockey.Mock((*upsertTask).packInsertMessage).Return(insertMsgs, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).packDeleteMessage).Return(deleteMsgs, nil).Build()
	defer m.UnPatch()

	err := task.Execute(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, fakeWAL.resolveCalls)
	require.Equal(t, 1, fakeWAL.appendCalls)
	require.Len(t, fakeWAL.appended, len(insertMsgs)+len(deleteMsgs))
	for _, msg := range fakeWAL.appended {
		meta, err := streamingmessage.ExtractPartialUpdateCAS(msg)
		require.NoError(t, err)
		require.Nil(t, meta)
	}
}

func TestPreparePartialUpdateCASGroupsResolveErrorStopsBeforeQuery(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	task.partialUpdateReadTs = 123
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	fakeWAL.resolveErr = errors.New("resolve pchannel failed")
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	err := task.preparePartialUpdateCASGroups(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, fakeWAL.resolveCalls)
	require.Equal(t, 0, fakeWAL.appendCalls)
	require.Empty(t, fakeWAL.appended)
	require.Zero(t, task.partialUpdateReadTs)
}

func TestPreparePartialUpdateCASGroupsRejectsInvalidTerm(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	fakeWAL := newPartialUpdateCASTestWAL(t, -1)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	err := task.preparePartialUpdateCASGroups(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid term")
	require.Empty(t, task.partialUpdateCASGroups)
}

func TestAttachPartialUpdateCASRequiresMetadataSnapshot(t *testing.T) {
	task, insertMsgs, deleteMsgs := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})

	err := task.attachPartialUpdateCAS(append(insertMsgs, deleteMsgs...))
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata snapshot is empty")
}

func TestPreparePartialUpdateCASGroupsSetsEveryVChannelTerm(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)

	require.NotEmpty(t, task.partialUpdateCASGroups)
	for _, meta := range task.partialUpdateCASGroups {
		require.Equal(t, int64(9), meta.GetObservedPchannelTerm())
	}
}

func TestAttachPartialUpdateCASValidatesPreparedGroups(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20, 30}, []int64{20, 10, 30}, []int64{20})
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)
	insertMsgs, deleteMsgs := buildPartialUpdateCASTestMessages(
		t,
		task.collectionID,
		partialUpdateCASTestVChannels,
		[]int64{20, 10, 30},
		[]int64{20},
		task.partialUpdateCASGroups,
	)

	// Validation uses the attempt-scoped channel snapshot and does not rebuild
	// metadata from the request payload after message encryption.
	task.req.FieldsData = nil
	err := task.attachPartialUpdateCAS(append(insertMsgs, deleteMsgs...))
	require.NoError(t, err)
}

func TestAttachPartialUpdateCASRejectsVChannelMismatch(t *testing.T) {
	newMeta := func() *messagespb.PartialUpdateCAS {
		return &messagespb.PartialUpdateCAS{
			ReadTs:               100,
			ObservedPchannelTerm: 1,
		}
	}

	t.Run("message vchannel is not in snapshot", func(t *testing.T) {
		task, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, nil)
		knownChannel := partialUpdateCASTestVChannels[0]
		unknownChannel := partialUpdateCASTestVChannels[1]
		task.partialUpdateCASGroups = map[string]*messagespb.PartialUpdateCAS{
			knownChannel: newMeta(),
		}
		messageGroups := map[string]*messagespb.PartialUpdateCAS{
			unknownChannel: newMeta(),
		}
		insertMsgs, _ := buildPartialUpdateCASTestMessages(
			t,
			task.collectionID,
			[]string{unknownChannel},
			[]int64{10},
			nil,
			messageGroups,
		)

		err := task.attachPartialUpdateCAS(insertMsgs)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Contains(t, err.Error(), "no CAS metadata")
	})

	t.Run("snapshot vchannel has no insert message", func(t *testing.T) {
		task, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, nil)
		presentChannel := partialUpdateCASTestVChannels[0]
		missingChannel := partialUpdateCASTestVChannels[1]
		task.partialUpdateCASGroups = map[string]*messagespb.PartialUpdateCAS{
			presentChannel: newMeta(),
			missingChannel: newMeta(),
		}
		insertMsgs, _ := buildPartialUpdateCASTestMessages(
			t,
			task.collectionID,
			[]string{presentChannel},
			[]int64{10},
			nil,
			task.partialUpdateCASGroups,
		)

		err := task.attachPartialUpdateCAS(insertMsgs)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Contains(t, err.Error(), "no insert message")
	})
}

func TestPartialUpdateCASMetadataSizeIsBounded(t *testing.T) {
	smallTask, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, nil)
	setPartialUpdateCASTestChannels(smallTask, partialUpdateCASTestVChannels[:1])
	smallGroups, err := smallTask.buildPartialUpdateCASGroups()
	require.NoError(t, err)

	largePKs := make([]int64, 1000)
	for idx := range largePKs {
		largePKs[idx] = int64(idx + 1)
	}
	largeTask, _, _ := partialUpdateCASTestTask(t, true, largePKs, largePKs, nil)
	setPartialUpdateCASTestChannels(largeTask, partialUpdateCASTestVChannels[:1])
	largeGroups, err := largeTask.buildPartialUpdateCASGroups()
	require.NoError(t, err)

	smallEncoded, err := streamingmessage.EncodeProto(smallGroups[partialUpdateCASTestVChannels[0]])
	require.NoError(t, err)
	largeEncoded, err := streamingmessage.EncodeProto(largeGroups[partialUpdateCASTestVChannels[0]])
	require.NoError(t, err)
	require.Equal(t, len(smallEncoded), len(largeEncoded))
}

func TestAttachPartialUpdateCASAcceptsEveryBuilderMarkedInsertChunk(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10, 20}, []int64{10, 20}, nil)
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels[:1])
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)

	first, _ := buildPartialUpdateCASTestMessages(t, task.collectionID, partialUpdateCASTestVChannels[:1], []int64{10}, nil, task.partialUpdateCASGroups)
	second, _ := buildPartialUpdateCASTestMessages(t, task.collectionID, partialUpdateCASTestVChannels[:1], []int64{20}, nil, task.partialUpdateCASGroups)
	msgs := []streamingmessage.MutableMessage{first[0], second[0]}
	require.NoError(t, task.attachPartialUpdateCAS(msgs))

	for _, msg := range msgs {
		meta, err := streamingmessage.ExtractPartialUpdateCAS(msg)
		require.NoError(t, err)
		require.NotNil(t, meta)
	}
}

func TestAttachPartialUpdateCASRejectsOversizedInvariant(t *testing.T) {
	task, _, _ := partialUpdateCASTestTask(t, true, []int64{10}, []int64{10}, nil)
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels[:1])
	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)
	preparePartialUpdateCASTestGroups(t, task)
	insertMsgs, _ := buildPartialUpdateCASTestMessages(t, task.collectionID, partialUpdateCASTestVChannels[:1], []int64{10}, nil, task.partialUpdateCASGroups)

	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, strconv.Itoa(insertMsgs[0].EstimateSize()-1)))
	defer Params.Reset(Params.PulsarCfg.MaxMessageSize.Key)

	err := task.attachPartialUpdateCAS(insertMsgs)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
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

func TestRetrieveByPKsUsesPartialUpdateReadTsAsSnapshotFence(t *testing.T) {
	const (
		beginTS = uint64(100)
		readTS  = uint64(200)
	)
	var captured *queryTask

	m := mockey.Mock(typeutil.GetPrimaryFieldSchema).Return(&schemapb.FieldSchema{
		FieldID:      100,
		Name:         "id",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}, nil).Build()
	defer m.UnPatch()

	m = mockey.Mock(validatePartitionTag).Return(nil).Build()
	defer m.UnPatch()

	m = mockey.Mock((*MetaCache).GetPartitionID).Return(int64(1002), nil).Build()
	defer m.UnPatch()

	m = mockey.Mock(planparserv2.CreateRequeryPlan).Return(&planpb.PlanNode{}).Build()
	defer m.UnPatch()

	m = mockey.Mock((*Proxy).query).To(
		func(_ *Proxy, ctx context.Context, qt *queryTask, sp trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			captured = qt
			return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
		},
	).Build()
	defer m.UnPatch()

	task := createTestUpdateTask()
	task.SetTs(beginTS)
	task.partialUpdateReadTs = readTS
	task.partitionKeyMode = false
	task.upsertMsg = &msgstream.UpsertMsg{
		DeleteMsg: &msgstream.DeleteMsg{
			DeleteRequest: &msgpb.DeleteRequest{PartitionName: "_default"},
		},
		InsertMsg: &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{PartitionName: "_default"},
		},
	}

	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}
	_, _, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

	require.NoError(t, err)
	require.NotNil(t, captured)
	require.NotNil(t, captured.RetrieveRequest)
	require.Equal(t, commonpb.ConsistencyLevel_Customized, captured.request.GetConsistencyLevel())
	require.Equal(t, commonpb.ConsistencyLevel_Customized, captured.GetConsistencyLevel())
	require.Equal(t, readTS, captured.request.GetGuaranteeTimestamp())
	require.Equal(t, readTS, captured.GetMvccTimestamp())
	require.Equal(t, readTS, captured.fixedSnapshotTimestamp)
	require.Equal(t, beginTS, task.BeginTs())
}

func TestRetrieveByPKsRejectsMissingPartialUpdateReadTs(t *testing.T) {
	m := mockey.Mock((*Proxy).query).Return(
		&milvuspb.QueryResults{Status: merr.Success()},
		segcore.StorageCost{},
		nil,
	).Build()
	defer m.UnPatch()

	task := createTestUpdateTask()
	task.req.PartialUpdate = true
	task.SetTs(100)
	task.partitionKeyMode = true

	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1}},
		},
	}
	_, _, err := retrieveByPKs(context.Background(), task, ids, []string{"*"})

	require.Error(t, err)
	require.Contains(t, err.Error(), "partial update read timestamp is unavailable")
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
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()

		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			UpdateTimestamp: 12345,
			Schema:          schema,
		}, nil).Build()

		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()

		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()

		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			Name: "_default",
		}, nil).Build()

		events := make([]string, 0, 2)
		fakeWAL := newPartialUpdateCASTestWAL(t, 9)
		fakeWAL.resolveHook = func(string) {
			events = append(events, "resolve")
		}
		oldWAL := streaming.WAL()
		streaming.SetWALForTest(fakeWAL)
		defer streaming.SetWALForTest(oldWAL)

		mockey.Mock((*upsertTask).queryPreExecute).To(func(task *upsertTask, ctx context.Context) error {
			events = append(events, "query")
			return nil
		}).Build()

		mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()

		mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()

		// Execute test
		task := createTestUpdateTask()
		task.req.PartialUpdate = true
		setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)

		err := task.PreExecute(context.Background())

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, task.result)
		assert.Equal(t, int64(1001), task.collectionID)
		assert.NotNil(t, task.schema)
		assert.NotNil(t, task.upsertMsg)
		require.GreaterOrEqual(t, len(events), 2)
		for _, event := range events[:len(events)-1] {
			require.Equal(t, "resolve", event)
		}
		require.Equal(t, "query", events[len(events)-1])
	})
}

func TestUpdateTaskPreExecuteSnapshotsOriginalPartialFieldsBeforeMerge(t *testing.T) {
	m := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
	defer m.UnPatch()
	schema := createTestSchema()
	m = mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
		UpdateTimestamp: 12345,
		Schema:          schema,
	}, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{Name: "_default"}, nil).Build()
	defer m.UnPatch()

	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	m = mockey.Mock((*upsertTask).queryPreExecute).To(func(task *upsertTask, ctx context.Context) error {
		typeutil.SetFieldDataValidData(task.req.FieldsData[1], []bool{true, false, true})
		return nil
	}).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).insertPreExecute).Return(nil).Build()
	defer m.UnPatch()
	m = mockey.Mock((*upsertTask).deletePreExecute).Return(nil).Build()
	defer m.UnPatch()

	task := createTestUpdateTask()
	task.req.PartialUpdate = true
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	originalFields := cloneFieldDataList(task.req.GetFieldsData())

	err := task.PreExecute(context.Background())
	require.NoError(t, err)
	require.Equal(t, originalFields, task.partialUpdateOriginalFields)
	require.NotSame(t, task.req.GetFieldsData()[0], task.partialUpdateOriginalFields[0])
}

func TestUpdateTask_PreExecute_GetCollectionIDError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_GetCollectionIDError", t, func() {
		expectedErr := merr.WrapErrCollectionNotFound("test_collection")
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(0), expectedErr).Build()

		task := createTestUpdateTask()

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
	})
}

func TestUpdateTask_PreExecute_PartitionKeyModeError(t *testing.T) {
	mockey.PatchConvey("TestUpdateTask_PreExecute_PartitionKeyModeError", t, func() {
		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			UpdateTimestamp: 12345,
			Schema:          schema,
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
		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			UpdateTimestamp: 12345,
			Schema:          schema,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			Name: "_default",
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
		schema := createTestSchema()
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
			UpdateTimestamp: 12345,
			Schema:          schema,
		}, nil).Build()
		mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{
			Name: "_default",
		}, nil).Build()

		expectedErr := merr.WrapErrParameterInvalidMsg("query pre-execute failed")
		mockey.Mock((*upsertTask).queryPreExecute).Return(expectedErr).Build()
		fakeWAL := newPartialUpdateCASTestWAL(t, 9)
		oldWAL := streaming.WAL()
		streaming.SetWALForTest(fakeWAL)
		defer streaming.SetWALForTest(oldWAL)

		task := createTestUpdateTask()
		task.req.PartialUpdate = true
		setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)

		err := task.PreExecute(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "query pre-execute failed")
	})
}

func TestUpsertTask_queryPreExecute_MixLogic(t *testing.T) {
	// Schema for the test collection
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
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
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: []bool{true, true}, Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"old1", "old2"}}}}},
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

func TestUpsertTaskQueryPreExecuteRejectsMissingAutoIDPrimaryKey(t *testing.T) {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name:   "test_autoid_partial_update",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, AutoID: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
		},
	})
	upsertData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200}}}}},
		},
	}
	queryResult := &milvuspb.QueryResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
			},
			{
				FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10}}}}},
			},
		},
	}
	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData:     upsertData,
			NumRows:        2,
			PartialUpdate:  true,
			CollectionName: "test_autoid_partial_update",
		},
		upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				FieldsData: upsertData,
				NumRows:    2,
				Version:    msgpb.InsertDataVersion_ColumnBased,
			},
		}},
		node: &Proxy{},
	}
	mockRetrieve := mockey.Mock(retrieveByPKs).Return(queryResult, segcore.StorageCost{}, nil).Build()
	defer mockRetrieve.UnPatch()

	err := task.queryPreExecute(context.Background())
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "requires every primary key to exist")
}

func TestUpsertTask_queryPreExecute_PureInsert(t *testing.T) {
	// Schema for the test collection
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
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
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
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

func TestUpsertTask_queryPreExecute_StructWholeReplace(t *testing.T) {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_struct_partial_update",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  200,
				Name:     "profile",
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 201, Name: "profile[age]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true},
				},
			},
		},
	})

	idField := func(ids ...int64) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}},
		}
	}
	valueField := func(values ...int32) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: values}}}},
		}
	}
	profileField := func(values ...int32) *schemapb.FieldData {
		rows := make([]*schemapb.ScalarField, 0, len(values))
		for _, value := range values {
			rows = append(rows, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{value}}},
			})
		}
		return &schemapb.FieldData{
			FieldName: "profile",
			FieldId:   200,
			Type:      schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{
					Fields: []*schemapb.FieldData{
						{
							FieldName: "age",
							FieldId:   201,
							Type:      schemapb.DataType_Array,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
								ElementType: schemapb.DataType_Int32,
								Data:        rows,
							}}}},
						},
					},
				},
			},
		}
	}
	profileCompactField := func(values []int32, validData []bool) *schemapb.FieldData {
		rows := make([]*schemapb.ScalarField, 0, len(values))
		for _, value := range values {
			rows = append(rows, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{value}}},
			})
		}
		field := profileField()
		subField := field.GetStructArrays().GetFields()[0]
		typeutil.SetFieldDataValidData(subField, validData)
		subField.GetScalars().GetArrayData().Data = rows
		return field
	}
	queryResult := func() *milvuspb.QueryResults {
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				idField(1, 2),
				valueField(10, 20),
				profileField(11, 22),
			},
		}
	}
	run := func(fields []*schemapb.FieldData) (*upsertTask, error) {
		task := &upsertTask{
			ctx:    context.Background(),
			schema: schema,
			req: &milvuspb.UpsertRequest{
				FieldsData:     fields,
				NumRows:        2,
				PartialUpdate:  true,
				CollectionName: "test_struct_partial_update",
			},
			upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: fields,
					NumRows:    2,
					Version:    msgpb.InsertDataVersion_ColumnBased,
				},
			}},
			node: &Proxy{},
		}
		mockRetrieve := mockey.Mock(retrieveByPKs).Return(queryResult(), segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()
		return task, task.queryPreExecute(context.Background())
	}
	structValues := func(field *schemapb.FieldData) []int32 {
		rows := field.GetStructArrays().GetFields()[0].GetScalars().GetArrayData().GetData()
		values := make([]int32, 0, len(rows))
		for _, row := range rows {
			values = append(values, row.GetIntData().GetData()[0])
		}
		return values
	}
	findProfile := func(task *upsertTask) *schemapb.FieldData {
		for _, field := range task.insertFieldData {
			if field.GetFieldName() == "profile" {
				return field
			}
		}
		return nil
	}

	t.Run("omitted struct preserves old value", func(t *testing.T) {
		task, err := run([]*schemapb.FieldData{
			idField(1, 2),
			valueField(100, 200),
		})
		assert.NoError(t, err)
		profile := findProfile(task)
		if assert.NotNil(t, profile) {
			assert.Equal(t, []int32{11, 22}, structValues(profile))
		}
	})

	t.Run("provided top-level struct replaces whole struct", func(t *testing.T) {
		task, err := run([]*schemapb.FieldData{
			idField(1, 2),
			valueField(100, 200),
			profileField(111, 222),
		})
		assert.NoError(t, err)
		profile := findProfile(task)
		if assert.NotNil(t, profile) {
			assert.Equal(t, []int32{111, 222}, structValues(profile))
		}
	})

	t.Run("nullable struct compact payload is compressed after merge", func(t *testing.T) {
		task, err := run([]*schemapb.FieldData{
			idField(1, 2),
			valueField(100, 200),
			profileCompactField([]int32{111}, []bool{true, false}),
		})
		assert.NoError(t, err)
		profile := findProfile(task)
		if assert.NotNil(t, profile) {
			subField := profile.GetStructArrays().GetFields()[0]
			assert.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(subField))
			rows := subField.GetScalars().GetArrayData().GetData()
			require.Len(t, rows, 1)
			assert.Equal(t, []int32{111}, rows[0].GetIntData().GetData())
		}
	})

	t.Run("nullable struct dense payload with null row is rejected", func(t *testing.T) {
		_, err := run([]*schemapb.FieldData{
			idField(1, 2),
			valueField(100, 200),
			profileCompactField([]int32{111, 222}, []bool{true, false}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "payload must be compact")
	})

	t.Run("mixed insert update keeps request struct rows", func(t *testing.T) {
		task := &upsertTask{
			ctx:    context.Background(),
			schema: schema,
			req: &milvuspb.UpsertRequest{
				FieldsData: []*schemapb.FieldData{
					idField(1, 3),
					valueField(100, 300),
					profileField(111, 333),
				},
				NumRows:        2,
				PartialUpdate:  true,
				CollectionName: "test_struct_partial_update",
			},
			upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: []*schemapb.FieldData{
						idField(1, 3),
						valueField(100, 300),
						profileField(111, 333),
					},
					NumRows: 2,
					Version: msgpb.InsertDataVersion_ColumnBased,
				},
			}},
			node: &Proxy{},
		}
		mockRetrieve := mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				idField(1),
				valueField(10),
				profileField(11),
			},
		}, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		err := task.queryPreExecute(context.Background())
		assert.NoError(t, err)
		profile := findProfile(task)
		if assert.NotNil(t, profile) {
			assert.Equal(t, []int32{111, 333}, structValues(profile))
		}
	})

	t.Run("direct struct sub-field update is rejected", func(t *testing.T) {
		subField := profileField(111, 222).GetStructArrays().GetFields()[0]
		subField.FieldName = "profile[age]"
		_, err := run([]*schemapb.FieldData{
			idField(1, 2),
			valueField(100, 200),
			subField,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "partial struct update is not supported")
	})
}

func TestValidateWholeStructFieldDataForPartialUpdateNestedArray(t *testing.T) {
	typeSchema := &schemapb.TypeSchema{
		Kind: &schemapb.TypeSchema_ArrayElement{
			ArrayElement: &schemapb.TypeSchema{
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
				Kind: &schemapb.TypeSchema_ArrayElement{
					ArrayElement: &schemapb.TypeSchema{
						Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32},
					},
				},
			},
		},
	}
	collectionSchema := &schemapb.CollectionSchema{
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 200,
				Name:    "profile",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     201,
						Name:        "profile[values]",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Array,
						TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "32"}},
						TypeSchema:  typeSchema,
					},
				},
			},
		},
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(collectionSchema)
	require.NoError(t, err)
	nestedRow := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int32},
		},
	}
	subFieldData := &schemapb.FieldData{
		FieldName: "values",
		FieldId:   201,
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_Array,
						Data:        []*schemapb.ScalarField{nestedRow},
					},
				},
			},
		},
	}
	fieldData := &schemapb.FieldData{
		FieldName: "profile",
		FieldId:   200,
		Type:      schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{
				Fields: []*schemapb.FieldData{subFieldData},
			},
		},
	}

	require.NoError(t, validateWholeStructFieldDataForPartialUpdate(
		schemaHelper,
		collectionSchema.GetStructArrayFields()[0],
		fieldData,
		1,
	))
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
					ValidData: []bool{true, false, true, false},
					Data: &schemapb.ScalarField_TimestamptzData{
						TimestamptzData: &schemapb.TimestamptzArray{
							Data: []int64{1000, 0, 3000, 0},
						},
					},
				},
			},
		}

		err := ToCompressedFormatNullable(field)
		assert.NoError(t, err)
		assert.Equal(t, []int64{1000, 3000}, field.GetScalars().GetTimestamptzData().GetData())
		assert.Equal(t, []bool{true, false, true, false}, typeutil.GetFieldDataValidData(field))
	})

	t.Run("geometry WKT with null values", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_Geometry,
			FieldName: "geometry_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					ValidData: []bool{true, false, true},
					Data: &schemapb.ScalarField_GeometryWktData{
						GeometryWktData: &schemapb.GeometryWktArray{
							Data: []string{"POINT (1 2)", "", "POINT (5 6)"},
						},
					},
				},
			},
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
					ValidData: []bool{true, false, true},
					Data: &schemapb.ScalarField_GeometryData{
						GeometryData: &schemapb.GeometryArray{
							Data: [][]byte{{0x01, 0x02}, nil, {0x05, 0x06}},
						},
					},
				},
			},
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
		assert.Len(t, typeutil.GetFieldDataValidData(fieldData), upsertIDSize)
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
		assert.Len(t, typeutil.GetFieldDataValidData(fieldData), upsertIDSize)
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
		schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
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
			mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{UpdateTimestamp: 12345, Schema: schema}, nil).Build()
			mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
			mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
			mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{Name: "_default"}, nil).Build()
			mockey.Mock((*MetaCache).GetDatabaseInfo).Return(&databaseInfo{DBID: 0}, nil).Build()
			mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()

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
				baseTask: baseTask{metaCache: &MetaCache{}},
				ctx:      ctx,
				schema:   schema,
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
		schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
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
			mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{UpdateTimestamp: 12345, Schema: schema}, nil).Build()
			mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build()
			mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
			mockey.Mock((*MetaCache).GetPartitionInfo).Return(&partitionInfo{Name: "_default"}, nil).Build()
			mockey.Mock((*MetaCache).GetDatabaseInfo).Return(&databaseInfo{DBID: 0}, nil).Build()
			mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()

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
				baseTask: baseTask{metaCache: &MetaCache{}},
				ctx:      ctx,
				schema:   schema,
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

func TestInsertPreExecute_FilterBM25AndMinHashOutputFields(t *testing.T) {
	paramtable.Init()

	numRows := 2

	getFieldNames := func(data []*schemapb.FieldData) []string {
		names := make([]string, 0, len(data))
		for _, fd := range data {
			names = append(names, fd.GetFieldName())
		}
		return names
	}

	t.Run("partial update filters BM25 and MinHash output fields", func(t *testing.T) {
		m := mockey.Mock(common.AllocAutoID).Return(int64(1000), int64(1000+numRows), nil).Build()
		defer m.UnPatch()

		schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name:   "test_filter_bm25_minhash",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "2000"}}},
				{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
				{FieldID: 103, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
				{FieldID: 104, Name: "mh", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "512"}}},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25",
					Type:             schemapb.FunctionType_BM25,
					InputFieldIds:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIds:   []int64{103},
					OutputFieldNames: []string{"sparse"},
				},
				{
					Name:             "minhash",
					Type:             schemapb.FunctionType_MinHash,
					InputFieldIds:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIds:   []int64{104},
					OutputFieldNames: []string{"mh"},
				},
			},
		})

		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "text", FieldId: 101, Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "world"}}}}},
			},
			{
				FieldName: "vec", FieldId: 102, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, numRows*4)}}}},
			},
			{
				FieldName: "sparse", FieldId: 103, Type: schemapb.DataType_SparseFloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{}}},
			},
			{
				FieldName: "mh", FieldId: 104, Type: schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 512, Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, numRows*512/8)}}},
			},
		}

		task := &upsertTask{
			ctx:         context.Background(),
			schema:      schema,
			idAllocator: &allocator.IDAllocator{},
			req: &milvuspb.UpsertRequest{
				CollectionName: "test_filter_bm25_minhash",
				PartialUpdate:  true,
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{
					InsertRequest: &msgpb.InsertRequest{
						CollectionName: "test_filter_bm25_minhash",
						Version:        msgpb.InsertDataVersion_ColumnBased,
						FieldsData:     fieldsData,
						NumRows:        uint64(numRows),
						PartitionName:  Params.CommonCfg.DefaultPartitionName.GetValue(),
					},
				},
			},
			result: &milvuspb.MutationResult{},
		}

		_ = task.insertPreExecute(context.Background())

		remainingFields := getFieldNames(task.upsertMsg.InsertMsg.GetFieldsData())
		assert.NotContains(t, remainingFields, "sparse")
		assert.NotContains(t, remainingFields, "mh")
		assert.Contains(t, remainingFields, "text")
		assert.Contains(t, remainingFields, "vec")
	})

	t.Run("partial update preserves non-BM25/MinHash function output fields", func(t *testing.T) {
		m := mockey.Mock(common.AllocAutoID).Return(int64(1000), int64(1000+numRows), nil).Build()
		defer m.UnPatch()

		// Schema with a text embedding function (non-BM25/MinHash)
		schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name:   "test_preserve_embedding",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "2000"}}},
				{FieldID: 102, Name: "embedding", DataType: schemapb.DataType_FloatVector, IsFunctionOutput: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			},
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "text_embedding",
					Type:             schemapb.FunctionType_TextEmbedding,
					InputFieldIds:    []int64{101},
					InputFieldNames:  []string{"text"},
					OutputFieldIds:   []int64{102},
					OutputFieldNames: []string{"embedding"},
				},
			},
		})

		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "text", FieldId: 101, Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "world"}}}}},
			},
			{
				FieldName: "embedding", FieldId: 102, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, numRows*4)}}}},
			},
		}

		task := &upsertTask{
			ctx:         context.Background(),
			schema:      schema,
			idAllocator: &allocator.IDAllocator{},
			req: &milvuspb.UpsertRequest{
				CollectionName: "test_preserve_embedding",
				PartialUpdate:  true,
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{
					InsertRequest: &msgpb.InsertRequest{
						CollectionName: "test_preserve_embedding",
						Version:        msgpb.InsertDataVersion_ColumnBased,
						FieldsData:     fieldsData,
						NumRows:        uint64(numRows),
						PartitionName:  Params.CommonCfg.DefaultPartitionName.GetValue(),
					},
				},
			},
			result: &milvuspb.MutationResult{},
		}

		_ = task.insertPreExecute(context.Background())

		// embedding (text embedding output) should NOT be filtered
		remainingFields := getFieldNames(task.upsertMsg.InsertMsg.GetFieldsData())
		assert.Contains(t, remainingFields, "text")
		assert.Contains(t, remainingFields, "embedding")
		assert.Len(t, remainingFields, 2)
	})

	t.Run("partial update with no functions keeps all fields", func(t *testing.T) {
		m := mockey.Mock(common.AllocAutoID).Return(int64(1000), int64(1000+numRows), nil).Build()
		defer m.UnPatch()

		noFuncSchema := mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name:   "test_no_func",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "2000"}}},
				{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			},
		})

		noFuncFieldsData := []*schemapb.FieldData{
			{
				FieldName: "text", FieldId: 101, Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "world"}}}}},
			},
			{
				FieldName: "vec", FieldId: 102, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, numRows*4)}}}},
			},
		}

		task := &upsertTask{
			ctx:         context.Background(),
			schema:      noFuncSchema,
			idAllocator: &allocator.IDAllocator{},
			req: &milvuspb.UpsertRequest{
				CollectionName: "test_no_func",
				PartialUpdate:  true,
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{
					InsertRequest: &msgpb.InsertRequest{
						CollectionName: "test_no_func",
						Version:        msgpb.InsertDataVersion_ColumnBased,
						FieldsData:     noFuncFieldsData,
						NumRows:        uint64(numRows),
						PartitionName:  Params.CommonCfg.DefaultPartitionName.GetValue(),
					},
				},
			},
			result: &milvuspb.MutationResult{},
		}

		_ = task.insertPreExecute(context.Background())

		remainingFields := getFieldNames(task.upsertMsg.InsertMsg.GetFieldsData())
		assert.Contains(t, remainingFields, "text")
		assert.Contains(t, remainingFields, "vec")
		assert.Len(t, remainingFields, 2)
	})
}

func TestInsertPreExecutePreservesAutoIDPrimaryKeyForPartialUpdate(t *testing.T) {
	m := mockey.Mock(common.AllocAutoID).Return(int64(1000), int64(1001), nil).Build()
	defer m.UnPatch()

	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name:   "test_autoid_partial_update",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
		},
	})
	fieldsData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{20}}}}},
		},
	}
	task := &upsertTask{
		ctx:         context.Background(),
		schema:      schema,
		idAllocator: &allocator.IDAllocator{},
		req: &milvuspb.UpsertRequest{
			CollectionName: "test_autoid_partial_update",
			PartialUpdate:  true,
		},
		upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: "test_autoid_partial_update",
				PartitionName:  Params.CommonCfg.DefaultPartitionName.GetValue(),
				FieldsData:     fieldsData,
				NumRows:        1,
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		}},
		result: &milvuspb.MutationResult{},
	}

	err := task.insertPreExecute(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{10}, task.result.GetIDs().GetIntId().GetData())
	require.Equal(t, []int64{10}, task.oldIDs.GetIntId().GetData())
	primaryField, err := typeutil.GetPrimaryFieldData(task.upsertMsg.InsertMsg.GetFieldsData(), schema.Fields[0])
	require.NoError(t, err)
	require.Equal(t, []int64{10}, primaryField.GetScalars().GetLongData().GetData())
	require.Equal(t, []int64{1000}, task.upsertMsg.InsertMsg.GetRowIDs())
}

func TestUpsertTask_queryPreExecute_NullableFields(t *testing.T) {
	dim := int64(4)

	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
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
				FieldName: "nullable_vec", FieldId: 102, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{ValidData: validData, Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: nullableData}}}},
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
					FieldName: "nullable_vec", FieldId: 102, Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{ValidData: validData, Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: nullableData}}}},
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
	assert.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(task1a.insertFieldData[2]))
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
	assert.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(task3a.insertFieldData[2]))
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
	assert.Equal(t, []bool{true, true, false, false}, typeutil.GetFieldDataValidData(task4b.insertFieldData[2]))
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
				assert.Equal(t, upsertIDSize, len(typeutil.GetFieldDataValidData(result)))
				// All ValidData should be false (null)
				for _, v := range typeutil.GetFieldDataValidData(result) {
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
				assert.Equal(t, upsertIDSize, len(typeutil.GetFieldDataValidData(result)))
				// All ValidData should be false (null)
				for _, v := range typeutil.GetFieldDataValidData(result) {
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
		assert.Equal(t, upsertIDSize, len(typeutil.GetFieldDataValidData(result)))
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

func TestUpsertTask_queryPreExecute_DefaultValueWithValidData(t *testing.T) {
	// Schema with a non-nullable field that has DefaultValue
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_default_value_upsert",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{
				FieldID: 102, Name: "default_col", DataType: schemapb.DataType_VarChar,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{StringData: "default_val"},
				},
			},
		},
	})

	// Upsert 3 rows; default_col in compressed format: 2 actual values, row 3 uses default
	upsertData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}}}}},
		},
		{
			FieldName: "default_col", FieldId: 102, Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: []bool{true, true, false}, Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b"}}}}},
		},
	}

	// Query result: existing records for PKs 1, 2
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
				FieldName: "default_col", FieldId: 102, Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"old1", "old2"}}}}},
			},
		},
	}

	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData: upsertData,
			NumRows:    3,
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: upsertData,
					NumRows:    3,
					Version:    msgpb.InsertDataVersion_ColumnBased,
				},
			},
		},
		node: &Proxy{},
	}

	mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
	defer mockRetrieve.UnPatch()

	err := task.queryPreExecute(context.Background())
	assert.NoError(t, err)

	// Verify default_col was expanded: "a", "b", "default_val"
	var defaultColField *schemapb.FieldData
	for _, f := range task.insertFieldData {
		if f.GetFieldName() == "default_col" {
			defaultColField = f
			break
		}
	}
	assert.NotNil(t, defaultColField)
	assert.Equal(t, []string{"a", "b", "default_val"}, defaultColField.GetScalars().GetStringData().GetData())
}

func TestUpsertTask_queryPreExecute_DefaultValueError(t *testing.T) {
	// Schema with a non-nullable field that has DefaultValue
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_default_value_error",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{
				FieldID: 102, Name: "default_col", DataType: schemapb.DataType_VarChar,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{StringData: "default_val"},
				},
			},
		},
	})

	// Upsert 3 rows; default_col has ValidData with wrong length (2 instead of 3)
	upsertData := []*schemapb.FieldData{
		{
			FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
		},
		{
			FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}}}}},
		},
		{
			// ValidData length (2) doesn't match numRows (3) → FillWithDefaultValue returns error
			FieldName: "default_col", FieldId: 102, Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}}},
		},
	}

	// Query result: existing records for PKs 1, 2
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
				FieldName: "default_col", FieldId: 102, Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"old1", "old2"}}}}},
			},
		},
	}

	task := &upsertTask{
		ctx:    context.Background(),
		schema: schema,
		req: &milvuspb.UpsertRequest{
			FieldsData: upsertData,
			NumRows:    3,
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					FieldsData: upsertData,
					NumRows:    3,
					Version:    msgpb.InsertDataVersion_ColumnBased,
				},
			},
		},
		node: &Proxy{},
	}

	mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
	defer mockRetrieve.UnPatch()

	err := task.queryPreExecute(context.Background())
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestUpsertTask_queryPreExecute_DynamicFieldValidData(t *testing.T) {
	// Schema with dynamic field enabled, simulating a collection with id + value + $meta
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name:               "test_dynamic_validdata",
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{
				FieldID: 102, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON,
				IsDynamic: true, Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{StringData: "{}"},
				},
			},
		},
	})

	t.Run("dynamic field with ValidData merges correctly", func(t *testing.T) {
		// Upsert 3 rows: IDs 1,2 (update), 3 (insert)
		// User provides dynamic field $meta WITHOUT ValidData
		// queryPreExecute will auto-fill ValidData with all-true before merge
		meta1, _ := json.Marshal(map[string]interface{}{"color": "gold"})
		meta2, _ := json.Marshal(map[string]interface{}{"color": "silver"})
		meta3, _ := json.Marshal(map[string]interface{}{"color": "bronze"})

		upsertData := []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
			},
			{
				FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}}}}},
			},
			{
				FieldName: common.MetaFieldName, FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: [][]byte{meta1, meta2, meta3}},
				}}},
				// No ValidData — queryPreExecute auto-fills with all-true
			},
		}

		// Query result: existing PKs 1, 2
		existMeta1, _ := json.Marshal(map[string]interface{}{"color": "red"})
		existMeta2, _ := json.Marshal(map[string]interface{}{"color": "blue"})
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
					FieldName: common.MetaFieldName, FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{Data: [][]byte{existMeta1, existMeta2}},
					}}},
				},
			},
		}

		task := &upsertTask{
			ctx:    context.Background(),
			schema: schema,
			req: &milvuspb.UpsertRequest{
				FieldsData: upsertData,
				NumRows:    3,
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{
					InsertRequest: &msgpb.InsertRequest{
						FieldsData: upsertData,
						NumRows:    3,
						Version:    msgpb.InsertDataVersion_ColumnBased,
					},
				},
			},
			node: &Proxy{},
		}

		mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		err := task.queryPreExecute(context.Background())
		assert.NoError(t, err)

		// Verify merged $meta has 3 entries with correct ValidData length
		var metaField *schemapb.FieldData
		for _, f := range task.insertFieldData {
			if f.GetFieldName() == common.MetaFieldName {
				metaField = f
				break
			}
		}
		assert.NotNil(t, metaField)
		metaData := metaField.GetScalars().GetJsonData().GetData()
		assert.Equal(t, 3, len(metaData), "merged $meta should have 3 rows")
		// ValidData should also have 3 entries (2 from update + 1 from insert)
		assert.Equal(t, 3, len(typeutil.GetFieldDataValidData(metaField)), "ValidData length should match row count")
	})

	t.Run("dynamic field without ValidData is auto-filled by queryPreExecute", func(t *testing.T) {
		// This test verifies the fix: when $meta has NO ValidData (SDK behavior),
		// queryPreExecute auto-fills it with all-true, so merge produces correct length
		meta1, _ := json.Marshal(map[string]interface{}{"color": "gold"})
		meta2, _ := json.Marshal(map[string]interface{}{"color": "silver"})
		meta3, _ := json.Marshal(map[string]interface{}{"color": "bronze"})

		upsertData := []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
			},
			{
				FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}}}}},
			},
			{
				FieldName: common.MetaFieldName, FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: [][]byte{meta1, meta2, meta3}},
				}}},
				// NO ValidData — queryPreExecute will auto-fill
			},
		}

		existMeta1, _ := json.Marshal(map[string]interface{}{"color": "red"})
		existMeta2, _ := json.Marshal(map[string]interface{}{"color": "blue"})
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
					FieldName: common.MetaFieldName, FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{Data: [][]byte{existMeta1, existMeta2}},
					}}},
				},
			},
		}

		task := &upsertTask{
			ctx:    context.Background(),
			schema: schema,
			req: &milvuspb.UpsertRequest{
				FieldsData: upsertData,
				NumRows:    3,
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{
					InsertRequest: &msgpb.InsertRequest{
						FieldsData: upsertData,
						NumRows:    3,
						Version:    msgpb.InsertDataVersion_ColumnBased,
					},
				},
			},
			node: &Proxy{},
		}

		mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		err := task.queryPreExecute(context.Background())
		assert.NoError(t, err)

		// queryPreExecute auto-fills ValidData on $meta, so merge produces correct length 3
		var metaField *schemapb.FieldData
		for _, f := range task.insertFieldData {
			if f.GetFieldName() == common.MetaFieldName {
				metaField = f
				break
			}
		}
		assert.NotNil(t, metaField)
		metaData := metaField.GetScalars().GetJsonData().GetData()
		assert.Equal(t, 3, len(metaData), "merged $meta should have 3 rows")
		validData := typeutil.GetFieldDataValidData(metaField)
		assert.Equal(t, 3, len(validData),
			"queryPreExecute auto-fills ValidData, merge produces correct length 3")
	})

	t.Run("v25 schema (non-nullable $meta) upsert should not fail", func(t *testing.T) {
		// 2.5-style schema: $meta is NOT nullable and has NO default value.
		// After upgrading to 2.6, existing collections retain this schema.
		// queryPreExecute must NOT unconditionally fill ValidData for $meta,
		// because CheckValidData expects len(ValidData)==0 for non-nullable fields.
		v25Schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name:               "test_v25_compat",
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
				{
					FieldID: 102, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON,
					IsDynamic: true,
					Nullable:  false, // 2.5 style: NOT nullable
					// No DefaultValue — 2.5 style
				},
			},
		})

		meta1, _ := json.Marshal(map[string]interface{}{"color": "gold"})
		meta2, _ := json.Marshal(map[string]interface{}{"color": "silver"})
		meta3, _ := json.Marshal(map[string]interface{}{"color": "bronze"})

		upsertData := []*schemapb.FieldData{
			{
				FieldName: "id", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
			},
			{
				FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{100, 200, 300}}}}},
			},
			{
				FieldName: common.MetaFieldName, FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: [][]byte{meta1, meta2, meta3}},
				}}},
				// No ValidData — SDK behavior
			},
		}

		existMeta1, _ := json.Marshal(map[string]interface{}{"color": "red"})
		existMeta2, _ := json.Marshal(map[string]interface{}{"color": "blue"})
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
					FieldName: common.MetaFieldName, FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{Data: [][]byte{existMeta1, existMeta2}},
					}}},
				},
			},
		}

		task := &upsertTask{
			ctx:    context.Background(),
			schema: v25Schema,
			req: &milvuspb.UpsertRequest{
				FieldsData: upsertData,
				NumRows:    3,
			},
			upsertMsg: &msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{
					InsertRequest: &msgpb.InsertRequest{
						FieldsData: upsertData,
						NumRows:    3,
						Version:    msgpb.InsertDataVersion_ColumnBased,
					},
				},
			},
			node: &Proxy{},
		}

		mockRetrieve := mockey.Mock(retrieveByPKs).Return(mockQueryResult, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		err := task.queryPreExecute(context.Background())
		assert.NoError(t, err, "queryPreExecute should not fail for 2.5-style non-nullable $meta")

		var metaField *schemapb.FieldData
		for _, f := range task.insertFieldData {
			if f.GetFieldName() == common.MetaFieldName {
				metaField = f
				break
			}
		}
		assert.NotNil(t, metaField)
		metaData := metaField.GetScalars().GetJsonData().GetData()
		assert.Equal(t, 3, len(metaData), "merged $meta should have 3 rows")
		// For non-nullable $meta, ValidData should remain empty (not auto-filled)
		assert.Empty(t, typeutil.GetFieldDataValidData(metaField),
			"non-nullable $meta should NOT have ValidData auto-filled")
	})
}
