package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

func TestInsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				NumRows: 0,
			},
		},
	}
	err = case1.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// checkFieldsDataBySchema was already checked by TestInsertTask_checkFieldsDataBySchema

	boolFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}
	int8FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}
	int16FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}
	int32FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}
	int64FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}
	floatFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Float}
	doubleFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Double}
	floatVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
	binaryVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector}
	float16VectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Float16Vector}
	bfloat16VectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_BFloat16Vector}
	varCharFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}

	numRows := 20
	dim := 128
	case2 := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    msgpb.InsertDataVersion_ColumnBased,
				RowIDs:     testutils.GenerateInt64Array(numRows),
				Timestamps: testutils.GenerateUint64Array(numRows),
			},
		},
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkRowNums",
			Description: "TestInsertTask_checkRowNums",
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
				float16VectorFieldSchema,
				bfloat16VectorFieldSchema,
				varCharFieldSchema,
			},
		},
	}

	// satisfied
	case2.insertMsg.NumRows = uint64(numRows)
	case2.insertMsg.FieldsData = []*schemapb.FieldData{
		newScalarFieldData(boolFieldSchema, "Bool", numRows),
		newScalarFieldData(int8FieldSchema, "Int8", numRows),
		newScalarFieldData(int16FieldSchema, "Int16", numRows),
		newScalarFieldData(int32FieldSchema, "Int32", numRows),
		newScalarFieldData(int64FieldSchema, "Int64", numRows),
		newScalarFieldData(floatFieldSchema, "Float", numRows),
		newScalarFieldData(doubleFieldSchema, "Double", numRows),
		newFloatVectorFieldData("FloatVector", numRows, dim),
		newBinaryVectorFieldData("BinaryVector", numRows, dim),
		newFloat16VectorFieldData("Float16Vector", numRows, dim),
		newBFloat16VectorFieldData("BFloat16Vector", numRows, dim),
		newScalarFieldData(varCharFieldSchema, "VarChar", numRows),
	}
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less bool data
	case2.insertMsg.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more bool data
	case2.insertMsg.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int8 data
	case2.insertMsg.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int8 data
	case2.insertMsg.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int16 data
	case2.insertMsg.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int16 data
	case2.insertMsg.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int32 data
	case2.insertMsg.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int32 data
	case2.insertMsg.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int64 data
	case2.insertMsg.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int64 data
	case2.insertMsg.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less float data
	case2.insertMsg.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more float data
	case2.insertMsg.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less double data
	case2.insertMsg.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.insertMsg.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less float vectors
	case2.insertMsg.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more float vectors
	case2.insertMsg.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less binary vectors
	case2.insertMsg.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more binary vectors
	case2.insertMsg.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less double data
	case2.insertMsg.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.insertMsg.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less float16 vectors
	case2.insertMsg.FieldsData[9] = newFloat16VectorFieldData("Float16Vector", numRows/2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more float16 vectors
	case2.insertMsg.FieldsData[9] = newFloat16VectorFieldData("Float16Vector", numRows*2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[9] = newFloat16VectorFieldData("Float16Vector", numRows, dim)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less bfloat16 vectors
	case2.insertMsg.FieldsData[10] = newBFloat16VectorFieldData("BFloat16Vector", numRows/2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more bfloat16 vectors
	case2.insertMsg.FieldsData[10] = newBFloat16VectorFieldData("BFloat16Vector", numRows*2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[10] = newBFloat16VectorFieldData("BFloat16Vector", numRows, dim)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)
}

func TestInsertTask(t *testing.T) {
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
		it := insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: collectionName,
				},
			},
			chMgr: chMgr,
		}
		err := it.setChannels()
		assert.NoError(t, err)
		resChannels := it.getChannels()
		assert.ElementsMatch(t, channels, resChannels)
		assert.ElementsMatch(t, channels, it.pChannels)
	})
}

func TestMaxInsertSize(t *testing.T) {
	t.Run("test MaxInsertSize", func(t *testing.T) {
		paramtable.Init()
		Params.Save(Params.QuotaConfig.MaxInsertSize.Key, "1")
		defer Params.Reset(Params.QuotaConfig.MaxInsertSize.Key)
		it := insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					DbName:         "hooooooo",
					CollectionName: "fooooo",
				},
			},
		}
		err := it.PreExecute(context.Background())
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})
}

func TestInsertTask_Function(t *testing.T) {
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
	f := schemapb.FieldData{
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
	data = append(data, &f)
	collectionName := "TestInsertTask_function"
	schema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "TestInsertTask_function",
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Status(nil),
		ID:     11198,
		Count:  10,
	}, nil)
	idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
	idAllocator.Start()
	defer idAllocator.Close()
	assert.NoError(t, err)
	task := insertTask{
		ctx: context.Background(),
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: collectionName,
				DbName:         "hooooooo",
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    msgpb.InsertDataVersion_ColumnBased,
				FieldsData: data,
				NumRows:    2,
			},
		},
		schema:      schema,
		idAllocator: idAllocator,
	}

	info := newSchemaInfo(schema)
	cache := NewMockCache(t)
	collectionID := UniqueID(0)
	cache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)

	cache.On("GetCollectionSchema",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(info, nil)

	cache.On("GetPartitionInfo",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(&partitionInfo{
		name:                "p1",
		partitionID:         10,
		createdTimestamp:    10001,
		createdUtcTimestamp: 10002,
	}, nil)
	cache.On("GetCollectionInfo",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(&collectionInfo{schema: info}, nil)
	cache.On("GetDatabaseInfo",
		mock.Anything,
		mock.Anything,
	).Return(&databaseInfo{properties: []*commonpb.KeyValuePair{}}, nil)

	globalMetaCache = cache
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
}

func TestInsertTaskForSchemaMismatch(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	mockCache := NewMockCache(t)
	globalMetaCache = mockCache
	ctx := context.Background()

	t.Run("schema ts mismatch", func(t *testing.T) {
		it := insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					DbName:         "hooooooo",
					CollectionName: "fooooo",
				},
			},
			schemaTimestamp: 99,
		}
		mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil)
		mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			updateTimestamp: 100,
		}, nil)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		err := it.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
	})
}
