package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRepackInsertDataForStreamingServicePreservesExplicitZeroSchemaVersion(t *testing.T) {
	paramtable.Init()

	oldCache := globalMetaCache
	cache := NewMockCache(t)
	cache.On("GetPartitionID", mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	insertMsg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Insert,
				SourceID: 1,
			},
			CollectionID:   100,
			DbName:         "db",
			CollectionName: "coll",
			PartitionName:  "_default",
			NumRows:        1,
			FieldsData: []*schemapb.FieldData{
				{
					FieldName: "pk",
					FieldId:   1,
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1}},
							},
						},
					},
				},
			},
			RowIDs:     []int64{1},
			Timestamps: []uint64{1},
		},
	}
	result := &milvuspb.MutationResult{
		IDs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}},
		},
	}

	// Idempotency disabled: no header decorator, so no key and no insert result.
	msgs, err := repackInsertDataForStreamingService(context.Background(), []string{"ch"}, insertMsg, result, nil, 0, nil)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)

	msg := message.MustAsMutableInsertMessageV1(msgs[0])
	header := msg.Header()
	assert.NotNil(t, header.SchemaVersion)
	assert.Equal(t, int32(0), header.GetSchemaVersion())
	assert.Empty(t, header.GetIdempotencyKey())
	_, ok := message.IdempotentInsertResultFromInsertHeader(header)
	assert.False(t, ok)

	// Idempotency enabled: the proxy decorator single-sources both the idempotency
	// key and the per-write-unit insert result onto the insert header.
	it := &insertTask{idempotencyEnabled: true, idempotencyKey: "key-1", result: result}
	msgs, err = repackInsertDataForStreamingService(context.Background(), []string{"ch"}, insertMsg, result, nil, 0, it.idempotentInsertHeaderDecorator())
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)

	msg = message.MustAsMutableInsertMessageV1(msgs[0])
	header = msg.Header()
	assert.Equal(t, "key-1", header.GetIdempotencyKey())
	extra, ok := message.IdempotentInsertResultFromInsertHeader(header)
	assert.True(t, ok)
	assert.Equal(t, []uint32{0}, extra.GetRowOffsets())
	assert.Equal(t, []int64{1}, extra.GetIds().GetIntId().GetData())
}

func TestRepackInsertDataForStreamingServiceSplitIdempotentMessagesShareKey(t *testing.T) {
	paramtable.Init()
	require.NoError(t, Params.Save(Params.PulsarCfg.MaxMessageSize.Key, "1"))
	t.Cleanup(func() { Params.Reset(Params.PulsarCfg.MaxMessageSize.Key) })

	oldCache := globalMetaCache
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	globalMetaCache = cache
	t.Cleanup(func() { globalMetaCache = oldCache })

	insertMsg, result := newIdempotentRepackInsertMsg(3)
	it := &insertTask{idempotencyEnabled: true, idempotencyKey: "key-split", result: result}

	msgs, err := repackInsertDataForStreamingService(
		context.Background(),
		[]string{"ch"},
		insertMsg,
		result,
		nil,
		0,
		it.idempotentInsertHeaderDecorator(),
	)
	require.NoError(t, err)
	require.Greater(t, len(msgs), 1)

	assert.Equal(t, []uint32{0, 1, 2}, collectIdempotentRepackOffsets(t, msgs, "ch", "key-split"))
}

func TestRepackInsertDataWithPartitionKeyForStreamingServiceSameVChannelMessagesShareKey(t *testing.T) {
	paramtable.Init()

	oldCache := globalMetaCache
	cache := NewMockCache(t)
	cache.EXPECT().GetPartitions(mock.Anything, "db", "coll").Return(map[string]int64{
		"partition_0": 300,
		"partition_1": 301,
	}, nil)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_0").Return(int64(300), nil)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "partition_1").Return(int64(301), nil)
	globalMetaCache = cache
	t.Cleanup(func() { globalMetaCache = oldCache })

	insertMsg, result := newIdempotentRepackInsertMsg(16)
	partitionKeys := insertMsg.GetFieldsData()[1]
	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, []string{"partition_0", "partition_1"})
	require.NoError(t, err)
	require.Contains(t, hashValues, uint32(0))
	require.Contains(t, hashValues, uint32(1))

	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "part", DataType: schemapb.DataType_Int64, IsPartitionKey: true},
			{FieldID: 3, Name: "payload", DataType: schemapb.DataType_VarChar},
		},
	}
	it := &insertTask{idempotencyEnabled: true, idempotencyKey: "key-partition", result: result}

	msgs, err := repackInsertDataWithPartitionKeyForStreamingService(
		context.Background(),
		[]string{"ch"},
		insertMsg,
		result,
		partitionKeys,
		nil,
		schema,
		0,
		it.idempotentInsertHeaderDecorator(),
	)
	require.NoError(t, err)
	require.Greater(t, len(msgs), 1)

	partitionIDs := typeutil.NewSet[int64]()
	for _, raw := range msgs {
		header := message.MustAsMutableInsertMessageV1(raw).Header()
		require.Len(t, header.GetPartitions(), 1)
		partitionIDs.Insert(header.GetPartitions()[0].GetPartitionId())
	}
	assert.ElementsMatch(t, []int64{300, 301}, partitionIDs.Collect())
	assert.ElementsMatch(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		collectIdempotentRepackOffsets(t, msgs, "ch", "key-partition"))
}

func newIdempotentRepackInsertMsg(rows int) (*msgstream.InsertMsg, *milvuspb.MutationResult) {
	pks := make([]int64, 0, rows)
	partitionKeys := make([]int64, 0, rows)
	payloads := make([]string, 0, rows)
	rowIDs := make([]int64, 0, rows)
	timestamps := make([]uint64, 0, rows)
	for i := 0; i < rows; i++ {
		pks = append(pks, int64(i+1))
		partitionKeys = append(partitionKeys, int64(i))
		payloads = append(payloads, "payload")
		rowIDs = append(rowIDs, int64(1000+i))
		timestamps = append(timestamps, uint64(2000+i))
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{Ctx: context.Background()},
		InsertRequest: &msgpb.InsertRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 1},
			CollectionID:   100,
			DbName:         "db",
			CollectionName: "coll",
			PartitionName:  "_default",
			NumRows:        uint64(rows),
			Version:        msgpb.InsertDataVersion_ColumnBased,
			FieldsData: []*schemapb.FieldData{
				int64FieldData("pk", 1, pks),
				int64FieldData("part", 2, partitionKeys),
				stringFieldData(3, "payload", payloads),
			},
			RowIDs:     rowIDs,
			Timestamps: timestamps,
		},
	}
	result := &milvuspb.MutationResult{
		IDs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
		},
	}
	return insertMsg, result
}

func stringFieldData(fieldID int64, name string, values []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name,
		FieldId:   fieldID,
		Type:      schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: values},
				},
			},
		},
	}
}

func collectIdempotentRepackOffsets(t *testing.T, msgs []message.MutableMessage, vchannel string, key string) []uint32 {
	t.Helper()
	offsets := make([]uint32, 0)
	for _, raw := range msgs {
		require.Equal(t, vchannel, raw.VChannel())
		msg := message.MustAsMutableInsertMessageV1(raw)
		header := msg.Header()
		require.Equal(t, key, header.GetIdempotencyKey())
		extra, ok := message.IdempotentInsertResultFromInsertHeader(header)
		require.True(t, ok)
		body := msg.MustBody()
		require.Equal(t, body.GetNumRows(), uint64(len(extra.GetRowOffsets())))
		require.Equal(t, body.GetNumRows(), uint64(len(extra.GetIds().GetIntId().GetData())))
		offsets = append(offsets, extra.GetRowOffsets()...)
	}
	return offsets
}

func TestInsertTaskPreExecuteTextRequiresStorageV3(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	})

	oldCache := globalMetaCache
	t.Cleanup(func() {
		globalMetaCache = oldCache
	})

	const (
		dbName         = "db"
		collectionName = "text_collection"
	)
	schema := mustNewSchemaInfo(newTextSchemaForStorageV3Test(collectionName))
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, dbName, collectionName).Return(int64(100), nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, dbName, collectionName, int64(100)).Return(&collectionInfo{}, nil)
	cache.EXPECT().GetCollectionSchema(mock.Anything, dbName, collectionName).Return(schema, nil)
	globalMetaCache = cache

	task := &insertTask{
		ctx: context.Background(),
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
				DbName:         dbName,
				CollectionName: collectionName,
				NumRows:        1,
			},
		},
	}

	err := task.PreExecute(context.Background())
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "TEXT field requires StorageV3")
}

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
	geometryFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Geometry}

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
				geometryFieldSchema,
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
		newScalarFieldData(geometryFieldSchema, "Geometry", numRows),
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

func TestInsertTask_KeepUserPK_WhenAllowInsertAutoIDTrue(t *testing.T) {
	paramtable.Init()
	// run auto-id path with field count check; allow user to pass PK
	Params.Save(Params.ProxyCfg.SkipAutoIDCheck.Key, "false")
	defer Params.Reset(Params.ProxyCfg.SkipAutoIDCheck.Key)

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

	nb := 5
	userIDs := []int64{101, 102, 103, 104, 105}

	collectionName := "TestInsertTask_KeepUserPK"
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.AllowInsertAutoIDKey, Value: "true"},
		},
	}

	pkFieldData := &schemapb.FieldData{
		FieldName: "id",
		FieldId:   100,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: userIDs},
				},
			},
		},
	}

	task := insertTask{
		ctx: context.Background(),
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				CollectionName: collectionName,
				DbName:         "test_db",
				PartitionName:  "_default",
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    msgpb.InsertDataVersion_ColumnBased,
				FieldsData: []*schemapb.FieldData{pkFieldData},
				NumRows:    uint64(nb),
			},
		},
		idAllocator: idAllocator,
	}

	info := mustNewSchemaInfo(schema)
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

	cache.On("GetCollectionInfo",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(&collectionInfo{schema: info}, nil)

	globalMetaCache = cache

	err = task.PreExecute(context.Background())
	assert.NoError(t, err)

	ids := task.result.IDs
	if ids.GetIntId() == nil {
		t.Fatalf("expected int IDs, got nil")
	}
	got := ids.GetIntId().GetData()

	assert.Equal(t, userIDs, got)
}

func TestInsertTask_Function(t *testing.T) {
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

	info := mustNewSchemaInfo(schema)
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
			schema: mustNewSchemaInfo(&schemapb.CollectionSchema{
				Name: "fooooo",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			}),
		}, nil)
		err := it.PreExecute(ctx)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
	})
}

func TestInsertTask_Namespace(t *testing.T) {
	paramtable.Init()
	cache := NewMockCache(t)
	globalMetaCache = cache
	cache.On("GetDatabaseInfo",
		mock.Anything,
		mock.Anything,
	).Return(&databaseInfo{properties: []*commonpb.KeyValuePair{}}, nil).Maybe()
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Maybe()
	ctx := context.Background()
	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Status(nil),
		ID:     11198,
		Count:  100,
	}, nil).Maybe()
	idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
	idAllocator.Start()
	defer idAllocator.Close()
	assert.NoError(t, err)

	schemaWithNamespaceEnabled := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: common.NamespaceFieldName, DataType: schemapb.DataType_VarChar, IsPartitionKey: true, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "100"},
			}},
		},
		EnableNamespace: true,
	}

	schemaWithNamespaceDisabled := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		},
	}

	t.Run("test insert with namespace enabled", func(t *testing.T) {
		cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Unset()
		cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Unset()
		cache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Unset()
		cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			schema: mustNewSchemaInfo(schemaWithNamespaceEnabled),
		}, nil).Maybe()
		cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(mustNewSchemaInfo(schemaWithNamespaceEnabled), nil).Maybe()
		cache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			name:                "p1",
			partitionID:         10,
			createdTimestamp:    10001,
			createdUtcTimestamp: 10002,
		}, nil).Maybe()
		namespace := "test"
		it := insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: "test",
					Namespace:      &namespace,
					NumRows:        100,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},
			schema:      schemaWithNamespaceEnabled,
			idAllocator: idAllocator,
		}
		err := it.PreExecute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(101), it.insertMsg.FieldsData[0].FieldId)

		// namespace data is not set
		it = insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: "test",
					NumRows:        100,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},
			idAllocator: idAllocator,
		}
		err = it.PreExecute(context.Background())
		assert.Error(t, err)
	})

	t.Run("test insert with namespace disabled", func(t *testing.T) {
		cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Unset()
		cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Unset()
		cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
			schema: mustNewSchemaInfo(schemaWithNamespaceDisabled),
		}, nil).Maybe()
		cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(mustNewSchemaInfo(schemaWithNamespaceDisabled), nil).Maybe()
		cache.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&partitionInfo{
			name:                "p1",
			partitionID:         10,
			createdTimestamp:    10001,
			createdUtcTimestamp: 10002,
		}, nil).Maybe()
		it := insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: "test",
					NumRows:        100,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},
			schema:      schemaWithNamespaceDisabled,
			idAllocator: idAllocator,
		}
		err := it.PreExecute(context.Background())
		assert.NoError(t, err)

		// namespace data is set
		namespace := "test"
		it = insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: &msgpb.InsertRequest{
					CollectionName: "test",
					Namespace:      &namespace,
					NumRows:        100,
					Version:        msgpb.InsertDataVersion_ColumnBased,
				},
			},
			idAllocator: idAllocator,
		}
		err = it.PreExecute(context.Background())
		assert.Error(t, err)
	})
}
