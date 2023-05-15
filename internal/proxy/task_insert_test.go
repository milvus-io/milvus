package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
)

func TestInsertTask_checkLengthOfFieldsData(t *testing.T) {
	var err error

	case1 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		},
		BaseInsertTask: BaseInsertTask{
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "TestInsertTask_checkLengthOfFieldsData",
				CollectionName: "TestInsertTask_checkLengthOfFieldsData",
				PartitionName:  "TestInsertTask_checkLengthOfFieldsData",
			},
		},
	}

	err = case1.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has two fields, neither of them are autoID
	case2 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// case2.BaseInsertTask = BaseInsertTask{
	// 	InsertRequest: internalpb.InsertRequest{
	// 		Base: &commonpb.MsgBase{
	// 			MsgType:  commonpb.MsgType_Insert,
	// 			MsgID:    0,
	// 			SourceID: Params.ProxyCfg.GetNodeID(),
	// 		},
	// 	},
	// }
	err = case2.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// the num of passed fields is less than needed
	case2.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
		},
	}
	err = case2.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// satisfied
	case2.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
		},
		{
			Type: schemapb.DataType_Int64,
		},
	}
	err = case2.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has two field, one of them are autoID
	case3 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   true,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// case3.req = &milvuspb.InsertRequest{}
	err = case3.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// satisfied
	case3.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
		},
	}
	err = case3.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has one field which is autoID
	case4 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   true,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// satisfied
	// case4.req = &milvuspb.InsertRequest{}
	err = case4.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)
}

func TestInsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := insertTask{
		BaseInsertTask: BaseInsertTask{
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				NumRows: 0,
			},
		},
	}
	err = case1.CheckAligned()
	assert.NoError(t, err)

	// checkLengthOfFieldsData was already checked by TestInsertTask_checkLengthOfFieldsData

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
	case2 := insertTask{
		BaseInsertTask: BaseInsertTask{
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    internalpb.InsertDataVersion_ColumnBased,
				RowIDs:     generateInt64Array(numRows),
				Timestamps: generateUint64Array(numRows),
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
				varCharFieldSchema,
			},
		},
	}

	// satisfied
	case2.NumRows = uint64(numRows)
	case2.FieldsData = []*schemapb.FieldData{
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
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less bool data
	case2.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more bool data
	case2.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int8 data
	case2.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int8 data
	case2.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int16 data
	case2.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int16 data
	case2.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int32 data
	case2.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int32 data
	case2.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int64 data
	case2.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int64 data
	case2.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less float data
	case2.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more float data
	case2.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, nil, err)

	// less double data
	case2.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, nil, err)

	// less float vectors
	case2.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more float vectors
	case2.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less binary vectors
	case2.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more binary vectors
	case2.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less double data
	case2.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)
}

func TestInsertTask(t *testing.T) {
	t.Run("test getChannels", func(t *testing.T) {
		collectionID := UniqueID(0)
		collectionName := "col-0"
		channels := []pChan{"mock-chan-0", "mock-chan-1"}
		cache := newMockCache()
		cache.setGetIDFunc(func(ctx context.Context, collectionName string) (UniqueID, error) {
			return collectionID, nil
		})
		globalMetaCache = cache
		chMgr := newMockChannelsMgr()
		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return channels, nil
		}
		it := insertTask{
			ctx: context.Background(),
			BaseInsertTask: msgstream.InsertMsg{
				InsertRequest: internalpb.InsertRequest{
					CollectionName: collectionName,
				},
			},
			chMgr: chMgr,
		}
		resChannels, err := it.getChannels()
		assert.NoError(t, err)
		assert.ElementsMatch(t, channels, resChannels)
		assert.ElementsMatch(t, channels, it.pChannels)

		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return nil, fmt.Errorf("mock err")
		}
		// get channels again, should return task's pChannels, so getChannelsFunc should not invoke again
		resChannels, err = it.getChannels()
		assert.NoError(t, err)
		assert.ElementsMatch(t, channels, resChannels)
	})
}

func TestAssignPartitionsByKey(t *testing.T) {
	Params.InitOnce()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()
	err := InitMetaCache(ctx, rc, nil, nil)
	assert.NoError(t, err)

	shardsNum := common.DefaultShardsNum
	prefix := "TestAssignPartitionsByKey"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64_field", false)
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key_field",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	schema.Fields = append(schema.Fields, partitionKeyField)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgID:     UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
				Timestamp: Timestamp(time.Now().UnixNano()),
			},
			DbName:         "",
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
			NumPartitions:  common.DefaultPartitionsWithPartitionKey,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}
	err = task.PreExecute(ctx)
	assert.NoError(t, err)
	err = task.Execute(ctx)
	assert.NoError(t, err)

	nb := 100
	partitionKeyFieldData := generateFieldData(schemapb.DataType_Int64, "partition_key_field", nb)
	rowOffset := []int{10, 20, 30, 40}
	it := &insertTask{
		BaseInsertTask: BaseInsertTask{
			BaseMsg: msgstream.BaseMsg{},
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Insert,
					MsgID:    0,
					SourceID: Params.ProxyCfg.GetNodeID(),
				},
				CollectionName: collectionName,
				NumRows:        uint64(nb),
				Version:        internalpb.InsertDataVersion_ColumnBased,
			},
		},
	}
	partition2RowOffsets, err := it.assignPartitionsByKey(ctx, rowOffset, partitionKeyFieldData)
	assert.NoError(t, err)
	for _, offsets := range partition2RowOffsets {
		for _, offset := range offsets {
			assert.Contains(t, rowOffset, offset)
		}
	}
}
