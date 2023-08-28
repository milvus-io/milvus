package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

func TestInsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				NumRows: 0,
			},
		},
	}
	err = case1.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// fillFieldsDataBySchema was already checked by TestInsertTask_fillFieldsDataBySchema

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
		insertMsg: &BaseInsertTask{
			InsertRequest: msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    msgpb.InsertDataVersion_ColumnBased,
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
		chMgr := newMockChannelsMgr()
		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return channels, nil
		}
		it := insertTask{
			ctx: context.Background(),
			insertMsg: &msgstream.InsertMsg{
				InsertRequest: msgpb.InsertRequest{
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

		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return nil, fmt.Errorf("mock err")
		}
		// get channels again, should return task's pChannels, so getChannelsFunc should not invoke again
		resChannels = it.getChannels()
		assert.ElementsMatch(t, channels, resChannels)
	})
}
