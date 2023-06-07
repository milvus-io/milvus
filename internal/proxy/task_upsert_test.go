// // Licensed to the LF AI & Data foundation under one
// // or more contributor license agreements. See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership. The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License. You may obtain a copy of the License at
// //
// //	http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
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
				InsertRequest: msgpb.InsertRequest{},
			},
		},
	}
	case1.upsertMsg.InsertMsg.InsertRequest = msgpb.InsertRequest{
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

	// fillFieldsDataBySchema was already checked by TestUpsertTask_fillFieldsDataBySchema

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
	case2 := upsertTask{
		req: &milvuspb.UpsertRequest{
			NumRows:    uint32(numRows),
			FieldsData: []*schemapb.FieldData{},
		},
		rowIDs:     generateInt64Array(numRows),
		timestamps: generateUint64Array(numRows),
		schema: &schemapb.CollectionSchema{
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
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: msgpb.InsertRequest{},
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
	case2.upsertMsg.InsertMsg.InsertRequest = msgpb.InsertRequest{
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
	assert.NoError(t, err)

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

		chMgr := newMockChannelsMgr()
		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return channels, nil
		}
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

		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return nil, fmt.Errorf("mock err")
		}
		// get channels again, should return task's pChannels, so getChannelsFunc should not invoke again
		resChannels = ut.getChannels()
		assert.ElementsMatch(t, channels, resChannels)
	})
}
