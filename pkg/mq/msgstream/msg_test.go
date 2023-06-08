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

package msgstream

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestBaseMsg(t *testing.T) {
	ctx := context.Background()
	baseMsg := &BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: Timestamp(0),
		EndTimestamp:   Timestamp(1),
		HashValues:     []uint32{2},
		MsgPosition:    nil,
	}

	position := &MsgPosition{
		ChannelName: "test-channel",
		MsgID:       []byte{},
		MsgGroup:    "test-group",
		Timestamp:   0,
	}

	assert.Equal(t, Timestamp(0), baseMsg.BeginTs())
	assert.Equal(t, Timestamp(1), baseMsg.EndTs())
	assert.Equal(t, []uint32{2}, baseMsg.HashKeys())
	assert.Equal(t, (*MsgPosition)(nil), baseMsg.Position())

	baseMsg.SetPosition(position)
	assert.Equal(t, position, baseMsg.Position())
}

func Test_convertToByteArray(t *testing.T) {
	{
		bytes := []byte{1, 2, 3}
		byteArray, err := convertToByteArray(bytes)
		assert.Equal(t, bytes, byteArray)
		assert.NoError(t, err)
	}

	{
		bytes := 4
		byteArray, err := convertToByteArray(bytes)
		assert.Equal(t, ([]byte)(nil), byteArray)
		assert.Error(t, err)
	}
}

func generateBaseMsg() BaseMsg {
	ctx := context.Background()
	return BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: Timestamp(0),
		EndTimestamp:   Timestamp(1),
		HashValues:     []uint32{2},
		MsgPosition:    nil,
	}
}

func TestInsertMsg(t *testing.T) {
	insertMsg := &InsertMsg{
		BaseMsg: generateBaseMsg(),
		InsertRequest: msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},

			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
			SegmentID:      7,
			ShardName:      "test-channel",
			Timestamps:     []uint64{2, 1, 3},
			RowData:        []*commonpb.Blob{},
		},
	}

	assert.NotNil(t, insertMsg.TraceCtx())

	ctx := context.Background()
	insertMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, insertMsg.TraceCtx())

	assert.Equal(t, int64(1), insertMsg.ID())
	assert.Equal(t, commonpb.MsgType_Insert, insertMsg.Type())
	assert.Equal(t, int64(3), insertMsg.SourceID())

	bytes, err := insertMsg.Marshal(insertMsg)
	assert.NoError(t, err)

	tsMsg, err := insertMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	insertMsg2, ok := tsMsg.(*InsertMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), insertMsg2.ID())
	assert.Equal(t, commonpb.MsgType_Insert, insertMsg2.Type())
	assert.Equal(t, int64(3), insertMsg2.SourceID())
}

func TestInsertMsg_Unmarshal_IllegalParameter(t *testing.T) {
	insertMsg := &InsertMsg{}
	tsMsg, err := insertMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestInsertMsg_RowBasedFormat(t *testing.T) {
	msg := &InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			Version: msgpb.InsertDataVersion_RowBased,
		},
	}
	assert.True(t, msg.IsRowBased())
}

func TestInsertMsg_ColumnBasedFormat(t *testing.T) {
	msg := &InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			Version: msgpb.InsertDataVersion_ColumnBased,
		},
	}
	assert.True(t, msg.IsColumnBased())
}

func TestInsertMsg_NRows(t *testing.T) {
	msg1 := &InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			RowData: []*commonpb.Blob{
				{},
				{},
			},
			FieldsData: nil,
			Version:    msgpb.InsertDataVersion_RowBased,
		},
	}
	assert.Equal(t, uint64(2), msg1.NRows())
	msg2 := &InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			RowData: nil,
			FieldsData: []*schemapb.FieldData{
				{},
			},
			NumRows: 2,
			Version: msgpb.InsertDataVersion_ColumnBased,
		},
	}
	assert.Equal(t, uint64(2), msg2.NRows())
}

func TestInsertMsg_CheckAligned(t *testing.T) {
	msg1 := &InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			Timestamps: []uint64{1},
			RowIDs:     []int64{1},
			RowData: []*commonpb.Blob{
				{},
			},
			FieldsData: nil,
			Version:    msgpb.InsertDataVersion_RowBased,
		},
	}
	msg1.InsertRequest.NumRows = 1
	assert.NoError(t, msg1.CheckAligned())
	msg1.InsertRequest.RowData = nil
	msg1.InsertRequest.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1},
						},
					},
				},
			},
		},
	}

	msg1.Version = msgpb.InsertDataVersion_ColumnBased
	assert.NoError(t, msg1.CheckAligned())
}

func TestInsertMsg_IndexMsg(t *testing.T) {
	msg := &InsertMsg{
		BaseMsg: BaseMsg{
			BeginTimestamp: 1,
			EndTimestamp:   2,
		},
		InsertRequest: msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     3,
				Timestamp: 4,
				SourceID:  5,
			},
			DbID:           6,
			CollectionID:   7,
			PartitionID:    8,
			CollectionName: "test",
			PartitionName:  "test",
			SegmentID:      9,
			ShardName:      "test",
			Timestamps:     []uint64{10},
			RowIDs:         []int64{11},
			RowData: []*commonpb.Blob{
				{
					Value: []byte{1},
				},
			},
			Version: msgpb.InsertDataVersion_RowBased,
		},
	}
	indexMsg := msg.IndexMsg(0)
	assert.Equal(t, uint64(10), indexMsg.GetTimestamps()[0])
	assert.Equal(t, int64(11), indexMsg.GetRowIDs()[0])
	assert.Equal(t, []byte{1}, indexMsg.GetRowData()[0].Value)

	msg.Version = msgpb.InsertDataVersion_ColumnBased
	msg.FieldsData = []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "test",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1},
						},
					},
				},
			},
			FieldId: 0,
		},
	}
	indexMsg = msg.IndexMsg(0)
	assert.Equal(t, uint64(10), indexMsg.GetTimestamps()[0])
	assert.Equal(t, int64(11), indexMsg.GetRowIDs()[0])
	assert.Equal(t, int64(1), indexMsg.FieldsData[0].Field.(*schemapb.FieldData_Scalars).Scalars.Data.(*schemapb.ScalarField_LongData).LongData.Data[0])
}

func TestDeleteMsg(t *testing.T) {
	deleteMsg := &DeleteMsg{
		BaseMsg: generateBaseMsg(),
		DeleteRequest: msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Delete,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},

			CollectionName:   "test_collection",
			ShardName:        "test-channel",
			Timestamps:       []uint64{2, 1, 3},
			Int64PrimaryKeys: []int64{1, 2, 3},
			NumRows:          3,
		},
	}

	assert.NotNil(t, deleteMsg.TraceCtx())

	ctx := context.Background()
	deleteMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, deleteMsg.TraceCtx())

	assert.Equal(t, int64(1), deleteMsg.ID())
	assert.Equal(t, commonpb.MsgType_Delete, deleteMsg.Type())
	assert.Equal(t, int64(3), deleteMsg.SourceID())

	bytes, err := deleteMsg.Marshal(deleteMsg)
	assert.NoError(t, err)

	tsMsg, err := deleteMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	deleteMsg2, ok := tsMsg.(*DeleteMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), deleteMsg2.ID())
	assert.Equal(t, commonpb.MsgType_Delete, deleteMsg2.Type())
	assert.Equal(t, int64(3), deleteMsg2.SourceID())
}

func TestDeleteMsg_Unmarshal_IllegalParameter(t *testing.T) {
	deleteMsg := &DeleteMsg{}
	tsMsg, err := deleteMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestTimeTickMsg(t *testing.T) {
	timeTickMsg := &TimeTickMsg{
		BaseMsg: generateBaseMsg(),
		TimeTickMsg: msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
		},
	}

	assert.NotNil(t, timeTickMsg.TraceCtx())

	ctx := context.Background()
	timeTickMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, timeTickMsg.TraceCtx())

	assert.Equal(t, int64(1), timeTickMsg.ID())
	assert.Equal(t, commonpb.MsgType_TimeTick, timeTickMsg.Type())
	assert.Equal(t, int64(3), timeTickMsg.SourceID())

	bytes, err := timeTickMsg.Marshal(timeTickMsg)
	assert.NoError(t, err)

	tsMsg, err := timeTickMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	timeTickMsg2, ok := tsMsg.(*TimeTickMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), timeTickMsg2.ID())
	assert.Equal(t, commonpb.MsgType_TimeTick, timeTickMsg2.Type())
	assert.Equal(t, int64(3), timeTickMsg2.SourceID())
}

func TestTimeTickMsg_Unmarshal_IllegalParameter(t *testing.T) {
	timeTickMsg := &TimeTickMsg{}
	tsMsg, err := timeTickMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestCreateCollectionMsg(t *testing.T) {
	createCollectionMsg := &CreateCollectionMsg{
		BaseMsg: generateBaseMsg(),
		CreateCollectionRequest: msgpb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:               "test_db",
			CollectionName:       "test_collection",
			PartitionName:        "test_partition",
			DbID:                 4,
			CollectionID:         5,
			PartitionID:          6,
			Schema:               []byte{},
			VirtualChannelNames:  []string{},
			PhysicalChannelNames: []string{},
		},
	}

	assert.NotNil(t, createCollectionMsg.TraceCtx())

	ctx := context.Background()
	createCollectionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, createCollectionMsg.TraceCtx())

	assert.Equal(t, int64(1), createCollectionMsg.ID())
	assert.Equal(t, commonpb.MsgType_CreateCollection, createCollectionMsg.Type())
	assert.Equal(t, int64(3), createCollectionMsg.SourceID())

	bytes, err := createCollectionMsg.Marshal(createCollectionMsg)
	assert.NoError(t, err)

	tsMsg, err := createCollectionMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	createCollectionMsg2, ok := tsMsg.(*CreateCollectionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), createCollectionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_CreateCollection, createCollectionMsg2.Type())
	assert.Equal(t, int64(3), createCollectionMsg2.SourceID())
}

func TestCreateCollectionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	createCollectionMsg := &CreateCollectionMsg{}
	tsMsg, err := createCollectionMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestDropCollectionMsg(t *testing.T) {
	dropCollectionMsg := &DropCollectionMsg{
		BaseMsg: generateBaseMsg(),
		DropCollectionRequest: msgpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			DbID:           4,
			CollectionID:   5,
		},
	}

	assert.NotNil(t, dropCollectionMsg.TraceCtx())

	ctx := context.Background()
	dropCollectionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, dropCollectionMsg.TraceCtx())

	assert.Equal(t, int64(1), dropCollectionMsg.ID())
	assert.Equal(t, commonpb.MsgType_DropCollection, dropCollectionMsg.Type())
	assert.Equal(t, int64(3), dropCollectionMsg.SourceID())

	bytes, err := dropCollectionMsg.Marshal(dropCollectionMsg)
	assert.NoError(t, err)

	tsMsg, err := dropCollectionMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	dropCollectionMsg2, ok := tsMsg.(*DropCollectionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), dropCollectionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_DropCollection, dropCollectionMsg2.Type())
	assert.Equal(t, int64(3), dropCollectionMsg2.SourceID())
}

func TestDropCollectionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	dropCollectionMsg := &DropCollectionMsg{}
	tsMsg, err := dropCollectionMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestCreatePartitionMsg(t *testing.T) {
	createPartitionMsg := &CreatePartitionMsg{
		BaseMsg: generateBaseMsg(),
		CreatePartitionRequest: msgpb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
		},
	}

	assert.NotNil(t, createPartitionMsg.TraceCtx())

	ctx := context.Background()
	createPartitionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, createPartitionMsg.TraceCtx())

	assert.Equal(t, int64(1), createPartitionMsg.ID())
	assert.Equal(t, commonpb.MsgType_CreatePartition, createPartitionMsg.Type())
	assert.Equal(t, int64(3), createPartitionMsg.SourceID())

	bytes, err := createPartitionMsg.Marshal(createPartitionMsg)
	assert.NoError(t, err)

	tsMsg, err := createPartitionMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	createPartitionMsg2, ok := tsMsg.(*CreatePartitionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), createPartitionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_CreatePartition, createPartitionMsg2.Type())
	assert.Equal(t, int64(3), createPartitionMsg2.SourceID())
}

func TestCreatePartitionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	createPartitionMsg := &CreatePartitionMsg{}
	tsMsg, err := createPartitionMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestDropPartitionMsg(t *testing.T) {
	dropPartitionMsg := &DropPartitionMsg{
		BaseMsg: generateBaseMsg(),
		DropPartitionRequest: msgpb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			DbName:         "test_db",
			CollectionName: "test_collection",
			PartitionName:  "test_partition",
			DbID:           4,
			CollectionID:   5,
			PartitionID:    6,
		},
	}

	assert.NotNil(t, dropPartitionMsg.TraceCtx())

	ctx := context.Background()
	dropPartitionMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, dropPartitionMsg.TraceCtx())

	assert.Equal(t, int64(1), dropPartitionMsg.ID())
	assert.Equal(t, commonpb.MsgType_DropPartition, dropPartitionMsg.Type())
	assert.Equal(t, int64(3), dropPartitionMsg.SourceID())

	bytes, err := dropPartitionMsg.Marshal(dropPartitionMsg)
	assert.NoError(t, err)

	tsMsg, err := dropPartitionMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	dropPartitionMsg2, ok := tsMsg.(*DropPartitionMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), dropPartitionMsg2.ID())
	assert.Equal(t, commonpb.MsgType_DropPartition, dropPartitionMsg2.Type())
	assert.Equal(t, int64(3), dropPartitionMsg2.SourceID())
}

func TestDropPartitionMsg_Unmarshal_IllegalParameter(t *testing.T) {
	dropPartitionMsg := &DropPartitionMsg{}
	tsMsg, err := dropPartitionMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}

func TestDataNodeTtMsg(t *testing.T) {
	dataNodeTtMsg := &DataNodeTtMsg{
		BaseMsg: generateBaseMsg(),
		DataNodeTtMsg: msgpb.DataNodeTtMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DataNodeTt,
				MsgID:     1,
				Timestamp: 2,
				SourceID:  3,
			},
			ChannelName: "test-channel",
			Timestamp:   4,
		},
	}

	assert.NotNil(t, dataNodeTtMsg.TraceCtx())

	ctx := context.Background()
	dataNodeTtMsg.SetTraceCtx(ctx)
	assert.Equal(t, ctx, dataNodeTtMsg.TraceCtx())

	assert.Equal(t, int64(1), dataNodeTtMsg.ID())
	assert.Equal(t, commonpb.MsgType_DataNodeTt, dataNodeTtMsg.Type())
	assert.Equal(t, int64(3), dataNodeTtMsg.SourceID())

	bytes, err := dataNodeTtMsg.Marshal(dataNodeTtMsg)
	assert.NoError(t, err)

	tsMsg, err := dataNodeTtMsg.Unmarshal(bytes)
	assert.NoError(t, err)

	dataNodeTtMsg2, ok := tsMsg.(*DataNodeTtMsg)
	assert.True(t, ok)
	assert.Equal(t, int64(1), dataNodeTtMsg2.ID())
	assert.Equal(t, commonpb.MsgType_DataNodeTt, dataNodeTtMsg2.Type())
	assert.Equal(t, int64(3), dataNodeTtMsg2.SourceID())
}

func TestDataNodeTtMsg_Unmarshal_IllegalParameter(t *testing.T) {
	dataNodeTtMsg := &DataNodeTtMsg{}
	tsMsg, err := dataNodeTtMsg.Unmarshal(10)
	assert.Error(t, err)
	assert.Nil(t, tsMsg)
}
