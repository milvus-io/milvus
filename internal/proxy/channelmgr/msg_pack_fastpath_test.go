// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package channelmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGenInsertMsgsByPartitionContiguousFastPath(t *testing.T) {
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, "1048576"))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)

	longData := []int64{10, 20, 30, 40}
	jsonData := [][]byte{[]byte(`{"row":0}`), []byte(`{"row":1}`), []byte(`{"row":2}`), []byte(`{"row":3}`)}
	validData := []bool{true, false, true, true}
	floatData := []float32{1, 2, 3, 4, 5, 6}
	fieldsData := []*schemapb.FieldData{
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 100,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: longData}},
			}},
		},
		{
			Type:      schemapb.DataType_JSON,
			FieldId:   101,
			IsDynamic: true,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsonData}},
			}},
		},
		{
			Type:    schemapb.DataType_FloatVector,
			FieldId: 102,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				ValidData: validData,
				Dim:       2,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: floatData},
				},
			}},
		},
	}
	hashValues := []uint32{0, 0, 0, 0}
	timestamps := []uint64{100, 101, 102, 103}
	rowIDs := []int64{200, 201, 202, 203}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:        context.Background(),
			HashValues: hashValues,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base:       &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: paramtable.GetNodeID()},
			NumRows:    4,
			FieldsData: fieldsData,
			Timestamps: timestamps,
			RowIDs:     rowIDs,
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}

	msgs, err := GenInsertMsgsByPartition(
		context.Background(), 0, 1, "test_partition", []int{1, 2, 3}, "test_channel", insertMsg, message.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	got := msgs[0].(*msgstream.InsertMsg)

	expectedFields := make([]*schemapb.FieldData, len(fieldsData))
	idxComputer := typeutil.NewFieldDataIdxComputer(fieldsData)
	for _, row := range []int64{1, 2, 3} {
		fieldIdxs := idxComputer.Compute(row)
		typeutil.AppendFieldData(expectedFields, fieldsData, row, fieldIdxs...)
	}
	for i := range expectedFields {
		assert.True(t, proto.Equal(expectedFields[i], got.FieldsData[i]))
	}

	assert.True(t, &longData[1] == &got.FieldsData[0].GetScalars().GetLongData().Data[0])
	assert.True(t, &jsonData[1] == &got.FieldsData[1].GetScalars().GetJsonData().Data[0])
	assert.True(t, &floatData[2] == &got.FieldsData[2].GetVectors().GetFloatVector().Data[0])
	assert.Nil(t, got.FieldsData[2].GetValidData())
	assert.True(t, &validData[1] == &got.FieldsData[2].GetVectors().ValidData[0])
	assert.True(t, &hashValues[1] == &got.HashValues[0])
	assert.True(t, &timestamps[1] == &got.Timestamps[0])
	assert.True(t, &rowIDs[1] == &got.RowIDs[0])

	serialized, err := message.NewInsertMessageBuilderV1().
		WithVChannel("test_channel").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(got.InsertRequest).
		BuildMutable()
	require.NoError(t, err)

	longData[1] = 999
	jsonData[1][0] = 'X'
	floatData[2] = 999
	validData[1] = true
	timestamps[1] = 999
	rowIDs[1] = 999

	body := message.MustAsMutableInsertMessageV1(serialized).MustBody()
	assert.Equal(t, []int64{20, 30, 40}, body.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []byte(`{"row":1}`), body.GetFieldsData()[1].GetScalars().GetJsonData().GetData()[0])
	assert.Equal(t, []float32{3, 4, 5, 6}, body.GetFieldsData()[2].GetVectors().GetFloatVector().GetData())
	assert.Equal(t, []bool{false, true, true}, typeutil.GetFieldDataValidData(body.GetFieldsData()[2]))
	assert.Equal(t, []uint64{101, 102, 103}, body.GetTimestamps())
	assert.Equal(t, []int64{201, 202, 203}, body.GetRowIDs())
}

func TestGenInsertMsgsByPartitionNonContiguousFallback(t *testing.T) {
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, "1048576"))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)

	floatData := []float32{1, 2, 3, 4, 5, 6}
	fieldData := &schemapb.FieldData{
		Type:    schemapb.DataType_FloatVector,
		FieldId: 100,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: floatData},
			},
		}},
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{Ctx: context.Background(), HashValues: []uint32{0, 0, 0}},
		InsertRequest: &msgpb.InsertRequest{
			Base:       &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: paramtable.GetNodeID()},
			NumRows:    3,
			FieldsData: []*schemapb.FieldData{fieldData},
			Timestamps: []uint64{100, 101, 102},
			RowIDs:     []int64{200, 201, 202},
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}

	msgs, err := GenInsertMsgsByPartition(
		context.Background(), 0, 1, "test_partition", []int{0, 2}, "test_channel", insertMsg, message.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	got := msgs[0].(*msgstream.InsertMsg)
	assert.Equal(t, []float32{1, 2, 5, 6}, got.FieldsData[0].GetVectors().GetFloatVector().GetData())
	assert.False(t, &floatData[0] == &got.FieldsData[0].GetVectors().GetFloatVector().Data[0])
}

func TestGenInsertMsgsByPartitionContiguousFastPathAfterSplit(t *testing.T) {
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().PulsarCfg.MaxMessageSize.Key, "17"))
	defer paramtable.Get().Reset(paramtable.Get().PulsarCfg.MaxMessageSize.Key)

	longData := []int64{10, 20, 30, 40}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{Ctx: context.Background(), HashValues: []uint32{0, 0, 0, 0}},
		InsertRequest: &msgpb.InsertRequest{
			Base:    &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: paramtable.GetNodeID()},
			NumRows: 4,
			FieldsData: []*schemapb.FieldData{
				{
					Type:    schemapb.DataType_Int64,
					FieldId: 100,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: longData}},
					}},
				},
			},
			Timestamps: []uint64{100, 101, 102, 103},
			RowIDs:     []int64{200, 201, 202, 203},
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}

	msgs, err := GenInsertMsgsByPartition(
		context.Background(), 0, 1, "test_partition", []int{0, 1, 2, 3}, "test_channel", insertMsg, message.WALNamePulsar,
	)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	first := msgs[0].(*msgstream.InsertMsg)
	second := msgs[1].(*msgstream.InsertMsg)
	assert.Equal(t, []int64{10, 20}, first.FieldsData[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []int64{30, 40}, second.FieldsData[0].GetScalars().GetLongData().GetData())
	assert.True(t, &longData[0] == &first.FieldsData[0].GetScalars().GetLongData().Data[0])
	assert.True(t, &longData[2] == &second.FieldsData[0].GetScalars().GetLongData().Data[0])
}
