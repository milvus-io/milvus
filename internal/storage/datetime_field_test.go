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

package storage

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
)

func TestNewFieldData_DateAndTime(t *testing.T) {
	dateField := &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "event_date",
		DataType: schemapb.DataType_Date,
	}
	dateData, err := NewFieldData(schemapb.DataType_Date, dateField, 4)
	require.NoError(t, err)
	require.IsType(t, &DateFieldData{}, dateData)
	assert.Equal(t, schemapb.DataType_Date, dateData.GetDataType())

	timeField := &schemapb.FieldSchema{
		FieldID:  201,
		Name:     "event_time",
		DataType: schemapb.DataType_Time,
	}
	timeData, err := NewFieldData(schemapb.DataType_Time, timeField, 4)
	require.NoError(t, err)
	require.IsType(t, &TimeFieldData{}, timeData)
	assert.Equal(t, schemapb.DataType_Time, timeData.GetDataType())
}

func TestColumnBasedInsertMsgToInsertData_DateTimeRoundtrip(t *testing.T) {
	const (
		dateFieldID = int64(200)
		timeFieldID = int64(201)
	)
	dateValues := []int32{19000, 19001}
	timeValues := []int64{3600000000, 7200000000}

	schema := &schemapb.CollectionSchema{
		Name: "datetime_schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: dateFieldID, Name: "event_date", DataType: schemapb.DataType_Date},
			{FieldID: timeFieldID, Name: "event_time", DataType: schemapb.DataType_Time},
		},
	}

	msg := &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			NumRows:    uint64(len(dateValues)),
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     []int64{1, 2},
			Timestamps: []uint64{100, 101},
			FieldsData: []*schemapb.FieldData{
				{
					Type:    schemapb.DataType_Date,
					FieldId: dateFieldID,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_DateData{
								DateData: &schemapb.DateArray{Data: dateValues},
							},
						},
					},
				},
				{
					Type:    schemapb.DataType_Time,
					FieldId: timeFieldID,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_TimeData{
								TimeData: &schemapb.TimeArray{Data: timeValues},
							},
						},
					},
				},
			},
		},
	}

	insertData, err := ColumnBasedInsertMsgToInsertData(msg, schema)
	require.NoError(t, err)

	dateFieldData, ok := insertData.Data[dateFieldID].(*DateFieldData)
	require.True(t, ok)
	assert.Equal(t, dateValues, dateFieldData.Data)

	timeFieldData, ok := insertData.Data[timeFieldID].(*TimeFieldData)
	require.True(t, ok)
	assert.Equal(t, timeValues, timeFieldData.Data)

	record, err := TransferInsertDataToInsertRecord(insertData)
	require.NoError(t, err)

	var gotDate []int32
	var gotTime []int64
	for _, fieldData := range record.GetFieldsData() {
		switch fieldData.GetFieldId() {
		case dateFieldID:
			gotDate = fieldData.GetScalars().GetDateData().GetData()
		case timeFieldID:
			gotTime = fieldData.GetScalars().GetTimeData().GetData()
		}
	}
	assert.Equal(t, dateValues, gotDate)
	assert.Equal(t, timeValues, gotTime)
}

func TestMilvusDataTypeToArrowType_DateTime(t *testing.T) {
	dateType := MilvusDataTypeToArrowType(schemapb.DataType_Date, 0)
	assert.Equal(t, reflect.TypeOf(&arrow.Int32Type{}), reflect.TypeOf(dateType))

	timeType := MilvusDataTypeToArrowType(schemapb.DataType_Time, 0)
	assert.Equal(t, reflect.TypeOf(&arrow.Int64Type{}), reflect.TypeOf(timeType))
}

func TestSerDeDateTime(t *testing.T) {
	tests := []struct {
		name string
		dt   schemapb.DataType
		v    any
		want any
	}{
		{"date", schemapb.DataType_Date, int32(19000), int32(19000)},
		{"date null", schemapb.DataType_Date, nil, nil},
		{"time", schemapb.DataType_Time, int64(3600000000), int64(3600000000)},
		{"time null", schemapb.DataType_Time, nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := serdeMap[tt.dt]
			builder := array.NewBuilder(memory.DefaultAllocator, entry.arrowType(0, schemapb.DataType_None))
			err := entry.serialize(builder, tt.v, schemapb.DataType_None)
			require.NoError(t, err)

			arr := builder.NewArray()
			defer arr.Release()
			got, err := entry.deserialize(arr, 0, schemapb.DataType_None, 0, false)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPayloadWriterReader_DateTime(t *testing.T) {
	t.Run("date", func(t *testing.T) {
		writer, err := NewPayloadWriter(schemapb.DataType_Date)
		require.NoError(t, err)
		defer writer.ReleasePayloadWriter()
		require.NoError(t, writer.AddDateToPayload([]int32{19000, 19001}, nil))
		require.NoError(t, writer.FinishPayloadWriter())
		buf, err := writer.GetPayloadBufferFromWriter()
		require.NoError(t, err)

		reader, err := NewPayloadReader(schemapb.DataType_Date, buf, false)
		require.NoError(t, err)
		defer reader.ReleasePayloadReader()

		val, _, err := reader.GetDateFromPayload()
		require.NoError(t, err)
		assert.Equal(t, []int32{19000, 19001}, val)
	})

	t.Run("time", func(t *testing.T) {
		writer, err := NewPayloadWriter(schemapb.DataType_Time)
		require.NoError(t, err)
		defer writer.ReleasePayloadWriter()
		require.NoError(t, writer.AddTimeToPayload([]int64{1000, 2000}, nil))
		require.NoError(t, writer.FinishPayloadWriter())
		buf, err := writer.GetPayloadBufferFromWriter()
		require.NoError(t, err)

		reader, err := NewPayloadReader(schemapb.DataType_Time, buf, false)
		require.NoError(t, err)
		defer reader.ReleasePayloadReader()

		val, _, err := reader.GetTimeFromPayload()
		require.NoError(t, err)
		assert.Equal(t, []int64{1000, 2000}, val)
	})
}
