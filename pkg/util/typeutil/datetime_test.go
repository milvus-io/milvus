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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestIsDateAndTimeType(t *testing.T) {
	assert.True(t, IsDateType(schemapb.DataType_Date))
	assert.False(t, IsDateType(schemapb.DataType_Time))
	assert.False(t, IsDateType(schemapb.DataType_Timestamptz))
	assert.True(t, IsTimeType(schemapb.DataType_Time))
	assert.False(t, IsTimeType(schemapb.DataType_Date))
	assert.True(t, IsPrimitiveType(schemapb.DataType_Date))
	assert.True(t, IsPrimitiveType(schemapb.DataType_Time))
	assert.False(t, IsIntegerType(schemapb.DataType_Date))
	assert.False(t, IsIntegerType(schemapb.DataType_Time))
	assert.False(t, IsArithmetic(schemapb.DataType_Date))
	assert.False(t, IsArithmetic(schemapb.DataType_Time))
}

func TestDateTimeFieldDataSizeAndEmpty(t *testing.T) {
	dateField := &schemapb.FieldSchema{FieldID: 101, Name: "d", DataType: schemapb.DataType_Date}
	emptyDate, err := GenEmptyFieldData(dateField)
	require.NoError(t, err)
	assert.NotNil(t, emptyDate.GetScalars().GetDateData())

	timeField := &schemapb.FieldSchema{FieldID: 102, Name: "t", DataType: schemapb.DataType_Time}
	emptyTime, err := GenEmptyFieldData(timeField)
	require.NoError(t, err)
	assert.NotNil(t, emptyTime.GetScalars().GetTimeData())

	dateCol := &schemapb.FieldData{
		Type: schemapb.DataType_Date,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DateData{
					DateData: &schemapb.DateArray{Data: []int32{0, 1}},
				},
			},
		},
	}
	assert.Equal(t, 8, CalcScalarSize(dateCol))

	timeCol := &schemapb.FieldData{
		Type: schemapb.DataType_Time,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimeData{
					TimeData: &schemapb.TimeArray{Data: []int64{0, 1_000_000}},
				},
			},
		},
	}
	assert.Equal(t, 16, CalcScalarSize(timeCol))
}
