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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCheckDateFieldData(t *testing.T) {
	v := newValidateUtil()
	field := &schemapb.FieldData{
		FieldName: "d",
		Type:      schemapb.DataType_Date,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"1970-01-01", "1970-01-02"}},
				},
			},
		},
	}
	require.NoError(t, v.checkDateFieldData(field))
	require.NotNil(t, field.GetScalars().GetDateData())
	assert.Equal(t, []int32{0, 1}, field.GetScalars().GetDateData().GetData())

	bad := &schemapb.FieldData{
		FieldName: "d",
		Type:      schemapb.DataType_Date,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"2024-06-22T00:00:00Z"}},
				},
			},
		},
	}
	assert.Error(t, v.checkDateFieldData(bad))
}

func TestCheckTimeFieldData(t *testing.T) {
	v := newValidateUtil()
	field := &schemapb.FieldData{
		FieldName: "tm",
		Type:      schemapb.DataType_Time,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"00:00:00", "00:00:01"}},
				},
			},
		},
	}
	require.NoError(t, v.checkTimeFieldData(field))
	require.NotNil(t, field.GetScalars().GetTimeData())
	assert.Equal(t, []int64{0, 1_000_000}, field.GetScalars().GetTimeData().GetData())

	bad := &schemapb.FieldData{
		FieldName: "tm",
		Type:      schemapb.DataType_Time,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"12:00:00Z"}},
				},
			},
		},
	}
	assert.Error(t, v.checkTimeFieldData(bad))
}

func TestValidateInsertDateTime(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "dt",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "d", DataType: schemapb.DataType_Date},
			{FieldID: 102, Name: "tm", DataType: schemapb.DataType_Time},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	data := []*schemapb.FieldData{
		{
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
				},
			},
		},
		{
			FieldName: "d",
			Type:      schemapb.DataType_Date,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"2024-06-22"}}},
				},
			},
		},
		{
			FieldName: "tm",
			Type:      schemapb.DataType_Time,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"13:45:30"}}},
				},
			},
		},
	}
	require.NoError(t, newValidateUtil().Validate(data, helper, 1))
	assert.NotNil(t, data[1].GetScalars().GetDateData())
	assert.NotNil(t, data[2].GetScalars().GetTimeData())
}

func TestDateTimePacked2IsoStr(t *testing.T) {
	results := []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Date,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: []int32{0}}},
				},
			},
		},
		{
			Type: schemapb.DataType_Time,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: []int64{0}}},
				},
			},
		},
	}
	dateTimePacked2IsoStr(results)
	assert.Equal(t, []string{"1970-01-01"}, results[0].GetScalars().GetStringData().GetData())
	assert.Equal(t, []string{"00:00:00"}, results[1].GetScalars().GetStringData().GetData())
}

func TestDateTimePacked2IsoStrGroupByFieldValues(t *testing.T) {
	rd := &schemapb.SearchResultData{
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Date,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: []int32{1}}},
					},
				},
			},
		},
		GroupByFieldValues: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Date,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: []int32{0}}},
					},
				},
			},
			{
				Type: schemapb.DataType_Time,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: []int64{1_000_000}}},
					},
				},
			},
		},
	}
	dateTimePacked2IsoStr(rd.GetFieldsData())
	dateTimePacked2IsoStr(rd.GetGroupByFieldValues())
	assert.Equal(t, []string{"1970-01-02"}, rd.GetFieldsData()[0].GetScalars().GetStringData().GetData())
	assert.Equal(t, []string{"1970-01-01"}, rd.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
	assert.Equal(t, []string{"00:00:01"}, rd.GetGroupByFieldValues()[1].GetScalars().GetStringData().GetData())
}
