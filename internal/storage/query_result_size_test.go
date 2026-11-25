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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestLogicalQueryResultBytes(t *testing.T) {
	column := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: make([]int64, 100)}},
		}},
	}
	bytes, err := LogicalQueryResultBytes([]*schemapb.FieldData{column}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 800, bytes)

	column.ValidData = []bool{true, false, true}
	column.GetScalars().GetLongData().Data = []int64{7, 999, 42}
	text := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "雪"}}},
		}},
	}
	bytes, err = LogicalQueryResultBytes([]*schemapb.FieldData{column, text}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 24, bytes)

	text.Type = schemapb.DataType_Timestamptz
	text.GetScalars().GetStringData().Data = []string{"2026-11-24T21:01:00+08:00", "2026-11-24T21:01:00.123456+08:00"}
	bytes, err = LogicalQueryResultBytes([]*schemapb.FieldData{text}, nil)
	require.NoError(t, err)
	require.EqualValues(t, 16, bytes)
}

func TestLogicalSearchResultBytes(t *testing.T) {
	result := &schemapb.SearchResultData{
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{91}}}},
		Scores: []float32{0.9},
		FieldsData: []*schemapb.FieldData{{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}},
			}},
		}},
		AggBuckets: []*schemapb.AggBucket{{
			Count: 2,
			Key:   []*schemapb.BucketKeyEntry{{Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "雪"}}},
			Metrics: map[string]*schemapb.MetricValue{
				"average": {Value: &schemapb.MetricValue_DoubleVal{DoubleVal: 3}},
				"unset":   nil,
			},
			Hits: []*schemapb.AggHit{{
				Pk:    &schemapb.AggHit_StrPk{StrPk: "pk"},
				Score: 0.6,
			}},
		}},
		OutputFields: []string{"a"},
		Topks:        []int64{1},
	}
	bytes, err := LogicalSearchResultBytes(result, nil)
	require.NoError(t, err)
	require.EqualValues(t, 45, bytes)

	bytes, err = LogicalSearchResultBytes(nil, nil)
	require.NoError(t, err)
	require.Zero(t, bytes)
}
