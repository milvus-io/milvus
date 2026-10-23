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

package dql

import (
	"cmp"
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func floatingOrderField(dataType schemapb.DataType) *schemapb.FieldData {
	values := []float64{
		math.Float64frombits(0x7ff8000000000001), math.Inf(1), math.Copysign(0, -1), math.Inf(-1),
		math.Float64frombits(0xfff8000000000002), 0, -1, 1, math.Float64frombits(0x7ff0000000000001), 123,
	}
	scalars := &schemapb.ScalarField{
		ValidData: []bool{true, true, true, true, true, true, true, true, true, false},
	}
	if dataType == schemapb.DataType_Float {
		values32 := []float32{
			math.Float32frombits(0x7fc00001), float32(math.Inf(1)), float32(math.Copysign(0, -1)), float32(math.Inf(-1)),
			math.Float32frombits(0xffc00002), 0, -1, 1, math.Float32frombits(0x7f800001), 123,
		}
		scalars.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values32}}
	} else {
		scalars.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: values}}
	}
	return &schemapb.FieldData{
		Type: dataType, FieldName: "value",
		Field: &schemapb.FieldData_Scalars{Scalars: scalars},
	}
}

func TestCompareFieldDataAtFloatingTotalOrder(t *testing.T) {
	// Independent value classes: -Inf, -1, zero, 1, +Inf, NaN.
	ranks := []int{5, 4, 2, 0, 5, 2, 1, 3, 5}
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		t.Run(dataType.String(), func(t *testing.T) {
			field := floatingOrderField(dataType)
			for i, leftRank := range ranks {
				for j, rightRank := range ranks {
					actual, err := compareFieldDataAt(field, i, j, false)
					require.NoError(t, err)
					require.Equal(t, cmp.Compare(leftRank, rightRank), actual, "rows %d and %d", i, j)
				}
			}
			for _, nullsFirst := range []bool{false, true} {
				actual, err := compareFieldDataAt(field, 9, 0, nullsFirst)
				require.NoError(t, err)
				if nullsFirst {
					require.Equal(t, -1, actual)
				} else {
					require.Equal(t, 1, actual)
				}
			}
		})
	}
}

func TestOrderByFloatingTotalOrder(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		for _, test := range []struct {
			name       string
			ascending  bool
			nullsFirst bool
			wantIDs    []int64
		}{
			{"ascending_nulls_last", true, false, []int64{3, 6, 2, 5, 7, 1, 0, 4, 8, 9}},
			{"descending_nulls_first", false, true, []int64{9, 0, 4, 8, 1, 7, 2, 5, 6, 3}},
		} {
			t.Run(dataType.String()+"/"+test.name, func(t *testing.T) {
				result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
					NumQueries: 1, Topks: []int64{10}, Scores: make([]float32, 10),
					Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{
						Data: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
					}}},
					FieldsData: []*schemapb.FieldData{floatingOrderField(dataType)},
				}}
				op := &orderByOperator{
					orderByFields:  []OrderByField{{FieldName: "value", Ascending: test.ascending, NullsFirst: test.nullsFirst}},
					groupByFieldId: -1,
				}
				ctx := context.Background()
				outputs, err := op.run(ctx, trace.SpanFromContext(ctx), result)
				require.NoError(t, err)
				require.Len(t, outputs, 1)
				sorted := outputs[0].(*milvuspb.SearchResults)
				require.Equal(t, test.wantIDs, sorted.GetResults().GetIds().GetIntId().GetData())
			})
		}
	}
}
