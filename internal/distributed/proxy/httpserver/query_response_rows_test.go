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

package httpserver

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func materializeQueryResponseRows(t *testing.T, rows *queryResponseRows) []map[string]interface{} {
	t.Helper()
	result := make([]map[string]interface{}, 0, rows.Len())
	for rowIndex := int64(0); rowIndex < rows.Len(); rowIndex++ {
		row, err := rows.Row(rowIndex)
		require.NoError(t, err)
		result = append(result, row)
	}
	return result
}

type cancelAfterErrChecksContext struct {
	context.Context
	remaining int
	done      chan struct{}
	canceled  bool
}

func newCancelAfterErrChecksContext(allowed int) *cancelAfterErrChecksContext {
	return &cancelAfterErrChecksContext{
		Context:   context.Background(),
		remaining: allowed,
		done:      make(chan struct{}),
	}
}

func (ctx *cancelAfterErrChecksContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *cancelAfterErrChecksContext) Err() error {
	if ctx.canceled {
		return context.Canceled
	}
	if ctx.remaining > 0 {
		ctx.remaining--
		return nil
	}
	ctx.canceled = true
	close(ctx.done)
	return context.Canceled
}

func TestNullableVectorPreflightObservesCancellationAfterValidDataScan(t *testing.T) {
	validData := make([]bool, responseContextCheckInterval+1)
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "vector",
		ValidData: validData,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
		}},
	}
	ctx := newCancelAfterErrChecksContext(2)

	_, err := newFieldDataRowAccessorWithContext(ctx, fieldData)

	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryResponseRowsMatchesExistingResponseValues(t *testing.T) {
	outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
	rows, err := newQueryResponseRows(
		0,
		outputFields,
		generateFieldData(),
		generateIDs(schemapb.DataType_Int64, 3),
		DefaultScores,
		true,
		nil,
	)
	require.NoError(t, err)

	actual := materializeQueryResponseRows(t, rows)
	expected := generateSearchResult(schemapb.DataType_Int64)
	assert.True(t, compareRows(actual, expected, compareRow))
}

func TestQueryResponseRowsDynamicAndNullableValues(t *testing.T) {
	dynamicField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "$meta",
		IsDynamic: true,
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{
				[]byte(`{"age": 18, "city": "shanghai"}`),
				[]byte(`{"age": 20, "city": "hangzhou"}`),
			}}},
		}},
	}
	nullableVector := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "vector",
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
				Data: []float32{0.1, 0.2, 0.3, 0.4},
			}},
		}},
	}

	rows, err := newQueryResponseRows(
		0,
		[]string{"age", "vector"},
		[]*schemapb.FieldData{dynamicField, nullableVector},
		nil,
		nil,
		true,
		nil,
	)
	require.NoError(t, err)
	actual := materializeQueryResponseRows(t, rows)
	require.Len(t, actual, 3)
	assert.Equal(t, json.Number("18"), actual[0]["age"])
	assert.NotContains(t, actual[0], "city")
	assert.Equal(t, []float32{0.1, 0.2}, actual[0]["vector"])
	assert.NotContains(t, actual[1], "age")
	assert.Nil(t, actual[1]["vector"])
	assert.Equal(t, json.Number("20"), actual[2]["age"])
	assert.Equal(t, []float32{0.3, 0.4}, actual[2]["vector"])
}

func TestQueryResponseRowsStructArrayValues(t *testing.T) {
	schema := buildStructArrayTestSchema()
	structSchema := schema.GetStructArrayFields()[0]
	row, err := parseStructArrayRow(
		`[{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}]`, structSchema, false)
	require.NoError(t, err)
	structField, err := buildStructArrayFieldData(structSchema, []structArrayRow{row})
	require.NoError(t, err)

	rows, err := newQueryResponseRows(0, []string{"my_struct"}, []*schemapb.FieldData{structField}, nil, nil, true, schema)
	require.NoError(t, err)
	actual, err := rows.Row(0)
	require.NoError(t, err)
	nested, ok := actual["my_struct"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, nested, 1)
	assert.EqualValues(t, 10, nested[0]["sub_int"])
	assert.Equal(t, []float32{1.1, 1.2, 1.3, 1.4}, nested[0]["sub_vec"])
}

func TestQueryResponseRowsRejectsMalformedInternalShapes(t *testing.T) {
	tests := []struct {
		name   string
		fields []*schemapb.FieldData
		ids    *schemapb.IDs
		scores []float32
	}{
		{
			name: "field row count mismatch",
			fields: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "first",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2}},
					}}},
				},
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "short",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1}},
					}}},
				},
			},
		},
		{
			name: "vector remainder",
			fields: []*schemapb.FieldData{{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
						Data: []float32{0.1, 0.2, 0.3},
					}},
				}},
			}},
		},
		{
			name: "short IDs",
			fields: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 2}},
				}}},
			}},
			ids: generateIDs(schemapb.DataType_Int64, 1),
		},
		{
			name: "empty first field with nonempty IDs",
			fields: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: "empty",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{},
				}}},
			}},
			ids: generateIDs(schemapb.DataType_Int64, 2),
		},
		{
			name: "empty first field with nonempty second field",
			fields: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "empty",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{},
					}}},
				},
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "nonempty",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1}},
					}}},
				},
			},
		},
		{
			name: "all-null field with inconsistent physical data",
			fields: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: "nullable",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1}},
				}}},
			}},
		},
		{
			name: "struct sub-field has extra logical rows",
			fields: []*schemapb.FieldData{{
				Type:      schemapb.DataType_ArrayOfStruct,
				FieldName: "struct",
				Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_Array,
						FieldName: "first",
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{{
								Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}},
							}}},
						}}},
					},
					{
						Type:      schemapb.DataType_Array,
						FieldName: "extra",
						Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{
								{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}},
								{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{2}}}},
							}},
						}}},
					},
				}}},
			}},
			ids: generateIDs(schemapb.DataType_Int64, 1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newQueryResponseRows(0, nil, test.fields, test.ids, test.scores, true, nil)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrServiceInternal), "unexpected error classification: %v", err)
		})
	}
}

func TestQueryResponseRowsAcceptsEmptyIDsWrapper(t *testing.T) {
	emptyPrimaryField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: FieldBookID,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{},
		}}},
	}

	rows, err := newQueryResponseRows(
		0,
		[]string{FieldBookID},
		[]*schemapb.FieldData{emptyPrimaryField},
		&schemapb.IDs{},
		[]float32{},
		true,
		generateCollectionSchema(schemapb.DataType_Int64, false, true),
	)

	require.NoError(t, err)
	assert.Zero(t, rows.Len())
	assert.Empty(t, materializeQueryResponseRows(t, rows))
}

func TestQueryResponseRowsExplicitCountPreservesLegacyPrefix(t *testing.T) {
	rows, err := newQueryResponseRows(
		1,
		[]string{FieldWordCount},
		lazyRouteFieldData(),
		generateIDs(schemapb.DataType_Int64, 2),
		[]float32{0.1, 0.2},
		true,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows.Len())
	row, err := rows.Row(0)
	require.NoError(t, err)
	assert.EqualValues(t, 10, row[FieldWordCount])
	assert.EqualValues(t, 1, row[DefaultPrimaryFieldName])
}

func TestQueryResponseRowsPreservesAllNullText(t *testing.T) {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Text,
		FieldName: "content",
		ValidData: []bool{false, false},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{},
		}}},
	}

	rows, err := newQueryResponseRows(0, []string{"content"}, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows.Len())
	for rowIndex := int64(0); rowIndex < rows.Len(); rowIndex++ {
		row, err := rows.Row(rowIndex)
		require.NoError(t, err)
		require.Contains(t, row, "content")
		require.Nil(t, row["content"])
	}
}

func TestQueryResponseRowsStopsPreflightWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newQueryResponseRowsWithContext(ctx, 0, []string{FieldWordCount}, lazyRouteFieldData(), nil, nil, true, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryResponseRowsStopsSparseConversionOnCancellation(t *testing.T) {
	elemCount := int(responseContextCheckInterval * 2)
	indices := make([]uint32, elemCount)
	values := make([]float32, elemCount)
	for index := 0; index < elemCount; index++ {
		indices[index] = uint32(index)
		values[index] = float32(index)
	}
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: "sparse",
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
				Contents: [][]byte{typeutil.CreateSparseFloatRow(indices, values)},
			}},
		}},
	}
	ctx := newCancelAfterErrChecksContext(3)
	rows := &queryResponseRows{
		ctx:                  ctx,
		rowsNum:              1,
		fieldDataList:        []*schemapb.FieldData{fieldData},
		fieldDataAccessors:   []*fieldDataRowAccessor{{fieldData: fieldData}},
		structArrayAccessors: make([]*structArrayRowAccessor, 1),
		enableInt64:          true,
		pkFieldName:          DefaultPrimaryFieldName,
	}

	_, err := rows.Row(0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryResponseRowsRejectsInvalidDynamicJSON(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`{"unterminated":`),
		[]byte(`[1, 2, 3]`),
		[]byte(`{"overflow": 1e400}`),
	} {
		t.Run(string(raw), func(t *testing.T) {
			field := &schemapb.FieldData{
				Type:      schemapb.DataType_JSON,
				FieldName: "$meta",
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{raw}}},
				}},
			}
			_, err := newQueryResponseRows(0, []string{"$meta"}, []*schemapb.FieldData{field}, nil, nil, true, nil)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrServiceInternal), "unexpected error classification: %v", err)
		})
	}

	nullField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "$meta",
		IsDynamic: true,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte("null")}}},
		}},
	}
	rows, err := newQueryResponseRows(0, []string{"$meta"}, []*schemapb.FieldData{nullField}, nil, nil, true, nil)
	require.NoError(t, err)
	row, err := rows.Row(0)
	require.NoError(t, err)
	assert.Empty(t, row)
}

func TestQueryResponseRowsRejectsDynamicJSONBeyondStreamingEncoderDepth(t *testing.T) {
	tests := []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{name: "maximum encodable depth", depth: maxDynamicJSONValueNestingDepth},
		{name: "first unsupported depth", depth: maxDynamicJSONValueNestingDepth + 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := []byte(`{"deep":` + strings.Repeat("[", test.depth) + `0` + strings.Repeat("]", test.depth) + `}`)
			field := &schemapb.FieldData{
				Type:      schemapb.DataType_JSON,
				FieldName: "$meta",
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{raw}}},
				}},
			}

			rows, err := newQueryResponseRows(0, []string{"$meta"}, []*schemapb.FieldData{field}, nil, nil, true, nil)
			if test.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, merr.ErrServiceInternal)
				return
			}

			require.NoError(t, err)
			row, err := rows.Row(0)
			require.NoError(t, err)
			var encoded bytes.Buffer
			require.NoError(t, json.NewEncoder(&encoded).Encode(row))
		})
	}
}

func TestQueryResponseRowsSkipsJSONValueValidationForNoopFields(t *testing.T) {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "text",
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"value"}}},
		}},
	}
	ctx := newCancelAfterErrChecksContext(1)
	rows := &queryResponseRows{
		ctx:                ctx,
		rowsNum:            1,
		fieldDataList:      []*schemapb.FieldData{field},
		fieldDataAccessors: []*fieldDataRowAccessor{{fieldData: field}},
	}

	require.NoError(t, rows.validateJSONValues())
	assert.False(t, ctx.canceled)
}

func TestQueryResponseRowsRejectsNonFiniteJSONValues(t *testing.T) {
	sparseNaN := typeutil.CreateSparseFloatRow([]uint32{1}, []float32{float32(math.NaN())})
	tests := []struct {
		name   string
		field  *schemapb.FieldData
		scores []float32
	}{
		{
			name: "float scalar",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Float,
				FieldName: "float",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{float32(math.NaN())}},
				}}},
			},
		},
		{
			name: "float vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "vector",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
						Data: []float32{0.1, float32(math.Inf(1))},
					}},
				}},
			},
		},
		{
			name: "array",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Array,
				FieldName: "array",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{{
						Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{math.Inf(-1)}}},
					}}},
				}}},
			},
		},
		{
			name: "sparse vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: "sparse",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{sparseNaN},
					}},
				}},
			},
		},
		{
			name: "score",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "value",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1}},
				}}},
			},
			scores: []float32{float32(math.NaN())},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newQueryResponseRows(0, []string{test.field.GetFieldName()}, []*schemapb.FieldData{test.field}, nil, test.scores, true, nil)
			require.Error(t, err)
			assert.True(t, errors.Is(err, merr.ErrServiceInternal), "unexpected error classification: %v", err)
		})
	}
}

func TestQueryResponseRowsRejectsNonFiniteStructVector(t *testing.T) {
	schema := buildStructArrayTestSchema()
	structSchema := schema.GetStructArrayFields()[0]
	row, err := parseStructArrayRow(
		`[{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}]`, structSchema, false)
	require.NoError(t, err)
	structField, err := buildStructArrayFieldData(structSchema, []structArrayRow{row})
	require.NoError(t, err)
	subVector := structField.GetStructArrays().GetFields()[1].GetVectors().GetVectorArray().GetData()[0]
	subVector.GetFloatVector().Data[0] = float32(math.NaN())

	_, err = newQueryResponseRows(0, []string{"my_struct"}, []*schemapb.FieldData{structField}, nil, nil, true, schema)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal), "unexpected error classification: %v", err)
}

func TestQueryResponseRowsBounds(t *testing.T) {
	rows, err := newQueryResponseRows(0, nil, generateFieldData(), nil, nil, true, nil)
	require.NoError(t, err)
	_, err = rows.Row(-1)
	require.Error(t, err)
	_, err = rows.Row(rows.Len())
	require.Error(t, err)
}

func BenchmarkQueryResponseRowsComplexFields(b *testing.B) {
	const rowCount = 128

	dynamicData := make([][]byte, rowCount)
	for index := range dynamicData {
		dynamicData[index] = []byte(`{"category":"benchmark","weight":1.25,"nested":{"rank":7}}`)
	}
	dynamicField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "$meta",
		IsDynamic: true,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: dynamicData}},
		}},
	}

	b.Run("dynamic_json/preflight", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			rows, err := newQueryResponseRows(0, []string{"$meta"}, []*schemapb.FieldData{dynamicField}, nil, nil, true, nil)
			if err != nil {
				b.Fatal(err)
			}
			if rows.Len() != rowCount {
				b.Fatalf("unexpected row count %d", rows.Len())
			}
		}
	})

	dynamicRows, err := newQueryResponseRows(0, []string{"$meta"}, []*schemapb.FieldData{dynamicField}, nil, nil, true, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("dynamic_json/row", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := dynamicRows.Row(int64(iteration % rowCount)); err != nil {
				b.Fatal(err)
			}
		}
	})

	schema := buildStructArrayTestSchema()
	structSchema := schema.GetStructArrayFields()[0]
	scalar := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{
		IntData: &schemapb.IntArray{Data: make([]int32, 32)},
	}}
	vector := &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{
		FloatVector: &schemapb.FloatArray{Data: make([]float32, 32*4)},
	}}
	structRows := make([]structArrayRow, rowCount)
	for index := range structRows {
		structRows[index] = structArrayRow{
			"sub_int": scalar,
			"sub_vec": vector,
		}
	}
	structField, err := buildStructArrayFieldData(structSchema, structRows)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("array_of_struct/preflight", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			rows, err := newQueryResponseRows(0, []string{"my_struct"}, []*schemapb.FieldData{structField}, nil, nil, true, schema)
			if err != nil {
				b.Fatal(err)
			}
			if rows.Len() != rowCount {
				b.Fatalf("unexpected row count %d", rows.Len())
			}
		}
	})

	complexRows, err := newQueryResponseRows(0, []string{"my_struct"}, []*schemapb.FieldData{structField}, nil, nil, true, schema)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("array_of_struct/row", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := complexRows.Row(int64(iteration % rowCount)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
