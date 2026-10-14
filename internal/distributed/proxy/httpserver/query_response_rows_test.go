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
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func collectQueryResponseRows(t *testing.T, rows *queryResponseRows, batchSize int) []map[string]interface{} {
	t.Helper()
	got := []map[string]interface{}{}
	for i := 0; i < 100; i++ {
		batch, err := rows.Next(context.Background(), batchSize)
		if errors.Is(err, io.EOF) {
			return got
		}
		require.NoError(t, err)
		require.NotEmpty(t, batch)
		require.LessOrEqual(t, len(batch), batchSize)
		got = append(got, batch...)
	}
	t.Fatal("projector never reached EOF")
	return nil
}

func TestQueryResponseRowsAllTypes(t *testing.T) {
	// Distinct values in every row catch resetting a column cursor per batch.
	fields := []*schemapb.FieldData{}
	addScalar := func(name string, typ schemapb.DataType, scalar *schemapb.ScalarField) {
		fields = append(fields, &schemapb.FieldData{FieldName: name, Type: typ, Field: &schemapb.FieldData_Scalars{Scalars: scalar}})
	}
	addScalar("bool", schemapb.DataType_Bool, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}}}})
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32} {
		addScalar(typ.String(), typ, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}}}})
	}
	addScalar("long", schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{9007199254740993, 2, 3}}}})
	addScalar("float", schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.25, 2.5, 3.75}}}})
	addScalar("double", schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.25, 2.5, 3.75}}}})
	addScalar("timestamp typed", schemapb.DataType_Timestamptz, &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1, 2, 3}}}})
	addScalar("geometry typed", schemapb.DataType_Geometry, &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1}, {2}, {3}}}}})
	for _, typ := range []schemapb.DataType{schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text, schemapb.DataType_Timestamptz, schemapb.DataType_Geometry} {
		scalar := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b", "c"}}}}
		if typ == schemapb.DataType_Geometry {
			scalar = &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT (1 1)", "POINT (2 2)", "POINT (3 3)"}}}}
		}
		addScalar(typ.String(), typ, scalar)
	}
	addScalar("array", schemapb.DataType_Array, &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64, Data: []*schemapb.ScalarField{
		{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{9007199254740993}}}},
		{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{2}}}},
		{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{3}}}},
	}}}})
	for _, typ := range []schemapb.DataType{schemapb.DataType_BinaryVector, schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_SparseFloatVector, schemapb.DataType_Int8Vector} {
		fd := generateVectorFieldData(typ)
		fd.FieldName = typ.String()
		// The existing half-vector fixture has three two-byte rows.
		if typ == schemapb.DataType_Float16Vector || typ == schemapb.DataType_BFloat16Vector {
			fd.GetVectors().Dim = 1
		}
		fields = append(fields, &fd)
	}
	fields = append(fields, jsonRespFieldData(`{"i":1}`, `{"i":2}`, `{"i":3}`))
	dynamic := jsonRespFieldData(`{"big":9223372036854775807,"hidden":1}`, `{"big":9007199254740993,"hidden":2}`, `{"big":3,"hidden":3}`)
	dynamic.FieldName = "$meta"
	dynamic.IsDynamic = true
	fields = append(fields, dynamic)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{9007199254740993, 2, 3}}}}
	for _, native := range []bool{false, true} {
		for _, legacyArray := range []bool{false, true} {
			for _, numeric := range []bool{false, true} {
				t.Run(fmt.Sprintf("native=%v/legacyArray=%v/numeric=%v", native, legacyArray, numeric), func(t *testing.T) {
					paramtable.Init()
					for _, config := range []struct{ key, value string }{{paramtable.Get().HTTPCfg.NativeJSONResponse.Key, strconv.FormatBool(native)}, {paramtable.Get().HTTPCfg.LegacyArrayResponse.Key, strconv.FormatBool(legacyArray)}} {
						require.NoError(t, paramtable.Get().Save(config.key, config.value))
						t.Cleanup(func() { _ = paramtable.Get().Reset(config.key) })
					}
					want, err := buildQueryResp(0, []string{"big"}, fields, ids, []float32{0.5, 0.25}, numeric, schema)
					require.NoError(t, err)
					for _, size := range []int{1, 3} {
						rows, err := newQueryResponseRows(0, []string{"big"}, fields, ids, []float32{0.5, 0.25}, numeric, schema)
						require.NoError(t, err)
						got := collectQueryResponseRows(t, rows, size)
						require.Equal(t, want, got)
						require.Equal(t, json.Number("9223372036854775807"), got[0]["big"])
						require.NotContains(t, got[0], "hidden")
					}
				})
			}
		}
	}
}

func TestQueryResponseRowsNullable(t *testing.T) {
	for _, dense := range []bool{false, true} {
		valid := []bool{true, false, false, true, true, false, true}
		data := []int64{11, 22, 33, 44}
		if dense {
			data = []int64{11, 0, 0, 22, 33, 0, 44}
		}
		fd := &schemapb.FieldData{Type: schemapb.DataType_Int64, FieldName: "value", ValidData: valid, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}}}}
		want, err := buildQueryResp(0, nil, []*schemapb.FieldData{fd}, nil, nil, true, nil)
		require.NoError(t, err)
		for _, size := range []int{1, 3} {
			rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{fd}, nil, nil, true, nil)
			require.NoError(t, err)
			require.Equal(t, want, collectQueryResponseRows(t, rows, size))
		}
	}
	// A compact nullable vector must advance only for valid rows.
	vector := generateVectorFieldData(schemapb.DataType_FloatVector)
	typeutil.SetFieldDataValidData(&vector, []bool{true, false, true, false, false, true, false})
	want, err := buildQueryResp(0, nil, []*schemapb.FieldData{&vector}, nil, nil, true, nil)
	require.NoError(t, err)
	rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{&vector}, nil, nil, true, nil)
	require.NoError(t, err)
	require.Equal(t, want, collectQueryResponseRows(t, rows, 3))
}

func TestQueryResponseRowsJSONFallback(t *testing.T) {
	paramtable.Init()
	config := paramtable.Get().HTTPCfg.NativeJSONResponse
	old := config.GetValue()
	require.NoError(t, paramtable.Get().Save(config.Key, "true"))
	t.Cleanup(func() { _ = paramtable.Get().Save(config.Key, old) })
	for _, bad := range []string{"legacy", string([]byte{'"', 0xff, '"'})} {
		fields := []*schemapb.FieldData{jsonRespFieldData(`{"first":1}`, `{"second":2}`, `{"third":3}`, bad)}
		want, err := buildQueryResp(0, nil, fields, nil, nil, true, nil)
		require.NoError(t, err)
		for _, size := range []int{1, 3} {
			rows, err := newQueryResponseRows(0, nil, fields, nil, nil, true, nil)
			require.NoError(t, err)
			first, err := rows.Next(context.Background(), size)
			require.NoError(t, err)
			require.IsType(t, "", first[0]["meta"], "invalid final row must degrade the first batch")
			got := append(first, collectQueryResponseRows(t, rows, size)...)
			require.Equal(t, want, got)
		}
	}
	// Dense nullable placeholders are not selected JSON values.
	field := jsonRespFieldData(`{"a":1}`, `invalid placeholder`, `{"b":2}`)
	field.ValidData = []bool{true, false, true}
	rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	got := collectQueryResponseRows(t, rows, 1)
	require.Equal(t, []map[string]interface{}{{"meta": json.RawMessage(`{"a":1}`)}, {"meta": nil}, {"meta": json.RawMessage(`{"b":2}`)}}, got)
	// Rows outside an explicit selected prefix must not trigger fallback.
	rows, err = newQueryResponseRows(1, nil, []*schemapb.FieldData{jsonRespFieldData(`{"a":1}`, `invalid`)}, nil, nil, true, nil)
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{{"meta": json.RawMessage(`{"a":1}`)}}, collectQueryResponseRows(t, rows, 1))
}

func TestQueryResponseRowsBatchOwnership(t *testing.T) {
	field := generateVectorFieldData(schemapb.DataType_FloatVector)
	rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{&field}, nil, nil, true, nil)
	require.NoError(t, err)
	first, err := rows.Next(context.Background(), 1)
	require.NoError(t, err)
	second, err := rows.Next(context.Background(), 1)
	require.NoError(t, err)
	first[0]["private"] = true
	require.NotContains(t, second[0], "private")
	require.Equal(t, []float32{0.1, 0.11}, first[0][FieldBookIntro])
	vector := first[0][FieldBookIntro].([]float32)
	require.Same(t, &field.GetVectors().GetFloatVector().Data[0], &vector[0], "float vectors borrow the protobuf buffer")
}

// This context deterministically cancels during a response scan, avoiding a
// timing-dependent test that might finish projection before cancellation.
type queryResponseCancelContext struct {
	context.Context
	checks    int
	remaining int
}

func (c *queryResponseCancelContext) Err() error {
	c.checks++
	if c.checks > c.remaining {
		return context.Canceled
	}
	return nil
}

func TestQueryResponseRowsCancellation(t *testing.T) {
	for _, stage := range []string{"nullable initialization", "JSON preflight", "row projection"} {
		t.Run(stage, func(t *testing.T) {
			var fields []*schemapb.FieldData
			switch stage {
			case "nullable initialization":
				fields = []*schemapb.FieldData{{Type: schemapb.DataType_Int64, FieldName: "n", ValidData: []bool{false, false, false, false, false, false, false, false, false, false}}}
			case "JSON preflight":
				fields = []*schemapb.FieldData{jsonRespFieldData(`{}`, `{}`, `{}`, `{}`, `{}`, `{}`, `{}`, `{}`, `{}`, `{}`)}
			}
			rows, err := newQueryResponseRows(10, nil, fields, nil, nil, true, nil)
			require.NoError(t, err)
			ctx := &queryResponseCancelContext{Context: context.Background(), remaining: 6}
			batch, err := rows.Next(ctx, 10)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, batch, "cancelled projection must discard the current batch")
		})
	}
	t.Run("cancelled before next batch", func(t *testing.T) {
		rows, err := newQueryResponseRows(2, nil, nil, nil, nil, true, nil)
		require.NoError(t, err)
		first, err := rows.Next(context.Background(), 1)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		batch, err := rows.Next(ctx, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, batch)
		require.Equal(t, []map[string]interface{}{{}}, first)
		// Cancellation before any row leaves the remaining cursor intact.
		require.Equal(t, []map[string]interface{}{{}}, collectQueryResponseRows(t, rows, 1))
	})
}

func TestQueryResponseRowsInvalidBatchSize(t *testing.T) {
	rows, err := newQueryResponseRows(1, nil, nil, nil, nil, true, nil)
	require.NoError(t, err)
	for _, size := range []int{0, -1} {
		batch, err := rows.Next(context.Background(), size)
		require.Error(t, err)
		require.Nil(t, batch)
	}
	require.Equal(t, []map[string]interface{}{{}}, collectQueryResponseRows(t, rows, 1))
}

func TestQueryResponseRowsEmptyAndStringIDs(t *testing.T) {
	rows, err := newQueryResponseRows(0, nil, nil, nil, nil, true, nil)
	require.NoError(t, err)
	require.Empty(t, collectQueryResponseRows(t, rows, 3))
	ids := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"first", "second", "third", "fourth"}}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "custom_pk", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar}}}
	rows, err = newQueryResponseRows(0, nil, nil, ids, []float32{0.5}, false, schema)
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{{"custom_pk": "first", "distance": float32(0.5)}, {"custom_pk": "second"}, {"custom_pk": "third"}, {"custom_pk": "fourth"}}, collectQueryResponseRows(t, rows, 3))
	_, err = newQueryResponseRows(-1, nil, nil, nil, nil, true, nil)
	require.Error(t, err)
}

func TestQueryResponseRowsDynamicExpansionAndErrors(t *testing.T) {
	field := jsonRespFieldData(`{"big":9223372036854775807,"a":1}`, `{"big":2,"a":3}`)
	field.FieldName = "$meta"
	field.IsDynamic = true
	rows, err := newQueryResponseRows(0, []string{"$meta"}, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{{"big": json.Number("9223372036854775807"), "a": json.Number("1")}, {"big": json.Number("2"), "a": json.Number("3")}}, collectQueryResponseRows(t, rows, 1))
	field = jsonRespFieldData(`{"a":1}`, `{"a":2} {"a":3}`)
	field.IsDynamic = true
	field.FieldName = "$meta"
	rows, err = newQueryResponseRows(0, []string{"a"}, []*schemapb.FieldData{field}, nil, nil, true, nil)
	require.NoError(t, err)
	first, err := rows.Next(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []map[string]interface{}{{"a": json.Number("1")}}, first)
	batch, err := rows.Next(context.Background(), 1)
	require.Error(t, err)
	require.Nil(t, batch)
}

func TestQueryResponseRowsStructArray(t *testing.T) {
	schema := buildStructArrayTestSchema()
	structSchema := schema.StructArrayFields[0]
	structSchema.Nullable = true
	first, err := parseStructArrayRow(`[{"sub_int":10,"sub_vec":[1,2,3,4]}]`, structSchema, false)
	require.NoError(t, err)
	empty, err := parseStructArrayRow(`[]`, structSchema, false)
	require.NoError(t, err)
	field, err := buildNullableStructArrayFieldData(structSchema, []structArrayRow{first, empty, first}, []bool{true, false, true, false, false, true, false})
	require.NoError(t, err)
	want, err := buildQueryResp(0, nil, []*schemapb.FieldData{field}, nil, nil, true, schema)
	require.NoError(t, err)
	for _, size := range []int{1, 3} {
		rows, err := newQueryResponseRows(0, nil, []*schemapb.FieldData{field}, nil, nil, true, schema)
		require.NoError(t, err)
		require.Equal(t, want, collectQueryResponseRows(t, rows, size))
	}
}

func TestQueryResponseRowsBatches(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3, 4, 7} {
		for _, batchSize := range []int{1, 3} {
			t.Run(fmt.Sprintf("rows=%d/batch=%d", count, batchSize), func(t *testing.T) {
				values := make([]int64, count)
				for i := range values {
					values[i] = int64(i + 10)
				}
				ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: values}}}
				want, err := buildQueryResp(0, nil, nil, ids, nil, true, nil)
				require.NoError(t, err)
				rows, err := newQueryResponseRows(0, nil, nil, ids, nil, true, nil)
				require.NoError(t, err)
				got := []map[string]interface{}{}
				for attempts := 0; attempts <= count; attempts++ {
					batch, err := rows.Next(context.Background(), batchSize)
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
					require.NotEmpty(t, batch, "remaining rows must produce a batch")
					require.LessOrEqual(t, len(batch), batchSize)
					got = append(got, batch...)
				}
				require.Equal(t, want, got)
				_, err = rows.Next(context.Background(), batchSize)
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

// These literals characterize the full builder before sharing its conversion
// logic. They catch compact-nullable indexing and whole-response JSON fallback
// independently of the batch-versus-full-builder comparison.
func TestQueryResponseRowsLegacyGolden(t *testing.T) {
	paramtable.Init()
	config := paramtable.Get().HTTPCfg.NativeJSONResponse
	previous := config.GetValue()
	require.NoError(t, paramtable.Get().Save(config.Key, "true"))
	t.Cleanup(func() { _ = paramtable.Get().Save(config.Key, previous) })

	field := &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldName: "value",
		ValidData: []bool{true, false, false, true, true, false, true},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{11, 22, 33, 44}}},
		}},
	}
	for _, tc := range []struct {
		name    string
		numeric bool
		want    []map[string]interface{}
	}{
		{"numeric", true, []map[string]interface{}{{"value": int64(11)}, {"value": nil}, {"value": nil}, {"value": int64(22)}, {"value": int64(33)}, {"value": nil}, {"value": int64(44)}}},
		{"string", false, []map[string]interface{}{{"value": "11"}, {"value": nil}, {"value": nil}, {"value": "22"}, {"value": "33"}, {"value": nil}, {"value": "44"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildQueryResp(0, nil, []*schemapb.FieldData{field}, nil, nil, tc.numeric, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("invalid final JSON degrades earlier rows", func(t *testing.T) {
		got, err := buildQueryResp(0, nil, []*schemapb.FieldData{jsonRespFieldData(`{"a":1}`, `{"b":2}`, `legacy`)}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Equal(t, []map[string]interface{}{{"meta": `{"a":1}`}, {"meta": `{"b":2}`}, {"meta": `legacy`}}, got)
	})
	t.Run("valid JSON remains native", func(t *testing.T) {
		got, err := buildQueryResp(0, nil, []*schemapb.FieldData{jsonRespFieldData(`{"a":1}`)}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Equal(t, []map[string]interface{}{{"meta": json.RawMessage(`{"a":1}`)}}, got)
	})
}
