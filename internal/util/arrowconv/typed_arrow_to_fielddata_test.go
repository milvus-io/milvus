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

package arrowconv

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestTypedArrowChunks(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	// Arbitrary field names verify that transport conversion has no Grep semantics.
	typ := arrow.StructOf(arrow.Field{Name: "label", Type: arrow.ListOf(arrow.BinaryTypes.String)}, arrow.Field{Name: "values", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))})
	schema := arrow.NewSchema([]arrow.Field{{Name: "calculated", Type: typ}}, nil)
	var records []arrow.Record
	for _, input := range []string{`[{"label":["a"],"values":[[1,2]]},{"label":[],"values":[]}]`, `[{"label":[],"values":[]},{"label":["b"],"values":[[3]]}]`} {
		values, _, err := array.FromJSON(pool, typ, strings.NewReader(input))
		require.NoError(t, err)
		records = append(records, array.NewRecord(schema, []arrow.Array{values}, 2))
		values.Release()
	}
	refs := []testTypedRowRef{{ResultIndex: 1, RowIndex: 1}, {ResultIndex: 0, RowIndex: 0}, {ResultIndex: 1, RowIndex: 1}}
	result, err := exportTestTypedRows(schema, records, refs)
	require.NoError(t, err)
	empty, err := exportTestTypedRows(schema, nil, nil)
	require.NoError(t, err)
	require.Len(t, empty, 1)
	require.Empty(t, empty[0].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data)
	for _, r := range records {
		r.Release()
	}
	fields := result[0].GetStructArrays().Fields
	require.Equal(t, "label", fields[0].FieldName)
	require.Equal(t, "values", fields[1].FieldName)
	labels := fields[0].GetScalars().GetArrayData().Data
	require.Equal(t, []string{"b"}, labels[0].GetStringData().Data)
	require.Equal(t, []string{"a"}, labels[1].GetStringData().Data)
	require.Equal(t, []string{"b"}, labels[2].GetStringData().Data)
	values := fields[1].GetScalars().GetArrayData().Data
	require.Equal(t, []int64{1, 2}, values[1].GetArrayData().Data[0].GetLongData().Data)
}

func TestComputedStructScalarChildren(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dtype arrow.DataType
		input string
		want  *schemapb.ScalarField
	}{
		{"string", arrow.BinaryTypes.String, `[["first","second"],[]]`, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"first", "second"}}}}},
		{"int64", arrow.PrimitiveTypes.Int64, `[[1,2],[]]`, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		{"bool", arrow.FixedWidthTypes.Boolean, `[[false,true],[]]`, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{false, true}}}}},
		{"double", arrow.PrimitiveTypes.Float64, `[[1.5,2.5],[]]`, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.5, 2.5}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, arrow.ListOf(tc.dtype), strings.NewReader(tc.input))
			require.NoError(t, err)
			rows, err := array.NewStructArray([]arrow.Array{values}, []string{"value"})
			require.NoError(t, err)
			values.Release()
			first, empty := array.NewSlice(rows, 0, 1), array.NewSlice(rows, 1, 2)
			column := arrow.NewChunked(rows.DataType(), []arrow.Array{first, empty, first})
			first.Release()
			empty.Release()
			rows.Release()
			out, err := ArrowChunkedStructColumnToFieldData("computed", column, nil)
			column.Release()
			require.NoError(t, err)
			data := out.GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
			require.Len(t, data, 3)
			require.True(t, proto.Equal(tc.want, data[0]))
			require.True(t, proto.Equal(tc.want, data[2]))
			require.NotSame(t, data[0], data[2])
			require.IsType(t, tc.want.GetData(), data[1].GetData())
			require.Empty(t, data[1].GetStringData().GetData())
			require.Empty(t, data[1].GetLongData().GetData())
			require.Empty(t, data[1].GetBoolData().GetData())
			require.Empty(t, data[1].GetDoubleData().GetData())
		})
	}
}

func TestComputedStructScalarChildErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dtype arrow.DataType
		input string
	}{
		{"null array", arrow.PrimitiveTypes.Int64, `[null]`},
		{"null element", arrow.PrimitiveTypes.Int64, `[[null]]`},
		{"unsigned unsupported", arrow.PrimitiveTypes.Uint64, `[[1]]`},
		{"unsupported", arrow.PrimitiveTypes.Int32, `[[1]]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, arrow.ListOf(tc.dtype), strings.NewReader(tc.input))
			require.NoError(t, err)
			rows, err := array.NewStructArray([]arrow.Array{values}, []string{"value"})
			require.NoError(t, err)
			values.Release()
			column := arrow.NewChunked(rows.DataType(), []arrow.Array{rows})
			rows.Release()
			defer column.Release()
			out, err := ArrowChunkedStructColumnToFieldData("computed", column, nil)
			require.Nil(t, out)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}

// Build sliced chunks to exercise column export after a reducer has already
// selected/reordered its rows. This adapter is test-only.
type testTypedRowRef struct{ ResultIndex, RowIndex int }

func exportTestTypedRows(schema *arrow.Schema, records []arrow.Record, rows []testTypedRowRef) ([]*schemapb.FieldData, error) {
	var output []*schemapb.FieldData
	for i, field := range schema.Fields() {
		chunks := make([]arrow.Array, 0, len(rows))
		for _, ref := range rows {
			chunks = append(chunks, array.NewSlice(records[ref.ResultIndex].Column(i), int64(ref.RowIndex), int64(ref.RowIndex+1)))
		}
		column := arrow.NewChunked(field.Type, chunks)
		for _, chunk := range chunks {
			chunk.Release()
		}
		value, err := ArrowChunkedStructColumnToFieldData(field.Name, column, nil)
		column.Release()
		if err != nil {
			return nil, err
		}
		output = append(output, value)
	}
	return output, nil
}

func TestComputedStructArrayChildren(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	typ := arrow.StructOf(
		arrow.Field{Name: "content", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		arrow.Field{Name: "starts", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))},
		arrow.Field{Name: "ends", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))},
	)
	rows, _, err := array.FromJSON(pool, typ, strings.NewReader(`[
		{"content":["skip"],"starts":[[0]],"ends":[[4]]},
		{"content":["foo bar foo","hello foo"],"starts":[[0,8],[6]],"ends":[[3,11],[9]]},
		{"content":[],"starts":[],"ends":[]} ]`))
	require.NoError(t, err)
	sliced := array.NewSlice(rows, 1, 3)
	column := arrow.NewChunked(typ, []arrow.Array{sliced, sliced})
	sliced.Release()
	rows.Release()
	result, err := ArrowChunkedStructColumnToFieldData("matches", column, nil)
	column.Release()
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_ArrayOfStruct, result.Type)
	require.Zero(t, result.FieldId)
	fields := result.GetStructArrays().Fields
	require.Len(t, fields, 3)
	for i, field := range fields {
		require.Equal(t, int64(i+1), field.FieldId)
		require.Len(t, field.GetScalars().GetArrayData().Data, 4)
	}
	require.Equal(t, []string{"foo bar foo", "hello foo"}, fields[0].GetScalars().GetArrayData().Data[0].GetStringData().Data)
	require.Equal(t, []int64{0, 8}, fields[1].GetScalars().GetArrayData().Data[2].GetArrayData().Data[0].GetLongData().Data)
	require.Equal(t, []int64{9}, fields[2].GetScalars().GetArrayData().Data[2].GetArrayData().Data[1].GetLongData().Data)
	require.Empty(t, fields[0].GetScalars().GetArrayData().Data[1].GetStringData().Data)

	empty := arrow.NewChunked(typ, nil)
	defer empty.Release()
	result, err = ArrowChunkedStructColumnToFieldData("matches", empty, nil)
	require.NoError(t, err)
	require.Len(t, result.GetStructArrays().Fields, 3)
	require.Equal(t, schemapb.DataType_Array, result.GetStructArrays().Fields[1].GetScalars().GetArrayData().ElementType)

	for _, input := range []string{
		`[null]`,
		`[{"content":null,"starts":[],"ends":[]}]`,
		`[{"content":["x"],"starts":[],"ends":[[1]]}]`,
		`[{"content":[null],"starts":[[0]],"ends":[[1]]}]`,
		`[{"content":["x"],"starts":[[null]],"ends":[[1]]}]`,
	} {
		t.Run(input, func(t *testing.T) {
			rows, _, err := array.FromJSON(pool, typ, strings.NewReader(input))
			require.NoError(t, err)
			column := arrow.NewChunked(typ, []arrow.Array{rows})
			rows.Release()
			defer column.Release()
			result, err := ArrowChunkedStructColumnToFieldData("matches", column, nil)
			require.Nil(t, result)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}

func TestStructRejectsStandaloneLists(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dtype arrow.DataType
		input string
	}{
		{"array", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[[1,2],[]]`},
		{"nested", arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)), `[[[1,2],[]],[[3]]]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, tc.dtype, strings.NewReader(tc.input))
			require.NoError(t, err)
			defer values.Release()
			empty := array.NewSlice(values, 0, 0)
			defer empty.Release()
			for _, chunks := range [][]arrow.Array{nil, {empty}, {values}} {
				column := arrow.NewChunked(tc.dtype, chunks)
				out, err := ArrowChunkedStructColumnToFieldData("array", column, nil)
				column.Release()
				require.Nil(t, out)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			}
		})
	}
}

func TestComputedStructArrayRejectsUnsupportedNestedLeaf(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	dtype := arrow.StructOf(arrow.Field{
		Name: "values", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Uint64)),
	})
	// An empty chunk, empty document, or empty inner array must not hide an
	// unsupported leaf type now that inference only maps the current level.
	for _, input := range []string{`[]`, `[{"values":[]}]`, `[{"values":[[]]}]`, `[{"values":[[1]]}]`} {
		t.Run(input, func(t *testing.T) {
			values, _, err := array.FromJSON(pool, dtype, strings.NewReader(input))
			require.NoError(t, err)
			column := arrow.NewChunked(dtype, []arrow.Array{values})
			values.Release()
			defer column.Release()
			out, err := ArrowChunkedStructColumnToFieldData("matches", column, nil)
			require.Nil(t, out)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
	column := arrow.NewChunked(dtype, nil)
	defer column.Release()
	out, err := ArrowChunkedStructColumnToFieldData("matches", column, nil)
	require.Nil(t, out)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
