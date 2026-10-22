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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestTypedArrowChunks(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	// Arbitrary field names verify that transport conversion has no Grep semantics.
	typ := arrow.ListOf(arrow.StructOf(arrow.Field{Name: "label", Type: arrow.BinaryTypes.String}, arrow.Field{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}))
	schema := arrow.NewSchema([]arrow.Field{{Name: "calculated", Type: typ}}, nil)
	var records []arrow.Record
	for _, input := range []string{`[[{"label":"a","values":[1,2]}],[]]`, `[[],[{"label":"b","values":[3]}]]`} {
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

func TestTypedScalarColumns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   arrow.DataType
		input string
	}{
		{"string", arrow.BinaryTypes.String, `["first","second"]`},
		{"int64", arrow.PrimitiveTypes.Int64, `[1,2]`},
		{"bool", arrow.FixedWidthTypes.Boolean, `[false,true]`},
		{"double", arrow.PrimitiveTypes.Float64, `[1.5,2.5]`},
		{"list", arrow.ListOf(arrow.BinaryTypes.String), `[["first"],[]]`},
		{"nested", arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)), `[[[1,2],[]],[[3]]]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, tc.typ, strings.NewReader(tc.input))
			require.NoError(t, err)
			schema := arrow.NewSchema([]arrow.Field{{Name: tc.name, Type: tc.typ}}, nil)
			record := array.NewRecord(schema, []arrow.Array{values}, 2)
			defer func() {
				if record != nil {
					record.Release()
				}
			}()
			values.Release()
			refs := []testTypedRowRef{{1, 1}, {0, 0}, {1, 1}}
			result, err := exportTestTypedRows(schema, []arrow.Record{record, record}, refs)
			require.NoError(t, err)
			expected, err := exportScalarRange(record.Column(0), 0, 2)
			require.NoError(t, err)
			empty, err := exportTestTypedRows(schema, nil, nil)
			require.NoError(t, err)
			require.Equal(t, result[0].Type, empty[0].Type)
			require.Equal(t, result[0].FieldName, empty[0].FieldName)
			record.Release()
			record = nil
			// Compare wire values after releasing all Arrow owners.
			want := &schemapb.FieldData{Type: result[0].Type, FieldName: tc.name, Field: &schemapb.FieldData_Scalars{Scalars: expected}}
			selected := typeutil.PrepareResultFieldData([]*schemapb.FieldData{want}, 3)[0]
			typeutil.AppendFieldDataByColumn(selected, want, []int64{1, 0, 1})
			require.True(t, proto.Equal(selected, result[0]))
		})
	}
}

func TestTypedScalarErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   arrow.DataType
		input string
	}{
		{"null scalar", arrow.PrimitiveTypes.Int64, `[null]`},
		{"null list", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[null]`},
		{"null child", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[[null]]`},
		{"unsigned unsupported", arrow.PrimitiveTypes.Uint64, `[1]`},
		{"unsupported", arrow.PrimitiveTypes.Int32, `[1]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, tc.typ, strings.NewReader(tc.input))
			require.NoError(t, err)
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.typ}}, nil)
			record := array.NewRecord(schema, []arrow.Array{values}, 1)
			values.Release()
			defer record.Release()
			_, err = exportTestTypedRows(schema, []arrow.Record{record}, []testTypedRowRef{{0, 0}})
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
		value, err := ArrowChunkedTypedColumnToFieldData(field.Name, column)
		column.Release()
		if err != nil {
			return nil, err
		}
		output = append(output, value)
	}
	return output, nil
}
