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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDeclaredStructColumn(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	// Deliberately reverse Arrow/schema field order and use non-positional IDs.
	typ := arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "at", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "text", Type: arrow.BinaryTypes.String},
	))
	schema := &schemapb.StructArrayFieldSchema{Name: "items", FieldID: 100, Nullable: true, Fields: []*schemapb.FieldSchema{
		{Name: "text", FieldID: 102, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Text, Nullable: true},
		{Name: "at", FieldID: 107, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Timestamptz, Nullable: true},
	}}
	before := proto.Clone(schema)
	values, _, err := array.FromJSON(pool, typ, strings.NewReader(`[[{"text":"skip","at":0}],[{"text":"你好","at":7},{"text":"b","at":9}],null,[]]`))
	require.NoError(t, err)
	first := array.NewSlice(values, 1, 3)
	last := array.NewSlice(values, 3, 4)
	column := arrow.NewChunked(typ, []arrow.Array{first, last})
	first.Release()
	last.Release()
	values.Release()
	result, err := ArrowChunkedStructColumnToFieldData("items", column, schema)
	column.Release()
	require.NoError(t, err)
	require.True(t, proto.Equal(before, schema))
	require.Equal(t, int64(100), result.FieldId)
	children := result.GetStructArrays().Fields
	require.Equal(t, int64(102), children[0].FieldId)
	require.Equal(t, int64(107), children[1].FieldId)
	require.Equal(t, schemapb.DataType_Text, children[0].GetScalars().GetArrayData().ElementType)
	require.Equal(t, schemapb.DataType_Timestamptz, children[1].GetScalars().GetArrayData().ElementType)
	require.Equal(t, []string{"你好", "b"}, children[0].GetScalars().GetArrayData().Data[0].GetStringData().Data)
	require.Equal(t, []int64{7, 9}, children[1].GetScalars().GetArrayData().Data[0].GetTimestamptzData().Data)
	for _, child := range children {
		require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(child))
		require.Len(t, child.GetScalars().GetArrayData().Data, 3)
	}
	// The identical Arrow layout can also be an inferred computed result.
	empty := arrow.NewChunked(typ, nil)
	defer empty.Release()
	inferred, err := ArrowChunkedTypedColumnToFieldData("calculated", empty)
	require.NoError(t, err)
	require.Zero(t, inferred.FieldId)
	require.Equal(t, int64(1), inferred.GetStructArrays().Fields[0].FieldId)
	require.Equal(t, schemapb.DataType_Int64, inferred.GetStructArrays().Fields[0].GetScalars().GetArrayData().ElementType)
	declared, err := ArrowChunkedStructColumnToFieldData("items", empty, schema)
	require.NoError(t, err)
	require.Equal(t, int64(102), declared.GetStructArrays().Fields[0].FieldId)
	require.Empty(t, declared.GetStructArrays().Fields[0].GetScalars().GetArrayData().Data)
}

func TestDeclaredStructRejectsMalformedData(t *testing.T) {
	typ := arrow.ListOf(arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64}))
	base := &schemapb.StructArrayFieldSchema{Name: "items", FieldID: 100, Fields: []*schemapb.FieldSchema{
		{Name: "value", FieldID: 101, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
	}}
	for _, tc := range []struct {
		name   string
		input  string
		change func(*schemapb.StructArrayFieldSchema)
	}{
		{"null document", `[null]`, nil},
		{"null struct element", `[[null]]`, nil},
		{"null child element", `[[{"value":null}]]`, nil},
		{"wrong type on empty", `[]`, func(s *schemapb.StructArrayFieldSchema) { s.Fields[0].ElementType = schemapb.DataType_Text }},
		{"missing child", `[]`, func(s *schemapb.StructArrayFieldSchema) { s.Fields[0].Name = "missing" }},
		{"wrong parent", `[]`, func(s *schemapb.StructArrayFieldSchema) { s.Name = "other" }},
		{"non-array child", `[]`, func(s *schemapb.StructArrayFieldSchema) { s.Fields[0].DataType = schemapb.DataType_Int64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			s := proto.Clone(base).(*schemapb.StructArrayFieldSchema)
			if tc.change != nil {
				tc.change(s)
			}
			values, _, err := array.FromJSON(pool, typ, strings.NewReader(tc.input))
			require.NoError(t, err)
			column := arrow.NewChunked(typ, []arrow.Array{values})
			values.Release()
			defer column.Release()
			output, err := ArrowChunkedStructColumnToFieldData("items", column, s)
			require.Nil(t, output)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}

func TestDeclaredStructVectorChild(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	typ := arrow.ListOf(arrow.StructOf(arrow.Field{Name: "vector", Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}}))
	b := array.NewListBuilder(pool, typ.Elem())
	sb := b.ValueBuilder().(*array.StructBuilder)
	vb := sb.FieldBuilder(0).(*array.FixedSizeBinaryBuilder)
	b.Append(true)
	sb.Append(true)
	vb.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
	sb.Append(true)
	vb.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
	b.AppendNull()
	values := b.NewArray()
	b.Release()
	column := arrow.NewChunked(typ, []arrow.Array{values})
	values.Release()
	defer column.Release()
	schema := &schemapb.StructArrayFieldSchema{Name: "items", FieldID: 100, Nullable: true, Fields: []*schemapb.FieldSchema{
		{Name: "vector", FieldID: 101, DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}
	out, err := ArrowChunkedStructColumnToFieldData("items", column, schema)
	require.NoError(t, err)
	child := out.GetStructArrays().Fields[0]
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(child))
	vectors := child.GetVectors().GetVectorArray()
	require.Equal(t, int64(2), vectors.Dim)
	require.Equal(t, schemapb.DataType_FloatVector, vectors.ElementType)
	require.Equal(t, []float32{1, 2, 3, 4}, vectors.Data[0].GetFloatVector().Data)
	require.Empty(t, vectors.Data[1].GetFloatVector().Data)
	schema.Fields[0].TypeParams[0].Value = "3"
	_, err = ArrowChunkedStructColumnToFieldData("items", column, schema)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
