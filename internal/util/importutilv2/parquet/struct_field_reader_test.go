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

package parquet

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestStructFieldReader_StringElementsOwnTheirBytes checks that a decoded row
// does not alias the arrow value buffer it was read from. The row is consumed
// long after the reader has moved on, and an aliased element would keep the
// whole buffer alive; clobbering the buffer proves the bytes were copied.
func TestStructFieldReader_StringElementsOwnTheirBytes(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "tag", Type: arrow.BinaryTypes.String})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	tb := sb.FieldBuilder(0).(*array.StringBuilder)

	appendElement := func(v *string) {
		sb.Append(true)
		if v == nil {
			tb.AppendNull()
			return
		}
		tb.Append(*v)
	}
	str := func(s string) *string { return &s }

	lb.Append(true) // ["aa", "bb"]
	appendElement(str("aa"))
	appendElement(str("bb"))
	lb.Append(true) // ["", "ccc"]
	appendElement(str(""))
	appendElement(str("ccc"))
	lb.Append(true) // [null, "dd"]
	appendElement(nil)
	appendElement(str("dd"))
	first := lb.NewArray()
	lb.Append(true) // [null, null]
	appendElement(nil)
	appendElement(nil)
	lb.Append(true) // []
	second := lb.NewArray()
	lb.Release()

	chunked := arrow.NewChunked(first.DataType(), []arrow.Array{first, second})
	first.Release()
	second.Release()
	t.Cleanup(chunked.Release)

	field := enScalarSubField(202, "s[tag]", schemapb.DataType_VarChar, true)
	reader := &StructFieldReader{structName: "s", field: field}
	data, _, err := reader.readArrayField(chunked)
	require.NoError(t, err)
	rows := data.([]*schemapb.ScalarField)
	require.Len(t, rows, 5)

	want := [][]string{{"aa", "bb"}, {"", "ccc"}, {"", "dd"}, {"", ""}, {}}
	require.Equal(t, want, stringRows(rows))
	require.Equal(t, []bool{false, true}, typeutil.GetArrayElementValidData(rows[2]), "first element is null")
	require.Equal(t, []bool{false, false}, typeutil.GetArrayElementValidData(rows[3]))
	require.Empty(t, typeutil.GetArrayElementValidData(rows[4]))

	for _, chunk := range chunked.Chunks() {
		tags, ok := chunk.(*array.List).ListValues().(*array.Struct).Field(0).(*array.String)
		require.True(t, ok)
		buf := tags.ValueBytes()
		for i := range buf {
			buf[i] = 'X'
		}
	}
	require.Equal(t, want, stringRows(rows), "decoded rows must not alias the arrow value buffer")
}

// TestStructFieldReader_StringElementNeedsStringChild keeps the type error when
// the declared element type is a string but the column carries another type.
func TestStructFieldReader_StringElementNeedsStringChild(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "tag", Type: arrow.PrimitiveTypes.Int32})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	ib := sb.FieldBuilder(0).(*array.Int32Builder)
	lb.Append(true)
	sb.Append(true)
	ib.Append(7)
	chunked := enChunk(t, lb)

	field := enScalarSubField(202, "s[tag]", schemapb.DataType_VarChar, true)
	reader := &StructFieldReader{structName: "s", field: field}
	_, _, err := reader.readArrayField(chunked)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expect")
	require.Contains(t, err.Error(), "int32")
}

// TestStructFieldReader_TextElementRejected pins that only String and VarChar
// elements are supported; Text has no struct element encoding.
func TestStructFieldReader_TextElementRejected(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "tag", Type: arrow.BinaryTypes.String})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	tb := sb.FieldBuilder(0).(*array.StringBuilder)
	lb.Append(true)
	sb.Append(true)
	tb.Append("aa")
	chunked := enChunk(t, lb)

	field := enScalarSubField(202, "s[tag]", schemapb.DataType_Text, true)
	reader := &StructFieldReader{structName: "s", field: field}
	_, _, err := reader.readArrayField(chunked)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported element type")
}

func stringRows(rows []*schemapb.ScalarField) [][]string {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.GetStringData().GetData())
	}
	return out
}
