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
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func requireLogicalSize(t *testing.T, size *datapb.FieldLogicalSize, payload, validity uint64) {
	t.Helper()
	require.NotNil(t, size, "missing logical-size entry")
	require.Equal(t, payload, size.PayloadBytes)
	require.Equal(t, validity, size.ValidityBits)
}

func logicalSizeForTest(f *schemapb.FieldSchema, a arrow.Array, rows int) *datapb.FieldLogicalSize {
	c := logicalSizeColumn{id: f.GetFieldID(), logicalValueType: logicalTypeForField(f, f.GetNullable())}
	batch := (&logicalSizeLayout{}).newBatch(int64(rows))
	c.measure(batch, a)
	return batch.Fields[f.GetFieldID()]
}

func logicalBinaryForTest(values ...[]byte) *array.Binary {
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	for _, value := range values {
		if value == nil {
			b.AppendNull()
		} else {
			b.Append(value)
		}
	}
	return b.NewBinaryArray()
}

func TestFieldLogicalSizeFixedWidths(t *testing.T) {
	for _, tc := range []struct {
		dt    schemapb.DataType
		arrow arrow.DataType
		width int
		dim   int
	}{
		{schemapb.DataType_Bool, arrow.FixedWidthTypes.Boolean, 1, 0},
		{schemapb.DataType_Int8, arrow.PrimitiveTypes.Int8, 1, 0},
		{schemapb.DataType_Int16, arrow.PrimitiveTypes.Int16, 2, 0},
		{schemapb.DataType_Int32, arrow.PrimitiveTypes.Int32, 4, 0},
		{schemapb.DataType_Float, arrow.PrimitiveTypes.Float32, 4, 0},
		{schemapb.DataType_Int64, arrow.PrimitiveTypes.Int64, 8, 0},
		{schemapb.DataType_Double, arrow.PrimitiveTypes.Float64, 8, 0},
		{schemapb.DataType_Timestamptz, arrow.PrimitiveTypes.Int64, 8, 0},
		{schemapb.DataType_BinaryVector, arrow.BinaryTypes.Binary, 2, 9},
		{schemapb.DataType_Int8Vector, arrow.BinaryTypes.Binary, 3, 3},
		{schemapb.DataType_Float16Vector, arrow.BinaryTypes.Binary, 6, 3},
		{schemapb.DataType_BFloat16Vector, arrow.BinaryTypes.Binary, 6, 3},
		{schemapb.DataType_FloatVector, arrow.BinaryTypes.Binary, 12, 3},
	} {
		t.Run(tc.dt.String(), func(t *testing.T) {
			f := &schemapb.FieldSchema{DataType: tc.dt, Nullable: true}
			if tc.dim > 0 {
				f.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(tc.dim)}}
			}
			b := array.NewBuilder(memory.DefaultAllocator, tc.arrow)
			defer b.Release()
			for _, valid := range []bool{true, true, false, true, false, true} {
				if !valid {
					b.AppendNull()
				} else if binary, ok := b.(*array.BinaryBuilder); ok {
					binary.Append(make([]byte, tc.width))
				} else {
					b.AppendEmptyValue()
				}
			}
			original := b.NewArray()
			defer original.Release()
			slice := array.NewSlice(original, 1, 6)
			defer slice.Release()
			// A record can have fewer rows than its backing columns.
			requireLogicalSize(t, logicalSizeForTest(f, slice, 3), uint64(2*tc.width), 3)
			if tc.dim > 0 {
				f.Nullable = false
				fixed := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: tc.width})
				defer fixed.Release()
				fixed.AppendEmptyValues(2)
				values := fixed.NewArray()
				defer values.Release()
				requireLogicalSize(t, logicalSizeForTest(f, values, 2), uint64(2*tc.width), 0)
			}
		})
	}
}

func TestFieldLogicalSizeVariablePayload(t *testing.T) {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues([]string{"discard", "猫", "", "ignored", "suffix"}, []bool{true, true, true, false, true})
	stringsCol := b.NewArray()
	b.Release()
	defer stringsCol.Release()
	slice := array.NewSlice(stringsCol, 1, 5)
	defer slice.Release()
	for _, dt := range []schemapb.DataType{schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text} {
		requireLogicalSize(t, logicalSizeForTest(&schemapb.FieldSchema{DataType: dt, Nullable: true}, slice, 3), 3, 3)
	}
	for _, tc := range []struct {
		dt     schemapb.DataType
		values [][]byte
		want   uint64
	}{
		{schemapb.DataType_JSON, [][]byte{[]byte(` {"x": ` + strconv.QuoteToASCII("猫") + `} `), nil, []byte(`{}`)}, 19},
		{schemapb.DataType_Geometry, [][]byte{make([]byte, 21), nil, make([]byte, 41)}, 62},
		{schemapb.DataType_SparseFloatVector, [][]byte{make([]byte, 16), nil, make([]byte, 8)}, 24},
	} {
		col := logicalBinaryForTest(tc.values...)
		requireLogicalSize(t, logicalSizeForTest(&schemapb.FieldSchema{DataType: tc.dt, Nullable: true}, col, 3), tc.want, 3)
		col.Release()
	}
}

func TestFieldLogicalSizeTextRefs(t *testing.T) {
	inline := append([]byte{0}, strings.Repeat("x", 23)...)
	external := append([]byte{1}, make([]byte, 23)...)
	col := logicalBinaryForTest(external, nil, []byte{0}, inline, external, []byte{}, []byte{2})
	defer col.Release()
	f := &schemapb.FieldSchema{DataType: schemapb.DataType_Text, Nullable: true}
	slice := array.NewSlice(col, 1, 5)
	defer slice.Release()
	requireLogicalSize(t, logicalSizeForTest(f, slice, 3), 23, 3)
	require.Nil(t, logicalSizeForTest(f, slice, 4), "a surviving external ref makes only this field unknown")
	for _, r := range [][2]int{{0, 1}, {4, 5}, {5, 6}, {6, 7}, {-1, 0}, {0, 8}, {2, 1}} {
		n, ok := TextLogicalPayloadBytes(col, r[0], r[1])
		require.False(t, ok)
		require.Zero(t, n)
	}
	for _, r := range [][2]int{{1, 2}, {2, 3}, {7, 7}} {
		n, ok := TextLogicalPayloadBytes(col, r[0], r[1])
		require.True(t, ok)
		require.Zero(t, n)
	}
}

func TestFieldLogicalSizeScalarArrays(t *testing.T) {
	// The legacy protobuf uses varints even for Int16 and can carry an empty
	// typed array. Count array values, not the marshaled length or Arrow offsets.
	f := &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int16, Nullable: true}
	encoded, err := proto.Marshal(&schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 300, -3}}}})
	require.NoError(t, err)
	legacy := logicalBinaryForTest(encoded, []byte{}, nil, []byte{0xff})
	defer legacy.Release()
	requireLogicalSize(t, logicalSizeForTest(f, legacy, 3), 6, 3)
	require.Nil(t, logicalSizeForTest(f, legacy, 4), "a malformed protobuf is unknown, not a zero")

	// Parent NULL has a physically nonempty span; it owns no element positions.
	f.ElementNullable = true
	b := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int16)
	defer b.Release()
	values := b.ValueBuilder().(*array.Int16Builder)
	b.Append(true)
	values.Append(300) // prefix outside the slice
	b.Append(true)
	values.AppendValues([]int16{7, 8}, []bool{true, false})
	b.Append(true) // empty list
	b.Append(false)
	values.Append(900)
	all := b.NewArray()
	defer all.Release()
	slice := array.NewSlice(all, 1, 4)
	defer slice.Release()
	requireLogicalSize(t, logicalSizeForTest(f, slice, 3), 2, 5)
}

func TestFieldLogicalSizeNestedArrays(t *testing.T) {
	leaf := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_VarChar}}
	inner := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
	f := &schemapb.FieldSchema{DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
		TypeSchema: &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}}
	outer := array.NewListBuilder(memory.DefaultAllocator, arrow.ListOf(arrow.BinaryTypes.String))
	defer outer.Release()
	children := outer.ValueBuilder().(*array.ListBuilder)
	values := children.ValueBuilder().(*array.StringBuilder)
	outer.Append(true)
	children.Append(true)
	values.Append("猫")
	values.AppendNull()
	children.Append(false)
	values.Append("ignored child of null")
	outer.Append(true) // []
	outer.AppendNull()
	col := outer.NewArray()
	defer col.Release()
	requireLogicalSize(t, logicalSizeForTest(f, col, 3), 3, 7) // 3 rows + 2 inner lists + 2 strings

	// The same nested row in protobuf form, with compact inner/leaf masks.
	row := &schemapb.ScalarField{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
		ElementType: schemapb.DataType_VarChar, Data: []*schemapb.ScalarField{{ValidData: []bool{true, false},
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"猫"}}}}},
	}}}
	encoded, err := proto.Marshal(row)
	require.NoError(t, err)
	legacy := logicalBinaryForTest(encoded, []byte{}, nil)
	defer legacy.Release()
	requireLogicalSize(t, logicalSizeForTest(f, legacy, 3), 3, 7)
}

func TestFieldLogicalSizeVectorArrays(t *testing.T) {
	for _, nullable := range []bool{true, false} {
		f := &schemapb.FieldSchema{DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
			Nullable: true, ElementNullable: nullable, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "3"}}}
		// The legacy fixed child is physically annotated nullable even when
		// the schema is element-non-nullable. It must not be charged bits.
		var child arrow.DataType = &arrow.FixedSizeBinaryType{ByteWidth: 12}
		if nullable {
			child = arrow.BinaryTypes.Binary
		}
		b := array.NewListBuilder(memory.DefaultAllocator, child)
		b.Append(true)
		wantBits := uint64(3)
		if nullable {
			v := b.ValueBuilder().(*array.BinaryBuilder)
			v.Append(make([]byte, 12))
			v.AppendNull()
			v.Append(make([]byte, 12))
			wantBits += 3
		} else {
			b.ValueBuilder().AppendEmptyValues(2)
		}
		b.AppendNull()
		b.Append(true)
		col := b.NewArray()
		b.Release()
		requireLogicalSize(t, logicalSizeForTest(f, col, 3), 24, wantBits)
		col.Release()
	}
}

func TestFieldLogicalSizeOverflow(t *testing.T) {
	n := uint64(math.MaxUint64 - 1)
	require.False(t, addLogicalCount(&n, 2))
	require.False(t, addLogicalProduct(&n, math.MaxUint64, 8))
	require.Equal(t, uint64(math.MaxUint64-1), n)
	require.True(t, addLogicalProduct(&n, 1, 1))
	require.Equal(t, uint64(math.MaxUint64), n)
}
