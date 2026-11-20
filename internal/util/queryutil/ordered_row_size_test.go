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

package queryutil

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestFieldDataRowSizeScalars(t *testing.T) {
	variants := (&schemapb.ScalarField{}).ProtoReflect().Descriptor().Oneofs().ByName("data").Fields()
	for i := 0; i < variants.Len(); i++ {
		variant := variants.Get(i)
		switch variant.Name() {
		case "date_data", "time_data", "mol_smiles_data":
			continue // Not produced by Query or Search results.
		}
		t.Run(string(variant.Name()), func(t *testing.T) {
			array := dynamicpb.NewMessage(variant.Message())
			dataField := array.Descriptor().Fields().ByName("data")
			values := array.Mutable(dataField).List()
			var expected []int
			for _, n := range []int{0, 127, 128, 16384, -1} {
				var value protoreflect.Value
				var size int
				switch dataField.Kind() {
				case protoreflect.BoolKind:
					value, size = protoreflect.ValueOfBool(n != 0), 1
				case protoreflect.Int32Kind:
					value, size = protoreflect.ValueOfInt32(int32(n)), 4
				case protoreflect.Int64Kind:
					value, size = protoreflect.ValueOfInt64(int64(n)), 8
				case protoreflect.FloatKind:
					value, size = protoreflect.ValueOfFloat32(float32(n)), 4
				case protoreflect.DoubleKind:
					value, size = protoreflect.ValueOfFloat64(float64(n)), 8
				case protoreflect.StringKind:
					size = max(0, n)
					value = protoreflect.ValueOfString(strings.Repeat("x", size))
				case protoreflect.BytesKind:
					size = max(0, n)
					value = protoreflect.ValueOfBytes(make([]byte, size))
				case protoreflect.MessageKind:
					leaf := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(n), -1}}}}
					nested := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{leaf, leaf}}}}
					value, size = protoreflect.ValueOfMessage(nested.ProtoReflect()), proto.Size(nested)
				default:
					t.Fatalf("add a sizing fixture for %s", dataField.Kind())
				}
				values.Append(value)
				expected = append(expected, size)
			}
			scalar := &schemapb.ScalarField{ValidData: []bool{true, false, true, true, true}}
			encoded, err := proto.Marshal(array)
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(encoded, scalar.ProtoReflect().Mutable(variant).Message().Interface()))
			field := &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: scalar}}
			fields := []*schemapb.FieldData{field}
			ordered := NewOrderedRowSizeCalculator(fields, nil, nil)
			batch := newRowSizeCalculator(&internalpb.RetrieveResults{FieldsData: fields})
			for row, want := range expected {
				require.EqualValues(t, want, ordered.RowSize(int64(row)), "row %d", row)
				require.EqualValues(t, want, batch.rowSize(int64(row)), "Batch row %d", row)
			}
		})
	}
}

func TestFieldDataRowSizeVectors(t *testing.T) {
	for _, tc := range []struct {
		vector *schemapb.VectorField
		kind   schemapb.DataType
		first  int
		last   int
	}{
		{&schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 16)}}}, schemapb.DataType_FloatVector, 32, 32},
		{&schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 2)}}, schemapb.DataType_BinaryVector, 1, 1},
		{&schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 32)}}, schemapb.DataType_Float16Vector, 16, 16},
		{&schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 32)}}, schemapb.DataType_BFloat16Vector, 16, 16},
		{&schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, 16)}}, schemapb.DataType_Int8Vector, 8, 8},
		{&schemapb.VectorField{Dim: 1000, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{make([]byte, 8), make([]byte, 16)}}}}, schemapb.DataType_SparseFloatVector, 8, 16},
	} {
		tc.vector.ValidData = []bool{true, false, true}
		fields := []*schemapb.FieldData{{Type: tc.kind, Field: &schemapb.FieldData_Vectors{Vectors: tc.vector}}}
		ordered := NewOrderedRowSizeCalculator(fields, nil, nil)
		batch := newRowSizeCalculator(&internalpb.RetrieveResults{FieldsData: fields})
		require.Nil(t, ordered.compactIndices, "stream sizing must not allocate a row-sized vector index")
		for row, want := range []int{tc.first, 0, tc.last} {
			require.EqualValues(t, want, ordered.RowSize(int64(row)), "%s row %d", tc.kind, row)
			require.EqualValues(t, want, batch.rowSize(int64(row)), "%s Batch row %d", tc.kind, row)
		}
	}
}

func TestFieldDataRowSizeNested(t *testing.T) {
	value := &schemapb.VectorField{Dim: 128, ValidData: []bool{true, true}, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 256)}}}
	vectorArray := &schemapb.VectorArray{Dim: 128, ElementType: schemapb.DataType_FloatVector, Data: []*schemapb.VectorField{value}}
	field := &schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_VectorArray{VectorArray: vectorArray}}}}
	structArray := &schemapb.FieldData{Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{field}}}}
	for _, input := range []*schemapb.FieldData{field, structArray} {
		fields := []*schemapb.FieldData{input}
		ordered := NewOrderedRowSizeCalculator(fields, nil, nil)
		batch := newRowSizeCalculator(&internalpb.RetrieveResults{FieldsData: fields})
		require.EqualValues(t, proto.Size(value), ordered.RowSize(0))
		require.EqualValues(t, proto.Size(value), batch.rowSize(0))
	}
	field.GetVectors().ValidData = []bool{false}
	vectorArray.Data = nil
	calc := NewOrderedRowSizeCalculator([]*schemapb.FieldData{field}, nil, nil)
	require.Zero(t, calc.RowSize(0))
}

func BenchmarkNullableVectorSizeCalculatorMemory(b *testing.B) {
	for _, rows := range []int{1024, 65536} {
		valid := make([]bool, rows)
		for i := 0; i < rows; i += 2 {
			valid[i] = true
		}
		fields := []*schemapb.FieldData{{
			Type: schemapb.DataType_FloatVector, ValidData: valid,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, rows/2*8)}},
			}},
		}}
		result := &internalpb.RetrieveResults{FieldsData: fields}
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			b.Run("cursor_before", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = typeutil.NewFieldDataIdxComputer(fields)
				}
			})
			b.Run("ordered", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = NewOrderedRowSizeCalculator(fields, nil, nil)
				}
			})
			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = newRowSizeCalculator(result)
				}
			})
		})
	}
}
