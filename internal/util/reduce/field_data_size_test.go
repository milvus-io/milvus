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

package reduce

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRetrieveUnitFieldNumbers(t *testing.T) {
	fields := (&internalpb.RetrieveResults{}).ProtoReflect().Descriptor().Fields()
	for _, tc := range []struct {
		name     protoreflect.Name
		number   protowire.Number
		repeated bool
	}{
		{"ids", RetrieveIDsFieldNumber, false},
		{"fields_data", RetrieveFieldsDataFieldNumber, true},
		{"element_indices", RetrieveElementIndicesFieldNumber, true},
		{"computed_results", RetrieveComputedResultsFieldNumber, true},
	} {
		t.Run(string(tc.name), func(t *testing.T) {
			field := fields.ByName(tc.name)
			require.NotNil(t, field)
			require.Equal(t, tc.number, field.Number())
			require.Equal(t, protoreflect.MessageKind, field.Kind())
			require.Equal(t, tc.repeated, field.IsList())
		})
	}
}

func TestSearchUnitFieldNumbers(t *testing.T) {
	fields := (&schemapb.SearchResultData{}).ProtoReflect().Descriptor().Fields()
	for _, tc := range []struct {
		name     protoreflect.Name
		number   protowire.Number
		kind     protoreflect.Kind
		repeated bool
	}{
		{"fields_data", SearchFieldsDataFieldNumber, protoreflect.MessageKind, true},
		{"scores", SearchScoresFieldNumber, protoreflect.FloatKind, true},
		{"ids", SearchIDsFieldNumber, protoreflect.MessageKind, false},
		{"group_by_field_value", SearchGroupByFieldValueFieldNumber, protoreflect.MessageKind, false},
		{"element_indices", SearchElementIndicesFieldNumber, protoreflect.MessageKind, false},
		{"group_by_field_values", SearchGroupByFieldValuesFieldNumber, protoreflect.MessageKind, true},
	} {
		t.Run(string(tc.name), func(t *testing.T) {
			field := fields.ByName(tc.name)
			require.NotNil(t, field)
			require.Equal(t, tc.number, field.Number())
			require.Equal(t, tc.kind, field.Kind())
			require.Equal(t, tc.repeated, field.IsList())
		})
	}
	field := (&schemapb.LongArray{}).ProtoReflect().Descriptor().Fields().ByName("data")
	require.NotNil(t, field)
	require.Equal(t, LongArrayDataFieldNumber, field.Number())
	require.Equal(t, protoreflect.Int64Kind, field.Kind())
	require.True(t, field.IsPacked())
	require.True(t, fields.ByName("scores").IsPacked())
}

// Size parity alone cannot distinguish incorrect field numbers with equal tag
// widths. Pin the descriptors separately, including newly added schema fields.
func TestFieldDataSizeFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		message proto.Message
		fields  map[protoreflect.Name]protowire.Number
	}{
		{&schemapb.IDs{}, map[protoreflect.Name]protowire.Number{
			"int_id": idsIntIDFieldNumber, "str_id": idsStrIDFieldNumber,
			"uuid_id": 3, // Known schema alternative; row sizing does not support it yet.
		}},
		{&schemapb.FieldData{}, map[protoreflect.Name]protowire.Number{
			"type": fieldDataTypeFieldNumber, "field_name": fieldDataNameFieldNumber,
			"scalars": fieldDataScalarsFieldNumber, "vectors": fieldDataVectorsFieldNumber,
			"field_id": fieldDataIDFieldNumber, "is_dynamic": fieldDataIsDynamicFieldNumber,
			"valid_data": fieldDataValidDataFieldNumber, "struct_arrays": fieldDataStructArraysFieldNumber,
		}},
		{&schemapb.ScalarField{}, map[protoreflect.Name]protowire.Number{
			"bool_data": scalarBoolDataFieldNumber, "int_data": scalarIntDataFieldNumber,
			"long_data": scalarLongDataFieldNumber, "float_data": scalarFloatDataFieldNumber,
			"double_data": scalarDoubleDataFieldNumber, "string_data": scalarStringDataFieldNumber,
			"bytes_data": scalarBytesDataFieldNumber, "array_data": scalarArrayDataFieldNumber,
			"json_data": scalarJSONDataFieldNumber, "geometry_data": scalarGeometryDataFieldNumber,
			"timestamptz_data": scalarTimestamptzDataFieldNumber, "geometry_wkt_data": scalarGeometryWktDataFieldNumber,
			"mol_data": scalarMolDataFieldNumber, "mol_smiles_data": scalarMolSmilesDataFieldNumber,
			"date_data": scalarDateDataFieldNumber, "time_data": scalarTimeDataFieldNumber,
			"valid_data": scalarValidDataFieldNumber,
		}},
		{&schemapb.VectorField{}, map[protoreflect.Name]protowire.Number{
			"dim": vectorDimFieldNumber, "float_vector": vectorFloatVectorFieldNumber,
			"binary_vector": vectorBinaryVectorFieldNumber, "float16_vector": vectorFloat16VectorFieldNumber,
			"bfloat16_vector": vectorBfloat16VectorFieldNumber, "sparse_float_vector": vectorSparseFloatVectorFieldNumber,
			"int8_vector": vectorInt8VectorFieldNumber, "vector_array": vectorArrayFieldNumber,
			"valid_data": vectorValidDataFieldNumber,
		}},
		{&schemapb.ArrayArray{}, map[protoreflect.Name]protowire.Number{
			"data": arrayDataFieldNumber, "element_type": arrayElementTypeFieldNumber,
		}},
		{&schemapb.SparseFloatArray{}, map[protoreflect.Name]protowire.Number{
			"contents": arrayDataFieldNumber, "dim": sparseFloatDimFieldNumber,
		}},
		{&schemapb.VectorArray{}, map[protoreflect.Name]protowire.Number{
			"dim": vectorArrayDimFieldNumber, "data": vectorArrayDataFieldNumber,
			"element_type": vectorArrayElementTypeFieldNumber,
		}},
		{&schemapb.StructArrayField{}, map[protoreflect.Name]protowire.Number{
			"fields": structArrayFieldsFieldNumber,
		}},
	} {
		descriptor := tc.message.ProtoReflect().Descriptor()
		t.Run(string(descriptor.Name()), func(t *testing.T) {
			fields := descriptor.Fields()
			require.Equal(t, len(tc.fields), fields.Len(), "schema changed: review Unit sizing")
			for name, number := range tc.fields {
				field := fields.ByName(name)
				require.NotNil(t, field, "missing field %s", name)
				require.Equal(t, number, field.Number(), "field %s", name)
				if name == "valid_data" {
					require.Equal(t, protoreflect.BoolKind, field.Kind())
					require.True(t, field.IsPacked())
				}
			}
		})
	}
}

func TestFieldDataRowSizeScalars(t *testing.T) {
	variants := (&schemapb.ScalarField{}).ProtoReflect().Descriptor().Oneofs().ByName("data").Fields()
	for i := 0; i < variants.Len(); i++ {
		variant := variants.Get(i)
		t.Run(string(variant.Name()), func(t *testing.T) {
			array := dynamicpb.NewMessage(variant.Message())
			dataField := array.Descriptor().Fields().ByName("data")
			require.Equal(t, arrayDataFieldNumber, dataField.Number())
			values := array.Mutable(dataField).List()
			for _, n := range []int64{0, 127, 128, 16384, -1} {
				var value protoreflect.Value
				switch dataField.Kind() {
				case protoreflect.BoolKind:
					value = protoreflect.ValueOfBool(n != 0)
				case protoreflect.Int32Kind:
					value = protoreflect.ValueOfInt32(int32(n))
				case protoreflect.Int64Kind:
					value = protoreflect.ValueOfInt64(n)
				case protoreflect.FloatKind:
					value = protoreflect.ValueOfFloat32(float32(n))
				case protoreflect.DoubleKind:
					value = protoreflect.ValueOfFloat64(float64(n))
				case protoreflect.StringKind:
					value = protoreflect.ValueOfString(strings.Repeat("x", int(max(0, n))))
				case protoreflect.BytesKind:
					value = protoreflect.ValueOfBytes([]byte(strings.Repeat("x", int(max(0, n)))))
				case protoreflect.MessageKind:
					value = protoreflect.ValueOfMessage((&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{n, -1}}}}).ProtoReflect())
				default:
					t.Fatalf("add a sizing fixture for %s", dataField.Kind())
				}
				values.Append(value)
			}
			if elementType := array.Descriptor().Fields().ByName("element_type"); elementType != nil {
				array.Set(elementType, protoreflect.ValueOfEnum(protoreflect.EnumNumber(schemapb.DataType_Int64)))
			}
			scalar := &schemapb.ScalarField{ValidData: []bool{true, false, true, true, true}}
			encoded, err := proto.Marshal(array)
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(encoded, scalar.ProtoReflect().Mutable(variant).Message().Interface()))
			field := &schemapb.FieldData{Type: schemapb.DataType_Array, FieldName: "values", FieldId: 16384, IsDynamic: true, Field: &schemapb.FieldData_Scalars{Scalars: scalar}}
			for row := 0; row < values.Len(); row++ {
				reference := proto.Clone(field).(*schemapb.FieldData)
				refScalar := reference.GetScalars()
				refValues := refScalar.ProtoReflect().Get(variant).Message().Mutable(dataField).List()
				value := refValues.Get(row)
				refValues.Truncate(0)
				refValues.Append(value)
				typeutil.SetFieldDataValidData(reference, refScalar.ValidData[row:row+1])
				got, err := FieldDataRowSize(field, int64(row), int64(row), false)
				require.NoError(t, err)
				require.Equal(t, proto.Size(reference), got, "row %d", row)
			}
		})
	}
}

func TestFieldDataRowSizeVectors(t *testing.T) {
	vectors := []*schemapb.VectorField{
		{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 16)}}},
		{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 2)}},
		{Dim: 8, Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 32)}},
		{Dim: 8, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 32)}},
		{Dim: 8, Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, 16)}},
		{Dim: 1000, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 1000, Contents: [][]byte{make([]byte, 8), make([]byte, 16)}}}},
	}
	types := []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector}
	for i, vector := range vectors {
		t.Run(types[i].String(), func(t *testing.T) {
			vector.ValidData = []bool{true, false, true}
			field := &schemapb.FieldData{Type: types[i], FieldId: 100, FieldName: "vector", Field: &schemapb.FieldData_Vectors{Vectors: vector}}
			for row, dataRow := range []int64{0, 1, 1} {
				end := dataRow + 1
				if row == 1 {
					end = dataRow
				}
				views, ok := typeutil.CreateFieldDataRangeView([]*schemapb.FieldData{field}, int64(row), int64(row+1), []int64{dataRow}, []int64{end})
				require.True(t, ok)
				// The reference retains the source container metadata and an empty
				// vector oneof for nulls, rather than the range-view normalization.
				if row == 1 {
					empty := typeutil.PrepareResultFieldData([]*schemapb.FieldData{field}, 0)[0]
					views[0].GetVectors().Data = empty.GetVectors().Data
				}
				if sparse := vector.GetSparseFloatVector(); sparse != nil {
					dim := sparse.Dim
					if row == 1 {
						dim = 0
					}
					views[0].GetVectors().Dim = dim
					views[0].GetVectors().GetSparseFloatVector().Dim = dim
				}
				got, err := FieldDataRowSize(field, int64(row), dataRow, false)
				require.NoError(t, err)
				require.Equal(t, proto.Size(views[0]), got, "row %d", row)
			}
		})
	}
}

func TestFieldDataRowSizeNested(t *testing.T) {
	field := &schemapb.FieldData{Type: schemapb.DataType_ArrayOfVector, FieldId: 101, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 128, ElementType: schemapb.DataType_FloatVector, Data: []*schemapb.VectorField{{Dim: 128, ValidData: []bool{true, true}, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 256)}}}}}}}}}
	got, err := FieldDataRowSize(field, 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, proto.Size(field), got)
	field = &schemapb.FieldData{FieldId: 103, FieldName: "struct", Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{field}}}}
	got, err = FieldDataRowSize(field, 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, proto.Size(field), got)
}

func TestFieldDataRowSizeNullVectorArrays(t *testing.T) {
	for _, elementType := range []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector,
	} {
		t.Run(elementType.String(), func(t *testing.T) {
			field := &schemapb.FieldData{
				Type: schemapb.DataType_ArrayOfVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 128, ValidData: []bool{false},
					Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 128, ElementType: elementType}},
				}},
			}
			reference := proto.Clone(field).(*schemapb.FieldData)
			empty, err := typeutil.NewEmptyArrayOfVectorRow(128, elementType)
			require.NoError(t, err)
			reference.GetVectors().GetVectorArray().Data = []*schemapb.VectorField{empty}
			typeutil.SetFieldDataValidData(reference, []bool{false})
			got, err := FieldDataRowSize(field, 0, -1, false)
			require.NoError(t, err)
			require.Equal(t, proto.Size(reference), got)
		})
	}
}

func TestFieldDataRowSizeInvalidData(t *testing.T) {
	field := &schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}}
	for _, row := range []int64{-1, 1} {
		_, err := FieldDataRowSize(field, row, row, false)
		require.Error(t, err)
	}
	_, err := FieldDataRowSize(nil, 0, 0, false)
	require.Error(t, err)
	field.GetScalars().ValidData = []bool{false}
	field.GetScalars().GetLongData().Data = nil
	got, err := FieldDataRowSize(field, 0, 0, false)
	require.NoError(t, err)
	field.GetScalars().GetLongData().Data = []int64{0}
	typeutil.SetFieldDataValidData(field, []bool{false})
	require.Equal(t, proto.Size(field), got)
	field.GetScalars().ValidData = nil
	field.ValidData = []bool{true}
	got, err = FieldDataRowSize(field, 0, 0, false)
	require.NoError(t, err)
	typeutil.SetFieldDataValidData(field, []bool{true})
	require.Equal(t, proto.Size(field), got)
}
