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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	pkgcommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const testStructName = "clips"

func testStructSchema(fields ...*schemapb.FieldSchema) *schemapb.StructArrayFieldSchema {
	return &schemapb.StructArrayFieldSchema{FieldID: 200, Name: testStructName, Fields: fields}
}

func scalarSubField(id int64, name string, elementType schemapb.DataType, elementNullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName(testStructName, name),
		DataType:        schemapb.DataType_Array,
		ElementType:     elementType,
		ElementNullable: elementNullable,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: pkgcommon.MaxCapacityKey, Value: "4"},
			{Key: pkgcommon.MaxLengthKey, Value: "8"},
		},
	}
}

// nestedSubField builds an Array<Array<leaf>> sub-field: outer capacity 4,
// inner capacity 3, VarChar leaf max_length 4.
func nestedSubField(id int64, name string, leaf schemapb.DataType, elementNullable bool, leafNullable bool) *schemapb.FieldSchema {
	leafNode := &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_LeafType{LeafType: leaf},
		Nullable:   leafNullable,
		TypeParams: []*commonpb.KeyValuePair{{Key: pkgcommon.MaxLengthKey, Value: "4"}},
	}
	inner := &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: leafNode},
		Nullable:   elementNullable,
		TypeParams: []*commonpb.KeyValuePair{{Key: pkgcommon.MaxCapacityKey, Value: "3"}},
	}
	root := &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: inner},
		TypeParams: []*commonpb.KeyValuePair{{Key: pkgcommon.MaxCapacityKey, Value: "4"}},
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName(testStructName, name),
		DataType:        schemapb.DataType_Array,
		ElementType:     schemapb.DataType_Array,
		ElementNullable: elementNullable,
		TypeSchema:      root,
	}
}

func vectorSubField(id int64, name string, elementNullable bool) *schemapb.FieldSchema {
	field := vectorArrayField(schemapb.DataType_FloatVector, 2)
	field.FieldID = id
	field.Name = typeutil.ConcatStructFieldName(testStructName, name)
	field.ElementNullable = elementNullable
	return field
}

func elements(objects ...map[string]any) []any {
	out := make([]any, 0, len(objects))
	for _, object := range objects {
		out = append(out, object)
	}
	return out
}

func withValid(row *schemapb.ScalarField, valid ...bool) *schemapb.ScalarField {
	typeutil.SetArrayElementValidData(row, valid)
	return row
}

func longRow(values ...int64) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}
}

func stringRow(values ...string) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}
}

func requireRowEqual(t *testing.T, expected proto.Message, actual any) {
	t.Helper()
	message, ok := actual.(proto.Message)
	require.True(t, ok, "row value %T is not a proto message", actual)
	require.True(t, proto.Equal(expected, message), "expected %v, got %v", expected, message)
}

func requireImportFailed(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrImportFailed)
	require.Contains(t, err.Error(), contains)
}

func TestParseStructArrayRow_ElementNullableScalar(t *testing.T) {
	tests := []struct {
		elementType schemapb.DataType
		values      []any
		expected    *schemapb.ScalarField
	}{
		{
			elementType: schemapb.DataType_Bool,
			values:      []any{true, nil, false},
			expected:    &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false, false}}}},
		},
		{
			elementType: schemapb.DataType_Int8,
			values:      []any{json.Number("-128"), nil, json.Number("127")},
			expected:    &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{-128, 0, 127}}}},
		},
		{
			elementType: schemapb.DataType_Int16,
			values:      []any{json.Number("-32768"), nil, json.Number("32767")},
			expected:    &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{-32768, 0, 32767}}}},
		},
		{
			elementType: schemapb.DataType_Int32,
			values:      []any{json.Number("1"), nil, json.Number("3")},
			expected:    &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 0, 3}}}},
		},
		{
			elementType: schemapb.DataType_Int64,
			values:      []any{json.Number("1"), nil, json.Number("3")},
			expected:    longRow(1, 0, 3),
		},
		{
			elementType: schemapb.DataType_Float,
			values:      []any{json.Number("0.5"), nil, json.Number("1.5")},
			expected:    &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{0.5, 0, 1.5}}}},
		},
		{
			elementType: schemapb.DataType_Double,
			values:      []any{json.Number("0.5"), nil, json.Number("1.5")},
			expected:    &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{0.5, 0, 1.5}}}},
		},
		{
			elementType: schemapb.DataType_VarChar,
			values:      []any{"a", nil, "c"},
			expected:    stringRow("a", "", "c"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.elementType.String(), func(t *testing.T) {
			field := scalarSubField(201, "v", tt.elementType, true)
			objects := make([]map[string]any, 0, len(tt.values))
			for _, value := range tt.values {
				objects = append(objects, map[string]any{"v": value})
			}
			row, err := ParseStructArrayRow(testStructSchema(field), elements(objects...))
			require.NoError(t, err)
			requireRowEqual(t, withValid(tt.expected, true, false, true), row[201])
		})
	}
}

func TestParseStructArrayRow_LegacyScalar(t *testing.T) {
	field := scalarSubField(201, "v", schemapb.DataType_Int64, false)
	row, err := ParseStructArrayRow(testStructSchema(field), elements(
		map[string]any{"v": json.Number("1")},
		map[string]any{"v": json.Number("2")},
	))
	require.NoError(t, err)
	requireRowEqual(t, longRow(1, 2), row[201])
	require.Nil(t, typeutil.GetArrayElementValidData(row[201].(*schemapb.ScalarField)))

	_, err = ParseStructArrayRow(testStructSchema(field), elements(
		map[string]any{"v": json.Number("1")},
		map[string]any{"v": nil},
	))
	requireImportFailed(t, err, "element 1 is null, but the sub-field is not element-nullable")
}

func TestParseStructArrayRow_MissingAndUnknownKeys(t *testing.T) {
	id := scalarSubField(201, "id", schemapb.DataType_Int64, false)
	score := scalarSubField(202, "score", schemapb.DataType_Double, true)
	emb := vectorSubField(203, "emb", true)
	schema := testStructSchema(id, score, emb)

	row, err := ParseStructArrayRow(schema, elements(
		map[string]any{"id": json.Number("1"), "score": json.Number("0.5"), "dropped": "x"},
		map[string]any{"id": json.Number("2"), "unknown": json.Number("7")},
	))
	require.NoError(t, err)
	require.Len(t, row, 3)
	requireRowEqual(t, longRow(1, 2), row[201])
	requireRowEqual(t, withValid(&schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{
		DoubleData: &schemapb.DoubleArray{Data: []float64{0.5, 0}},
	}}, true, false), row[202])
	expectedVector := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{}}},
	}
	typeutil.SetVectorArrayElementValidData(expectedVector, []bool{false, false})
	requireRowEqual(t, expectedVector, row[203])

	_, err = ParseStructArrayRow(schema, elements(
		map[string]any{"id": json.Number("1")},
		map[string]any{"score": json.Number("0.5")},
	))
	requireImportFailed(t, err, "sub-field 'id' of struct field 'clips' is missing from element 1 and is not element-nullable")
}

func TestParseStructArrayRow_EmptyAndAllNullRows(t *testing.T) {
	legacy := scalarSubField(201, "id", schemapb.DataType_Int64, false)
	nullable := scalarSubField(202, "tag", schemapb.DataType_VarChar, true)
	nested := nestedSubField(203, "tags", schemapb.DataType_Int64, true, true)
	emb := vectorSubField(204, "emb", true)

	row, err := ParseStructArrayRow(testStructSchema(legacy, nullable, nested, emb), []any{})
	require.NoError(t, err)
	require.Len(t, row, 4)
	requireRowEqual(t, longRow(), row[201])
	requireRowEqual(t, stringRow(), row[202])
	requireRowEqual(t, &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64},
	}}, row[203])
	requireRowEqual(t, &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
	}, row[204])

	// Every element omits every element-nullable sub-field.
	row, err = ParseStructArrayRow(testStructSchema(nullable, nested, emb), elements(map[string]any{}, map[string]any{}))
	require.NoError(t, err)
	requireRowEqual(t, withValid(stringRow("", ""), false, false), row[202])
	requireRowEqual(t, withValid(&schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_Int64,
			Data:        []*schemapb.ScalarField{longRow(), longRow()},
		},
	}}, false, false), row[203])
	expectedVector := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
	}
	typeutil.SetVectorArrayElementValidData(expectedVector, []bool{false, false})
	requireRowEqual(t, expectedVector, row[204])
}

func TestParseStructArrayRow_RejectsInvalidElements(t *testing.T) {
	schema := testStructSchema(scalarSubField(201, "v", schemapb.DataType_Int64, true))

	_, err := ParseStructArrayRow(schema, []any{map[string]any{"v": json.Number("1")}, nil})
	requireImportFailed(t, err, "element 1 of struct field 'clips' is null; a struct element cannot be null")

	_, err = ParseStructArrayRow(schema, []any{"not an object"})
	requireImportFailed(t, err, "element 0 of struct field 'clips' should be a key-value map")
}

func TestParseStructArrayRow_Nested(t *testing.T) {
	field := nestedSubField(201, "tags", schemapb.DataType_Int64, true, true)
	row, err := ParseStructArrayRow(testStructSchema(field), elements(
		map[string]any{"tags": []any{json.Number("1"), nil}},
		map[string]any{"tags": nil},
		map[string]any{"tags": []any{}},
		map[string]any{},
	))
	require.NoError(t, err)
	expected := withValid(&schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_Int64,
			Data: []*schemapb.ScalarField{
				withValid(longRow(1, 0), true, false),
				longRow(),
				withValid(longRow()),
				longRow(),
			},
		},
	}}, true, false, true, false)
	requireRowEqual(t, expected, row[201])
	// A null inner array is a typed empty leaf without valid_data.
	children := row[201].(*schemapb.ScalarField).GetArrayData().GetData()
	require.NotNil(t, children[1].GetLongData())
	require.Nil(t, typeutil.GetArrayElementValidData(children[1]))

	// Neither level nullable: dense children without valid_data.
	field = nestedSubField(201, "tags", schemapb.DataType_VarChar, false, false)
	row, err = ParseStructArrayRow(testStructSchema(field), elements(
		map[string]any{"tags": []any{"a", "bb"}},
		map[string]any{"tags": []any{}},
	))
	require.NoError(t, err)
	requireRowEqual(t, &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_VarChar,
			Data:        []*schemapb.ScalarField{stringRow("a", "bb"), stringRow()},
		},
	}}, row[201])
}

func TestParseStructArrayRow_NestedRejectsNullAtNonNullableLevel(t *testing.T) {
	innerNotNullable := nestedSubField(201, "tags", schemapb.DataType_Int64, false, true)
	_, err := ParseStructArrayRow(testStructSchema(innerNotNullable), elements(map[string]any{"tags": nil}))
	requireImportFailed(t, err, "element 0 is null, but the sub-field is not element-nullable")

	// A missing key at a non-element-nullable nested sub-field is an error too.
	_, err = ParseStructArrayRow(testStructSchema(innerNotNullable), elements(map[string]any{}))
	requireImportFailed(t, err, "is missing from element 0 and is not element-nullable")

	leafNotNullable := nestedSubField(201, "tags", schemapb.DataType_Int64, true, false)
	_, err = ParseStructArrayRow(testStructSchema(leafNotNullable), elements(
		map[string]any{"tags": []any{json.Number("1")}},
		map[string]any{"tags": []any{json.Number("1"), nil}},
	))
	requireImportFailed(t, err, "element 1 inner element 1 is null, but the array leaf is not nullable")

	_, err = ParseStructArrayRow(testStructSchema(leafNotNullable), elements(map[string]any{"tags": json.Number("1")}))
	requireImportFailed(t, err, "element 0 should be an array")
}

func TestParseStructArrayRow_Capacity(t *testing.T) {
	// Outer capacity counts null elements.
	nullable := scalarSubField(201, "v", schemapb.DataType_Int64, true)
	five := make([]map[string]any, 0, 5)
	for i := 0; i < 5; i++ {
		five = append(five, map[string]any{"v": nil})
	}
	_, err := ParseStructArrayRow(testStructSchema(nullable), elements(five...))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "exceeds max_capacity(4)")

	nested := nestedSubField(201, "v", schemapb.DataType_Int64, true, true)
	_, err = ParseStructArrayRow(testStructSchema(nested), elements(five...))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "exceeds max_capacity(4)")

	// Inner capacity counts null leaves.
	_, err = ParseStructArrayRow(testStructSchema(nested), elements(
		map[string]any{"v": []any{json.Number("1"), nil, nil, nil}},
	))
	requireImportFailed(t, err, "inner array capacity(4) of field 'clips[v]' element 0 exceeds max_capacity(3)")

	_, err = ParseStructArrayRow(testStructSchema(nested), elements(
		map[string]any{"v": []any{json.Number("1"), nil, nil}},
	))
	require.NoError(t, err)
}

func TestParseStructArrayRow_VarCharMaxLength(t *testing.T) {
	single := scalarSubField(201, "v", schemapb.DataType_VarChar, true)
	_, err := ParseStructArrayRow(testStructSchema(single), elements(map[string]any{"v": "12345678"}))
	require.NoError(t, err)
	_, err = ParseStructArrayRow(testStructSchema(single), elements(map[string]any{"v": "123456789"}))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "exceeds max_length(8)")

	// The nested leaf uses the max_length of the leaf TypeSchema node.
	nested := nestedSubField(201, "v", schemapb.DataType_VarChar, true, true)
	_, err = ParseStructArrayRow(testStructSchema(nested), elements(map[string]any{"v": []any{"1234", nil}}))
	require.NoError(t, err)
	_, err = ParseStructArrayRow(testStructSchema(nested), elements(map[string]any{"v": []any{"12345"}}))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "exceeds max_length(4)")

	_, err = ParseStructArrayRow(testStructSchema(single), elements(map[string]any{"v": string([]byte{0xC0, 0xAF})}))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "invalid UTF-8")
}

func TestParseStructArrayRow_ValueChecks(t *testing.T) {
	tests := []struct {
		name        string
		field       *schemapb.FieldSchema
		value       any
		expectedErr error
		contains    string
	}{
		{"int8 overflow", scalarSubField(201, "v", schemapb.DataType_Int8, false), json.Number("128"), merr.ErrImportFailed, "out of range for Int8"},
		{"int8 underflow", scalarSubField(201, "v", schemapb.DataType_Int8, true), json.Number("-129"), merr.ErrImportFailed, "out of range for Int8"},
		{"int16 overflow", scalarSubField(201, "v", schemapb.DataType_Int16, false), json.Number("32768"), merr.ErrImportFailed, "out of range for Int16"},
		{"int32 overflow", scalarSubField(201, "v", schemapb.DataType_Int32, false), json.Number("2147483648"), merr.ErrImportFailed, "out of range for Int32"},
		{"nested int8 overflow", nestedSubField(201, "v", schemapb.DataType_Int8, false, false), []any{json.Number("300")}, merr.ErrImportFailed, "out of range for Int8"},
		{"int64 not integer", scalarSubField(201, "v", schemapb.DataType_Int64, false), json.Number("0.5"), merr.ErrImportFailed, "failed to parse '0.5' as Int64"},
		{"float NaN", scalarSubField(201, "v", schemapb.DataType_Float, true), json.Number("NaN"), merr.ErrParameterInvalid, "not a number or infinity"},
		{"double Inf", scalarSubField(201, "v", schemapb.DataType_Double, false), json.Number("Inf"), merr.ErrParameterInvalid, "not a number or infinity"},
		{"float out of range", scalarSubField(201, "v", schemapb.DataType_Float, false), json.Number("1e40"), merr.ErrImportFailed, "failed to parse '1e40' as Float"},
		{"bool type", scalarSubField(201, "v", schemapb.DataType_Bool, false), json.Number("1"), merr.ErrImportFailed, "expected element type 'Bool'"},
		{"int type", scalarSubField(201, "v", schemapb.DataType_Int32, true), "1", merr.ErrImportFailed, "expected element type 'Int32'"},
		{"varchar type", scalarSubField(201, "v", schemapb.DataType_VarChar, false), json.Number("1"), merr.ErrImportFailed, "expected element type 'VarChar'"},
		{"float type", scalarSubField(201, "v", schemapb.DataType_Float, false), true, merr.ErrImportFailed, "expected element type 'Float'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseStructArrayRow(testStructSchema(tt.field), elements(map[string]any{"v": tt.value}))
			require.Error(t, err)
			require.ErrorIs(t, err, tt.expectedErr)
			require.Contains(t, err.Error(), tt.contains)
		})
	}
}

func TestParseStructArrayRow_ElementNullableVector(t *testing.T) {
	emb := vectorSubField(201, "emb", true)
	row, err := ParseStructArrayRow(testStructSchema(emb), elements(
		map[string]any{"emb": numbers("0.1", "0.2")},
		map[string]any{"emb": nil},
		map[string]any{"emb": numbers("0.3", "0.4")},
	))
	require.NoError(t, err)
	expected := &schemapb.VectorField{
		Dim:  2,
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2, 0.3, 0.4}}},
	}
	typeutil.SetVectorArrayElementValidData(expected, []bool{true, false, true})
	requireRowEqual(t, expected, row[201])

	// The dimension error reports the logical element position.
	_, err = ParseStructArrayRow(testStructSchema(emb), elements(
		map[string]any{"emb": nil},
		map[string]any{"emb": numbers("0.1")},
	))
	requireImportFailed(t, err, "position=1")

	legacy := vectorSubField(201, "emb", false)
	row, err = ParseStructArrayRow(testStructSchema(legacy), elements(map[string]any{"emb": numbers("0.1", "0.2")}))
	require.NoError(t, err)
	require.Nil(t, typeutil.GetVectorArrayElementValidData(row[201].(*schemapb.VectorField)))

	_, err = ParseStructArrayRow(testStructSchema(legacy), elements(map[string]any{"emb": nil}))
	requireImportFailed(t, err, "element 0 is null, but the sub-field is not element-nullable")

	// Capacity counts null vectors.
	five := make([]map[string]any, 0, 5)
	for i := 0; i < 5; i++ {
		five = append(five, map[string]any{"emb": nil})
	}
	vectorField := vectorSubField(201, "emb", true)
	vectorField.TypeParams[1].Value = "4"
	_, err = ParseStructArrayRow(testStructSchema(vectorField), elements(five...))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "exceeds max_capacity(4)")
}

func TestStructSubFieldRow_UnsupportedType(t *testing.T) {
	_, err := StructSubFieldRow(&schemapb.FieldSchema{Name: "clips[v]", DataType: schemapb.DataType_Int64}, nil, 0)
	require.ErrorIs(t, err, merr.ErrImportSysFailed)
}
