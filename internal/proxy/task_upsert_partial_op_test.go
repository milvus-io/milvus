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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func arrayIntFieldSchema(name string, isPK bool, maxCap int) *schemapb.FieldSchema {
	typeParams := []*commonpb.KeyValuePair{}
	if maxCap > 0 {
		typeParams = append(typeParams, &commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: itoa(maxCap)})
	}
	return &schemapb.FieldSchema{
		Name:         name,
		IsPrimaryKey: isPK,
		DataType:     schemapb.DataType_Array,
		ElementType:  schemapb.DataType_Int64,
		TypeParams:   typeParams,
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		return "-" + string(buf)
	}
	return string(buf)
}

func arrayLongFieldData(name string, rows [][]int64) *schemapb.FieldData {
	rowFields := make([]*schemapb.ScalarField, len(rows))
	for i, row := range rows {
		rowFields[i] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: row}}}
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data:        rowFields,
				ElementType: schemapb.DataType_Int64,
			}},
		}},
	}
}

func op(name string, t schemapb.FieldPartialUpdateOp_OpType) *schemapb.FieldPartialUpdateOp {
	return &schemapb.FieldPartialUpdateOp{FieldName: name, Op: t}
}

func TestHasNonReplacePartialOp(t *testing.T) {
	assert.False(t, hasNonReplacePartialOp(&milvuspb.UpsertRequest{}))
	assert.False(t, hasNonReplacePartialOp(&milvuspb.UpsertRequest{FieldOps: []*schemapb.FieldPartialUpdateOp{
		op("tags", schemapb.FieldPartialUpdateOp_REPLACE),
	}}))
	assert.True(t, hasNonReplacePartialOp(&milvuspb.UpsertRequest{FieldOps: []*schemapb.FieldPartialUpdateOp{
		op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND),
	}}))
	assert.True(t, hasNonReplacePartialOp(&milvuspb.UpsertRequest{FieldOps: []*schemapb.FieldPartialUpdateOp{
		op("a", schemapb.FieldPartialUpdateOp_REPLACE),
		op("b", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE),
	}}))
}

func TestValidateFieldPartialUpdateOps_NoOp(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})}}
	seen, err := validateFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.False(t, seen)
}

func TestValidateFieldPartialUpdateOps_ReplaceIgnored(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_REPLACE)},
	}
	seen, err := validateFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.False(t, seen)
}

func TestValidateFieldPartialUpdateOps_AppendOnArrayField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	seen, err := validateFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestValidateFieldPartialUpdateOps_RemoveOnArrayField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE)},
	}
	seen, err := validateFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestValidateFieldPartialUpdateOps_RejectsEmptyFieldName(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field_name is required")
}

func TestValidateFieldPartialUpdateOps_RejectsDuplicateField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps: []*schemapb.FieldPartialUpdateOp{
			op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND),
			op("tags", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE),
		},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestValidateFieldPartialUpdateOps_RejectsOpOnPK(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", true, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key")
}

func TestValidateFieldPartialUpdateOps_RejectsUnknownField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("other", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("other", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestValidateFieldPartialUpdateOps_RejectsNonArrayField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{Name: "tags", DataType: schemapb.DataType_VarChar},
	}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Array field")
}

func TestValidateFieldPartialUpdateOps_RejectsElementTypeMismatch(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
	}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "element type")
}

func TestValidateFieldPartialUpdateOps_RejectsUnsupportedOpEnum(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_OpType(9999))},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported partial update op")
}

func TestValidateFieldPartialUpdateOps_RejectsOpWithoutFieldData(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		// FieldsData empty — op targets a field that was not sent
		FieldsData: []*schemapb.FieldData{},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not present in fields_data")
}

func TestValidateFieldPartialUpdateOps_RejectsPayloadExceedingCapacity(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 2)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2, 3, 4}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_capacity")
}

func TestValidateFieldPartialUpdateOps_SkipsCapacityWhenUnset(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 0)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2, 3, 4, 5}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	seen, err := validateFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestValidateFieldPartialUpdateOps_RejectsNilArrayData(t *testing.T) {
	// FieldData declares type=Array but carries no ArrayData: the merge
	// path would deref nil; validate must reject up front.
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	fdNoArray := &schemapb.FieldData{
		FieldName: "tags",
		Type:      schemapb.DataType_Array,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
	}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{fdNoArray},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, err := validateFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not an Array")
}

func TestValidateFieldPartialUpdateOps_SkipsCapacityWhenMalformed(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "not-a-number"}},
	}}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2, 3, 4}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	seen, err := validateFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestBuildFieldOpMap(t *testing.T) {
	assert.Nil(t, buildFieldOpMap(&milvuspb.UpsertRequest{}))
	got := buildFieldOpMap(&milvuspb.UpsertRequest{FieldOps: []*schemapb.FieldPartialUpdateOp{
		op("a", schemapb.FieldPartialUpdateOp_ARRAY_APPEND),
		op("b", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE),
	}})
	require.Len(t, got, 2)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, got["a"])
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, got["b"])
}

func TestPerRowArrayLen(t *testing.T) {
	tests := []struct {
		name string
		row  *schemapb.ScalarField
		et   schemapb.DataType
		want int
	}{
		{"bool", &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}}}, schemapb.DataType_Bool, 2},
		{"int32", &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}}}}, schemapb.DataType_Int32, 3},
		{"int8", &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}}, schemapb.DataType_Int8, 1},
		{"int16", &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, schemapb.DataType_Int16, 2},
		{"int64", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}}, schemapb.DataType_Int64, 4},
		{"float", &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1}}}}, schemapb.DataType_Float, 1},
		{"double", &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1, 2}}}}, schemapb.DataType_Double, 2},
		{"varchar", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b", "c"}}}}, schemapb.DataType_VarChar, 3},
		{"string", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}}, schemapb.DataType_String, 1},
		{"unsupported", &schemapb.ScalarField{}, schemapb.DataType_JSON, 0},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, perRowArrayLen(tc.row, tc.et), tc.name)
	}
}

func TestReadMaxCapacity(t *testing.T) {
	fs := &schemapb.FieldSchema{TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "42"}}}
	assert.Equal(t, 42, readMaxCapacity(fs))

	fs = &schemapb.FieldSchema{TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "not-int"}}}
	assert.Equal(t, -1, readMaxCapacity(fs))

	fs = &schemapb.FieldSchema{TypeParams: []*commonpb.KeyValuePair{{Key: "other", Value: "42"}}}
	assert.Equal(t, -1, readMaxCapacity(fs))

	assert.Equal(t, -1, readMaxCapacity(&schemapb.FieldSchema{}))
}

func TestFindFieldSchemaByName(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "tags"}, {Name: "scores"}}}
	got, err := findFieldSchemaByName(schema, "scores")
	require.NoError(t, err)
	assert.Equal(t, "scores", got.GetName())

	_, err = findFieldSchemaByName(schema, "missing")
	require.Error(t, err)
}

func TestItoa(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{{0, "0"}, {1, "1"}, {42, "42"}, {-7, "-7"}, {1024, "1024"}}
	for _, tc := range cases {
		assert.Equal(t, tc.want, itoa(tc.in))
	}
}
