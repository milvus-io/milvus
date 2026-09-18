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
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

func pathReplaceScalarRow(data any) *schemapb.ScalarField {
	row := &schemapb.ScalarField{}
	switch values := data.(type) {
	case []bool:
		row.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: values}}
	case []int32:
		row.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: values}}
	case []int64:
		row.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}
	case []float32:
		row.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values}}
	case []float64:
		row.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: values}}
	case []string:
		row.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}
	default:
		panic("unsupported test scalar row")
	}
	return row
}

func TestVectorArrayElementWidth(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int64
		want        int
	}{
		{"float uses float32 entries", schemapb.DataType_FloatVector, 3, 3},
		{"binary aligned", schemapb.DataType_BinaryVector, 16, 2},
		{"binary rounded up", schemapb.DataType_BinaryVector, 9, 2},
		{"float16", schemapb.DataType_Float16Vector, 3, 6},
		{"bfloat16", schemapb.DataType_BFloat16Vector, 3, 6},
		{"int8", schemapb.DataType_Int8Vector, 3, 3},
		{"zero dimension", schemapb.DataType_FloatVector, 0, 0},
		{"negative dimension", schemapb.DataType_FloatVector, -1, 0},
		{"unsupported scalar", schemapb.DataType_Int64, 3, 0},
		{"unsupported sparse vector", schemapb.DataType_SparseFloatVector, 3, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			width, err := vectorArrayElementWidth(test.elementType, test.dim)
			if test.want == 0 {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				assert.Equal(t, merr.InputError, merr.GetErrorType(err))
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.want, width)
		})
	}
}

func TestReplaceVectorArrayRowElement(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int64
		base        *schemapb.VectorField
		update      *schemapb.VectorField
		want        *schemapb.VectorField
	}{
		{
			"float", schemapb.DataType_FloatVector, 2,
			&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{9, 8}}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 9, 8}}}},
		},
		{
			"binary", schemapb.DataType_BinaryVector, 8,
			&schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{9}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 9}}},
		},
		{
			"binary rounded up", schemapb.DataType_BinaryVector, 9,
			&schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2, 3, 4}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{9, 8}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2, 9, 8}}},
		},
		{
			"float16", schemapb.DataType_Float16Vector, 2,
			&schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{9, 10, 11, 12}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 9, 10, 11, 12}}},
		},
		{
			"bfloat16", schemapb.DataType_BFloat16Vector, 2,
			&schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{9, 10, 11, 12}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{1, 2, 3, 4, 9, 10, 11, 12}}},
		},
		{
			"int8", schemapb.DataType_Int8Vector, 2,
			&schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{9, 8}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 9, 8}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := proto.Clone(test.base).(*schemapb.VectorField)
			update := proto.Clone(test.update).(*schemapb.VectorField)
			got, err := replaceVectorArrayRowElement(base, update, 1, test.elementType, test.dim)
			require.NoError(t, err)
			assert.True(t, proto.Equal(test.want, got))
			assert.True(t, proto.Equal(test.base, base))
			assert.True(t, proto.Equal(test.update, update))

			// Ordinary StructArray flattening must use the same element width
			// as PATH_REPLACE's row counting and element copying.
			vectorField := structElementCountTestVectorArray("field2")
			vectorField.GetVectors().GetVectorArray().Data = []*schemapb.VectorField{base}
			schema := &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{
						Name: "field2", DataType: schemapb.DataType_ArrayOfVector, ElementType: test.elementType,
						TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.FormatInt(test.dim, 10)}},
					},
				},
			}}}
			insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
				structElementCountTestScalarArray("field1", []int32{1, 2}), vectorField))
			require.NoError(t, checkAndFlattenStructFieldData(schema, insertMsg))
			assert.Len(t, insertMsg.GetFieldsData(), 2)
		})
	}

	withValidity := proto.Clone(tests[0].base).(*schemapb.VectorField)
	withValidity.ValidData = []bool{true, true}
	_, err := replaceVectorArrayRowElement(withValidity, tests[0].update, 0, schemapb.DataType_FloatVector, 2)
	require.ErrorContains(t, err, "does not support ArrayOfVector element valid_data")
	_, err = replaceVectorArrayRowElement(tests[0].base,
		&schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{9, 8}}},
		1, schemapb.DataType_FloatVector, 2)
	require.ErrorContains(t, err, "does not match element type")
}

func TestVectorArrayRowElementCountRejectsInvalidPayload(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int64
		row         *schemapb.VectorField
	}{
		{"nil row", schemapb.DataType_FloatVector, 2, nil},
		{"zero dimension", schemapb.DataType_FloatVector, 0, &schemapb.VectorField{}},
		{"unsupported type", schemapb.DataType_SparseFloatVector, 2, &schemapb.VectorField{}},
		{"missing float payload", schemapb.DataType_FloatVector, 2, &schemapb.VectorField{}},
		{"nil float payload", schemapb.DataType_FloatVector, 2, &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{}}},
		{"missing binary payload", schemapb.DataType_BinaryVector, 8, &schemapb.VectorField{}},
		{"missing float16 payload", schemapb.DataType_Float16Vector, 2, &schemapb.VectorField{}},
		{"missing bfloat16 payload", schemapb.DataType_BFloat16Vector, 2, &schemapb.VectorField{}},
		{"missing int8 payload", schemapb.DataType_Int8Vector, 2, &schemapb.VectorField{}},
		{"truncated vector", schemapb.DataType_Float16Vector, 2, &schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := vectorArrayRowElementCount(test.row, test.elementType, test.dim)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Equal(t, merr.InputError, merr.GetErrorType(err))
			_, err = replaceVectorArrayRowElement(test.row, test.row, 0, test.elementType, test.dim)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func TestCheckAndFlattenStructFieldDataRejectsInvalidVectorWidth(t *testing.T) {
	for _, test := range []struct {
		name        string
		elementType schemapb.DataType
		dim         string
		wantError   string
	}{
		{"zero dimension", schemapb.DataType_FloatVector, "0", "invalid dim 0"},
		{"negative dimension", schemapb.DataType_FloatVector, "-1", "invalid dim -1"},
		{"unsupported type", schemapb.DataType_SparseFloatVector, "2", "unsupported array-of-vector element type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{{
				Name: "test_struct",
				Fields: []*schemapb.FieldSchema{
					{Name: "field1", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{
						Name: "field2", DataType: schemapb.DataType_ArrayOfVector, ElementType: test.elementType,
						TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: test.dim}},
					},
				},
			}}}
			insertMsg := structElementCountTestInsertMsg(structElementCountTestStructData(
				structElementCountTestScalarArray("field1", []int32{1}),
				structElementCountTestVectorArray("field2", []float32{1, 2})))
			err := checkAndFlattenStructFieldData(schema, insertMsg)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Equal(t, merr.InputError, merr.GetErrorType(err))
			assert.ErrorContains(t, err, test.wantError)
			assert.ErrorContains(t, err, "sub-field 'field2' in struct 'test_struct'")
		})
	}
}

func recursiveArrayLongFieldData(name string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_Array,
				Data: []*schemapb.ScalarField{{
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_Int64,
						Data: []*schemapb.ScalarField{{
							Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
						}},
					}},
				}},
			}},
		}},
	}
}

func op(name string, t schemapb.FieldPartialUpdateOp_OpType) *schemapb.FieldPartialUpdateOp {
	return &schemapb.FieldPartialUpdateOp{FieldName: name, Op: t}
}

func pathOp(name, path string) *schemapb.FieldPartialUpdateOp {
	return &schemapb.FieldPartialUpdateOp{FieldName: name, Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, Path: path}
}

func pathReplaceStructSchema() *schemapb.StructArrayFieldSchema {
	return &schemapb.StructArrayFieldSchema{
		FieldID: 100,
		Name:    "profile",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "profile[age]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "profile[city]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "profile[embedding]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
}

func structScalarChildFieldData(name string, rows ...*schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name,
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows, ElementType: schemapb.DataType_Int64}},
		}},
	}
}

func structStringChildFieldData(name string, rows ...*schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name,
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows, ElementType: schemapb.DataType_VarChar}},
		}},
	}
}

func structVectorChildFieldData(name string, rows ...*schemapb.VectorField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name,
		Type:      schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Data: rows, Dim: 2, ElementType: schemapb.DataType_FloatVector,
			}},
		}},
	}
}

func TestValidateFieldPartialUpdateOps_NoOp(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})}}
	_, seen, err := resolveFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.False(t, seen)
}

func TestValidateFieldPartialUpdateOps_ReplaceIgnored(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_REPLACE)},
	}
	_, seen, err := resolveFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.False(t, seen)
}

func TestValidateFieldPartialUpdateOps_AppendOnArrayField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, seen, err := resolveFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestValidateFieldPartialUpdateOps_RemoveOnArrayField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE)},
	}
	_, seen, err := resolveFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestValidateFieldPartialUpdateOps_RejectsRecursiveArrayOps(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		Name:        "nested",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array,
		TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{
			ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{
				ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{
					LeafType: schemapb.DataType_Int64,
				}},
			}},
		}},
	}}}

	for _, partialOp := range []schemapb.FieldPartialUpdateOp_OpType{
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND,
		schemapb.FieldPartialUpdateOp_ARRAY_REMOVE,
	} {
		t.Run(partialOp.String(), func(t *testing.T) {
			req := &milvuspb.UpsertRequest{
				FieldsData: []*schemapb.FieldData{recursiveArrayLongFieldData("nested")},
				FieldOps:   []*schemapb.FieldPartialUpdateOp{op("nested", partialOp)},
			}
			_, _, err := resolveFieldPartialUpdateOps(req, schema)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not supported for recursive ARRAY field")
		})
	}
}

func TestValidateFieldPartialUpdateOps_RejectsEmptyFieldName(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
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
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestValidateFieldPartialUpdateOps_RejectsOpOnPK(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", true, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key")
}

func TestValidateFieldPartialUpdateOps_RejectsUnknownField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("other", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("other", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
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
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Array field")
}

func TestValidateFieldPartialUpdateOps_RejectsStructOps(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "profile",
				Fields: []*schemapb.FieldSchema{
					{Name: "profile[age]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
				},
			},
		},
	}

	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct}},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("profile", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported for struct field")

	req = &milvuspb.UpsertRequest{
		FieldOps: []*schemapb.FieldPartialUpdateOp{op("profile[age]", schemapb.FieldPartialUpdateOp_REPLACE)},
	}
	_, _, err = resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partial struct update is not supported")
}

func TestValidateFieldPartialUpdateOps_RejectsElementTypeMismatch(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
	}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "element type")
}

func TestValidateFieldPartialUpdateOps_RejectsUnsupportedOpEnum(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 8)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_OpType(9999))},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
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
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not present in fields_data")
}

func TestValidateFieldPartialUpdateOps_RejectsPayloadExceedingCapacity(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 2)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2, 3, 4}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_capacity")
}

func TestValidateFieldPartialUpdateOps_SkipsCapacityWhenUnset(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("tags", false, 0)}}
	req := &milvuspb.UpsertRequest{
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("tags", [][]int64{{1, 2, 3, 4, 5}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)},
	}
	_, seen, err := resolveFieldPartialUpdateOps(req, schema)
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
	_, _, err := resolveFieldPartialUpdateOps(req, schema)
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
	_, seen, err := resolveFieldPartialUpdateOps(req, schema)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestParsePathReplace(t *testing.T) {
	valid := []struct {
		path     string
		index    int
		child    string
		hasChild bool
	}{
		{"[0]", 0, "", false},
		{"[12]", 12, "", false},
		{"[1][age]", 1, "age", true},
	}
	for _, test := range valid {
		index, child, hasChild, err := parsePathReplace(test.path)
		require.NoError(t, err, test.path)
		assert.Equal(t, test.index, index)
		assert.Equal(t, test.child, child)
		assert.Equal(t, test.hasChild, hasChild)
	}

	overflowingIndex := "[" + strconv.FormatUint(uint64(^uint(0)>>1)+1, 10) + "]"
	for _, path := range []string{"", "profile[1]", "[-1]", "[01]", "[ 1]", "[1] ", "[1][]", "[1][age][x]", "[*]", overflowingIndex} {
		_, _, _, err := parsePathReplace(path)
		assert.Error(t, err, path)
	}
}

func TestObservePathReplaceParentOperations(t *testing.T) {
	req := &milvuspb.UpsertRequest{
		DbName:         t.Name(),
		CollectionName: t.Name(),
	}
	labels := []string{paramtable.GetStringNodeID(), req.GetDbName(), req.GetCollectionName()}
	arrayOps := metrics.ProxyPathReplaceParentOperations.WithLabelValues(append(labels, pathReplaceParentArray)...)
	structOps := metrics.ProxyPathReplaceParentOperations.WithLabelValues(append(labels, pathReplaceParentStructArray)...)

	arrayOpsBefore := testutil.ToFloat64(arrayOps)
	structOpsBefore := testutil.ToFloat64(structOps)

	observePathReplaceParentOperations(req, map[string]*fieldPartialUpdatePlan{
		"scores":  {op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, arrayParent: arrayIntFieldSchema("scores", false, 8)},
		"profile": {op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, structParent: pathReplaceStructSchema()},
		"replace": {op: schemapb.FieldPartialUpdateOp_REPLACE},
	})

	assert.Equal(t, arrayOpsBefore+1, testutil.ToFloat64(arrayOps))
	assert.Equal(t, structOpsBefore+1, testutil.ToFloat64(structOps))
}

func TestResolveFieldPartialUpdateOps_ArrayPathReplace(t *testing.T) {
	fieldSchema := arrayIntFieldSchema("scores", false, 8)
	fd := arrayLongFieldData("scores", [][]int64{{100}, {200}})
	req := &milvuspb.UpsertRequest{
		NumRows:    2,
		FieldsData: []*schemapb.FieldData{fd},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{pathOp("scores", "[1]")},
	}

	plans, seen, err := resolveFieldPartialUpdateOps(req, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{fieldSchema}})
	require.NoError(t, err)
	assert.True(t, seen)
	require.NotNil(t, plans["scores"])
	assert.Equal(t, 1, plans["scores"].index)
	assert.Same(t, fieldSchema, plans["scores"].arrayParent)
}

func TestResolveFieldPartialUpdateOps_RejectsInvalidArrayOperand(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("scores", false, 8)}}
	elementValidData := arrayLongFieldData("scores", [][]int64{{1}})
	elementValidData.GetScalars().GetArrayData().GetData()[0].ValidData = []bool{true}
	tests := []struct {
		name string
		fd   *schemapb.FieldData
		op   *schemapb.FieldPartialUpdateOp
	}{
		{"missing path", arrayLongFieldData("scores", [][]int64{{1}}), pathOp("scores", "")},
		{"child on scalar array", arrayLongFieldData("scores", [][]int64{{1}}), pathOp("scores", "[0][age]")},
		{"multiple elements", arrayLongFieldData("scores", [][]int64{{1, 2}}), pathOp("scores", "[0]")},
		{"wrong row count", arrayLongFieldData("scores", [][]int64{{1}, {2}}), pathOp("scores", "[0]")},
		{"element valid data", elementValidData, pathOp("scores", "[0]")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{NumRows: 1, FieldsData: []*schemapb.FieldData{test.fd}, FieldOps: []*schemapb.FieldPartialUpdateOp{test.op}}, schema)
			assert.Error(t, err)
		})
	}

	_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows:    1,
		FieldsData: []*schemapb.FieldData{arrayLongFieldData("scores", [][]int64{{1}})},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{{FieldName: "scores", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND, Path: "[0]"}},
	}, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only supported for PATH_REPLACE")
}

func TestResolveFieldPartialUpdateOps_PathReplaceElementType(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("scores", false, 8)}}
	for _, test := range []struct {
		name        string
		elementType schemapb.DataType
		row         *schemapb.ScalarField
		wantError   bool
	}{
		{"omitted REST metadata", schemapb.DataType_None, pathReplaceScalarRow([]int64{100}), false},
		{"explicit matching metadata", schemapb.DataType_Int64, pathReplaceScalarRow([]int64{100}), false},
		{"explicit wrong metadata", schemapb.DataType_Float, pathReplaceScalarRow([]int64{100}), true},
		{"omitted metadata wrong payload", schemapb.DataType_None, pathReplaceScalarRow([]float32{100}), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fd := arrayLongFieldData("scores", [][]int64{{100}})
			fd.GetScalars().GetArrayData().ElementType = test.elementType
			fd.GetScalars().GetArrayData().Data[0] = test.row
			_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
				NumRows: 1, FieldsData: []*schemapb.FieldData{fd},
				FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("scores", "[1]")},
			}, schema)
			if test.wantError {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestResolveFieldPartialUpdateOps_StructWholeElementAndExplicitChild(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	age := structScalarChildFieldData("age",
		&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}}},
		&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{20}}}},
	)
	city := structStringChildFieldData("city",
		&schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"Hangzhou"}}}},
		&schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"Shanghai"}}}},
	)
	fd := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{age, city}}}}
	schema := &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{structSchema}}

	_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{NumRows: 2, FieldsData: []*schemapb.FieldData{fd}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[1]")}}, schema)
	require.ErrorContains(t, err, "requires all struct children")

	_, _, err = resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{NumRows: 2, FieldsData: []*schemapb.FieldData{fd}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[1][age]")}}, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires exactly that child")

	oneChild := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{age}}}}
	plans, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{NumRows: 2, FieldsData: []*schemapb.FieldData{oneChild}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[1][age]")}}, schema)
	require.NoError(t, err)
	assert.Equal(t, "age", structChildRawName(plans["profile"].explicitChild))
	_, _, err = resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{NumRows: 2, FieldsData: []*schemapb.FieldData{oneChild}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[1]")}}, schema)
	require.ErrorContains(t, err, "requires all struct children")
	embedding := structVectorChildFieldData("embedding",
		&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}},
		&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3, 4}}}},
	)
	fd.GetStructArrays().Fields = []*schemapb.FieldData{embedding, city, age}
	plans, _, err = resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{NumRows: 2, FieldsData: []*schemapb.FieldData{fd}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[1]")}}, schema)
	require.NoError(t, err)
	assert.Equal(t, structSchema.GetFields(), plans["profile"].operandChildren)
}

func TestResolveFieldPartialUpdateOps_ValidatesStructOperandChildName(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	schema := &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{structSchema}}
	tests := []struct {
		name      string
		childName string
		valid     bool
	}{
		{name: "raw name", childName: "age", valid: true},
		{name: "canonical name", childName: "profile[age]", valid: true},
		{name: "missing child", childName: "profile[", valid: false},
		{name: "missing parent", childName: "[", valid: false},
		{name: "missing closing bracket", childName: "profile[age", valid: false},
		{name: "wrong parent", childName: "other[age]", valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			age := structScalarChildFieldData(test.childName, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}},
			})
			operand := &schemapb.FieldData{
				FieldName: "profile",
				Type:      schemapb.DataType_ArrayOfStruct,
				Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
					Fields: []*schemapb.FieldData{age},
				}},
			}
			plans, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
				NumRows:    1,
				FieldsData: []*schemapb.FieldData{operand},
				FieldOps:   []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[0][age]")},
			}, schema)
			if test.valid {
				require.NoError(t, err)
				require.Len(t, plans["profile"].operandChildren, 1)
				assert.Equal(t, "age", structChildRawName(plans["profile"].operandChildren[0]))
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, "not found in struct field")
		})
	}
}

func TestResolveFieldPartialUpdateOps_RejectsStructChildElementValidData(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	schema := &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{structSchema}}
	age := structScalarChildFieldData("age", &schemapb.ScalarField{
		ValidData: []bool{false},
		Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}},
	})
	ageOperand := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{
		StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{age}},
	}}
	_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows: 1, FieldsData: []*schemapb.FieldData{ageOperand}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[0][age]")},
	}, schema)
	require.ErrorContains(t, err, "does not support Array element valid_data")

	city := structStringChildFieldData("city", &schemapb.ScalarField{
		ValidData: []bool{false},
		Data:      &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}},
	})
	cityOperand := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{
		StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{city}},
	}}
	_, _, err = resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows: 1, FieldsData: []*schemapb.FieldData{cityOperand}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[0][city]")},
	}, schema)
	require.ErrorContains(t, err, "does not support Array element valid_data")

	embedding := structVectorChildFieldData("embedding", &schemapb.VectorField{
		ValidData: []bool{true},
		Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
	})
	embeddingOperand := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{
		StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{embedding}},
	}}
	_, _, err = resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows: 1, FieldsData: []*schemapb.FieldData{embeddingOperand}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[0][embedding]")},
	}, schema)
	require.ErrorContains(t, err, "does not support ArrayOfVector element valid_data")
}

func TestResolveFieldPartialUpdateOps_RejectsStructVectorDimensionMismatch(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	embedding := structVectorChildFieldData("embedding", &schemapb.VectorField{
		Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
	})
	embedding.GetVectors().Dim = 4
	operand := &schemapb.FieldData{
		FieldName: "profile",
		Type:      schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
			Fields: []*schemapb.FieldData{embedding},
		}},
	}
	_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows: 1, FieldsData: []*schemapb.FieldData{operand}, FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[0][embedding]")},
	}, &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{structSchema}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dimension")
}

func TestResolveFieldPartialUpdateOps_RejectsDuplicateFieldData(t *testing.T) {
	fd := arrayLongFieldData("scores", [][]int64{{1}})
	_, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows:    1,
		FieldsData: []*schemapb.FieldData{fd, fd},
		FieldOps:   []*schemapb.FieldPartialUpdateOp{pathOp("scores", "[0]")},
	}, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{arrayIntFieldSchema("scores", false, 8)}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate fields_data")
}

func TestApplyStructPathReplacePreservesOmittedChildren(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	age := structScalarChildFieldData("age", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}}})
	city := structStringChildFieldData("city", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A", "B", "C"}}}})
	embedding := structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}})
	old := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{age, city, embedding}}}}
	operandAge := structScalarChildFieldData("age", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{99}}}})
	operand := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{operandAge}}}}
	plan := &fieldPartialUpdatePlan{op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, structParent: structSchema, index: 1, explicitChild: structSchema.GetFields()[0], operandChildren: []*schemapb.FieldSchema{structSchema.GetFields()[0]}}

	require.NoError(t, validateExistingStructPathRows(old, plan, []int64{0}))
	dst := proto.Clone(old).(*schemapb.FieldData)
	require.NoError(t, applyStructPathReplace(dst, operand, plan, []int64{0}, []int64{0}))
	assert.Equal(t, []int64{10, 99, 30}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[0]).GetScalars().GetArrayData().GetData()[0].GetLongData().GetData())
	assert.Equal(t, []string{"A", "B", "C"}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[1]).GetScalars().GetArrayData().GetData()[0].GetStringData().GetData())
	assert.Equal(t, []float32{1, 2, 3, 4, 5, 6}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[2]).GetVectors().GetVectorArray().GetData()[0].GetFloatVector().GetData())

	// The same index without a child suffix replaces the complete element.
	operand.GetStructArrays().Fields = []*schemapb.FieldData{
		structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{9, 8}}}}),
		operandAge,
		structStringChildFieldData("city", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"X"}}}}),
	}
	plans, _, err := resolveFieldPartialUpdateOps(&milvuspb.UpsertRequest{
		NumRows: 1, FieldsData: []*schemapb.FieldData{operand},
		FieldOps: []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[1]")},
	}, &schemapb.CollectionSchema{StructArrayFields: []*schemapb.StructArrayFieldSchema{structSchema}})
	require.NoError(t, err)
	oldBefore, operandBefore := proto.Clone(old), proto.Clone(operand)
	dst = proto.Clone(old).(*schemapb.FieldData)
	require.NoError(t, applyStructPathReplace(dst, operand, plans["profile"], []int64{0}, []int64{0}))
	assert.Equal(t, []int64{10, 99, 30}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[0]).GetScalars().GetArrayData().GetData()[0].GetLongData().GetData())
	assert.Equal(t, []string{"A", "X", "C"}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[1]).GetScalars().GetArrayData().GetData()[0].GetStringData().GetData())
	assert.Equal(t, []float32{1, 2, 9, 8, 5, 6}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[2]).GetVectors().GetVectorArray().GetData()[0].GetFloatVector().GetData())
	assert.True(t, proto.Equal(oldBefore, old))
	assert.True(t, proto.Equal(operandBefore, operand))
}

func TestApplyStructPathReplaceVectorChild(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	age := structScalarChildFieldData("age", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}}})
	city := structStringChildFieldData("city", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A", "B"}}}})
	embedding := structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}})
	old := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{age, city, embedding}}}}
	operandEmbedding := structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{9, 8}}}})
	operand := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{operandEmbedding}}}}
	plan := &fieldPartialUpdatePlan{op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, structParent: structSchema, index: 0, explicitChild: structSchema.GetFields()[2], operandChildren: []*schemapb.FieldSchema{structSchema.GetFields()[2]}}

	dst := proto.Clone(old).(*schemapb.FieldData)
	require.NoError(t, applyStructPathReplace(dst, operand, plan, []int64{0}, []int64{0}))
	assert.Equal(t, []float32{9, 8, 3, 4}, findStructChildFieldData(dst.GetStructArrays().GetFields(), structSchema.GetFields()[2]).GetVectors().GetVectorArray().GetData()[0].GetFloatVector().GetData())
}

func TestApplyStructPathReplacePreservesMaterializationCause(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	tests := []struct {
		name      string
		dst       *schemapb.FieldData
		operand   *schemapb.FieldData
		child     *schemapb.FieldSchema
		wantCause string
	}{
		{
			name: "scalar child",
			dst: &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
				structScalarChildFieldData("age", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}}}),
			}}}},
			operand: &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
				structScalarChildFieldData("age", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{30, 40}}}}),
			}}}},
			child:     structSchema.GetFields()[0],
			wantCause: "operand must contain exactly one Array element",
		},
		{
			name: "vector child",
			dst: &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
				structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}),
			}}}},
			operand: &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
				structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{9}}}}),
			}}}},
			child:     structSchema.GetFields()[2],
			wantCause: "not divisible by element width",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := &fieldPartialUpdatePlan{
				op:              schemapb.FieldPartialUpdateOp_PATH_REPLACE,
				structParent:    structSchema,
				index:           0,
				operandChildren: []*schemapb.FieldSchema{test.child},
			}
			err := applyStructPathReplace(test.dst, test.operand, plan, []int64{0}, []int64{0})
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
			assert.ErrorContains(t, err, test.wantCause)
		})
	}
}

func TestApplyStructPathReplaceRejectsInvalidRowMappings(t *testing.T) {
	schema := pathReplaceStructSchema()
	for _, child := range []*schemapb.FieldSchema{schema.GetFields()[0], schema.GetFields()[2]} {
		t.Run(child.GetName(), func(t *testing.T) {
			var data *schemapb.FieldData
			if child.GetDataType() == schemapb.DataType_Array {
				data = structScalarChildFieldData("age", pathReplaceScalarRow([]int64{18}))
			} else {
				data = structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}})
			}
			field := &schemapb.FieldData{
				FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct,
				Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{data}}},
			}
			plan := &fieldPartialUpdatePlan{op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, structParent: schema, operandChildren: []*schemapb.FieldSchema{child}}
			for _, indices := range [][2]int64{{-1, 0}, {1, 0}, {0, -1}, {0, 1}} {
				err := applyStructPathReplace(proto.Clone(field).(*schemapb.FieldData), field, plan, []int64{indices[0]}, []int64{indices[1]})
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
			}
		})
	}
}

func TestUpsertTaskQueryPreExecutePathReplaceAlignsRowsByPrimaryKey(t *testing.T) {
	idField := func(ids ...int64) *schemapb.FieldData {
		return &schemapb.FieldData{
			FieldName: "id",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}},
			}},
		}
	}
	collectionSchema := &schemapb.CollectionSchema{
		Name: "path_replace_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		},
	}
	schema := mustNewSchemaInfo(collectionSchema)

	newTask := func() *upsertTask {
		requestFields := []*schemapb.FieldData{
			idField(2, 1),
			arrayLongFieldData("scores", [][]int64{{200}, {100}}),
		}
		requestFields[1].FieldId = 101
		// Keep REST's omitted metadata through materialization: production
		// calls insertPreExecute only after queryPreExecute has merged rows.
		requestFields[1].GetScalars().GetArrayData().ElementType = schemapb.DataType_None
		request := &milvuspb.UpsertRequest{
			DbName:         "default",
			CollectionName: "path_replace_test",
			NumRows:        2,
			PartialUpdate:  true,
			FieldsData:     requestFields,
			FieldOps:       []*schemapb.FieldPartialUpdateOp{pathOp("scores", "[1]")},
		}
		plans, _, err := resolveFieldPartialUpdateOps(request, collectionSchema)
		require.NoError(t, err)
		return &upsertTask{
			ctx:                     context.Background(),
			schema:                  schema,
			req:                     request,
			fieldPartialUpdatePlans: plans,
			upsertMsg: &msgstream.UpsertMsg{InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
				DbName:         request.GetDbName(),
				CollectionName: request.GetCollectionName(),
				FieldsData:     requestFields,
				NumRows:        uint64(request.GetNumRows()),
				Version:        msgpb.InsertDataVersion_ColumnBased,
			}}},
			node: &Proxy{},
		}
	}

	t.Run("append capacity remains an input error", func(t *testing.T) {
		task := newTask()
		appendSchema := proto.Clone(collectionSchema).(*schemapb.CollectionSchema)
		appendSchema.Fields[1].TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "2"}}
		task.schema = mustNewSchemaInfo(appendSchema)
		task.req.FieldsData[1].GetScalars().GetArrayData().ElementType = schemapb.DataType_Int64
		task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{FieldName: "scores", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND}}
		plans, _, err := resolveFieldPartialUpdateOps(task.req, appendSchema)
		require.NoError(t, err)
		task.fieldPartialUpdatePlans = plans
		existing := arrayLongFieldData("scores", [][]int64{{10, 11}, {20, 21}})
		existing.FieldId = 101
		mockRetrieve := mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status: merr.Success(), FieldsData: []*schemapb.FieldData{idField(1, 2), existing},
		}, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		// Each singleton operand fits max_capacity; only combining it with the
		// existing row exceeds the limit. Exercise the real shared merge helper.
		_, err = task.queryPreExecute(context.Background())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Equal(t, merr.InputError, merr.GetErrorType(err))
		assert.Equal(t, merr.Code(merr.ErrParameterInvalid), merr.Status(err).GetCode())
		assert.ErrorContains(t, err, "max_capacity")
	})

	t.Run("path merge failure remains a system error", func(t *testing.T) {
		task := newTask()
		existing := arrayLongFieldData("scores", [][]int64{{10, 11}, {20, 21}})
		existing.FieldId = 101
		mockRetrieve := mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status: merr.Success(), FieldsData: []*schemapb.FieldData{idField(1, 2), existing},
		}, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()
		// Validation sees valid request and stored rows. Inject an unexpected
		// row-operation failure only after they enter the shared merge loop.
		cause := merr.WrapErrParameterInvalidMsg("injected row operation failure")
		mockRowOp := mockey.Mock(typeutil.ApplyArrayRowOp).Return(nil, cause).Build()
		defer mockRowOp.UnPatch()
		_, err := task.queryPreExecute(context.Background())
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.ErrorIs(t, err, cause)
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Status(err).GetCode())
		assert.ErrorContains(t, err, "injected row operation failure")
		assert.EqualValues(t, 1, mockRowOp.Times())
	})

	t.Run("shuffled retrieve result", func(t *testing.T) {
		task := newTask()
		existingScores := arrayLongFieldData("scores", [][]int64{{10, 11}, {20, 21}})
		existingScores.FieldId = 101
		mockRetrieve := mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{idField(1, 2), existingScores},
		}, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		_, err := task.queryPreExecute(context.Background())
		require.NoError(t, err)
		var scores *schemapb.FieldData
		for _, field := range task.insertFieldData {
			if field.GetFieldName() == "scores" {
				scores = field
				break
			}
		}
		require.NotNil(t, scores)
		rows := scores.GetScalars().GetArrayData().GetData()
		require.Len(t, rows, 2)
		assert.Equal(t, []int64{20, 200}, rows[0].GetLongData().GetData())
		assert.Equal(t, []int64{10, 100}, rows[1].GetLongData().GetData())
		assert.Equal(t, []int64{2, 1}, task.deletePKs.GetIntId().GetData())
	})

	t.Run("missing primary key", func(t *testing.T) {
		task := newTask()
		// A positional replacement is not an insert, even when required
		// insert-only fields are absent from the operand.
		schemaWithRequiredField := proto.Clone(collectionSchema).(*schemapb.CollectionSchema)
		schemaWithRequiredField.Fields = append(schemaWithRequiredField.Fields,
			&schemapb.FieldSchema{FieldID: 102, Name: "required", DataType: schemapb.DataType_Int64})
		task.schema = mustNewSchemaInfo(schemaWithRequiredField)
		existingScores := arrayLongFieldData("scores", [][]int64{{10, 11}})
		existingScores.FieldId = 101
		mockRetrieve := mockey.Mock(retrieveByPKs).Return(&milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{idField(1), existingScores},
		}, segcore.StorageCost{}, nil).Build()
		defer mockRetrieve.UnPatch()

		_, err := task.queryPreExecute(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires every primary key")
	})

	for _, rowCount := range []uint32{1, 3} {
		t.Run(fmt.Sprintf("struct operand rows %d with two primary keys", rowCount), func(t *testing.T) {
			task := newTask()
			structSchema := proto.Clone(collectionSchema).(*schemapb.CollectionSchema)
			structSchema.Fields = structSchema.Fields[:1]
			structSchema.StructArrayFields = []*schemapb.StructArrayFieldSchema{pathReplaceStructSchema()}
			task.schema = mustNewSchemaInfo(structSchema)
			rows := make([]*schemapb.ScalarField, rowCount)
			for i := range rows {
				rows[i] = pathReplaceScalarRow([]int64{18})
			}
			operand := &schemapb.FieldData{
				FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct,
				Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{structScalarChildFieldData("age", rows...)}}},
			}
			task.req.NumRows = rowCount
			task.req.FieldsData = []*schemapb.FieldData{idField(1, 2), operand}
			task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{pathOp("profile", "[0][age]")}
			plans, _, err := resolveFieldPartialUpdateOps(task.req, structSchema)
			require.NoError(t, err)
			task.fieldPartialUpdatePlans = plans
			// Neither retrieval nor materialization should see this invalid request.
			mockRetrieve := mockey.Mock(retrieveByPKs).Return(nil, segcore.StorageCost{}, merr.ErrServiceInternal).Build()
			defer mockRetrieve.UnPatch()
			_, err = task.queryPreExecute(context.Background())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Equal(t, merr.InputError, merr.GetErrorType(err))
			assert.ErrorContains(t, err, "does not match primary key count")
			assert.Zero(t, mockRetrieve.Times())
		})
	}
}

func TestValidateExistingPathRowsRejectsInvalidTargets(t *testing.T) {
	arraySchema := arrayIntFieldSchema("scores", false, 8)
	arrayPlan := &fieldPartialUpdatePlan{op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, arrayParent: arraySchema, index: 2}
	arrayData := arrayLongFieldData("scores", [][]int64{{1, 2}})
	err := validateExistingArrayPathRows(arrayData, []int64{0}, []int64{0}, arrayPlan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	arrayPlan.index = 0
	typeutil.SetFieldDataValidData(arrayData, []bool{false})
	err = validateExistingArrayPathRows(arrayData, []int64{0}, []int64{0}, arrayPlan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null parent")
	typeutil.SetFieldDataValidData(arrayData, nil)

	arrayData.GetScalars().GetArrayData().GetData()[0] = &schemapb.ScalarField{
		Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}},
	}
	err = validateExistingArrayPathRows(arrayData, []int64{0}, []int64{0}, arrayPlan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match element type")

	arrayData.GetScalars().GetArrayData().GetData()[0] = &schemapb.ScalarField{
		ValidData: []bool{true},
		Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
	}
	err = validateExistingArrayPathRows(arrayData, []int64{0}, []int64{0}, arrayPlan)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.ErrorContains(t, err, "does not support Array element valid_data")
}

func TestValidateExistingStructPathRowsRejectsMalformedAlignment(t *testing.T) {
	structSchema := pathReplaceStructSchema()
	age := structScalarChildFieldData("age", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}}})
	city := structStringChildFieldData("city", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A"}}}})
	embedding := structVectorChildFieldData("embedding", &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}})
	old := &schemapb.FieldData{FieldName: "profile", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{age, city, embedding}}}}
	plan := &fieldPartialUpdatePlan{op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, structParent: structSchema, index: 0}

	err := validateExistingStructPathRows(old, plan, []int64{0})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unaligned child lengths")

	old.GetStructArrays().Fields = append(old.GetStructArrays().Fields, age)
	err = validateExistingStructPathRows(old, plan, []int64{0})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "children, expected")
}

func TestValidateExistingStructPathRowsAllowsNestedArraySibling(t *testing.T) {
	nestedSchema := &schemapb.FieldSchema{
		FieldID:     101,
		Name:        "profile[nested]",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array,
		TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{
			ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{
				ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{
					LeafType: schemapb.DataType_Int32,
				}},
			}},
		}},
	}
	scalarSchema := &schemapb.FieldSchema{
		FieldID:     102,
		Name:        "profile[score]",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int32,
	}
	structSchema := &schemapb.StructArrayFieldSchema{
		FieldID: 100,
		Name:    "profile",
		Fields:  []*schemapb.FieldSchema{nestedSchema, scalarSchema},
	}
	nested := structElementCountTestNestedIntArray("nested", [][]int32{{1, 2}, {3}})
	scalar := structElementCountTestScalarArray("score", []int32{10, 20})
	scalar.GetScalars().GetArrayData().ElementType = schemapb.DataType_Int32
	old := &schemapb.FieldData{
		FieldName: "profile",
		Type:      schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
			Fields: []*schemapb.FieldData{nested, scalar},
		}},
	}
	operandScalar := structElementCountTestScalarArray("score", []int32{99})
	operandScalar.GetScalars().GetArrayData().ElementType = schemapb.DataType_Int32
	operand := &schemapb.FieldData{
		FieldName: "profile",
		Type:      schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
			Fields: []*schemapb.FieldData{operandScalar},
		}},
	}
	plan := &fieldPartialUpdatePlan{
		op:              schemapb.FieldPartialUpdateOp_PATH_REPLACE,
		structParent:    structSchema,
		index:           1,
		operandChildren: []*schemapb.FieldSchema{scalarSchema},
	}
	originalNested := proto.Clone(nested).(*schemapb.FieldData)

	require.NoError(t, validateExistingStructPathRows(old, plan, []int64{0}))
	require.NoError(t, applyStructPathReplace(old, operand, plan, []int64{0}, []int64{0}))
	assert.True(t, proto.Equal(originalNested, nested))
	assert.Equal(t, []int32{10, 99}, scalar.GetScalars().GetArrayData().GetData()[0].GetIntData().GetData())
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
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, perRowArrayLen(tc.row, tc.et))
			count, err := scalarArrayRowElementCount(tc.row, tc.et)
			if tc.et == schemapb.DataType_JSON {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, count)
				_, err = scalarArrayRowElementCount(&schemapb.ScalarField{}, tc.et)
				require.ErrorContains(t, err, "payload does not match element type")
			}
		})
	}
}

func TestScalarArrayRowElementCountRejectsInvalidPayload(t *testing.T) {
	for _, test := range []struct {
		name        string
		elementType schemapb.DataType
		row         *schemapb.ScalarField
	}{
		{"nil row", schemapb.DataType_Bool, nil},
		{"wrong oneof", schemapb.DataType_Bool, pathReplaceScalarRow([]int32{1})},
		{"nil bool payload", schemapb.DataType_Bool, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{}}},
		{"nil int payload", schemapb.DataType_Int32, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{}}},
		{"nil long payload", schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{}}},
		{"nil float payload", schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{}}},
		{"nil double payload", schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{}}},
		{"nil string payload", schemapb.DataType_VarChar, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := scalarArrayRowElementCount(test.row, test.elementType)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Equal(t, merr.InputError, merr.GetErrorType(err))
		})
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
