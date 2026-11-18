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

package dml

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestClientRequestPayload(t *testing.T) {
	schema, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_Float16Vector},
			{FieldID: 102, Name: "derived", DataType: schemapb.DataType_FloatVector, IsFunctionOutput: true},
			{FieldID: 103, Name: "small", DataType: schemapb.DataType_Int8},
			{FieldID: 104, Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int16},
		},
	})
	require.NoError(t, err)
	strings := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"é", "中"}}}}
	ints := &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}
	vector := &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}}
	fields := []*schemapb.FieldData{
		{FieldName: "id", Field: &schemapb.FieldData_Scalars{Scalars: strings}},
		{FieldName: "embedding", Field: vector},
		{FieldName: "derived", Field: vector},
		{FieldName: "small", Field: &schemapb.FieldData_Scalars{Scalars: ints}},
		{FieldName: "arr", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{ints}}}}}},
	}
	var size RequestPayload
	size.measure(fields, schema)
	require.Equal(t, RequestPayload{FieldBytes: 5 + 16 + 2 + 4, PrimaryKeyBytes: 5}, size)
	// DML normalizes the supplied FP32 vector and may append generated fields.
	fields[1].Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 8)}}}
	require.EqualValues(t, 27, size.FieldBytes)
	size.measure(fields[1:], schema)
	require.Equal(t, RequestPayload{FieldBytes: 8 + 2 + 4}, size)
}

func TestClientNestedPayload(t *testing.T) {
	vec := &schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3}}}
	sparse := &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{make([]byte, 8), make([]byte, 16)}}}}
	array := &schemapb.VectorField{Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Data: []*schemapb.VectorField{vec, vec}}}}
	json := &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{"k":1}`)}}}}
	field := &schemapb.FieldData{Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
		{Field: &schemapb.FieldData_Vectors{Vectors: array}},
		{Field: &schemapb.FieldData_Vectors{Vectors: sparse}},
		{Field: &schemapb.FieldData_Scalars{Scalars: json}},
	}}}}
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{})
	require.NoError(t, err)
	var payload RequestPayload
	payload.measure([]*schemapb.FieldData{field}, helper)
	require.EqualValues(t, 6+24+7, payload.FieldBytes)
	require.Zero(t, fieldValueBytes(nil, nil))
	require.EqualValues(t, 16, primaryKeyBytes(&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1}}}}))
	require.EqualValues(t, 3, primaryKeyBytes(&schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"中", ""}}}}))
}

func TestDeleteExpressionBytes(t *testing.T) {
	expr := "name == {p} && age in {a}"
	params := map[string]*schemapb.TemplateValue{
		"p": {Val: &schemapb.TemplateValue_StringVal{StringVal: "中"}},
		"a": {Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{Data: &schemapb.TemplateArrayValue_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 12}}}}}},
	}
	require.EqualValues(t, len(expr)+3+16, deleteExpressionBytes(expr, params))
}
