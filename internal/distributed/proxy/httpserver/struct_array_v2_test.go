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

package httpserver

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func buildStructArrayTestSchema() *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "id",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}
	vec := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}
	subInt := &schemapb.FieldSchema{
		FieldID:     103,
		Name:        "sub_int",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int32,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxCapacityKey, Value: "10"},
		},
	}
	subVec := &schemapb.FieldSchema{
		FieldID:     104,
		Name:        "sub_vec",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
			{Key: common.MaxCapacityKey, Value: "10"},
		},
	}
	structField := &schemapb.StructArrayFieldSchema{
		FieldID: 102,
		Name:    "my_struct",
		Fields:  []*schemapb.FieldSchema{subInt, subVec},
	}
	return &schemapb.CollectionSchema{
		Name:              "c_test",
		Fields:            []*schemapb.FieldSchema{pk, vec},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structField},
	}
}

func TestStructArrayFieldSchema_GetProto(t *testing.T) {
	ctx := context.Background()
	good := StructArrayFieldSchema{
		FieldName:   "my_struct",
		Description: "struct field",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_int",
				DataType:        "Array",
				ElementDataType: "Int32",
				ElementTypeParams: map[string]interface{}{
					"max_capacity": 10,
				},
			},
			{
				FieldName:       "sub_vec",
				DataType:        "ArrayOfVector",
				ElementDataType: "FloatVector",
				ElementTypeParams: map[string]interface{}{
					"dim":          4,
					"max_capacity": 10,
				},
			},
		},
	}
	proto, err := good.GetProto(ctx)
	require.NoError(t, err)
	assert.Equal(t, "my_struct", proto.GetName())
	assert.Len(t, proto.GetFields(), 2)
	assert.Equal(t, schemapb.DataType_Array, proto.GetFields()[0].GetDataType())
	assert.Equal(t, schemapb.DataType_Int32, proto.GetFields()[0].GetElementType())
	assert.Equal(t, schemapb.DataType_ArrayOfVector, proto.GetFields()[1].GetDataType())
	assert.Equal(t, schemapb.DataType_FloatVector, proto.GetFields()[1].GetElementType())

	// Reject sub-field without Array / ArrayOfVector data type.
	bad := StructArrayFieldSchema{
		FieldName: "bad",
		Fields: []FieldSchema{
			{FieldName: "raw_int", DataType: "Int32"},
		},
	}
	_, err = bad.GetProto(ctx)
	assert.Error(t, err)

	// Reject primary key in sub-field.
	badPK := StructArrayFieldSchema{
		FieldName: "bad_pk",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_pk",
				DataType:        "Array",
				ElementDataType: "Int32",
				IsPrimary:       true,
			},
		},
	}
	_, err = badPK.GetProto(ctx)
	assert.Error(t, err)

	// Reject empty sub-field list.
	_, err = (&StructArrayFieldSchema{FieldName: "empty"}).GetProto(ctx)
	assert.Error(t, err)

	// Reject duplicated sub-field name.
	dup := StructArrayFieldSchema{
		FieldName: "dup",
		Fields: []FieldSchema{
			{FieldName: "s", DataType: "Array", ElementDataType: "Int32"},
			{FieldName: "s", DataType: "Array", ElementDataType: "Int32"},
		},
	}
	_, err = dup.GetProto(ctx)
	assert.Error(t, err)

	// Reject nullable sub-field.
	nullable := StructArrayFieldSchema{
		FieldName: "bad_nullable",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_null",
				DataType:        "Array",
				ElementDataType: "Int32",
				Nullable:        true,
			},
		},
	}
	_, err = nullable.GetProto(ctx)
	assert.Error(t, err)

	// Reject sub-field with defaultValue.
	withDefault := StructArrayFieldSchema{
		FieldName: "bad_default",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_default",
				DataType:        "Array",
				ElementDataType: "Int32",
				DefaultValue:    float64(1),
			},
		},
	}
	_, err = withDefault.GetProto(ctx)
	assert.Error(t, err)

	// Reject partition key in sub-field.
	partKey := StructArrayFieldSchema{
		FieldName: "bad_part",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_part",
				DataType:        "Array",
				ElementDataType: "Int32",
				IsPartitionKey:  true,
			},
		},
	}
	_, err = partKey.GetProto(ctx)
	assert.Error(t, err)

	// Reject clustering key in sub-field.
	clusterKey := StructArrayFieldSchema{
		FieldName: "bad_cluster",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_cluster",
				DataType:        "Array",
				ElementDataType: "Int32",
				IsClusteringKey: true,
			},
		},
	}
	_, err = clusterKey.GetProto(ctx)
	assert.Error(t, err)
}

func TestParseStructArrayRow_Scalar(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	raw := `[{"sub_int": 1, "sub_vec": [0.1, 0.2, 0.3, 0.4]},
	         {"sub_int": 2, "sub_vec": [0.5, 0.6, 0.7, 0.8]}]`
	row, err := parseStructArrayRow(raw, schema)
	require.NoError(t, err)
	require.Len(t, row, 2)

	scalar, ok := row["sub_int"].(*schemapb.ScalarField)
	require.True(t, ok)
	assert.Equal(t, []int32{1, 2}, scalar.GetIntData().GetData())

	vecField, ok := row["sub_vec"].(*schemapb.VectorField)
	require.True(t, ok)
	va := vecField.GetVectorArray()
	require.NotNil(t, va)
	require.Len(t, va.GetData(), 1)
	assert.Equal(t,
		[]float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
		va.GetData()[0].GetFloatVector().GetData())
}

func TestParseStructArrayRow_MissingField(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	raw := `[{"sub_int": 1}]`
	_, err := parseStructArrayRow(raw, schema)
	assert.Error(t, err)
}

func TestParseStructArrayRow_NotArray(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	_, err := parseStructArrayRow(`{"sub_int": 1}`, schema)
	assert.Error(t, err)
}

func TestBuildStructArrayFieldData_RoundTrip(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	r1, err := parseStructArrayRow(`[{"sub_int": 1, "sub_vec": [0.1, 0.2, 0.3, 0.4]},
	                                 {"sub_int": 2, "sub_vec": [0.5, 0.6, 0.7, 0.8]}]`, schema)
	require.NoError(t, err)
	r2, err := parseStructArrayRow(`[{"sub_int": 3, "sub_vec": [0.9, 1.0, 1.1, 1.2]}]`, schema)
	require.NoError(t, err)

	fd, err := buildStructArrayFieldData(schema, []structArrayRow{r1, r2})
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_ArrayOfStruct, fd.GetType())
	subs := fd.GetStructArrays().GetFields()
	require.Len(t, subs, 2)

	// sub_int has 2 rows.
	assert.Equal(t, schemapb.DataType_Array, subs[0].GetType())
	assert.Len(t, subs[0].GetScalars().GetArrayData().GetData(), 2)
	// sub_vec vector array has 2 rows of packed vectors.
	assert.Equal(t, schemapb.DataType_ArrayOfVector, subs[1].GetType())
	assert.Len(t, subs[1].GetVectors().GetVectorArray().GetData(), 2)

	// Round-trip: read back via extractStructArrayRow (schema supplies dim).
	extracted0, err := extractStructArrayRow(fd, 0, buildStructArrayTestSchema())
	require.NoError(t, err)
	require.Len(t, extracted0, 2)
	assert.EqualValues(t, int32(1), extracted0[0]["sub_int"])
	assert.EqualValues(t, []float32{0.1, 0.2, 0.3, 0.4}, extracted0[0]["sub_vec"])

	extracted1, err := extractStructArrayRow(fd, 1, buildStructArrayTestSchema())
	require.NoError(t, err)
	require.Len(t, extracted1, 1)
	assert.EqualValues(t, int32(3), extracted1[0]["sub_int"])
}

func TestAnyToColumns_StructArray(t *testing.T) {
	schema := buildStructArrayTestSchema()
	body := []byte(`{
		"data": [
			{
				"id": 1,
				"vec": [0.1, 0.2, 0.3, 0.4],
				"my_struct": [
					{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]},
					{"sub_int": 20, "sub_vec": [2.1, 2.2, 2.3, 2.4]}
				]
			},
			{
				"id": 2,
				"vec": [0.5, 0.6, 0.7, 0.8],
				"my_struct": [
					{"sub_int": 30, "sub_vec": [3.1, 3.2, 3.3, 3.4]}
				]
			}
		]
	}`)
	rows, _, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	fds, err := anyToColumns(rows, nil, schema, true, false)
	require.NoError(t, err)

	// Locate the struct field data in the output.
	var structFD *schemapb.FieldData
	for _, fd := range fds {
		if fd.GetType() == schemapb.DataType_ArrayOfStruct {
			structFD = fd
			break
		}
	}
	require.NotNil(t, structFD)
	assert.Equal(t, "my_struct", structFD.GetFieldName())
	subs := structFD.GetStructArrays().GetFields()
	require.Len(t, subs, 2)
	// sub_int row 0 should have 2 elements, row 1 should have 1 element.
	arrayData := subs[0].GetScalars().GetArrayData().GetData()
	require.Len(t, arrayData, 2)
	assert.Equal(t, []int32{10, 20}, arrayData[0].GetIntData().GetData())
	assert.Equal(t, []int32{30}, arrayData[1].GetIntData().GetData())
	// sub_vec row 0 packs two 4-dim vectors.
	vecData := subs[1].GetVectors().GetVectorArray().GetData()
	require.Len(t, vecData, 2)
	assert.Len(t, vecData[0].GetFloatVector().GetData(), 8)
	assert.Len(t, vecData[1].GetFloatVector().GetData(), 4)
}

func TestBuildQueryResp_StructArrayRoundTrip(t *testing.T) {
	schema := buildStructArrayTestSchema()
	body := []byte(`{
		"data": [
			{
				"id": 1,
				"vec": [0.1, 0.2, 0.3, 0.4],
				"my_struct": [
					{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]},
					{"sub_int": 20, "sub_vec": [2.1, 2.2, 2.3, 2.4]}
				]
			}
		]
	}`)
	rows, _, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)
	fds, err := anyToColumns(rows, nil, schema, true, false)
	require.NoError(t, err)

	// Filter to the struct field only.
	var structFD *schemapb.FieldData
	for _, fd := range fds {
		if fd.GetType() == schemapb.DataType_ArrayOfStruct {
			structFD = fd
			break
		}
	}
	require.NotNil(t, structFD)

	resp, err := buildQueryResp(0, []string{"my_struct"}, []*schemapb.FieldData{structFD}, nil, nil, true, schema)
	require.NoError(t, err)
	require.Len(t, resp, 1)

	// Marshal/unmarshal through JSON to ensure the shape is serialisable.
	blob, err := json.Marshal(resp[0]["my_struct"])
	require.NoError(t, err)
	var decoded []map[string]interface{}
	require.NoError(t, json.Unmarshal(blob, &decoded))
	require.Len(t, decoded, 2)
	assert.EqualValues(t, 10, decoded[0]["sub_int"])
}

func TestPrintStructArrayFieldsV2(t *testing.T) {
	schema := buildStructArrayTestSchema()
	printed := printStructArrayFieldsV2(schema.GetStructArrayFields())
	require.Len(t, printed, 1)
	entry := printed[0]
	assert.Equal(t, "my_struct", entry[HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_ArrayOfStruct.String(), entry[HTTPReturnFieldType])
	subs, ok := entry["fields"].([]gin.H)
	require.True(t, ok)
	require.Len(t, subs, 2)
	assert.Equal(t, "sub_int", subs[0][HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_Array.String(), subs[0][HTTPReturnFieldType])
	assert.Equal(t, schemapb.DataType_Int32.String(), subs[0][HTTPReturnFieldElementType])
	assert.Equal(t, "sub_vec", subs[1][HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_FloatVector.String(), subs[1][HTTPReturnFieldElementType])
}
