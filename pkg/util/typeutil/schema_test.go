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

package typeutil

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

func TestSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int8",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     2,
			},
			{
				FieldID:      101,
				Name:         "field_int16",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     3,
			},
			{
				FieldID:      102,
				Name:         "field_int32",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     4,
			},
			{
				FieldID:      103,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
			{
				FieldID:      104,
				Name:         "field_float",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     10,
			},
			{
				FieldID:      105,
				Name:         "field_double",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     11,
			},
			{
				FieldID:      106,
				Name:         "field_string",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     21,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "125",
					},
				},
			},
			{
				FieldID:      107,
				Name:         "field_float_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     101,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "nlist",
						Value: "128",
					},
				},
			},
			{
				FieldID:      108,
				Name:         "field_binary_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     100,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:     109,
				Name:        "field_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
			{
				FieldID:  110,
				Name:     "field_json",
				DataType: schemapb.DataType_JSON,
			},
			{
				FieldID:      111,
				Name:         "field_float16_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     102,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:      112,
				Name:         "field_bfloat16_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     103,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			// Do not test on sparse float vector field.
		},
	}

	t.Run("EstimateSizePerRecord", func(t *testing.T) {
		size, err := EstimateSizePerRecord(schema)
		assert.Equal(t, 680+DynamicFieldMaxLength*3, size)
		assert.NoError(t, err)
	})

	t.Run("SchemaHelper", func(t *testing.T) {
		_, err := CreateSchemaHelper(nil)
		assert.Error(t, err)

		helper, err := CreateSchemaHelper(schema)
		assert.NoError(t, err)

		field, err := helper.GetPrimaryKeyField()
		assert.NoError(t, err)
		assert.Equal(t, "field_int64", field.Name)

		field1, err := helper.GetFieldFromName("field_int8")
		assert.NoError(t, err)
		assert.Equal(t, "field_int8", field1.Name)

		field2, err := helper.GetFieldFromID(102)
		assert.NoError(t, err)
		assert.Equal(t, "field_int32", field2.Name)

		dim, err := helper.GetVectorDimFromID(107)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim)
		dim1, err := helper.GetVectorDimFromID(108)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim1)
		_, err = helper.GetVectorDimFromID(103)
		assert.Error(t, err)

		dim2, err := helper.GetVectorDimFromID(111)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim2)

		dim3, err := helper.GetVectorDimFromID(112)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim3)
	})

	t.Run("Type", func(t *testing.T) {
		assert.False(t, IsVectorType(schemapb.DataType_Bool))
		assert.False(t, IsVectorType(schemapb.DataType_Int8))
		assert.False(t, IsVectorType(schemapb.DataType_Int16))
		assert.False(t, IsVectorType(schemapb.DataType_Int32))
		assert.False(t, IsVectorType(schemapb.DataType_Int64))
		assert.False(t, IsVectorType(schemapb.DataType_Float))
		assert.False(t, IsVectorType(schemapb.DataType_Double))
		assert.False(t, IsVectorType(schemapb.DataType_String))
		assert.True(t, IsVectorType(schemapb.DataType_BinaryVector))
		assert.True(t, IsVectorType(schemapb.DataType_FloatVector))
		assert.True(t, IsVectorType(schemapb.DataType_Float16Vector))
		assert.True(t, IsVectorType(schemapb.DataType_BFloat16Vector))
		assert.True(t, IsVectorType(schemapb.DataType_SparseFloatVector))

		assert.False(t, IsIntegerType(schemapb.DataType_Bool))
		assert.True(t, IsIntegerType(schemapb.DataType_Int8))
		assert.True(t, IsIntegerType(schemapb.DataType_Int16))
		assert.True(t, IsIntegerType(schemapb.DataType_Int32))
		assert.True(t, IsIntegerType(schemapb.DataType_Int64))
		assert.False(t, IsIntegerType(schemapb.DataType_Float))
		assert.False(t, IsIntegerType(schemapb.DataType_Double))
		assert.False(t, IsIntegerType(schemapb.DataType_String))
		assert.False(t, IsIntegerType(schemapb.DataType_BinaryVector))
		assert.False(t, IsIntegerType(schemapb.DataType_FloatVector))
		assert.False(t, IsIntegerType(schemapb.DataType_Float16Vector))
		assert.False(t, IsIntegerType(schemapb.DataType_BFloat16Vector))
		assert.False(t, IsIntegerType(schemapb.DataType_SparseFloatVector))

		assert.False(t, IsFloatingType(schemapb.DataType_Bool))
		assert.False(t, IsFloatingType(schemapb.DataType_Int8))
		assert.False(t, IsFloatingType(schemapb.DataType_Int16))
		assert.False(t, IsFloatingType(schemapb.DataType_Int32))
		assert.False(t, IsFloatingType(schemapb.DataType_Int64))
		assert.True(t, IsFloatingType(schemapb.DataType_Float))
		assert.True(t, IsFloatingType(schemapb.DataType_Double))
		assert.False(t, IsFloatingType(schemapb.DataType_String))
		assert.False(t, IsFloatingType(schemapb.DataType_BinaryVector))
		assert.False(t, IsFloatingType(schemapb.DataType_FloatVector))
		assert.False(t, IsFloatingType(schemapb.DataType_Float16Vector))
		assert.False(t, IsFloatingType(schemapb.DataType_BFloat16Vector))
		assert.False(t, IsFloatingType(schemapb.DataType_SparseFloatVector))

		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Bool))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Int8))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Int16))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Int32))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Int64))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Float))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Double))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_String))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_BinaryVector))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_FloatVector))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Float16Vector))
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_BFloat16Vector))
		assert.True(t, IsSparseFloatVectorType(schemapb.DataType_SparseFloatVector))
	})
}

func TestSchema_GetVectorFieldSchema(t *testing.T) {
	schemaNormal := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
			{
				FieldID:      107,
				Name:         "field_float_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     101,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}

	t.Run("GetVectorFieldSchema", func(t *testing.T) {
		fieldSchema := GetVectorFieldSchemas(schemaNormal)
		assert.Equal(t, 1, len(fieldSchema))
		assert.Equal(t, "field_float_vector", fieldSchema[0].Name)
	})

	schemaSparse := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
			{
				FieldID:      107,
				Name:         "field_sparse_float_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     104,
				TypeParams:   []*commonpb.KeyValuePair{},
			},
		},
	}

	t.Run("GetSparseFloatVectorFieldSchema", func(t *testing.T) {
		fieldSchema := GetVectorFieldSchemas(schemaSparse)
		assert.Equal(t, 1, len(fieldSchema))
		assert.Equal(t, "field_sparse_float_vector", fieldSchema[0].Name)
	})

	schemaInvalid := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
		},
	}

	t.Run("GetVectorFieldSchemaInvalid", func(t *testing.T) {
		res := GetVectorFieldSchemas(schemaInvalid)
		assert.Equal(t, 0, len(res))
	})
}

func TestSchemaHelper_GetDynamicField(t *testing.T) {
	t.Run("with_dynamic_schema", func(t *testing.T) {
		sch := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
				{
					FieldID:   102,
					Name:      "$meta",
					DataType:  schemapb.DataType_JSON,
					IsDynamic: true,
				},
			},
		}

		helper, err := CreateSchemaHelper(sch)
		require.NoError(t, err)

		f, err := helper.GetDynamicField()
		assert.NoError(t, err)
		assert.NotNil(t, f)
		assert.EqualValues(t, 102, f.FieldID)
	})

	t.Run("without_dynamic_schema", func(t *testing.T) {
		sch := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
			},
		}

		helper, err := CreateSchemaHelper(sch)
		require.NoError(t, err)

		_, err = helper.GetDynamicField()
		assert.Error(t, err)
	})

	t.Run("multiple_dynamic_fields", func(t *testing.T) {
		sch := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
				{
					FieldID:   102,
					Name:      "$meta",
					DataType:  schemapb.DataType_JSON,
					IsDynamic: true,
				},
				{
					FieldID:   103,
					Name:      "other_json",
					DataType:  schemapb.DataType_JSON,
					IsDynamic: true,
				},
			},
		}

		_, err := CreateSchemaHelper(sch)
		assert.Error(t, err)
	})
}

func TestSchemaHelper_GetClusteringKeyField(t *testing.T) {
	t.Run("with_clustering_key", func(t *testing.T) {
		sch := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
				{
					FieldID:         102,
					Name:            "group",
					DataType:        schemapb.DataType_Int64,
					IsClusteringKey: true,
				},
			},
		}

		helper, err := CreateSchemaHelper(sch)
		require.NoError(t, err)

		f, err := helper.GetClusteringKeyField()
		assert.NoError(t, err)
		assert.NotNil(t, f)
		assert.EqualValues(t, 102, f.FieldID)
	})

	t.Run("without_clusteriny_key_schema", func(t *testing.T) {
		sch := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
			},
		}

		helper, err := CreateSchemaHelper(sch)
		require.NoError(t, err)

		_, err = helper.GetClusteringKeyField()
		assert.Error(t, err)
	})

	t.Run("multiple_dynamic_fields", func(t *testing.T) {
		sch := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "field_float_vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
				},
				{
					FieldID:         102,
					Name:            "group",
					DataType:        schemapb.DataType_Int64,
					IsClusteringKey: true,
				},
				{
					FieldID:         103,
					Name:            "batch",
					DataType:        schemapb.DataType_VarChar,
					IsClusteringKey: true,
				},
			},
		}

		_, err := CreateSchemaHelper(sch)
		assert.Error(t, err)
	})
}

func TestSchema_invalid(t *testing.T) {
	t.Run("Duplicate field name", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     2,
				},
				{
					FieldID:      101,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     3,
				},
			},
		}
		_, err := CreateSchemaHelper(schema)
		assert.Error(t, err)
		assert.EqualError(t, err, "duplicated fieldName: field_int8")
	})
	t.Run("Duplicate field id", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     2,
				},
				{
					FieldID:      100,
					Name:         "field_int16",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     3,
				},
			},
		}
		_, err := CreateSchemaHelper(schema)
		assert.Error(t, err)
		assert.EqualError(t, err, "duplicated fieldID: 100")
	})
	t.Run("Duplicated primary key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: true,
					Description:  "",
					DataType:     2,
				},
				{
					FieldID:      101,
					Name:         "field_int16",
					IsPrimaryKey: true,
					Description:  "",
					DataType:     3,
				},
			},
		}
		_, err := CreateSchemaHelper(schema)
		assert.Error(t, err)
		assert.EqualError(t, err, "primary key is not unique")
	})
	t.Run("field not exist", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     2,
				},
			},
		}
		helper, err := CreateSchemaHelper(schema)
		assert.NoError(t, err)

		_, err = helper.GetPrimaryKeyField()
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get primary key field: no primary in schema")

		_, err = helper.GetFieldFromName("none")
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get field schema by name: fieldName(none) not found")

		_, err = helper.GetFieldFromID(101)
		assert.Error(t, err)
		assert.EqualError(t, err, "fieldID(101) not found")
	})
	t.Run("vector dim not exist", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      103,
					Name:         "field_int64",
					IsPrimaryKey: true,
					Description:  "",
					DataType:     5,
				},
				{
					FieldID:      107,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     101,
				},
			},
		}
		helper, err := CreateSchemaHelper(schema)
		assert.NoError(t, err)

		_, err = helper.GetVectorDimFromID(100)
		assert.Error(t, err)

		_, err = helper.GetVectorDimFromID(103)
		assert.Error(t, err)

		_, err = helper.GetVectorDimFromID(107)
		assert.Error(t, err)
	})
}

func genFieldData(fieldName string, fieldID int64, fieldType schemapb.DataType, fieldValue interface{}, dim int64) *schemapb.FieldData {
	var fieldData *schemapb.FieldData
	switch fieldType {
	case schemapb.DataType_Bool:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Bool,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: fieldValue.([]bool),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int8:
		data := []int32{}
		for _, v := range fieldValue.([]int8) {
			data = append(data, int32(v))
		}
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int8,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: data,
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int16:
		data := []int32{}
		for _, v := range fieldValue.([]int16) {
			data = append(data, int32(v))
		}
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int16,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: data,
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int32:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: fieldValue.([]int32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int64:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: fieldValue.([]int64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Float:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Double:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Double,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: fieldValue.([]float64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_String:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_String,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: fieldValue.([]string),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_VarChar:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: fieldValue.([]string),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_BinaryVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: fieldValue.([]byte),
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_FloatVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Float16Vector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Float16Vector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: fieldValue.([]byte),
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_BFloat16Vector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_BFloat16Vector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: fieldValue.([]byte),
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_SparseFloatVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_SparseFloatVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Dim:      dim,
							Contents: [][]byte{fieldValue.([]byte)},
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Array:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Array,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data:        fieldValue.([]*schemapb.ScalarField),
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
			FieldId: fieldID,
		}

	case schemapb.DataType_JSON:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_JSON,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: fieldValue.([][]byte),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	default:
		log.Error("not supported field type", zap.String("field type", fieldType.String()))
	}

	return fieldData
}

func TestAppendFieldData(t *testing.T) {
	const (
		Dim                        = 8
		BoolFieldName              = "BoolField"
		Int32FieldName             = "Int32Field"
		Int64FieldName             = "Int64Field"
		FloatFieldName             = "FloatField"
		DoubleFieldName            = "DoubleField"
		BinaryVectorFieldName      = "BinaryVectorField"
		FloatVectorFieldName       = "FloatVectorField"
		Float16VectorFieldName     = "Float16VectorField"
		BFloat16VectorFieldName    = "BFloat16VectorField"
		ArrayFieldName             = "ArrayField"
		SparseFloatVectorFieldName = "SparseFloatVectorField"
		BoolFieldID                = common.StartOfUserFieldID + 1
		Int32FieldID               = common.StartOfUserFieldID + 2
		Int64FieldID               = common.StartOfUserFieldID + 3
		FloatFieldID               = common.StartOfUserFieldID + 4
		DoubleFieldID              = common.StartOfUserFieldID + 5
		BinaryVectorFieldID        = common.StartOfUserFieldID + 6
		FloatVectorFieldID         = common.StartOfUserFieldID + 7
		Float16VectorFieldID       = common.StartOfUserFieldID + 8
		BFloat16VectorFieldID      = common.StartOfUserFieldID + 9
		ArrayFieldID               = common.StartOfUserFieldID + 10
		SparseFloatVectorFieldID   = common.StartOfUserFieldID + 11
	)
	BoolArray := []bool{true, false}
	Int32Array := []int32{1, 2}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	BinaryVector := []byte{0x12, 0x34}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}
	Float16Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}
	BFloat16Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}
	ArrayArray := []*schemapb.ScalarField{
		{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: []int32{1, 2, 3},
				},
			},
		},
		{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: []int32{4, 5, 6},
				},
			},
		},
	}
	SparseFloatVector := &schemapb.SparseFloatArray{
		Dim: 231,
		Contents: [][]byte{
			CreateSparseFloatRow([]uint32{}, []float32{}),
			CreateSparseFloatRow([]uint32{60, 80, 230}, []float32{2.1, 2.2, 2.3}),
		},
	}

	result := make([]*schemapb.FieldData, 11)
	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[0:Dim/8], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Float16VectorFieldName, Float16VectorFieldID, schemapb.DataType_Float16Vector, Float16Vector[0:Dim*2], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BFloat16VectorFieldName, BFloat16VectorFieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector[0:Dim*2], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(ArrayFieldName, ArrayFieldID, schemapb.DataType_Array, ArrayArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(SparseFloatVectorFieldName, SparseFloatVectorFieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[0], SparseFloatVector.Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[Dim/8:2*Dim/8], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[Dim:2*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Float16VectorFieldName, Float16VectorFieldID, schemapb.DataType_Float16Vector, Float16Vector[2*Dim:4*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BFloat16VectorFieldName, BFloat16VectorFieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector[2*Dim:4*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(ArrayFieldName, ArrayFieldID, schemapb.DataType_Array, ArrayArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(SparseFloatVectorFieldName, SparseFloatVectorFieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[1], SparseFloatVector.Dim))

	AppendFieldData(result, fieldDataArray1, 0)
	AppendFieldData(result, fieldDataArray2, 0)

	assert.Equal(t, BoolArray, result[0].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array, result[1].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array, result[2].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray, result[3].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray, result[4].GetScalars().GetDoubleData().Data)
	assert.Equal(t, BinaryVector, result[5].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector, result[6].GetVectors().GetFloatVector().Data)
	assert.Equal(t, Float16Vector, result[7].GetVectors().Data.(*schemapb.VectorField_Float16Vector).Float16Vector)
	assert.Equal(t, BFloat16Vector, result[8].GetVectors().Data.(*schemapb.VectorField_Bfloat16Vector).Bfloat16Vector)
	assert.Equal(t, ArrayArray, result[9].GetScalars().GetArrayData().Data)
	assert.Equal(t, SparseFloatVector, result[10].GetVectors().GetSparseFloatVector())
}

func TestDeleteFieldData(t *testing.T) {
	const (
		Dim                        = 8
		BoolFieldName              = "BoolField"
		Int32FieldName             = "Int32Field"
		Int64FieldName             = "Int64Field"
		FloatFieldName             = "FloatField"
		DoubleFieldName            = "DoubleField"
		JSONFieldName              = "JSONField"
		BinaryVectorFieldName      = "BinaryVectorField"
		FloatVectorFieldName       = "FloatVectorField"
		Float16VectorFieldName     = "Float16VectorField"
		BFloat16VectorFieldName    = "BFloat16VectorField"
		SparseFloatVectorFieldName = "SparseFloatVectorField"
	)

	const (
		BoolFieldID = common.StartOfUserFieldID + iota
		Int32FieldID
		Int64FieldID
		FloatFieldID
		DoubleFieldID
		JSONFieldID
		BinaryVectorFieldID
		FloatVectorFieldID
		Float16VectorFieldID
		BFloat16VectorFieldID
		SparseFloatVectorFieldID
	)
	BoolArray := []bool{true, false}
	Int32Array := []int32{1, 2}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	JSONArray := [][]byte{[]byte("{\"hello\":0}"), []byte("{\"key\":1}")}
	BinaryVector := []byte{0x12, 0x34}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}
	Float16Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}
	BFloat16Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}
	SparseFloatVector := &schemapb.SparseFloatArray{
		Dim: 231,
		Contents: [][]byte{
			CreateSparseFloatRow([]uint32{30, 41, 52}, []float32{1.1, 1.2, 1.3}),
			CreateSparseFloatRow([]uint32{60, 80, 230}, []float32{2.1, 2.2, 2.3}),
		},
	}

	result1 := make([]*schemapb.FieldData, 11)
	result2 := make([]*schemapb.FieldData, 11)
	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(JSONFieldName, JSONFieldID, schemapb.DataType_JSON, JSONArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[0:Dim/8], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Float16VectorFieldName, Float16VectorFieldID, schemapb.DataType_Float16Vector, Float16Vector[0:2*Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BFloat16VectorFieldName, BFloat16VectorFieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector[0:2*Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(SparseFloatVectorFieldName, SparseFloatVectorFieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[0], SparseFloatVector.Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(JSONFieldName, JSONFieldID, schemapb.DataType_JSON, JSONArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[Dim/8:2*Dim/8], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[Dim:2*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Float16VectorFieldName, Float16VectorFieldID, schemapb.DataType_Float16Vector, Float16Vector[2*Dim:4*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BFloat16VectorFieldName, BFloat16VectorFieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector[2*Dim:4*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(SparseFloatVectorFieldName, SparseFloatVectorFieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[1], SparseFloatVector.Dim))

	AppendFieldData(result1, fieldDataArray1, 0)
	AppendFieldData(result1, fieldDataArray2, 0)
	DeleteFieldData(result1)
	assert.Equal(t, BoolArray[0:1], result1[BoolFieldID-common.StartOfUserFieldID].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array[0:1], result1[Int32FieldID-common.StartOfUserFieldID].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array[0:1], result1[Int64FieldID-common.StartOfUserFieldID].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray[0:1], result1[FloatFieldID-common.StartOfUserFieldID].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray[0:1], result1[DoubleFieldID-common.StartOfUserFieldID].GetScalars().GetDoubleData().Data)
	assert.Equal(t, JSONArray[0:1], result1[JSONFieldID-common.StartOfUserFieldID].GetScalars().GetJsonData().Data)
	assert.Equal(t, BinaryVector[0:Dim/8], result1[BinaryVectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector[0:Dim], result1[FloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetFloatVector().Data)
	assert.Equal(t, Float16Vector[0:2*Dim], result1[Float16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Float16Vector).Float16Vector)
	assert.Equal(t, BFloat16Vector[0:2*Dim], result1[BFloat16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Bfloat16Vector).Bfloat16Vector)
	tmpSparseFloatVector := proto.Clone(SparseFloatVector).(*schemapb.SparseFloatArray)
	tmpSparseFloatVector.Contents = [][]byte{SparseFloatVector.Contents[0]}
	assert.Equal(t, tmpSparseFloatVector.Contents, result1[SparseFloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetSparseFloatVector().Contents)

	AppendFieldData(result2, fieldDataArray2, 0)
	AppendFieldData(result2, fieldDataArray1, 0)
	DeleteFieldData(result2)
	assert.Equal(t, BoolArray[1:2], result2[BoolFieldID-common.StartOfUserFieldID].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array[1:2], result2[Int32FieldID-common.StartOfUserFieldID].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array[1:2], result2[Int64FieldID-common.StartOfUserFieldID].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray[1:2], result2[FloatFieldID-common.StartOfUserFieldID].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray[1:2], result2[DoubleFieldID-common.StartOfUserFieldID].GetScalars().GetDoubleData().Data)
	assert.Equal(t, JSONArray[1:2], result2[JSONFieldID-common.StartOfUserFieldID].GetScalars().GetJsonData().Data)
	assert.Equal(t, BinaryVector[Dim/8:2*Dim/8], result2[BinaryVectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector[Dim:2*Dim], result2[FloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetFloatVector().Data)
	assert.Equal(t, Float16Vector[2*Dim:4*Dim], result2[Float16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Float16Vector).Float16Vector)
	assert.Equal(t, BFloat16Vector[2*Dim:4*Dim], result2[BFloat16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Bfloat16Vector).Bfloat16Vector)
	tmpSparseFloatVector = proto.Clone(SparseFloatVector).(*schemapb.SparseFloatArray)
	tmpSparseFloatVector.Contents = [][]byte{SparseFloatVector.Contents[1]}
	assert.EqualExportedValues(t, tmpSparseFloatVector, result2[SparseFloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetSparseFloatVector())
}

func TestEstimateEntitySize(t *testing.T) {
	samples := []*schemapb.FieldData{
		{
			FieldId:   111,
			FieldName: "float16_vector",
			Type:      schemapb.DataType_Float16Vector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim:  64,
					Data: &schemapb.VectorField_Float16Vector{},
				},
			},
		},
		{
			FieldId:   112,
			FieldName: "bfloat16_vector",
			Type:      schemapb.DataType_BFloat16Vector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim:  128,
					Data: &schemapb.VectorField_Bfloat16Vector{},
				},
			},
		},
	}
	size, error := EstimateEntitySize(samples, int(0))
	assert.NoError(t, error)
	assert.True(t, size == 384)
}

func TestGetPrimaryFieldSchema(t *testing.T) {
	int64Field := &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "int64Field",
		DataType: schemapb.DataType_Int64,
	}

	floatField := &schemapb.FieldSchema{
		FieldID:  2,
		Name:     "floatField",
		DataType: schemapb.DataType_Float,
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{int64Field, floatField},
	}

	// no primary field error
	_, err := GetPrimaryFieldSchema(schema)
	assert.Error(t, err)
	int64Field.IsPrimaryKey = true
	primaryField, err := GetPrimaryFieldSchema(schema)
	assert.NoError(t, err)
	assert.Equal(t, schemapb.DataType_Int64, primaryField.DataType)

	hasPartitionKey := HasPartitionKey(schema)
	assert.False(t, hasPartitionKey)
	int64Field.IsPartitionKey = true
	hasPartitionKey2 := HasPartitionKey(schema)
	assert.True(t, hasPartitionKey2)
}

func TestGetClusterKeyFieldSchema(t *testing.T) {
	int64Field := &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "int64Field",
		DataType: schemapb.DataType_Int64,
	}

	clusterKeyfloatField := &schemapb.FieldSchema{
		FieldID:         2,
		Name:            "floatField",
		DataType:        schemapb.DataType_Float,
		IsClusteringKey: true,
	}

	unClusterKeyfloatField := &schemapb.FieldSchema{
		FieldID:         2,
		Name:            "floatField",
		DataType:        schemapb.DataType_Float,
		IsClusteringKey: false,
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{int64Field, clusterKeyfloatField},
	}
	schema2 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{int64Field, unClusterKeyfloatField},
	}

	hasClusterKey1 := HasClusterKey(schema)
	assert.True(t, hasClusterKey1)
	hasClusterKey2 := HasClusterKey(schema2)
	assert.False(t, hasClusterKey2)
}

func TestGetPK(t *testing.T) {
	type args struct {
		data *schemapb.IDs
		idx  int64
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			args: args{
				data: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				idx: 5, // > len(data)
			},
			want: nil,
		},
		{
			args: args{
				data: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				idx: 1,
			},
			want: int64(2),
		},
		{
			args: args{
				data: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
				idx: 1,
			},
			want: "2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPK(tt.args.data, tt.args.idx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPK(%v, %v) = %v, want %v", tt.args.data, tt.args.idx, got, tt.want)
			}
		})
	}
}

func TestAppendPKs(t *testing.T) {
	intPks := &schemapb.IDs{}
	AppendPKs(intPks, int64(1))
	assert.ElementsMatch(t, []int64{1}, intPks.GetIntId().GetData())
	AppendPKs(intPks, int64(2))
	assert.ElementsMatch(t, []int64{1, 2}, intPks.GetIntId().GetData())

	strPks := &schemapb.IDs{}
	AppendPKs(strPks, "1")
	assert.ElementsMatch(t, []string{"1"}, strPks.GetStrId().GetData())
	AppendPKs(strPks, "2")
	assert.ElementsMatch(t, []string{"1", "2"}, strPks.GetStrId().GetData())
}

func TestSwapPK(t *testing.T) {
	intPks := &schemapb.IDs{}
	AppendPKs(intPks, int64(1))
	AppendPKs(intPks, int64(2))
	AppendPKs(intPks, int64(3))
	require.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())

	SwapPK(intPks, 0, 1)
	assert.Equal(t, []int64{2, 1, 3}, intPks.GetIntId().GetData())
	SwapPK(intPks, 0, 1)
	assert.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())
	SwapPK(intPks, 0, 2)
	assert.Equal(t, []int64{3, 2, 1}, intPks.GetIntId().GetData())
	SwapPK(intPks, 0, 2)
	assert.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())
	SwapPK(intPks, 1, 2)
	assert.Equal(t, []int64{1, 3, 2}, intPks.GetIntId().GetData())
	SwapPK(intPks, 1, 2)
	assert.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())

	strPks := &schemapb.IDs{}
	AppendPKs(strPks, "1")
	AppendPKs(strPks, "2")
	AppendPKs(strPks, "3")

	require.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())

	SwapPK(strPks, 0, 1)
	assert.Equal(t, []string{"2", "1", "3"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 0, 1)
	assert.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 0, 2)
	assert.Equal(t, []string{"3", "2", "1"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 0, 2)
	assert.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 1, 2)
	assert.Equal(t, []string{"1", "3", "2"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 1, 2)
	assert.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())
}

func TestComparePk(t *testing.T) {
	intPks := &schemapb.IDs{}
	AppendPKs(intPks, int64(1))
	AppendPKs(intPks, int64(2))
	AppendPKs(intPks, int64(3))
	require.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())

	less := ComparePKInSlice(intPks, 0, 1)
	assert.True(t, less)
	less = ComparePKInSlice(intPks, 0, 2)
	assert.True(t, less)
	less = ComparePKInSlice(intPks, 1, 2)
	assert.True(t, less)

	less = ComparePKInSlice(intPks, 1, 0)
	assert.False(t, less)
	less = ComparePKInSlice(intPks, 2, 0)
	assert.False(t, less)
	less = ComparePKInSlice(intPks, 2, 1)
	assert.False(t, less)

	strPks := &schemapb.IDs{}
	AppendPKs(strPks, "1")
	AppendPKs(strPks, "2")
	AppendPKs(strPks, "3")

	require.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())

	less = ComparePKInSlice(strPks, 0, 1)
	assert.True(t, less)
	less = ComparePKInSlice(strPks, 0, 2)
	assert.True(t, less)
	less = ComparePKInSlice(strPks, 1, 2)
	assert.True(t, less)

	less = ComparePKInSlice(strPks, 1, 0)
	assert.False(t, less)
	less = ComparePKInSlice(strPks, 2, 0)
	assert.False(t, less)
	less = ComparePKInSlice(strPks, 2, 1)
	assert.False(t, less)
}

func TestCalcColumnSize(t *testing.T) {
	fieldValues := map[int64]any{
		100: []int8{0, 1},
		101: []int16{0, 1},
		102: []int32{0, 1},
		103: []int64{0, 1},
		104: []float32{0, 1},
		105: []float64{0, 1},
		106: []string{"0", "1"},
		107: []float32{0, 1, 2, 3},
		109: []*schemapb.ScalarField{
			{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3},
					},
				},
			},
			{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{4, 5, 6},
					},
				},
			},
		},
		110: [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)},
	}
	schema := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int8",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int8,
			},
			{
				FieldID:      101,
				Name:         "field_int16",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Int16,
			},
			{
				FieldID:      102,
				Name:         "field_int32",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      103,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      104,
				Name:         "field_float",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      105,
				Name:         "field_double",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Double,
			},
			{
				FieldID:      106,
				Name:         "field_string",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "125",
					},
				},
			},
			{
				FieldID:     109,
				Name:        "field_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
			{
				FieldID:  110,
				Name:     "field_json",
				DataType: schemapb.DataType_JSON,
			},
		},
	}

	for _, field := range schema.GetFields() {
		values := fieldValues[field.GetFieldID()]
		fieldData := genFieldData(field.GetName(), field.GetFieldID(), field.GetDataType(), values, 0)
		size := CalcColumnSize(fieldData)
		expected := 0
		switch field.GetDataType() {
		case schemapb.DataType_VarChar:
			data := values.([]string)
			for _, v := range data {
				expected += len(v)
			}
		case schemapb.DataType_Array:
			data := values.([]*schemapb.ScalarField)
			for _, v := range data {
				expected += binary.Size(v.GetIntData().GetData())
			}
		case schemapb.DataType_JSON:
			data := values.([][]byte)
			for _, v := range data {
				expected += len(v)
			}

		default:
			expected = binary.Size(fieldValues[field.GetFieldID()])
		}

		assert.Equal(t, expected, size, field.GetName())
	}
}

func TestGetDataAndGetDataSize(t *testing.T) {
	const (
		Dim       = 8
		fieldName = "field-0"
		fieldID   = 0
	)

	BoolArray := []bool{true, false}
	Int8Array := []int8{1, 2}
	Int16Array := []int16{3, 4}
	Int32Array := []int32{5, 6}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	VarCharArray := []string{"a", "b"}
	BinaryVector := []byte{0x12, 0x34}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}
	Float16Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
	}
	BFloat16Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
	}
	SparseFloatVector := &schemapb.SparseFloatArray{
		Dim: 231,
		Contents: [][]byte{
			CreateSparseFloatRow([]uint32{}, []float32{}),
			CreateSparseFloatRow([]uint32{60, 80, 230}, []float32{2.1, 2.2, 2.3}),
		},
	}

	boolData := genFieldData(fieldName, fieldID, schemapb.DataType_Bool, BoolArray, 1)
	int8Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int8, Int8Array, 1)
	int16Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int16, Int16Array, 1)
	int32Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int32, Int32Array, 1)
	int64Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int64, Int64Array, 1)
	floatData := genFieldData(fieldName, fieldID, schemapb.DataType_Float, FloatArray, 1)
	doubleData := genFieldData(fieldName, fieldID, schemapb.DataType_Double, DoubleArray, 1)
	varCharData := genFieldData(fieldName, fieldID, schemapb.DataType_VarChar, VarCharArray, 1)
	binVecData := genFieldData(fieldName, fieldID, schemapb.DataType_BinaryVector, BinaryVector, Dim)
	floatVecData := genFieldData(fieldName, fieldID, schemapb.DataType_FloatVector, FloatVector, Dim)
	float16VecData := genFieldData(fieldName, fieldID, schemapb.DataType_Float16Vector, Float16Vector, Dim)
	bfloat16VecData := genFieldData(fieldName, fieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector, Dim)
	sparseFloatData := genFieldData(fieldName, fieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[0], SparseFloatVector.Dim)
	invalidData := &schemapb.FieldData{
		Type: schemapb.DataType_None,
	}

	t.Run("test GetPKSize", func(t *testing.T) {
		int64DataRes := GetPKSize(int64Data)
		varCharDataRes := GetPKSize(varCharData)

		assert.Equal(t, 2, int64DataRes)
		assert.Equal(t, 2, varCharDataRes)
	})

	t.Run("test GetData", func(t *testing.T) {
		boolDataRes := GetData(boolData, 0)
		int8DataRes := GetData(int8Data, 0)
		int16DataRes := GetData(int16Data, 0)
		int32DataRes := GetData(int32Data, 0)
		int64DataRes := GetData(int64Data, 0)
		floatDataRes := GetData(floatData, 0)
		doubleDataRes := GetData(doubleData, 0)
		varCharDataRes := GetData(varCharData, 0)
		binVecDataRes := GetData(binVecData, 0)
		floatVecDataRes := GetData(floatVecData, 0)
		float16VecDataRes := GetData(float16VecData, 0)
		bfloat16VecDataRes := GetData(bfloat16VecData, 0)
		sparseFloatDataRes := GetData(sparseFloatData, 0)
		invalidDataRes := GetData(invalidData, 0)

		assert.Equal(t, BoolArray[0], boolDataRes)
		assert.Equal(t, int32(Int8Array[0]), int8DataRes)
		assert.Equal(t, int32(Int16Array[0]), int16DataRes)
		assert.Equal(t, Int32Array[0], int32DataRes)
		assert.Equal(t, Int64Array[0], int64DataRes)
		assert.Equal(t, FloatArray[0], floatDataRes)
		assert.Equal(t, DoubleArray[0], doubleDataRes)
		assert.Equal(t, VarCharArray[0], varCharDataRes)
		assert.ElementsMatch(t, BinaryVector[:Dim/8], binVecDataRes)
		assert.ElementsMatch(t, FloatVector[:Dim], floatVecDataRes)
		assert.ElementsMatch(t, Float16Vector[:2*Dim], float16VecDataRes)
		assert.ElementsMatch(t, BFloat16Vector[:2*Dim], bfloat16VecDataRes)
		assert.Equal(t, SparseFloatVector.Contents[0], sparseFloatDataRes)
		assert.Nil(t, invalidDataRes)
	})
}

func TestMergeFieldData(t *testing.T) {
	sparseFloatRows := [][]byte{
		// 3 rows for dst
		CreateSparseFloatRow([]uint32{30, 41, 52}, []float32{1.1, 1.2, 1.3}),
		CreateSparseFloatRow([]uint32{60, 80, 230}, []float32{2.1, 2.2, 2.3}),
		CreateSparseFloatRow([]uint32{300, 410, 520}, []float32{1.1, 1.2, 1.3}),
		// 3 rows for src
		CreateSparseFloatRow([]uint32{600, 800, 2300}, []float32{2.1, 2.2, 2.3}),
		CreateSparseFloatRow([]uint32{90, 141, 352}, []float32{1.1, 1.2, 1.3}),
		CreateSparseFloatRow([]uint32{}, []float32{}),
	}

	t.Run("merge data", func(t *testing.T) {
		dstFields := []*schemapb.FieldData{
			genFieldData("int64", 100, schemapb.DataType_Int64, []int64{1, 2, 3}, 1),
			genFieldData("vector", 101, schemapb.DataType_FloatVector, []float32{1, 2, 3}, 1),
			genFieldData("json", 102, schemapb.DataType_JSON, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)}, 1),
			genFieldData("array", 103, schemapb.DataType_Array, []*schemapb.ScalarField{
				{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{1, 2, 3},
						},
					},
				},
			}, 1),
			{
				Type:      schemapb.DataType_Array,
				FieldName: "bytes",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BytesData{},
					},
				},
				FieldId: 104,
			},
			{
				Type:      schemapb.DataType_Array,
				FieldName: "bytes",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BytesData{
							BytesData: &schemapb.BytesArray{
								Data: [][]byte{[]byte("hello"), []byte("world")},
							},
						},
					},
				},
				FieldId: 105,
			},
			{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: "sparseFloat",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 521,
						Data: &schemapb.VectorField_SparseFloatVector{
							SparseFloatVector: &schemapb.SparseFloatArray{
								Dim:      521,
								Contents: sparseFloatRows[:3],
							},
						},
					},
				},
				FieldId: 106,
			},
			genFieldData("float16_vector", 111, schemapb.DataType_Float16Vector, []byte("12345678"), 4),
			genFieldData("bfloat16_vector", 112, schemapb.DataType_BFloat16Vector, []byte("12345678"), 4),
		}

		srcFields := []*schemapb.FieldData{
			genFieldData("int64", 100, schemapb.DataType_Int64, []int64{4, 5, 6}, 1),
			genFieldData("vector", 101, schemapb.DataType_FloatVector, []float32{4, 5, 6}, 1),
			genFieldData("json", 102, schemapb.DataType_JSON, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)}, 1),
			genFieldData("array", 103, schemapb.DataType_Array, []*schemapb.ScalarField{
				{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{4, 5, 6},
						},
					},
				},
			}, 1),
			{
				Type:      schemapb.DataType_Array,
				FieldName: "bytes",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BytesData{
							BytesData: &schemapb.BytesArray{
								Data: [][]byte{[]byte("hoo"), []byte("foo")},
							},
						},
					},
				},
				FieldId: 104,
			},
			{
				Type:      schemapb.DataType_Array,
				FieldName: "bytes",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BytesData{
							BytesData: &schemapb.BytesArray{
								Data: [][]byte{[]byte("hoo")},
							},
						},
					},
				},
				FieldId: 105,
			},
			{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: "sparseFloat",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 2301,
						Data: &schemapb.VectorField_SparseFloatVector{
							SparseFloatVector: &schemapb.SparseFloatArray{
								Dim:      2301,
								Contents: sparseFloatRows[3:],
							},
						},
					},
				},
				FieldId: 106,
			},
			genFieldData("float16_vector", 111, schemapb.DataType_Float16Vector, []byte("abcdefgh"), 4),
			genFieldData("bfloat16_vector", 112, schemapb.DataType_BFloat16Vector, []byte("ABCDEFGH"), 4),
		}

		err := MergeFieldData(dstFields, srcFields)
		assert.NoError(t, err)

		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, dstFields[0].GetScalars().GetLongData().Data)
		assert.Equal(t, []float32{1, 2, 3, 4, 5, 6}, dstFields[1].GetVectors().GetFloatVector().Data)
		assert.Equal(t, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`), []byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)},
			dstFields[2].GetScalars().GetJsonData().Data)
		assert.Equal(t, []*schemapb.ScalarField{
			{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3},
					},
				},
			},
			{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{4, 5, 6},
					},
				},
			},
		},
			dstFields[3].GetScalars().GetArrayData().Data)
		assert.Equal(t, [][]byte{[]byte("hoo"), []byte("foo")}, dstFields[4].GetScalars().GetBytesData().Data)
		assert.Equal(t, [][]byte{[]byte("hello"), []byte("world"), []byte("hoo")}, dstFields[5].GetScalars().GetBytesData().Data)
		assert.Equal(t, &schemapb.SparseFloatArray{
			Dim:      2301,
			Contents: sparseFloatRows,
		}, dstFields[6].GetVectors().GetSparseFloatVector())
		assert.Equal(t, []byte("12345678abcdefgh"), dstFields[7].GetVectors().GetFloat16Vector())
		assert.Equal(t, []byte("12345678ABCDEFGH"), dstFields[8].GetVectors().GetBfloat16Vector())
	})

	t.Run("merge with nil", func(t *testing.T) {
		srcFields := []*schemapb.FieldData{
			genFieldData("int64", 100, schemapb.DataType_Int64, []int64{1, 2, 3}, 1),
			genFieldData("vector", 101, schemapb.DataType_FloatVector, []float32{1, 2, 3}, 1),
			genFieldData("json", 102, schemapb.DataType_JSON, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)}, 1),
			genFieldData("array", 103, schemapb.DataType_Array, []*schemapb.ScalarField{
				{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{1, 2, 3},
						},
					},
				},
			}, 1),
			{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: "sparseFloat",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 521,
						Data: &schemapb.VectorField_SparseFloatVector{
							SparseFloatVector: &schemapb.SparseFloatArray{
								Dim:      521,
								Contents: sparseFloatRows[:3],
							},
						},
					},
				},
				FieldId: 104,
			},
			genFieldData("float16_vector", 111, schemapb.DataType_Float16Vector, []byte("12345678"), 4),
			genFieldData("bfloat16_vector", 112, schemapb.DataType_BFloat16Vector, []byte("12345678"), 4),
		}

		dstFields := []*schemapb.FieldData{
			{Type: schemapb.DataType_Int64, FieldName: "int64", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{}}}, FieldId: 100},
			{Type: schemapb.DataType_FloatVector, FieldName: "vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{}}}, FieldId: 101},
			{Type: schemapb.DataType_JSON, FieldName: "json", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{}}}, FieldId: 102},
			{Type: schemapb.DataType_Array, FieldName: "array", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{}}}, FieldId: 103},
			{Type: schemapb.DataType_SparseFloatVector, FieldName: "sparseFloat", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{}}}, FieldId: 104},
			{Type: schemapb.DataType_Float16Vector, FieldName: "float16_vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{}}}, FieldId: 111},
			{Type: schemapb.DataType_BFloat16Vector, FieldName: "bfloat16_vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{}}}, FieldId: 112},
		}

		err := MergeFieldData(dstFields, srcFields)
		assert.NoError(t, err)

		assert.Equal(t, []int64{1, 2, 3}, dstFields[0].GetScalars().GetLongData().Data)
		assert.Equal(t, []float32{1, 2, 3}, dstFields[1].GetVectors().GetFloatVector().Data)
		assert.Equal(t, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)},
			dstFields[2].GetScalars().GetJsonData().Data)
		assert.Equal(t, []*schemapb.ScalarField{
			{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3},
					},
				},
			},
		},
			dstFields[3].GetScalars().GetArrayData().Data)
		assert.Equal(t, &schemapb.SparseFloatArray{
			Dim:      521,
			Contents: sparseFloatRows[:3],
		}, dstFields[4].GetVectors().GetSparseFloatVector())
		assert.Equal(t, []byte("12345678"), dstFields[5].GetVectors().GetFloat16Vector())
		assert.Equal(t, []byte("12345678"), dstFields[6].GetVectors().GetBfloat16Vector())
	})

	t.Run("error case", func(t *testing.T) {
		emptyField := &schemapb.FieldData{
			Type: schemapb.DataType_None,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: nil,
				},
			},
		}

		err := MergeFieldData([]*schemapb.FieldData{emptyField}, []*schemapb.FieldData{emptyField})
		assert.Error(t, err)
	})
}

type FieldDataSuite struct {
	suite.Suite
}

func (s *FieldDataSuite) TestPrepareFieldData() {
	fieldID := int64(100)
	fieldName := "testField"
	topK := int64(100)

	s.Run("bool", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Bool, field.GetType())

		s.EqualValues(topK, cap(field.GetScalars().GetBoolData().GetData()))
	})

	s.Run("int", func() {
		dataTypes := []schemapb.DataType{
			schemapb.DataType_Int32,
			schemapb.DataType_Int16,
			schemapb.DataType_Int8,
		}
		for _, dataType := range dataTypes {
			samples := []*schemapb.FieldData{
				{
					FieldId:   fieldID,
					FieldName: fieldName,
					Type:      dataType,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_IntData{},
						},
					},
				},
			}

			fields := PrepareResultFieldData(samples, topK)
			s.Require().Len(fields, 1)
			field := fields[0]
			s.Equal(fieldID, field.GetFieldId())
			s.Equal(fieldName, field.GetFieldName())
			s.Equal(dataType, field.GetType())

			s.EqualValues(topK, cap(field.GetScalars().GetIntData().GetData()))
		}
	})

	s.Run("long", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Int64, field.GetType())

		s.EqualValues(topK, cap(field.GetScalars().GetLongData().GetData()))
	})

	s.Run("float", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Float, field.GetType())

		s.EqualValues(topK, cap(field.GetScalars().GetFloatData().GetData()))
	})

	s.Run("double", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Double, field.GetType())

		s.EqualValues(topK, cap(field.GetScalars().GetDoubleData().GetData()))
	})

	s.Run("string", func() {
		dataTypes := []schemapb.DataType{
			schemapb.DataType_VarChar,
			schemapb.DataType_String,
		}
		for _, dataType := range dataTypes {
			samples := []*schemapb.FieldData{
				{
					FieldId:   fieldID,
					FieldName: fieldName,
					Type:      dataType,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{},
						},
					},
				},
			}

			fields := PrepareResultFieldData(samples, topK)
			s.Require().Len(fields, 1)
			field := fields[0]
			s.Equal(fieldID, field.GetFieldId())
			s.Equal(fieldName, field.GetFieldName())
			s.Equal(dataType, field.GetType())

			s.EqualValues(topK, cap(field.GetScalars().GetStringData().GetData()))
		}
	})

	s.Run("json", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_JSON,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_JSON, field.GetType())

		s.EqualValues(topK, cap(field.GetScalars().GetJsonData().GetData()))
	})

	s.Run("array", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								ElementType: schemapb.DataType_Bool,
							},
						},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Array, field.GetType())

		s.EqualValues(topK, cap(field.GetScalars().GetArrayData().GetData()))
		s.Equal(schemapb.DataType_Bool, field.GetScalars().GetArrayData().GetElementType())
	})

	s.Run("float_vector", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  128,
						Data: &schemapb.VectorField_FloatVector{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_FloatVector, field.GetType())

		s.EqualValues(128, field.GetVectors().GetDim())
		s.EqualValues(topK*128, cap(field.GetVectors().GetFloatVector().GetData()))
	})

	s.Run("float16_vector", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Float16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  128,
						Data: &schemapb.VectorField_Float16Vector{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Float16Vector, field.GetType())

		s.EqualValues(128, field.GetVectors().GetDim())
		s.EqualValues(topK*128*2, cap(field.GetVectors().GetFloat16Vector()))
	})

	s.Run("bfloat16_vector", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_BFloat16Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  128,
						Data: &schemapb.VectorField_Bfloat16Vector{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_BFloat16Vector, field.GetType())

		s.EqualValues(128, field.GetVectors().GetDim())
		s.EqualValues(topK*128*2, cap(field.GetVectors().GetBfloat16Vector()))
	})

	s.Run("binary_vector", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_BinaryVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  128,
						Data: &schemapb.VectorField_BinaryVector{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_BinaryVector, field.GetType())

		s.EqualValues(128, field.GetVectors().GetDim())
		s.EqualValues(topK*128/8, cap(field.GetVectors().GetBinaryVector()))
	})

	s.Run("sparse_float_vector", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_SparseFloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  128,
						Data: &schemapb.VectorField_SparseFloatVector{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_SparseFloatVector, field.GetType())

		s.EqualValues(0, field.GetVectors().GetDim())
		s.EqualValues(topK, cap(field.GetVectors().GetSparseFloatVector().GetContents()))
	})
}

func TestFieldData(t *testing.T) {
	suite.Run(t, new(FieldDataSuite))
}

func TestValidateSparseFloatRows(t *testing.T) {
	t.Run("valid rows", func(t *testing.T) {
		rows := [][]byte{
			CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}),
			CreateSparseFloatRow([]uint32{2, 4, 6}, []float32{4.0, 5.0, 6.0}),
			CreateSparseFloatRow([]uint32{0, 7, 8}, []float32{7.0, 8.0, 9.0}),
			// we allow empty row(including indices with 0 value)
			CreateSparseFloatRow([]uint32{}, []float32{}),
			CreateSparseFloatRow([]uint32{24}, []float32{0}),
			{},
		}
		err := ValidateSparseFloatRows(rows...)
		assert.NoError(t, err)
	})

	t.Run("nil row", func(t *testing.T) {
		err := ValidateSparseFloatRows(nil)
		assert.Error(t, err)
	})

	t.Run("incorrect lengths", func(t *testing.T) {
		rows := [][]byte{
			make([]byte, 10),
		}
		err := ValidateSparseFloatRows(rows...)
		assert.Error(t, err)
	})

	t.Run("unordered index", func(t *testing.T) {
		rows := [][]byte{
			CreateSparseFloatRow([]uint32{100, 2000, 500}, []float32{1.0, 2.0, 3.0}),
		}
		err := ValidateSparseFloatRows(rows...)
		assert.Error(t, err)
	})

	t.Run("same index", func(t *testing.T) {
		rows := [][]byte{
			CreateSparseFloatRow([]uint32{100, 100, 500}, []float32{1.0, 2.0, 3.0}),
		}
		err := ValidateSparseFloatRows(rows...)
		assert.Error(t, err)
	})

	t.Run("negative value", func(t *testing.T) {
		rows := [][]byte{
			CreateSparseFloatRow([]uint32{100, 200, 500}, []float32{-1.0, 2.0, 3.0}),
		}
		err := ValidateSparseFloatRows(rows...)
		assert.Error(t, err)
	})

	t.Run("invalid value", func(t *testing.T) {
		rows := [][]byte{
			CreateSparseFloatRow([]uint32{100, 200, 500}, []float32{float32(math.NaN()), 2.0, 3.0}),
		}
		err := ValidateSparseFloatRows(rows...)
		assert.Error(t, err)

		rows = [][]byte{
			CreateSparseFloatRow([]uint32{100, 200, 500}, []float32{float32(math.Inf(1)), 2.0, 3.0}),
		}
		err = ValidateSparseFloatRows(rows...)
		assert.Error(t, err)

		rows = [][]byte{
			CreateSparseFloatRow([]uint32{100, 200, 500}, []float32{float32(math.Inf(-1)), 2.0, 3.0}),
		}
		err = ValidateSparseFloatRows(rows...)
		assert.Error(t, err)
	})

	t.Run("invalid index", func(t *testing.T) {
		rows := [][]byte{
			CreateSparseFloatRow([]uint32{3, 5, math.MaxUint32}, []float32{1.0, 2.0, 3.0}),
		}
		err := ValidateSparseFloatRows(rows...)
		assert.Error(t, err)
	})

	t.Run("no rows", func(t *testing.T) {
		err := ValidateSparseFloatRows()
		assert.NoError(t, err)
	})
}

func TestParseJsonSparseFloatRow(t *testing.T) {
	t.Run("valid row 1", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{1, 3, 5}, "values": []interface{}{1.0, 2.0, 3.0}}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}), res)
	})

	t.Run("valid row 2", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{3, 1, 5}, "values": []interface{}{1.0, 2.0, 3.0}}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{2.0, 1.0, 3.0}), res)
	})

	t.Run("valid row 3", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{1, 3, 5}, "values": []interface{}{1, 2, 3}}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}), res)
	})

	t.Run("valid row 4", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{math.MaxInt32 + 1}, "values": []interface{}{1.0}}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{math.MaxInt32 + 1}, []float32{1.0}), res)
	})

	t.Run("valid row 5", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{}, "values": []interface{}{}}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{}, []float32{}), res)
	})

	t.Run("valid row 6", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{1}, "values": []interface{}{0}}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1}, []float32{0}), res)
	})

	t.Run("invalid row 1", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{1, 3, 5}, "values": []interface{}{1.0, 2.0}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 2", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{1}, "values": []interface{}{1.0, 2.0}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 4", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{3}, "values": []interface{}{-0.2}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 5", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{3.1}, "values": []interface{}{0.2}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 6", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{-1}, "values": []interface{}{0.2}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 7", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{math.MaxUint32}, "values": []interface{}{1.0}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 8", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{math.MaxUint32 + 10}, "values": []interface{}{1.0}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 9", func(t *testing.T) {
		row := map[string]interface{}{"indices": []interface{}{10}, "values": []interface{}{float64(math.MaxFloat32) * 2}}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("valid dict row 1", func(t *testing.T) {
		row := map[string]interface{}{"1": 1.0, "3": 2.0, "5": 3.0}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}), res)
	})

	t.Run("valid dict row 2", func(t *testing.T) {
		row := map[string]interface{}{"3": 1.0, "1": 2.0, "5": 3.0}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{2.0, 1.0, 3.0}), res)
	})

	t.Run("valid dict row 3", func(t *testing.T) {
		row := map[string]interface{}{}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{}, []float32{}), res)
	})

	t.Run("valid dict row 4", func(t *testing.T) {
		row := map[string]interface{}{"1": 0}
		res, err := CreateSparseFloatRowFromMap(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1}, []float32{0}), res)
	})

	t.Run("invalid dict row 1", func(t *testing.T) {
		row := map[string]interface{}{"a": 1.0, "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 2", func(t *testing.T) {
		row := map[string]interface{}{"1": "a", "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 3", func(t *testing.T) {
		row := map[string]interface{}{"1": "1.0", "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 4", func(t *testing.T) {
		row := map[string]interface{}{"-1": 1.0, "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 5", func(t *testing.T) {
		row := map[string]interface{}{"1": -1.0, "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 7", func(t *testing.T) {
		row := map[string]interface{}{fmt.Sprint(math.MaxUint32): 1.0, "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 8", func(t *testing.T) {
		row := map[string]interface{}{fmt.Sprint(math.MaxUint32 + 10): 1.0, "3": 2.0, "5": 3.0}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 8", func(t *testing.T) {
		row := map[string]interface{}{fmt.Sprint(math.MaxUint32 + 10): 1.0, "3": 2.0, "5": float64(math.MaxFloat32) * 2}
		_, err := CreateSparseFloatRowFromMap(row)
		assert.Error(t, err)
	})
}

func TestParseJsonSparseFloatRowBytes(t *testing.T) {
	t.Run("valid row 1", func(t *testing.T) {
		row := []byte(`{"indices":[1,3,5],"values":[1.0,2.0,3.0]}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}), res)
	})

	t.Run("valid row 2", func(t *testing.T) {
		row := []byte(`{"indices":[3,1,5],"values":[1.0,2.0,3.0]}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{2.0, 1.0, 3.0}), res)
	})

	t.Run("valid row 3", func(t *testing.T) {
		row := []byte(`{"indices":[1, 3, 5], "values":[1, 2, 3]}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}), res)
	})

	t.Run("valid row 3", func(t *testing.T) {
		row := []byte(`{"indices":[2147483648], "values":[1.0]}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{math.MaxInt32 + 1}, []float32{1.0}), res)
	})

	t.Run("valid row 4", func(t *testing.T) {
		row := []byte(`{"indices":[], "values":[]}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{}, []float32{}), res)
	})

	t.Run("valid row 5", func(t *testing.T) {
		row := []byte(`{"indices":[1], "values":[0]}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1}, []float32{0}), res)
	})

	t.Run("invalid row 1", func(t *testing.T) {
		row := []byte(`{"indices":[1,3,5],"values":[1.0,2.0,3.0`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 2", func(t *testing.T) {
		row := []byte(`{"indices":[1,3,5],"values":[1.0,2.0]`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 3", func(t *testing.T) {
		row := []byte(`{"indices":[1],"values":[1.0,2.0]`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 4", func(t *testing.T) {
		row := []byte(`{"indices":[],"values":[]`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 5", func(t *testing.T) {
		row := []byte(`{"indices":[-3],"values":[0.2]`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 6", func(t *testing.T) {
		row := []byte(`{"indices":[3],"values":[-0.2]`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid row 7", func(t *testing.T) {
		row := []byte(`{"indices": []interface{}{3.1}, "values": []interface{}{0.2}}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("valid dict row 1", func(t *testing.T) {
		row := []byte(`{"1": 1.0, "3": 2.0, "5": 3.0}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.0, 2.0, 3.0}), res)
	})

	t.Run("valid dict row 2", func(t *testing.T) {
		row := []byte(`{"3": 1.0, "1": 2.0, "5": 3.0}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{2.0, 1.0, 3.0}), res)
	})

	t.Run("valid dict row 3", func(t *testing.T) {
		row := []byte(`{}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{}, []float32{}), res)
	})

	t.Run("valid dict row 4", func(t *testing.T) {
		row := []byte(`{"1": 0}`)
		res, err := CreateSparseFloatRowFromJSON(row)
		assert.NoError(t, err)
		assert.Equal(t, CreateSparseFloatRow([]uint32{1}, []float32{0}), res)
	})

	t.Run("invalid dict row 1", func(t *testing.T) {
		row := []byte(`{"a": 1.0, "3": 2.0, "5": 3.0}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 2", func(t *testing.T) {
		row := []byte(`{"1": "a", "3": 2.0, "5": 3.0}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 3", func(t *testing.T) {
		row := []byte(`{"1": "1.0", "3": 2.0, "5": 3.0}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 4", func(t *testing.T) {
		row := []byte(`{"1": 1.0, "3": 2.0, "5": }`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 5", func(t *testing.T) {
		row := []byte(`{"-1": 1.0, "3": 2.0, "5": 3.0}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 6", func(t *testing.T) {
		row := []byte(`{"1": -1.0, "3": 2.0, "5": 3.0}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 7", func(t *testing.T) {
		row := []byte(``)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})

	t.Run("invalid dict row 8", func(t *testing.T) {
		row := []byte(`{"1.1": 1.0, "3": 2.0, "5": 3.0}`)
		_, err := CreateSparseFloatRowFromJSON(row)
		assert.Error(t, err)
	})
}
