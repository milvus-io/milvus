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
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
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
				DataType:     schemapb.DataType_String,
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
				DataType:     schemapb.DataType_FloatVector,
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
				DataType:     schemapb.DataType_BinaryVector,
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
				DataType:     schemapb.DataType_Float16Vector,
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
				DataType:     schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			// Do not test on sparse float vector field.
			{
				FieldID:      113,
				Name:         "field_int8_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:      114,
				Name:         "field_geometry",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Geometry,
			},
		},
	}

	t.Run("EstimateSizePerRecord", func(t *testing.T) {
		size, err := EstimateSizePerRecord(schema)
		assert.Equal(t, 2731, size)
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

		dim, err = helper.GetVectorDimFromID(108)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim)

		_, err = helper.GetVectorDimFromID(103)
		assert.Error(t, err)

		dim, err = helper.GetVectorDimFromID(111)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim)

		dim, err = helper.GetVectorDimFromID(112)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim)

		dim, err = helper.GetVectorDimFromID(113)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim)
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
		assert.True(t, IsVectorType(schemapb.DataType_Int8Vector))
		assert.True(t, IsVectorType(schemapb.DataType_ArrayOfVector))

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
		assert.False(t, IsIntegerType(schemapb.DataType_Int8Vector))

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
		assert.False(t, IsFloatingType(schemapb.DataType_Int8Vector))

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
		assert.False(t, IsSparseFloatVectorType(schemapb.DataType_Int8Vector))

		assert.True(t, IsVectorArrayType(schemapb.DataType_ArrayOfVector))
	})
}

func TestSchema_GetVectorFieldSchemas(t *testing.T) {
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

	t.Run("GetVectorFieldSchemas", func(t *testing.T) {
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
	case schemapb.DataType_Int8Vector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int8Vector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: fieldValue.([]byte),
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
	case schemapb.DataType_ArrayOfVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_ArrayOfVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Data:        fieldValue.([]*schemapb.VectorField),
							ElementType: schemapb.DataType_FloatVector,
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Geometry:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Geometry,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryData{
						GeometryData: &schemapb.GeometryArray{
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
		Int8VectorFieldName        = "Int8VectorField"
		VectorArrayFieldName       = "VectorArrayField"
		GeometryFieldName          = "GeometryField"
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
		Int8VectorFieldID          = common.StartOfUserFieldID + 12
		VectorArrayFieldID         = common.StartOfUserFieldID + 13
		GeometryFieldID            = common.StartOfUserFieldID + 14
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
	Int8Vector := []byte{
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
	VectorArray := []*schemapb.VectorField{
		{
			Dim: Dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: FloatVector,
				},
			},
		},
		{
			Dim: Dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: FloatVector,
				},
			},
		},
	}

	result := make([]*schemapb.FieldData, 14)
	// POINT (30.123 -10.456) and LINESTRING (30.123 -10.456, 10.789 30.123, -40.567 40.890)
	GeometryArray := [][]byte{
		{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40},
		{0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40, 0x03, 0xA6, 0xB4, 0xA6, 0xA4, 0xD2, 0xC5, 0xC0, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A},
	}

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
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int8VectorFieldName, Int8VectorFieldID, schemapb.DataType_Int8Vector, Int8Vector[0:Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(VectorArrayFieldName, VectorArrayFieldID, schemapb.DataType_ArrayOfVector, VectorArray[0:1], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(GeometryFieldName, GeometryFieldID, schemapb.DataType_Geometry, GeometryArray[0:1], 1))

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
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int8VectorFieldName, Int8VectorFieldID, schemapb.DataType_Int8Vector, Int8Vector[Dim:2*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(VectorArrayFieldName, VectorArrayFieldID, schemapb.DataType_ArrayOfVector, VectorArray[1:2], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(GeometryFieldName, GeometryFieldID, schemapb.DataType_Geometry, GeometryArray[1:2], 1))

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
	assert.Equal(t, Int8Vector, result[11].GetVectors().Data.(*schemapb.VectorField_Int8Vector).Int8Vector)
	assert.Equal(t, VectorArray, result[12].GetVectors().GetVectorArray().Data)
	assert.Equal(t, GeometryArray, result[13].GetScalars().GetGeometryData().Data)
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
		GeometryFieldName          = "GeometryField"
		BinaryVectorFieldName      = "BinaryVectorField"
		FloatVectorFieldName       = "FloatVectorField"
		Float16VectorFieldName     = "Float16VectorField"
		BFloat16VectorFieldName    = "BFloat16VectorField"
		SparseFloatVectorFieldName = "SparseFloatVectorField"
		Int8VectorFieldName        = "Int8VectorField"
	)

	const (
		BoolFieldID = common.StartOfUserFieldID + iota
		Int32FieldID
		Int64FieldID
		FloatFieldID
		DoubleFieldID
		JSONFieldID
		GeometryFiledID
		BinaryVectorFieldID
		FloatVectorFieldID
		Float16VectorFieldID
		BFloat16VectorFieldID
		SparseFloatVectorFieldID
		Int8VectorFieldID
	)
	BoolArray := []bool{true, false}
	Int32Array := []int32{1, 2}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	JSONArray := [][]byte{[]byte("{\"hello\":0}"), []byte("{\"key\":1}")}
	// POINT (30.123 -10.456) and LINESTRING (30.123 -10.456, 10.789 30.123, -40.567 40.890)
	GeometryArray := [][]byte{
		{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40},
		{0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40, 0x03, 0xA6, 0xB4, 0xA6, 0xA4, 0xD2, 0xC5, 0xC0, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A},
	}
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
	Int8Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
	}

	result1 := make([]*schemapb.FieldData, 13)
	result2 := make([]*schemapb.FieldData, 13)
	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(JSONFieldName, JSONFieldID, schemapb.DataType_JSON, JSONArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(GeometryFieldName, GeometryFiledID, schemapb.DataType_Geometry, GeometryArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[0:Dim/8], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Float16VectorFieldName, Float16VectorFieldID, schemapb.DataType_Float16Vector, Float16Vector[0:2*Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BFloat16VectorFieldName, BFloat16VectorFieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector[0:2*Dim], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(SparseFloatVectorFieldName, SparseFloatVectorFieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[0], SparseFloatVector.Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int8VectorFieldName, Int8VectorFieldID, schemapb.DataType_Int8Vector, Int8Vector[0:Dim], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(JSONFieldName, JSONFieldID, schemapb.DataType_JSON, JSONArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(GeometryFieldName, GeometryFiledID, schemapb.DataType_Geometry, GeometryArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[Dim/8:2*Dim/8], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[Dim:2*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Float16VectorFieldName, Float16VectorFieldID, schemapb.DataType_Float16Vector, Float16Vector[2*Dim:4*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BFloat16VectorFieldName, BFloat16VectorFieldID, schemapb.DataType_BFloat16Vector, BFloat16Vector[2*Dim:4*Dim], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(SparseFloatVectorFieldName, SparseFloatVectorFieldID, schemapb.DataType_SparseFloatVector, SparseFloatVector.Contents[1], SparseFloatVector.Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int8VectorFieldName, Int8VectorFieldID, schemapb.DataType_Int8Vector, Int8Vector[Dim:2*Dim], Dim))

	AppendFieldData(result1, fieldDataArray1, 0)
	AppendFieldData(result1, fieldDataArray2, 0)
	DeleteFieldData(result1)
	assert.Equal(t, BoolArray[0:1], result1[BoolFieldID-common.StartOfUserFieldID].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array[0:1], result1[Int32FieldID-common.StartOfUserFieldID].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array[0:1], result1[Int64FieldID-common.StartOfUserFieldID].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray[0:1], result1[FloatFieldID-common.StartOfUserFieldID].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray[0:1], result1[DoubleFieldID-common.StartOfUserFieldID].GetScalars().GetDoubleData().Data)
	assert.Equal(t, JSONArray[0:1], result1[JSONFieldID-common.StartOfUserFieldID].GetScalars().GetJsonData().Data)
	assert.Equal(t, GeometryArray[0:1], result1[GeometryFiledID-common.StartOfUserFieldID].GetScalars().GetGeometryData().Data)
	assert.Equal(t, BinaryVector[0:Dim/8], result1[BinaryVectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector[0:Dim], result1[FloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetFloatVector().Data)
	assert.Equal(t, Float16Vector[0:2*Dim], result1[Float16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Float16Vector).Float16Vector)
	assert.Equal(t, BFloat16Vector[0:2*Dim], result1[BFloat16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Bfloat16Vector).Bfloat16Vector)
	tmpSparseFloatVector := proto.Clone(SparseFloatVector).(*schemapb.SparseFloatArray)
	tmpSparseFloatVector.Contents = [][]byte{SparseFloatVector.Contents[0]}
	assert.Equal(t, tmpSparseFloatVector.Contents, result1[SparseFloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetSparseFloatVector().Contents)
	assert.Equal(t, Int8Vector[0:Dim], result1[Int8VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Int8Vector).Int8Vector)

	AppendFieldData(result2, fieldDataArray2, 0)
	AppendFieldData(result2, fieldDataArray1, 0)
	DeleteFieldData(result2)
	assert.Equal(t, BoolArray[1:2], result2[BoolFieldID-common.StartOfUserFieldID].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array[1:2], result2[Int32FieldID-common.StartOfUserFieldID].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array[1:2], result2[Int64FieldID-common.StartOfUserFieldID].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray[1:2], result2[FloatFieldID-common.StartOfUserFieldID].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray[1:2], result2[DoubleFieldID-common.StartOfUserFieldID].GetScalars().GetDoubleData().Data)
	assert.Equal(t, JSONArray[1:2], result2[JSONFieldID-common.StartOfUserFieldID].GetScalars().GetJsonData().Data)
	assert.Equal(t, GeometryArray[1:2], result2[GeometryFiledID-common.StartOfUserFieldID].GetScalars().GetGeometryData().Data)
	assert.Equal(t, BinaryVector[Dim/8:2*Dim/8], result2[BinaryVectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector[Dim:2*Dim], result2[FloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetFloatVector().Data)
	assert.Equal(t, Float16Vector[2*Dim:4*Dim], result2[Float16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Float16Vector).Float16Vector)
	assert.Equal(t, BFloat16Vector[2*Dim:4*Dim], result2[BFloat16VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Bfloat16Vector).Bfloat16Vector)
	tmpSparseFloatVector = proto.Clone(SparseFloatVector).(*schemapb.SparseFloatArray)
	tmpSparseFloatVector.Contents = [][]byte{SparseFloatVector.Contents[1]}
	assert.EqualExportedValues(t, tmpSparseFloatVector, result2[SparseFloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetSparseFloatVector())
	assert.Equal(t, Int8Vector[Dim:2*Dim], result2[Int8VectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_Int8Vector).Int8Vector)
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
		111: [][]byte{
			// POINT (30.123 -10.456) and LINESTRING (30.123 -10.456, 10.789 30.123, -40.567 40.890)
			{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40},
			{0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40, 0x03, 0xA6, 0xB4, 0xA6, 0xA4, 0xD2, 0xC5, 0xC0, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A},
		},
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
			{
				FieldID:  111,
				Name:     "field_geometry",
				DataType: schemapb.DataType_Geometry,
			},
		},
	}

	for _, field := range schema.GetFields() {
		values := fieldValues[field.GetFieldID()]
		fieldData := genFieldData(field.GetName(), field.GetFieldID(), field.GetDataType(), values, 0)
		size := CalcScalarSize(fieldData)
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

		case schemapb.DataType_Geometry:
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
	Int8Vector := []byte{
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
		0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
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
	int8VecData := genFieldData(fieldName, fieldID, schemapb.DataType_Int8Vector, Int8Vector, Dim)
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
		boolDataRes := getData(boolData, 0)
		int8DataRes := getData(int8Data, 0)
		int16DataRes := getData(int16Data, 0)
		int32DataRes := getData(int32Data, 0)
		int64DataRes := getData(int64Data, 0)
		floatDataRes := getData(floatData, 0)
		doubleDataRes := getData(doubleData, 0)
		varCharDataRes := getData(varCharData, 0)
		binVecDataRes := getData(binVecData, 0)
		floatVecDataRes := getData(floatVecData, 0)
		float16VecDataRes := getData(float16VecData, 0)
		bfloat16VecDataRes := getData(bfloat16VecData, 0)
		sparseFloatDataRes := getData(sparseFloatData, 0)
		int8VecDataRes := getData(int8VecData, 0)
		invalidDataRes := getData(invalidData, 0)

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
		assert.ElementsMatch(t, Int8Vector[:Dim], int8VecDataRes)
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
			genFieldData("int8_vector", 113, schemapb.DataType_Int8Vector, []byte("12345678"), 4),
			genFieldData("geometry", 114, schemapb.DataType_Geometry, [][]byte{
				{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40},
				{0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40, 0x03, 0xA6, 0xB4, 0xA6, 0xA4, 0xD2, 0xC5, 0xC0, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A},
			}, 1),
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
			genFieldData("int8_vector", 113, schemapb.DataType_Int8Vector, []byte("abcdefgh"), 4),
			genFieldData("geometry", 114, schemapb.DataType_Geometry, [][]byte{
				{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40},
				{0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40, 0x03, 0xA6, 0xB4, 0xA6, 0xA4, 0xD2, 0xC5, 0xC0, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A},
			}, 1),
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
		assert.Equal(t, []byte("12345678abcdefgh"), dstFields[9].GetVectors().GetInt8Vector())
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
			genFieldData("int8_vector", 113, schemapb.DataType_Int8Vector, []byte("12345678"), 4),
			genFieldData("geometry", 114, schemapb.DataType_Geometry, [][]byte{
				{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40},
				{0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x40, 0x03, 0xA6, 0xB4, 0xA6, 0xA4, 0xD2, 0xC5, 0xC0, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A},
			}, 1),
		}

		dstFields := []*schemapb.FieldData{
			{Type: schemapb.DataType_Int64, FieldName: "int64", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{}}}, FieldId: 100},
			{Type: schemapb.DataType_FloatVector, FieldName: "vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{}}}, FieldId: 101},
			{Type: schemapb.DataType_JSON, FieldName: "json", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{}}}, FieldId: 102},
			{Type: schemapb.DataType_Array, FieldName: "array", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{}}}, FieldId: 103},
			{Type: schemapb.DataType_SparseFloatVector, FieldName: "sparseFloat", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{}}}, FieldId: 104},
			{Type: schemapb.DataType_Float16Vector, FieldName: "float16_vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{}}}, FieldId: 111},
			{Type: schemapb.DataType_BFloat16Vector, FieldName: "bfloat16_vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{}}}, FieldId: 112},
			{Type: schemapb.DataType_Int8Vector, FieldName: "int8_vector", Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{}}}, FieldId: 113},
			{Type: schemapb.DataType_Geometry, FieldName: "geometry", Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{}}}, FieldId: 114},
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
		assert.Equal(t, []byte("12345678"), dstFields[7].GetVectors().GetInt8Vector())
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

	s.Run("int8_vector", func() {
		samples := []*schemapb.FieldData{
			{
				FieldId:   fieldID,
				FieldName: fieldName,
				Type:      schemapb.DataType_Int8Vector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  128,
						Data: &schemapb.VectorField_Int8Vector{},
					},
				},
			},
		}

		fields := PrepareResultFieldData(samples, topK)
		s.Require().Len(fields, 1)
		field := fields[0]
		s.Equal(fieldID, field.GetFieldId())
		s.Equal(fieldName, field.GetFieldName())
		s.Equal(schemapb.DataType_Int8Vector, field.GetType())

		s.EqualValues(128, field.GetVectors().GetDim())
		s.EqualValues(topK*128, cap(field.GetVectors().GetInt8Vector()))
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

// test EstimateSparseVectorNNZFromPlaceholderGroup: given a PlaceholderGroup
// with various nq and averageNNZ, test if the estimated number of non-zero
// elements is close to the actual number.
func TestSparsePlaceholderGroupSize(t *testing.T) {
	nqs := []int{1, 10, 100, 1000, 10000}
	averageNNZs := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048}
	numCases := 0
	casesWithLargeError := 0
	for _, nq := range nqs {
		for _, averageNNZ := range averageNNZs {
			variants := make([]int, 0)
			for i := 1; i <= averageNNZ/2; i *= 2 {
				variants = append(variants, i)
			}

			for _, variant := range variants {
				numCases++
				contents := make([][]byte, nq)
				contentsSize := 0
				totalNNZ := 0
				for i := range contents {
					// nnz of each row is in range [averageNNZ - variant/2, averageNNZ + variant/2] and at least 1.
					nnz := averageNNZ + variant/2 + rand.Intn(variant)
					if nnz < 1 {
						nnz = 1
					}
					indices := make([]uint32, nnz)
					values := make([]float32, nnz)
					for j := 0; j < nnz; j++ {
						indices[j] = uint32(i*averageNNZ + j)
						values[j] = float32(i*averageNNZ + j)
					}
					contents[i] = CreateSparseFloatRow(indices, values)
					contentsSize += len(contents[i])
					totalNNZ += nnz
				}

				placeholderGroup := &commonpb.PlaceholderGroup{
					Placeholders: []*commonpb.PlaceholderValue{
						{
							Tag:    "$0",
							Type:   commonpb.PlaceholderType_SparseFloatVector,
							Values: contents,
						},
					},
				}
				bytes, _ := proto.Marshal(placeholderGroup)
				estimatedNNZ := EstimateSparseVectorNNZFromPlaceholderGroup(bytes, nq)
				errorRatio := (float64(totalNNZ-estimatedNNZ) / float64(totalNNZ)) * 100
				assert.Less(t, errorRatio, 10.0)
				if errorRatio > 5.0 {
					casesWithLargeError++
				}
				// keep the logs for easy debugging.
				// fmt.Printf("nq: %d, total nnz: %d, overhead bytes: %d, len of bytes: %d\n", nq, totalNNZ, len(bytes)-contentsSize, len(bytes))
				// fmt.Printf("\tnq: %d, total nnz: %d, estimated nnz: %d, diff: %d, error ratio: %f%%\n", nq, totalNNZ, estimatedNNZ, totalNNZ-estimatedNNZ, errorRatio)
			}
		}
	}
	largeErrorRatio := (float64(casesWithLargeError) / float64(numCases)) * 100
	// no more than 2% cases have large error ratio.
	assert.Less(t, largeErrorRatio, 2.0)
}

func TestGetDataIterator(t *testing.T) {
	tests := []struct {
		name  string
		field *schemapb.FieldData
		want  []any
	}{
		{
			name: "empty field",
			field: &schemapb.FieldData{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{},
						},
					},
				},
			},
			want: []any{},
		},
		{
			name: "ints",
			field: &schemapb.FieldData{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3},
							},
						},
					},
				},
			},
			want: []any{int64(1), int64(2), int64(3)},
		},
		{
			name: "ints with nulls",
			field: &schemapb.FieldData{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3},
							},
						},
					},
				},
				ValidData: []bool{true, false, true, true},
			},
			want: []any{int64(1), nil, int64(2), int64(3)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			itr := GetDataIterator(tt.field)
			for i, want := range tt.want {
				got := itr(i)
				assert.Equal(t, want, got)
			}
		})
	}
}

func TestUpdateFieldData(t *testing.T) {
	const (
		Dim                  = 8
		BoolFieldName        = "BoolField"
		Int64FieldName       = "Int64Field"
		FloatFieldName       = "FloatField"
		StringFieldName      = "StringField"
		FloatVectorFieldName = "FloatVectorField"
		BoolFieldID          = common.StartOfUserFieldID + 1
		Int64FieldID         = common.StartOfUserFieldID + 2
		FloatFieldID         = common.StartOfUserFieldID + 3
		StringFieldID        = common.StartOfUserFieldID + 4
		FloatVectorFieldID   = common.StartOfUserFieldID + 5
	)

	t.Run("update scalar fields", func(t *testing.T) {
		// Create base data
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Bool,
				FieldName: BoolFieldName,
				FieldId:   BoolFieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true, false, true, false},
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int64,
				FieldName: Int64FieldName,
				FieldId:   Int64FieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4},
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: StringFieldName,
				FieldId:   StringFieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"a", "b", "c", "d"},
							},
						},
					},
				},
			},
		}

		// Create update data (only update some fields)
		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Bool,
				FieldName: BoolFieldName,
				FieldId:   BoolFieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{false, true, false, true}, // Updated values
							},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: StringFieldName,
				FieldId:   StringFieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"x", "y", "z", "w"}, // Updated values
							},
						},
					},
				},
			},
			// Note: Int64Field is not in update, so it should remain unchanged
		}

		// Update index 1
		err := UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)

		// Check results
		// Bool field should be updated at index 1
		assert.Equal(t, true, baseData[0].GetScalars().GetBoolData().Data[1])
		// Int64 field should remain unchanged at index 1
		assert.Equal(t, int64(2), baseData[1].GetScalars().GetLongData().Data[1])
		// String field should be updated at index 1
		assert.Equal(t, "y", baseData[2].GetScalars().GetStringData().Data[1])

		// Other indices should remain unchanged
		assert.Equal(t, true, baseData[0].GetScalars().GetBoolData().Data[0])
		assert.Equal(t, int64(1), baseData[1].GetScalars().GetLongData().Data[0])
		assert.Equal(t, "a", baseData[2].GetScalars().GetStringData().Data[0])
	})

	t.Run("update vector fields", func(t *testing.T) {
		// Create base vector data
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: FloatVectorFieldName,
				FieldId:   FloatVectorFieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: Dim,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{
									1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, // vector 0
									9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, // vector 1
								},
							},
						},
					},
				},
			},
		}

		// Create update data
		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: FloatVectorFieldName,
				FieldId:   FloatVectorFieldID,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: Dim,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{
									100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, // vector 0
									900.0, 1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0, 1600.0, // vector 1
								},
							},
						},
					},
				},
			},
		}

		// Update index 1 (second vector)
		err := UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)

		// Check results
		vectorData := baseData[0].GetVectors().GetFloatVector().Data

		// First vector should remain unchanged
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(i+1), vectorData[i])
		}

		// Second vector should be updated
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(900+i*100), vectorData[Dim+i])
		}
	})

	t.Run("no update fields", func(t *testing.T) {
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: Int64FieldName,
				FieldId:   Int64FieldID,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4},
							},
						},
					},
				},
			},
		}

		// Empty update data
		updateData := []*schemapb.FieldData{}

		// Update should succeed but change nothing
		err := UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)

		// Data should remain unchanged
		assert.Equal(t, int64(2), baseData[0].GetScalars().GetLongData().Data[1])
	})

	t.Run("update with ValidData", func(t *testing.T) {
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: Int64FieldName,
				FieldId:   Int64FieldID,
				ValidData: []bool{true, true, true, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4},
							},
						},
					},
				},
			},
		}

		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: Int64FieldName,
				FieldId:   Int64FieldID,
				ValidData: []bool{false, false, true, false},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{0, 0, 30, 0},
							},
						},
					},
				},
			},
		}

		// Update index 1
		err := UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)
		err = UpdateFieldData(baseData, updateData, 2, 2)
		require.NoError(t, err)

		// Check that ValidData was updated
		assert.Equal(t, false, baseData[0].ValidData[1])
		// Check that data was updated
		assert.Equal(t, int64(0), baseData[0].GetScalars().GetLongData().Data[1])
		assert.Equal(t, int64(30), baseData[0].GetScalars().GetLongData().Data[2])
	})

	t.Run("update dynamic json field", func(t *testing.T) {
		// Create base data with dynamic JSON field
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "json_field",
				FieldId:   1,
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{
									[]byte(`{"key1": "value1", "key2": 123}`),
									[]byte(`{"key3": true, "key4": 456.789}`),
								},
							},
						},
					},
				},
			},
		}

		// Create update data
		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "json_field",
				FieldId:   1,
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{
									[]byte(`{"key2": 999, "key5": "new_value"}`),
									[]byte(`{"key4": 111.222, "key6": false}`),
								},
							},
						},
					},
				},
			},
		}

		// Test updating first row
		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.NoError(t, err)

		// Verify first row was correctly merged
		firstRow := baseData[0].GetScalars().GetJsonData().Data[0]
		var result map[string]interface{}
		err = json.Unmarshal(firstRow, &result)
		require.NoError(t, err)

		// Check merged values
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, float64(999), result["key2"]) // Updated value
		assert.Equal(t, "new_value", result["key5"])  // New value

		// Test updating second row
		err = UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)

		// Verify second row was correctly merged
		secondRow := baseData[0].GetScalars().GetJsonData().Data[1]
		err = json.Unmarshal(secondRow, &result)
		require.NoError(t, err)

		// Check merged values
		assert.Equal(t, true, result["key3"])
		assert.Equal(t, float64(111.222), result["key4"]) // Updated value
		assert.Equal(t, false, result["key6"])            // New value
	})

	t.Run("update non-dynamic json field", func(t *testing.T) {
		// Create base data with non-dynamic JSON field
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "json_field",
				FieldId:   1,
				IsDynamic: false,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{
									[]byte(`{"key1": "value1"}`),
								},
							},
						},
					},
				},
			},
		}

		// Create update data
		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "json_field",
				FieldId:   1,
				IsDynamic: false,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{
									[]byte(`{"key2": "value2"}`),
								},
							},
						},
					},
				},
			},
		}

		// Test updating
		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.NoError(t, err)

		// For non-dynamic fields, the update should completely replace the old value
		result := baseData[0].GetScalars().GetJsonData().Data[0]
		assert.Equal(t, []byte(`{"key2": "value2"}`), result)
	})

	t.Run("invalid json data", func(t *testing.T) {
		// Create base data with invalid JSON
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "json_field",
				FieldId:   1,
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{
									[]byte(`invalid json`),
								},
							},
						},
					},
				},
			},
		}

		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_JSON,
				FieldName: "json_field",
				FieldId:   1,
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: [][]byte{
									[]byte(`{"key": "value"}`),
								},
							},
						},
					},
				},
			},
		}

		// Test updating with invalid base JSON
		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal base json")

		// Create base data with valid JSON but invalid update
		baseData[0].GetScalars().GetJsonData().Data[0] = []byte(`{"key": "value"}`)
		updateData[0].GetScalars().GetJsonData().Data[0] = []byte(`invalid json`)

		// Test updating with invalid update JSON
		err = UpdateFieldData(baseData, updateData, 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal update json")
	})

	t.Run("nullable field with valid data index mapping", func(t *testing.T) {
		// Test the new logic for nullable fields where updateIdx needs to be mapped to actual data index
		// Scenario: data=[1,2,3], valid_data=[true, false, true]

		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "nullable_int_field",
				FieldId:   1,
				ValidData: []bool{true, true, true}, // All base data is valid
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{100, 200, 300},
							},
						},
					},
				},
			},
		}

		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "nullable_int_field",
				FieldId:   1,
				ValidData: []bool{true, false, true}, // Only indices 0 and 2 are valid
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{999, 0, 777},
							},
						},
					},
				},
			},
		}

		// Test updating at index 1
		err := UpdateFieldData(baseData, updateData, 0, 1)
		require.NoError(t, err)

		// Since valid_data[1] = false, no data should be updated
		assert.Equal(t, int64(0), baseData[0].GetScalars().GetLongData().Data[0])
		assert.Equal(t, false, baseData[0].ValidData[0])

		// Test updating at index 2
		err = UpdateFieldData(baseData, updateData, 1, 2)
		require.NoError(t, err)

		assert.Equal(t, int64(777), baseData[0].GetScalars().GetLongData().Data[1])
		assert.Equal(t, true, baseData[0].ValidData[1])
	})

	t.Run("nullable field with complex valid data pattern", func(t *testing.T) {
		// Test more complex pattern: data=[1,2,3,4,5], valid_data=[false, true, false, true, false]
		// This tests the index mapping logic more thoroughly

		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Float,
				FieldName: "complex_nullable_field",
				FieldId:   2,
				ValidData: []bool{true, true, true, true, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{1.1, 2.2, 3.3, 4.4, 5.5},
							},
						},
					},
				},
			},
		}

		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Float,
				FieldName: "complex_nullable_field",
				FieldId:   2,
				ValidData: []bool{false, true, false, true, false}, // Only indices 1 and 3 are valid
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: []float32{0, 999.9, 0, 888.8, 0},
							},
						},
					},
				},
			},
		}

		// Test updating at index 1
		err := UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)
		assert.Equal(t, float32(999.9), baseData[0].GetScalars().GetFloatData().Data[1])
		assert.Equal(t, true, baseData[0].ValidData[0])

		// Test updating at index 3
		err = UpdateFieldData(baseData, updateData, 3, 3)
		require.NoError(t, err)
		assert.Equal(t, float32(888.8), baseData[0].GetScalars().GetFloatData().Data[3])
		assert.Equal(t, true, baseData[0].ValidData[1])

		// Test updating at index 0
		err = UpdateFieldData(baseData, updateData, 2, 2)
		require.NoError(t, err)
		assert.Equal(t, float32(0), baseData[0].GetScalars().GetFloatData().Data[2])
		assert.Equal(t, false, baseData[0].ValidData[2])
	})
}

// TestUpdateFieldData_BoundsChecking tests the enhanced bounds checking for UpdateFieldData function
func TestUpdateFieldData_BoundsChecking(t *testing.T) {
	const (
		Dim = 8
	)

	// Helper function to create test scalar field data
	createScalarFieldData := func(fieldName string, fieldID int64, dataType schemapb.DataType, data interface{}) *schemapb.FieldData {
		fieldData := &schemapb.FieldData{
			Type:      dataType,
			FieldName: fieldName,
			FieldId:   fieldID,
		}

		switch dataType {
		case schemapb.DataType_Bool:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{Data: data.([]bool)},
					},
				},
			}
		case schemapb.DataType_Int32:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: data.([]int32)},
					},
				},
			}
		case schemapb.DataType_Int64:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: data.([]int64)},
					},
				},
			}
		case schemapb.DataType_Float:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{Data: data.([]float32)},
					},
				},
			}
		case schemapb.DataType_Double:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{Data: data.([]float64)},
					},
				},
			}
		case schemapb.DataType_VarChar:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: data.([]string)},
					},
				},
			}
		case schemapb.DataType_Array:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data:        data.([]*schemapb.ScalarField),
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			}
		case schemapb.DataType_JSON:
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{Data: data.([][]byte)},
					},
				},
			}
		}
		return fieldData
	}

	// Helper function to create test vector field data (unused in this test, but kept for consistency)
	_ = func(fieldName string, fieldID int64, dataType schemapb.DataType, data interface{}, dim int64) *schemapb.FieldData {
		fieldData := &schemapb.FieldData{
			Type:      dataType,
			FieldName: fieldName,
			FieldId:   fieldID,
		}

		switch dataType {
		case schemapb.DataType_BinaryVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_FloatVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: data.([]float32)},
					},
				},
			}
		case schemapb.DataType_Float16Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_BFloat16Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_SparseFloatVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: data.(*schemapb.SparseFloatArray),
					},
				},
			}
		case schemapb.DataType_Int8Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: data.([]byte),
					},
				},
			}
		}
		return fieldData
	}

	t.Run("scalar field bounds checking - baseIdx out of bounds", func(t *testing.T) {
		// Test case: baseIdx is out of bounds for base data
		baseData := []*schemapb.FieldData{
			createScalarFieldData("bool_field", 1, schemapb.DataType_Bool, []bool{true, false}),
		}
		updateData := []*schemapb.FieldData{
			createScalarFieldData("bool_field", 1, schemapb.DataType_Bool, []bool{false, true}),
		}

		// baseIdx = 2 is out of bounds (base data only has 2 elements: indices 0,1)
		err := UpdateFieldData(baseData, updateData, 2, 0)
		require.NoError(t, err) // Should not panic, just skip update

		// Original data should remain unchanged
		assert.Equal(t, []bool{true, false}, baseData[0].GetScalars().GetBoolData().Data)
	})

	t.Run("scalar field bounds checking - updateIdx out of bounds", func(t *testing.T) {
		// Test case: updateIdx is out of bounds for update data
		baseData := []*schemapb.FieldData{
			createScalarFieldData("int_field", 1, schemapb.DataType_Int32, []int32{1, 2, 3}),
		}
		updateData := []*schemapb.FieldData{
			createScalarFieldData("int_field", 1, schemapb.DataType_Int32, []int32{10, 20}), // Only 2 elements
		}

		// updateIdx = 2 is out of bounds (update data only has 2 elements: indices 0,1)
		err := UpdateFieldData(baseData, updateData, 1, 2)
		require.NoError(t, err) // Should not panic, just skip update

		// Original data should remain unchanged
		assert.Equal(t, []int32{1, 2, 3}, baseData[0].GetScalars().GetIntData().Data)
	})

	t.Run("scalar field bounds checking - both indices in bounds", func(t *testing.T) {
		// Test case: both indices are in bounds, update should succeed
		baseData := []*schemapb.FieldData{
			createScalarFieldData("long_field", 1, schemapb.DataType_Int64, []int64{1, 2, 3, 4}),
		}
		updateData := []*schemapb.FieldData{
			createScalarFieldData("long_field", 1, schemapb.DataType_Int64, []int64{10, 20, 30, 40}),
		}

		// Both indices are in bounds
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Data at baseIdx=2 should be updated with data from updateIdx=1
		assert.Equal(t, int64(20), baseData[0].GetScalars().GetLongData().Data[2])
		// Other data should remain unchanged
		assert.Equal(t, int64(1), baseData[0].GetScalars().GetLongData().Data[0])
		assert.Equal(t, int64(2), baseData[0].GetScalars().GetLongData().Data[1])
		assert.Equal(t, int64(4), baseData[0].GetScalars().GetLongData().Data[3])
	})

	t.Run("scalar field nil data checking - baseData is nil", func(t *testing.T) {
		// Test case: baseData is nil
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Float,
				FieldName: "float_field",
				FieldId:   1,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: nil, // baseData is nil
						},
					},
				},
			},
		}
		updateData := []*schemapb.FieldData{
			createScalarFieldData("float_field", 1, schemapb.DataType_Float, []float32{1.1, 2.2}),
		}

		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.NoError(t, err) // Should not panic, just skip update
	})

	t.Run("scalar field nil data checking - updateData is nil", func(t *testing.T) {
		// Test case: updateData is nil
		baseData := []*schemapb.FieldData{
			createScalarFieldData("double_field", 1, schemapb.DataType_Double, []float64{1.1, 2.2}),
		}
		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Double,
				FieldName: "double_field",
				FieldId:   1,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: nil, // updateData is nil
						},
					},
				},
			},
		}

		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.NoError(t, err) // Should not panic, just skip update

		// Original data should remain unchanged
		assert.Equal(t, []float64{1.1, 2.2}, baseData[0].GetScalars().GetDoubleData().Data)
	})

	t.Run("test all scalar field types bounds checking", func(t *testing.T) {
		// Test all scalar field types with bounds checking
		scalarTypes := []struct {
			name       string
			dataType   schemapb.DataType
			baseData   interface{}
			updateData interface{}
		}{
			{
				name:       "BoolData",
				dataType:   schemapb.DataType_Bool,
				baseData:   []bool{true, false, true},
				updateData: []bool{false, true, false},
			},
			{
				name:       "IntData",
				dataType:   schemapb.DataType_Int32,
				baseData:   []int32{1, 2, 3},
				updateData: []int32{10, 20, 30},
			},
			{
				name:       "LongData",
				dataType:   schemapb.DataType_Int64,
				baseData:   []int64{1, 2, 3},
				updateData: []int64{10, 20, 30},
			},
			{
				name:       "FloatData",
				dataType:   schemapb.DataType_Float,
				baseData:   []float32{1.1, 2.2, 3.3},
				updateData: []float32{10.1, 20.2, 30.3},
			},
			{
				name:       "DoubleData",
				dataType:   schemapb.DataType_Double,
				baseData:   []float64{1.1, 2.2, 3.3},
				updateData: []float64{10.1, 20.2, 30.3},
			},
			{
				name:       "StringData",
				dataType:   schemapb.DataType_VarChar,
				baseData:   []string{"a", "b", "c"},
				updateData: []string{"x", "y", "z"},
			},
		}

		for _, testCase := range scalarTypes {
			t.Run(testCase.name, func(t *testing.T) {
				baseData := []*schemapb.FieldData{
					createScalarFieldData("test_field", 1, testCase.dataType, testCase.baseData),
				}
				updateData := []*schemapb.FieldData{
					createScalarFieldData("test_field", 1, testCase.dataType, testCase.updateData),
				}

				// Test bounds checking - baseIdx out of bounds
				err := UpdateFieldData(baseData, updateData, 3, 0) // baseIdx=3 is out of bounds
				require.NoError(t, err)

				// Test bounds checking - updateIdx out of bounds
				err = UpdateFieldData(baseData, updateData, 0, 3) // updateIdx=3 is out of bounds
				require.NoError(t, err)

				// Test successful update
				err = UpdateFieldData(baseData, updateData, 1, 1)
				require.NoError(t, err)

				// Verify the update worked correctly
				switch testCase.dataType {
				case schemapb.DataType_Bool:
					assert.Equal(t, true, baseData[0].GetScalars().GetBoolData().Data[1])
				case schemapb.DataType_Int32:
					assert.Equal(t, int32(20), baseData[0].GetScalars().GetIntData().Data[1])
				case schemapb.DataType_Int64:
					assert.Equal(t, int64(20), baseData[0].GetScalars().GetLongData().Data[1])
				case schemapb.DataType_Float:
					assert.Equal(t, float32(20.2), baseData[0].GetScalars().GetFloatData().Data[1])
				case schemapb.DataType_Double:
					assert.Equal(t, float64(20.2), baseData[0].GetScalars().GetDoubleData().Data[1])
				case schemapb.DataType_VarChar:
					assert.Equal(t, "y", baseData[0].GetScalars().GetStringData().Data[1])
				}
			})
		}
	})
}

// TestUpdateFieldData_VectorBoundsChecking tests the enhanced bounds checking for vector fields in UpdateFieldData function
func TestUpdateFieldData_VectorBoundsChecking(t *testing.T) {
	const (
		Dim = 8
	)

	// Helper function to create test vector field data
	createVectorFieldData := func(fieldName string, fieldID int64, dataType schemapb.DataType, data interface{}, dim int64) *schemapb.FieldData {
		fieldData := &schemapb.FieldData{
			Type:      dataType,
			FieldName: fieldName,
			FieldId:   fieldID,
		}

		switch dataType {
		case schemapb.DataType_BinaryVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_FloatVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: data.([]float32)},
					},
				},
			}
		case schemapb.DataType_Float16Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_BFloat16Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_SparseFloatVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: data.(*schemapb.SparseFloatArray),
					},
				},
			}
		case schemapb.DataType_Int8Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: data.([]byte),
					},
				},
			}
		}
		return fieldData
	}

	t.Run("vector field bounds checking - baseIdx out of bounds", func(t *testing.T) {
		// Test case: baseIdx is out of bounds for base vector data
		baseData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}, Dim), // Only 1 vector
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0}, Dim),
		}

		// baseIdx = 1 is out of bounds (base data only has 1 vector: index 0)
		err := UpdateFieldData(baseData, updateData, 1, 0)
		require.NoError(t, err) // Should not panic, just skip update

		// Original data should remain unchanged
		originalVector := baseData[0].GetVectors().GetFloatVector().Data
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(i+1), originalVector[i])
		}
	})

	t.Run("vector field bounds checking - updateIdx out of bounds", func(t *testing.T) {
		// Test case: updateIdx is out of bounds for update vector data
		baseData := []*schemapb.FieldData{
			createVectorFieldData("binary_vector_field", 1, schemapb.DataType_BinaryVector,
				make([]byte, 2*Dim/8), Dim), // 2 vectors
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("binary_vector_field", 1, schemapb.DataType_BinaryVector,
				make([]byte, 1*Dim/8), Dim), // Only 1 vector
		}

		// updateIdx = 1 is out of bounds (update data only has 1 vector: index 0)
		err := UpdateFieldData(baseData, updateData, 0, 1)
		require.NoError(t, err) // Should not panic, just skip update
	})

	t.Run("vector field bounds checking - both indices in bounds", func(t *testing.T) {
		// Test case: both indices are in bounds, update should succeed
		baseData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, // vector 0
					9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, // vector 1
				}, Dim),
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{
					100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, // vector 0
					900.0, 1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0, 1600.0, // vector 1
				}, Dim),
		}

		// Both indices are in bounds
		err := UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)

		// Vector at baseIdx=1 should be updated with vector from updateIdx=1
		updatedVector := baseData[0].GetVectors().GetFloatVector().Data
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(900+i*100), updatedVector[Dim+i])
		}
		// Vector at baseIdx=0 should remain unchanged
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(i+1), updatedVector[i])
		}
	})

	t.Run("vector field nil data checking - baseData is nil", func(t *testing.T) {
		// Test case: baseData is nil
		baseData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "float_vector_field",
				FieldId:   1,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: Dim,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: nil, // baseData is nil
						},
					},
				},
			},
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}, Dim),
		}

		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.NoError(t, err) // Should not panic, just skip update
	})

	t.Run("vector field nil data checking - updateData is nil", func(t *testing.T) {
		// Test case: updateData is nil
		baseData := []*schemapb.FieldData{
			createVectorFieldData("binary_vector_field", 1, schemapb.DataType_BinaryVector,
				make([]byte, Dim/8), Dim),
		}
		updateData := []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_BinaryVector,
				FieldName: "binary_vector_field",
				FieldId:   1,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: Dim,
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: nil, // updateData is nil
						},
					},
				},
			},
		}

		err := UpdateFieldData(baseData, updateData, 0, 0)
		require.NoError(t, err) // Should not panic, just skip update
	})

	t.Run("test all vector field types bounds checking", func(t *testing.T) {
		// Test all vector field types with bounds checking
		vectorTypes := []struct {
			name       string
			dataType   schemapb.DataType
			baseData   interface{}
			updateData interface{}
		}{
			{
				name:       "BinaryVector",
				dataType:   schemapb.DataType_BinaryVector,
				baseData:   make([]byte, 2*Dim/8), // 2 vectors
				updateData: make([]byte, 2*Dim/8), // 2 vectors
			},
			{
				name:       "FloatVector",
				dataType:   schemapb.DataType_FloatVector,
				baseData:   []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0},                 // 2 vectors
				updateData: []float32{10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0}, // 2 vectors
			},
			{
				name:       "Float16Vector",
				dataType:   schemapb.DataType_Float16Vector,
				baseData:   make([]byte, 2*Dim*2), // 2 vectors
				updateData: make([]byte, 2*Dim*2), // 2 vectors
			},
			{
				name:       "BFloat16Vector",
				dataType:   schemapb.DataType_BFloat16Vector,
				baseData:   make([]byte, 2*Dim*2), // 2 vectors
				updateData: make([]byte, 2*Dim*2), // 2 vectors
			},
			{
				name:       "Int8Vector",
				dataType:   schemapb.DataType_Int8Vector,
				baseData:   make([]byte, 2*Dim), // 2 vectors
				updateData: make([]byte, 2*Dim), // 2 vectors
			},
		}

		for _, testCase := range vectorTypes {
			t.Run(testCase.name, func(t *testing.T) {
				baseData := []*schemapb.FieldData{
					createVectorFieldData("test_vector_field", 1, testCase.dataType, testCase.baseData, Dim),
				}
				updateData := []*schemapb.FieldData{
					createVectorFieldData("test_vector_field", 1, testCase.dataType, testCase.updateData, Dim),
				}

				// Test bounds checking - baseIdx out of bounds
				err := UpdateFieldData(baseData, updateData, 2, 0) // baseIdx=2 is out of bounds
				require.NoError(t, err)

				// Test bounds checking - updateIdx out of bounds
				err = UpdateFieldData(baseData, updateData, 0, 2) // updateIdx=2 is out of bounds
				require.NoError(t, err)

				// Test successful update
				err = UpdateFieldData(baseData, updateData, 1, 1)
				require.NoError(t, err)
			})
		}
	})

	t.Run("sparse float vector bounds checking", func(t *testing.T) {
		// Create sparse float vector data
		baseSparseData := &schemapb.SparseFloatArray{
			Dim: 10,
			Contents: [][]byte{
				CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.1, 3.3, 5.5}), // vector 0
				CreateSparseFloatRow([]uint32{2, 4, 6}, []float32{2.2, 4.4, 6.6}), // vector 1
			},
		}
		updateSparseData := &schemapb.SparseFloatArray{
			Dim: 10,
			Contents: [][]byte{
				CreateSparseFloatRow([]uint32{7, 9}, []float32{7.7, 9.9}),   // vector 0
				CreateSparseFloatRow([]uint32{8, 10}, []float32{8.8, 10.0}), // vector 1
			},
		}

		baseData := []*schemapb.FieldData{
			createVectorFieldData("sparse_vector_field", 1, schemapb.DataType_SparseFloatVector, baseSparseData, 10),
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("sparse_vector_field", 1, schemapb.DataType_SparseFloatVector, updateSparseData, 10),
		}

		// Test bounds checking - baseIdx out of bounds
		err := UpdateFieldData(baseData, updateData, 2, 0) // baseIdx=2 is out of bounds
		require.NoError(t, err)

		// Test bounds checking - updateIdx out of bounds
		err = UpdateFieldData(baseData, updateData, 0, 2) // updateIdx=2 is out of bounds
		require.NoError(t, err)

		// Test successful update
		err = UpdateFieldData(baseData, updateData, 1, 1)
		require.NoError(t, err)

		// Verify the update worked correctly
		updatedContents := baseData[0].GetVectors().GetSparseFloatVector().Contents
		assert.Equal(t, updateSparseData.Contents[1], updatedContents[1])
	})
}

// TestUpdateFieldData_IndexFix tests the index fix for vector fields in UpdateFieldData function
// This test verifies that baseIdx is used correctly for base data indexing instead of updateIdx
func TestUpdateFieldData_IndexFix(t *testing.T) {
	const (
		Dim = 8
	)

	// Helper function to create test vector field data
	createVectorFieldData := func(fieldName string, fieldID int64, dataType schemapb.DataType, data interface{}, dim int64) *schemapb.FieldData {
		fieldData := &schemapb.FieldData{
			Type:      dataType,
			FieldName: fieldName,
			FieldId:   fieldID,
		}

		switch dataType {
		case schemapb.DataType_BinaryVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_FloatVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: data.([]float32)},
					},
				},
			}
		case schemapb.DataType_Float16Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_BFloat16Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: data.([]byte),
					},
				},
			}
		case schemapb.DataType_SparseFloatVector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: data.(*schemapb.SparseFloatArray),
					},
				},
			}
		case schemapb.DataType_Int8Vector:
			fieldData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: data.([]byte),
					},
				},
			}
		}
		return fieldData
	}

	t.Run("vector field index fix - binary vector", func(t *testing.T) {
		// Test case: Verify that baseIdx is used for base data indexing, not updateIdx
		// Create base data with 3 vectors
		baseData := []*schemapb.FieldData{
			createVectorFieldData("binary_vector_field", 1, schemapb.DataType_BinaryVector,
				make([]byte, 3*Dim/8), Dim), // 3 vectors
		}
		// Create update data with 2 vectors
		updateData := []*schemapb.FieldData{
			createVectorFieldData("binary_vector_field", 1, schemapb.DataType_BinaryVector,
				make([]byte, 2*Dim/8), Dim), // 2 vectors
		}

		// Fill update data with specific pattern
		updateBytes := updateData[0].GetVectors().GetBinaryVector()
		for i := range updateBytes {
			updateBytes[i] = 0xFF // Fill with 0xFF pattern
		}

		// Update baseIdx=2 with updateIdx=1
		// This should update the 3rd vector in base data with the 2nd vector from update data
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Verify that the correct vector was updated
		baseBytes := baseData[0].GetVectors().GetBinaryVector()
		baseStartIdx := 2 * (Dim / 8)
		updateStartIdx := 1 * (Dim / 8)

		// Check that the 3rd vector in base data matches the 2nd vector from update data
		for i := 0; i < Dim/8; i++ {
			assert.Equal(t, updateBytes[updateStartIdx+i], baseBytes[baseStartIdx+i])
		}
	})

	t.Run("vector field index fix - float vector", func(t *testing.T) {
		// Test case: Verify that baseIdx is used for base data indexing, not updateIdx
		baseData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{
					1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, // vector 0
					9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, // vector 1
					17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, // vector 2
				}, Dim),
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("float_vector_field", 1, schemapb.DataType_FloatVector,
				[]float32{
					100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, // vector 0
					900.0, 1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0, 1600.0, // vector 1
				}, Dim),
		}

		// Update baseIdx=2 with updateIdx=1
		// This should update the 3rd vector in base data with the 2nd vector from update data
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Verify that the correct vector was updated
		baseVectorData := baseData[0].GetVectors().GetFloatVector().Data
		// Vector 0 should remain unchanged
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(i+1), baseVectorData[i])
		}
		// Vector 1 should remain unchanged
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(9+i), baseVectorData[Dim+i])
		}
		// Vector 2 should be updated with update vector 1
		for i := 0; i < Dim; i++ {
			assert.Equal(t, float32(900+i*100), baseVectorData[2*Dim+i])
		}
	})

	t.Run("vector field index fix - float16 vector", func(t *testing.T) {
		// Test case: Verify that baseIdx is used for base data indexing, not updateIdx
		baseData := []*schemapb.FieldData{
			createVectorFieldData("float16_vector_field", 1, schemapb.DataType_Float16Vector,
				make([]byte, 3*Dim*2), Dim), // 3 vectors
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("float16_vector_field", 1, schemapb.DataType_Float16Vector,
				make([]byte, 2*Dim*2), Dim), // 2 vectors
		}

		// Fill update data with specific pattern
		updateBytes := updateData[0].GetVectors().GetFloat16Vector()
		for i := range updateBytes {
			updateBytes[i] = 0xAA // Fill with 0xAA pattern
		}

		// Update baseIdx=2 with updateIdx=1
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Verify that the correct vector was updated
		baseBytes := baseData[0].GetVectors().GetFloat16Vector()
		baseStartIdx := 2 * (Dim * 2)
		updateStartIdx := 1 * (Dim * 2)

		// Check that the 3rd vector in base data matches the 2nd vector from update data
		for i := 0; i < Dim*2; i++ {
			assert.Equal(t, updateBytes[updateStartIdx+i], baseBytes[baseStartIdx+i])
		}
	})

	t.Run("vector field index fix - bfloat16 vector", func(t *testing.T) {
		// Test case: Verify that baseIdx is used for base data indexing, not updateIdx
		baseData := []*schemapb.FieldData{
			createVectorFieldData("bfloat16_vector_field", 1, schemapb.DataType_BFloat16Vector,
				make([]byte, 3*Dim*2), Dim), // 3 vectors
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("bfloat16_vector_field", 1, schemapb.DataType_BFloat16Vector,
				make([]byte, 2*Dim*2), Dim), // 2 vectors
		}

		// Fill update data with specific pattern
		updateBytes := updateData[0].GetVectors().GetBfloat16Vector()
		for i := range updateBytes {
			updateBytes[i] = 0xBB // Fill with 0xBB pattern
		}

		// Update baseIdx=2 with updateIdx=1
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Verify that the correct vector was updated
		baseBytes := baseData[0].GetVectors().GetBfloat16Vector()
		baseStartIdx := 2 * (Dim * 2)
		updateStartIdx := 1 * (Dim * 2)

		// Check that the 3rd vector in base data matches the 2nd vector from update data
		for i := 0; i < Dim*2; i++ {
			assert.Equal(t, updateBytes[updateStartIdx+i], baseBytes[baseStartIdx+i])
		}
	})

	t.Run("vector field index fix - int8 vector", func(t *testing.T) {
		// Test case: Verify that baseIdx is used for base data indexing, not updateIdx
		baseData := []*schemapb.FieldData{
			createVectorFieldData("int8_vector_field", 1, schemapb.DataType_Int8Vector,
				make([]byte, 3*Dim), Dim), // 3 vectors
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("int8_vector_field", 1, schemapb.DataType_Int8Vector,
				make([]byte, 2*Dim), Dim), // 2 vectors
		}

		// Fill update data with specific pattern
		updateBytes := updateData[0].GetVectors().GetInt8Vector()
		for i := range updateBytes {
			updateBytes[i] = 0xCC // Fill with 0xCC pattern
		}

		// Update baseIdx=2 with updateIdx=1
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Verify that the correct vector was updated
		baseBytes := baseData[0].GetVectors().GetInt8Vector()
		baseStartIdx := 2 * Dim
		updateStartIdx := 1 * Dim

		// Check that the 3rd vector in base data matches the 2nd vector from update data
		for i := 0; i < Dim; i++ {
			assert.Equal(t, updateBytes[updateStartIdx+i], baseBytes[baseStartIdx+i])
		}
	})

	t.Run("vector field index fix - sparse float vector", func(t *testing.T) {
		// Test case: Verify that baseIdx is used for base data indexing, not updateIdx
		baseSparseData := &schemapb.SparseFloatArray{
			Dim: 10,
			Contents: [][]byte{
				CreateSparseFloatRow([]uint32{1, 3, 5}, []float32{1.1, 3.3, 5.5}), // vector 0
				CreateSparseFloatRow([]uint32{2, 4, 6}, []float32{2.2, 4.4, 6.6}), // vector 1
				CreateSparseFloatRow([]uint32{7, 9}, []float32{7.7, 9.9}),         // vector 2
			},
		}
		updateSparseData := &schemapb.SparseFloatArray{
			Dim: 10,
			Contents: [][]byte{
				CreateSparseFloatRow([]uint32{10, 12}, []float32{10.0, 12.0}), // vector 0
				CreateSparseFloatRow([]uint32{11, 13}, []float32{11.0, 13.0}), // vector 1
			},
		}

		baseData := []*schemapb.FieldData{
			createVectorFieldData("sparse_vector_field", 1, schemapb.DataType_SparseFloatVector, baseSparseData, 10),
		}
		updateData := []*schemapb.FieldData{
			createVectorFieldData("sparse_vector_field", 1, schemapb.DataType_SparseFloatVector, updateSparseData, 10),
		}

		// Update baseIdx=2 with updateIdx=1
		// This should update the 3rd vector in base data with the 2nd vector from update data
		err := UpdateFieldData(baseData, updateData, 2, 1)
		require.NoError(t, err)

		// Verify that the correct vector was updated
		updatedContents := baseData[0].GetVectors().GetSparseFloatVector().Contents
		// Vector 0 should remain unchanged
		assert.Equal(t, baseSparseData.Contents[0], updatedContents[0])
		// Vector 1 should remain unchanged
		assert.Equal(t, baseSparseData.Contents[1], updatedContents[1])
		// Vector 2 should be updated with update vector 1
		assert.Equal(t, updateSparseData.Contents[1], updatedContents[2])
	})
}

func TestGetNeedProcessFunctions(t *testing.T) {
	{
		f, err := GetNeedProcessFunctions([]int64{}, []*schemapb.FunctionSchema{}, false, false)
		assert.Len(t, f, 0)
		assert.NoError(t, err)
	}
	{
		fs := []*schemapb.FunctionSchema{{Name: "test_func", OutputFieldIds: []int64{1}}}
		_, err := GetNeedProcessFunctions([]int64{1, 2}, fs, false, false)
		assert.ErrorContains(t, err, "Insert data has function output field")
		f, err := GetNeedProcessFunctions([]int64{1, 2}, fs, true, false)
		assert.NoError(t, err)
		assert.Len(t, f, 0)
	}
	{
		fs := []*schemapb.FunctionSchema{{Name: "test_func", OutputFieldIds: []int64{1}}}
		_, err := GetNeedProcessFunctions([]int64{1}, fs, false, false)
		assert.ErrorContains(t, err, "Insert data has function output field")
		f, err := GetNeedProcessFunctions([]int64{1}, fs, true, false)
		assert.NoError(t, err)
		assert.Len(t, f, 0)
	}
	{
		fs := []*schemapb.FunctionSchema{{Name: "test_func", OutputFieldIds: []int64{1}}, {Name: "test_func2", OutputFieldIds: []int64{2}}}
		_, err := GetNeedProcessFunctions([]int64{1}, fs, false, false)
		assert.Error(t, err)
		f, err := GetNeedProcessFunctions([]int64{1}, fs, true, false)
		assert.NoError(t, err)
		assert.Len(t, f, 1)
		assert.Equal(t, f[0].Name, "test_func2")
	}
	{
		fs := []*schemapb.FunctionSchema{{Name: "test_func", Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{1}}}
		_, err := GetNeedProcessFunctions([]int64{1}, fs, true, false)
		assert.ErrorContains(t, err, "Attempt to insert bm25 function output field")
	}
	{
		fs := []*schemapb.FunctionSchema{
			{Name: "test_func", InputFieldIds: []int64{1, 2}, OutputFieldIds: []int64{3}},
			{Name: "test_func2", InputFieldIds: []int64{1}, OutputFieldIds: []int64{2}},
		}
		_, err := GetNeedProcessFunctions([]int64{1, 2}, fs, false, true)
		assert.Error(t, err)
		f, err := GetNeedProcessFunctions([]int64{1, 2}, fs, true, true)
		assert.NoError(t, err)
		assert.Len(t, f, 1)
		assert.Equal(t, f[0].Name, "test_func")
	}
}
