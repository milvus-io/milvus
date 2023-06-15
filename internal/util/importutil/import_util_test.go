// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package importutil

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/assert"
)

// sampleSchema() return a schema contains all supported data types with an int64 primary key
func sampleSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      102,
				Name:         "FieldBool",
				IsPrimaryKey: false,
				Description:  "bool",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      103,
				Name:         "FieldInt8",
				IsPrimaryKey: false,
				Description:  "int8",
				DataType:     schemapb.DataType_Int8,
			},
			{
				FieldID:      104,
				Name:         "FieldInt16",
				IsPrimaryKey: false,
				Description:  "int16",
				DataType:     schemapb.DataType_Int16,
			},
			{
				FieldID:      105,
				Name:         "FieldInt32",
				IsPrimaryKey: false,
				Description:  "int32",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      106,
				Name:         "FieldInt64",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      107,
				Name:         "FieldFloat",
				IsPrimaryKey: false,
				Description:  "float",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      108,
				Name:         "FieldDouble",
				IsPrimaryKey: false,
				Description:  "double",
				DataType:     schemapb.DataType_Double,
			},
			{
				FieldID:      109,
				Name:         "FieldString",
				IsPrimaryKey: false,
				Description:  "string",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "128"},
				},
			},
			{
				FieldID:      110,
				Name:         "FieldBinaryVector",
				IsPrimaryKey: false,
				Description:  "binary_vector",
				DataType:     schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "16"},
				},
			},
			{
				FieldID:      111,
				Name:         "FieldFloatVector",
				IsPrimaryKey: false,
				Description:  "float_vector",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
			{
				FieldID:      112,
				Name:         "FieldJSON",
				IsPrimaryKey: false,
				Description:  "json",
				DataType:     schemapb.DataType_JSON,
			},
		},
	}
	return schema
}

// sampleContent/sampleRow is json structs to represent sampleSchema() for testing
type sampleRow struct {
	FieldBool         bool
	FieldInt8         int8
	FieldInt16        int16
	FieldInt32        int32
	FieldInt64        int64
	FieldFloat        float32
	FieldDouble       float64
	FieldString       string
	FieldJSON         string
	FieldBinaryVector []int
	FieldFloatVector  []float32
}
type sampleContent struct {
	Rows []sampleRow
}

// strKeySchema() return a schema with a varchar primary key
func strKeySchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "UID",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "uid",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "1024"},
				},
			},
			{
				FieldID:      102,
				Name:         "FieldInt32",
				IsPrimaryKey: false,
				Description:  "int_scalar",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      103,
				Name:         "FieldFloat",
				IsPrimaryKey: false,
				Description:  "float_scalar",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      104,
				Name:         "FieldString",
				IsPrimaryKey: false,
				Description:  "string_scalar",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "128"},
				},
			},
			{
				FieldID:      105,
				Name:         "FieldBool",
				IsPrimaryKey: false,
				Description:  "bool_scalar",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      106,
				Name:         "FieldFloatVector",
				IsPrimaryKey: false,
				Description:  "vectors",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}
	return schema
}

// strKeyContent/strKeyRow is json structs to represent strKeySchema() for testing
type strKeyRow struct {
	UID              string
	FieldInt32       int32
	FieldFloat       float32
	FieldString      string
	FieldBool        bool
	FieldFloatVector []float32
}
type strKeyContent struct {
	Rows []strKeyRow
}

func jsonNumber(value string) json.Number {
	return json.Number(value)
}

func createFieldsData(collectionSchema *schemapb.CollectionSchema, rowCount int) map[storage.FieldID]interface{} {
	fieldsData := make(map[storage.FieldID]interface{})

	// internal fields
	rowIDData := make([]int64, 0)
	timestampData := make([]int64, 0)
	for i := 0; i < rowCount; i++ {
		rowIDData = append(rowIDData, int64(i))
		timestampData = append(timestampData, baseTimestamp+int64(i))
	}
	fieldsData[0] = rowIDData
	fieldsData[1] = timestampData

	// user-defined fields
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		switch schema.DataType {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rowCount; i++ {
				boolData = append(boolData, (i%3 != 0))
			}
			fieldsData[schema.GetFieldID()] = boolData
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rowCount; i++ {
				floatData = append(floatData, float32(i/2))
			}
			fieldsData[schema.GetFieldID()] = floatData
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rowCount; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			fieldsData[schema.GetFieldID()] = doubleData
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rowCount; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			fieldsData[schema.GetFieldID()] = int8Data
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rowCount; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			fieldsData[schema.GetFieldID()] = int16Data
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rowCount; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			fieldsData[schema.GetFieldID()] = int32Data
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rowCount; i++ {
				int64Data = append(int64Data, int64(i))
			}
			fieldsData[schema.GetFieldID()] = int64Data
		case schemapb.DataType_BinaryVector:
			dim, _ := getFieldDimension(schema)
			binVecData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				vec := make([]byte, 0)
				for k := 0; k < dim/8; k++ {
					vec = append(vec, byte(i%256))
				}
				binVecData = append(binVecData, vec)
			}
			fieldsData[schema.GetFieldID()] = binVecData
		case schemapb.DataType_FloatVector:
			dim, _ := getFieldDimension(schema)
			floatVecData := make([][]float32, 0)
			for i := 0; i < rowCount; i++ {
				vec := make([]float32, 0)
				for k := 0; k < dim; k++ {
					vec = append(vec, float32((i+k)/5))
				}
				floatVecData = append(floatVecData, vec)
			}
			fieldsData[schema.GetFieldID()] = floatVecData
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rowCount; i++ {
				varcharData = append(varcharData, "no."+strconv.Itoa(i))
			}
			fieldsData[schema.GetFieldID()] = varcharData
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				jsonData = append(jsonData, []byte(fmt.Sprintf("{\"y\": %d}", i)))
			}
			fieldsData[schema.GetFieldID()] = jsonData

		default:
			return nil
		}
	}

	return fieldsData
}

func createBlockData(collectionSchema *schemapb.CollectionSchema, fieldsData map[storage.FieldID]interface{}) BlockData {
	blockData := initBlockData(collectionSchema)
	if fieldsData != nil {
		// internal field
		blockData[common.RowIDField].(*storage.Int64FieldData).Data = append(blockData[common.RowIDField].(*storage.Int64FieldData).Data, fieldsData[common.RowIDField].([]int64)...)

		// user custom fields
		for i := 0; i < len(collectionSchema.Fields); i++ {
			schema := collectionSchema.Fields[i]
			fieldID := schema.GetFieldID()
			switch schema.DataType {
			case schemapb.DataType_Bool:
				blockData[fieldID].(*storage.BoolFieldData).Data = append(blockData[fieldID].(*storage.BoolFieldData).Data, fieldsData[fieldID].([]bool)...)
			case schemapb.DataType_Float:
				blockData[fieldID].(*storage.FloatFieldData).Data = append(blockData[fieldID].(*storage.FloatFieldData).Data, fieldsData[fieldID].([]float32)...)
			case schemapb.DataType_Double:
				blockData[fieldID].(*storage.DoubleFieldData).Data = append(blockData[fieldID].(*storage.DoubleFieldData).Data, fieldsData[fieldID].([]float64)...)
			case schemapb.DataType_Int8:
				blockData[fieldID].(*storage.Int8FieldData).Data = append(blockData[fieldID].(*storage.Int8FieldData).Data, fieldsData[fieldID].([]int8)...)
			case schemapb.DataType_Int16:
				blockData[fieldID].(*storage.Int16FieldData).Data = append(blockData[fieldID].(*storage.Int16FieldData).Data, fieldsData[fieldID].([]int16)...)
			case schemapb.DataType_Int32:
				blockData[fieldID].(*storage.Int32FieldData).Data = append(blockData[fieldID].(*storage.Int32FieldData).Data, fieldsData[fieldID].([]int32)...)
			case schemapb.DataType_Int64:
				blockData[fieldID].(*storage.Int64FieldData).Data = append(blockData[fieldID].(*storage.Int64FieldData).Data, fieldsData[fieldID].([]int64)...)
			case schemapb.DataType_BinaryVector:
				binVectors := fieldsData[fieldID].([][]byte)
				for _, vec := range binVectors {
					blockData[fieldID].(*storage.BinaryVectorFieldData).Data = append(blockData[fieldID].(*storage.BinaryVectorFieldData).Data, vec...)
				}
			case schemapb.DataType_FloatVector:
				floatVectors := fieldsData[fieldID].([][]float32)
				for _, vec := range floatVectors {
					blockData[fieldID].(*storage.FloatVectorFieldData).Data = append(blockData[fieldID].(*storage.FloatVectorFieldData).Data, vec...)
				}
			case schemapb.DataType_String, schemapb.DataType_VarChar:
				blockData[fieldID].(*storage.StringFieldData).Data = append(blockData[fieldID].(*storage.StringFieldData).Data, fieldsData[fieldID].([]string)...)
			case schemapb.DataType_JSON:
				blockData[fieldID].(*storage.JSONFieldData).Data = append(blockData[fieldID].(*storage.JSONFieldData).Data, fieldsData[fieldID].([][]byte)...)
			default:
				return nil
			}
		}
	}
	return blockData
}

func createShardsData(collectionSchema *schemapb.CollectionSchema, fieldsData map[storage.FieldID]interface{},
	shardNum int32, partitionIDs []int64) []ShardData {
	shardsData := make([]ShardData, 0, shardNum)
	for i := 0; i < int(shardNum); i++ {
		shardData := make(ShardData)
		for p := 0; p < len(partitionIDs); p++ {
			blockData := createBlockData(collectionSchema, fieldsData)
			shardData[partitionIDs[p]] = blockData

		}
		shardsData = append(shardsData, shardData)
	}

	return shardsData
}

func Test_IsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	assert.False(t, isCanceled(ctx))
	cancel()
	assert.True(t, isCanceled(ctx))
}

func Test_InitSegmentData(t *testing.T) {
	testFunc := func(schema *schemapb.CollectionSchema) {
		fields := initBlockData(schema)
		assert.Equal(t, len(schema.Fields)+1, len(fields))

		for _, field := range schema.Fields {
			data, ok := fields[field.FieldID]
			assert.True(t, ok)
			assert.NotNil(t, data)
		}
		printFieldsDataInfo(fields, "dummy", []string{})
	}
	testFunc(sampleSchema())
	testFunc(strKeySchema())

	// unsupported data type
	schema := &schemapb.CollectionSchema{
		Name:   "schema",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "flag",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_None,
			},
		},
	}
	data := initBlockData(schema)
	assert.Nil(t, data)
}

func Test_parseFloat(t *testing.T) {
	value, err := parseFloat("dummy", 32, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("NaN", 32, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("Inf", 32, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("Infinity", 32, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("3.5e+38", 32, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("1.8e+308", 64, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("3.14159", 32, "")
	assert.True(t, math.Abs(value-3.14159) < 0.000001)
	assert.NoError(t, err)

	value, err = parseFloat("2.718281828459045", 64, "")
	assert.True(t, math.Abs(value-2.718281828459045) < 0.0000000000000001)
	assert.NoError(t, err)

	value, err = parseFloat("Inf", 32, "")
	assert.Zero(t, value)
	assert.Error(t, err)

	value, err = parseFloat("NaN", 64, "")
	assert.Zero(t, value)
	assert.Error(t, err)
}

func Test_InitValidators(t *testing.T) {
	validators := make(map[storage.FieldID]*Validator)
	err := initValidators(nil, validators)
	assert.Error(t, err)

	schema := sampleSchema()
	// success case
	err = initValidators(schema, validators)
	assert.NoError(t, err)
	assert.Equal(t, len(schema.Fields), len(validators))
	for _, field := range schema.Fields {
		fieldID := field.GetFieldID()
		assert.Equal(t, field.GetName(), validators[fieldID].fieldName)
		assert.Equal(t, field.GetIsPrimaryKey(), validators[fieldID].primaryKey)
		assert.Equal(t, field.GetAutoID(), validators[fieldID].autoID)
		if field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_String {
			assert.False(t, validators[fieldID].isString)
		} else {
			assert.True(t, validators[fieldID].isString)
		}
	}

	name2ID := make(map[string]storage.FieldID)
	for _, field := range schema.Fields {
		name2ID[field.GetName()] = field.GetFieldID()
	}

	fields := initBlockData(schema)
	assert.NotNil(t, fields)

	checkConvertFunc := func(funcName string, validVal interface{}, invalidVal interface{}) {
		id := name2ID[funcName]
		v, ok := validators[id]
		assert.True(t, ok)

		fieldData := fields[id]
		preNum := fieldData.RowNum()
		err = v.convertFunc(validVal, fieldData)
		assert.NoError(t, err)
		postNum := fieldData.RowNum()
		assert.Equal(t, 1, postNum-preNum)

		err = v.convertFunc(invalidVal, fieldData)
		assert.Error(t, err)
	}

	t.Run("check convert functions", func(t *testing.T) {
		var validVal interface{} = true
		var invalidVal interface{} = 5
		checkConvertFunc("FieldBool", validVal, invalidVal)

		validVal = jsonNumber("100")
		invalidVal = jsonNumber("128")
		checkConvertFunc("FieldInt8", validVal, invalidVal)
		invalidVal = jsonNumber("65536")
		checkConvertFunc("FieldInt16", validVal, invalidVal)
		invalidVal = jsonNumber("2147483648")
		checkConvertFunc("FieldInt32", validVal, invalidVal)
		invalidVal = jsonNumber("1.2")
		checkConvertFunc("FieldInt64", validVal, invalidVal)
		invalidVal = jsonNumber("dummy")
		checkConvertFunc("FieldFloat", validVal, invalidVal)
		checkConvertFunc("FieldDouble", validVal, invalidVal)

		invalidVal = "6"
		checkConvertFunc("FieldInt8", validVal, invalidVal)
		checkConvertFunc("FieldInt16", validVal, invalidVal)
		checkConvertFunc("FieldInt32", validVal, invalidVal)
		checkConvertFunc("FieldInt64", validVal, invalidVal)
		checkConvertFunc("FieldFloat", validVal, invalidVal)
		checkConvertFunc("FieldDouble", validVal, invalidVal)

		validVal = "aa"
		checkConvertFunc("FieldString", validVal, nil)

		validVal = map[string]interface{}{"x": 5, "y": true, "z": "hello"}
		checkConvertFunc("FieldJSON", validVal, nil)
		checkConvertFunc("FieldJSON", "{\"x\": 8}", "{")

		// the binary vector dimension is 16, shoud input two uint8 values, each value should between 0~255
		validVal = []interface{}{jsonNumber("100"), jsonNumber("101")}
		invalidVal = []interface{}{jsonNumber("100"), jsonNumber("1256")}
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)

		invalidVal = false
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)
		invalidVal = []interface{}{jsonNumber("100")}
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)
		invalidVal = []interface{}{jsonNumber("100"), 0}
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)

		// the float vector dimension is 4, each value should be valid float number
		validVal = []interface{}{jsonNumber("1"), jsonNumber("2"), jsonNumber("3"), jsonNumber("4")}
		invalidVal = []interface{}{jsonNumber("1"), jsonNumber("2"), jsonNumber("3"), jsonNumber("dummy")}
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
		invalidVal = false
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
		invalidVal = []interface{}{jsonNumber("1")}
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
		invalidVal = []interface{}{jsonNumber("1"), jsonNumber("2"), jsonNumber("3"), true}
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
	})

	t.Run("init error cases", func(t *testing.T) {
		schema = &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields:      make([]*schemapb.FieldSchema, 0),
		}
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      111,
			Name:         "FieldFloatVector",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "aa"},
			},
		})

		validators = make(map[storage.FieldID]*Validator)
		err = initValidators(schema, validators)
		assert.Error(t, err)

		schema.Fields = make([]*schemapb.FieldSchema, 0)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      110,
			Name:         "FieldBinaryVector",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_BinaryVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "aa"},
			},
		})

		err = initValidators(schema, validators)
		assert.Error(t, err)

		// unsupported data type
		schema.Fields = make([]*schemapb.FieldSchema, 0)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      110,
			Name:         "dummy",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_None,
		})

		err = initValidators(schema, validators)
		assert.Error(t, err)
	})

	t.Run("json field", func(t *testing.T) {
		schema = &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  102,
					Name:     "FieldJSON",
					DataType: schemapb.DataType_JSON,
				},
			},
		}

		validators = make(map[storage.FieldID]*Validator)
		err = initValidators(schema, validators)
		assert.NoError(t, err)

		v, ok := validators[102]
		assert.True(t, ok)

		fields := initBlockData(schema)
		assert.NotNil(t, fields)
		fieldData := fields[102]

		err = v.convertFunc("{\"x\": 1, \"y\": 5}", fieldData)
		assert.NoError(t, err)
		assert.Equal(t, 1, fieldData.RowNum())

		err = v.convertFunc("{}", fieldData)
		assert.NoError(t, err)
		assert.Equal(t, 2, fieldData.RowNum())

		err = v.convertFunc("", fieldData)
		assert.Error(t, err)
		assert.Equal(t, 2, fieldData.RowNum())
	})
}

func Test_GetFileNameAndExt(t *testing.T) {
	filePath := "aaa/bbb/ccc.txt"
	name, ext := GetFileNameAndExt(filePath)
	assert.EqualValues(t, "ccc", name)
	assert.EqualValues(t, ".txt", ext)
}

func Test_GetFieldDimension(t *testing.T) {
	schema := &schemapb.FieldSchema{
		FieldID:      111,
		Name:         "FieldFloatVector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}

	dim, err := getFieldDimension(schema)
	assert.NoError(t, err)
	assert.Equal(t, 4, dim)

	schema.TypeParams = []*commonpb.KeyValuePair{
		{Key: common.DimKey, Value: "abc"},
	}
	dim, err = getFieldDimension(schema)
	assert.Error(t, err)
	assert.Equal(t, 0, dim)

	schema.TypeParams = []*commonpb.KeyValuePair{}
	dim, err = getFieldDimension(schema)
	assert.Error(t, err)
	assert.Equal(t, 0, dim)
}

func Test_FillDynamicData(t *testing.T) {
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Name:               "schema",
		Description:        "schema",
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      106,
				Name:         "FieldID",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      113,
				Name:         "FieldDynamic",
				IsPrimaryKey: false,
				IsDynamic:    true,
				Description:  "dynamic field",
				DataType:     schemapb.DataType_JSON,
			},
		},
	}

	partitionID := int64(1)
	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		assert.Equal(t, partitionID, partID)
		return nil
	}

	rowCount := 1000
	idData := &storage.Int64FieldData{
		Data: make([]int64, 0),
	}
	for i := 0; i < rowCount; i++ {
		idData.Data = append(idData.Data, int64(i)) // this is primary key
	}

	t.Run("dynamic field is filled", func(t *testing.T) {
		blockData := BlockData{
			106: idData,
		}

		shardsData := []ShardData{
			{
				partitionID: blockData,
			},
		}

		err := fillDynamicData(blockData, schema)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(blockData))
		assert.Contains(t, blockData, int64(113))
		assert.Equal(t, rowCount, blockData[113].RowNum())
		assert.Equal(t, []byte("{}"), blockData[113].GetRow(0).([]byte))

		err = tryFlushBlocks(ctx, shardsData, schema, flushFunc, 1, 1, false)
		assert.NoError(t, err)
	})

	t.Run("collection is dynamic by no dynamic field", func(t *testing.T) {
		blockData := BlockData{
			106: idData,
		}
		schema.Fields[1].IsDynamic = false
		err := fillDynamicData(blockData, schema)
		assert.Error(t, err)

		shardsData := []ShardData{
			{
				partitionID: blockData,
			},
		}

		err = tryFlushBlocks(ctx, shardsData, schema, flushFunc, 1024*1024, 1, true)
		assert.Error(t, err)

		err = tryFlushBlocks(ctx, shardsData, schema, flushFunc, 1024, 1, false)
		assert.Error(t, err)

		err = tryFlushBlocks(ctx, shardsData, schema, flushFunc, 1024*1024, 1, false)
		assert.Error(t, err)
	})

	t.Run("collection is not dynamic", func(t *testing.T) {
		blockData := BlockData{
			106: idData,
		}
		schema.EnableDynamicField = false
		err := fillDynamicData(blockData, schema)
		assert.NoError(t, err)
	})
}

func Test_TryFlushBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	flushCounter := 0
	flushRowCount := 0
	partitionID := int64(1)
	flushFunc := func(fields BlockData, shardID int, partID int64) error {
		assert.Equal(t, partitionID, partID)
		flushCounter++
		rowCount := 0
		for _, v := range fields {
			rowCount = v.RowNum()
			break
		}
		flushRowCount += rowCount
		for _, v := range fields {
			assert.Equal(t, rowCount, v.RowNum())
		}
		return nil
	}

	blockSize := int64(1024)
	maxTotalSize := int64(4096)
	shardNum := int32(3)
	schema := sampleSchema()

	// prepare flush data, 3 shards, each shard 10 rows
	rowCount := 10
	fieldsData := createFieldsData(schema, rowCount)
	shardsData := createShardsData(schema, fieldsData, shardNum, []int64{partitionID})

	t.Run("non-force flush", func(t *testing.T) {
		err := tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, false)
		assert.NoError(t, err)
		assert.Equal(t, 0, flushCounter)
		assert.Equal(t, 0, flushRowCount)
	})

	t.Run("force flush", func(t *testing.T) {
		err := tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, true)
		assert.NoError(t, err)
		assert.Equal(t, int(shardNum), flushCounter)
		assert.Equal(t, rowCount*int(shardNum), flushRowCount)
	})

	t.Run("after force flush, no data left", func(t *testing.T) {
		flushCounter = 0
		flushRowCount = 0
		err := tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, flushCounter)
		assert.Equal(t, 0, flushRowCount)
	})

	t.Run("flush when segment size exceeds blockSize", func(t *testing.T) {
		shardsData = createShardsData(schema, fieldsData, shardNum, []int64{partitionID})
		blockSize = 100 // blockSize is 100 bytes, less than the 10 rows size
		err := tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, false)
		assert.NoError(t, err)
		assert.Equal(t, int(shardNum), flushCounter)
		assert.Equal(t, rowCount*int(shardNum), flushRowCount)

		flushCounter = 0
		flushRowCount = 0
		err = tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, true) // no data left
		assert.NoError(t, err)
		assert.Equal(t, 0, flushCounter)
		assert.Equal(t, 0, flushRowCount)
	})

	t.Run("flush when segments total size exceeds maxTotalSize", func(t *testing.T) {
		shardsData = createShardsData(schema, fieldsData, shardNum, []int64{partitionID})
		blockSize = 4096   // blockSize is 4096 bytes, larger than the 10 rows size
		maxTotalSize = 100 // maxTotalSize is 100 bytes, less than the 30 rows size
		err := tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, flushCounter) // only the max segment is flushed
		assert.Equal(t, 10, flushRowCount)

		flushCounter = 0
		flushRowCount = 0
		err = tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, true) // two segments left
		assert.NoError(t, err)
		assert.Equal(t, 2, flushCounter)
		assert.Equal(t, 20, flushRowCount)
	})

	t.Run("call flush function failed", func(t *testing.T) {
		flushErrFunc := func(fields BlockData, shardID int, partID int64) error {
			return errors.New("error")
		}
		shardsData = createShardsData(schema, fieldsData, shardNum, []int64{partitionID})
		err := tryFlushBlocks(ctx, shardsData, schema, flushErrFunc, blockSize, maxTotalSize, true) // failed to force flush
		assert.Error(t, err)
		err = tryFlushBlocks(ctx, shardsData, schema, flushErrFunc, 1, maxTotalSize, false) // failed to flush block larger than blockSize
		assert.Error(t, err)
		err = tryFlushBlocks(ctx, shardsData, schema, flushErrFunc, blockSize, maxTotalSize, false) // failed to flush biggest block
		assert.Error(t, err)
	})

	t.Run("illegal schema", func(t *testing.T) {
		illegalSchema := &schemapb.CollectionSchema{
			Name: "schema",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      106,
					Name:         "ID",
					IsPrimaryKey: true,
					AutoID:       false,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  108,
					Name:     "FieldDouble",
					DataType: schemapb.DataType_Double,
				},
			},
		}
		shardsData = createShardsData(illegalSchema, fieldsData, shardNum, []int64{partitionID})
		illegalSchema.Fields[1].DataType = schemapb.DataType_None
		err := tryFlushBlocks(ctx, shardsData, illegalSchema, flushFunc, 100, maxTotalSize, true)
		assert.Error(t, err)

		illegalSchema.Fields[1].DataType = schemapb.DataType_Double
		shardsData = createShardsData(illegalSchema, fieldsData, shardNum, []int64{partitionID})
		illegalSchema.Fields[1].DataType = schemapb.DataType_None
		err = tryFlushBlocks(ctx, shardsData, illegalSchema, flushFunc, 100, maxTotalSize, false)
		assert.Error(t, err)

		illegalSchema.Fields[1].DataType = schemapb.DataType_Double
		shardsData = createShardsData(illegalSchema, fieldsData, shardNum, []int64{partitionID})
		illegalSchema.Fields[1].DataType = schemapb.DataType_None
		err = tryFlushBlocks(ctx, shardsData, illegalSchema, flushFunc, 4096, maxTotalSize, false)
		assert.Error(t, err)
	})

	t.Run("canceled", func(t *testing.T) {
		cancel()
		flushCounter = 0
		flushRowCount = 0
		shardsData = createShardsData(schema, fieldsData, shardNum, []int64{partitionID})
		err := tryFlushBlocks(ctx, shardsData, schema, flushFunc, blockSize, maxTotalSize, true)
		assert.Error(t, err)
		assert.Equal(t, 0, flushCounter)
		assert.Equal(t, 0, flushRowCount)
	})
}

func Test_GetTypeName(t *testing.T) {
	str := getTypeName(schemapb.DataType_Bool)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_Int8)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_Int16)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_Int32)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_Int64)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_Float)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_Double)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_VarChar)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_String)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_BinaryVector)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_FloatVector)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_JSON)
	assert.NotEmpty(t, str)
	str = getTypeName(schemapb.DataType_None)
	assert.Equal(t, "InvalidType", str)
}

func Test_PkToShard(t *testing.T) {
	a := int32(99)
	shard, err := pkToShard(a, 2)
	assert.Error(t, err)
	assert.Zero(t, shard)

	s := "abcdef"
	shardNum := uint32(3)
	shard, err = pkToShard(s, shardNum)
	assert.NoError(t, err)
	hash := typeutil.HashString2Uint32(s)
	assert.Equal(t, hash%shardNum, shard)

	pk := int64(100)
	shardNum = uint32(4)
	shard, err = pkToShard(pk, shardNum)
	assert.NoError(t, err)
	hash, _ = typeutil.Hash32Int64(pk)
	assert.Equal(t, hash%shardNum, shard)

	pk = int64(99999)
	shardNum = uint32(5)
	shard, err = pkToShard(pk, shardNum)
	assert.NoError(t, err)
	hash, _ = typeutil.Hash32Int64(pk)
	assert.Equal(t, hash%shardNum, shard)
}

func Test_UpdateKVInfo(t *testing.T) {
	err := UpdateKVInfo(nil, "a", "1")
	assert.Error(t, err)

	infos := make([]*commonpb.KeyValuePair, 0)

	err = UpdateKVInfo(&infos, "a", "1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(infos))
	assert.Equal(t, "1", infos[0].Value)

	err = UpdateKVInfo(&infos, "a", "2")
	assert.NoError(t, err)
	assert.Equal(t, "2", infos[0].Value)

	err = UpdateKVInfo(&infos, "b", "5")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(infos))
	assert.Equal(t, "5", infos[1].Value)
}
