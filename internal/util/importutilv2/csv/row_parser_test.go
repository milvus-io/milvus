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

package csv

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/encoding/wkt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RowParserSuite struct {
	suite.Suite

	autoID      bool
	hasNullable bool
	hasDynamic  bool
	nullKey     string
	schema      *schemapb.CollectionSchema
}

type testCase struct {
	name             string
	content          map[string]string
	dontCheckDynamic bool
}

func (suite *RowParserSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *RowParserSuite) SetupTest() {
	// default suite params
	suite.nullKey = ""
	suite.setSchema(true, true, true)
}

func (suite *RowParserSuite) setSchema(autoID bool, hasNullable bool, hasDynamic bool) {
	suite.autoID = autoID
	suite.hasNullable = hasNullable
	suite.hasDynamic = hasDynamic
	suite.schema = suite.createAllTypesSchema()
}

func (suite *RowParserSuite) createArrayFieldSchema(id int64, name string, elementType schemapb.DataType, nullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:     id,
		Name:        name,
		DataType:    schemapb.DataType_Array,
		ElementType: elementType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxCapacityKey,
				Value: "4",
			},
			{
				Key:   common.MaxLengthKey,
				Value: "8",
			},
		},
		Nullable: nullable,
	}
}

func (suite *RowParserSuite) createAllTypesSchema() *schemapb.CollectionSchema {
	structArray := &schemapb.StructArrayFieldSchema{
		FieldID: 1000,
		Name:    "struct_array",
		Fields: []*schemapb.FieldSchema{
			suite.createArrayFieldSchema(1001, "struct_array[sub_bool]", schemapb.DataType_Bool, false),
			suite.createArrayFieldSchema(1002, "struct_array[sub_int8]", schemapb.DataType_Int8, false),
			suite.createArrayFieldSchema(1003, "struct_array[sub_int16]", schemapb.DataType_Int16, false),
			suite.createArrayFieldSchema(1004, "struct_array[sub_int32]", schemapb.DataType_Int32, false),
			suite.createArrayFieldSchema(1005, "struct_array[sub_int64]", schemapb.DataType_Int64, false),
			suite.createArrayFieldSchema(1006, "struct_array[sub_float]", schemapb.DataType_Float, false),
			suite.createArrayFieldSchema(1007, "struct_array[sub_double]", schemapb.DataType_Double, false),
			suite.createArrayFieldSchema(1008, "struct_array[sub_str]", schemapb.DataType_VarChar, false),
			{
				FieldID:     1009,
				Name:        "struct_array[sub_float_vector]",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxCapacityKey,
						Value: "4",
					},
					{
						Key:   common.DimKey,
						Value: "2",
					},
				},
			},
		},
	}

	schema := &schemapb.CollectionSchema{
		EnableDynamicField: suite.hasDynamic,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       suite.autoID,
			},

			{
				FieldID:    21,
				Name:       "float_vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:    22,
				Name:       "bin_vector",
				DataType:   schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "16"}},
			},
			{
				FieldID:  23,
				Name:     "sparse_vector",
				DataType: schemapb.DataType_SparseFloatVector,
			},
			{
				FieldID:    24,
				Name:       "f16_vector",
				DataType:   schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:    25,
				Name:       "bf16_vector",
				DataType:   schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:    26,
				Name:       "int8_vector",
				DataType:   schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:          27,
				Name:             "function_sparse_vector",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},

			suite.createArrayFieldSchema(50, "array_bool", schemapb.DataType_Bool, suite.hasNullable),
			suite.createArrayFieldSchema(51, "array_int8", schemapb.DataType_Int8, suite.hasNullable),
			suite.createArrayFieldSchema(52, "array_int16", schemapb.DataType_Int16, suite.hasNullable),
			suite.createArrayFieldSchema(53, "array_int32", schemapb.DataType_Int32, suite.hasNullable),
			suite.createArrayFieldSchema(54, "array_int64", schemapb.DataType_Int64, suite.hasNullable),
			suite.createArrayFieldSchema(55, "array_float", schemapb.DataType_Float, suite.hasNullable),
			suite.createArrayFieldSchema(56, "array_double", schemapb.DataType_Double, suite.hasNullable),
			suite.createArrayFieldSchema(57, "array_varchar", schemapb.DataType_VarChar, suite.hasNullable),

			{
				FieldID:  101,
				Name:     "bool",
				DataType: schemapb.DataType_Bool,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  102,
				Name:     "int8",
				DataType: schemapb.DataType_Int8,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  103,
				Name:     "int16",
				DataType: schemapb.DataType_Int16,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  104,
				Name:     "int32",
				DataType: schemapb.DataType_Int32,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  105,
				Name:     "int64",
				DataType: schemapb.DataType_Int64,
				Nullable: suite.hasNullable,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_LongData{
						LongData: int64(100),
					},
				},
			},
			{
				FieldID:  106,
				Name:     "float",
				DataType: schemapb.DataType_Float,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  107,
				Name:     "double",
				DataType: schemapb.DataType_Double,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  108,
				Name:     "varchar",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "8",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  109,
				Name:     "json",
				DataType: schemapb.DataType_JSON,
				Nullable: suite.hasNullable,
			},
			{
				FieldID:  110,
				Name:     "geometry",
				DataType: schemapb.DataType_Geometry,
				Nullable: suite.hasNullable,
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{structArray},
	}

	if suite.hasDynamic {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:   9999,
			Name:      "$meta",
			DataType:  schemapb.DataType_JSON,
			IsDynamic: true,
		})
	}

	return schema
}

func (suite *RowParserSuite) genAllTypesRowData(resetKey string, resetVal string, deleteKeys ...string) map[string]string {
	rawContent := make(map[string]string)
	if !suite.autoID {
		rawContent["id"] = "1"
	}
	rawContent["float_vector"] = "[0.1, 0.2]"
	rawContent["bin_vector"] = "[22, 33]"
	rawContent["f16_vector"] = "[0.2, 0.3]"
	rawContent["bf16_vector"] = "[0.3, 0.4]"
	rawContent["int8_vector"] = "[2, 5]"
	rawContent["sparse_vector"] = "{\"1\":0.5,\"10\":1.5,\"100\":2.5}"
	rawContent["array_bool"] = "[true, false]"
	rawContent["array_int8"] = "[1, 2]"
	rawContent["array_int16"] = "[1, 2]"
	rawContent["array_int32"] = "[1, 2]"
	rawContent["array_int64"] = "[1, 2]"
	rawContent["array_float"] = "[0.1, 0.2]"
	rawContent["array_double"] = "[0.2, 0.3]"
	rawContent["array_varchar"] = "[\"aaa\", \"bbb\"]"
	rawContent["bool"] = "true"
	rawContent["int8"] = "8"
	rawContent["int16"] = "16"
	rawContent["int32"] = "32"
	rawContent["int64"] = "64"
	rawContent["float"] = "3.14"
	rawContent["double"] = "6.28"
	rawContent["varchar"] = "test"
	rawContent["json"] = "{\"a\": 1}"
	rawContent["x"] = "2"
	rawContent["$meta"] = "{\"dynamic\": \"dummy\"}"

	rawContent["struct_array"] = "[{\"sub_bool\": true, \"sub_int8\": 3, \"sub_int16\": 4, \"sub_int16\": 5, \"sub_int32\": 6," +
		"\"sub_int64\": 7, \"sub_float\": 3.1415, \"sub_double\": 99.99, \"sub_float_vector\": [0.1, 0.2], \"sub_str\": \"hello1\"}, " +
		"{\"sub_bool\": false, \"sub_int8\": 13, \"sub_int16\": 14, \"sub_int16\": 15, \"sub_int32\": 16," +
		"\"sub_int64\": 17, \"sub_float\": 13.1415, \"sub_double\": 199.99, \"sub_float_vector\": [0.3, 0.4], \"sub_str\": \"hello2\"}]"
	rawContent["geometry"] = "POINT (30.123 -10.456)"
	rawContent[resetKey] = resetVal // reset a value
	for _, deleteKey := range deleteKeys {
		delete(rawContent, deleteKey) // delete a key
	}

	return rawContent
}

func convertVector[T any](t *testing.T, rawVal string) []T {
	var vec []T
	err := json.Unmarshal([]byte(rawVal), &vec)
	assert.NoError(t, err)
	return vec
}

func toBinVector(t *testing.T, vec []float32, method func(float32) []byte) []byte {
	res := make([]byte, len(vec)*2)
	for i := 0; i < len(vec); i++ {
		copy(res[i*2:], method(vec[i]))
	}
	return res
}

func compareArrays[T any](t *testing.T, rawVal string, val []T, parseFn func(s string) T) {
	var arr []interface{}
	desc := json.NewDecoder(strings.NewReader(rawVal))
	desc.UseNumber()
	err := desc.Decode(&arr)
	assert.NoError(t, err)
	values := make([]T, len(arr))
	for i, v := range arr {
		if parseFn != nil {
			value, ok := v.(json.Number)
			assert.True(t, ok)
			num := parseFn(value.String())
			values[i] = num
		} else {
			value, ok := v.(T)
			assert.True(t, ok)
			values[i] = value
		}
	}
	assert.Equal(t, len(values), len(val))
	assert.Equal(t, values, val)
}

func compareValues(t *testing.T, field *schemapb.FieldSchema, val any) {
	if field.GetDefaultValue() != nil {
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			assert.Equal(t, field.GetDefaultValue().GetBoolData(), val.(bool))
		case schemapb.DataType_Int8:
			assert.Equal(t, field.GetDefaultValue().GetIntData(), int32(val.(int8)))
		case schemapb.DataType_Int16:
			assert.Equal(t, field.GetDefaultValue().GetIntData(), int32(val.(int16)))
		case schemapb.DataType_Int32:
			assert.Equal(t, field.GetDefaultValue().GetIntData(), val.(int32))
		case schemapb.DataType_Int64:
			assert.Equal(t, field.GetDefaultValue().GetLongData(), val.(int64))
		case schemapb.DataType_Float:
			assert.Equal(t, field.GetDefaultValue().GetFloatData(), val.(float32))
		case schemapb.DataType_Double:
			assert.Equal(t, field.GetDefaultValue().GetDoubleData(), val.(float64))
		case schemapb.DataType_VarChar:
			assert.Equal(t, field.GetDefaultValue().GetStringData(), val.(string))
		case schemapb.DataType_Geometry:
			assert.Equal(t, field.GetDefaultValue().GetStringData(), val.(string))
		default:
		}
	} else if field.GetNullable() {
		assert.Nil(t, val)
	}
}

func (suite *RowParserSuite) genRowContent(schema *schemapb.CollectionSchema, content map[string]string) ([]string, []string) {
	header := make([]string, 0, len(content))
	rowContent := make([]string, 0, len(content))

	for k, v := range content {
		header = append(header, k)
		rowContent = append(rowContent, v)
	}

	return header, rowContent
}

func parseInt[T int8 | int16 | int32 | int64](t *testing.T, rawVal string, bitSize int) T {
	num, err := strconv.ParseInt(rawVal, 10, bitSize)
	assert.NoError(t, err)
	return T(num)
}

func parseFloat[T float32 | float64](t *testing.T, rawVal string, bitSize int) T {
	num, err := strconv.ParseFloat(rawVal, bitSize)
	assert.NoError(t, err)
	return T(num)
}

func (suite *RowParserSuite) runValid(c *testCase) {
	t := suite.T()
	t.Helper()
	t.Log(c.name)

	schema := suite.createAllTypesSchema()
	header, rowContent := suite.genRowContent(schema, c.content)
	parser, err := NewRowParser(schema, header, suite.nullKey)
	suite.NoError(err)

	row, err := parser.Parse(rowContent)
	suite.NoError(err)

	if suite.autoID {
		_, ok := row[1]
		suite.False(ok)
	} else {
		val, ok := row[1]
		suite.True(ok)
		suite.Equal(parseInt[int64](t, c.content["id"], 64), val)
	}

	for _, field := range schema.GetFields() {
		val, ok := row[field.GetFieldID()]
		if field.GetAutoID() || field.GetIsFunctionOutput() {
			suite.False(ok)
		}

		if !ok || field.GetIsDynamic() {
			continue
		}

		t := suite.T()
		rawVal, ok := c.content[field.GetName()]
		if !ok || rawVal == suite.nullKey {
			compareValues(t, field, val)
			continue
		}

		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			b, err := strconv.ParseBool(rawVal)
			suite.NoError(err)
			suite.Equal(b, val)
		case schemapb.DataType_Int8:
			suite.Equal(parseInt[int8](t, rawVal, 8), val)
		case schemapb.DataType_Int16:
			suite.Equal(parseInt[int16](t, rawVal, 16), val)
		case schemapb.DataType_Int32:
			suite.Equal(parseInt[int32](t, rawVal, 32), val)
		case schemapb.DataType_Int64:
			suite.Equal(parseInt[int64](t, rawVal, 64), val)
		case schemapb.DataType_Float:
			suite.Equal(parseFloat[float32](t, rawVal, 32), val)
		case schemapb.DataType_Double:
			suite.Equal(parseFloat[float64](t, rawVal, 64), val)
		case schemapb.DataType_VarChar:
			suite.Equal(rawVal, val)
		case schemapb.DataType_JSON:
			suite.Equal([]byte(rawVal), val)
		case schemapb.DataType_FloatVector:
			vec := convertVector[float32](t, rawVal)
			suite.Equal(vec, val.([]float32))
		case schemapb.DataType_BinaryVector:
			vec := convertVector[byte](t, rawVal)
			suite.Equal(vec, val.([]byte))
		case schemapb.DataType_Float16Vector:
			vec := convertVector[float32](t, rawVal)
			binVec := toBinVector(t, vec, typeutil.Float32ToFloat16Bytes)
			suite.Equal(binVec, val.([]byte))
		case schemapb.DataType_BFloat16Vector:
			vec := convertVector[float32](t, rawVal)
			binVec := toBinVector(t, vec, typeutil.Float32ToBFloat16Bytes)
			suite.Equal(binVec, val.([]byte))
		case schemapb.DataType_Int8Vector:
			vec := convertVector[int8](t, rawVal)
			suite.Equal(vec, val.([]int8))
		case schemapb.DataType_SparseFloatVector:
			var vec map[string]interface{}
			dec := json.NewDecoder(strings.NewReader(rawVal))
			dec.UseNumber()
			err := dec.Decode(&vec)
			suite.NoError(err)
			sparse, _ := typeutil.CreateSparseFloatRowFromMap(vec)
			suite.Equal(sparse, val)

		case schemapb.DataType_Array:
			sf, _ := val.(*schemapb.ScalarField)
			switch field.GetElementType() {
			case schemapb.DataType_Bool:
				compareArrays[bool](t, rawVal, sf.GetBoolData().GetData(), nil)
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
				compareArrays[int32](t, rawVal, sf.GetIntData().GetData(), func(s string) int32 {
					num, err := strconv.ParseInt(s, 10, 32)
					suite.NoError(err)
					return int32(num)
				})
			case schemapb.DataType_Int64:
				compareArrays[int64](t, rawVal, sf.GetLongData().GetData(), func(s string) int64 {
					num, err := strconv.ParseInt(s, 10, 64)
					suite.NoError(err)
					return num
				})
			case schemapb.DataType_Float:
				compareArrays[float32](t, rawVal, sf.GetFloatData().GetData(), func(s string) float32 {
					num, err := strconv.ParseFloat(s, 32)
					suite.NoError(err)
					return float32(num)
				})
			case schemapb.DataType_Double:
				compareArrays[float64](t, rawVal, sf.GetDoubleData().GetData(), func(s string) float64 {
					num, err := strconv.ParseFloat(s, 64)
					suite.NoError(err)
					return num
				})
			case schemapb.DataType_VarChar:
				compareArrays[string](t, rawVal, sf.GetStringData().GetData(), nil)
			default:
				continue
			}
		case schemapb.DataType_ArrayOfVector:
			// Handle ArrayOfVector validation
			vf, _ := val.(*schemapb.VectorField)
			if vf.GetFloatVector() != nil {
				// Parse expected vectors from raw string
				var expectedVectors [][]float32
				err := json.Unmarshal([]byte(rawVal), &expectedVectors)
				suite.NoError(err)

				// Flatten expected vectors
				var expectedFlat []float32
				for _, vec := range expectedVectors {
					expectedFlat = append(expectedFlat, vec...)
				}

				suite.Equal(expectedFlat, vf.GetFloatVector().GetData())
			}
		case schemapb.DataType_Geometry:
			geomT, err := wkt.Unmarshal(rawVal)
			suite.NoError(err)
			wkbValue, err := wkb.Marshal(geomT, wkb.NDR, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
			suite.NoError(err)
			suite.Equal(wkbValue, val)
		default:
			continue
		}
	}

	// Validate struct array sub-fields
	for _, structArray := range schema.GetStructArrayFields() {
		// Check if struct_array was provided in the test data
		if structArrayRaw, ok := c.content[structArray.GetName()]; ok {
			// Parse the struct array JSON
			var structArrayData []map[string]any
			dec := json.NewDecoder(strings.NewReader(structArrayRaw))
			dec.UseNumber()
			err := dec.Decode(&structArrayData)
			suite.NoError(err)

			// For each sub-field in the struct array
			for _, subField := range structArray.GetFields() {
				subFieldName := subField.GetName()
				originalSubFieldName := subFieldName[len(structArray.GetName())+1 : len(subFieldName)-1]
				val, ok := row[subField.GetFieldID()]
				suite.True(ok, "Sub-field %s should exist in row", subFieldName)

				// Validate based on sub-field type
				switch subField.GetDataType() {
				case schemapb.DataType_ArrayOfVector:
					vf, ok := val.(*schemapb.VectorField)
					suite.True(ok, "Sub-field %s should be a VectorField", subFieldName)

					// Extract expected vectors from struct array data
					var expectedVectors [][]any
					for _, elem := range structArrayData {
						if vec, ok := elem[originalSubFieldName].([]any); ok {
							expectedVectors = append(expectedVectors, vec)
						}
					}

					// Flatten and compare
					var expectedFlat []float32
					for _, vec := range expectedVectors {
						var vecFlat []float32
						for _, val := range vec {
							jval := val.(json.Number)
							fval, _ := jval.Float64()
							vecFlat = append(vecFlat, float32(fval))
						}
						expectedFlat = append(expectedFlat, vecFlat...)
					}
					suite.Equal(expectedFlat, vf.GetFloatVector().GetData())

				case schemapb.DataType_Array:
					sf, ok := val.(*schemapb.ScalarField)
					suite.True(ok, "Sub-field %s should be a ScalarField", subFieldName)

					// Extract expected values from struct array data
					var expectedValues []string
					for _, elem := range structArrayData {
						if v, ok := elem[originalSubFieldName].(string); ok {
							expectedValues = append(expectedValues, v)
						}
					}

					// Compare based on element type
					if subField.GetElementType() == schemapb.DataType_VarChar {
						suite.Equal(expectedValues, sf.GetStringData().GetData())
					}
				}
			}
		}
	}

	if suite.hasDynamic {
		val, ok := row[9999]
		suite.True(ok)
		if !c.dontCheckDynamic {
			var dynamic interface{}
			err = json.Unmarshal(val.([]byte), &dynamic)
			suite.NoError(err)
			dy, ok := dynamic.(map[string]any)
			suite.True(ok)
			dummy, ok := dy["dynamic"]
			suite.True(ok)
			suite.Equal("dummy", dummy)
			_, ok = dy["x"]
			suite.True(ok)
		}
	} else {
		_, ok := row[9999]
		suite.False(ok)
	}
}

func (suite *RowParserSuite) TestValid() {
	suite.setSchema(true, true, true)
	suite.runValid(&testCase{name: "A/N/D valid parse", content: suite.genAllTypesRowData("x", "2")})
	suite.runValid(&testCase{name: "A/N/D no $meta", content: suite.genAllTypesRowData("int32", "2", "x", "$meta"), dontCheckDynamic: true})
	suite.runValid(&testCase{name: "A/N/D no nullable field", content: suite.genAllTypesRowData("$meta", "{\"a\": 666}", "int32"), dontCheckDynamic: true})
	suite.runValid(&testCase{name: "A/N/D nullable field bool is nil", content: suite.genAllTypesRowData("bool", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field int8 is nil", content: suite.genAllTypesRowData("int8", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field int16 is nil", content: suite.genAllTypesRowData("int16", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field int32 is nil", content: suite.genAllTypesRowData("int32", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field int64 is nil", content: suite.genAllTypesRowData("int64", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field float is nil", content: suite.genAllTypesRowData("float", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field double is nil", content: suite.genAllTypesRowData("double", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field varchar is nil", content: suite.genAllTypesRowData("varchar", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field json is nil", content: suite.genAllTypesRowData("json", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D nullable field array_int8 is nil", content: suite.genAllTypesRowData("array_int8", suite.nullKey)})

	// Test struct array parsing
	suite.runValid(&testCase{name: "A/N/D struct array valid", content: suite.genAllTypesRowData("x", "2")})

	suite.nullKey = "ABCDEF"
	suite.runValid(&testCase{name: "A/N/D null key 1", content: suite.genAllTypesRowData("int64", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D null key 2", content: suite.genAllTypesRowData("double", suite.nullKey)})
	suite.runValid(&testCase{name: "A/N/D null key 3", content: suite.genAllTypesRowData("array_varchar", suite.nullKey)})

	suite.setSchema(false, true, true)
	suite.runValid(&testCase{name: "N/D valid parse", content: suite.genAllTypesRowData("x", "2")})
	suite.runValid(&testCase{name: "N/D no nullable field", content: suite.genAllTypesRowData("x", "2", "int32")})
	suite.runValid(&testCase{name: "N/D string JSON", content: suite.genAllTypesRowData("json", "{\"a\": 666}")})
	suite.runValid(&testCase{name: "N/D no default value field", content: suite.genAllTypesRowData("x", "2", "int64")})
	suite.runValid(&testCase{name: "N/D default value field is nil", content: suite.genAllTypesRowData("int64", suite.nullKey)})

	suite.setSchema(false, false, true)
	suite.runValid(&testCase{name: "D valid parse", content: suite.genAllTypesRowData("json", "{\"a\": 666}")})

	suite.setSchema(false, false, false)
	suite.runValid(&testCase{name: "_ valid parse", content: suite.genAllTypesRowData("x", "2")})
}

func (suite *RowParserSuite) runParseError(c *testCase) {
	t := suite.T()
	t.Helper()
	t.Log(c.name)

	schema := suite.createAllTypesSchema()
	header, rowContent := suite.genRowContent(schema, c.content)
	parser, err := NewRowParser(schema, header, suite.nullKey)
	suite.NoError(err)

	_, err = parser.Parse(rowContent)
	suite.Error(err)
}

func (suite *RowParserSuite) TestParseError() {
	suite.setSchema(true, true, false)

	// parse an empty row
	schema := suite.createAllTypesSchema()
	content := suite.genAllTypesRowData("x", "2")
	header, _ := suite.genRowContent(schema, content)
	parser, err := NewRowParser(schema, header, suite.nullKey)
	suite.NoError(err)
	_, err = parser.Parse([]string{})
	suite.Error(err)

	// auto-generated pk no need to provide
	content["id"] = "1"
	header, _ = suite.genRowContent(schema, content)
	parser, err = NewRowParser(schema, header, suite.nullKey)
	suite.Error(err)
	suite.Nil(parser)

	// field value missed
	content = suite.genAllTypesRowData("x", "2", "float_vector")
	header, _ = suite.genRowContent(schema, content)
	parser, err = NewRowParser(schema, header, suite.nullKey)
	suite.Error(err)
	suite.Nil(parser)

	// function output no need provide
	content = suite.genAllTypesRowData("x", "2")
	content["function_sparse_vector"] = "{\"1\":0.5,\"10\":1.5,\"100\":2.5}"
	header, _ = suite.genRowContent(schema, content)
	parser, err = NewRowParser(schema, header, suite.nullKey)
	suite.Error(err)
	suite.Nil(parser)

	genCases := func() []*testCase {
		return []*testCase{
			{name: "duplicate key for dynamic", content: suite.genAllTypesRowData("$meta", "{\"x\": 8}")},
			{name: "illegal JSON content for dynamic", content: suite.genAllTypesRowData("$meta", "{*&%%&$*(&}")},
			{name: "not a JSON for dynamic", content: suite.genAllTypesRowData("$meta", "][")},
			{name: "exceeds max length varchar", content: suite.genAllTypesRowData("varchar", "aaaaaaaaaa")},
			{name: "exceeds max capacity", content: suite.genAllTypesRowData("array_int8", "[1, 2, 3, 4, 5]")},
			{name: "type error bool", content: suite.genAllTypesRowData("bool", "0.2")},
			{name: "type error int8", content: suite.genAllTypesRowData("int8", "illegal")},
			{name: "type error int16", content: suite.genAllTypesRowData("int16", "illegal")},
			{name: "type error int32", content: suite.genAllTypesRowData("int32", "illegal")},
			{name: "type error int64", content: suite.genAllTypesRowData("int64", "illegal")},
			{name: "type error float", content: suite.genAllTypesRowData("float", "illegal")},
			{name: "type error double", content: suite.genAllTypesRowData("double", "illegal")},
			{name: "illegal json", content: suite.genAllTypesRowData("json", "][")},
			{name: "not utf8 varchar", content: suite.genAllTypesRowData("varchar", string([]byte{0xC0, 0xAF}))},
			{name: "type error array_int8", content: suite.genAllTypesRowData("array_int8", "illegal")},
			{name: "element parse error array_bool", content: suite.genAllTypesRowData("array_bool", "[\"0.2\"]")},
			{name: "element parse error array_int8", content: suite.genAllTypesRowData("array_int8", "[\"0.2\"]")},
			{name: "element parse error array_int16", content: suite.genAllTypesRowData("array_int16", "[\"0.2\"]")},
			{name: "element parse error array_int32", content: suite.genAllTypesRowData("array_int32", "[\"0.2\"]")},
			{name: "element parse error array_int64", content: suite.genAllTypesRowData("array_int64", "[\"0.2\"]")},
			{name: "element parse error array_float", content: suite.genAllTypesRowData("array_float", "[\"illegal\"]")},
			{name: "element parse error array_double", content: suite.genAllTypesRowData("array_double", "[\"illegal\"]")},
			{name: "exceeds max length array_varchar", content: suite.genAllTypesRowData("array_varchar", "[\"aaaaaaaaaa\"]")},
			{name: "illegal JSON content", content: suite.genAllTypesRowData("json", "{*&%%&$*(&}")},
			{name: "invalid float", content: suite.genAllTypesRowData("float", "Infinity")},
			{name: "invalid double", content: suite.genAllTypesRowData("double", "NaN")},
			{name: "element range error bin_vector", content: suite.genAllTypesRowData("bin_vector", "[256, 0]")},
			{name: "type error float_vector", content: suite.genAllTypesRowData("float_vector", "illegal")},
			{name: "type error bin_vector", content: suite.genAllTypesRowData("bin_vector", "illegal")},
			{name: "type error f16_vector", content: suite.genAllTypesRowData("f16_vector", "illegal")},
			{name: "type error bf16_vector", content: suite.genAllTypesRowData("bf16_vector", "illegal")},
			{name: "type error int8_vector", content: suite.genAllTypesRowData("int8_vector", "illegal")},
			{name: "type error sparse_vector", content: suite.genAllTypesRowData("sparse_vector", "illegal")},
			{name: "dim error float_vector", content: suite.genAllTypesRowData("float_vector", "[0.1]")},
			{name: "dim error bin_vector", content: suite.genAllTypesRowData("bin_vector", "[55]")},
			{name: "dim error f16_vector", content: suite.genAllTypesRowData("f16_vector", "[0.2]")},
			{name: "dim error bf16_vector", content: suite.genAllTypesRowData("bf16_vector", "[0.3]")},
			{name: "dim error int8_vector", content: suite.genAllTypesRowData("int8_vector", "[1]")},
			{name: "format error sparse_vector", content: suite.genAllTypesRowData("sparse_vector", "{\"indices\": 3}")},
		}
	}

	suite.setSchema(true, true, true)
	for _, c := range genCases() {
		suite.runParseError(&testCase{name: "A/D " + c.name, content: c.content})
	}
	suite.setSchema(true, false, true)
	for _, c := range genCases() {
		suite.runParseError(&testCase{name: "A/D " + c.name, content: c.content})
	}
	suite.setSchema(false, true, false)
	for _, c := range genCases() {
		// dynamic is disabled, no need to check dynamic field
		if strings.Contains(c.name, "dynamic") {
			continue
		}
		suite.runParseError(&testCase{name: "_ " + c.name, content: c.content})
	}
}

func TestCsvRowParser(t *testing.T) {
	suite.Run(t, new(RowParserSuite))
}
