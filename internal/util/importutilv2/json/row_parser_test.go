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

package json

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

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
}

type testCase struct {
	name             string
	content          any
	dontCheckDynamic bool
}

func (suite *RowParserSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *RowParserSuite) SetupTest() {
	// default suite params
	suite.autoID = true
	suite.hasNullable = true
	suite.hasDynamic = true
}

func (suite *RowParserSuite) createAllTypesSchema() *schemapb.CollectionSchema {
	structArray := &schemapb.StructArrayFieldSchema{
		FieldID: 110,
		Name:    "struct_array",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     111,
				Name:        "sub_float_vector",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "2",
					},
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
			},
			{
				FieldID:     112,
				Name:        "sub_str",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
					{
						Key:   "max_length",
						Value: "8",
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

			{
				FieldID:     50,
				Name:        "array_bool",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Bool,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     51,
				Name:        "array_int8",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int8,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     52,
				Name:        "array_int16",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int16,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     53,
				Name:        "array_int32",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     54,
				Name:        "array_int64",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     55,
				Name:        "array_float",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Float,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     56,
				Name:        "array_double",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Double,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
				},
				Nullable: suite.hasNullable,
			},
			{
				FieldID:     57,
				Name:        "array_varchar",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_capacity",
						Value: "4",
					},
					{
						Key:   "max_length",
						Value: "8",
					},
				},
				Nullable: suite.hasNullable,
			},

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

func (suite *RowParserSuite) genAllTypesRowData(resetKey string, resetVal any, deleteKeys ...string) map[string]any {
	rawContent := make(map[string]any)
	if !suite.autoID {
		rawContent["id"] = 1
	}
	rawContent["float_vector"] = []float32{0.1, 0.2}
	rawContent["bin_vector"] = []any{json.Number("44"), json.Number("88")}
	rawContent["f16_vector"] = []any{json.Number("0.2"), json.Number("0.2")}
	rawContent["bf16_vector"] = []any{json.Number("0.3"), json.Number("0.3")}
	rawContent["int8_vector"] = []any{json.Number("1"), json.Number("2")}
	rawContent["sparse_vector"] = map[string]float64{"1": 0.1, "2": 0.2}
	rawContent["array_bool"] = []any{true, false}
	rawContent["array_int8"] = []any{json.Number("1"), json.Number("2")}
	rawContent["array_int16"] = []any{json.Number("1"), json.Number("2")}
	rawContent["array_int32"] = []any{json.Number("1"), json.Number("2")}
	rawContent["array_int64"] = []any{json.Number("1"), json.Number("2")}
	rawContent["array_float"] = []any{json.Number("0.1"), json.Number("0.2")}
	rawContent["array_double"] = []any{json.Number("0.2"), json.Number("0.3")}
	rawContent["array_varchar"] = []string{"aaa", "bbb"}
	rawContent["bool"] = true
	rawContent["int8"] = 8
	rawContent["int16"] = 16
	rawContent["int32"] = 32
	rawContent["int64"] = 64
	rawContent["float"] = 3.14
	rawContent["double"] = 6.28
	rawContent["varchar"] = "test"
	rawContent["json"] = map[string]any{"a": 1}
	rawContent["x"] = 6
	rawContent["$meta"] = map[string]any{"dynamic": "dummy"}
	rawContent["struct_array"] = []any{
		// struct array element 1
		map[string]any{
			"sub_float_vector": []float32{0.1, 0.2},
			"sub_str":          "hello1",
		},
		// struct array element 2
		map[string]any{
			"sub_float_vector": []float32{0.3, 0.4},
			"sub_str":          "hello2",
		},
	}

	rawContent[resetKey] = resetVal // reset a value
	for _, deleteKey := range deleteKeys {
		delete(rawContent, deleteKey) // delete a key
	}

	var jsonContent map[string]any
	jsonBytes, err := json.Marshal(rawContent)
	assert.NoError(suite.T(), err)
	desc := json.NewDecoder(strings.NewReader(string(jsonBytes)))
	desc.UseNumber()
	err = desc.Decode(&jsonContent)
	assert.NoError(suite.T(), err)
	return jsonContent
}

func compareVectors[T any](t *testing.T, rawVal any, val any, rawConvert func(r interface{}) []T, width int) {
	rv, _ := rawVal.([]interface{})
	stdVec := make([]T, len(rv)*width)
	for i, r := range rv {
		copy(stdVec[i*width:], rawConvert(r))
	}
	assert.Equal(t, stdVec, val.([]T))
}

func compareArrays[T any](t *testing.T, rawVal any, val any, rawConvert func(r interface{}) T) {
	rv, _ := rawVal.([]interface{})
	arr := make([]T, 0, len(rv))
	for _, r := range rv {
		arr = append(arr, rawConvert(r))
	}
	assert.Equal(t, arr, val.([]T))
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
		default:
		}
	} else if field.GetNullable() {
		assert.Nil(t, val)
	}
}

func (suite *RowParserSuite) runValid(c *testCase) {
	t := suite.T()
	t.Helper()
	t.Log(c.name)

	schema := suite.createAllTypesSchema()
	parser, err := NewRowParser(schema)
	suite.NoError(err)

	row, err := parser.Parse(c.content)
	suite.NoError(err)

	if raw, ok := c.content.(map[string]any); ok {
		if suite.autoID {
			_, ok := row[1]
			suite.False(ok)
		} else {
			val, ok := row[1]
			suite.True(ok)
			id, _ := raw["id"].(json.Number).Int64()
			suite.Equal(id, val)
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
			rawVal, ok := raw[field.GetName()]
			if !ok || rawVal == nil {
				compareValues(t, field, val)
				continue
			}

			switch field.GetDataType() {
			case schemapb.DataType_Bool:
				suite.Equal(rawVal, val)
			case schemapb.DataType_Int8:
				v, _ := rawVal.(json.Number).Int64()
				suite.Equal(int8(v), val)
			case schemapb.DataType_Int16:
				v, _ := rawVal.(json.Number).Int64()
				suite.Equal(int16(v), val)
			case schemapb.DataType_Int32:
				v, _ := rawVal.(json.Number).Int64()
				suite.Equal(int32(v), val)
			case schemapb.DataType_Int64:
				v, _ := rawVal.(json.Number).Int64()
				suite.Equal(v, val)
			case schemapb.DataType_Float:
				v, _ := rawVal.(json.Number).Float64()
				suite.Equal(float32(v), val)
			case schemapb.DataType_Double:
				v, _ := rawVal.(json.Number).Float64()
				suite.Equal(v, val)
			case schemapb.DataType_VarChar:
				suite.Equal(rawVal, val)
			case schemapb.DataType_JSON:
				if field.GetName() != "$meta" {
					jsonBytes, _ := json.Marshal(rawVal)
					newStr := string(jsonBytes)
					newStr = strings.ReplaceAll(newStr, `"{`, `{`)
					newStr = strings.ReplaceAll(newStr, `}"`, `}`)
					newStr = strings.ReplaceAll(newStr, `\"`, `"`)
					suite.Equal([]byte(newStr), val)
				}
			case schemapb.DataType_FloatVector:
				compareVectors[float32](t, rawVal, val, func(r interface{}) []float32 {
					v, _ := r.(json.Number).Float64()
					return []float32{float32(v)}
				}, 1)
			case schemapb.DataType_BinaryVector:
				compareVectors[byte](t, rawVal, val, func(r interface{}) []byte {
					v, _ := r.(json.Number).Int64()
					return []byte{byte(uint64(v))}
				}, 1)
			case schemapb.DataType_Float16Vector:
				compareVectors[byte](t, rawVal, val, func(r interface{}) []byte {
					v, _ := r.(json.Number).Float64()
					return typeutil.Float32ToFloat16Bytes(float32(v))
				}, 2)
			case schemapb.DataType_BFloat16Vector:
				compareVectors[byte](t, rawVal, val, func(r interface{}) []byte {
					v, _ := r.(json.Number).Float64()
					return typeutil.Float32ToBFloat16Bytes(float32(v))
				}, 2)
			case schemapb.DataType_SparseFloatVector:
				sparse, _ := typeutil.CreateSparseFloatRowFromMap(rawVal.(map[string]interface{}))
				suite.Equal(sparse, val)

			case schemapb.DataType_Array:
				sf, _ := val.(*schemapb.ScalarField)
				switch field.GetElementType() {
				case schemapb.DataType_Bool:
					compareArrays[bool](t, rawVal, sf.GetBoolData().GetData(), func(r interface{}) bool {
						return r.(bool)
					})
				case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
					compareArrays[int32](t, rawVal, sf.GetIntData().GetData(), func(r interface{}) int32 {
						v, _ := r.(json.Number).Int64()
						return int32(v)
					})
				case schemapb.DataType_Int64:
					compareArrays[int64](t, rawVal, sf.GetLongData().GetData(), func(r interface{}) int64 {
						v, _ := r.(json.Number).Int64()
						return v
					})
				case schemapb.DataType_Float:
					compareArrays[float32](t, rawVal, sf.GetFloatData().GetData(), func(r interface{}) float32 {
						v, _ := r.(json.Number).Float64()
						return float32(v)
					})
				case schemapb.DataType_Double:
					compareArrays[float64](t, rawVal, sf.GetDoubleData().GetData(), func(r interface{}) float64 {
						v, _ := r.(json.Number).Float64()
						return v
					})
				case schemapb.DataType_VarChar:
					compareArrays[string](t, rawVal, sf.GetStringData().GetData(), func(r interface{}) string {
						v, _ := r.(string)
						return v
					})
				default:
					continue
				}
			default:
				continue
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

func (suite *RowParserSuite) setSchema(autoID bool, hasNullable bool, hasDynamic bool) {
	suite.autoID = autoID
	suite.hasNullable = hasNullable
	suite.hasDynamic = hasDynamic
}

func (suite *RowParserSuite) TestValid() {
	suite.setSchema(true, true, true)
	suite.runValid(&testCase{name: "A/N/D valid parse", content: suite.genAllTypesRowData("x", 2)})
	suite.runValid(&testCase{name: "A/N/D no $meta", content: suite.genAllTypesRowData("int32", 2, "x", "$meta"), dontCheckDynamic: true})
	suite.runValid(&testCase{name: "A/N/D no nullable field", content: suite.genAllTypesRowData("$meta", "{\"a\": 666}", "int32"), dontCheckDynamic: true})
	suite.runValid(&testCase{name: "A/N/D nullable field bool is nil", content: suite.genAllTypesRowData("bool", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field int8 is nil", content: suite.genAllTypesRowData("int8", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field int16 is nil", content: suite.genAllTypesRowData("int16", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field int32 is nil", content: suite.genAllTypesRowData("int32", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field int64 is nil", content: suite.genAllTypesRowData("int64", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field float is nil", content: suite.genAllTypesRowData("float", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field double is nil", content: suite.genAllTypesRowData("double", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field varchar is nil", content: suite.genAllTypesRowData("varchar", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field json is nil", content: suite.genAllTypesRowData("json", nil)})
	suite.runValid(&testCase{name: "A/N/D nullable field array_int8 is nil", content: suite.genAllTypesRowData("array_int8", nil)})

	suite.setSchema(false, true, true)
	suite.runValid(&testCase{name: "N/D valid parse", content: suite.genAllTypesRowData("x", 2)})
	suite.runValid(&testCase{name: "N/D no nullable field", content: suite.genAllTypesRowData("x", 2, "int32")})
	suite.runValid(&testCase{name: "N/D string JSON", content: suite.genAllTypesRowData("json", "{\"a\": 666}")})
	suite.runValid(&testCase{name: "N/D no default value field", content: suite.genAllTypesRowData("x", 2, "int64")})
	suite.runValid(&testCase{name: "N/D default value field is nil", content: suite.genAllTypesRowData("int64", nil)})

	suite.setSchema(false, false, true)
	suite.runValid(&testCase{name: "D valid parse", content: suite.genAllTypesRowData("json", "{\"a\": 666}")})

	suite.setSchema(false, false, false)
	suite.runValid(&testCase{name: "_ valid parse 1", content: suite.genAllTypesRowData("x", 2)})
	suite.runValid(&testCase{name: "_ valid parse 2", content: suite.genAllTypesRowData("x", 2, "function_sparse_vector")})
}

func (suite *RowParserSuite) runParseError(c *testCase) {
	t := suite.T()
	t.Helper()
	t.Log(c.name)

	schema := suite.createAllTypesSchema()
	parser, err := NewRowParser(schema)
	suite.NoError(err)

	_, err = parser.Parse(c.content)
	suite.Error(err)
}

func (suite *RowParserSuite) TestParseError() {
	suite.setSchema(true, true, false)
	suite.runParseError(&testCase{name: "not a key-value map", content: "illegal"})
	suite.runParseError(&testCase{name: "auto-generated pk no need to provide", content: suite.genAllTypesRowData("id", 2)})

	genCases := func() []*testCase {
		return []*testCase{
			{name: "duplicate key for dynamic 1", content: suite.genAllTypesRowData("$meta", map[string]any{"x": 8})},
			{name: "duplicate key  or dynamic 2", content: suite.genAllTypesRowData("$meta", "{\"x\": 8}")},
			{name: "illegal JSON content for dynamic", content: suite.genAllTypesRowData("$meta", "{*&%%&$*(&}")},
			{name: "not a JSON for dynamic", content: suite.genAllTypesRowData("$meta", []int{})},
			{name: "exceeds max length varchar", content: suite.genAllTypesRowData("varchar", "aaaaaaaaaa")},
			{name: "exceeds max capacity", content: suite.genAllTypesRowData("array_int8", []int{1, 2, 3, 4, 5})},
			{name: "field value missed", content: suite.genAllTypesRowData("x", 2, "float_vector")},
			{name: "type error bool", content: suite.genAllTypesRowData("bool", 0.2)},
			{name: "type error int8", content: suite.genAllTypesRowData("int8", []int32{})},
			{name: "type error int16", content: suite.genAllTypesRowData("int16", []int32{})},
			{name: "type error int32", content: suite.genAllTypesRowData("int32", []int32{})},
			{name: "type error int64", content: suite.genAllTypesRowData("int64", []int32{})},
			{name: "type error float", content: suite.genAllTypesRowData("float", []int32{})},
			{name: "type error double", content: suite.genAllTypesRowData("double", []int32{})},
			{name: "type error varchar", content: suite.genAllTypesRowData("varchar", []int32{})},
			{name: "type error json", content: suite.genAllTypesRowData("json", []int32{})},
			{name: "type error array_int8", content: suite.genAllTypesRowData("array_int8", "illegal")},
			{name: "element type error array_bool", content: suite.genAllTypesRowData("array_bool", []string{"illegal"})},
			{name: "element type error array_int8", content: suite.genAllTypesRowData("array_int8", []string{"illegal"})},
			{name: "element type error array_int16", content: suite.genAllTypesRowData("array_int16", []string{"illegal"})},
			{name: "element type error array_int32", content: suite.genAllTypesRowData("array_int32", []string{"illegal"})},
			{name: "element type error array_int64", content: suite.genAllTypesRowData("array_int64", []string{"illegal"})},
			{name: "element type error array_float", content: suite.genAllTypesRowData("array_float", []string{"illegal"})},
			{name: "element type error array_double", content: suite.genAllTypesRowData("array_double", []string{"illegal"})},
			{name: "element type error array_varchar", content: suite.genAllTypesRowData("array_varchar", []bool{false})},
			{name: "exceeds max length array_varchar", content: suite.genAllTypesRowData("array_varchar", []string{"aaaaaaaaaa"})},
			{name: "element parse error bool", content: suite.genAllTypesRowData("array_int8", []any{json.Number("0.2")})},
			{name: "element parse error bool", content: suite.genAllTypesRowData("array_int16", []any{json.Number("0.2")})},
			{name: "element parse error bool", content: suite.genAllTypesRowData("array_int32", []any{json.Number("0.2")})},
			{name: "element parse error bool", content: suite.genAllTypesRowData("array_int64", []any{json.Number("0.2")})},
			{name: "illegal JSON content", content: suite.genAllTypesRowData("json", "{*&%%&$*(&}")},
			{name: "parse error int8", content: suite.genAllTypesRowData("int8", 0.2)},
			{name: "parse error int16", content: suite.genAllTypesRowData("int16", 0.2)},
			{name: "parse error int32", content: suite.genAllTypesRowData("int32", 0.2)},
			{name: "parse error int64", content: suite.genAllTypesRowData("int64", 0.2)},
			{name: "invalid float", content: suite.genAllTypesRowData("float", "Infinity")},
			{name: "invalid double", content: suite.genAllTypesRowData("double", "NaN")},
			{name: "type error float_vector", content: suite.genAllTypesRowData("float_vector", "illegal")},
			{name: "element parse error float_vector", content: suite.genAllTypesRowData("float_vector", []any{false, true})},
			{name: "type error bin_vector", content: suite.genAllTypesRowData("bin_vector", "illegal")},
			{name: "element parse error bin_vector", content: suite.genAllTypesRowData("bin_vector", []any{true, false})},
			{name: "element range error bin_vector", content: suite.genAllTypesRowData("bin_vector", []any{json.Number("256"), json.Number("0")})},
			{name: "type error f16_vector", content: suite.genAllTypesRowData("f16_vector", "illegal")},
			{name: "element parse error f16_vector", content: suite.genAllTypesRowData("f16_vector", []any{true, false})},
			{name: "type error bf16_vector", content: suite.genAllTypesRowData("bf16_vector", "illegal")},
			{name: "element parse error bf16_vector", content: suite.genAllTypesRowData("bf16_vector", []any{true, false})},
			{name: "type error int8_vector", content: suite.genAllTypesRowData("int8_vector", "illegal")},
			{name: "element parse error int8_vector", content: suite.genAllTypesRowData("int8_vector", []any{true, false})},
			{name: "type error sparse_vector", content: suite.genAllTypesRowData("sparse_vector", "illegal")},
			{name: "dim error float_vector", content: suite.genAllTypesRowData("float_vector", []float32{0.1})},
			{name: "dim error bin_vector", content: suite.genAllTypesRowData("bin_vector", []any{json.Number("44")})},
			{name: "dim error f16_vector", content: suite.genAllTypesRowData("f16_vector", []any{json.Number("0.2")})},
			{name: "dim error bf16_vector", content: suite.genAllTypesRowData("bf16_vector", []any{json.Number("0.3")})},
			{name: "dim error int8_vector", content: suite.genAllTypesRowData("int8_vector", []any{json.Number("1")})},
			{name: "format error sparse_vector", content: suite.genAllTypesRowData("sparse_vector", map[string]any{"indices": []int64{}})},
			{name: "function output no need provide", content: suite.genAllTypesRowData("function_sparse_vector", map[string]float64{"1": 0.1, "2": 0.2})},
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

func TestJsonRowParser(t *testing.T) {
	suite.Run(t, new(RowParserSuite))
}
