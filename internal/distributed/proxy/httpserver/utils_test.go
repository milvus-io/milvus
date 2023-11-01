package httpserver

import (
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

const (
	FieldWordCount = "word_count"
	FieldBookID    = "book_id"
	FieldBookIntro = "book_intro"
)

func generatePrimaryField(datatype schemapb.DataType) schemapb.FieldSchema {
	return schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         FieldBookID,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     datatype,
		AutoID:       false,
	}
}

func generateIds(dataType schemapb.DataType, num int) *schemapb.IDs {
	var intArray []int64
	if num == 0 {
		intArray = []int64{}
	} else {
		for i := int64(1); i < int64(num+1); i++ {
			intArray = append(intArray, i)
		}
	}
	switch dataType {
	case schemapb.DataType_Int64:
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: intArray,
				},
			},
		}
	case schemapb.DataType_VarChar:
		stringArray := formatInt64(intArray)
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: stringArray,
				},
			},
		}
	}
	return nil
}

func generateVectorFieldSchema(useBinary bool) schemapb.FieldSchema {
	if useBinary {
		return schemapb.FieldSchema{
			FieldID:      common.StartOfUserFieldID + 2,
			Name:         "field-binary",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     100,
			AutoID:       false,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: "8",
				},
			},
		}
	}
	return schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID + 2,
		Name:         FieldBookIntro,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     101,
		AutoID:       false,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "2",
			},
		},
	}
}

func generateCollectionSchema(datatype schemapb.DataType, useBinary bool) *schemapb.CollectionSchema {
	primaryField := generatePrimaryField(datatype)
	vectorField := generateVectorFieldSchema(useBinary)
	return &schemapb.CollectionSchema{
		Name:        DefaultCollectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			&primaryField, {
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         FieldWordCount,
				IsPrimaryKey: false,
				Description:  "",
				DataType:     5,
				AutoID:       false,
			}, &vectorField,
		},
		EnableDynamicField: true,
	}
}

func generateIndexes() []*milvuspb.IndexDescription {
	return []*milvuspb.IndexDescription{
		{
			IndexName: DefaultIndexName,
			IndexID:   442051985533243300,
			Params: []*commonpb.KeyValuePair{
				{
					Key:   common.MetricTypeKey,
					Value: DefaultMetricType,
				},
				{
					Key:   "index_type",
					Value: "IVF_FLAT",
				},
				{
					Key:   Params,
					Value: "{\"nlist\":1024}",
				},
			},
			State:     3,
			FieldName: FieldBookIntro,
		},
	}
}

func generateVectorFieldData(useBinary bool) schemapb.FieldData {
	if useBinary {
		return schemapb.FieldData{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: "field-binary",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 8,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: []byte{byte(0), byte(1), byte(2)},
					},
				},
			},
			IsDynamic: false,
		}
	}
	return schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: FieldBookIntro,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: []float32{0.1, 0.11, 0.2, 0.22, 0.3, 0.33},
					},
				},
			},
		},
		IsDynamic: false,
	}
}

func generateFieldData() []*schemapb.FieldData {
	fieldData1 := schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: FieldBookID,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{1, 2, 3},
					},
				},
			},
		},
		IsDynamic: false,
	}

	fieldData2 := schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: FieldWordCount,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{1000, 2000, 3000},
					},
				},
			},
		},
		IsDynamic: false,
	}

	fieldData3 := generateVectorFieldData(false)
	return []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
}

func generateSearchResult(dataType schemapb.DataType) []map[string]interface{} {
	row1 := map[string]interface{}{
		DefaultPrimaryFieldName: int64(1),
		FieldBookID:             int64(1),
		FieldWordCount:          int64(1000),
		FieldBookIntro:          []float32{0.1, 0.11},
		HTTPReturnDistance:      float32(0.01),
	}
	row2 := map[string]interface{}{
		DefaultPrimaryFieldName: int64(2),
		FieldBookID:             int64(2),
		FieldWordCount:          int64(2000),
		FieldBookIntro:          []float32{0.2, 0.22},
		HTTPReturnDistance:      float32(0.04),
	}
	row3 := map[string]interface{}{
		DefaultPrimaryFieldName: int64(3),
		FieldBookID:             int64(3),
		FieldWordCount:          int64(3000),
		FieldBookIntro:          []float32{0.3, 0.33},
		HTTPReturnDistance:      float32(0.09),
	}
	if dataType == schemapb.DataType_String {
		row1[DefaultPrimaryFieldName] = "1"
		row2[DefaultPrimaryFieldName] = "2"
		row3[DefaultPrimaryFieldName] = "3"
	}
	return []map[string]interface{}{row1, row2, row3}
}

func generateQueryResult64(withDistance bool) []map[string]interface{} {
	row1 := map[string]interface{}{
		FieldBookID:    float64(1),
		FieldWordCount: float64(1000),
		FieldBookIntro: []float64{0.1, 0.11},
	}
	row2 := map[string]interface{}{
		FieldBookID:    float64(2),
		FieldWordCount: float64(2000),
		FieldBookIntro: []float64{0.2, 0.22},
	}
	row3 := map[string]interface{}{
		FieldBookID:    float64(3),
		FieldWordCount: float64(3000),
		FieldBookIntro: []float64{0.3, 0.33},
	}
	if withDistance {
		row1[HTTPReturnDistance] = float64(0.01)
		row2[HTTPReturnDistance] = float64(0.04)
		row3[HTTPReturnDistance] = float64(0.09)
	}
	return []map[string]interface{}{row1, row2, row3}
}

func TestPrintCollectionDetails(t *testing.T) {
	coll := generateCollectionSchema(schemapb.DataType_Int64, false)
	indexes := generateIndexes()
	assert.Equal(t, []gin.H{
		{
			HTTPReturnFieldName:       FieldBookID,
			HTTPReturnFieldType:       "Int64",
			HTTPReturnFieldPrimaryKey: true,
			HTTPReturnFieldAutoID:     false,
			HTTPReturnDescription:     "",
		},
		{
			HTTPReturnFieldName:       FieldWordCount,
			HTTPReturnFieldType:       "Int64",
			HTTPReturnFieldPrimaryKey: false,
			HTTPReturnFieldAutoID:     false,
			HTTPReturnDescription:     "",
		},
		{
			HTTPReturnFieldName:       FieldBookIntro,
			HTTPReturnFieldType:       "FloatVector(2)",
			HTTPReturnFieldPrimaryKey: false,
			HTTPReturnFieldAutoID:     false,
			HTTPReturnDescription:     "",
		},
	}, printFields(coll.Fields))
	assert.Equal(t, []gin.H{
		{
			HTTPReturnIndexName:        DefaultIndexName,
			HTTPReturnIndexField:       FieldBookIntro,
			HTTPReturnIndexMetricsType: DefaultMetricType,
		},
	}, printIndexes(indexes))
	assert.Equal(t, DefaultMetricType, getMetricType(indexes[0].Params))
	assert.Equal(t, DefaultMetricType, getMetricType(nil))
	fields := []*schemapb.FieldSchema{}
	for _, field := range newCollectionSchema(coll).Fields {
		if field.DataType == schemapb.DataType_VarChar {
			fields = append(fields, field)
		}
	}
	assert.Equal(t, []gin.H{
		{
			HTTPReturnFieldName:       "field-varchar",
			HTTPReturnFieldType:       "VarChar(10)",
			HTTPReturnFieldPrimaryKey: false,
			HTTPReturnFieldAutoID:     false,
			HTTPReturnDescription:     "",
		},
	}, printFields(fields))
}

func TestPrimaryField(t *testing.T) {
	coll := generateCollectionSchema(schemapb.DataType_Int64, false)
	primaryField := generatePrimaryField(schemapb.DataType_Int64)
	field, ok := getPrimaryField(coll)
	assert.Equal(t, true, ok)
	assert.Equal(t, primaryField, *field)

	assert.Equal(t, "1,2,3", joinArray([]int64{1, 2, 3}))
	assert.Equal(t, "1,2,3", joinArray([]string{"1", "2", "3"}))

	jsonStr := "{\"id\": [1, 2, 3]}"
	idStr := gjson.Get(jsonStr, "id")
	rangeStr, err := convertRange(&primaryField, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1,2,3", rangeStr)
	filter, err := checkGetPrimaryKey(coll, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "book_id in [1,2,3]", filter)

	primaryField = generatePrimaryField(schemapb.DataType_VarChar)
	jsonStr = "{\"id\": [\"1\", \"2\", \"3\"]}"
	idStr = gjson.Get(jsonStr, "id")
	rangeStr, err = convertRange(&primaryField, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1,2,3", rangeStr)
	filter, err = checkGetPrimaryKey(coll, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "book_id in [1,2,3]", filter)
}

func TestInsertWithDynamicFields(t *testing.T) {
	body := "{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}"
	req := InsertReq{}
	coll := generateCollectionSchema(schemapb.DataType_Int64, false)
	var err error
	err, req.Data = checkAndSetData(body, &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Schema: coll,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(0), req.Data[0]["id"])
	assert.Equal(t, int64(1), req.Data[0]["book_id"])
	assert.Equal(t, int64(2), req.Data[0]["word_count"])
	fieldsData, err := anyToColumns(req.Data, coll)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, fieldsData[len(fieldsData)-1].IsDynamic)
	assert.Equal(t, schemapb.DataType_JSON, fieldsData[len(fieldsData)-1].Type)
	assert.Equal(t, "{\"classified\":false,\"id\":0}", string(fieldsData[len(fieldsData)-1].GetScalars().GetJsonData().GetData()[0]))
}

func TestSerialize(t *testing.T) {
	parameters := []float32{0.11111, 0.22222}
	// assert.Equal(t, "\ufffd\ufffd\ufffd=\ufffd\ufffdc\u003e", string(serialize(parameters)))
	// assert.Equal(t, "vector2PlaceholderGroupBytes", string(vector2PlaceholderGroupBytes(parameters))) // todo
	assert.Equal(t, "\xa4\x8d\xe3=\xa4\x8dc>", string(serialize(parameters)))
	assert.Equal(t, "\n\x10\n\x02$0\x10e\x1a\b\xa4\x8d\xe3=\xa4\x8dc>", string(vector2PlaceholderGroupBytes(parameters))) // todo
}

func compareRow64(m1 map[string]interface{}, m2 map[string]interface{}) bool {
	for key, value := range m1 {
		if key == FieldBookIntro {
			arr1 := value.([]interface{})
			arr2 := m2[key].([]float64)
			if len(arr1) != len(arr2) {
				return false
			}
			for j, element := range arr1 {
				if element != arr2[j] {
					return false
				}
			}
		} else if value != m2[key] {
			return false
		}
	}

	for key, value := range m2 {
		if key == FieldBookIntro {
			continue
		} else if value != m1[key] {
			return false
		}
	}
	return true
}

func compareRow(m1 map[string]interface{}, m2 map[string]interface{}) bool {
	for key, value := range m1 {
		if key == FieldBookIntro {
			arr1 := value.([]float32)
			arr2 := m2[key].([]float32)
			if len(arr1) != len(arr2) {
				return false
			}
			for j, element := range arr1 {
				if element != arr2[j] {
					return false
				}
			}
		} else if (key == "field-binary") || (key == "field-json") {
			arr1 := value.([]byte)
			arr2 := m2[key].([]byte)
			if len(arr1) != len(arr2) {
				return false
			}
			for j, element := range arr1 {
				if element != arr2[j] {
					return false
				}
			}
		} else if value != m2[key] {
			return false
		}
	}

	for key, value := range m2 {
		if (key == FieldBookIntro) || (key == "field-binary") || (key == "field-json") {
			continue
		} else if value != m1[key] {
			return false
		}
	}
	return true
}

type CompareFunc func(map[string]interface{}, map[string]interface{}) bool

func compareRows(row1 []map[string]interface{}, row2 []map[string]interface{}, compareFunc CompareFunc) bool {
	if len(row1) != len(row2) {
		return false
	}
	for i, row := range row1 {
		if !compareFunc(row, row2[i]) {
			return false
		}
	}
	return true
}

func TestBuildQueryResp(t *testing.T) {
	outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
	rows, err := buildQueryResp(int64(0), outputFields, generateFieldData(), generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, true) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
	assert.Equal(t, nil, err)
	exceptRows := generateSearchResult(schemapb.DataType_Int64)
	assert.Equal(t, true, compareRows(rows, exceptRows, compareRow))
}

func newCollectionSchema(coll *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	fieldSchema1 := schemapb.FieldSchema{
		Name:     "field-bool",
		DataType: schemapb.DataType_Bool,
	}
	coll.Fields = append(coll.Fields, &fieldSchema1)

	fieldSchema2 := schemapb.FieldSchema{
		Name:     "field-int8",
		DataType: schemapb.DataType_Int8,
	}
	coll.Fields = append(coll.Fields, &fieldSchema2)

	fieldSchema3 := schemapb.FieldSchema{
		Name:     "field-int16",
		DataType: schemapb.DataType_Int16,
	}
	coll.Fields = append(coll.Fields, &fieldSchema3)

	fieldSchema4 := schemapb.FieldSchema{
		Name:     "field-int32",
		DataType: schemapb.DataType_Int32,
	}
	coll.Fields = append(coll.Fields, &fieldSchema4)

	fieldSchema5 := schemapb.FieldSchema{
		Name:     "field-float",
		DataType: schemapb.DataType_Float,
	}
	coll.Fields = append(coll.Fields, &fieldSchema5)

	fieldSchema6 := schemapb.FieldSchema{
		Name:     "field-double",
		DataType: schemapb.DataType_Double,
	}
	coll.Fields = append(coll.Fields, &fieldSchema6)

	fieldSchema7 := schemapb.FieldSchema{
		Name:     "field-string",
		DataType: schemapb.DataType_String,
	}
	coll.Fields = append(coll.Fields, &fieldSchema7)

	fieldSchema8 := schemapb.FieldSchema{
		Name:     "field-varchar",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "max_length", Value: "10"},
		},
	}
	coll.Fields = append(coll.Fields, &fieldSchema8)

	fieldSchema9 := schemapb.FieldSchema{
		Name:      "field-json",
		DataType:  schemapb.DataType_JSON,
		IsDynamic: false,
	}
	coll.Fields = append(coll.Fields, &fieldSchema9)

	//fieldSchema10 := schemapb.FieldSchema{
	//	Name:      "$meta",
	//	DataType:  schemapb.DataType_JSON,
	//	IsDynamic: true,
	//}
	//coll.Fields = append(coll.Fields, &fieldSchema10)

	return coll
}

func withUnsupportField(coll *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	fieldSchema10 := schemapb.FieldSchema{
		Name:      "field-array",
		DataType:  schemapb.DataType_Array,
		IsDynamic: false,
	}
	coll.Fields = append(coll.Fields, &fieldSchema10)

	return coll
}

func withDynamicField(coll *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	fieldSchema11 := schemapb.FieldSchema{
		Name:      "$meta",
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}
	coll.Fields = append(coll.Fields, &fieldSchema11)

	return coll
}

func newFieldData(fieldDatas []*schemapb.FieldData, firstFieldType schemapb.DataType) []*schemapb.FieldData {
	fieldData1 := schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: "field-bool",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: []bool{true, true, true},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData1)

	fieldData2 := schemapb.FieldData{
		Type:      schemapb.DataType_Int8,
		FieldName: "field-int8",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{0, 1, 2},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData2)

	fieldData3 := schemapb.FieldData{
		Type:      schemapb.DataType_Int16,
		FieldName: "field-int16",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{0, 1, 2},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData3)

	fieldData4 := schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "field-int32",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{0, 1, 2},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData4)

	fieldData5 := schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "field-float",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{0, 1, 2},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData5)

	fieldData6 := schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: "field-double",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{0, 1, 2},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData6)

	fieldData7 := schemapb.FieldData{
		Type:      schemapb.DataType_String,
		FieldName: "field-string",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"0", "1", "2"},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData7)

	fieldData8 := schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "field-varchar",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"0", "1", "2"},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData8)

	fieldData9 := schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "field-json",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: [][]byte{[]byte(`{"XXX": 0}`), []byte(`{"XXX": 0}`), []byte(`{"XXX": 0}`)},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData9)

	fieldData10 := schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "field-array",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}},
							{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}},
							{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}},
						},
					},
				},
			},
		},
		IsDynamic: false,
	}

	fieldData11 := schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "$meta",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: [][]byte{[]byte(`{"XXX": 0, "YYY": "0"}`), []byte(`{"XXX": 1, "YYY": "1"}`), []byte(`{"XXX": 2, "YYY": "2"}`)},
					},
				},
			},
		},
		IsDynamic: true,
	}
	fieldDatas = append(fieldDatas, &fieldData11)

	switch firstFieldType {
	case schemapb.DataType_None:
		break
	case schemapb.DataType_Bool:
		return []*schemapb.FieldData{&fieldData1}
	case schemapb.DataType_Int8:
		return []*schemapb.FieldData{&fieldData2}
	case schemapb.DataType_Int16:
		return []*schemapb.FieldData{&fieldData3}
	case schemapb.DataType_Int32:
		return []*schemapb.FieldData{&fieldData4}
	case schemapb.DataType_Float:
		return []*schemapb.FieldData{&fieldData5}
	case schemapb.DataType_Double:
		return []*schemapb.FieldData{&fieldData6}
	case schemapb.DataType_String:
		return []*schemapb.FieldData{&fieldData7}
	case schemapb.DataType_VarChar:
		return []*schemapb.FieldData{&fieldData8}
	case schemapb.DataType_BinaryVector:
		vectorField := generateVectorFieldData(true)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_FloatVector:
		vectorField := generateVectorFieldData(false)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Array:
		return []*schemapb.FieldData{&fieldData10}
	case schemapb.DataType_JSON:
		return []*schemapb.FieldData{&fieldData9}
	default:
		return []*schemapb.FieldData{
			{
				FieldName: "wrong-field-type",
				Type:      firstFieldType,
			},
		}
	}

	return fieldDatas
}

func newSearchResult(results []map[string]interface{}) []map[string]interface{} {
	for i, result := range results {
		result["field-bool"] = true
		result["field-int8"] = int8(i)
		result["field-int16"] = int16(i)
		result["field-int32"] = int32(i)
		result["field-float"] = float32(i)
		result["field-double"] = float64(i)
		result["field-varchar"] = strconv.Itoa(i)
		result["field-string"] = strconv.Itoa(i)
		result["field-binary"] = []byte{byte(i)}
		result["field-json"] = []byte(`{"XXX": 0}`)
		result["XXX"] = float64(i)
		result["YYY"] = strconv.Itoa(i)
		results[i] = result
	}
	return results
}

func TestAnyToColumn(t *testing.T) {
	data, err := anyToColumns(newSearchResult(generateSearchResult(schemapb.DataType_Int64)), newCollectionSchema(generateCollectionSchema(schemapb.DataType_Int64, false)))
	assert.Equal(t, nil, err)
	assert.Equal(t, 13, len(data))
}

func TestBuildQueryResps(t *testing.T) {
	outputFields := []string{"XXX", "YYY"}
	outputFieldsList := [][]string{outputFields, {"$meta"}, {"$meta", FieldBookID, FieldBookIntro, "YYY"}}
	for _, theOutputFields := range outputFieldsList {
		rows, err := buildQueryResp(int64(0), theOutputFields, newFieldData(generateFieldData(), schemapb.DataType_None), generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, true)
		assert.Equal(t, nil, err)
		exceptRows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
		assert.Equal(t, true, compareRows(rows, exceptRows, compareRow))
	}

	dataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector,
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Float, schemapb.DataType_Double,
		schemapb.DataType_String, schemapb.DataType_VarChar,
		schemapb.DataType_JSON, schemapb.DataType_Array,
	}
	for _, dateType := range dataTypes {
		_, err := buildQueryResp(int64(0), outputFields, newFieldData([]*schemapb.FieldData{}, dateType), generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, true)
		assert.Equal(t, nil, err)
	}

	_, err := buildQueryResp(int64(0), outputFields, newFieldData([]*schemapb.FieldData{}, 1000), generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, true)
	assert.Equal(t, "the type(1000) of field(wrong-field-type) is not supported, use other sdk please", err.Error())

	res, err := buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, true)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	res, err = buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, false)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	res, err = buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIds(schemapb.DataType_VarChar, 3), []float32{0.01, 0.04, 0.09}, true)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	_, err = buildQueryResp(int64(0), outputFields, generateFieldData(), generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04, 0.09}, false)
	assert.Equal(t, nil, err)

	// len(rows) != len(scores), didn't show distance
	_, err = buildQueryResp(int64(0), outputFields, newFieldData(generateFieldData(), schemapb.DataType_None), generateIds(schemapb.DataType_Int64, 3), []float32{0.01, 0.04}, true)
	assert.Equal(t, nil, err)
}
