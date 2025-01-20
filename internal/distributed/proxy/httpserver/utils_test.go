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
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	FieldWordCount = "word_count"
	FieldBookID    = "book_id"
	FieldBookIntro = "book_intro"
	FieldVarchar   = "varchar_field"
)

var DefaultScores = []float32{0.01, 0.04, 0.09}

func generatePrimaryField(datatype schemapb.DataType, autoID bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         FieldBookID,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     datatype,
		AutoID:       autoID,
	}
}

func generateIDs(dataType schemapb.DataType, num int) *schemapb.IDs {
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

func generateVectorFieldSchema(dataType schemapb.DataType) *schemapb.FieldSchema {
	dim := "2"
	if dataType == schemapb.DataType_BinaryVector {
		dim = "8"
	}
	typeParams := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: dim,
		},
	}
	if dataType == schemapb.DataType_SparseFloatVector {
		typeParams = nil
	}
	return &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID + int64(dataType),
		IsPrimaryKey: false,
		DataType:     dataType,
		AutoID:       false,
		TypeParams:   typeParams,
	}
}

func generateCollectionSchema(primaryDataType schemapb.DataType, autoID bool, isDynamic bool) *schemapb.CollectionSchema {
	primaryField := generatePrimaryField(primaryDataType, autoID)
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = FieldBookIntro
	fields := []*schemapb.FieldSchema{
		primaryField, {
			FieldID:      common.StartOfUserFieldID + 1,
			Name:         FieldWordCount,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     5,
			AutoID:       false,
		}, vectorField,
	}
	if isDynamic {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID:      common.StartOfUserFieldID + 2,
			Name:         "$meta",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     23,
			AutoID:       false,
			IsDynamic:    true,
		})
	}
	return &schemapb.CollectionSchema{
		Name:               DefaultCollectionName,
		Description:        "",
		AutoID:             autoID,
		Fields:             fields,
		EnableDynamicField: isDynamic,
	}
}

func generateDocInDocOutCollectionSchema(primaryDataType schemapb.DataType) *schemapb.CollectionSchema {
	primaryField := generatePrimaryField(primaryDataType, false)
	vectorField := generateVectorFieldSchema(schemapb.DataType_SparseFloatVector)
	vectorField.Name = FieldBookIntro
	vectorField.IsFunctionOutput = true
	return &schemapb.CollectionSchema{
		Name:        DefaultCollectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			primaryField, {
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         FieldWordCount,
				IsPrimaryKey: false,
				Description:  "",
				DataType:     5,
				AutoID:       false,
			}, vectorField, {
				FieldID:      common.StartOfUserFieldID + 2,
				Name:         FieldVarchar,
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_VarChar,
				AutoID:       false,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "sum",
				Type:             schemapb.FunctionType_BM25,
				InputFieldNames:  []string{FieldVarchar},
				OutputFieldNames: []string{FieldBookIntro},
			},
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

func generateVectorFieldData(vectorType schemapb.DataType) schemapb.FieldData {
	switch vectorType {
	case schemapb.DataType_BinaryVector:
		return schemapb.FieldData{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: FieldBookIntro,
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
	case schemapb.DataType_Float16Vector:
		return schemapb.FieldData{
			Type:      schemapb.DataType_Float16Vector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 8,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: []byte{byte(0), byte(0), byte(1), byte(1), byte(2), byte(2)},
					},
				},
			},
			IsDynamic: false,
		}
	case schemapb.DataType_BFloat16Vector:
		return schemapb.FieldData{
			Type:      schemapb.DataType_BFloat16Vector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 8,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: []byte{byte(0), byte(0), byte(1), byte(1), byte(2), byte(2)},
					},
				},
			},
			IsDynamic: false,
		}
	case schemapb.DataType_FloatVector:
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
	case schemapb.DataType_SparseFloatVector:
		contents := make([][]byte, 0, 3)
		contents = append(contents, typeutil.CreateSparseFloatRow([]uint32{1, 2, 3}, []float32{0.1, 0.11, 0.2}))
		contents = append(contents, typeutil.CreateSparseFloatRow([]uint32{100, 200, 300}, []float32{10.1, 20.11, 30.2}))
		contents = append(contents, typeutil.CreateSparseFloatRow([]uint32{1000, 2000, 3000}, []float32{5000.1, 7000.11, 9000.2}))
		return schemapb.FieldData{
			Type:      schemapb.DataType_SparseFloatVector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: int64(3001),
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Dim:      int64(3001),
							Contents: contents,
						},
					},
				},
			},
			IsDynamic: false,
		}
	default:
		panic("unsupported vector type")
	}
}

func generateFieldData(dataType schemapb.DataType) []*schemapb.FieldData {
	var fieldData1 *schemapb.FieldData
	switch dataType {
	case schemapb.DataType_Int64:
		fieldData1 = &schemapb.FieldData{
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
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		fieldData1 = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: FieldBookID,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
			},
			IsDynamic: false,
		}
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

	fieldData3 := generateVectorFieldData(schemapb.DataType_FloatVector)
	return []*schemapb.FieldData{fieldData1, &fieldData2, &fieldData3}
}

func wrapRequestBody(data []map[string]interface{}) ([]byte, error) {
	body := map[string]interface{}{}
	body["data"] = data
	return json.Marshal(body)
}

func generateRawRows(dataType schemapb.DataType) []map[string]interface{} {
	row1 := map[string]interface{}{
		FieldBookID:    int64(1),
		FieldWordCount: int64(1000),
		FieldBookIntro: []float32{0.1, 0.11},
	}
	row2 := map[string]interface{}{
		FieldBookID:    int64(2),
		FieldWordCount: int64(2000),
		FieldBookIntro: []float32{0.2, 0.22},
	}
	row3 := map[string]interface{}{
		FieldBookID:    int64(3),
		FieldWordCount: int64(3000),
		FieldBookIntro: []float32{0.3, 0.33},
	}
	if dataType == schemapb.DataType_String {
		row1[FieldBookID] = "1"
		row2[FieldBookID] = "2"
		row3[FieldBookID] = "3"
	}
	return []map[string]interface{}{row1, row2, row3}
}

func generateRequestBody(dataType schemapb.DataType) ([]byte, error) {
	return wrapRequestBody(generateRawRows(dataType))
}

func generateRequestBodyWithArray(dataType schemapb.DataType) ([]byte, error) {
	rows := generateRawRows(dataType)
	for _, result := range rows {
		result["array-bool"] = "[true]"
		result["array-int8"] = "[0]"
		result["array-int16"] = "[0]"
		result["array-int32"] = "[0]"
		result["array-int64"] = "[0]"
		result["array-float"] = "[0.0]"
		result["array-double"] = "[0.0]"
		result["array-varchar"] = "[\"\"]"
	}
	return wrapRequestBody(rows)
}

func generateSearchResult(dataType schemapb.DataType) []map[string]interface{} {
	rows := generateRawRows(dataType)
	for i, row := range rows {
		row[DefaultPrimaryFieldName] = row[FieldBookID]
		row[HTTPReturnDistance] = DefaultScores[i]
	}
	return rows
}

func generateNestSearchResult(dataType schemapb.DataType) [][]map[string]interface{} {
	var res [][]map[string]interface{}
	row1 := map[string]interface{}{
		FieldBookID:    int64(1),
		FieldWordCount: int64(1000),
		FieldBookIntro: []float32{0.1, 0.11},
	}
	row1[HTTPReturnDistance] = DefaultScores[0]

	row2 := map[string]interface{}{
		FieldBookID:    int64(2),
		FieldWordCount: int64(2000),
		FieldBookIntro: []float32{0.2, 0.22},
	}
	row2[HTTPReturnDistance] = DefaultScores[1]

	row3 := map[string]interface{}{
		FieldBookID:    int64(3),
		FieldWordCount: int64(3000),
		FieldBookIntro: []float32{0.3, 0.33},
	}
	row3[HTTPReturnDistance] = DefaultScores[2]

	if dataType == schemapb.DataType_String || dataType == schemapb.DataType_VarChar {
		row1[FieldBookID] = "1"
		row2[FieldBookID] = "2"
		row3[FieldBookID] = "3"
	}
	row1[DefaultPrimaryFieldName] = row1[FieldBookID]
	row2[DefaultPrimaryFieldName] = row2[FieldBookID]
	row3[DefaultPrimaryFieldName] = row3[FieldBookID]

	res1 := []map[string]interface{}{row1}
	res2 := []map[string]interface{}{row2, row3}
	res = append(res, res1, res2)
	return res
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
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	indexes := generateIndexes()
	assert.Equal(t, []gin.H{
		{
			HTTPReturnFieldName:          FieldBookID,
			HTTPReturnFieldType:          "Int64",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldPrimaryKey:    true,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
		},
		{
			HTTPReturnFieldName:          FieldWordCount,
			HTTPReturnFieldType:          "Int64",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
		},
		{
			HTTPReturnFieldName:          FieldBookIntro,
			HTTPReturnFieldType:          "FloatVector(2)",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
		},
	}, printFields(coll.Fields))
	assert.Equal(t, []gin.H{
		{
			HTTPReturnFieldName:          FieldBookID,
			HTTPReturnFieldType:          "Int64",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldPrimaryKey:    true,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
			HTTPReturnFieldID:            int64(100),
		},
		{
			HTTPReturnFieldName:          FieldWordCount,
			HTTPReturnFieldType:          "Int64",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
			HTTPReturnFieldID:            int64(101),
		},
		{
			HTTPReturnFieldName:          FieldBookIntro,
			HTTPReturnFieldType:          "FloatVector",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
			HTTPReturnFieldID:            int64(201),
			Params: []*commonpb.KeyValuePair{
				{Key: Dim, Value: "2"},
			},
		},
	}, printFieldsV2(coll.Fields))
	assert.Equal(t, []gin.H{
		{
			HTTPIndexName:             DefaultIndexName,
			HTTPIndexField:            FieldBookIntro,
			HTTPReturnIndexMetricType: DefaultMetricType,
		},
	}, printIndexes(indexes))
	assert.Equal(t, DefaultMetricType, getMetricType(indexes[0].Params))
	assert.Equal(t, DefaultMetricType, getMetricType(nil))
	fields := []*schemapb.FieldSchema{}
	for _, field := range newCollectionSchema(coll).Fields {
		if field.DataType == schemapb.DataType_VarChar {
			fields = append(fields, field)
		} else if field.DataType == schemapb.DataType_Array {
			fields = append(fields, field)
		}
	}
	assert.Equal(t, []gin.H{
		{
			HTTPReturnFieldName:          "field-varchar",
			HTTPReturnFieldType:          "VarChar(10)",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
		},
		{
			HTTPReturnFieldName:          "field-array",
			HTTPReturnFieldType:          "Array",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
		},
	}, printFields(fields))
	assert.Equal(t, []gin.H{
		{
			HTTPReturnFieldName:          "field-varchar",
			HTTPReturnFieldType:          "VarChar",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnDescription:        "",
			HTTPReturnFieldID:            int64(0),
			Params: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "10"},
			},
		},
		{
			HTTPReturnFieldName:          "field-array",
			HTTPReturnFieldType:          "Array",
			HTTPReturnFieldPartitionKey:  false,
			HTTPReturnFieldClusteringKey: false,
			HTTPReturnFieldNullable:      false,
			HTTPReturnFieldPrimaryKey:    false,
			HTTPReturnFieldAutoID:        false,
			HTTPReturnDescription:        "",
			HTTPReturnFieldID:            int64(0),
			HTTPReturnFieldElementType:   "Bool",
		},
	}, printFieldsV2(fields))
}

func TestPrimaryField(t *testing.T) {
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	primaryField := generatePrimaryField(schemapb.DataType_Int64, false)
	field, ok := getPrimaryField(coll)
	assert.Equal(t, true, ok)
	assert.EqualExportedValues(t, primaryField, field)

	assert.Equal(t, "1,2,3", joinArray([]int64{1, 2, 3}))
	assert.Equal(t, "1,2,3", joinArray([]string{"1", "2", "3"}))

	jsonStr := "{\"id\": [1, 2, 3]}"
	idStr := gjson.Get(jsonStr, "id")
	rangeStr, err := convertRange(primaryField, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1,2,3", rangeStr)
	filter, err := checkGetPrimaryKey(coll, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "book_id in [1,2,3]", filter)

	primaryField = generatePrimaryField(schemapb.DataType_VarChar, false)
	jsonStr = "{\"id\": [\"1\", \"2\", \"3\"]}"
	idStr = gjson.Get(jsonStr, "id")
	rangeStr, err = convertRange(primaryField, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, `"1","2","3"`, rangeStr)
	coll2 := generateCollectionSchema(schemapb.DataType_VarChar, false, true)
	filter, err = checkGetPrimaryKey(coll2, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, `book_id in ["1","2","3"]`, filter)
}

func TestAnyToColumns(t *testing.T) {
	t.Run("insert with dynamic field", func(t *testing.T) {
		body := "{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(0), req.Data[0]["id"])
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, fieldsData[len(fieldsData)-1].IsDynamic)
		assert.Equal(t, schemapb.DataType_JSON, fieldsData[len(fieldsData)-1].Type)
		assert.Equal(t, "{\"classified\":false,\"id\":0}", string(fieldsData[len(fieldsData)-1].GetScalars().GetJsonData().GetData()[0]))
	})

	t.Run("upsert with dynamic field", func(t *testing.T) {
		body := "{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(0), req.Data[0]["id"])
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, fieldsData[len(fieldsData)-1].IsDynamic)
		assert.Equal(t, schemapb.DataType_JSON, fieldsData[len(fieldsData)-1].Type)
		assert.Equal(t, "{\"classified\":false,\"id\":0}", string(fieldsData[len(fieldsData)-1].GetScalars().GetJsonData().GetData()[0]))
	})

	t.Run("insert with dynamic field, but pass pk when autoid==true", func(t *testing.T) {
		body := "{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, true)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(0), req.Data[0]["id"])
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		_, err = anyToColumns(req.Data, nil, coll, true)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "no need to pass pk field"))
	})

	t.Run("pass more field", func(t *testing.T) {
		body := "{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}"
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		err, _, _ = checkAndSetData(body, coll)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "has pass more fiel"))
	})

	t.Run("insert with autoid==false", func(t *testing.T) {
		body := "{\"data\": {\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("insert with autoid==false but has no pk", func(t *testing.T) {
		body := "{\"data\": { \"book_intro\": [0.1, 0.2], \"word_count\": 2}}"
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
		var err error
		err, _, _ = checkAndSetData(body, coll)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "strconv.ParseInt: parsing \"\": invalid syntax"))
	})

	t.Run("insert with autoid==true", func(t *testing.T) {
		body := "{\"data\": { \"book_intro\": [0.1, 0.2], \"word_count\": 2}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("upsert with autoid==true", func(t *testing.T) {
		body := "{\"data\": {\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("upsert with autoid==false", func(t *testing.T) {
		body := "{\"data\": {\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}"
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		err, req.Data, _ = checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})
}

func TestCheckAndSetData(t *testing.T) {
	t.Run("invalid field name with dynamic field", func(t *testing.T) {
		body := "{\"data\": {\"id\": 0,\"$meta\": 2,\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}"
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		var err error
		err, _, _ = checkAndSetData(body, coll)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "use the invalid field name"))
	})
	t.Run("without vector", func(t *testing.T) {
		body := "{\"data\": {}}"
		var err error
		primaryField := generatePrimaryField(schemapb.DataType_Int64, true)
		floatVectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		floatVectorField.Name = "floatVector"
		binaryVectorField := generateVectorFieldSchema(schemapb.DataType_BinaryVector)
		binaryVectorField.Name = "binaryVector"
		float16VectorField := generateVectorFieldSchema(schemapb.DataType_Float16Vector)
		float16VectorField.Name = "float16Vector"
		bfloat16VectorField := generateVectorFieldSchema(schemapb.DataType_BFloat16Vector)
		bfloat16VectorField.Name = "bfloat16Vector"
		err, _, _ = checkAndSetData(body, &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				primaryField, floatVectorField,
			},
			EnableDynamicField: true,
		})
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "missing vector field"))
		err, _, _ = checkAndSetData(body, &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				primaryField, binaryVectorField,
			},
			EnableDynamicField: true,
		})
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "missing vector field"))
		err, _, _ = checkAndSetData(body, &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				primaryField, float16VectorField,
			},
			EnableDynamicField: true,
		})
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "missing vector field"))
		err, _, _ = checkAndSetData(body, &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				primaryField, bfloat16VectorField,
			},
			EnableDynamicField: true,
		})
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "missing vector field"))
	})

	t.Run("with pk when autoID == True when upsert", func(t *testing.T) {
		arrayFieldName := "array-int64"
		body := "{\"data\": {\"book_id\": 9999999999999999, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}"
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        arrayFieldName,
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		})
		err, data, validData := checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(data))
		assert.Equal(t, 0, len(validData))
	})

	t.Run("without pk when autoID == True when insert", func(t *testing.T) {
		arrayFieldName := "array-int64"
		body := "{\"data\": {\"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}"
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        arrayFieldName,
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		})
		err, data, validData := checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(data))
		assert.Equal(t, 0, len(validData))
	})

	t.Run("with pk when autoID == false", func(t *testing.T) {
		arrayFieldName := "array-int64"
		body := "{\"data\": {\"book_id\": 9999999999999999, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}"
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        arrayFieldName,
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		})
		err, data, validData := checkAndSetData(body, coll)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(data))
		assert.Equal(t, 0, len(validData))
	})
}

func TestInsertWithInt64(t *testing.T) {
	arrayFieldName := "array-int64"
	body := "{\"data\": {\"book_id\": 9999999999999999, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}"
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:        arrayFieldName,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	})
	err, data, validData := checkAndSetData(body, coll)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(data))
	assert.Equal(t, 0, len(validData))
	assert.Equal(t, int64(9999999999999999), data[0][FieldBookID])
	arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
	assert.Equal(t, int64(9999999999999999), arr.GetLongData().GetData()[0])
}

func TestInsertWithNullableField(t *testing.T) {
	arrayFieldName := "array-int64"
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:        arrayFieldName,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	})
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:     "nullable",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	body := "{\"data\": [{\"book_id\": 9999999999999999, \"\nullable\": null,\"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]},{\"book_id\": 1, \"nullable\": 1,\"book_intro\": [0.3, 0.4], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}]"
	err, data, validData := checkAndSetData(body, coll)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(data))
	assert.Equal(t, 1, len(validData))
	assert.Equal(t, 2, len(validData["nullable"]))
	assert.False(t, validData["nullable"][0])
	assert.True(t, validData["nullable"][1])
	assert.Equal(t, int64(9999999999999999), data[0][FieldBookID])
	arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
	assert.Equal(t, int64(9999999999999999), arr.GetLongData().GetData()[0])
	assert.Equal(t, 4, len(data[0]))
	assert.Equal(t, 5, len(data[1]))

	fieldData, err := anyToColumns(data, validData, coll, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(coll.Fields), len(fieldData))
}

func TestInsertWithDefaultValueField(t *testing.T) {
	arrayFieldName := "array-int64"
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:        arrayFieldName,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	})
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:     "fid",
		DataType: schemapb.DataType_Int64,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_LongData{
				LongData: 10,
			},
		},
	})
	body := "{\"data\": [{\"book_id\": 9999999999999999, \"\fid\": null,\"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]},{\"book_id\": 1, \"fid\": 1,\"book_intro\": [0.3, 0.4], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}]"
	err, data, validData := checkAndSetData(body, coll)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(data))
	assert.Equal(t, 1, len(validData))
	assert.Equal(t, 2, len(validData["fid"]))
	assert.False(t, validData["fid"][0])
	assert.True(t, validData["fid"][1])
	assert.Equal(t, int64(9999999999999999), data[0][FieldBookID])
	arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
	assert.Equal(t, int64(9999999999999999), arr.GetLongData().GetData()[0])
	assert.Equal(t, 4, len(data[0]))
	assert.Equal(t, 5, len(data[1]))

	fieldData, err := anyToColumns(data, validData, coll, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(coll.Fields), len(fieldData))
}

func TestSerialize(t *testing.T) {
	parameters := []float32{0.11111, 0.22222}
	assert.Equal(t, "\n\x10\n\x02$0\x10e\x1a\b\xa4\x8d\xe3=\xa4\x8dc>", string(vectors2PlaceholderGroupBytes([][]float32{parameters}))) // todo

	// test serialize fp32 to {fp32, fp16, bf16}
	requestBody := "{\"data\": [[0.11111, 0.22222]]}"
	vectors := gjson.Get(requestBody, HTTPRequestData)

	// fp32 -> fp32
	values, err := serializeFloatVectors(vectors.Raw, schemapb.DataType_FloatVector, 2, -1, typeutil.Float32ArrayToBytes)
	assert.Nil(t, err)
	placeholderValue := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: values,
	}
	bytes, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			placeholderValue,
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, "\n\x10\n\x02$0\x10e\x1a\b\xa4\x8d\xe3=\xa4\x8dc>", string(bytes)) // todo

	// fp32 -> fp16/bf16
	for _, testcase := range []struct {
		dataType      schemapb.DataType
		serializeFunc func([]float32) []byte
		byteStr       string
	}{
		{schemapb.DataType_Float16Vector, typeutil.Float32ArrayToFloat16Bytes, "\n\f\n\x02$0\x10e\x1a\x04\x1c/\x1c3"},
		{schemapb.DataType_BFloat16Vector, typeutil.Float32ArrayToBFloat16Bytes, "\n\f\n\x02$0\x10e\x1a\x04\xe3=c>"},
	} {
		values, err = serializeFloatOrByteVectors(vectors, testcase.dataType, 2, testcase.serializeFunc)
		assert.Nil(t, err)
		placeholderValue := &commonpb.PlaceholderValue{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_FloatVector,
			Values: values,
		}
		bytes, err := proto.Marshal(&commonpb.PlaceholderGroup{
			Placeholders: []*commonpb.PlaceholderValue{
				placeholderValue,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, testcase.byteStr, string(bytes))
	}

	for _, dataType := range []schemapb.DataType{schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		request := map[string]interface{}{
			HTTPRequestData: []interface{}{
				[]byte{1, 2},
			},
		}
		requestBody, _ := json.Marshal(request)
		values, err = serializeByteVectors(gjson.Get(string(requestBody), HTTPRequestData).Raw, dataType, -1, 2)
		assert.Nil(t, err)
		placeholderValue = &commonpb.PlaceholderValue{
			Tag:    "$0",
			Values: values,
		}
		_, err = proto.Marshal(&commonpb.PlaceholderGroup{
			Placeholders: []*commonpb.PlaceholderValue{
				placeholderValue,
			},
		})
		assert.Nil(t, err)
	}
}

func TestConvertQueries2Placeholder(t *testing.T) {
	fp16Req := map[string]interface{}{
		HTTPRequestData: []interface{}{
			typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1}),
			typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1, 1, 1}),
		},
	}
	fp16ReqBody, _ := json.Marshal(fp16Req)
	const Float16VecJSON = `{"data":["HC8cMwAAALw=","ADwAPAA8ADw="]}`
	assert.Equal(t, Float16VecJSON, string(fp16ReqBody))

	bf16Req := map[string]interface{}{
		HTTPRequestData: []interface{}{
			typeutil.Float32ArrayToBFloat16Bytes([]float32{0.11111, 0.22222, 0, -1}),
			typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 1, 1, 1}),
		},
	}
	bf16ReqBody, _ := json.Marshal(bf16Req)
	const BFloat16VecJSON = `{"data":["4z1jPgAAgL8=","gD+AP4A/gD8="]}`
	assert.Equal(t, BFloat16VecJSON, string(bf16ReqBody))

	type testCase struct {
		requestBody     string
		dataType        schemapb.DataType
		dim             int64
		placehoderValue func() [][]byte
	}
	testCases := make([]testCase, 0)

	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_FloatVector} {
		// corner case: empty data
		testCases = append(testCases, []testCase{
			{
				"{\"data\": []}",
				dataType,
				0,
				func() [][]byte {
					return [][]byte{}
				},
			}, {
				"{\"data\": []}",
				dataType,
				100,
				func() [][]byte {
					return [][]byte{}
				},
			}, {
				"{\"data\": [[], []]}",
				dataType,
				0,
				func() [][]byte {
					return [][]byte{{}, {}}
				},
			},
		}...)
	}

	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		// corner case: empty float16/bfloat16 vector
		testCases = append(testCases, []testCase{
			{
				`"{"data": ["", ""]}"`,
				dataType,
				0,
				func() [][]byte {
					return [][]byte{nil, nil}
				},
			}, {
				`"{"data": [""]}"`,
				dataType,
				0,
				func() [][]byte {
					return [][]byte{nil}
				},
			},
		}...)
	}

	testCases = append(testCases, []testCase{
		{
			"{\"data\": [[0.11111, 0.22222]]}",
			schemapb.DataType_FloatVector,
			2,
			func() [][]byte {
				bv := typeutil.Float32ArrayToBytes([]float32{0.11111, 0.22222})
				return [][]byte{bv}
			},
		}, {
			"{\"data\": [[0.11111, 0.22222, 0, -1]]}",
			schemapb.DataType_Float16Vector,
			4,
			func() [][]byte {
				bv := typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				return [][]byte{bv}
			},
		}, {
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1, 1]]}",
			schemapb.DataType_Float16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		}, {
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1, 1]]}",
			schemapb.DataType_BFloat16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToBFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		}, {
			Float16VecJSON,
			schemapb.DataType_Float16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		}, {
			BFloat16VecJSON,
			schemapb.DataType_BFloat16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToBFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		},
	}...)

	for _, testcase := range testCases {
		phv, err := convertQueries2Placeholder(testcase.requestBody, testcase.dataType, testcase.dim)
		assert.Nil(t, err)
		assert.Equal(t, testcase.placehoderValue(), phv.GetValues())
	}

	for _, testcase := range []testCase{
		// mismatched Datatype
		{
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1, 1]]}",
			schemapb.DataType_Float16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToBFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		}, {
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1, 1]]}",
			schemapb.DataType_BFloat16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		},
	} {
		phv, err := convertQueries2Placeholder(testcase.requestBody, testcase.dataType, testcase.dim)
		assert.Nil(t, err)
		assert.NotEqual(t, testcase.placehoderValue(), phv.GetValues())
	}

	for _, testcase := range []testCase{
		// mismatched dimension
		{
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1, 1]]}",
			schemapb.DataType_Float16Vector,
			2,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToBFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		}, {
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1, 1]]}",
			schemapb.DataType_BFloat16Vector,
			8,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		}, {
			"{\"data\": [[0.11111, 0.22222, 0, -1], [1, 1, 1]]}",
			schemapb.DataType_BFloat16Vector,
			4,
			func() [][]byte {
				bv1 := typeutil.Float32ArrayToFloat16Bytes([]float32{0.11111, 0.22222, 0, -1})
				bv2 := typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1, 1, 1})
				return [][]byte{bv1, bv2}
			},
		},
	} {
		_, err := convertQueries2Placeholder(testcase.requestBody, testcase.dataType, testcase.dim)
		assert.NotNil(t, err)
	}
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
		} else if key == "field-json" {
			arr1 := value.(string)
			arr2 := m2[key].([]byte)
			if arr1 != string(arr2) {
				return false
			}
		} else if strings.HasPrefix(key, "array-") {
			continue
		} else if value != m2[key] {
			return false
		}
	}

	for key, value := range m2 {
		if (key == FieldBookIntro) || (key == "field-json") || (key == "field-array") {
			continue
		} else if strings.HasPrefix(key, "array-") {
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

func TestBuildSearchResp(t *testing.T) {
	t.Run("with multi nq with int64 type", func(t *testing.T) {
		outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
		resp, err := buildSearchResp([]int64{1, 2}, outputFields, generateFieldData(schemapb.DataType_Int64), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(resp))
		expect := generateNestSearchResult(schemapb.DataType_Int64)

		assert.Equal(t, true, compareRows(resp[0], expect[0], compareRow))
		assert.Equal(t, true, compareRows(resp[1], expect[1], compareRow))
	})

	t.Run("with one nq with int64 type", func(t *testing.T) {
		outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
		resp, err := buildSearchResp([]int64{3}, outputFields, generateFieldData(schemapb.DataType_Int64), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
		assert.Equal(t, nil, err)
		expect := generateSearchResult(schemapb.DataType_Int64)
		assert.Equal(t, true, compareRows(resp[0], expect, compareRow))
	})

	t.Run("with multi nq with string type", func(t *testing.T) {
		outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
		resp, err := buildSearchResp([]int64{1, 2}, outputFields, generateFieldData(schemapb.DataType_VarChar), generateIDs(schemapb.DataType_VarChar, 3), DefaultScores, true) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(resp))
		expect := generateNestSearchResult(schemapb.DataType_String)
		assert.Equal(t, true, compareRows(resp[0], expect[0], compareRow))
		assert.Equal(t, true, compareRows(resp[1], expect[1], compareRow))
	})

	t.Run("with one nq with string type", func(t *testing.T) {
		outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
		resp, err := buildSearchResp([]int64{3}, outputFields, generateFieldData(schemapb.DataType_VarChar), generateIDs(schemapb.DataType_VarChar, 3), DefaultScores, true) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
		assert.Equal(t, nil, err)
		expect := generateSearchResult(schemapb.DataType_String)
		assert.Equal(t, true, compareRows(resp[0], expect, compareRow))
	})
}

func TestBuildQueryResp(t *testing.T) {
	outputFields := []string{FieldBookID, FieldWordCount, "author", "date"}
	rows, err := buildQueryResp(int64(0), outputFields, generateFieldData(schemapb.DataType_Int64), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, 0) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
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

	fieldSchema10 := schemapb.FieldSchema{
		Name:        "field-array",
		DataType:    schemapb.DataType_Array,
		IsDynamic:   false,
		ElementType: schemapb.DataType_Bool,
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

func withArrayField(coll *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	fieldSchema0 := schemapb.FieldSchema{
		Name:        "array-bool",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Bool,
	}
	coll.Fields = append(coll.Fields, &fieldSchema0)
	fieldSchema1 := schemapb.FieldSchema{
		Name:        "array-int8",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int8,
	}
	coll.Fields = append(coll.Fields, &fieldSchema1)
	fieldSchema2 := schemapb.FieldSchema{
		Name:        "array-int16",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int16,
	}
	coll.Fields = append(coll.Fields, &fieldSchema2)
	fieldSchema3 := schemapb.FieldSchema{
		Name:        "array-int32",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int32,
	}
	coll.Fields = append(coll.Fields, &fieldSchema3)
	fieldSchema4 := schemapb.FieldSchema{
		Name:        "array-int64",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	}
	coll.Fields = append(coll.Fields, &fieldSchema4)
	fieldSchema5 := schemapb.FieldSchema{
		Name:        "array-float",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Float,
	}
	coll.Fields = append(coll.Fields, &fieldSchema5)
	fieldSchema6 := schemapb.FieldSchema{
		Name:        "array-double",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Double,
	}
	coll.Fields = append(coll.Fields, &fieldSchema6)
	fieldSchema7 := schemapb.FieldSchema{
		Name:        "array-varchar",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_VarChar,
	}
	coll.Fields = append(coll.Fields, &fieldSchema7)
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
		return fieldDatas
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
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_FloatVector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Float16Vector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_BFloat16Vector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Array:
		return []*schemapb.FieldData{&fieldData10}
	case schemapb.DataType_JSON:
		return []*schemapb.FieldData{&fieldData9}
	case schemapb.DataType_SparseFloatVector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	default:
		return []*schemapb.FieldData{
			{
				FieldName: "wrong-field-type",
				Type:      firstFieldType,
			},
		}
	}
}

func newNullableFieldData(fieldDatas []*schemapb.FieldData, firstFieldType schemapb.DataType) []*schemapb.FieldData {
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
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
		ValidData: []bool{true, false, true},
		IsDynamic: false,
	}

	fieldData11 := schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "field-int64",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{0, 1, 2},
					},
				},
			},
		},
		ValidData: []bool{true, false, true},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData11)

	switch firstFieldType {
	case schemapb.DataType_None:
		return fieldDatas
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
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_FloatVector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Float16Vector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_BFloat16Vector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Array:
		return []*schemapb.FieldData{&fieldData10}
	case schemapb.DataType_JSON:
		return []*schemapb.FieldData{&fieldData9}
	case schemapb.DataType_SparseFloatVector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Int64:
		return []*schemapb.FieldData{&fieldData11}
	default:
		return []*schemapb.FieldData{
			{
				FieldName: "wrong-field-type",
				Type:      firstFieldType,
			},
		}
	}
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
		result["field-json"] = []byte(`{"XXX": 0}`)
		result["field-array"] = []bool{true}
		result["array-bool"] = []bool{true}
		result["array-int8"] = []int32{0}
		result["array-int16"] = []int32{0}
		result["array-int32"] = []int32{0}
		result["array-int64"] = []int64{0}
		result["array-float"] = []float32{0}
		result["array-double"] = []float64{0}
		result["array-varchar"] = []string{""}
		result["XXX"] = float64(i)
		result["YYY"] = strconv.Itoa(i)
		results[i] = result
	}
	return results
}

func newCollectionSchemaWithArray(coll *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	fieldSchema1 := schemapb.FieldSchema{
		Name:        "array-bool",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Bool,
	}
	coll.Fields = append(coll.Fields, &fieldSchema1)

	fieldSchema2 := schemapb.FieldSchema{
		Name:        "array-int8",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int8,
	}
	coll.Fields = append(coll.Fields, &fieldSchema2)

	fieldSchema3 := schemapb.FieldSchema{
		Name:        "array-int16",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int16,
	}
	coll.Fields = append(coll.Fields, &fieldSchema3)

	fieldSchema4 := schemapb.FieldSchema{
		Name:        "array-int32",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int32,
	}
	coll.Fields = append(coll.Fields, &fieldSchema4)

	fieldSchema5 := schemapb.FieldSchema{
		Name:        "array-int64",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	}
	coll.Fields = append(coll.Fields, &fieldSchema5)

	fieldSchema6 := schemapb.FieldSchema{
		Name:        "array-float",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Float,
	}
	coll.Fields = append(coll.Fields, &fieldSchema6)

	fieldSchema7 := schemapb.FieldSchema{
		Name:        "array-double",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Double,
	}
	coll.Fields = append(coll.Fields, &fieldSchema7)

	fieldSchema8 := schemapb.FieldSchema{
		Name:        "array-varchar",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_VarChar,
	}
	coll.Fields = append(coll.Fields, &fieldSchema8)

	return coll
}

func newRowsWithArray(results []map[string]interface{}) []map[string]interface{} {
	for i, result := range results {
		result["array-bool"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{
					Data: []bool{true},
				},
			},
		}
		result["array-int8"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: []int32{0},
				},
			},
		}
		result["array-int16"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: []int32{0},
				},
			},
		}
		result["array-int32"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: []int32{0},
				},
			},
		}
		result["array-int64"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{
					Data: []int64{0},
				},
			},
		}
		result["array-float"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{
					Data: []float32{0},
				},
			},
		}
		result["array-double"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{
					Data: []float64{0},
				},
			},
		}
		result["array-varchar"] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{
					Data: []string{""},
				},
			},
		}
		results[i] = result
	}
	return results
}

func TestArray(t *testing.T) {
	body, _ := generateRequestBody(schemapb.DataType_Int64)
	collectionSchema := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	err, rows, validRows := checkAndSetData(string(body), collectionSchema)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(validRows))
	assert.Equal(t, true, compareRows(rows, generateRawRows(schemapb.DataType_Int64), compareRow))
	data, err := anyToColumns(rows, validRows, collectionSchema, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(collectionSchema.Fields), len(data))

	body, _ = generateRequestBodyWithArray(schemapb.DataType_Int64)
	collectionSchema = newCollectionSchemaWithArray(generateCollectionSchema(schemapb.DataType_Int64, false, true))
	err, rows, validRows = checkAndSetData(string(body), collectionSchema)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(validRows))
	assert.Equal(t, true, compareRows(rows, newRowsWithArray(generateRawRows(schemapb.DataType_Int64)), compareRow))
	data, err = anyToColumns(rows, validRows, collectionSchema, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(collectionSchema.Fields), len(data))
}

func TestVector(t *testing.T) {
	floatVector := "vector-float"
	binaryVector := "vector-binary"
	float16Vector := "vector-float16"
	bfloat16Vector := "vector-bfloat16"
	sparseFloatVector := "vector-sparse-float"
	testcaseRows := []map[string]interface{}{
		{
			FieldBookID:       int64(1),
			floatVector:       []float32{0.1, 0.11},
			binaryVector:      []byte{1},
			float16Vector:     []byte{1, 1, 11, 11},
			bfloat16Vector:    []byte{1, 1, 11, 11},
			sparseFloatVector: map[uint32]float32{0: 0.1, 1: 0.11},
		},
		{
			FieldBookID:       int64(2),
			floatVector:       []float32{0.2, 0.22},
			binaryVector:      []byte{2},
			float16Vector:     []byte{2, 2, 22, 22},
			bfloat16Vector:    []byte{2, 2, 22, 22},
			sparseFloatVector: map[uint32]float32{1000: 0.3, 200: 0.44},
		},
		{
			FieldBookID:       int64(3),
			floatVector:       []float32{0.3, 0.33},
			binaryVector:      []byte{3},
			float16Vector:     []byte{3, 3, 33, 33},
			bfloat16Vector:    []byte{3, 3, 33, 33},
			sparseFloatVector: map[uint32]float32{987621: 32190.31, 32189: 0.0001},
		},
		{
			FieldBookID:       int64(4),
			floatVector:       []float32{0.4, 0.44},
			binaryVector:      []byte{4},
			float16Vector:     []float32{0.4, 0.44},
			bfloat16Vector:    []float32{0.4, 0.44},
			sparseFloatVector: map[uint32]float32{25: 0.1, 1: 0.11},
		},
		{
			FieldBookID:       int64(5),
			floatVector:       []float32{-0.4, -0.44},
			binaryVector:      []byte{5},
			float16Vector:     []int64{99999999, -99999999},
			bfloat16Vector:    []int64{99999999, -99999999},
			sparseFloatVector: map[uint32]float32{1121: 0.1, 3: 0.11},
		},
	}
	body, err := wrapRequestBody(testcaseRows)
	assert.Nil(t, err)
	primaryField := generatePrimaryField(schemapb.DataType_Int64, false)
	floatVectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	floatVectorField.Name = floatVector
	binaryVectorField := generateVectorFieldSchema(schemapb.DataType_BinaryVector)
	binaryVectorField.Name = binaryVector
	float16VectorField := generateVectorFieldSchema(schemapb.DataType_Float16Vector)
	float16VectorField.Name = float16Vector
	bfloat16VectorField := generateVectorFieldSchema(schemapb.DataType_BFloat16Vector)
	bfloat16VectorField.Name = bfloat16Vector
	sparseFloatVectorField := generateVectorFieldSchema(schemapb.DataType_SparseFloatVector)
	sparseFloatVectorField.Name = sparseFloatVector
	collectionSchema := &schemapb.CollectionSchema{
		Name:        DefaultCollectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			primaryField, floatVectorField, binaryVectorField, float16VectorField, bfloat16VectorField, sparseFloatVectorField,
		},
		EnableDynamicField: true,
	}
	err, rows, validRows := checkAndSetData(string(body), collectionSchema)
	assert.Equal(t, nil, err)
	for i, row := range rows {
		assert.Equal(t, 2, len(row[floatVector].([]float32)))
		assert.Equal(t, 1, len(row[binaryVector].([]byte)))
		if fv, ok := testcaseRows[i][float16Vector].([]float32); ok {
			assert.Equal(t, fv, row[float16Vector].([]float32))
		} else if iv, ok := testcaseRows[i][float16Vector].([]int64); ok {
			assert.Equal(t, len(iv), len(row[float16Vector].([]float32)))
		} else {
			assert.Equal(t, 4, len(row[float16Vector].([]byte)))
			assert.Equal(t, testcaseRows[i][float16Vector].([]byte), row[float16Vector].([]byte))
		}
		if fv, ok := testcaseRows[i][bfloat16Vector].([]float32); ok {
			assert.Equal(t, fv, row[float16Vector].([]float32))
		} else if iv, ok := testcaseRows[i][bfloat16Vector].([]int64); ok {
			assert.Equal(t, len(iv), len(row[bfloat16Vector].([]float32)))
		} else {
			assert.Equal(t, 4, len(row[bfloat16Vector].([]byte)))
			assert.Equal(t, testcaseRows[i][bfloat16Vector].([]byte), row[bfloat16Vector].([]byte))
		}
		// all test sparse rows have 2 elements, each should be of 8 bytes
		assert.Equal(t, 16, len(row[sparseFloatVector].([]byte)))
	}
	assert.Equal(t, 0, len(validRows))
	data, err := anyToColumns(rows, validRows, collectionSchema, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(collectionSchema.Fields)+1, len(data))

	assertError := func(field string, value interface{}) {
		row := make(map[string]interface{})
		for k, v := range testcaseRows[0] {
			row[k] = v
		}
		row[field] = value
		body, _ = wrapRequestBody([]map[string]interface{}{row})
		err, _, _ = checkAndSetData(string(body), collectionSchema)
		assert.Error(t, err)
	}

	assertError(binaryVector, []int64{99999999, -99999999})
	assertError(floatVector, []float64{math.MaxFloat64, 0})
	assertError(sparseFloatVector, map[uint32]float32{0: -0.1, 1: 0.11, 2: 0.12})
}

func TestBuildQueryResps(t *testing.T) {
	outputFields := []string{"XXX", "YYY"}
	outputFieldsList := [][]string{outputFields, {"$meta"}, {"$meta", FieldBookID, FieldBookIntro, "YYY"}}
	for _, theOutputFields := range outputFieldsList {
		rows, err := buildQueryResp(int64(0), theOutputFields, newFieldData(generateFieldData(schemapb.DataType_Int64), schemapb.DataType_None), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, 0)
		assert.Equal(t, nil, err)
		exceptRows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
		assert.Equal(t, true, compareRows(rows, exceptRows, compareRow))
	}

	dataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_SparseFloatVector,
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Float, schemapb.DataType_Double,
		schemapb.DataType_String, schemapb.DataType_VarChar,
		schemapb.DataType_JSON, schemapb.DataType_Array,
	}
	for _, dateType := range dataTypes {
		_, err := buildQueryResp(int64(0), outputFields, newFieldData([]*schemapb.FieldData{}, dateType), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, 0)
		assert.Equal(t, nil, err)
	}

	_, err := buildQueryResp(int64(0), outputFields, newFieldData([]*schemapb.FieldData{}, 1000), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, 0)
	assert.Equal(t, "the type(1000) of field(wrong-field-type) is not supported, use other sdk please", err.Error())

	res, err := buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, 0)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	res, err = buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIDs(schemapb.DataType_Int64, 3), DefaultScores, false, 0)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	res, err = buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIDs(schemapb.DataType_VarChar, 3), DefaultScores, true, 0)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	_, err = buildQueryResp(int64(0), outputFields, generateFieldData(schemapb.DataType_Int64), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, false, 0)
	assert.Equal(t, nil, err)

	// len(rows) != len(scores), didn't show distance
	_, err = buildQueryResp(int64(0), outputFields, newFieldData(generateFieldData(schemapb.DataType_Int64), schemapb.DataType_None), generateIDs(schemapb.DataType_Int64, 3), []float32{0.01, 0.04}, true, 0)
	assert.Equal(t, nil, err)
}

func TestConvertConsistencyLevel(t *testing.T) {
	consistencyLevel, useDefaultConsistency, err := convertConsistencyLevel("")
	assert.Equal(t, nil, err)
	assert.Equal(t, consistencyLevel, commonpb.ConsistencyLevel_Bounded)
	assert.Equal(t, true, useDefaultConsistency)
	consistencyLevel, useDefaultConsistency, err = convertConsistencyLevel("Strong")
	assert.Equal(t, nil, err)
	assert.Equal(t, consistencyLevel, commonpb.ConsistencyLevel_Strong)
	assert.Equal(t, false, useDefaultConsistency)
	_, _, err = convertConsistencyLevel("test")
	assert.NotNil(t, err)
}

func TestConvertToExtraParams(t *testing.T) {
	indexParams := IndexParam{
		MetricType: "L2",
		IndexType:  "IVF_FLAT",
		Params: map[string]interface{}{
			"nlist": 128,
		},
	}
	params, err := convertToExtraParams(indexParams)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(params))
	for _, pair := range params {
		if pair.Key == common.MetricTypeKey {
			assert.Equal(t, "L2", pair.Value)
		}
		if pair.Key == common.IndexTypeKey {
			assert.Equal(t, "IVF_FLAT", pair.Value)
		}
		if pair.Key == common.IndexParamsKey {
			assert.Equal(t, string("{\"nlist\":128}"), pair.Value)
		}
	}
}

func TestGenerateExpressionTemplate(t *testing.T) {
	var mixedList []interface{}
	var mixedAns [][]byte

	mixedList = append(mixedList, float64(1))
	mixedList = append(mixedList, "10")
	mixedList = append(mixedList, true)

	val, _ := json.Marshal(1)
	mixedAns = append(mixedAns, val)
	val, _ = json.Marshal("10")
	mixedAns = append(mixedAns, val)
	val, _ = json.Marshal(true)
	mixedAns = append(mixedAns, val)
	// all passed number is float64 type, so all the number type has convert to float64
	expressionTemplates := []map[string]interface{}{
		{"str": "10"},
		{"min": float64(1), "max": float64(10)},
		{"bool": true},
		{"float64": 1.1},
		{"int64": float64(1)},
		{"list_of_str": []interface{}{"1", "10", "100"}},
		{"list_of_bool": []interface{}{true, false, true}},
		{"list_of_float": []interface{}{1.1, 10.1, 100.1}},
		{"list_of_int": []interface{}{float64(1), float64(10), float64(100)}},
		{"list_of_json": mixedList},
	}
	ans := []map[string]*schemapb.TemplateValue{
		{
			"str": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_StringVal{
					StringVal: "10",
				},
			},
		},
		{
			"min": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_Int64Val{
					Int64Val: 1,
				},
			},
			"max": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_Int64Val{
					Int64Val: 10,
				},
			},
		},
		{
			"bool": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_BoolVal{
					BoolVal: true,
				},
			},
		},
		{
			"float64": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_FloatVal{
					FloatVal: 1.1,
				},
			},
		},
		{
			"int64": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_Int64Val{
					Int64Val: 1,
				},
			},
		},
		{
			"list_of_str": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_ArrayVal{
					ArrayVal: &schemapb.TemplateArrayValue{
						Data: &schemapb.TemplateArrayValue_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"1", "10", "100"},
							},
						},
					},
				},
			},
		},
		{
			"list_of_bool": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_ArrayVal{
					ArrayVal: &schemapb.TemplateArrayValue{
						Data: &schemapb.TemplateArrayValue_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true, false, true},
							},
						},
					},
				},
			},
		},
		{
			"list_of_float": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_ArrayVal{
					ArrayVal: &schemapb.TemplateArrayValue{
						Data: &schemapb.TemplateArrayValue_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: []float64{1.1, 10.1, 100.1},
							},
						},
					},
				},
			},
		},
		{
			"list_of_int": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_ArrayVal{
					ArrayVal: &schemapb.TemplateArrayValue{
						Data: &schemapb.TemplateArrayValue_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 10, 100},
							},
						},
					},
				},
			},
		},
		{
			"list_of_json": &schemapb.TemplateValue{
				Val: &schemapb.TemplateValue_ArrayVal{
					ArrayVal: &schemapb.TemplateArrayValue{
						Data: &schemapb.TemplateArrayValue_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: mixedAns,
							},
						},
					},
				},
			},
		},
	}
	for i, template := range expressionTemplates {
		actual := generateExpressionTemplate(template)
		assert.Equal(t, actual, ans[i])
	}
}
