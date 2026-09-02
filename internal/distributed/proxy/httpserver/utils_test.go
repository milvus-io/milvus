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
	"fmt"
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	FieldWordCount = "word_count"
	FieldBookID    = "book_id"
	FieldBookIntro = "book_intro"
	FieldVarchar   = "varchar_field"
	FieldNarrowInt = "narrow_int"
	FieldText      = "text_field"
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

func generateNarrowIntegerCollectionSchema(dataType schemapb.DataType) *schemapb.CollectionSchema {
	schema := generateCollectionSchema(schemapb.DataType_Int64, false, false)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 100,
		Name:     FieldNarrowInt,
		DataType: dataType,
	})
	return schema
}

func hasStructArrayInt64Value(fieldsData []*schemapb.FieldData, structName, subName string, expected int64) bool {
	for _, fieldData := range fieldsData {
		if fieldData.GetFieldName() != structName {
			continue
		}
		for _, subFieldData := range fieldData.GetStructArrays().GetFields() {
			if subFieldData.GetFieldName() != subName {
				continue
			}
			rows := subFieldData.GetScalars().GetArrayData().GetData()
			return len(rows) == 1 && len(rows[0].GetLongData().GetData()) == 1 && rows[0].GetLongData().GetData()[0] == expected
		}
	}
	return false
}

func generateTextCollectionSchema(nullable bool) *schemapb.CollectionSchema {
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 10,
		Name:     FieldText,
		DataType: schemapb.DataType_Text,
		Nullable: nullable,
	})
	return coll
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
	case schemapb.DataType_Int8Vector:
		return schemapb.FieldData{
			Type:      schemapb.DataType_Int8Vector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 2,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: []byte{0x00, 0x1, 0x2, 0x3, 0x4, 0x5},
					},
				},
			},
			IsDynamic: false,
		}
	default:
		panic("unsupported vector type")
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

	fieldData3 := generateVectorFieldData(schemapb.DataType_FloatVector)
	return []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
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
		switch field.DataType {
		case schemapb.DataType_VarChar:
			fields = append(fields, field)
		case schemapb.DataType_Array:
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

	// the ids are carried as a template value, so the expression text no longer
	// contains anything the caller sent
	jsonStr := "{\"id\": [1, 2, 3]}"
	idStr := gjson.Get(jsonStr, "id")
	filter, values, err := checkGetPrimaryKey(coll, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "book_id in {"+primaryKeyTemplateVar+"}", filter)
	assert.Equal(t, []int64{1, 2, 3},
		values[primaryKeyTemplateVar].GetArrayVal().GetLongData().GetData())

	jsonStr = "{\"id\": [\"1\", \"2\", \"3\"]}"
	idStr = gjson.Get(jsonStr, "id")
	coll2 := generateCollectionSchema(schemapb.DataType_VarChar, false, true)
	filter, values, err = checkGetPrimaryKey(coll2, idStr)
	assert.Equal(t, nil, err)
	assert.Equal(t, "book_id in {"+primaryKeyTemplateVar+"}", filter)
	assert.Equal(t, []string{"1", "2", "3"},
		values[primaryKeyTemplateVar].GetArrayVal().GetStringData().GetData())
}

func TestAnyToColumns(t *testing.T) {
	t.Run("insert with dynamic field", func(t *testing.T) {
		body := []byte("{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, json.Number("0"), req.Data[0]["id"]) // a dynamic key keeps its literal
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, true, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, fieldsData[len(fieldsData)-1].IsDynamic)
		assert.Equal(t, schemapb.DataType_JSON, fieldsData[len(fieldsData)-1].Type)
		assert.Equal(t, "{\"classified\":false,\"databaseID\":null,\"id\":0}", string(fieldsData[len(fieldsData)-1].GetScalars().GetJsonData().GetData()[0]))
	})

	t.Run("upsert with dynamic field", func(t *testing.T) {
		body := []byte("{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, json.Number("0"), req.Data[0]["id"]) // a dynamic key keeps its literal
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, false, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, fieldsData[len(fieldsData)-1].IsDynamic)
		assert.Equal(t, schemapb.DataType_JSON, fieldsData[len(fieldsData)-1].Type)
		assert.Equal(t, "{\"classified\":false,\"databaseID\":null,\"id\":0}", string(fieldsData[len(fieldsData)-1].GetScalars().GetJsonData().GetData()[0]))
	})

	t.Run("insert with dynamic field, but pass pk when autoid==true", func(t *testing.T) {
		body := []byte("{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, true)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, json.Number("0"), req.Data[0]["id"]) // a dynamic key keeps its literal
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		_, err = anyToColumns(req.Data, nil, coll, true, false)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "no need to pass pk field"))
	})

	t.Run("insert,autoid==true,allow_insert_auto_id=true", func(t *testing.T) {
		body := []byte("{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, true)
		coll.Properties = append(coll.Properties, &commonpb.KeyValuePair{
			Key:   common.AllowInsertAutoIDKey,
			Value: "true",
		})
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, json.Number("0"), req.Data[0]["id"]) // a dynamic key keeps its literal
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		t.Log(req.Data)
		_, err = anyToColumns(req.Data, nil, coll, true, false)
		assert.NoError(t, err)
	})

	t.Run("pass more field", func(t *testing.T) {
		body := []byte("{\"data\": {\"id\": 0, \"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}")
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		_, _, err = checkAndSetData(body, coll, false)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "has pass more fiel"))
	})

	t.Run("insert with autoid==false", func(t *testing.T) {
		body := []byte("{\"data\": {\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, true, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("insert with autoid==false but has no pk", func(t *testing.T) {
		body := []byte("{\"data\": { \"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
		var err error
		_, _, err = checkAndSetData(body, coll, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterMissing)
		assert.Contains(t, err.Error(), FieldBookID)
	})

	t.Run("insert with varchar pk missing when autoid==false", func(t *testing.T) {
		body := []byte("{\"data\": {\"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		coll := generateCollectionSchema(schemapb.DataType_VarChar, false, false)

		_, _, err := checkAndSetData(body, coll, false)

		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterMissing)
		assert.Contains(t, err.Error(), FieldBookID)
	})

	t.Run("insert with null varchar pk when autoid==false", func(t *testing.T) {
		body := []byte("{\"data\": {\"book_id\": null, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		coll := generateCollectionSchema(schemapb.DataType_VarChar, false, false)

		_, _, err := checkAndSetData(body, coll, false)

		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), FieldBookID)
		assert.Contains(t, err.Error(), "not nullable")
	})

	t.Run("insert with varchar pk missing when autoid==true", func(t *testing.T) {
		body := []byte("{\"data\": {\"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		coll := generateCollectionSchema(schemapb.DataType_VarChar, true, false)

		data, validData, err := checkAndSetData(body, coll, false)

		require.NoError(t, err)
		require.Len(t, data, 1)
		assert.NotContains(t, data[0], FieldBookID)
		assert.Empty(t, validData)

		fieldsData, err := anyToColumns(data, validData, coll, true, false)
		require.NoError(t, err)
		for _, fieldData := range fieldsData {
			assert.NotEqual(t, FieldBookID, fieldData.GetFieldName())
		}
	})

	t.Run("insert with autoid==true", func(t *testing.T) {
		body := []byte("{\"data\": { \"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, true, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("upsert with autoid==true", func(t *testing.T) {
		body := []byte("{\"data\": {\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, false, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("upsert with autoid==false", func(t *testing.T) {
		body := []byte("{\"data\": {\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2}}")
		req := InsertReq{}
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		var err error
		req.Data, _, err = checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, int64(1), req.Data[0]["book_id"])
		assert.Equal(t, []float32{0.1, 0.2}, req.Data[0]["book_intro"])
		assert.Equal(t, int64(2), req.Data[0]["word_count"])
		fieldsData, err := anyToColumns(req.Data, nil, coll, false, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 3, len(fieldsData))
		assert.Equal(t, false, fieldsData[len(fieldsData)-1].IsDynamic)
	})

	t.Run("partial update with inconsistent fields should fail", func(t *testing.T) {
		// Create a simple schema with two fields: a and b
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					AutoID:       false,
				},
				{
					FieldID:  101,
					Name:     "a",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  102,
					Name:     "b",
					DataType: schemapb.DataType_Int64,
				},
			},
			EnableDynamicField: false,
		}

		// Create two rows: first row updates only field 'a', second row updates only field 'b'
		rows := []map[string]interface{}{
			{
				"id": int64(1),
				"a":  int64(100), // Only field 'a' is provided
			},
			{
				"id": int64(2),
				"b":  int64(200), // Only field 'b' is provided
			},
		}

		// Test with partial update = true, this should fail
		// because different rows are updating different fields
		_, err := anyToColumns(rows, nil, schema, false, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has length 1, expected 2")
	})

	t.Run("partial update with consistent missing fields should succeed", func(t *testing.T) {
		// Create a simple schema with two fields: a and b
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					AutoID:       false,
				},
				{
					FieldID:  101,
					Name:     "a",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  102,
					Name:     "b",
					DataType: schemapb.DataType_Int64,
					Nullable: true, // Make field 'b' nullable
				},
			},
			EnableDynamicField: false,
		}

		// Create two rows: both rows update only field 'a', field 'b' is missing in both
		rows := []map[string]interface{}{
			{
				"id": int64(1),
				"a":  int64(100), // Only field 'a' is provided
			},
			{
				"id": int64(2),
				"a":  int64(200), // Only field 'a' is provided
			},
		}

		// Test with partial update = true, this should succeed
		// because the same fields are being updated in all rows
		fieldsData, err := anyToColumns(rows, nil, schema, false, true)
		assert.NoError(t, err)
		assert.NotNil(t, fieldsData)

		// Should have id and a fields, but not b (since it's not provided and nullable)
		fieldNames := make(map[string]bool)
		for _, fd := range fieldsData {
			fieldNames[fd.FieldName] = true
		}
		assert.True(t, fieldNames["id"])
		assert.True(t, fieldNames["a"])
		// Field 'b' should not be present since it wasn't provided in any row
		assert.False(t, fieldNames["b"])
	})

	t.Run("function output field not provided in any row", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					Name:     "vec",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "2"},
					},
				},
				{
					FieldID:          102,
					Name:             "fn_out",
					DataType:         schemapb.DataType_Int64,
					IsFunctionOutput: true,
				},
			},
		}
		rows := []map[string]interface{}{
			{"id": int64(1), "vec": []float32{0.1, 0.2}},
			{"id": int64(2), "vec": []float32{0.3, 0.4}},
		}
		fieldsData, err := anyToColumns(rows, nil, schema, true, false)
		assert.NoError(t, err)
		fieldNames := make(map[string]bool)
		for _, fd := range fieldsData {
			fieldNames[fd.FieldName] = true
		}
		assert.True(t, fieldNames["id"])
		assert.True(t, fieldNames["vec"])
		assert.False(t, fieldNames["fn_out"])
	})

	t.Run("function output field provided in all rows", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					Name:     "vec",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "2"},
					},
				},
				{
					FieldID:          102,
					Name:             "fn_out",
					DataType:         schemapb.DataType_Int64,
					IsFunctionOutput: true,
				},
			},
		}
		rows := []map[string]interface{}{
			{"id": int64(1), "vec": []float32{0.1, 0.2}, "fn_out": int64(10)},
			{"id": int64(2), "vec": []float32{0.3, 0.4}, "fn_out": int64(20)},
		}
		fieldsData, err := anyToColumns(rows, nil, schema, true, false)
		assert.NoError(t, err)
		fieldNames := make(map[string]bool)
		for _, fd := range fieldsData {
			fieldNames[fd.FieldName] = true
		}
		assert.True(t, fieldNames["fn_out"])
	})

	t.Run("function output field provided in row 0 but missing in later row", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					Name:     "vec",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "2"},
					},
				},
				{
					FieldID:          102,
					Name:             "fn_out",
					DataType:         schemapb.DataType_Int64,
					IsFunctionOutput: true,
				},
			},
		}
		rows := []map[string]interface{}{
			{"id": int64(1), "vec": []float32{0.1, 0.2}, "fn_out": int64(10)},
			{"id": int64(2), "vec": []float32{0.3, 0.4}}, // fn_out missing
		}
		_, err := anyToColumns(rows, nil, schema, true, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not has field fn_out")
	})

	t.Run("function output field missing in row 0 but provided in later row", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					Name:     "vec",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "2"},
					},
				},
				{
					FieldID:          102,
					Name:             "fn_out",
					DataType:         schemapb.DataType_Int64,
					IsFunctionOutput: true,
				},
			},
		}
		rows := []map[string]interface{}{
			{"id": int64(1), "vec": []float32{0.1, 0.2}},                      // fn_out missing
			{"id": int64(2), "vec": []float32{0.3, 0.4}, "fn_out": int64(20)}, // fn_out provided
		}
		// row 0 doesn't have fn_out but row 1 does, column is allocated,
		// so row 0 hits the "does not has field" error
		_, err := anyToColumns(rows, nil, schema, true, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not has field fn_out")
	})
}

func TestCheckAndSetData(t *testing.T) {
	t.Run("integer range validation", func(t *testing.T) {
		tests := []struct {
			name              string
			dataType          schemapb.DataType
			min               int64
			max               int64
			invalidValues     []int64
			invalidJSONValues []string
		}{
			{
				name: "int8", dataType: schemapb.DataType_Int8, min: math.MinInt8, max: math.MaxInt8,
				invalidValues: []int64{-129, 128, 200, 256, 1000, 65535},
				invalidJSONValues: []string{
					"127.00000000000000000000000000001",
					"-128.00000000000000000000000000001",
					"1e-999999",
				},
			},
			{
				name: "int16", dataType: schemapb.DataType_Int16, min: math.MinInt16, max: math.MaxInt16,
				invalidValues: []int64{-32769, 32768, 65535, 100000},
				invalidJSONValues: []string{
					"32767.00000000000000000000000000001",
					"-32768.00000000000000000000000000001",
					"1e-999999",
				},
			},
			{
				name: "int32", dataType: schemapb.DataType_Int32, min: math.MinInt32, max: math.MaxInt32,
				invalidValues: []int64{-2147483649, 2147483648, 4294967295},
				invalidJSONValues: []string{
					"2147483647.00000000000000000001",
					"-2147483648.00000000000000000001",
					"1e-999999",
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				schema := generateNarrowIntegerCollectionSchema(test.dataType)

				for _, value := range []int64{test.min, test.max} {
					literal := strconv.FormatInt(value, 10)
					for _, valueJSON := range []string{literal, strconv.Quote(literal)} {
						body := []byte(fmt.Sprintf(`{"data":[{"book_id":1,"book_intro":[0.1,0.2],"word_count":2,"%s":%s}]}`, FieldNarrowInt, valueJSON))
						rows, validData, err := checkAndSetData(body, schema, false)
						require.NoError(t, err)
						require.Len(t, rows, 1)
						assert.EqualValues(t, value, rows[0][FieldNarrowInt])

						fieldsData, err := anyToColumns(rows, validData, schema, true, false)
						require.NoError(t, err)
						var narrowFieldData *schemapb.FieldData
						for _, fieldData := range fieldsData {
							if fieldData.GetFieldName() == FieldNarrowInt {
								narrowFieldData = fieldData
								break
							}
						}
						require.NotNil(t, narrowFieldData)
						assert.Equal(t, []int32{int32(value)}, narrowFieldData.GetScalars().GetIntData().GetData())
					}
				}

				for _, input := range []struct {
					raw      string
					expected int64
				}{
					{raw: "100.0", expected: 100},
					{raw: "1e2", expected: 100},
					{raw: strconv.Quote("100"), expected: 100},
				} {
					body := []byte(fmt.Sprintf(`{"data":[{"book_id":1,"book_intro":[0.1,0.2],"word_count":2,"%s":%s}]}`, FieldNarrowInt, input.raw))
					rows, _, err := checkAndSetData(body, schema, false)
					require.NoError(t, err)
					require.Len(t, rows, 1)
					assert.EqualValues(t, input.expected, rows[0][FieldNarrowInt])
				}

				for _, value := range test.invalidValues {
					literal := strconv.FormatInt(value, 10)
					for _, valueJSON := range []string{literal, strconv.Quote(literal)} {
						body := []byte(fmt.Sprintf(`{"data":[{"book_id":1,"book_intro":[0.1,0.2],"word_count":2,"%s":%s}]}`, FieldNarrowInt, valueJSON))
						_, _, err := checkAndSetData(body, schema, false)
						require.Error(t, err)
						assert.ErrorIs(t, err, merr.ErrParameterInvalid)
						assert.Contains(t, err.Error(), fmt.Sprintf("actual=%d", value))
						assert.Contains(t, err.Error(), FieldNarrowInt)
						assert.Contains(t, err.Error(), fmt.Sprintf("[%d, %d]", test.min, test.max))
					}
				}

				for _, valueJSON := range test.invalidJSONValues {
					body := []byte(fmt.Sprintf(`{"data":[{"book_id":1,"book_intro":[0.1,0.2],"word_count":2,"%s":%s}]}`, FieldNarrowInt, valueJSON))
					_, _, err := checkAndSetData(body, schema, false)
					require.Error(t, err)
					assert.ErrorIs(t, err, merr.ErrParameterInvalid)
					assert.Contains(t, err.Error(), "actual="+valueJSON)
					assert.Contains(t, err.Error(), FieldNarrowInt)
					assert.Contains(t, err.Error(), fmt.Sprintf("[%d, %d]", test.min, test.max))
				}
			})
		}
	})

	t.Run("invalid field name with dynamic field", func(t *testing.T) {
		body := []byte("{\"data\": {\"id\": 0,\"$meta\": 2,\"book_id\": 1, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"classified\": false, \"databaseID\": null}}")
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		var err error
		_, _, err = checkAndSetData(body, coll, false)
		assert.Error(t, err)
		assert.Equal(t, true, strings.HasPrefix(err.Error(), "use the invalid field name"))
	})
	t.Run("without vector", func(t *testing.T) {
		body := []byte("{\"data\": {}}")
		primaryField := generatePrimaryField(schemapb.DataType_Int64, true)
		floatVectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		floatVectorField.Name = "floatVector"
		binaryVectorField := generateVectorFieldSchema(schemapb.DataType_BinaryVector)
		binaryVectorField.Name = "binaryVector"
		float16VectorField := generateVectorFieldSchema(schemapb.DataType_Float16Vector)
		float16VectorField.Name = "float16Vector"
		bfloat16VectorField := generateVectorFieldSchema(schemapb.DataType_BFloat16Vector)
		bfloat16VectorField.Name = "bfloat16Vector"
		int8VectorField := generateVectorFieldSchema(schemapb.DataType_Int8Vector)
		int8VectorField.Name = "int8Vector"

		for _, vectorField := range []*schemapb.FieldSchema{
			floatVectorField,
			binaryVectorField,
			float16VectorField,
			bfloat16VectorField,
			int8VectorField,
		} {
			_, _, err := checkAndSetData(body, &schemapb.CollectionSchema{
				Name: DefaultCollectionName,
				Fields: []*schemapb.FieldSchema{
					primaryField, vectorField,
				},
				EnableDynamicField: true,
			}, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterMissing)
			assert.Contains(t, err.Error(), vectorField.GetName())
		}
	})

	t.Run("with pk when autoID == True when upsert", func(t *testing.T) {
		arrayFieldName := "array-int64"
		body := []byte("{\"data\": {\"book_id\": 9999999999999999, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}")
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        arrayFieldName,
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		})
		data, validData, err := checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(data))
		assert.Equal(t, 0, len(validData))
	})

	t.Run("without pk when autoID == True when insert", func(t *testing.T) {
		arrayFieldName := "array-int64"
		body := []byte("{\"data\": {\"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}")
		coll := generateCollectionSchema(schemapb.DataType_Int64, true, false)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        arrayFieldName,
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		})
		data, validData, err := checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(data))
		assert.Equal(t, 0, len(validData))
	})

	t.Run("with pk when autoID == false", func(t *testing.T) {
		arrayFieldName := "array-int64"
		body := []byte("{\"data\": {\"book_id\": 9999999999999999, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}")
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, false)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        arrayFieldName,
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		})
		data, validData, err := checkAndSetData(body, coll, false)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(data))
		assert.Equal(t, 0, len(validData))
	})
}

func TestInsertWithInt64(t *testing.T) {
	arrayFieldName := "array-int64"
	body := []byte("{\"data\": {\"book_id\": 9999999999999999, \"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}}")
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:        arrayFieldName,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	})
	data, validData, err := checkAndSetData(body, coll, false)
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
	body := []byte("{\"data\": [{\"book_id\": 9999999999999999, \"\nullable\": null,\"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]},{\"book_id\": 1, \"nullable\": 1,\"book_intro\": [0.3, 0.4], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}]")
	data, validData, err := checkAndSetData(body, coll, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(data))
	assert.Equal(t, 1, len(validData))
	assert.Equal(t, 2, len(validData["nullable"]))
	assert.False(t, validData["nullable"][0])
	assert.True(t, validData["nullable"][1])
	assert.Equal(t, int64(9999999999999999), data[0][FieldBookID])
	arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
	assert.Equal(t, int64(9999999999999999), arr.GetLongData().GetData()[0])
	// the row carries an explicit null for a dynamic key, which used to be dropped
	assert.Equal(t, 5, len(data[0]))
	assert.Equal(t, 5, len(data[1]))

	fieldData, err := anyToColumns(data, validData, coll, true, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(coll.Fields), len(fieldData))
}

func TestTextFieldDMLConversion(t *testing.T) {
	longText := strings.Repeat("x", 64*1024+1)
	coll := generateTextCollectionSchema(true)
	body, err := wrapRequestBody([]map[string]interface{}{
		{FieldBookID: int64(1), FieldWordCount: int64(10), FieldBookIntro: []float32{0.1, 0.2}, FieldText: "short text"},
		{FieldBookID: int64(2), FieldWordCount: int64(20), FieldBookIntro: []float32{0.3, 0.4}, FieldText: nil},
		{FieldBookID: int64(3), FieldWordCount: int64(30), FieldBookIntro: []float32{0.5, 0.6}, FieldText: longText},
	})
	require.NoError(t, err)

	rows, validData, err := checkAndSetData(body, coll, false)
	require.NoError(t, err)
	assert.Equal(t, "short text", rows[0][FieldText])
	assert.NotContains(t, rows[1], FieldText)
	assert.Equal(t, longText, rows[2][FieldText])
	assert.Equal(t, []bool{true, false, true}, validData[FieldText])

	for _, testcase := range []struct {
		name     string
		inInsert bool
	}{
		{name: "insert", inInsert: true},
		{name: "upsert", inInsert: false},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			fieldsData, err := anyToColumns(rows, validData, coll, testcase.inInsert, false)
			require.NoError(t, err)

			textFieldData := getFieldDataByName(fieldsData, FieldText)
			require.NotNil(t, textFieldData)
			assert.Equal(t, schemapb.DataType_Text, textFieldData.GetType())
			assert.Equal(t, []string{"short text", longText}, textFieldData.GetScalars().GetStringData().GetData())
			assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(textFieldData))
		})
	}
}

func TestTextFieldRESTInputValidation(t *testing.T) {
	coll := generateTextCollectionSchema(false)
	newRow := func() map[string]interface{} {
		return map[string]interface{}{
			FieldBookID:    int64(1),
			FieldWordCount: int64(10),
			FieldBookIntro: []float32{0.1, 0.2},
		}
	}

	t.Run("missing required text", func(t *testing.T) {
		body, err := wrapRequestBody([]map[string]interface{}{newRow()})
		require.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, false)
		require.NoError(t, err)
		require.NotContains(t, rows[0], FieldText)

		_, err = anyToColumns(rows, validData, coll, true, false)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "does not has field "+FieldText)
	})

	for _, testcase := range []struct {
		name  string
		value interface{}
	}{
		{name: "null", value: nil},
		{name: "number", value: 123},
		{name: "boolean", value: true},
	} {
		t.Run("reject "+testcase.name, func(t *testing.T) {
			row := newRow()
			row[FieldText] = testcase.value
			body, err := wrapRequestBody([]map[string]interface{}{row})
			require.NoError(t, err)

			_, _, err = checkAndSetData(body, coll, false)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}

	t.Run("accept empty string", func(t *testing.T) {
		row := newRow()
		row[FieldText] = ""
		body, err := wrapRequestBody([]map[string]interface{}{row})
		require.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, false)
		require.NoError(t, err)
		assert.Equal(t, "", rows[0][FieldText])

		fieldsData, err := anyToColumns(rows, validData, coll, true, false)
		require.NoError(t, err)
		textFieldData := getFieldDataByName(fieldsData, FieldText)
		require.NotNil(t, textFieldData)
		assert.Equal(t, []string{""}, textFieldData.GetScalars().GetStringData().GetData())
	})
}

func TestInsertWithNullableVectorFields(t *testing.T) {
	testcases := []struct {
		name        string
		dataType    schemapb.DataType
		vectorValue interface{}
		checkData   func(*testing.T, *schemapb.FieldData)
	}{
		{
			name:        "float vector",
			dataType:    schemapb.DataType_FloatVector,
			vectorValue: []float32{0.1, 0.2},
			checkData: func(t *testing.T, fieldData *schemapb.FieldData) {
				assert.Equal(t, []float32{0.1, 0.2}, fieldData.GetVectors().GetFloatVector().GetData())
			},
		},
		{
			name:        "binary vector",
			dataType:    schemapb.DataType_BinaryVector,
			vectorValue: []byte{1},
			checkData: func(t *testing.T, fieldData *schemapb.FieldData) {
				assert.Equal(t, []byte{1}, fieldData.GetVectors().GetBinaryVector())
			},
		},
		{
			name:        "float16 vector",
			dataType:    schemapb.DataType_Float16Vector,
			vectorValue: []float32{0.1, 0.2},
			checkData: func(t *testing.T, fieldData *schemapb.FieldData) {
				assert.Len(t, fieldData.GetVectors().GetFloat16Vector(), 4)
			},
		},
		{
			name:        "bfloat16 vector",
			dataType:    schemapb.DataType_BFloat16Vector,
			vectorValue: []float32{0.1, 0.2},
			checkData: func(t *testing.T, fieldData *schemapb.FieldData) {
				assert.Len(t, fieldData.GetVectors().GetBfloat16Vector(), 4)
			},
		},
		{
			name:        "sparse float vector",
			dataType:    schemapb.DataType_SparseFloatVector,
			vectorValue: map[uint32]float32{1: 0.1, 2: 0.2},
			checkData: func(t *testing.T, fieldData *schemapb.FieldData) {
				assert.Len(t, fieldData.GetVectors().GetSparseFloatVector().GetContents(), 1)
			},
		},
		{
			name:        "int8 vector",
			dataType:    schemapb.DataType_Int8Vector,
			vectorValue: []int8{1, 2},
			checkData: func(t *testing.T, fieldData *schemapb.FieldData) {
				assert.Equal(t, []byte{1, 2}, fieldData.GetVectors().GetInt8Vector())
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			primaryField := generatePrimaryField(schemapb.DataType_Int64, false)
			vectorField := generateVectorFieldSchema(testcase.dataType)
			vectorField.Name = FieldBookIntro
			vectorField.Nullable = true
			coll := &schemapb.CollectionSchema{
				Name: DefaultCollectionName,
				Fields: []*schemapb.FieldSchema{
					primaryField,
					vectorField,
				},
			}
			body, err := wrapRequestBody([]map[string]interface{}{
				{FieldBookID: int64(1), FieldBookIntro: nil},
				{FieldBookID: int64(2), FieldBookIntro: testcase.vectorValue},
				{FieldBookID: int64(3), FieldBookIntro: nil},
			})
			assert.NoError(t, err)

			rows, validData, err := checkAndSetData(body, coll, false)
			assert.NoError(t, err)
			assert.Equal(t, []bool{false, true, false}, validData[FieldBookIntro])

			fieldsData, err := anyToColumns(rows, validData, coll, true, false)
			assert.NoError(t, err)

			var vectorFieldData *schemapb.FieldData
			for _, fieldData := range fieldsData {
				if fieldData.GetFieldName() == FieldBookIntro {
					vectorFieldData = fieldData
					break
				}
			}
			assert.NotNil(t, vectorFieldData)
			assert.Equal(t, []bool{false, true, false}, typeutil.GetFieldDataValidData(vectorFieldData))
			testcase.checkData(t, vectorFieldData)
		})
	}
}

func getFieldDataByName(fieldsData []*schemapb.FieldData, fieldName string) *schemapb.FieldData {
	for _, fieldData := range fieldsData {
		if fieldData.GetFieldName() == fieldName {
			return fieldData
		}
	}
	return nil
}

func TestPartialUpdateWithNullableExplicitNull(t *testing.T) {
	t.Run("nullable scalar all null is kept as update", func(t *testing.T) {
		coll := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				{
					Name:     "nullable",
					FieldID:  common.StartOfUserFieldID + 1,
					DataType: schemapb.DataType_Int64,
					Nullable: true,
				},
			},
		}
		body, err := wrapRequestBody([]map[string]interface{}{
			{FieldBookID: int64(1), "nullable": nil},
			{FieldBookID: int64(2), "nullable": nil},
		})
		assert.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, true)
		assert.NoError(t, err)
		assert.Equal(t, []bool{false, false}, validData["nullable"])

		fieldsData, err := anyToColumns(rows, validData, coll, false, true)
		assert.NoError(t, err)
		nullableField := getFieldDataByName(fieldsData, "nullable")
		assert.NotNil(t, nullableField)
		assert.Equal(t, []bool{false, false}, typeutil.GetFieldDataValidData(nullableField))
		assert.Empty(t, nullableField.GetScalars().GetLongData().GetData())
	})

	t.Run("nullable scalar mixed null and value is kept as compact update", func(t *testing.T) {
		coll := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				{
					Name:     "nullable",
					FieldID:  common.StartOfUserFieldID + 1,
					DataType: schemapb.DataType_Int64,
					Nullable: true,
				},
			},
		}
		body, err := wrapRequestBody([]map[string]interface{}{
			{FieldBookID: int64(1), "nullable": nil},
			{FieldBookID: int64(2), "nullable": int64(20)},
		})
		assert.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, true)
		assert.NoError(t, err)
		assert.Equal(t, []bool{false, true}, validData["nullable"])

		fieldsData, err := anyToColumns(rows, validData, coll, false, true)
		assert.NoError(t, err)
		nullableField := getFieldDataByName(fieldsData, "nullable")
		assert.NotNil(t, nullableField)
		assert.Equal(t, []bool{false, true}, typeutil.GetFieldDataValidData(nullableField))
		assert.Equal(t, []int64{20}, nullableField.GetScalars().GetLongData().GetData())
	})

	t.Run("missing nullable field is skipped for partial update", func(t *testing.T) {
		coll := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				{
					Name:     "nullable",
					FieldID:  common.StartOfUserFieldID + 1,
					DataType: schemapb.DataType_Int64,
					Nullable: true,
				},
			},
		}
		body, err := wrapRequestBody([]map[string]interface{}{
			{FieldBookID: int64(1)},
			{FieldBookID: int64(2)},
		})
		assert.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, true)
		assert.NoError(t, err)
		assert.NotContains(t, validData, "nullable")

		fieldsData, err := anyToColumns(rows, validData, coll, false, true)
		assert.NoError(t, err)
		assert.Nil(t, getFieldDataByName(fieldsData, "nullable"))
	})

	t.Run("mixed missing and null nullable field is rejected for partial update", func(t *testing.T) {
		coll := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				{
					Name:     "nullable",
					FieldID:  common.StartOfUserFieldID + 1,
					DataType: schemapb.DataType_Int64,
					Nullable: true,
				},
			},
		}
		body, err := wrapRequestBody([]map[string]interface{}{
			{FieldBookID: int64(1)},
			{FieldBookID: int64(2), "nullable": nil},
		})
		assert.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, true)
		assert.NoError(t, err)
		_, err = anyToColumns(rows, validData, coll, false, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column nullable has length 1, expected 2")
	})

	t.Run("nullable vector all null is kept as update", func(t *testing.T) {
		vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		vectorField.Name = FieldBookIntro
		vectorField.Nullable = true
		coll := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				vectorField,
			},
		}
		body, err := wrapRequestBody([]map[string]interface{}{
			{FieldBookID: int64(1), FieldBookIntro: nil},
			{FieldBookID: int64(2), FieldBookIntro: nil},
		})
		assert.NoError(t, err)

		rows, validData, err := checkAndSetData(body, coll, true)
		assert.NoError(t, err)
		assert.Equal(t, []bool{false, false}, validData[FieldBookIntro])

		fieldsData, err := anyToColumns(rows, validData, coll, false, true)
		assert.NoError(t, err)
		vectorFieldData := getFieldDataByName(fieldsData, FieldBookIntro)
		assert.NotNil(t, vectorFieldData)
		assert.Equal(t, []bool{false, false}, typeutil.GetFieldDataValidData(vectorFieldData))
		assert.Empty(t, vectorFieldData.GetVectors().GetFloatVector().GetData())
		assert.Equal(t, int64(2), vectorFieldData.GetVectors().GetDim())
	})
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
	body := []byte("{\"data\": [{\"book_id\": 9999999999999999, \"\fid\": null,\"book_intro\": [0.1, 0.2], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]},{\"book_id\": 1, \"fid\": 1,\"book_intro\": [0.3, 0.4], \"word_count\": 2, \"" + arrayFieldName + "\": [9999999999999999]}]")
	data, validData, err := checkAndSetData(body, coll, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(data))
	assert.Equal(t, 1, len(validData))
	assert.Equal(t, 2, len(validData["fid"]))
	assert.False(t, validData["fid"][0])
	assert.True(t, validData["fid"][1])
	assert.Equal(t, int64(9999999999999999), data[0][FieldBookID])
	arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
	assert.Equal(t, int64(9999999999999999), arr.GetLongData().GetData()[0])
	// the row carries an explicit null for a dynamic key, which used to be dropped
	assert.Equal(t, 5, len(data[0]))
	assert.Equal(t, 5, len(data[1]))

	fieldData, err := anyToColumns(data, validData, coll, true, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(coll.Fields), len(fieldData))
}

// Without Accept-Type-Allow-Int64 the response renders Int64 array elements as
// strings, so the insert path has to read that form back: a row this API emits
// must be acceptable to it unchanged.
func TestInsertQuotedInt64ArrayElements(t *testing.T) {
	arrayFieldName := "array-int64"
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:        arrayFieldName,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	})

	row := func(value string) []byte {
		return []byte(`{"data": [{"book_id": 1, "book_intro": [0.1, 0.2], "word_count": 2, "` +
			arrayFieldName + `": ` + value + `}]}`)
	}

	t.Run("quoted elements round-trip", func(t *testing.T) {
		// 9007199254740993 is the first integer a float64 cannot hold, which is
		// why the response quotes it in the first place.
		data, _, err := checkAndSetData(row(`["9007199254740993", "-1", "010"]`), coll, false)
		require.NoError(t, err)
		arr, ok := data[0][arrayFieldName].(*schemapb.ScalarField)
		require.True(t, ok)
		// "010" reads as decimal 10, matching how a quoted top-level Int64 is read.
		assert.Equal(t, []int64{9007199254740993, -1, 10}, arr.GetLongData().GetData())
	})

	t.Run("native elements still work", func(t *testing.T) {
		data, _, err := checkAndSetData(row(`[9007199254740993]`), coll, false)
		require.NoError(t, err)
		arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
		assert.Equal(t, []int64{9007199254740993}, arr.GetLongData().GetData())
	})

	t.Run("the two forms may be mixed", func(t *testing.T) {
		// Each element is read on its own, the way each row of a plain Int64
		// column is: nothing about one element decides how the next is read.
		data, _, err := checkAndSetData(row(`[9007199254740993, "1"]`), coll, false)
		require.NoError(t, err)
		arr, _ := data[0][arrayFieldName].(*schemapb.ScalarField)
		assert.Equal(t, []int64{9007199254740993, 1}, arr.GetLongData().GetData())
	})

	// Accepting the quoted form does not accept nonsense in it.
	for _, value := range []string{
		`["9223372036854775808"]`, // out of range
		`["1.5"]`,
		`["abc"]`,
		`[""]`,
		`[{}]`,
	} {
		t.Run("rejects "+value, func(t *testing.T) {
			_, _, err := checkAndSetData(row(value), coll, false)
			assert.Error(t, err)
		})
	}
}

// An array element accepts the same quoted forms the same type accepts as a
// plain column, and a struct array's sub-field accepts exactly what a top-level
// Array column accepts -- the two read through one function, and this pins that
// they cannot drift apart.
func TestQuotedArrayElementsMatchPlainColumns(t *testing.T) {
	for _, tc := range []struct {
		elementType schemapb.DataType
		quoted      string
		want        interface{}
		rejects     []string
	}{
		{schemapb.DataType_Bool, `["true", "false"]`, []bool{true, false}, []string{`["yes please"]`, `[{}]`}},
		{schemapb.DataType_Int8, `["-8", "127"]`, []int32{-8, 127}, []string{`["128"]`, `["1.5"]`, `["abc"]`}},
		{schemapb.DataType_Int16, `["-16", "32767"]`, []int32{-16, 32767}, []string{`["32768"]`, `["abc"]`}},
		{schemapb.DataType_Int32, `["-32", "2147483647"]`, []int32{-32, 2147483647}, []string{`["2147483648"]`, `["abc"]`}},
		{schemapb.DataType_Int64, `["9007199254740993", "010"]`, []int64{9007199254740993, 10}, []string{`["9223372036854775808"]`, `["abc"]`, `[""]`}},
		// strconv reads NaN/Inf/Infinity without an error, case-insensitively,
		// and an array element has no later check that would catch them.
		{schemapb.DataType_Float, `["1.5", "-2"]`, []float32{1.5, -2}, []string{
			`["3.5e38"]`, `["abc"]`, `["NaN"]`, `["nan"]`, `["Inf"]`, `["+Inf"]`, `["-inf"]`, `["Infinity"]`,
		}},
		{schemapb.DataType_Double, `["1.5", "-2"]`, []float64{1.5, -2}, []string{
			`["abc"]`, `["1e400"]`, `[1e400]`, `["NaN"]`, `["Inf"]`, `["-Infinity"]`,
		}},
	} {
		t.Run(tc.elementType.String(), func(t *testing.T) {
			read := func(scalar *schemapb.ScalarField) interface{} {
				switch tc.elementType {
				case schemapb.DataType_Bool:
					return scalar.GetBoolData().GetData()
				case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
					return scalar.GetIntData().GetData()
				case schemapb.DataType_Int64:
					return scalar.GetLongData().GetData()
				case schemapb.DataType_Float:
					return scalar.GetFloatData().GetData()
				default:
					return scalar.GetDoubleData().GetData()
				}
			}

			// The top-level Array column.
			coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
			coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
				Name:        "arr",
				DataType:    schemapb.DataType_Array,
				ElementType: tc.elementType,
			})
			column := func(value string) ([]map[string]interface{}, error) {
				body := []byte(`{"data": [{"book_id": 1, "book_intro": [0.1, 0.2], "word_count": 2, "arr": ` + value + `}]}`)
				data, _, err := checkAndSetData(body, coll, false)
				return data, err
			}

			// The same array as a struct sub-field.
			sub := &schemapb.FieldSchema{Name: "arr", DataType: schemapb.DataType_Array, ElementType: tc.elementType}

			data, err := column(tc.quoted)
			require.NoError(t, err)
			scalar, ok := data[0]["arr"].(*schemapb.ScalarField)
			require.True(t, ok)
			assert.Equal(t, tc.want, read(scalar))

			subScalar, err := buildStructSubArrayScalar(sub, gjson.Parse(tc.quoted).Array(), false)
			require.NoError(t, err)
			assert.Equal(t, tc.want, read(subScalar))

			for _, bad := range tc.rejects {
				_, err := column(bad)
				assert.Error(t, err, "column %s", bad)
				_, err = buildStructSubArrayScalar(sub, gjson.Parse(bad).Array(), false)
				assert.Error(t, err, "sub-field %s", bad)
			}
		})
	}
}

// compatibilityMode restores the value handling of the releases before the REST
// insert validation work: it decides whether a number that does not denote a
// value of the element type is converted anyway. It says nothing about how a
// value is spelled, so the quoted spelling -- which this API itself emits for an
// Int64 without Accept-Type-Allow-Int64, in this mode too -- stays readable, and
// the wrapping conversions do not spread to a column that never had them.
func TestCompatibilityModeChangesValuesNotSpellings(t *testing.T) {
	params := paramtable.Get()
	key := params.HTTPCfg.CompatibilityMode.Key
	params.Save(key, "true")
	defer params.Reset(key)

	column := func(elementType schemapb.DataType, value string) error {
		coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
		coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
			Name:        "arr",
			DataType:    schemapb.DataType_Array,
			ElementType: elementType,
		})
		body := []byte(`{"data": [{"book_id": 1, "book_intro": [0.1, 0.2], "word_count": 2, "arr": ` + value + `}]}`)
		_, _, err := checkAndSetData(body, coll, false)
		return err
	}

	// A column never converted a number it could not represent, and still does
	// not: gjson would have stored 2 for 2.7, a wrapped numeral for one past
	// int64, and +Inf for a magnitude past float32.
	for _, tc := range []struct {
		name        string
		elementType schemapb.DataType
		value       string
	}{
		{"fraction into an integer", schemapb.DataType_Int32, `[1, 2.7, 3]`},
		{"numeral past int64", schemapb.DataType_Int64, `[1, 99999999999999999999]`},
		{"magnitude past float32", schemapb.DataType_Float, `[1.0, 1e50, 2.0]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, column(tc.elementType, tc.value))
		})
	}

	// The spelling this mode's own responses use is still readable.
	t.Run("the quoted spelling round-trips", func(t *testing.T) {
		assert.NoError(t, column(schemapb.DataType_Int64, `["9007199254740993"]`))
	})

	sub := &schemapb.FieldSchema{Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32}

	// The struct sub-field keeps the lenient conversions it has always had --
	// this is the one place they ever lived.
	scalar, err := buildStructSubArrayScalar(sub, gjson.Parse(`[1, 2.7, 3]`).Array(), true)
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2, 3}, scalar.GetIntData().GetData())

	// ...and reads the quoted spelling here too, on the same grounds.
	scalar, err = buildStructSubArrayScalar(sub, gjson.Parse(`["1", "2"]`).Array(), true)
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2}, scalar.GetIntData().GetData())
}

// A rejected array says which element was wrong, not just that the value was.
func TestArrayElementErrorNamesTheElement(t *testing.T) {
	coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:        "arr",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int8,
	})
	body := []byte(`{"data": [{"book_id": 1, "book_intro": [0.1, 0.2], "word_count": 2, "arr": [1, "abc", 3]}]}`)
	_, _, err := checkAndSetData(body, coll, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "element 1")
	assert.Contains(t, err.Error(), "[-128, 127]")
}

// Same convention for the Int64 sub-field of a struct array.
func TestStructSubArrayQuotedInt64(t *testing.T) {
	sub := &schemapb.FieldSchema{
		Name:        "scores",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
	}

	scalar, err := buildStructSubArrayScalar(sub, gjson.Parse(`["9007199254740993", "010"]`).Array(), false)
	require.NoError(t, err)
	assert.Equal(t, []int64{9007199254740993, 10}, scalar.GetLongData().GetData())

	scalar, err = buildStructSubArrayScalar(sub, gjson.Parse(`[9007199254740993]`).Array(), false)
	require.NoError(t, err)
	assert.Equal(t, []int64{9007199254740993}, scalar.GetLongData().GetData())

	for _, raw := range []string{`["9223372036854775808"]`, `["1.5"]`, `["abc"]`, `[""]`} {
		_, err = buildStructSubArrayScalar(sub, gjson.Parse(raw).Array(), false)
		assert.Error(t, err, raw)
	}
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

	{
		request := map[string]interface{}{
			HTTPRequestData: []interface{}{
				[]int8{1, 2},
			},
		}
		requestBody, _ := json.Marshal(request)
		values, err = serializeInt8Vectors(gjson.Get(string(requestBody), HTTPRequestData).Raw, schemapb.DataType_Int8Vector, 2, typeutil.Int8ArrayToBytes)
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
					return [][]byte{{}, {}}
				},
			}, {
				`"{"data": [""]}"`,
				dataType,
				0,
				func() [][]byte {
					return [][]byte{{}}
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
		assert.Equal(t, testcase.placehoderValue(), phv.GetValues(),
			fmt.Sprintf("check equal fail, data: %s, type: %s, dim: %d", testcase.requestBody, testcase.dataType, testcase.dim))
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
		assert.NotEqual(t, testcase.placehoderValue(), phv.GetValues(),
			fmt.Sprintf("check not equal fail, data: %s, type: %s, dim: %d", testcase.requestBody, testcase.dataType, testcase.dim))
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

// sameValue compares two response values. Dynamic-field numbers are decoded
// with UseNumber so that an integer past 2^53 keeps its digits, which makes
// them json.Number rather than the native type the expected rows carry.
func sameValue(a interface{}, b interface{}) bool {
	if a == b {
		return true
	}
	an, aok := a.(json.Number)
	bn, bok := b.(json.Number)
	switch {
	case aok && bok:
		return an == bn
	case aok:
		return an.String() == fmt.Sprintf("%v", b)
	case bok:
		return bn.String() == fmt.Sprintf("%v", a)
	}
	return false
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
			// The field reads back as the document it holds, or as the text of
			// that document when proxy.http.nativeJSONResponse is off.
			var got []byte
			switch v := value.(type) {
			case string:
				got = []byte(v)
			case json.RawMessage:
				got = []byte(v)
			default:
				return false
			}
			if string(got) != string(m2[key].([]byte)) {
				return false
			}
		} else if key == "field-geometry" {
			arr1 := value.(string)
			arr2 := m2[key].(string)
			if arr2 != arr1 {
				return false
			}
		} else if strings.HasPrefix(key, "array-") {
			continue
		} else if !sameValue(value, m2[key]) {
			return false
		}
	}

	for key, value := range m2 {
		if (key == FieldBookIntro) || (key == "field-json") || (key == "field-geometry") || (key == "field-array") {
			continue
		} else if strings.HasPrefix(key, "array-") {
			continue
		} else if !sameValue(value, m1[key]) {
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
	rows, err := buildQueryResp(int64(0), outputFields, generateFieldData(), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, nil) // []*schemapb.FieldData{&fieldData1, &fieldData2, &fieldData3}
	assert.Equal(t, nil, err)
	exceptRows := generateSearchResult(schemapb.DataType_Int64)
	assert.Equal(t, true, compareRows(rows, exceptRows, compareRow))
}

func TestResultErrMessage(t *testing.T) {
	t.Run("marked message is echoed", func(t *testing.T) {
		err := asSafeMessage(merr.WrapErrParameterInvalidMsg(
			"the type(%v) of field(%v) is not supported, use other sdk please",
			schemapb.DataType_Geometry, "geo"))
		assert.Contains(t, resultErrMessage(err), "use other sdk please")
	})

	t.Run("marked message survives field wrapping", func(t *testing.T) {
		err := asSafeMessage(merr.WrapErrParameterInvalidMsg(
			"the type(%v) of field(%v) is not supported, use other sdk please",
			schemapb.DataType_Geometry, "geo"))
		msg := resultErrMessage(wrapOutputFieldErr("geo", err))
		assert.Contains(t, msg, "use other sdk please")
		assert.Contains(t, msg, "geo")
	})

	// Being an InputError is not enough on its own: most ParameterInvalid errors
	// on this path describe a server-side shape mismatch.
	t.Run("unmarked input error is not echoed", func(t *testing.T) {
		err := merr.WrapErrParameterInvalidMsg("field %s has %d valid rows, but data length is %d", "tags", 2, 1)
		msg := resultErrMessage(wrapOutputFieldErr("tags", err))
		assert.Contains(t, msg, "tags")
		assert.NotContains(t, msg, "valid rows")
		assert.NotContains(t, msg, "data length")
	})

	t.Run("server fault names the field and nothing else", func(t *testing.T) {
		inner := merr.WrapErrServiceInternalMsg("array row scalar field is nil")
		err := wrapOutputFieldErr("user_email_tags", merr.Wrapf(inner, "row %d", 3))
		msg := resultErrMessage(err)
		// The caller learns which output field to drop or report...
		assert.Contains(t, msg, "user_email_tags")
		// ...but nothing about internal structures or row offsets.
		assert.NotContains(t, msg, "service internal error")
		assert.NotContains(t, msg, "scalar field")
		assert.NotContains(t, msg, "row 3")
	})

	t.Run("non-milvus error is not echoed", func(t *testing.T) {
		err := json.Unmarshal([]byte("{bad"), &struct{}{})
		require.Error(t, err)
		msg := resultErrMessage(err)
		assert.Equal(t, merr.ErrInvalidSearchResult.Error(), msg)
	})

	t.Run("stage fault names the stage and nothing else", func(t *testing.T) {
		inner := merr.WrapErrServiceInternalMsg("search_aggregation agg_topks sum %d does not match bucket count %d", 3, 2)
		msg := resultErrMessage(wrapResultStageErr("search aggregation result", inner))
		assert.Contains(t, msg, "search aggregation result")
		assert.NotContains(t, msg, "agg_topks")
		assert.NotContains(t, msg, "bucket count")
	})
}

// A dynamic field whose stored bytes are not one JSON document is the caller's own
// data, so the reason reaches them. The internal $meta name does not: it is not one
// of their output fields, so naming it would point at a column they cannot drop.
func TestBuildQueryRespDynamicFieldNotSingleDocument(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"trailing document", `{"a": 1}{"b": 2}`},
		{"not json at all", `not-json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fieldData := &schemapb.FieldData{
				Type:      schemapb.DataType_JSON,
				FieldName: common.MetaFieldName,
				IsDynamic: true,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(tc.raw)}},
						},
					},
				},
			}
			_, err := buildQueryResp(int64(0), []string{"a"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
			require.Error(t, err)

			msg := resultErrMessage(err)
			assert.Contains(t, msg, "dynamic field does not hold a single JSON document")
			assert.NotContains(t, msg, common.MetaFieldName)
		})
	}
}

// proxy.http.legacyArrayResponse lets a deployment keep serving the pre-fix shape
// while clients that parsed it migrate. Default stays off.
func TestBuildQueryRespLegacyArrayResponse(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "tags",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_VarChar,
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b"}}}},
						},
					},
				},
			},
		},
	}
	render := func() string {
		rows, err := buildQueryResp(0, []string{"tags"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		require.NoError(t, err)
		payload, err := json.Marshal(gin.H{"data": rows})
		require.NoError(t, err)
		return string(payload)
	}

	params := paramtable.Get()
	key := params.HTTPCfg.LegacyArrayResponse.Key

	// Default: native JSON array.
	require.False(t, params.HTTPCfg.LegacyArrayResponse.GetAsBool())
	assert.JSONEq(t, `{"data":[{"tags":["a","b"]}]}`, render())

	// Switched on: the pre-fix protobuf wrapper shape. Rendered by the
	// encoder that serves responses, since reproducing that output is the
	// whole point of the switch.
	params.Save(key, "true")
	defer params.Reset(key)
	assert.JSONEq(t, `{"data":[{"tags":{"Data":{"StringData":{"data":["a","b"]}}}}]}`, render())

	// And back off again, so the switch is not one-way.
	params.Reset(key)
	assert.JSONEq(t, `{"data":[{"tags":["a","b"]}]}`, render())
}

// The switch restores a shape, not a rendering: Accept-Type-Allow-Int64 says
// what the caller's JSON parser can hold, which the shape does not change, so an
// Int64 past the JavaScript safe range still arrives as a string inside the
// wrapper rather than as a number that would round on arrival.
func TestLegacyArrayResponseKeepsInt64Safe(t *testing.T) {
	longRow := func(values ...int64) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:      schemapb.DataType_Array,
			FieldName: "ids",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int64,
							Data: []*schemapb.ScalarField{
								{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}},
							},
						},
					},
				},
			},
		}
	}

	params := paramtable.Get()
	key := params.HTTPCfg.LegacyArrayResponse.Key
	params.Save(key, "true")
	defer params.Reset(key)

	render := func(enableInt64 bool) string {
		rows, err := buildQueryResp(0, []string{"ids"},
			[]*schemapb.FieldData{longRow(9007199254740993, 1)}, nil, nil, enableInt64, nil)
		require.NoError(t, err)
		payload, err := json.Marshal(gin.H{"data": rows})
		require.NoError(t, err)
		return string(payload)
	}

	// Without the header the value cannot be a JSON number, in any shape.
	assert.JSONEq(t,
		`{"data":[{"ids":{"Data":{"LongData":{"data":["9007199254740993","1"]}}}}]}`,
		render(false))
	assert.NotContains(t, render(false), "9007199254740993,")

	// With it, the message is rendered as it stands.
	assert.JSONEq(t,
		`{"data":[{"ids":{"Data":{"LongData":{"data":[9007199254740993,1]}}}}]}`,
		render(true))

	// A row with no Int64 in it is untouched either way.
	stringRow := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "tags",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_VarChar,
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}},
						},
					},
				},
			},
		},
	}
	rows, err := buildQueryResp(0, []string{"tags"}, []*schemapb.FieldData{stringRow}, nil, nil, false, nil)
	require.NoError(t, err)
	payload, err := json.Marshal(gin.H{"data": rows})
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":[{"tags":{"Data":{"StringData":{"data":["a"]}}}}]}`, string(payload))
}

// The redaction rules must hold for the errors buildQueryResp actually produces,
// not only for hand-built ones. Each case drives a real producer and asserts on
// what the client would receive.
func TestResultErrMessageFromRealProducers(t *testing.T) {
	t.Run("valid-data shape mismatch is not echoed", func(t *testing.T) {
		// ValidData describes 3 rows of which 2 are valid, but only 1 row exists.
		fieldData := &schemapb.FieldData{
			Type:      schemapb.DataType_Array,
			FieldName: "tags",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_VarChar,
							Data: []*schemapb.ScalarField{
								{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}},
							},
						},
					},
				},
			},
			ValidData: []bool{true, true, false},
		}

		_, err := buildQueryResp(0, []string{"tags"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		require.Error(t, err)
		// The producer really does spell out the internal shape...
		require.Contains(t, err.Error(), "valid rows")

		// ...and the client must not see it.
		msg := resultErrMessage(err)
		assert.Contains(t, msg, "tags")
		assert.NotContains(t, msg, "valid rows")
		assert.NotContains(t, msg, "data length")
	})

	t.Run("unsupported field type is echoed", func(t *testing.T) {
		fieldData := &schemapb.FieldData{Type: schemapb.DataType_None, FieldName: "weird"}
		_, err := buildQueryResp(0, []string{"weird"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		require.Error(t, err)
		assert.Contains(t, resultErrMessage(err), "use other sdk please")
	})

	t.Run("unsupported primary key type is echoed", func(t *testing.T) {
		_, err := buildQueryResp(0, nil, nil, &schemapb.IDs{}, nil, true, nil)
		require.Error(t, err)
		assert.Contains(t, resultErrMessage(err), "use other sdk please")
	})
}

func TestScalarFieldToRESTAny(t *testing.T) {
	testCases := []struct {
		name        string
		field       *schemapb.ScalarField
		enableInt64 bool
		expected    any
	}{
		{
			name:     "bool",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}}},
			expected: []bool{true, false},
		},
		{
			name:     "int",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}},
			expected: []int32{1, 2},
		},
		{
			name:        "int64 enabled",
			field:       &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{9007199254740993}}}},
			enableInt64: true,
			expected:    []int64{9007199254740993},
		},
		{
			name:     "int64 disabled",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{9007199254740993}}}},
			expected: []string{"9007199254740993"},
		},
		{
			name:     "float",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.5}}}},
			expected: []float32{1.5},
		},
		{
			name:     "double",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{2.5}}}},
			expected: []float64{2.5},
		},
		{
			name:     "varchar",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b"}}}},
			expected: []string{"a", "b"},
		},
		{
			name:     "empty",
			field:    &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
			expected: []string{},
		},
		{
			name: "nested",
			field: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}},
				{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}},
			}}}},
			enableInt64: true,
			expected:    []any{[]int64{1, 2}, []string{"a"}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := scalarFieldToRESTAny(testCase.field, testCase.enableInt64)
			require.NoError(t, err)
			assert.Equal(t, testCase.expected, actual)
		})
	}

	// A missing row degrades to null rather than failing the whole request: the
	// previous implementation rendered it as null, and this change only fixes the
	// serialization format.
	t.Run("nil field renders as null", func(t *testing.T) {
		value, err := scalarFieldToRESTAny(nil, true)
		require.NoError(t, err)
		assert.Nil(t, value)
	})

	// Likewise for a ScalarField whose oneof was never set, which is what segcore
	// emits for an empty array with an undetermined element type.
	t.Run("unset oneof renders as empty array", func(t *testing.T) {
		value, err := scalarFieldToRESTAny(&schemapb.ScalarField{}, true)
		require.NoError(t, err)
		assert.Equal(t, []any{}, value)
	})

	// A oneof that is set but genuinely unsupported still fails loudly.
	t.Run("unsupported field", func(t *testing.T) {
		_, err := scalarFieldToRESTAny(&schemapb.ScalarField{Data: &schemapb.ScalarField_BytesData{}}, true)
		require.Error(t, err)
		assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))
	})
}

// buildQueryResp must survive both degraded row shapes end to end rather than
// failing the query, and must still tell them apart in the emitted JSON.
func TestBuildQueryRespDegradedArrayRows(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "tags",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_VarChar,
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}},
							nil, // missing row -> null
							{},  // unset oneof -> []
						},
					},
				},
			},
		},
	}

	rows, err := buildQueryResp(0, []string{"tags"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
	require.NoError(t, err)
	payload, err := json.Marshal(gin.H{"data": rows})
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":[{"tags":["a"]},{"tags":null},{"tags":[]}]}`, string(payload))
}

func TestBuildQueryRespNativeArrayValues(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "tags",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "world"}}}},
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
						},
					},
				},
			},
		},
	}

	rows, err := buildQueryResp(0, []string{"tags"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
	require.NoError(t, err)
	payload, err := json.Marshal(gin.H{"data": rows})
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":[{"tags":["hello","world"]},{"tags":[]}]}`, string(payload))

	fieldData.ValidData = []bool{true, false, true}
	rows, err = buildQueryResp(0, []string{"tags"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
	require.NoError(t, err)
	payload, err = json.Marshal(gin.H{"data": rows})
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":[{"tags":["hello","world"]},{"tags":null},{"tags":[]}]}`, string(payload))

	int64FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "ids",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{9007199254740993}}}},
						},
					},
				},
			},
		},
	}
	rows, err = buildQueryResp(0, []string{"ids"}, []*schemapb.FieldData{int64FieldData}, nil, nil, false, nil)
	require.NoError(t, err)
	payload, err = json.Marshal(gin.H{"data": rows})
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":[{"ids":["9007199254740993"]}]}`, string(payload))
}

func TestBuildQueryRespComplexNativeArrayJSON(t *testing.T) {
	arrayField := func(name string, validData []bool, rows ...*schemapb.ScalarField) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:      schemapb.DataType_Array,
			FieldName: name,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{Data: rows},
					},
				},
			},
			ValidData: validData,
		}
	}

	fieldData := []*schemapb.FieldData{
		arrayField("bool_arr", []bool{true, false, true},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{}}},
		),
		arrayField("int_arr", []bool{true, true, true},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{-128, 0, 127}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{42}}}},
		),
		arrayField("long_arr", []bool{true, true, false},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{9007199254740993, -9007199254740993}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}},
		),
		arrayField("float_arr", []bool{false, true, true},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.25, -2.5}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{}}},
		),
		arrayField("double_arr", []bool{true, false, true},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{3.125, -0.0625}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{}}},
		),
		arrayField("string_arr", []bool{false, true, true},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hello", "世界", "quote\" and slash\\"}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
		),
	}
	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{9007199254740993, 2, 3}},
		},
	}
	scores := []float32{0.875, 0.5, -0.25}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	}

	testCases := []struct {
		name              string
		enableInt64       bool
		expected          string
		expectedInt64JSON []string
	}{
		{
			name: "stringify int64",
			expectedInt64JSON: []string{
				`"pk":"9007199254740993"`,
				`"long_arr":["9007199254740993","-9007199254740993"]`,
			},
			expected: `{
				"data": [
					{
						"pk": "9007199254740993",
						"distance": 0.875,
						"bool_arr": [true, false],
						"int_arr": [-128, 0, 127],
						"long_arr": ["9007199254740993", "-9007199254740993"],
						"float_arr": null,
						"double_arr": [3.125, -0.0625],
						"string_arr": null
					},
					{
						"pk": "2",
						"distance": 0.5,
						"bool_arr": null,
						"int_arr": [],
						"long_arr": [],
						"float_arr": [1.25, -2.5],
						"double_arr": null,
						"string_arr": ["hello", "世界", "quote\" and slash\\"]
					},
					{
						"pk": "3",
						"distance": -0.25,
						"bool_arr": [],
						"int_arr": [42],
						"long_arr": null,
						"float_arr": [],
						"double_arr": [],
						"string_arr": []
					}
				]
			}`,
		},
		{
			name:        "preserve int64",
			enableInt64: true,
			expectedInt64JSON: []string{
				`"pk":9007199254740993`,
				`"long_arr":[9007199254740993,-9007199254740993]`,
			},
			expected: `{
				"data": [
					{
						"pk": 9007199254740993,
						"distance": 0.875,
						"bool_arr": [true, false],
						"int_arr": [-128, 0, 127],
						"long_arr": [9007199254740993, -9007199254740993],
						"float_arr": null,
						"double_arr": [3.125, -0.0625],
						"string_arr": null
					},
					{
						"pk": 2,
						"distance": 0.5,
						"bool_arr": null,
						"int_arr": [],
						"long_arr": [],
						"float_arr": [1.25, -2.5],
						"double_arr": null,
						"string_arr": ["hello", "世界", "quote\" and slash\\"]
					},
					{
						"pk": 3,
						"distance": -0.25,
						"bool_arr": [],
						"int_arr": [42],
						"long_arr": null,
						"float_arr": [],
						"double_arr": [],
						"string_arr": []
					}
				]
			}`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := buildQueryResp(0, nil, fieldData, ids, scores, testCase.enableInt64, schema)
			require.NoError(t, err)
			payload, err := json.Marshal(gin.H{"data": rows})
			require.NoError(t, err)
			assert.JSONEq(t, testCase.expected, string(payload))
			for _, expected := range testCase.expectedInt64JSON {
				assert.Contains(t, string(payload), expected)
			}
		})
	}
}

// Int64 elements inside an ArrayOfStruct sub-field must honor
// Accept-Type-Allow-Int64 exactly like a top-level Array field does. Values beyond
// the JavaScript safe-integer range are asserted against the raw JSON, because
// assert.JSONEq decodes both sides into float64 and would pass even if the digits
// were silently rounded.
func TestBuildQueryRespStructArrayInt64(t *testing.T) {
	const (
		bigPositive = int64(9007199254740993)
		bigNegative = int64(-9007199254740993)
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 101,
				Name:    "my_struct",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     102,
						Name:        "score",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int64,
					},
				},
			},
		},
	}

	newFieldData := func() *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:      schemapb.DataType_ArrayOfStruct,
			FieldName: "my_struct",
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{
					Fields: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_Array,
							FieldName: "score",
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_ArrayData{
										ArrayData: &schemapb.ArrayArray{
											ElementType: schemapb.DataType_Int64,
											Data: []*schemapb.ScalarField{
												{Data: &schemapb.ScalarField_LongData{
													LongData: &schemapb.LongArray{Data: []int64{bigPositive, bigNegative}},
												}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	testCases := []struct {
		name        string
		enableInt64 bool
		expected    string
		expectedRaw []string
	}{
		{
			name:        "stringify int64",
			enableInt64: false,
			expected:    `{"data":[{"my_struct":[{"score":"9007199254740993"},{"score":"-9007199254740993"}]}]}`,
			expectedRaw: []string{`"score":"9007199254740993"`, `"score":"-9007199254740993"`},
		},
		{
			name:        "preserve int64",
			enableInt64: true,
			expected:    `{"data":[{"my_struct":[{"score":9007199254740993},{"score":-9007199254740993}]}]}`,
			expectedRaw: []string{`"score":9007199254740993`, `"score":-9007199254740993`},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := buildQueryResp(0, nil, []*schemapb.FieldData{newFieldData()}, nil, nil, testCase.enableInt64, schema)
			require.NoError(t, err)
			payload, err := json.Marshal(gin.H{"data": rows})
			require.NoError(t, err)
			assert.JSONEq(t, testCase.expected, string(payload))
			for _, raw := range testCase.expectedRaw {
				assert.Contains(t, string(payload), raw)
			}
		})
	}
}

func TestBuildQueryRespDynamicFieldLargeIntPrecision(t *testing.T) {
	t.Run("dynamic field integers above 2^53 keep exact precision", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			Type:      schemapb.DataType_JSON,
			FieldName: FieldBookIntro,
			IsDynamic: true,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: [][]byte{
								[]byte(`{"big_val":9223372036854775807,"dyn_int":9007199254740993,"small":42}`),
							},
						},
					},
				},
			},
		}

		rows, err := buildQueryResp(0, []string{"big_val", "dyn_int", "small"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		row := rows[0]
		bigVal, ok := row["big_val"].(json.Number)
		require.True(t, ok, "big_val should be json.Number, got %T", row["big_val"])
		assert.Equal(t, "9223372036854775807", bigVal.String())

		dynInt, ok := row["dyn_int"].(json.Number)
		require.True(t, ok, "dyn_int should be json.Number, got %T", row["dyn_int"])
		assert.Equal(t, "9007199254740993", dynInt.String())

		small, ok := row["small"].(json.Number)
		require.True(t, ok, "small should be json.Number, got %T", row["small"])
		assert.Equal(t, "42", small.String())

		// Serializing the row back must emit the exact digits, not a float64-rounded value.
		out, err := json.Marshal(row)
		require.NoError(t, err)
		assert.Contains(t, string(out), "9223372036854775807")
		assert.Contains(t, string(out), "9007199254740993")
	})
}

func TestBuildQueryRespWithTextField(t *testing.T) {
	longText := strings.Repeat("x", 64*1024+1)
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Text,
		FieldName: FieldText,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"short text", longText}},
				},
			},
		},
		ValidData: []bool{true, false, true},
	}

	count, err := fieldDataValueCount(fieldData)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	rows, err := buildQueryResp(0, []string{FieldText}, []*schemapb.FieldData{fieldData}, nil, nil, true, generateTextCollectionSchema(true))
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, "short text", rows[0][FieldText])
	assert.Nil(t, rows[1][FieldText])
	assert.Equal(t, longText, rows[2][FieldText])
}

func TestBuildQueryRespWithNullableCompactFields(t *testing.T) {
	t.Run("nullable vector derives logical rows from ValidData", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					ValidData: []bool{true, false, true},
					Dim:       2,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{0.1, 0.2, 0.3, 0.4},
						},
					},
				},
			},
		}

		rows, err := buildQueryResp(0, []string{FieldBookIntro}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		assert.NoError(t, err)
		assert.Len(t, rows, 3)
		assert.Equal(t, []float32{0.1, 0.2}, rows[0][FieldBookIntro])
		assert.Nil(t, rows[1][FieldBookIntro])
		assert.Equal(t, []float32{0.3, 0.4}, rows[2][FieldBookIntro])
	})

	t.Run("nullable vector all null keeps logical rows", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					ValidData: []bool{false, false},
					Dim:       2,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{},
					},
				},
			},
		}

		rows, err := buildQueryResp(0, []string{FieldBookIntro}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		assert.NoError(t, err)
		assert.Len(t, rows, 2)
		assert.Nil(t, rows[0][FieldBookIntro])
		assert.Nil(t, rows[1][FieldBookIntro])
	})

	t.Run("nullable vector rejects full row payload", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: FieldBookIntro,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					ValidData: []bool{true, false, true},
					Dim:       2,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{
								0.1, 0.2,
								0.0, 0.0,
								0.3, 0.4,
							},
						},
					},
				},
			},
		}

		_, err := buildQueryResp(0, []string{FieldBookIntro}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nullable vector field")
	})

	t.Run("nullable dense vector rejects partial row payload", func(t *testing.T) {
		cases := []struct {
			name      string
			fieldData *schemapb.FieldData
		}{
			{
				name: "float_vector",
				fieldData: &schemapb.FieldData{
					Type:      schemapb.DataType_FloatVector,
					FieldName: FieldBookIntro,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							ValidData: []bool{true, false},
							Dim:       2,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2, 0.3}},
							},
						},
					},
				},
			},
			{
				name: "binary_vector",
				fieldData: &schemapb.FieldData{
					Type:      schemapb.DataType_BinaryVector,
					FieldName: FieldBookIntro,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							ValidData: []bool{true, false},
							Dim:       16,
							Data: &schemapb.VectorField_BinaryVector{
								BinaryVector: []byte{0x01, 0x02, 0x03},
							},
						},
					},
				},
			},
			{
				name: "float16_vector",
				fieldData: &schemapb.FieldData{
					Type:      schemapb.DataType_Float16Vector,
					FieldName: FieldBookIntro,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							ValidData: []bool{true, false},
							Dim:       2,
							Data: &schemapb.VectorField_Float16Vector{
								Float16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05},
							},
						},
					},
				},
			},
			{
				name: "bfloat16_vector",
				fieldData: &schemapb.FieldData{
					Type:      schemapb.DataType_BFloat16Vector,
					FieldName: FieldBookIntro,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							ValidData: []bool{true, false},
							Dim:       2,
							Data: &schemapb.VectorField_Bfloat16Vector{
								Bfloat16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05},
							},
						},
					},
				},
			},
			{
				name: "int8_vector",
				fieldData: &schemapb.FieldData{
					Type:      schemapb.DataType_Int8Vector,
					FieldName: FieldBookIntro,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							ValidData: []bool{true, false},
							Dim:       2,
							Data: &schemapb.VectorField_Int8Vector{
								Int8Vector: []byte{0x01, 0x02, 0x03},
							},
						},
					},
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := buildQueryResp(0, []string{FieldBookIntro}, []*schemapb.FieldData{tc.fieldData}, nil, nil, true, nil)
				require.Error(t, err)
				assert.True(t,
					strings.Contains(err.Error(), "row width") || strings.Contains(err.Error(), "divide the dim"),
					"unexpected error: %s", err.Error())
			})
		}
	})

	t.Run("nullable scalar compact data uses physical index", func(t *testing.T) {
		fieldData := &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: FieldWordCount,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					ValidData: []bool{false, true},
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{20}},
					},
				},
			},
		}

		rows, err := buildQueryResp(0, []string{FieldWordCount}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
		assert.NoError(t, err)
		assert.Len(t, rows, 2)
		assert.Nil(t, rows[0][FieldWordCount])
		assert.Equal(t, int64(20), rows[1][FieldWordCount])
	})
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

	fieldSchema11 := schemapb.FieldSchema{
		Name:      "field-geometry",
		DataType:  schemapb.DataType_Geometry,
		IsDynamic: false,
	}
	coll.Fields = append(coll.Fields, &fieldSchema11)
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

	fieldData12 := schemapb.FieldData{
		Type:      schemapb.DataType_Geometry,
		FieldName: "field-geometry",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryWktData{
					GeometryWktData: &schemapb.GeometryWktArray{
						Data: []string{
							`POINT (30.123 -10.456)`,
							`POINT (30.123 -10.456)`,
							`POINT (30.123 -10.456)`,
							// wkb:{0x01, 0x01, 0x00, 0x00, 0x00, 0xD2, 0x4A, 0x4D, 0x6A, 0x8B, 0x3C, 0x5C, 0x0A, 0x0D, 0x1B, 0x4F, 0x4F, 0x9A, 0x3D, 0x4},
						},
					},
				},
			},
		},
		IsDynamic: false,
	}
	fieldDatas = append(fieldDatas, &fieldData12)

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
	case schemapb.DataType_Int8Vector:
		vectorField := generateVectorFieldData(firstFieldType)
		return []*schemapb.FieldData{&vectorField}
	case schemapb.DataType_Array:
		return []*schemapb.FieldData{&fieldData10}
	case schemapb.DataType_JSON:
		return []*schemapb.FieldData{&fieldData9}
	case schemapb.DataType_Geometry:
		return []*schemapb.FieldData{&fieldData12}
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
				ValidData: []bool{true, false, true},
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
		Type:      schemapb.DataType_Int64,
		FieldName: "field-int64",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				ValidData: []bool{true, false, true},
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{0, 1, 2},
					},
				},
			},
		},
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
	case schemapb.DataType_Int8Vector:
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
		result["field-geometry"] = `POINT (30.123 -10.456)`
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
	rows, validRows, err := checkAndSetData(body, collectionSchema, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(validRows))
	assert.Equal(t, true, compareRows(rows, generateRawRows(schemapb.DataType_Int64), compareRow))
	data, err := anyToColumns(rows, validRows, collectionSchema, true, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(collectionSchema.Fields), len(data))

	body, _ = generateRequestBodyWithArray(schemapb.DataType_Int64)
	collectionSchema = newCollectionSchemaWithArray(generateCollectionSchema(schemapb.DataType_Int64, false, true))
	rows, validRows, err = checkAndSetData(body, collectionSchema, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(validRows))
	assert.Equal(t, true, compareRows(rows, newRowsWithArray(generateRawRows(schemapb.DataType_Int64)), compareRow))
	data, err = anyToColumns(rows, validRows, collectionSchema, true, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(collectionSchema.Fields), len(data))
}

func TestVector(t *testing.T) {
	floatVector := "vector-float"
	binaryVector := "vector-binary"
	float16Vector := "vector-float16"
	bfloat16Vector := "vector-bfloat16"
	sparseFloatVector := "vector-sparse-float"
	int8Vector := "vector-int8"
	testcaseRows := []map[string]interface{}{
		{
			FieldBookID:       int64(1),
			floatVector:       []float32{0.1, 0.11},
			binaryVector:      []byte{1},
			float16Vector:     []byte{1, 1, 11, 11},
			bfloat16Vector:    []byte{1, 1, 11, 11},
			sparseFloatVector: map[uint32]float32{0: 0.1, 1: 0.11},
			int8Vector:        []int8{1, 11},
		},
		{
			FieldBookID:       int64(2),
			floatVector:       []float32{0.2, 0.22},
			binaryVector:      []byte{2},
			float16Vector:     []byte{2, 2, 22, 22},
			bfloat16Vector:    []byte{2, 2, 22, 22},
			sparseFloatVector: map[uint32]float32{1000: 0.3, 200: 0.44},
			int8Vector:        []int8{2, 22},
		},
		{
			FieldBookID:       int64(3),
			floatVector:       []float32{0.3, 0.33},
			binaryVector:      []byte{3},
			float16Vector:     []byte{3, 3, 33, 33},
			bfloat16Vector:    []byte{3, 3, 33, 33},
			sparseFloatVector: map[uint32]float32{987621: 32190.31, 32189: 0.0001},
			int8Vector:        []int8{3, 33},
		},
		{
			FieldBookID:       int64(4),
			floatVector:       []float32{0.4, 0.44},
			binaryVector:      []byte{4},
			float16Vector:     []float32{0.4, 0.44},
			bfloat16Vector:    []float32{0.4, 0.44},
			sparseFloatVector: map[uint32]float32{25: 0.1, 1: 0.11},
			int8Vector:        []int8{4, 44},
		},
		{
			FieldBookID:       int64(5),
			floatVector:       []float32{-0.4, -0.44},
			binaryVector:      []byte{5},
			float16Vector:     []int64{99999999, -99999999},
			bfloat16Vector:    []int64{99999999, -99999999},
			sparseFloatVector: map[uint32]float32{1121: 0.1, 3: 0.11},
			int8Vector:        []int8{-128, 127},
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
	int8VectorField := generateVectorFieldSchema(schemapb.DataType_Int8Vector)
	int8VectorField.Name = int8Vector
	collectionSchema := &schemapb.CollectionSchema{
		Name:        DefaultCollectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			primaryField, floatVectorField, binaryVectorField, float16VectorField, bfloat16VectorField, sparseFloatVectorField,
		},
		EnableDynamicField: true,
	}
	rows, validRows, err := checkAndSetData(body, collectionSchema, false)
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
	data, err := anyToColumns(rows, validRows, collectionSchema, true, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(collectionSchema.Fields)+1, len(data))

	assertError := func(field string, value interface{}) {
		row := make(map[string]interface{})
		for k, v := range testcaseRows[0] {
			row[k] = v
		}
		row[field] = value
		body, _ = wrapRequestBody([]map[string]interface{}{row})
		_, _, err = checkAndSetData(body, collectionSchema, false)
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
		rows, err := buildQueryResp(int64(0), theOutputFields, newFieldData(generateFieldData(), schemapb.DataType_None), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, nil)
		assert.Equal(t, nil, err)
		exceptRows := newSearchResult(generateSearchResult(schemapb.DataType_Int64))
		assert.Equal(t, true, compareRows(rows, exceptRows, compareRow))
	}

	dataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector, schemapb.DataType_SparseFloatVector, schemapb.DataType_Int8Vector,
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Float, schemapb.DataType_Double,
		schemapb.DataType_String, schemapb.DataType_VarChar,
		schemapb.DataType_JSON, schemapb.DataType_Array,
		schemapb.DataType_Geometry,
	}
	for _, dateType := range dataTypes {
		_, err := buildQueryResp(int64(0), outputFields, newFieldData([]*schemapb.FieldData{}, dateType), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, nil)
		assert.Equal(t, nil, err)
	}

	_, err := buildQueryResp(int64(0), outputFields, newFieldData([]*schemapb.FieldData{}, 1000), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, nil)
	assert.Contains(t, err.Error(), "the type(1000) of field(wrong-field-type) is not supported, use other sdk please")
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))

	res, err := buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIDs(schemapb.DataType_Int64, 3), DefaultScores, true, nil)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	res, err = buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIDs(schemapb.DataType_Int64, 3), DefaultScores, false, nil)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	res, err = buildQueryResp(int64(0), outputFields, []*schemapb.FieldData{}, generateIDs(schemapb.DataType_VarChar, 3), DefaultScores, true, nil)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, nil, err)

	_, err = buildQueryResp(int64(0), outputFields, generateFieldData(), generateIDs(schemapb.DataType_Int64, 3), DefaultScores, false, nil)
	assert.Equal(t, nil, err)

	// len(rows) != len(scores), didn't show distance
	_, err = buildQueryResp(int64(0), outputFields, newFieldData(generateFieldData(), schemapb.DataType_None), generateIDs(schemapb.DataType_Int64, 3), []float32{0.01, 0.04}, true, nil)
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
		if pair.Key == common.ParamsKey {
			assert.Equal(t, string("{\"nlist\":128}"), pair.Value)
		}
	}
}

func TestGenerateExpressionTemplate(t *testing.T) {
	var mixedAns [][]byte

	val, _ := json.Marshal(1)
	mixedAns = append(mixedAns, val)
	val, _ = json.Marshal("10")
	mixedAns = append(mixedAns, val)
	val, _ = json.Marshal(true)
	mixedAns = append(mixedAns, val)
	// the parameters now arrive as the raw JSON the caller wrote; the expected
	// values below are unchanged, so the classification has to match what the
	// previous already-decoded path produced
	expressionTemplates := []map[string]string{
		{"str": `"10"`},
		{"min": `1`, "max": `10`},
		{"bool": `true`},
		{"float64": `1.1`},
		{"int64": `1`},
		{"list_of_str": `["1","10","100"]`},
		{"list_of_bool": `[true,false,true]`},
		{"list_of_float": `[1.1,10.1,100.1]`},
		{"list_of_int": `[1,10,100]`},
		{"list_of_json": `[1,"10",true]`},
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
		actual, err := generateExpressionTemplate(rawParams(t, template))
		require.NoError(t, err)
		assert.Equal(t, ans[i], actual)
	}
}

func TestGenerateSearchParams(t *testing.T) {
	t.Run("searchParams.params must be a dict", func(t *testing.T) {
		reqSearchParams := map[string]interface{}{"params": 0}
		_, err := generateSearchParams(reqSearchParams)
		assert.NotNil(t, err)
	})

	t.Run("ambiguous parameter", func(t *testing.T) {
		reqSearchParams := map[string]interface{}{"radius": 100, "params": map[string]interface{}{"radius": 10}}
		_, err := generateSearchParams(reqSearchParams)
		assert.NotNil(t, err)
	})

	t.Run("no ambiguous parameter", func(t *testing.T) {
		reqSearchParams := map[string]interface{}{"radius": 10, "params": map[string]interface{}{"radius": 10.0}}
		_, err := generateSearchParams(reqSearchParams)
		assert.Nil(t, err)

		reqSearchParams = map[string]interface{}{"radius": 10.0, "params": map[string]interface{}{"radius": 10}}
		_, err = generateSearchParams(reqSearchParams)
		assert.Nil(t, err)

		reqSearchParams = map[string]interface{}{"radius": 10, "params": map[string]interface{}{"radius": 10}}
		searchParams, err := generateSearchParams(reqSearchParams)
		assert.Equal(t, 3, len(searchParams))
		assert.Nil(t, err)
		for _, kvs := range searchParams {
			if kvs.Key == "radius" {
				assert.Equal(t, "10", kvs.Value)
			}
			if kvs.Key == "params" {
				var paramsMap map[string]interface{}
				err := json.Unmarshal([]byte(kvs.Value), &paramsMap)
				assert.Nil(t, err)
				assert.Equal(t, 1, len(paramsMap))
				assert.Equal(t, paramsMap["radius"], float64(10))
			}
		}
	})

	t.Run("old format", func(t *testing.T) {
		reqSearchParams := map[string]interface{}{"metric_type": "L2", "params": map[string]interface{}{"radius": 10}}
		searchParams, err := generateSearchParams(reqSearchParams)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(searchParams))
		for _, kvs := range searchParams {
			if kvs.Key == "metric_type" {
				assert.Equal(t, "L2", kvs.Value)
			}
			if kvs.Key == "params" {
				var paramsMap map[string]interface{}
				err := json.Unmarshal([]byte(kvs.Value), &paramsMap)
				assert.Nil(t, err)
				assert.Equal(t, 2, len(paramsMap))
				assert.Equal(t, paramsMap["radius"], float64(10))
				assert.Equal(t, paramsMap["metric_type"], "L2")
			}
		}
	})

	t.Run("new format", func(t *testing.T) {
		reqSearchParams := map[string]interface{}{"metric_type": "L2", "radius": 10}
		searchParams, err := generateSearchParams(reqSearchParams)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(searchParams))
		for _, kvs := range searchParams {
			if kvs.Key == "metric_type" {
				assert.Equal(t, "L2", kvs.Value)
			}
			if kvs.Key == "radius" {
				assert.Equal(t, "10", kvs.Value)
			}
			if kvs.Key == "params" {
				var paramsMap map[string]interface{}
				err := json.Unmarshal([]byte(kvs.Value), &paramsMap)
				assert.Nil(t, err)
				assert.Equal(t, 2, len(paramsMap))
				assert.Equal(t, paramsMap["radius"], float64(10))
				assert.Equal(t, paramsMap["metric_type"], "L2")
			}
		}
	})
}

func TestConvertSearchAggregationReq(t *testing.T) {
	req := &SearchAggregationReq{
		Fields:     []string{" brand "},
		Size:       3,
		SearchSize: 5,
		Metrics: map[string]MetricAggregationReq{
			" avg_price ": {Op: " avg ", FieldName: " price "},
		},
		Order: []AggregationOrderReq{{Key: " avg_price ", Direction: " desc "}},
		TopHits: &TopHitsReq{
			Size: 2,
			Sort: []AggregationSortReq{{FieldName: " _score ", Direction: " asc "}},
		},
		SubAggregation: &SearchAggregationReq{
			Fields: []string{"color"},
			Size:   2,
		},
	}

	spec, err := convertSearchAggregationReq(req)
	require.NoError(t, err)
	require.Equal(t, []string{"brand"}, spec.GetFields())
	require.EqualValues(t, 3, spec.GetSize())
	require.EqualValues(t, 5, spec.GetSearchSize())
	require.Equal(t, "avg", spec.GetMetrics()["avg_price"].GetOp())
	require.Equal(t, "price", spec.GetMetrics()["avg_price"].GetFieldName())
	require.Equal(t, "avg_price", spec.GetOrder()[0].GetKey())
	require.Equal(t, "desc", spec.GetOrder()[0].GetDirection())
	require.EqualValues(t, 2, spec.GetTopHits().GetSize())
	require.Equal(t, "_score", spec.GetTopHits().GetSort()[0].GetFieldName())
	require.Equal(t, "color", spec.GetSubAggregation().GetFields()[0])

	testCases := []struct {
		name string
		req  *SearchAggregationReq
		msg  string
	}{
		{name: "empty fields", req: &SearchAggregationReq{Size: 1}, msg: "fields must be non-empty"},
		{name: "blank field", req: &SearchAggregationReq{Fields: []string{" "}, Size: 1}, msg: "non-empty field names"},
		{name: "bad size", req: &SearchAggregationReq{Fields: []string{"brand"}, Size: 0}, msg: "size must be positive"},
		{name: "bad search size", req: &SearchAggregationReq{Fields: []string{"brand"}, Size: 2, SearchSize: 1}, msg: "greater than or equal to size"},
		{name: "blank metric op", req: &SearchAggregationReq{Fields: []string{"brand"}, Size: 1, Metrics: map[string]MetricAggregationReq{"m": {FieldName: "price"}}}, msg: "op must be non-empty"},
		{name: "blank order direction", req: &SearchAggregationReq{Fields: []string{"brand"}, Size: 1, Order: []AggregationOrderReq{{Key: "_count"}}}, msg: "direction must be non-empty"},
		{name: "bad top hits size", req: &SearchAggregationReq{Fields: []string{"brand"}, Size: 1, TopHits: &TopHitsReq{}}, msg: "topHits.size must be positive"},
	}

	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			_, err := convertSearchAggregationReq(testcase.req)
			require.Error(t, err)
			require.Contains(t, err.Error(), testcase.msg)
		})
	}
}

func TestBuildSearchAggregationResp(t *testing.T) {
	results := &schemapb.SearchResultData{
		NumQueries: 1,
		AggTopks:   []int64{1},
		AggBuckets: []*schemapb.AggBucket{
			{
				Key: []*schemapb.BucketKeyEntry{
					{FieldId: 101, FieldName: "brand", Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "acme"}},
					{FieldId: 102, FieldName: "model_id", Value: &schemapb.BucketKeyEntry_IntVal{IntVal: 9}},
				},
				Count: 3,
				Metrics: map[string]*schemapb.MetricValue{
					"avg_price": {Value: &schemapb.MetricValue_DoubleVal{DoubleVal: 12.5}},
					"stock":     {Value: &schemapb.MetricValue_IntVal{IntVal: 7}},
				},
				Hits: []*schemapb.AggHit{
					{
						Pk:    &schemapb.AggHit_IntPk{IntPk: 1001},
						Score: 0.8,
						Fields: []*schemapb.AggHitField{
							{FieldId: 201, FieldName: "price", Value: &schemapb.AggHitField_IntVal{IntVal: 99}},
							{FieldId: 202, FieldName: "title", Value: &schemapb.AggHitField_StringVal{StringVal: "item"}},
						},
					},
				},
				SubGroups: []*schemapb.AggBucket{
					{
						Key:   []*schemapb.BucketKeyEntry{{FieldId: 103, FieldName: "color", Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "red"}}},
						Count: 1,
					},
				},
			},
		},
	}

	resp, err := buildSearchAggregationResp(results, false, generateCollectionSchema(schemapb.DataType_Int64, false, true))
	require.NoError(t, err)
	require.Len(t, resp, 1)
	buckets := resp[0]["buckets"].([]gin.H)
	require.Len(t, buckets, 1)
	bucket := buckets[0]
	require.Equal(t, "3", bucket["count"])

	keys := bucket["key"].([]gin.H)
	require.Equal(t, "brand", keys[0]["fieldName"])
	require.Equal(t, "101", keys[0]["fieldId"])
	require.Equal(t, "acme", keys[0]["value"])
	require.Equal(t, "9", keys[1]["value"])

	metrics := bucket["metrics"].(gin.H)
	require.Equal(t, 12.5, metrics["avg_price"])
	require.Equal(t, "7", metrics["stock"])

	hits := bucket["hits"].([]gin.H)
	require.Equal(t, "1001", hits[0][FieldBookID])
	require.Equal(t, float32(0.8), hits[0][HTTPReturnDistance])
	require.Equal(t, "99", hits[0]["price"])
	require.Equal(t, "item", hits[0]["title"])

	subGroups := bucket["subGroups"].([]gin.H)
	require.Equal(t, "1", subGroups[0]["count"])

	_, err = buildSearchAggregationResp(&schemapb.SearchResultData{NumQueries: 1, AggBuckets: results.GetAggBuckets()}, true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing agg_topks")

	_, err = buildSearchAggregationResp(&schemapb.SearchResultData{AggTopks: []int64{1}, AggBuckets: results.GetAggBuckets()}, true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing nq")

	_, err = buildSearchAggregationResp(&schemapb.SearchResultData{NumQueries: 2, AggTopks: []int64{1}, AggBuckets: results.GetAggBuckets()}, true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match nq")

	_, err = buildSearchAggregationResp(&schemapb.SearchResultData{NumQueries: 1, AggTopks: []int64{2}, AggBuckets: results.GetAggBuckets()}, true, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match bucket count")

	// The reduce contract detail above stays in the log; the caller gets the stage
	// so a bad aggregation response is still distinguishable when reported.
	msg := resultErrMessage(err)
	require.Contains(t, msg, "search aggregation result")
	require.NotContains(t, msg, "agg_topks")
	require.NotContains(t, msg, "bucket count")
}

func TestGenFunctionSchem(t *testing.T) {
	{
		funcSchema := &FunctionSchema{
			FunctionName:    "test",
			Description:     "",
			FunctionType:    "unknow",
			InputFieldNames: []string{"test"},
		}
		_, err := genFunctionSchema(context.Background(), funcSchema)
		assert.ErrorContains(t, err, "Unsupported function type:")
	}
	{
		funcSchema := &FunctionSchema{
			FunctionName:    "test",
			Description:     "",
			FunctionType:    "Rerank",
			InputFieldNames: []string{"test"},
		}
		_, err := genFunctionSchema(context.Background(), funcSchema)
		assert.NoError(t, err)
	}
	{
		funcSchema := &FunctionSchema{
			FunctionName:    "test",
			Description:     "",
			FunctionType:    "Rerank",
			InputFieldNames: []string{"test"},
			Params: map[string]interface{}{
				"test": []string{"test", "test2"},
				"test2": map[string]interface{}{
					"test3": "test4",
				},
				"test3":   []int{1, 2, 3},
				"test4":   nil,
				"weights": []float64{0.7, 0.3},
			},
		}
		result, err := genFunctionSchema(context.Background(), funcSchema)
		assert.NoError(t, err)
		params := funcutil.KeyValuePair2Map(result.GetParams())
		assert.Equal(t, "null", params["test4"])
		assert.Equal(t, "[0.7,0.3]", params["weights"])
	}
}

func TestGenFunctionScore(t *testing.T) {
	{
		fScore := FunctionScore{}
		funcSchema := FunctionSchema{
			FunctionName:    "test",
			Description:     "",
			FunctionType:    "unknow",
			InputFieldNames: []string{"test"},
		}

		fScore.Functions = append(fScore.Functions, funcSchema)
		_, err := genFunctionScore(context.Background(), &fScore)
		assert.ErrorContains(t, err, "Unsupported function typ")
	}
	{
		fScore := FunctionScore{}
		funcSchema := FunctionSchema{
			FunctionName:    "test",
			Description:     "",
			FunctionType:    "Rerank",
			InputFieldNames: []string{"test"},
		}

		fScore.Functions = append(fScore.Functions, funcSchema)
		fScore.Params = map[string]interface{}{"testStr": "test", "testInt": 6, "testBool": true}
		_, err := genFunctionScore(context.Background(), &fScore)
		assert.NoError(t, err)
	}
}

func TestGenFunctionChains(t *testing.T) {
	column := "$score"
	chains, err := genFunctionChains([]FunctionChainReq{
		{
			Name:  "l2",
			Stage: "FunctionChainStageL2Rerank",
			Ops: []FunctionChainOpReq{
				{
					Op:      "map",
					Outputs: []string{"new_score"},
					Expr: &FunctionChainExprReq{
						Name: "num_combine",
						Args: []FunctionChainExprArgReq{
							{Column: &column},
							{Literal: 2.5},
						},
						Params: map[string]interface{}{
							"mode":    "weighted",
							"weights": []interface{}{1.0, 2.0},
							"nested":  map[string]interface{}{"flag": true, "count": 3.0},
						},
					},
				},
				{
					Op:     "sort",
					Inputs: []string{"new_score"},
					Params: map[string]interface{}{"column": "new_score", "desc": true},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, chains, 1)
	chainPB := chains[0]
	assert.Equal(t, "l2", chainPB.GetName())
	assert.Equal(t, schemapb.FunctionChainStage_FunctionChainStageL2Rerank, chainPB.GetStage())
	require.Len(t, chainPB.GetOps(), 2)

	mapOp := chainPB.GetOps()[0]
	assert.Equal(t, "map", mapOp.GetOp())
	assert.Equal(t, []string{"new_score"}, mapOp.GetOutputs())
	require.NotNil(t, mapOp.GetExpr())
	assert.Equal(t, "num_combine", mapOp.GetExpr().GetName())
	assert.Equal(t, "$score", mapOp.GetExpr().GetArgs()[0].GetColumn().GetName())
	assert.Equal(t, 2.5, mapOp.GetExpr().GetArgs()[1].GetLiteral().GetDoubleValue())
	assert.Equal(t, "weighted", mapOp.GetExpr().GetParams()["mode"].GetStringValue())
	assert.Equal(t, int64(1), mapOp.GetExpr().GetParams()["weights"].GetArrayValue().GetValues()[0].GetInt64Value())
	assert.Equal(t, int64(3), mapOp.GetExpr().GetParams()["nested"].GetObjectValue().GetFields()["count"].GetInt64Value())

	sortOp := chainPB.GetOps()[1]
	assert.Equal(t, "sort", sortOp.GetOp())
	assert.True(t, sortOp.GetParams()["desc"].GetBoolValue())
	assert.Equal(t, "new_score", sortOp.GetParams()["column"].GetStringValue())
}

func TestGenFunctionChainsInvalid(t *testing.T) {
	column := "field"
	emptyColumn := " "
	_, err := genFunctionChains([]FunctionChainReq{{Stage: "BadStage"}})
	assert.ErrorContains(t, err, "unsupported function chain stage")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageUnspecified"}})
	assert.ErrorContains(t, err, "unsupported function chain stage")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: " "}}}})
	assert.ErrorContains(t, err, "op name is empty")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Expr: &FunctionChainExprReq{Name: " "}}}}})
	assert.ErrorContains(t, err, "expr name is empty")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Expr: &FunctionChainExprReq{Name: "expr", Args: []FunctionChainExprArgReq{{Column: &column, Literal: 1}}}}}}})
	assert.ErrorContains(t, err, "exactly one of column or literal is required")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Expr: &FunctionChainExprReq{Name: "expr", Args: []FunctionChainExprArgReq{{}}}}}}})
	assert.ErrorContains(t, err, "exactly one of column or literal is required")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Expr: &FunctionChainExprReq{Name: "expr", Args: []FunctionChainExprArgReq{{Column: &emptyColumn}}}}}}})
	assert.ErrorContains(t, err, "column name is empty")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Params: map[string]interface{}{"bad": nil}}}}})
	assert.ErrorContains(t, err, "function param value is nil")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Params: map[string]interface{}{" ": 1.0}}}}})
	assert.ErrorContains(t, err, "param name is empty")

	_, err = genFunctionChains([]FunctionChainReq{{Stage: "FunctionChainStageL2Rerank", Ops: []FunctionChainOpReq{{Op: "map", Params: map[string]interface{}{"object": map[string]interface{}{" ": 1.0}}}}}})
	assert.ErrorContains(t, err, "object field name is empty")
}

func TestParseUsernamePassword(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("token with credential separator", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/", nil)
		c.Request.Header.Set("Authorization", "Bearer testuser:testpass")

		username, password, ok := ParseUsernamePassword(c)
		assert.True(t, ok)
		assert.Equal(t, "testuser", username)
		assert.Equal(t, "testpass", password)
	})

	t.Run("token without credential separator", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/", nil)
		c.Request.Header.Set("Authorization", "Bearer tokenonly")

		username, password, ok := ParseUsernamePassword(c)
		assert.False(t, ok)
		assert.Equal(t, "", username)
		assert.Equal(t, "", password)
	})

	t.Run("empty authorization header", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/", nil)

		username, password, ok := ParseUsernamePassword(c)
		assert.False(t, ok)
		assert.Equal(t, "", username)
		assert.Equal(t, "", password)
	})
}

func TestConvertIDsToSchemapbIDs(t *testing.T) {
	int64PkField := &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         "id",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}

	varcharPkField := &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         "id",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_VarChar,
	}

	t.Run("empty ids array", func(t *testing.T) {
		_, err := convertIDsToSchemapbIDs(nil, int64PkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ids array cannot be empty")
	})

	t.Run("int64 pk with float64 values (whole numbers)", func(t *testing.T) {
		// JSON numbers are decoded as float64
		ids := rawIDs(`1`, `2`, `3`)
		result, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		intIds := result.GetIntId()
		assert.NotNil(t, intIds)
		assert.Equal(t, []int64{1, 2, 3}, intIds.Data)
	})

	t.Run("int64 pk with float64 values having fractional part", func(t *testing.T) {
		ids := rawIDs(`1.5`)
		_, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not an integer in the int64 range")
	})

	t.Run("int64 pk with float64 values - second element has fractional part", func(t *testing.T) {
		ids := rawIDs(`1`, `2.9`)
		_, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "index 1")
		assert.Contains(t, err.Error(), "is not an integer in the int64 range")
	})

	t.Run("int64 pk with int64 values", func(t *testing.T) {
		ids := rawIDs(`100`, `200`)
		result, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		intIds := result.GetIntId()
		assert.NotNil(t, intIds)
		assert.Equal(t, []int64{100, 200}, intIds.Data)
	})

	t.Run("int64 pk with int values", func(t *testing.T) {
		ids := rawIDs(`10`, `20`)
		result, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		intIds := result.GetIntId()
		assert.NotNil(t, intIds)
		assert.Equal(t, []int64{10, 20}, intIds.Data)
	})

	t.Run("int64 pk with valid string values", func(t *testing.T) {
		ids := rawIDs(`"123"`, `"456"`)
		result, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		intIds := result.GetIntId()
		assert.NotNil(t, intIds)
		assert.Equal(t, []int64{123, 456}, intIds.Data)
	})

	t.Run("int64 pk with invalid string values", func(t *testing.T) {
		ids := rawIDs(`"not_a_number"`)
		_, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid int64 id")
	})

	t.Run("int64 pk with invalid type", func(t *testing.T) {
		ids := rawIDs(`true`)
		_, err := convertIDsToSchemapbIDs(ids, int64PkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid id type")
	})

	t.Run("varchar pk with string values", func(t *testing.T) {
		ids := rawIDs(`"abc"`, `"def"`, `"ghi"`)
		result, err := convertIDsToSchemapbIDs(ids, varcharPkField)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		strIds := result.GetStrId()
		assert.NotNil(t, strIds)
		assert.Equal(t, []string{"abc", "def", "ghi"}, strIds.Data)
	})

	t.Run("varchar pk with empty string", func(t *testing.T) {
		ids := rawIDs(`""`)
		_, err := convertIDsToSchemapbIDs(ids, varcharPkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty string id")
	})

	t.Run("varchar pk with number values", func(t *testing.T) {
		ids := rawIDs(`123`, `456`, `789`)
		result, err := convertIDsToSchemapbIDs(ids, varcharPkField)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		strIds := result.GetStrId()
		assert.NotNil(t, strIds)
		assert.Equal(t, []string{"123", "456", "789"}, strIds.Data)
	})

	t.Run("varchar pk with invalid type", func(t *testing.T) {
		ids := rawIDs(`[1,2,3]`)
		_, err := convertIDsToSchemapbIDs(ids, varcharPkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid id type")
	})

	t.Run("unsupported pk type", func(t *testing.T) {
		boolPkField := &schemapb.FieldSchema{
			FieldID:      common.StartOfUserFieldID,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Bool,
		}
		ids := rawIDs(`1`)
		_, err := convertIDsToSchemapbIDs(ids, boolPkField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported primary key type")
	})
}

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

func TestSchemaForPathReplaceOperands(t *testing.T) {
	schema := buildStructArrayTestSchema()
	body := []byte(`{"data":[
		{"id":1,"my_struct":[{"sub_int":18}]},
		{"id":2,"my_struct":[{"sub_int":21}]}
	]}`)
	ops := []*schemapb.FieldPartialUpdateOp{{
		FieldName: "my_struct",
		Op:        schemapb.FieldPartialUpdateOp_PATH_REPLACE,
		Path:      "[1][sub_int]",
	}}

	requestSchema, err := schemaForPathReplaceOperands(body, schema, ops)
	require.NoError(t, err)
	require.Len(t, requestSchema.GetStructArrayFields()[0].GetFields(), 1)
	assert.Equal(t, "sub_int", subShortName(requestSchema.GetStructArrayFields()[0].GetFields()[0]))
	assert.Len(t, schema.GetStructArrayFields()[0].GetFields(), 2, "collection schema must not be mutated")

	rows, validData, err := checkAndSetData(body, requestSchema, true)
	require.NoError(t, err)
	fieldsData, err := anyToColumns(rows, validData, requestSchema, false, true)
	require.NoError(t, err)
	var structData *schemapb.FieldData
	for _, field := range fieldsData {
		if field.GetFieldName() == "my_struct" {
			structData = field
			break
		}
	}
	require.NotNil(t, structData)
	require.Len(t, structData.GetStructArrays().GetFields(), 1)
	assert.Equal(t, "sub_int", subShortName(requestSchema.GetStructArrayFields()[0].GetFields()[0]))
}

func TestSchemaForPathReplaceOperandsRejectsDifferentMasks(t *testing.T) {
	schema := buildStructArrayTestSchema()
	body := []byte(`{"data":[
		{"id":1,"my_struct":[{"sub_int":18}]},
		{"id":2,"my_struct":[{"sub_vec":[0.1,0.2,0.3,0.4]}]}
	]}`)
	ops := []*schemapb.FieldPartialUpdateOp{{FieldName: "my_struct", Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, Path: "[1]"}}

	_, err := schemaForPathReplaceOperands(body, schema, ops)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match request mask")
}

func TestSchemaForPathReplaceOperandsKeepsBracketedDynamicKeyLiteral(t *testing.T) {
	schema := buildStructArrayTestSchema()
	schema.EnableDynamicField = true
	body := []byte(`{"data":[{
		"id":1,
		"my_struct":[{"sub_int":18}],
		"my_struct[1][sub_int]":"literal"
	}]}`)
	ops := []*schemapb.FieldPartialUpdateOp{{FieldName: "my_struct", Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, Path: "[1][sub_int]"}}

	requestSchema, err := schemaForPathReplaceOperands(body, schema, ops)
	require.NoError(t, err)
	rows, validData, err := checkAndSetData(body, requestSchema, true)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "literal", rows[0]["my_struct[1][sub_int]"])

	fieldsData, err := anyToColumns(rows, validData, requestSchema, false, true)
	require.NoError(t, err)
	var dynamicData *schemapb.FieldData
	for _, field := range fieldsData {
		if field.GetIsDynamic() {
			dynamicData = field
			break
		}
	}
	require.NotNil(t, dynamicData)
	require.Len(t, dynamicData.GetScalars().GetJsonData().GetData(), 1)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(dynamicData.GetScalars().GetJsonData().GetData()[0], &decoded))
	assert.Equal(t, "literal", decoded["my_struct[1][sub_int]"])
}

func TestSchemaForPathReplaceOperandsRejectsScalarArrayNull(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "c_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool},
		},
	}
	pathReplaceOps := []*schemapb.FieldPartialUpdateOp{{
		FieldName: "scores",
		Op:        schemapb.FieldPartialUpdateOp_PATH_REPLACE,
		Path:      "[1]",
	}}
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	for _, operand := range []string{`[null]`, `"[null]"`} {
		body := []byte(fmt.Sprintf(`{"data":[{"id":1,"scores":%s}]}`, operand))
		_, err := schemaForPathReplaceOperands(body, schema, pathReplaceOps)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "null operand element at index 0")
	}

	// The strict raw-token check belongs only to the new PATH_REPLACE
	// operation. It must not change compatibility-mode handling for an
	// ordinary whole-field replacement.
	body := []byte(`{"data":[{"id":1,"scores":[null]}]}`)
	requestSchema, err := schemaForPathReplaceOperands(body, schema, []*schemapb.FieldPartialUpdateOp{{
		FieldName: "scores",
		Op:        schemapb.FieldPartialUpdateOp_REPLACE,
	}})
	require.NoError(t, err)
	rows, _, err := checkAndSetData(body, requestSchema, false)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	array, ok := rows[0]["scores"].(*schemapb.ScalarField)
	require.True(t, ok)
	assert.Equal(t, []bool{false}, array.GetBoolData().GetData())
}

func TestStructArrayFieldSchemaGetProto(t *testing.T) {
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

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad",
		Fields: []FieldSchema{
			{FieldName: "raw_int", DataType: "Int32"},
		},
	}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad_pk",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_pk",
				DataType:        "Array",
				ElementDataType: "Int32",
				IsPrimary:       true,
			},
		},
	}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{FieldName: "empty"}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{
		FieldName: "dup",
		Fields: []FieldSchema{
			{FieldName: "s", DataType: "Array", ElementDataType: "Int32"},
			{FieldName: "s", DataType: "Array", ElementDataType: "Int32"},
		},
	}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad_nullable",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_null",
				DataType:        "Array",
				ElementDataType: "Int32",
				Nullable:        true,
			},
		},
	}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad_default",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_default",
				DataType:        "Array",
				ElementDataType: "Int32",
				DefaultValue:    float64(1),
			},
		},
	}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad_part",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_part",
				DataType:        "Array",
				ElementDataType: "Int32",
				IsPartitionKey:  true,
			},
		},
	}).GetProto(ctx)
	assert.Error(t, err)

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad_cluster",
		Fields: []FieldSchema{
			{
				FieldName:       "sub_cluster",
				DataType:        "Array",
				ElementDataType: "Int32",
				IsClusteringKey: true,
			},
		},
	}).GetProto(ctx)
	assert.Error(t, err)
}

func TestParseStructArrayRowScalar(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	raw := `[{"sub_int": 1, "sub_vec": [0.1, 0.2, 0.3, 0.4]},
	         {"sub_int": 2, "sub_vec": [0.5, 0.6, 0.7, 0.8]}]`
	row, err := parseStructArrayRow(raw, schema, false)
	require.NoError(t, err)
	require.Len(t, row, 2)

	scalar, ok := row["sub_int"].(*schemapb.ScalarField)
	require.True(t, ok)
	assert.Equal(t, []int32{1, 2}, scalar.GetIntData().GetData())

	vecField, ok := row["sub_vec"].(*schemapb.VectorField)
	require.True(t, ok)
	assert.Equal(t,
		[]float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
		vecField.GetFloatVector().GetData())
}

func TestParseStructArrayRowMissingField(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	_, err := parseStructArrayRow(`[{"sub_int": 1}]`, schema, false)
	assert.Error(t, err)
}

func TestParseStructArrayRowNotArray(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	_, err := parseStructArrayRow(`{"sub_int": 1}`, schema, false)
	assert.Error(t, err)
}

func TestBuildStructArrayFieldDataRoundTrip(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	r1, err := parseStructArrayRow(`[{"sub_int": 1, "sub_vec": [0.1, 0.2, 0.3, 0.4]},
	                                 {"sub_int": 2, "sub_vec": [0.5, 0.6, 0.7, 0.8]}]`, schema, false)
	require.NoError(t, err)
	r2, err := parseStructArrayRow(`[{"sub_int": 3, "sub_vec": [0.9, 1.0, 1.1, 1.2]}]`, schema, false)
	require.NoError(t, err)

	fd, err := buildStructArrayFieldData(schema, []structArrayRow{r1, r2})
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_ArrayOfStruct, fd.GetType())
	subs := fd.GetStructArrays().GetFields()
	require.Len(t, subs, 2)

	assert.Equal(t, schemapb.DataType_Array, subs[0].GetType())
	assert.Len(t, subs[0].GetScalars().GetArrayData().GetData(), 2)
	assert.Equal(t, schemapb.DataType_ArrayOfVector, subs[1].GetType())
	assert.Len(t, subs[1].GetVectors().GetVectorArray().GetData(), 2)

	accessor, err := newStructArrayRowAccessor(fd, buildStructArrayTestSchema())
	require.NoError(t, err)
	extracted0, err := accessor.row(0, true)
	require.NoError(t, err)
	require.Len(t, extracted0, 2)
	assert.EqualValues(t, int32(1), extracted0[0]["sub_int"])
	assert.EqualValues(t, []float32{0.1, 0.2, 0.3, 0.4}, extracted0[0]["sub_vec"])

	extracted1, err := accessor.row(1, true)
	require.NoError(t, err)
	require.Len(t, extracted1, 1)
	assert.EqualValues(t, int32(3), extracted1[0]["sub_int"])
}

func TestAnyToColumnsStructArray(t *testing.T) {
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

	arrayData := subs[0].GetScalars().GetArrayData().GetData()
	require.Len(t, arrayData, 2)
	assert.Equal(t, []int32{10, 20}, arrayData[0].GetIntData().GetData())
	assert.Equal(t, []int32{30}, arrayData[1].GetIntData().GetData())

	vecData := subs[1].GetVectors().GetVectorArray().GetData()
	require.Len(t, vecData, 2)
	assert.Len(t, vecData[0].GetFloatVector().GetData(), 8)
	assert.Len(t, vecData[1].GetFloatVector().GetData(), 4)
}

func TestAnyToColumnsStructArrayAsOnlyVectorField(t *testing.T) {
	schema := buildStructArrayTestSchema()
	schema.Fields = schema.Fields[:1]
	body := []byte(`{
		"data": [
			{
				"id": 1,
				"my_struct": [
					{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}
				]
			}
		]
	}`)

	rows, _, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)

	fds, err := anyToColumns(rows, nil, schema, true, false)
	require.NoError(t, err)
	require.Len(t, fds, 2)
	assert.Equal(t, schemapb.DataType_Int64, fds[0].GetType())
	assert.Equal(t, schemapb.DataType_ArrayOfStruct, fds[1].GetType())
}

func TestAnyToColumnsRejectsSchemaWithoutVectorOrFunction(t *testing.T) {
	schema := buildStructArrayTestSchema()
	schema.Fields = schema.Fields[:1]
	schema.StructArrayFields[0].Fields = schema.StructArrayFields[0].Fields[:1]
	body := []byte(`{
		"data": [
			{
				"id": 1,
				"my_struct": [{"sub_int": 10}]
			}
		]
	}`)

	rows, _, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)

	_, err = anyToColumns(rows, nil, schema, true, false)
	require.ErrorContains(t, err, "has no vector field or functions")
}

func TestAnyToColumnsNullableStructArray(t *testing.T) {
	schema := buildStructArrayTestSchema()
	schema.GetStructArrayFields()[0].Nullable = true
	body := []byte(`{
		"data": [
			{
				"id": 1,
				"vec": [0.1, 0.2, 0.3, 0.4],
				"my_struct": [{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}]
			},
			{
				"id": 2,
				"vec": [0.5, 0.6, 0.7, 0.8],
				"my_struct": null
			},
			{
				"id": 3,
				"vec": [0.9, 1.0, 1.1, 1.2],
				"my_struct": []
			}
		]
	}`)
	rows, validData, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, true}, validData["my_struct"])

	fds, err := anyToColumns(rows, validData, schema, true, false)
	require.NoError(t, err)
	var structFD *schemapb.FieldData
	for _, fd := range fds {
		if fd.GetType() == schemapb.DataType_ArrayOfStruct {
			structFD = fd
			break
		}
	}
	require.NotNil(t, structFD)
	subs := structFD.GetStructArrays().GetFields()
	require.Len(t, subs, 2)
	for _, sub := range subs {
		assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(sub))
	}
	assert.Len(t, subs[0].GetScalars().GetArrayData().GetData(), 2)
	assert.Len(t, subs[1].GetVectors().GetVectorArray().GetData(), 2)
}

func TestAnyToColumnsNullableStructArrayAllNull(t *testing.T) {
	schema := buildStructArrayTestSchema()
	schema.GetStructArrayFields()[0].Nullable = true
	body := []byte(`{
		"data": [
			{"id": 1, "vec": [0.1, 0.2, 0.3, 0.4], "my_struct": null},
			{"id": 2, "vec": [0.5, 0.6, 0.7, 0.8]}
		]
	}`)
	rows, validData, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)

	fds, err := anyToColumns(rows, validData, schema, true, false)
	require.NoError(t, err)
	var structFD *schemapb.FieldData
	for _, fd := range fds {
		if fd.GetType() == schemapb.DataType_ArrayOfStruct {
			structFD = fd
			break
		}
	}
	require.NotNil(t, structFD)
	subs := structFD.GetStructArrays().GetFields()
	require.Len(t, subs, 2)
	for _, sub := range subs {
		assert.Equal(t, []bool{false, false}, typeutil.GetFieldDataValidData(sub))
	}
	assert.Empty(t, subs[0].GetScalars().GetArrayData().GetData())
	assert.Empty(t, subs[1].GetVectors().GetVectorArray().GetData())
}

func TestBuildQueryRespStructArrayRoundTrip(t *testing.T) {
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

	blob, err := json.Marshal(resp[0]["my_struct"])
	require.NoError(t, err)
	var decoded []map[string]interface{}
	require.NoError(t, json.Unmarshal(blob, &decoded))
	require.Len(t, decoded, 2)
	assert.EqualValues(t, 10, decoded[0]["sub_int"])
}

func TestBuildQueryRespNullableStructArrayCompact(t *testing.T) {
	schema := buildStructArrayTestSchema()
	structSchema := schema.GetStructArrayFields()[0]
	structSchema.Nullable = true
	first, err := parseStructArrayRow(
		`[{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}]`, structSchema, false)
	require.NoError(t, err)
	third, err := parseStructArrayRow(`[]`, structSchema, false)
	require.NoError(t, err)
	structFD, err := buildNullableStructArrayFieldData(
		structSchema, []structArrayRow{first, third}, []bool{true, false, true})
	require.NoError(t, err)

	resp, err := buildQueryResp(0, []string{"my_struct"}, []*schemapb.FieldData{structFD}, nil, nil, true, schema)
	require.NoError(t, err)
	require.Len(t, resp, 3)
	assert.NotNil(t, resp[0]["my_struct"])
	assert.Nil(t, resp[1]["my_struct"])
	assert.Equal(t, []map[string]interface{}{}, resp[2]["my_struct"])
}

func TestBuildQueryRespNullableStructArrayDense(t *testing.T) {
	schema := buildStructArrayTestSchema()
	structSchema := schema.GetStructArrayFields()[0]
	structSchema.Nullable = true
	first, err := parseStructArrayRow(
		`[{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}]`, structSchema, false)
	require.NoError(t, err)
	nullPlaceholder, err := parseStructArrayRow(`[]`, structSchema, false)
	require.NoError(t, err)
	third, err := parseStructArrayRow(
		`[{"sub_int": 30, "sub_vec": [3.1, 3.2, 3.3, 3.4]}]`, structSchema, false)
	require.NoError(t, err)
	structFD, err := buildStructArrayFieldData(
		structSchema, []structArrayRow{first, nullPlaceholder, third})
	require.NoError(t, err)
	for _, sub := range structFD.GetStructArrays().GetFields() {
		typeutil.SetFieldDataValidData(sub, []bool{true, false, true})
	}

	resp, err := buildQueryResp(0, []string{"my_struct"}, []*schemapb.FieldData{structFD}, nil, nil, true, schema)
	require.NoError(t, err)
	require.Len(t, resp, 3)
	assert.NotNil(t, resp[0]["my_struct"])
	assert.Nil(t, resp[1]["my_struct"])
	assert.NotNil(t, resp[2]["my_struct"])
}

func TestBuildQueryRespNullableStructArrayRejectsMismatchedValidData(t *testing.T) {
	schema := buildStructArrayTestSchema()
	structSchema := schema.GetStructArrayFields()[0]
	row, err := parseStructArrayRow(
		`[{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4]}]`, structSchema, false)
	require.NoError(t, err)
	structFD, err := buildStructArrayFieldData(structSchema, []structArrayRow{row})
	require.NoError(t, err)
	subs := structFD.GetStructArrays().GetFields()
	require.Len(t, subs, 2)
	typeutil.SetFieldDataValidData(subs[0], []bool{true})
	typeutil.SetFieldDataValidData(subs[1], []bool{false})

	_, err = buildQueryResp(0, []string{"my_struct"}, []*schemapb.FieldData{structFD}, nil, nil, true, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent valid data")
}

func TestIsEmbeddingListData(t *testing.T) {
	assert.False(t, isEmbeddingListData(`{"data": [[0.1, 0.2, 0.3, 0.4]]}`))
	assert.False(t, isEmbeddingListData(`{"data": [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]}`))
	assert.False(t, isEmbeddingListData(`{"data": ["YmFzZTY0"]}`))
	assert.False(t, isEmbeddingListData(`{"data": ["YmFzZTY0", "YmFzZTY1"]}`))

	assert.True(t, isEmbeddingListData(`{"data": [[[0.1, 0.2, 0.3, 0.4]]]}`))
	assert.True(t, isEmbeddingListData(`{"data": [[[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]], [[0.9, 1.0, 1.1, 1.2]]]}`))
	assert.True(t, isEmbeddingListData(`{"data": [["YmFzZTY0", "YmFzZTY1"]]}`))

	assert.False(t, isEmbeddingListData(`{"data": []}`))
	assert.False(t, isEmbeddingListData(`{"data": "not-array"}`))
	assert.False(t, isEmbeddingListData(`{"data": [[]]}`))
}

func TestPrintStructArrayFieldsV2(t *testing.T) {
	schema := buildStructArrayTestSchema()
	schema.GetStructArrayFields()[0].Nullable = true
	printed := printStructArrayFieldsV2(schema.GetStructArrayFields())
	require.Len(t, printed, 1)
	entry := printed[0]
	assert.Equal(t, "my_struct", entry[HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_ArrayOfStruct.String(), entry[HTTPReturnFieldType])
	assert.Equal(t, true, entry[HTTPReturnFieldNullable])
	subs, ok := entry["fields"].([]gin.H)
	require.True(t, ok)
	require.Len(t, subs, 2)
	assert.Equal(t, "sub_int", subs[0][HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_Array.String(), subs[0][HTTPReturnFieldType])
	assert.Equal(t, schemapb.DataType_Int32.String(), subs[0][HTTPReturnFieldElementType])
	assert.Equal(t, "sub_vec", subs[1][HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_FloatVector.String(), subs[1][HTTPReturnFieldElementType])
}

func TestParseJSONInteger(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		bitSize int
		value   int64
		ok      bool
	}{
		{name: "int8 max decimal", raw: "127.0", bitSize: 8, value: 127, ok: true},
		{name: "int8 exponent", raw: "1e2", bitSize: 8, value: 100, ok: true},
		{name: "negative scale integer", raw: "100e-2", bitSize: 8, value: 1, ok: true},
		{name: "decimal exponent integer", raw: "1.2300e2", bitSize: 8, value: 123, ok: true},
		{name: "exact beyond float64", raw: "9007199254740993.0", bitSize: 64, value: 9007199254740993, ok: true},
		{name: "int64 max decimal", raw: "9223372036854775807.0", bitSize: 64, value: math.MaxInt64, ok: true},
		{name: "int64 min decimal", raw: "-9223372036854775808.0", bitSize: 64, value: math.MinInt64, ok: true},
		{name: "int64 max scaled", raw: "92233720368547758070e-1", bitSize: 64, value: math.MaxInt64, ok: true},
		{name: "int64 min scaled", raw: "-92233720368547758080e-1", bitSize: 64, value: math.MinInt64, ok: true},
		{name: "zero huge positive exponent", raw: "0.0e999999", bitSize: 64, value: 0, ok: true},
		{name: "negative zero huge negative exponent", raw: "-0e-999999", bitSize: 64, value: 0, ok: true},
		{name: "int8 overflow after scaling", raw: "1280e-1", bitSize: 8, ok: false},
		{name: "fraction", raw: "127.5", bitSize: 8, ok: false},
		{name: "negative scale fraction", raw: "100e-3", bitSize: 8, ok: false},
		{name: "precision-sensitive fraction", raw: "1.00000000000000000000000000001", bitSize: 64, ok: false},
		{name: "int64 max adjacent fraction", raw: "9223372036854775806.9", bitSize: 64, ok: false},
		{name: "int64 max scaled overflow", raw: "92233720368547758080e-1", bitSize: 64, ok: false},
		{name: "int64 min scaled overflow", raw: "-92233720368547758090e-1", bitSize: 64, ok: false},
		{name: "huge positive exponent", raw: "1e999999", bitSize: 64, ok: false},
		{name: "huge negative exponent", raw: "1e-999999", bitSize: 64, ok: false},
		{name: "leading plus", raw: "+1", bitSize: 64, ok: false},
		{name: "leading zero", raw: "01", bitSize: 64, ok: false},
		{name: "missing integer part", raw: ".1", bitSize: 64, ok: false},
		{name: "missing fraction", raw: "1.", bitSize: 64, ok: false},
		{name: "missing exponent", raw: "1e", bitSize: 64, ok: false},
		{name: "invalid bit size", raw: "1", bitSize: 7, ok: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, ok := parseJSONInteger(test.raw, test.bitSize)
			assert.Equal(t, test.ok, ok)
			if test.ok {
				assert.Equal(t, test.value, value)
			}
		})
	}

	largeScaledInteger := "1" + strings.Repeat("0", 4096) + "e-4096"
	value, ok := parseJSONInteger(largeScaledInteger, 64)
	require.True(t, ok)
	assert.Equal(t, int64(1), value)

	_, ok = parseJSONInteger("1e-999999", 64)
	assert.False(t, ok)
	allocations := testing.AllocsPerRun(1000, func() {
		parseJSONInteger("1e-999999", 64)
	})
	assert.LessOrEqual(t, allocations, float64(2))
}

func TestStructArrayScalarSubFieldTypes(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		raw         string
		assertFn    func(*testing.T, *schemapb.ScalarField)
	}{
		{
			name:        "bool",
			elementType: schemapb.DataType_Bool,
			raw:         `[true,false]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []bool{true, false}, sf.GetBoolData().GetData())
			},
		},
		{
			name:        "int8",
			elementType: schemapb.DataType_Int8,
			raw:         `[-128,127,127.0,1e2]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []int32{-128, 127, 127, 100}, sf.GetIntData().GetData())
			},
		},
		{
			name:        "int16",
			elementType: schemapb.DataType_Int16,
			raw:         `[-32768,32767]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []int32{-32768, 32767}, sf.GetIntData().GetData())
			},
		},
		{
			name:        "int32",
			elementType: schemapb.DataType_Int32,
			raw:         `[-2147483648,2147483647]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []int32{math.MinInt32, math.MaxInt32}, sf.GetIntData().GetData())
			},
		},
		{
			name:        "int64",
			elementType: schemapb.DataType_Int64,
			raw:         `[-9223372036854775808,9223372036854775807,9007199254740993.0]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []int64{math.MinInt64, math.MaxInt64, 9007199254740993}, sf.GetLongData().GetData())
			},
		},
		{
			name:        "float",
			elementType: schemapb.DataType_Float,
			raw:         `[1.5,2.5]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []float32{1.5, 2.5}, sf.GetFloatData().GetData())
			},
		},
		{
			name:        "double",
			elementType: schemapb.DataType_Double,
			raw:         `[1.25,2.25]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []float64{1.25, 2.25}, sf.GetDoubleData().GetData())
			},
		},
		{
			name:        "string",
			elementType: schemapb.DataType_VarChar,
			raw:         `["red","blue"]`,
			assertFn: func(t *testing.T, sf *schemapb.ScalarField) {
				assert.Equal(t, []string{"red", "blue"}, sf.GetStringData().GetData())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub := &schemapb.FieldSchema{Name: test.name, ElementType: test.elementType}
			got, err := buildStructSubArrayScalar(sub, gjson.Parse(test.raw).Array(), false)
			require.NoError(t, err)
			test.assertFn(t, got)
		})
	}

	_, err := buildStructSubArrayScalar(&schemapb.FieldSchema{Name: "bad", ElementType: schemapb.DataType_JSON}, gjson.Parse(`[{}]`).Array(), false)
	assert.Error(t, err)

	_, err = buildStructSubArrayScalar(&schemapb.FieldSchema{Name: "bad_bool", ElementType: schemapb.DataType_Bool}, gjson.Parse(`[1]`).Array(), false)
	assert.Error(t, err)
}

func TestStructArrayNarrowIntegerSubFieldValidation(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		raw         string
		rangeText   string
	}{
		{name: "int8 above max", elementType: schemapb.DataType_Int8, raw: `[128]`, rangeText: "[-128, 127]"},
		{name: "int8 below min", elementType: schemapb.DataType_Int8, raw: `[-129]`, rangeText: "[-128, 127]"},
		{name: "int8 int32 wraparound", elementType: schemapb.DataType_Int8, raw: `[4294967296]`, rangeText: "[-128, 127]"},
		{name: "int8 fraction", elementType: schemapb.DataType_Int8, raw: `[127.5]`, rangeText: "[-128, 127]"},
		{name: "int16 above max", elementType: schemapb.DataType_Int16, raw: `[32768]`, rangeText: "[-32768, 32767]"},
		{name: "int16 below min", elementType: schemapb.DataType_Int16, raw: `[-32769]`, rangeText: "[-32768, 32767]"},
		{name: "int16 int32 wraparound", elementType: schemapb.DataType_Int16, raw: `[4294967296]`, rangeText: "[-32768, 32767]"},
		{name: "int32 above max", elementType: schemapb.DataType_Int32, raw: `[2147483648]`, rangeText: "[-2147483648, 2147483647]"},
		{name: "int32 below min", elementType: schemapb.DataType_Int32, raw: `[-2147483649]`, rangeText: "[-2147483648, 2147483647]"},
		{name: "int64 above max", elementType: schemapb.DataType_Int64, raw: `[9223372036854775808]`, rangeText: "[-9223372036854775808, 9223372036854775807]"},
		{name: "int64 below min", elementType: schemapb.DataType_Int64, raw: `[-9223372036854775809]`, rangeText: "[-9223372036854775808, 9223372036854775807]"},
		{name: "int64 fraction", elementType: schemapb.DataType_Int64, raw: `[127.5]`, rangeText: "[-9223372036854775808, 9223372036854775807]"},
		{name: "int64 huge positive exponent", elementType: schemapb.DataType_Int64, raw: `[1e999999]`, rangeText: "[-9223372036854775808, 9223372036854775807]"},
		{name: "int64 huge negative exponent", elementType: schemapb.DataType_Int64, raw: `[1e-999999]`, rangeText: "[-9223372036854775808, 9223372036854775807]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub := &schemapb.FieldSchema{Name: "narrow", ElementType: test.elementType}
			_, err := buildStructSubArrayScalar(sub, gjson.Parse(test.raw).Array(), false)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "sub-field narrow")
			assert.Contains(t, err.Error(), "value="+strings.TrimSuffix(strings.TrimPrefix(test.raw, "["), "]"))
			assert.Contains(t, err.Error(), test.rangeText)
		})
	}
}

func TestStructArrayVectorSubFieldTypes(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         string
		raw         string
		assertFn    func(*testing.T, *schemapb.VectorField)
	}{
		{
			name:        "float16",
			elementType: schemapb.DataType_Float16Vector,
			dim:         "2",
			raw:         `[[0.1,0.2],"AQIDBA=="]`,
			assertFn: func(t *testing.T, vf *schemapb.VectorField) {
				assert.Len(t, vf.GetFloat16Vector(), 8)
			},
		},
		{
			name:        "bfloat16",
			elementType: schemapb.DataType_BFloat16Vector,
			dim:         "2",
			raw:         `[[0.1,0.2],[0.3,0.4]]`,
			assertFn: func(t *testing.T, vf *schemapb.VectorField) {
				assert.Len(t, vf.GetBfloat16Vector(), 8)
			},
		},
		{
			name:        "binary",
			elementType: schemapb.DataType_BinaryVector,
			dim:         "16",
			raw:         `["AQI=","AwQ="]`,
			assertFn: func(t *testing.T, vf *schemapb.VectorField) {
				assert.Equal(t, []byte{1, 2, 3, 4}, vf.GetBinaryVector())
			},
		},
		{
			name:        "int8",
			elementType: schemapb.DataType_Int8Vector,
			dim:         "2",
			raw:         `[[1,-2],[3,4]]`,
			assertFn: func(t *testing.T, vf *schemapb.VectorField) {
				assert.Equal(t, []byte{1, 254, 3, 4}, vf.GetInt8Vector())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub := &schemapb.FieldSchema{
				Name:        test.name,
				ElementType: test.elementType,
				TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: test.dim}},
			}
			got, err := buildStructSubVectorField(sub, gjson.Parse(test.raw).Array())
			require.NoError(t, err)
			test.assertFn(t, got)
		})
	}

	_, err := buildStructSubVectorField(&schemapb.FieldSchema{
		Name:        "bad_dim",
		ElementType: schemapb.DataType_FloatVector,
		TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
	}, gjson.Parse(`[[0.1]]`).Array())
	assert.Error(t, err)

	_, err = buildStructSubVectorField(&schemapb.FieldSchema{
		Name:        "bad_type",
		ElementType: schemapb.DataType_JSON,
		TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
	}, gjson.Parse(`[{}]`).Array())
	assert.Error(t, err)
}

func TestEmbeddingListPlaceholderTypes(t *testing.T) {
	tests := []struct {
		name            string
		elementType     schemapb.DataType
		body            string
		dim             int64
		placeholderType commonpb.PlaceholderType
		valueLen        int
	}{
		{
			name:            "float",
			elementType:     schemapb.DataType_FloatVector,
			body:            `{"data": [[[0.1,0.2],[0.3,0.4]]]}`,
			dim:             2,
			placeholderType: commonpb.PlaceholderType_EmbListFloatVector,
			valueLen:        16,
		},
		{
			name:            "float16",
			elementType:     schemapb.DataType_Float16Vector,
			body:            `{"data": [[[0.1,0.2],[0.3,0.4]]]}`,
			dim:             2,
			placeholderType: commonpb.PlaceholderType_EmbListFloat16Vector,
			valueLen:        8,
		},
		{
			name:            "bfloat16",
			elementType:     schemapb.DataType_BFloat16Vector,
			body:            `{"data": [[[0.1,0.2],[0.3,0.4]]]}`,
			dim:             2,
			placeholderType: commonpb.PlaceholderType_EmbListBFloat16Vector,
			valueLen:        8,
		},
		{
			name:            "binary",
			elementType:     schemapb.DataType_BinaryVector,
			body:            `{"data": [["AQI=","AwQ="]]}`,
			dim:             16,
			placeholderType: commonpb.PlaceholderType_EmbListBinaryVector,
			valueLen:        4,
		},
		{
			name:            "int8",
			elementType:     schemapb.DataType_Int8Vector,
			body:            `{"data": [[[1,2],[3,4]]]}`,
			dim:             2,
			placeholderType: commonpb.PlaceholderType_EmbListInt8Vector,
			valueLen:        4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := convertEmbListQueries2Placeholder(test.body, test.elementType, test.dim)
			require.NoError(t, err)
			assert.Equal(t, test.placeholderType, got.GetType())
			require.Len(t, got.GetValues(), 1)
			assert.Len(t, got.GetValues()[0], test.valueLen)
		})
	}

	for _, test := range []struct {
		name        string
		body        string
		elementType schemapb.DataType
		dim         int64
	}{
		{name: "data_not_array", body: `{"data":"bad"}`, elementType: schemapb.DataType_FloatVector, dim: 2},
		{name: "empty_data", body: `{"data":[]}`, elementType: schemapb.DataType_FloatVector, dim: 2},
		{name: "query_not_array", body: `{"data":[1]}`, elementType: schemapb.DataType_FloatVector, dim: 2},
		{name: "empty_embedding_list", body: `{"data":[[]]}`, elementType: schemapb.DataType_FloatVector, dim: 2},
		{name: "unsupported_element", body: `{"data":[[true]]}`, elementType: schemapb.DataType_Bool, dim: 2},
		{name: "float_dim_mismatch", body: `{"data":[[[0.1]]]}`, elementType: schemapb.DataType_FloatVector, dim: 2},
		{name: "binary_not_string", body: `{"data":[[[1,2]]]}`, elementType: schemapb.DataType_BinaryVector, dim: 16},
		{name: "int8_not_array", body: `{"data":[["AQI="]]}`, elementType: schemapb.DataType_Int8Vector, dim: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := convertEmbListQueries2Placeholder(test.body, test.elementType, test.dim)
			assert.Error(t, err)
		})
	}
}

func TestGeneratePlaceholderGroupStructArrayField(t *testing.T) {
	schema := buildStructArrayTestSchema()

	standard, err := generatePlaceholderGroup(context.Background(), `{"data": [[0.1,0.2,0.3,0.4]]}`, schema, "sub_vec")
	require.NoError(t, err)
	group := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(standard, group))
	require.Len(t, group.GetPlaceholders(), 1)
	assert.Equal(t, commonpb.PlaceholderType_FloatVector, group.GetPlaceholders()[0].GetType())

	embList, err := generatePlaceholderGroup(context.Background(), `{"data": [[[0.1,0.2,0.3,0.4]]]}`, schema, "sub_vec")
	require.NoError(t, err)
	group.Reset()
	require.NoError(t, proto.Unmarshal(embList, group))
	require.Len(t, group.GetPlaceholders(), 1)
	assert.Equal(t, commonpb.PlaceholderType_EmbListFloatVector, group.GetPlaceholders()[0].GetType())
}

func TestStructArrayFieldDataErrorPaths(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]

	_, err := buildStructArrayFieldData(schema, nil)
	assert.Error(t, err)

	_, err = buildStructArrayFieldData(schema, []structArrayRow{{"sub_int": &schemapb.ScalarField{}}})
	assert.Error(t, err)

	_, err = buildStructArrayFieldData(schema, []structArrayRow{
		{
			"sub_int": int64(1),
			"sub_vec": &schemapb.VectorField{},
		},
	})
	assert.Error(t, err)

	badTypeSchema := &schemapb.StructArrayFieldSchema{
		Name:   "bad",
		Fields: []*schemapb.FieldSchema{{Name: "sub", DataType: schemapb.DataType_Bool}},
	}
	_, err = buildStructArrayFieldData(badTypeSchema, []structArrayRow{{"sub": true}})
	assert.Error(t, err)

	noDimSchema := &schemapb.StructArrayFieldSchema{
		Name: "bad_dim",
		Fields: []*schemapb.FieldSchema{{
			Name:     "sub_vec",
			DataType: schemapb.DataType_ArrayOfVector,
		}},
	}
	_, err = buildStructArrayFieldData(noDimSchema, []structArrayRow{{"sub_vec": &schemapb.VectorField{}}})
	assert.Error(t, err)
}

func TestExtractStructArrayRowErrorPaths(t *testing.T) {
	accessor, err := newStructArrayRowAccessor(&schemapb.FieldData{
		Type: schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{},
		},
	}, buildStructArrayTestSchema())
	require.NoError(t, err)
	empty, err := accessor.row(0, true)
	require.NoError(t, err)
	assert.Empty(t, empty)

	schema := buildStructArrayTestSchema()
	row, err := parseStructArrayRow(`[{"sub_int": 1, "sub_vec": [0.1, 0.2, 0.3, 0.4]}]`, schema.GetStructArrayFields()[0], false)
	require.NoError(t, err)
	fd, err := buildStructArrayFieldData(schema.GetStructArrayFields()[0], []structArrayRow{row})
	require.NoError(t, err)

	accessor, err = newStructArrayRowAccessor(fd, schema)
	require.NoError(t, err)
	_, err = accessor.row(2, true)
	assert.Error(t, err)

	mismatch := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: "my_struct",
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Array,
					FieldName: "sub_int",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
							Data: []*schemapb.ScalarField{{
								Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}},
							}},
						}},
					}},
				},
				{
					Type:      schemapb.DataType_ArrayOfVector,
					FieldName: "sub_vec",
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
							ElementType: schemapb.DataType_FloatVector,
							Data: []*schemapb.VectorField{{
								Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2, 0.3, 0.4}}},
							}},
						}},
					}},
				},
			}},
		},
	}
	accessor, err = newStructArrayRowAccessor(mismatch, schema)
	require.NoError(t, err)
	_, err = accessor.row(0, true)
	assert.Error(t, err)

	missingDimSchema := &schemapb.CollectionSchema{
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{Name: "my_struct"}},
	}
	accessor, err = newStructArrayRowAccessor(mismatch, missingDimSchema)
	require.NoError(t, err)
	_, err = accessor.row(0, true)
	assert.Error(t, err)

	unsupported := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: "my_struct",
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Array,
					FieldName: "sub_int",
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
							Data: []*schemapb.ScalarField{{
								Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}},
							}},
						}},
					}},
				},
				{Type: schemapb.DataType_Bool, FieldName: "bad"},
			}},
		},
	}
	accessor, err = newStructArrayRowAccessor(unsupported, schema)
	require.NoError(t, err)
	_, err = accessor.row(0, true)
	assert.Error(t, err)
}

func TestStructArrayHelperValueConversions(t *testing.T) {
	assert.Equal(t, "sub", structFieldShortName("my_struct[sub]"))
	assert.Equal(t, "plain", structFieldShortName("plain"))

	scalars := []*schemapb.ScalarField{
		{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}},
		{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1}}}},
		{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{2}}}},
		{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{3.5}}}},
		{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{4.5}}}},
		{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"five"}}}},
	}
	for _, scalar := range scalars {
		assert.Len(t, scalarArrayToInterfaces(scalar, true), 1)
	}
	assert.Nil(t, scalarArrayToInterfaces(&schemapb.ScalarField{}, true))

	vectorTests := []struct {
		name        string
		elementType schemapb.DataType
		vector      *schemapb.VectorField
		dim         int64
		count       int
	}{
		{
			name:        "float",
			elementType: schemapb.DataType_FloatVector,
			vector: &schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2, 0.3, 0.4}},
			}},
			dim:   2,
			count: 2,
		},
		{
			name:        "float16",
			elementType: schemapb.DataType_Float16Vector,
			vector:      &schemapb.VectorField{Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4}}},
			dim:         2,
			count:       1,
		},
		{
			name:        "bfloat16",
			elementType: schemapb.DataType_BFloat16Vector,
			vector:      &schemapb.VectorField{Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{1, 2, 3, 4}}},
			dim:         2,
			count:       1,
		},
		{
			name:        "binary",
			elementType: schemapb.DataType_BinaryVector,
			vector:      &schemapb.VectorField{Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}}},
			dim:         16,
			count:       1,
		},
		{
			name:        "int8",
			elementType: schemapb.DataType_Int8Vector,
			vector:      &schemapb.VectorField{Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 254}}},
			dim:         2,
			count:       1,
		},
	}
	for _, test := range vectorTests {
		t.Run(test.name, func(t *testing.T) {
			count, err := vectorFieldElemCount(test.vector, test.elementType, test.dim)
			require.NoError(t, err)
			assert.Equal(t, test.count, count)
			values, err := vectorFieldToInterfaces(test.vector, test.elementType, test.dim)
			require.NoError(t, err)
			assert.Len(t, values, test.count)
		})
	}

	_, err := vectorFieldElemCount(&schemapb.VectorField{}, schemapb.DataType_FloatVector, 0)
	assert.Error(t, err)
	_, err = vectorFieldElemCount(&schemapb.VectorField{}, schemapb.DataType_JSON, 1)
	assert.Error(t, err)
	_, err = vectorFieldToInterfaces(&schemapb.VectorField{}, schemapb.DataType_FloatVector, 0)
	assert.Error(t, err)
	_, err = vectorFieldToInterfaces(&schemapb.VectorField{}, schemapb.DataType_JSON, 1)
	assert.Error(t, err)
}

func TestStructArrayCheckAndSetPartialUpdate(t *testing.T) {
	schema := buildStructArrayTestSchema()
	body := []byte(`{"data": [{"id": 1, "vec": [0.1,0.2,0.3,0.4]}]}`)
	rows, validData, err := checkAndSetData(body, schema, true)
	require.NoError(t, err)
	assert.NotContains(t, rows[0], "my_struct")
	fds, err := anyToColumns(rows, validData, schema, false, true)
	require.NoError(t, err)
	for _, fd := range fds {
		assert.NotEqual(t, schemapb.DataType_ArrayOfStruct, fd.GetType())
	}

	_, _, err = checkAndSetData(body, schema, false)
	assert.Error(t, err)
}

func TestStructArrayFieldDataValueCount(t *testing.T) {
	schema := buildStructArrayTestSchema().GetStructArrayFields()[0]
	row, err := parseStructArrayRow(`[{"sub_int": 1, "sub_vec": [0.1,0.2,0.3,0.4]}]`, schema, false)
	require.NoError(t, err)
	fd, err := buildStructArrayFieldData(schema, []structArrayRow{row})
	require.NoError(t, err)
	count, err := fieldDataValueCount(fd)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	subs := fd.GetStructArrays().GetFields()
	fd.GetStructArrays().Fields = []*schemapb.FieldData{subs[1], subs[0]}
	count, err = fieldDataValueCount(fd)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	fd.GetStructArrays().Fields = nil
	count, err = fieldDataValueCount(fd)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	fd.GetStructArrays().Fields = []*schemapb.FieldData{{Type: schemapb.DataType_Bool}}
	_, err = fieldDataValueCount(fd)
	assert.Error(t, err)
}

func TestStructArrayFieldSchemaGetProtoTypeParams(t *testing.T) {
	proto, err := (&StructArrayFieldSchema{
		FieldName:   "my_struct",
		Description: "with params",
		Nullable:    true,
		TypeParams: map[string]interface{}{
			common.MaxCapacityKey: 8,
		},
		Fields: []FieldSchema{
			{FieldName: "sub_int", DataType: "Array", ElementDataType: "Int32"},
		},
	}).GetProto(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "my_struct", proto.GetName())
	assert.Equal(t, "with params", proto.GetDescription())
	assert.True(t, proto.GetNullable())
	require.Len(t, proto.GetTypeParams(), 1)
	assert.Equal(t, common.MaxCapacityKey, proto.GetTypeParams()[0].GetKey())
	assert.Equal(t, "8", proto.GetTypeParams()[0].GetValue())
	subParams := proto.GetFields()[0].GetTypeParams()
	require.Len(t, subParams, 1)
	assert.Equal(t, common.MaxCapacityKey, subParams[0].GetKey())
	assert.Equal(t, "8", subParams[0].GetValue())

	_, err = (&StructArrayFieldSchema{
		FieldName: "bad_params",
		TypeParams: map[string]interface{}{
			"bad": func() {},
		},
		Fields: []FieldSchema{
			{FieldName: "sub_int", DataType: "Array", ElementDataType: "Int32"},
		},
	}).GetProto(context.Background())
	assert.Error(t, err)
}

func TestFieldSchemaStructArrayHelpers(t *testing.T) {
	var nilField *FieldSchema
	assert.False(t, nilField.IsStructArrayField())
	assert.False(t, (&FieldSchema{DataType: "Int64"}).IsStructArrayField())
	assert.True(t, (&FieldSchema{DataType: "Array", ElementDataType: "Struct"}).IsStructArrayField())
	assert.True(t, (&FieldSchema{DataType: "ArrayOfStruct"}).IsStructArrayField())

	proto, err := (&FieldSchema{
		FieldName:       "clips",
		Description:     "clip metadata",
		DataType:        "Array",
		ElementDataType: "Struct",
		Nullable:        true,
		ElementTypeParams: map[string]interface{}{
			common.MaxCapacityKey: 16,
		},
		TypeParams: map[string]interface{}{
			common.MaxCapacityKey: 32,
		},
		Fields: []FieldSchema{
			{
				FieldName:       "tag",
				DataType:        "Array",
				ElementDataType: "VarChar",
				ElementTypeParams: map[string]interface{}{
					common.MaxLengthKey: 64,
				},
			},
			{
				FieldName:       "scores",
				DataType:        "Array",
				ElementDataType: "Int64",
				ElementTypeParams: map[string]interface{}{
					common.MaxCapacityKey: 7,
				},
			},
		},
	}).GetStructArrayProto(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "clips", proto.GetName())
	assert.Equal(t, "clip metadata", proto.GetDescription())
	assert.True(t, proto.GetNullable())
	assert.Equal(t, "32", kvPairsToMap(proto.GetTypeParams())[common.MaxCapacityKey])

	require.Len(t, proto.GetFields(), 2)
	tagParams := kvPairsToMap(proto.GetFields()[0].GetTypeParams())
	assert.Equal(t, "64", tagParams[common.MaxLengthKey])
	assert.Equal(t, "32", tagParams[common.MaxCapacityKey])

	scoreParams := kvPairsToMap(proto.GetFields()[1].GetTypeParams())
	assert.Equal(t, "7", scoreParams[common.MaxCapacityKey])
}

func TestPrintStructArrayFieldsV2QualifiedSubFields(t *testing.T) {
	printed := printStructArrayFieldsV2([]*schemapb.StructArrayFieldSchema{
		{
			FieldID:     10,
			Name:        "my_struct",
			Description: "qualified names",
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     11,
					Name:        "my_struct[sub_int]",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
				{
					FieldID:     12,
					Name:        "my_struct[sub_vec]",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
				},
			},
		},
	})
	require.Len(t, printed, 1)
	entry := printed[0]
	assert.Equal(t, "my_struct", entry[HTTPReturnFieldName])
	params := entry[Params].([]*commonpb.KeyValuePair)
	require.Len(t, params, 1)
	assert.Equal(t, common.MaxCapacityKey, params[0].GetKey())
	assert.Equal(t, "16", params[0].GetValue())

	subs := entry["fields"].([]gin.H)
	require.Len(t, subs, 2)
	assert.Equal(t, "sub_int", subs[0][HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_Int32.String(), subs[0][HTTPReturnFieldElementType])
	assert.Equal(t, "sub_vec", subs[1][HTTPReturnFieldName])
	assert.Equal(t, schemapb.DataType_FloatVector.String(), subs[1][HTTPReturnFieldElementType])
}

func TestCheckAndSetDataStructArrayRows(t *testing.T) {
	schema := buildStructArrayTestSchema()
	body := []byte(`{"data": [
		{
			"id": 1,
			"vec": [0.1, 0.2, 0.3, 0.4],
			"my_struct": [
				{"sub_int": 10, "sub_vec": [1.1, 1.2, 1.3, 1.4], "ignored": true}
			]
		}
	]}`)

	rows, validData, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)
	require.Empty(t, validData)
	require.Len(t, rows, 1)
	structRow, ok := rows[0]["my_struct"].(structArrayRow)
	require.True(t, ok)
	assert.Contains(t, structRow, "sub_int")
	assert.Contains(t, structRow, "sub_vec")

	int64Schema := buildStructArrayTestSchema()
	int64Schema.GetStructArrayFields()[0].GetFields()[0].ElementType = schemapb.DataType_Int64
	int64Body := []byte(`{"data":[{"id":1,"vec":[0.1,0.2,0.3,0.4],"my_struct":[{"sub_int":9007199254740993.0,"sub_vec":[1.1,1.2,1.3,1.4]}]}]}`)
	int64Rows, _, err := checkAndSetData(int64Body, int64Schema, false)
	require.NoError(t, err)
	int64StructRow, ok := int64Rows[0]["my_struct"].(structArrayRow)
	require.True(t, ok)
	int64Scalar, ok := int64StructRow["sub_int"].(*schemapb.ScalarField)
	require.True(t, ok)
	assert.Equal(t, []int64{9007199254740993}, int64Scalar.GetLongData().GetData())

	_, _, err = checkAndSetData([]byte(`{"data": [{"id": 1, "vec": [0.1,0.2,0.3,0.4], "my_struct": [1]}]}`), schema, false)
	assert.Error(t, err)

	for _, test := range []struct {
		name        string
		elementType schemapb.DataType
		value       string
		rangeText   string
	}{
		{name: "int8 int32 wraparound", elementType: schemapb.DataType_Int8, value: "4294967296", rangeText: "[-128, 127]"},
		{name: "int16 int32 wraparound", elementType: schemapb.DataType_Int16, value: "4294967296", rangeText: "[-32768, 32767]"},
		{name: "int32 overflow", elementType: schemapb.DataType_Int32, value: "2147483648", rangeText: "[-2147483648, 2147483647]"},
		{name: "fraction", elementType: schemapb.DataType_Int8, value: "127.5", rangeText: "[-128, 127]"},
		{name: "int64 overflow", elementType: schemapb.DataType_Int64, value: "9223372036854775808", rangeText: "[-9223372036854775808, 9223372036854775807]"},
		{name: "int64 fraction", elementType: schemapb.DataType_Int64, value: "127.5", rangeText: "[-9223372036854775808, 9223372036854775807]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			testSchema := buildStructArrayTestSchema()
			testSchema.GetStructArrayFields()[0].GetFields()[0].ElementType = test.elementType
			invalidBody := []byte(fmt.Sprintf(`{"data":[{"id":1,"vec":[0.1,0.2,0.3,0.4],"my_struct":[{"sub_int":%s,"sub_vec":[1.1,1.2,1.3,1.4]}]}]}`, test.value))
			_, _, err := checkAndSetData(invalidBody, testSchema, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "sub-field sub_int")
			assert.Contains(t, err.Error(), "value="+test.value)
			assert.Contains(t, err.Error(), test.rangeText)
		})
	}
}

func TestParseStructArrayRowQualifiedSchemaNames(t *testing.T) {
	schema := &schemapb.StructArrayFieldSchema{
		Name: "my_struct",
		Fields: []*schemapb.FieldSchema{
			{
				Name:        "my_struct[sub_int]",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
			{
				Name:        "my_struct[sub_vec]",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
		},
	}
	row, err := parseStructArrayRow(`[{"sub_int": 1, "sub_vec": [0.1,0.2], "unknown": "ignored"}]`, schema, false)
	require.NoError(t, err)
	assert.Contains(t, row, "sub_int")
	assert.Contains(t, row, "sub_vec")
	assert.NotContains(t, row, "unknown")

	unsupported := proto.Clone(schema).(*schemapb.StructArrayFieldSchema)
	unsupported.Fields = append(unsupported.Fields, &schemapb.FieldSchema{Name: "bad", DataType: schemapb.DataType_Bool})
	_, err = parseStructArrayRow(`[{"sub_int": 1, "sub_vec": [0.1,0.2], "bad": true}]`, unsupported, false)
	assert.Error(t, err)
}

func TestByteVectorElementErrorPaths(t *testing.T) {
	_, err := decodeByteVectorElement(gjson.Parse(`"bad-base64"`), 2, 4, true)
	assert.Error(t, err)

	_, err = decodeByteVectorElement(gjson.Parse(`"AQI="`), 2, 4, true)
	assert.Error(t, err)

	_, err = decodeByteVectorElement(gjson.Parse(`true`), 2, 4, true)
	assert.Error(t, err)

	_, err = decodeByteVectorElement(gjson.Parse(`[0.1]`), 2, 4, true)
	assert.Error(t, err)
}

func TestEncodeEmbListQueryErrorPaths(t *testing.T) {
	_, err := encodeEmbListQuery(gjson.Parse(`[[0.1]]`).Array(), schemapb.DataType_FloatVector, 2, 0)
	assert.Error(t, err)

	_, err = encodeEmbListQuery(gjson.Parse(`["AQI="]`).Array(), schemapb.DataType_BinaryVector, 32, 0)
	assert.Error(t, err)

	_, err = encodeEmbListQuery(gjson.Parse(`[[1, 2, 300]]`).Array(), schemapb.DataType_Int8Vector, 3, 0)
	assert.Error(t, err)

	_, err = encodeEmbListQuery(gjson.Parse(`[[true]]`).Array(), schemapb.DataType_Bool, 1, 0)
	assert.Error(t, err)
}

func int64FieldTestSchema() *schemapb.CollectionSchema {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	return &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
			{
				Name:     "count",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
}

func insertOneInt64Value(t *testing.T, value string) ([]map[string]interface{}, error) {
	t.Helper()
	body := []byte(fmt.Sprintf(
		`{"data": {"%s": 1, "vector": [0.1, 0.2], "count": %s}}`, FieldBookID, value))
	rows, _, err := checkAndSetData(body, int64FieldTestSchema(), false)
	return rows, err
}

// gjson's String() renders a number through float64 as soon as the raw text is
// not all digits, so a decimal or exponent form lost precision before reaching
// json.Number and was then accepted as a valid int64.
func TestCheckAndSetDataInt64FieldParsesRawLiteral(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected int64
	}{
		{"plain integer", `42`, 42},
		{"negative", `-7`, -7},
		{"int64 upper bound", `9223372036854775807`, 9223372036854775807},
		{"int64 lower bound", `-9223372036854775808`, -9223372036854775808},
		{"integer valued decimal", `1.0`, 1},
		{"integer valued exponent", `1e3`, 1000},
		{"negative exponent that is exact", `100e-2`, 1},
		{"beyond 2^53 as a plain integer", `9007199254740993`, 9007199254740993},
		// this used to be stored as 9007199254740992
		{"beyond 2^53 as a decimal", `9007199254740993.0`, 9007199254740993},
		{"quoted integer", `"42"`, 42},
		// base-10, not strconv base detection: this path carries Int64 primary keys
		{"quoted zero padded integer", `"010"`, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneInt64Value(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.expected, rows[0]["count"])
		})
	}
}

func TestCheckAndSetDataInt64FieldRejectsNonIntegers(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"fraction", `1.5`},
		{"exponent past int64", `1e19`},
		{"integer past int64", `9223372036854775808`},
		{"integer below int64", `-9223372036854775809`},
		// used to be stored as 0 because String() rendered it to "0"
		{"underflow to zero", `1e-400`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := insertOneInt64Value(t, tt.value)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "count")
		})
	}
}

// Quoted integers are read as base 10 for every integer width. cast relied on
// strconv's base detection, so "010" meant 8 in a narrow integer field and 10
// in an Int64 field.
func TestCheckAndSetDataQuotedIntegerIsDecimal(t *testing.T) {
	narrowSchema := func(dataType schemapb.DataType) *schemapb.CollectionSchema {
		vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		vectorField.Name = "vector"
		return &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				vectorField,
				{Name: "narrow", DataType: dataType},
			},
		}
	}

	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
	} {
		t.Run(dataType.String()+" zero padded", func(t *testing.T) {
			body := []byte(fmt.Sprintf(
				`{"data": {"%s": 1, "vector": [0.1, 0.2], "narrow": "010"}}`, FieldBookID))
			rows, _, err := checkAndSetData(body, narrowSchema(dataType), false)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			switch dataType {
			case schemapb.DataType_Int8:
				assert.Equal(t, int8(10), rows[0]["narrow"])
			case schemapb.DataType_Int16:
				assert.Equal(t, int16(10), rows[0]["narrow"])
			case schemapb.DataType_Int32:
				assert.Equal(t, int32(10), rows[0]["narrow"])
			}
		})

		t.Run(dataType.String()+" hex prefix is rejected", func(t *testing.T) {
			body := []byte(fmt.Sprintf(
				`{"data": {"%s": 1, "vector": [0.1, 0.2], "narrow": "0x10"}}`, FieldBookID))
			_, _, err := checkAndSetData(body, narrowSchema(dataType), false)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

// proxy.http.compatibilityMode restores the previous integer handling,
// including the two's complement wraparound this PR removes.
func TestCheckAndSetDataIntegerCompatibilityMode(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	narrowSchema := func(dataType schemapb.DataType) *schemapb.CollectionSchema {
		vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		vectorField.Name = "vector"
		return &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				vectorField,
				{Name: "narrow", DataType: dataType},
			},
		}
	}
	insert := func(t *testing.T, schema *schemapb.CollectionSchema, value string) map[string]interface{} {
		t.Helper()
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "narrow": %s}}`, FieldBookID, value))
		rows, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		return rows[0]
	}

	t.Run("int8 wraps again", func(t *testing.T) {
		assert.Equal(t, int8(-128), insert(t, narrowSchema(schemapb.DataType_Int8), `128`)["narrow"])
	})

	t.Run("int32 wraps again", func(t *testing.T) {
		assert.Equal(t, int32(0), insert(t, narrowSchema(schemapb.DataType_Int32), `4294967296`)["narrow"])
	})

	t.Run("quoted integer uses base detection again", func(t *testing.T) {
		assert.Equal(t, int8(8), insert(t, narrowSchema(schemapb.DataType_Int8), `"010"`)["narrow"])
	})

	t.Run("int64 loses precision again", func(t *testing.T) {
		rows, err := insertOneInt64Value(t, `9007199254740993.0`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(9007199254740992), rows[0]["count"])
	})
}

// proxy.http.compatibilityMode restores the behavior a client saw before a
// missing non-nullable field was rejected: the value was silently stored empty.
func TestCheckAndSetDataCompatibilityModeSwitch(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.CompatibilityMode.Key

	schema := func() *schemapb.CollectionSchema {
		vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		vectorField.Name = "vector"
		return &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				vectorField,
				{Name: "name", DataType: schemapb.DataType_VarChar},
			},
		}
	}
	missing := []byte(fmt.Sprintf(`{"data": {"%s": 1, "vector": [0.1, 0.2]}}`, FieldBookID))
	explicitNull := []byte(fmt.Sprintf(`{"data": {"%s": 1, "vector": [0.1, 0.2], "name": null}}`, FieldBookID))

	t.Run("default rejects a missing field", func(t *testing.T) {
		_, _, err := checkAndSetData(missing, schema(), false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterMissing)
	})

	t.Run("default rejects an explicit null", func(t *testing.T) {
		_, _, err := checkAndSetData(explicitNull, schema(), false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("enabled stores the empty value again", func(t *testing.T) {
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)

		rows, _, err := checkAndSetData(missing, schema(), false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "", rows[0]["name"])

		rows, _, err = checkAndSetData(explicitNull, schema(), false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "", rows[0]["name"])
	})

	t.Run("enabled does not weaken nullable handling", func(t *testing.T) {
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)

		nullableSchema := schema()
		nullableSchema.Fields[2].Nullable = true
		rows, validData, err := checkAndSetData(explicitNull, nullableSchema, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, []bool{false}, validData["name"])
	})
}

func jsonFieldTestSchema() *schemapb.CollectionSchema {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	return &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
			{
				Name:     "json_field",
				DataType: schemapb.DataType_JSON,
			},
		},
	}
}

func insertOneJSONValue(t *testing.T, value string) []byte {
	t.Helper()
	body := []byte(fmt.Sprintf(
		`{"data": {"%s": 1, "vector": [0.1, 0.2], "json_field": %s}}`, FieldBookID, value))
	rows, _, err := checkAndSetData(body, jsonFieldTestSchema(), false)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	stored, ok := rows[0]["json_field"].([]byte)
	require.True(t, ok, "json field must be stored as raw bytes")
	return stored
}

// The REST adapter used to store gjson's String() rendering of the JSON field.
// That unquotes JSON strings and renders numbers through float64, producing
// bytes that are not a JSON document at all. Keep the original token instead.
func TestCheckAndSetDataJSONFieldKeepsRawToken(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"object", `{"a": 1}`, `{"a": 1}`},
		{"nested object", `{"a": {"b": [1, 2]}}`, `{"a": {"b": [1, 2]}}`},
		{"array", `[1, 2, 3]`, `[1, 2, 3]`},
		{"bool", `true`, `true`},
		{"integer", `42`, `42`},
		{"negative", `-7`, `-7`},
		{"decimal", `10.0`, `10.0`},
		// gjson String() dropped the quotes here, storing `hello`.
		{"string", `"hello"`, `"hello"`},
		{"empty string", `""`, `""`},
		{"string with braces", `"{not json"`, `"{not json"`},
		// gjson String() rendered this through float64, storing a truncated
		// mantissa. Numbers outside the float64 range (1e400) never reach here:
		// the gin binder decodes `data` into []map[string]interface{} and
		// rejects them before checkAndSetData runs.
		{"integer beyond 2^53", `9007199254740993.0`, `9007199254740993.0`},
		{"integer beyond int64", `12345678901234567890`, `12345678901234567890`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := insertOneJSONValue(t, tt.value)
			assert.Equal(t, tt.expected, string(stored))
			assert.True(t, json.Valid(stored),
				"stored bytes must stay a valid JSON document, got %q", string(stored))
		})
	}
}

// A JSON document handed over as a JSON string keeps being unwrapped so that
// clients relying on that input form are not broken.
func TestCheckAndSetDataJSONFieldUnwrapsEncodedDocument(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"encoded object", `"{\"a\": 1}"`, `{"a": 1}`},
		{"encoded array", `"[1, 2]"`, `[1, 2]`},
		{"plain text stays quoted", `"hello"`, `"hello"`},
	}

	// While the field reads back as the text of its document, that text is the
	// field's wire form and decoding it is how a caller sends a document.
	params := paramtable.Get()
	key := params.HTTPCfg.NativeJSONResponse.Key
	params.Save(key, "false")
	defer params.Reset(key)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := insertOneJSONValue(t, tt.value)
			assert.Equal(t, tt.expected, string(stored))
			assert.True(t, json.Valid(stored))
		})
	}
}

// The response returns a JSON field as the document it holds, so every document
// it can return has to be sendable back. A document that is itself a string used
// to fail: it came back as the string it is, and the insert path read that
// string as the document's text and stored what it spelled -- a number for
// "123", a boolean for "true", an object for "{\"a\":1}".
func TestJSONFieldDocumentRoundTrips(t *testing.T) {
	require.True(t, paramtable.Get().HTTPCfg.NativeJSONResponse.GetAsBool(),
		"the round trip below is what returning documents natively requires")

	for _, document := range []string{
		`{"a":1}`,
		`[1,2]`,
		`"123"`,
		`"true"`,
		`"null"`,
		`"hello"`,
		`"{\"a\": 1}"`,
		`123`,
		`true`,
	} {
		t.Run(document, func(t *testing.T) {
			fieldData := &schemapb.FieldData{
				Type:      schemapb.DataType_JSON,
				FieldName: "meta",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(document)}},
						},
					},
				},
			}
			rows, err := buildQueryResp(int64(0), []string{"meta"}, []*schemapb.FieldData{fieldData}, nil, nil, true, nil)
			require.NoError(t, err)

			// What the caller receives, rendered by the encoder that serves it.
			payload, err := json.Marshal(rows[0]["meta"])
			require.NoError(t, err)
			assert.JSONEq(t, document, string(payload))

			// ...and sent straight back.
			stored := insertOneJSONValue(t, string(payload))
			assert.Equal(t, document, string(stored))
		})
	}
}

func dynamicFieldTestSchema() *schemapb.CollectionSchema {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	return &schemapb.CollectionSchema{
		Name:               DefaultCollectionName,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
		},
	}
}

func insertOneDynamicValue(t *testing.T, value string) ([]map[string]interface{}, error) {
	t.Helper()
	body := []byte(fmt.Sprintf(
		`{"data": {"%s": 1, "vector": [0.1, 0.2], "dyn": %s}}`, FieldBookID, value))
	rows, _, err := checkAndSetData(body, dynamicFieldTestSchema(), false)
	return rows, err
}

// A dynamic field is stored as JSON text, so decoding a number into
// int64/float64 and re-encoding it only loses information. cast.ToInt64 also
// discarded its error and yielded 0, so anything it could not parse -- 1e300,
// 1e19, integers beyond int64 -- was silently stored as 0.
func TestCheckAndSetDataDynamicFieldKeepsNumberLiteral(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"small integer", `1`, `1`},
		{"negative integer", `-7`, `-7`},
		{"zero", `0`, `0`},
		{"decimal keeps its fraction", `10.0`, `10.0`},
		{"fraction", `0.5`, `0.5`},
		{"integer beyond 2^53", `9007199254740993`, `9007199254740993`},
		{"decimal beyond 2^53", `9007199254740993.0`, `9007199254740993.0`},
		// each of these used to be stored as 0
		{"exponent form", `1e19`, `1e19`},
		{"large exponent", `1e300`, `1e300`},
		{"negative exponent", `1e-7`, `1e-7`},
		{"integer beyond int64", `12345678901234567890`, `12345678901234567890`},
		{"uint64 upper bound", `18446744073709551615`, `18446744073709551615`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneDynamicValue(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, json.Number(tt.expected), rows[0]["dyn"])

			// the dynamic field is serialized back to JSON before it is stored
			marshaled, err := json.Marshal(map[string]interface{}{"dyn": rows[0]["dyn"]})
			require.NoError(t, err)
			assert.Equal(t, `{"dyn":`+tt.expected+`}`, string(marshaled))
		})
	}
}

// simdjson reports BIGINT_ERROR for integer literals beyond 64 bits, so storing
// them would turn every query touching that path into an error. Reject at
// insert time instead.
func TestCheckAndSetDataDynamicFieldRejectsUnrepresentableNumber(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"one past uint64", `18446744073709551616`},
		{"far beyond uint64", `123456789012345678901234567890`},
		{"negative beyond int64", `-9223372036854775809`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := insertOneDynamicValue(t, tt.value)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "dyn")
			assert.Contains(t, err.Error(), "exceeds the 64-bit range")
		})
	}
}

// A JSON field whose document is itself a number gets the same 64-bit limit as
// a dynamic field: simdjson reports BIGINT_ERROR beyond that, so storing it
// would make every query touching the field fail instead of the insert.
func TestCheckAndSetDataJSONFieldRejectsUnrepresentableNumber(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"one past uint64", `18446744073709551616`},
		{"far beyond uint64", `123456789012345678901234567890`},
		{"negative beyond int64", `-9223372036854775809`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf(
				`{"data": {"%s": 1, "vector": [0.1, 0.2], "json_field": %s}}`, FieldBookID, tt.value))
			_, _, err := checkAndSetData(body, jsonFieldTestSchema(), false)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "json_field")
			assert.Contains(t, err.Error(), "exceeds the 64-bit range")
		})
	}
}

// Nested numbers get the same walk as top-level ones: an earlier version
// checked only the top level, so an oversized integer one brace deep was
// stored and made the row unreadable all the same.
func TestCheckAndSetDataJSONFieldRejectsNestedUnreadableNumbers(t *testing.T) {
	// nested numbers used to be left unchecked; they are now covered by the same
	// walk the duplicate-key scan makes
	body := []byte(fmt.Sprintf(
		`{"data": {"%s": 1, "vector": [0.1, 0.2], "json_field": {"a": 123456789012345678901234567890}}}`, FieldBookID))
	_, _, err := checkAndSetData(body, jsonFieldTestSchema(), false)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "exceeds the 64-bit range")
}

// gjson's Value() decodes a whole subtree into Go values, so every nested
// number went through float64 even after the top-level literal was preserved.
func TestCheckAndSetDataDynamicFieldKeepsNestedLiterals(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		// the value is kept as written; the encoder only compacts whitespace on
		// the way out, which the marshaled expectation below accounts for
		{"nested integer beyond 2^53", `{"a": 9007199254740993}`, `{"a":9007199254740993}`},
		{"nested integer beyond int64", `{"a": 12345678901234567890}`, `{"a":12345678901234567890}`},
		{"nested decimal", `{"a": 10.0}`, `{"a":10.0}`},
		{"nested exponent", `{"a": 1e300}`, `{"a":1e300}`},
		{"array element beyond 2^53", `[9007199254740993]`, `[9007199254740993]`},
		{"deeply nested", `{"a": {"b": [9007199254740993]}}`, `{"a":{"b":[9007199254740993]}}`},
		{"strings keep their escapes", `{"a": "x\"y"}`, `{"a":"x\"y"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneDynamicValue(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			// stored exactly as the caller wrote it, whitespace included
			assert.Equal(t, json.RawMessage(tt.value), rows[0]["dyn"])

			// and every value survives the encoder, which compacts whitespace
			marshaled, err := json.Marshal(map[string]interface{}{"dyn": rows[0]["dyn"]})
			require.NoError(t, err)
			assert.Equal(t, `{"dyn":`+tt.expected+`}`, string(marshaled))
		})
	}
}

// proxy.http.compatibilityMode restores the previous handling for both the
// JSON field and the dynamic field, including the values that were destroyed.
func TestCheckAndSetDataJSONAndDynamicCompatibilityMode(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	t.Run("json field is rendered again", func(t *testing.T) {
		// the unquoted form is what the field used to store
		assert.Equal(t, `hello`, string(insertOneJSONValue(t, `"hello"`)))
		assert.Equal(t, `9007199254740992`, string(insertOneJSONValue(t, `9007199254740993.0`)))
	})

	t.Run("json field accepts an oversized integer again", func(t *testing.T) {
		stored := insertOneJSONValue(t, `123456789012345678901234567890`)
		assert.Equal(t, `123456789012345678901234567890`, string(stored))
	})

	t.Run("dynamic field decodes again", func(t *testing.T) {
		rows, err := insertOneDynamicValue(t, `1e300`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(0), rows[0]["dyn"])

		rows, err = insertOneDynamicValue(t, `10.0`)
		require.NoError(t, err)
		assert.Equal(t, float64(10), rows[0]["dyn"])
	})

	t.Run("dynamic field accepts an oversized integer again", func(t *testing.T) {
		rows, err := insertOneDynamicValue(t, `123456789012345678901234567890`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(0), rows[0]["dyn"])
	})

	t.Run("nested values are decoded again", func(t *testing.T) {
		rows, err := insertOneDynamicValue(t, `{"a": 9007199254740993}`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		marshaled, err := json.Marshal(map[string]interface{}{"dyn": rows[0]["dyn"]})
		require.NoError(t, err)
		assert.Equal(t, `{"dyn":{"a":9007199254740992}}`, string(marshaled))
	})
}

// The engine-compatibility check on the assembled dynamic wrapper is gated on
// compatibilityMode like the per-value checks are: the wrapper adds one level
// of nesting, so a value nested to the per-document limit fails only once it
// is wrapped, and the previous handling stored that wrapper unexamined.
func TestDynamicWrapperCheckHonorsCompatibilityMode(t *testing.T) {
	paramtable.Init()

	// passes the per-value depth check on its own, fails once the wrapper's
	// object adds a level
	deep := strings.Repeat("[", 1023) + "1" + strings.Repeat("]", 1023)

	toColumns := func(t *testing.T) error {
		t.Helper()
		rows, err := insertOneDynamicValue(t, deep)
		require.NoError(t, err)
		_, err = anyToColumns(rows, nil, dynamicFieldTestSchema(), true, false)
		return err
	}

	t.Run("strict mode refuses the wrapped document", func(t *testing.T) {
		err := toColumns(t)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "nests deeper")
	})

	t.Run("compatibility mode stores it as before", func(t *testing.T) {
		key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)
		require.NoError(t, toColumns(t))
	})
}

func stringFieldTestSchema() *schemapb.CollectionSchema {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	return &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
			{Name: "name", DataType: schemapb.DataType_VarChar},
		},
	}
}

func insertOneStringValue(t *testing.T, value string) ([]map[string]interface{}, error) {
	t.Helper()
	body := []byte(fmt.Sprintf(
		`{"data": {"%s": 1, "vector": [0.1, 0.2], "name": %s}}`, FieldBookID, value))
	rows, _, err := checkAndSetData(body, stringFieldTestSchema(), false)
	return rows, err
}

// gjson's String() renders a number through float64 with the 'f' verb, so the
// stored text was not the text the caller wrote.
func TestCheckAndSetDataStringFieldKeepsNumberLiteral(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"string passes through", `"abc"`, `abc`},
		{"escapes are decoded once", `"a\"b"`, `a"b`},
		{"empty string", `""`, ``},
		{"integer", `12345`, `12345`},
		{"negative", `-7`, `-7`},
		{"bool", `true`, `true`},
		// each of these used to be rewritten
		{"trailing zero is kept", `1.50`, `1.50`},
		{"decimal point is kept", `1.0`, `1.0`},
		{"exponent is kept", `1e19`, `1e19`},
		{"large exponent is not expanded", `1e300`, `1e300`},
		{"precision is kept", `9007199254740993.0`, `9007199254740993.0`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneStringValue(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.expected, rows[0]["name"])
		})
	}
}

// 1e300 used to be stored as its full decimal expansion: five bytes in, 301
// bytes stored, and a max_length error that named a size the caller never sent.
func TestCheckAndSetDataStringFieldDoesNotExpandExponents(t *testing.T) {
	rows, err := insertOneStringValue(t, `1e300`)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Len(t, rows[0]["name"], len("1e300"))
}

func TestCheckAndSetDataStringFieldRejectsStructures(t *testing.T) {
	tests := []struct {
		name  string
		value string
		kind  string
	}{
		{"object", `{"a": 1}`, "object"},
		{"array", `[1, 2]`, "array"},
		{"empty object", `{}`, "object"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := insertOneStringValue(t, tt.value)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "name")
			assert.Contains(t, err.Error(), "expects a string")
			assert.Contains(t, err.Error(), tt.kind)
		})
	}
}

// proxy.http.compatibilityMode restores the previous String() rendering for a
// string field, including accepting an object as its text.
func TestCheckAndSetDataStringFieldCompatibilityMode(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"object is stringified again", `{"a": 1}`, `{"a": 1}`},
		{"array is stringified again", `[1, 2]`, `[1, 2]`},
		{"decimal point is dropped again", `1.0`, `1`},
		{"exponent is expanded again", `1e19`, `10000000000000000000`},
		{"string is unchanged", `"abc"`, `abc`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneStringValue(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.expected, rows[0]["name"])
		})
	}
}

// Dimensions a JSON test suite is expected to cover (compare the PostgreSQL
// json/jsonb regression tests): escapes, non-ASCII, duplicate keys, key order,
// empty containers, deep nesting and number spellings. The dynamic field now
// stores the document as written, so all of them survive verbatim. Four of
// these used to be normalized by the decode and re-encode round trip, and the
// normalized form is what a client got back.
func TestCheckAndSetDataDynamicFieldDocumentFidelity(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		// these were normalized before (duplicate keys are covered separately,
		// they are now rejected outright)
		{"key order is kept", `{"z":1,"m":2,"a":3}`},
		{"escaped solidus is kept", `{"a":"x\/y"}`},
		{"negative zero is kept", `{"a":-0}`},

		{"unicode escape", `{"a":"café"}`},
		{"non ascii utf8", `{"a":"café"}`},
		{"escaped control chars", `{"a":"l1\nl2\tx"}`},
		{"escaped nul", "{\"a\":\"x\\u0000y\"}"},
		{"emoji", `{"a":"🙂"}`},
		{"empty object", `{}`},
		{"empty array", `[]`},
		{"empty key", `{"":1}`},
		{"deep nesting", `{"a":{"b":{"c":[1]}}}`},
		{"mixed array", `[1,"a",true,null,{"b":2}]`},
		{"null member", `{"a":null}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneDynamicValue(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			marshaled, err := json.Marshal(map[string]interface{}{"dyn": rows[0]["dyn"]})
			require.NoError(t, err)
			assert.Equal(t, `{"dyn":`+tt.value+`}`, string(marshaled))
		})
	}
}

// The same fidelity requirement for a declared JSON field.
func TestCheckAndSetDataJSONFieldDocumentFidelity(t *testing.T) {
	values := []string{
		`{"z":1,"m":2,"a":3}`,
		`{"a":"x\/y"}`,
		`{"a":-0}`,
		`{"a":"café"}`,
		`{"a":"café"}`,
		`{"a":"l1\nl2\tx"}`,
		"{\"a\":\"x\\u0000y\"}",
		`{"a":"🙂"}`,
		`{}`,
		`[]`,
		`{"":1}`,
		`{"a":{"b":{"c":[1]}}}`,
		`[1,"a",true,null,{"b":2}]`,
		`{"a":null}`,
	}

	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			stored := insertOneJSONValue(t, value)
			assert.Equal(t, value, string(stored))
			assert.True(t, json.Valid(stored))
		})
	}
}

// The surrogate shapes PostgreSQL's json_encoding.sql exercises. The request
// binder accepts a lone surrogate and replaces it with U+FFFD, but simdjson
// refuses to decode the string it belongs to and reports STRING_ERROR, so
// keeping the token verbatim would make the value unreadable. These documents
// therefore fall back to the decoded form.
func TestCheckAndSetDataLoneSurrogateFallsBack(t *testing.T) {
	replacement := "�"

	t.Run("a valid pair is kept verbatim", func(t *testing.T) {
		const value = `{"a":"😄"}`
		rows, err := insertOneDynamicValue(t, value)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, json.RawMessage(value), rows[0]["dyn"])
	})

	for _, tt := range []struct {
		name  string
		value string
	}{
		{"two high surrogates in a row", `{"a":"\ud83d\ud83d"}`},
		{"surrogates in the wrong order", `{"a":"\ude04\ud83d"}`},
		{"orphan high surrogate", `{"a":"\ud83dX"}`},
		{"orphan low surrogate", `{"a":"\ude04X"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := insertOneDynamicValue(t, tt.value)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			// not the raw token: it would be unreadable
			assert.NotEqual(t, json.RawMessage(tt.value), rows[0]["dyn"])

			marshaled, err := json.Marshal(map[string]interface{}{"dyn": rows[0]["dyn"]})
			require.NoError(t, err)
			assert.Contains(t, string(marshaled), replacement)
			assert.True(t, json.Valid(marshaled))
		})
	}

	t.Run("a json field falls back the same way", func(t *testing.T) {
		stored := insertOneJSONValue(t, `{"a":"\ud83dX"}`)
		assert.NotEqual(t, `{"a":"\ud83dX"}`, string(stored))
		assert.True(t, json.Valid(stored))
		assert.Contains(t, string(stored), replacement)
	})
}

func TestHasLoneSurrogate(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{`{"a":"😄"}`, false},
		{`{"a":"😄🐶"}`, false},
		{`{"a":"\ud83d\ud83d"}`, true},
		{`{"a":"\ude04\ud83d"}`, true},
		{`{"a":"\ud83dX"}`, true},
		{`{"a":"\ude04X"}`, true},
		{`{"a":"café"}`, false},
		{"{\"a\":\"x\\u0000y\"}", false},
		{`{"a":"plain"}`, false},
		{`{"a":1,"b":[1,2]}`, false},
		// an escaped backslash does not start an escape
		{`{"a":"esc \\u0024 not an escape"}`, false},
		// truncated escape at the end of the document
		{`{"a":"trailing \ud8"}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			assert.Equal(t, tt.want, hasLoneSurrogate(tt.raw))
		})
	}
}

// RFC 8259 says an object name SHOULD be unique, and the readers disagree when
// it is not: encoding/json, Python and PostgreSQL's jsonb keep the last value,
// while gjson and simdjson keep the first. A caller who stored a duplicate
// would read the document back, parse it with their own library and disagree
// with what a Milvus filter matches, so the document is rejected.
func TestCheckAndSetDataRejectsDuplicateKeys(t *testing.T) {
	tests := []struct {
		name  string
		value string
		key   string
	}{
		{"top level", `{"a":1,"a":2}`, "a"},
		{"same value twice", `{"a":1,"a":1}`, "a"},
		{"nested object", `{"a":{"b":1,"b":2}}`, "b"},
		{"inside an array element", `{"a":[{"c":1,"c":2}]}`, "c"},
		{"deeply nested", `{"x":{"y":{"z":1,"z":2}}}`, "z"},
	}

	for _, tt := range tests {
		t.Run("dynamic field "+tt.name, func(t *testing.T) {
			_, err := insertOneDynamicValue(t, tt.value)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "dyn")
			assert.Contains(t, err.Error(), tt.key)
			assert.Contains(t, err.Error(), "twice")
		})
	}

	t.Run("json field", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "json_field": {"a":1,"a":2}}}`, FieldBookID))
		_, _, err := checkAndSetData(body, jsonFieldTestSchema(), false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "json_field")
	})

	t.Run("the same key in sibling array elements is fine", func(t *testing.T) {
		const value = `[{"d":1},{"d":2}]`
		rows, err := insertOneDynamicValue(t, value)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, json.RawMessage(value), rows[0]["dyn"])
	})

	t.Run("compatibility mode keeps the old silent dedup", func(t *testing.T) {
		paramtable.Init()
		key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)

		rows, err := insertOneDynamicValue(t, `{"a":1,"a":2}`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		marshaled, err := json.Marshal(map[string]interface{}{"dyn": rows[0]["dyn"]})
		require.NoError(t, err)
		assert.Equal(t, `{"dyn":{"a":1}}`, string(marshaled))
	})
}

func TestCheckEngineCompatible(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{"unique keys", `{"a":1,"b":2}`, ""},
		{"sibling array elements repeat a key", `[{"d":1},{"d":2}]`, ""},
		{"nested but unique", `{"a":[1,2],"b":{"c":[{"e":1}]}}`, ""},
		{"empty object", `{}`, ""},
		{"empty array", `[]`, ""},
		{"scalar document", `"scalar"`, ""},
		{"exact int64", `{"a":9007199254740993}`, ""},
		{"uint64 upper bound", `{"a":18446744073709551615}`, ""},

		{"duplicate at the top level", `{"a":1,"a":2}`, "declares the key a twice"},
		{"duplicate nested", `{"a":{"b":1,"b":2}}`, "declares the key b twice"},
		{"duplicate inside an array element", `{"a":[{"c":1,"c":2}]}`, "declares the key c twice"},
		{"duplicate empty key", `{"":1,"":2}`, "twice"},
		{"integer past 64 bits", `{"a":18446744073709551616}`, "exceeds the 64-bit range"},
		{"integer past 64 bits, nested", `{"a":[{"b":123456789012345678901234567890}]}`, "exceeds the 64-bit range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkEngineCompatible("f", tt.document)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	t.Run("invalid UTF-8", func(t *testing.T) {
		err := checkEngineCompatible("f", "{\"a\":\"x\xffy\"}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid UTF-8")
	})

	// simdjson counts containers: 1023 arrays holding a scalar and 1024 empty
	// arrays both parse, 1024 arrays holding a scalar do not
	t.Run("depth", func(t *testing.T) {
		assert.NoError(t, checkEngineCompatible("f", strings.Repeat("[", 1023)+"5"+strings.Repeat("]", 1023)))
		assert.NoError(t, checkEngineCompatible("f", strings.Repeat("[", 1024)+strings.Repeat("]", 1024)))
		require.Error(t, checkEngineCompatible("f", strings.Repeat("[", 1024)+"5"+strings.Repeat("]", 1024)))
	})
}

// The same declarations must answer the same with or without a fat sibling
// pushing the document to a very different size -- the tokenizer has no size
// regimes, and this pins that it never grows one.
func TestCheckEngineCompatibleSizeIndifferent(t *testing.T) {
	pad := `"` + strings.Repeat("x", 1024) + `"`
	for name, tc := range map[string]struct {
		small    string
		rejected bool
	}{
		"duplicate key":     {`{"a": 1, "a": 2}`, true},
		"oversized integer": {`{"n": 18446744073709551616}`, true},
		"clean document":    {`{"a": 1, "b": [true, "x", 1.5]}`, false},
		"nested dup":        {`{"o": {"k": 1, "k": 2}}`, true},
	} {
		t.Run(name, func(t *testing.T) {
			smallErr := checkEngineCompatible("f", tc.small)
			// same declarations plus a fat sibling to cross the size gate
			large := `{"pad": ` + pad + `, "doc": ` + tc.small + `}`
			require.Greater(t, len(large), 1024)
			largeErr := checkEngineCompatible("f", large)
			if tc.rejected {
				require.Error(t, smallErr, "small document")
				require.Error(t, largeErr, "large document")
			} else {
				require.NoError(t, smallErr, "small document")
				require.NoError(t, largeErr, "large document")
			}
		})
	}

	t.Run("invalid utf-8 past the gate", func(t *testing.T) {
		require.Error(t, checkEngineCompatible("f", `{"pad": `+pad+`, "a": "x\xffy"}`))
	})
}

// PostgreSQL's json.sql has a -- Recursion section that feeds it
// repeat('[', 10000). The duplicate-key check walks the document recursively,
// so the depth it can be handed matters: the request binder decodes into
// []map[string]interface{} first and encoding/json refuses to nest deeper than
// 10000, which leaves 9997 levels once the request envelope is accounted for.
// The walk has to survive everything the binder lets past it.
func TestCheckEngineCompatibleHandlesDeepNestingWithoutPanicking(t *testing.T) {
	// the walk is recursive and the binder allows 9997 levels, so it has to
	// survive whatever gets past the binder
	for _, depth := range []int{100, 1000, 5000, 9997} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			doc := strings.Repeat("[", depth) + strings.Repeat("]", depth)
			assert.NotPanics(t, func() {
				_ = checkEngineCompatible("f", doc)
			})
		})
	}
}

// PostgreSQL accepts a bare scalar as a whole JSON document, including null.
// A JSON field given a null is resolved by the nullable handling before the
// value ever reaches the JSON branch, so it must not be confused with a
// document whose text is the four letters n-u-l-l.
func TestCheckAndSetDataJSONFieldNullVersusNullDocument(t *testing.T) {
	t.Run("a JSON null is a missing value, not a document", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "json_field": null}}`, FieldBookID))
		_, _, err := checkAndSetData(body, jsonFieldTestSchema(), false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "not nullable")
	})

	// While the field reads back as the text of its document, that text is the
	// field's wire form, so a string holding a document is unwrapped -- and the
	// string "null" cannot be expressed. Returning the document itself is what
	// removes that limit, which is why the unwrapping follows the same setting.
	t.Run("a string holding a document is unwrapped in the legacy shape", func(t *testing.T) {
		params := paramtable.Get()
		key := params.HTTPCfg.NativeJSONResponse.Key
		params.Save(key, "false")
		defer params.Reset(key)

		assert.Equal(t, `null`, string(insertOneJSONValue(t, `"null"`)))
		assert.Equal(t, `true`, string(insertOneJSONValue(t, `"true"`)))
		assert.Equal(t, `123`, string(insertOneJSONValue(t, `"123"`)))
		// text that is not a document keeps its quotes
		assert.Equal(t, `"hello"`, string(insertOneJSONValue(t, `"hello"`)))
	})

	t.Run("a nullable field accepts the null", func(t *testing.T) {
		schema := jsonFieldTestSchema()
		schema.Fields[2].Nullable = true
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "json_field": null}}`, FieldBookID))
		_, validData, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
		assert.Equal(t, []bool{false}, validData["json_field"])
	})
}

func rawParams(t *testing.T, params map[string]string) map[string]json.RawMessage {
	t.Helper()
	out := make(map[string]json.RawMessage, len(params))
	for k, v := range params {
		out[k] = json.RawMessage(v)
	}
	return out
}

// An expression template parameter used to be handed over already decoded into
// interface{}, so every number had been through float64.
func TestGenerateExpressionTemplateKeepsIntegersExact(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int64
	}{
		{"small", `42`, 42},
		{"negative", `-7`, -7},
		// this used to become 9007199254740992
		{"beyond 2^53", `9007199254740993`, 9007199254740993},
		{"int64 upper bound", `9223372036854775807`, 9223372036854775807},
		{"int64 lower bound", `-9223372036854775808`, -9223372036854775808},
		// integer valued spellings stay integers, as before
		{"integer valued decimal", `1.0`, 1},
		{"integer valued exponent", `1e3`, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": tt.raw}))
			require.NoError(t, err)
			require.Contains(t, values, "v")
			assert.Equal(t, tt.want, values["v"].GetInt64Val())
		})
	}

	t.Run("a real fraction stays a float", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": `1.5`}))
		require.NoError(t, err)
		assert.Equal(t, 1.5, values["v"].GetFloatVal())
	})
}

// null, an empty array and an object all reached a panic, so the caller got a
// 500 from a plain parameter mistake.
func TestGenerateExpressionTemplateRejectsInsteadOfPanicking(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		message string
	}{
		{"null", `null`, "must not be null"},
		{"empty array", `[]`, "must not be an empty array"},
		{"object", `{"a":1}`, "must be a bool, number, string or array"},
		{"null inside an array", `[1,null,2]`, "must not contain null"},
		{"empty nested array", `[[]]`, "must not be an empty array"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": tt.raw}))
				require.Error(t, err)
				assert.Nil(t, values)
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				assert.Contains(t, err.Error(), "v")
				assert.Contains(t, err.Error(), tt.message)
			})
		})
	}
}

func TestGenerateExpressionTemplateArrays(t *testing.T) {
	t.Run("integer array stays exact", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{
			"v": `[1, 9007199254740993, -7]`,
		}))
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 9007199254740993, -7},
			values["v"].GetArrayVal().GetLongData().GetData())
	})

	t.Run("string array", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": `["a","b"]`}))
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, values["v"].GetArrayVal().GetStringData().GetData())
	})

	t.Run("bool array", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": `[true,false]`}))
		require.NoError(t, err)
		assert.Equal(t, []bool{true, false}, values["v"].GetArrayVal().GetBoolData().GetData())
	})

	t.Run("float array", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": `[1.5, 2.5]`}))
		require.NoError(t, err)
		assert.Equal(t, []float64{1.5, 2.5}, values["v"].GetArrayVal().GetDoubleData().GetData())
	})

	t.Run("mixed types fall back to json", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": `[1,"a"]`}))
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("1"), []byte(`"a"`)},
			values["v"].GetArrayVal().GetJsonData().GetData())
	})

	t.Run("nested arrays", func(t *testing.T) {
		values, err := generateExpressionTemplate(rawParams(t, map[string]string{"v": `[[1,2],[3]]`}))
		require.NoError(t, err)
		nested := values["v"].GetArrayVal().GetArrayData().GetData()
		require.Len(t, nested, 2)
		assert.Equal(t, []int64{1, 2}, nested[0].GetLongData().GetData())
	})
}

// An empty id list used to build `pk in []`, which the planner accepts and
// which matches nothing, so a batch loop handed an empty batch did nothing and
// reported success. Moving the ids into a template value must not turn that
// into an error -- but an id that is absent, or null, still is one: on the
// delete endpoints it means the caller named neither a filter nor an id.
func TestCheckGetPrimaryKeyEmptyVersusAbsent(t *testing.T) {
	for _, pk := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pk.String(), func(t *testing.T) {
			coll := generateCollectionSchema(pk, false, true)

			t.Run("an empty list stays a no-op", func(t *testing.T) {
				_, values, err := checkGetPrimaryKey(coll, gjson.Get(`{"id": []}`, "id"))
				require.NoError(t, err)
				array := values[primaryKeyTemplateVar].GetArrayVal()
				require.NotNil(t, array, "the element type comes from the schema, so it survives an empty list")
				assert.Empty(t, array.GetLongData().GetData())
				assert.Empty(t, array.GetStringData().GetData())
			})

			for name, body := range map[string]string{
				"an absent id is still required": `{}`,
				"a null id is still required":    `{"id": null}`,
			} {
				t.Run(name, func(t *testing.T) {
					_, _, err := checkGetPrimaryKey(coll, gjson.Get(body, "id"))
					require.Error(t, err)
					assert.Contains(t, err.Error(), "is required")
				})
			}
		})
	}
}

// The id list used to be formatted into the expression text with no escaping,
// so a quote in an id was parsed as syntax. A caller naming one id could match
// several rows, which on the delete endpoint removed rows they never named.
func TestCheckGetPrimaryKeyIsNotInjectable(t *testing.T) {
	varcharColl := generateCollectionSchema(schemapb.DataType_VarChar, false, true)

	t.Run("a quote and comma stay one id", func(t *testing.T) {
		// produced `book_id in ["alice", "bob"]` before
		idStr := gjson.Get(`{"id": ["alice\", \"bob"]}`, "id")
		filter, values, err := checkGetPrimaryKey(varcharColl, idStr)
		require.NoError(t, err)
		assert.Equal(t, "book_id in {"+primaryKeyTemplateVar+"}", filter)

		ids := values[primaryKeyTemplateVar].GetArrayVal().GetStringData().GetData()
		require.Len(t, ids, 1, "one id in must stay one id out")
		assert.Equal(t, `alice", "bob`, ids[0])
	})

	t.Run("a bracket cannot close the list", func(t *testing.T) {
		idStr := gjson.Get(`{"id": ["a\"] or book_id != [\"z"]}`, "id")
		_, values, err := checkGetPrimaryKey(varcharColl, idStr)
		require.NoError(t, err)
		ids := values[primaryKeyTemplateVar].GetArrayVal().GetStringData().GetData()
		require.Len(t, ids, 1)
		assert.Equal(t, `a"] or book_id != ["z`, ids[0])
	})

	t.Run("a quote as ordinary data is usable", func(t *testing.T) {
		// `book_id in ["say "hi""]` did not parse, so the id was unfetchable
		idStr := gjson.Get(`{"id": ["say \"hi\""]}`, "id")
		_, values, err := checkGetPrimaryKey(varcharColl, idStr)
		require.NoError(t, err)
		assert.Equal(t, []string{`say "hi"`},
			values[primaryKeyTemplateVar].GetArrayVal().GetStringData().GetData())
	})

	t.Run("a backslash is not an escape", func(t *testing.T) {
		idStr := gjson.Get(`{"id": ["back\\slash"]}`, "id")
		_, values, err := checkGetPrimaryKey(varcharColl, idStr)
		require.NoError(t, err)
		assert.Equal(t, []string{`back\slash`},
			values[primaryKeyTemplateVar].GetArrayVal().GetStringData().GetData())
	})
}

func TestCheckGetPrimaryKeyTyping(t *testing.T) {
	int64Coll := generateCollectionSchema(schemapb.DataType_Int64, false, true)
	varcharColl := generateCollectionSchema(schemapb.DataType_VarChar, false, true)

	t.Run("a quoted integer is base 10", func(t *testing.T) {
		// cast used strconv base detection, so "010" looked up primary key 8
		idStr := gjson.Get(`{"id": ["010", "9"]}`, "id")
		_, values, err := checkGetPrimaryKey(int64Coll, idStr)
		require.NoError(t, err)
		assert.Equal(t, []int64{10, 9},
			values[primaryKeyTemplateVar].GetArrayVal().GetLongData().GetData())
	})

	t.Run("an integer beyond 2^53 keeps its value", func(t *testing.T) {
		idStr := gjson.Get(`{"id": [9007199254740993]}`, "id")
		_, values, err := checkGetPrimaryKey(int64Coll, idStr)
		require.NoError(t, err)
		assert.Equal(t, []int64{9007199254740993},
			values[primaryKeyTemplateVar].GetArrayVal().GetLongData().GetData())
	})

	for _, tt := range []struct {
		name string
		coll *schemapb.CollectionSchema
		ids  string
	}{
		{"missing id", int64Coll, `{}`},
		{"a fraction for an Int64 key", int64Coll, `{"id": [1.5]}`},
		{"a non-integer string for an Int64 key", int64Coll, `{"id": ["abc"]}`},
		{"an integer past int64", int64Coll, `{"id": [9223372036854775808]}`},
		{"an object for a VarChar key", varcharColl, `{"id": [{"a":1}]}`},
		{"a bool for a VarChar key", varcharColl, `{"id": [true]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := checkGetPrimaryKey(tt.coll, gjson.Get(tt.ids, "id"))
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func rawIDs(raws ...string) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(raws))
	for _, r := range raws {
		out = append(out, json.RawMessage(r))
	}
	return out
}

// An array has no element-level validity, so a null element could only be
// stored as the element type's zero value. sonic already refused one for an
// integer or string element; a boolean or float element silently became false
// or 0.
func TestCheckAndSetDataRejectsNullArrayElements(t *testing.T) {
	elementTypes := map[string]schemapb.DataType{
		"bool":    schemapb.DataType_Bool,
		"int32":   schemapb.DataType_Int32,
		"int64":   schemapb.DataType_Int64,
		"float":   schemapb.DataType_Float,
		"double":  schemapb.DataType_Double,
		"varchar": schemapb.DataType_VarChar,
	}
	values := map[schemapb.DataType]string{
		schemapb.DataType_Bool:    `[true, null]`,
		schemapb.DataType_Int32:   `[1, null]`,
		schemapb.DataType_Int64:   `[1, null]`,
		schemapb.DataType_Float:   `[1.5, null]`,
		schemapb.DataType_Double:  `[1.5, null]`,
		schemapb.DataType_VarChar: `["a", null]`,
	}

	for name, elementType := range elementTypes {
		t.Run(name, func(t *testing.T) {
			vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
			vectorField.Name = "vector"
			schema := &schemapb.CollectionSchema{
				Name: DefaultCollectionName,
				Fields: []*schemapb.FieldSchema{
					generatePrimaryField(schemapb.DataType_Int64, false),
					vectorField,
					{Name: "arr", DataType: schemapb.DataType_Array, ElementType: elementType},
				},
			}
			body := []byte(fmt.Sprintf(
				`{"data": {"%s": 1, "vector": [0.1, 0.2], "arr": %s}}`, FieldBookID, values[elementType]))
			_, _, err := checkAndSetData(body, schema, false)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "index 1")
			assert.Contains(t, err.Error(), "cannot be null")
		})
	}
}

// An absent key and an explicit null are different requests: the first says
// nothing about the field, the second says it should be null. Collapsing them
// meant a partial update could not clear a dynamic field.
func TestCheckAndSetDataDynamicExplicitNull(t *testing.T) {
	schema := dynamicFieldTestSchema()

	t.Run("an explicit null is carried", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "dyn": null}}`, FieldBookID))
		rows, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, json.RawMessage("null"), rows[0]["dyn"])
	})

	t.Run("an absent key stays absent", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2]}}`, FieldBookID))
		rows, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.NotContains(t, rows[0], "dyn")
	})

	t.Run("compatibility mode drops it again", func(t *testing.T) {
		paramtable.Init()
		key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)

		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "dyn": null}}`, FieldBookID))
		rows, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.NotContains(t, rows[0], "dyn")
	})
}

func jsonRespFieldData(docs ...string) *schemapb.FieldData {
	data := make([][]byte, 0, len(docs))
	for _, d := range docs {
		data = append(data, []byte(d))
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "meta",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: data},
				},
			},
		},
	}
}

// A JSON field reads back as a string by default, while the same value in the
// dynamic field reads back as a document. proxy.http.nativeJSONResponse removes
// that difference, and degrades the whole response rather than part of it when a
// row written before the insert path was fixed does not hold a document.
func TestBuildQueryRespNativeJSON(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.NativeJSONResponse.Key

	t.Run("switched off: strings", func(t *testing.T) {
		paramtable.Get().Save(key, "false")
		defer paramtable.Get().Reset(key)

		rows, err := buildQueryResp(0, []string{"meta"},
			[]*schemapb.FieldData{jsonRespFieldData(`{"a":1}`, `{"b":2}`)}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, `{"a":1}`, rows[0]["meta"])
		assert.Equal(t, `{"b":2}`, rows[1]["meta"])
	})

	t.Run("on by default: documents", func(t *testing.T) {
		rows, err := buildQueryResp(0, []string{"meta"},
			[]*schemapb.FieldData{jsonRespFieldData(`{"a":1}`, `{"b":2}`)}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, json.RawMessage(`{"a":1}`), rows[0]["meta"])

		// the whole response marshals as documents
		out, err := json.Marshal(map[string]interface{}{"data": rows})
		require.NoError(t, err)
		assert.Contains(t, string(out), `"meta":{"a":1}`)
		assert.NotContains(t, string(out), `"meta":"{`)
	})

	t.Run("on, one legacy row: the whole response degrades", func(t *testing.T) {
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)

		// `hello` is what a pre-fix insert of {"meta": "hello"} stored
		rows, err := buildQueryResp(0, []string{"meta"},
			[]*schemapb.FieldData{jsonRespFieldData(`{"a":1}`, `hello`, `{"b":2}`)}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Len(t, rows, 3)

		// every row is a string, not just the bad one, so a caller never sees a
		// mixture
		for i, row := range rows {
			_, isString := row["meta"].(string)
			assert.True(t, isString, "row %d should have degraded to a string", i)
		}
		assert.Equal(t, `hello`, rows[1]["meta"])

		// and the response still marshals, which it would not have done natively
		out, err := json.Marshal(map[string]interface{}{"data": rows})
		require.NoError(t, err)
		assert.Contains(t, string(out), `"meta":"hello"`)
	})
}

// An expression template array is typed from its first element, and an integer
// too large for an int64 is classified as a float. In a mixed array that sent
// the whole array down the JSON branch, where the value was compared as a
// double, so 9223372036854775809 matched the row holding ...808.
func TestTemplateArrayChecksEveryElement(t *testing.T) {
	t.Run("an oversized integer after a float is rejected", func(t *testing.T) {
		_, err := templateValueFromJSON("v", gjson.Parse(`[1.5, 9223372036854775809]`), 0, maxExprParamsDepthCeiling)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "9223372036854775809")
	})

	t.Run("an oversized integer after a string is rejected", func(t *testing.T) {
		_, err := templateValueFromJSON("v", gjson.Parse(`["a", 18446744073709551615]`), 0, maxExprParamsDepthCeiling)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("a whole number far beyond the 64-bit integers passes as a double", func(t *testing.T) {
		value, err := templateValueFromJSON("v", gjson.Parse(`["a", 99999999999999999999]`), 0, maxExprParamsDepthCeiling)
		require.NoError(t, err)
		assert.NotNil(t, value.GetArrayVal())
	})

	t.Run("a mixed array of representable values still works", func(t *testing.T) {
		value, err := templateValueFromJSON("v", gjson.Parse(`[1.5, 9223372036854775807]`), 0, maxExprParamsDepthCeiling)
		require.NoError(t, err)
		assert.NotNil(t, value.GetArrayVal())
	})
}

// A vector handed over as the text of its own JSON shape used to be accepted on
// insert and refused on search, because insert read gjson's String() -- which
// drops a string node's quotes -- while search reads the raw element. The row
// could be written with a spelling that could not look it up again.
func TestCheckAndSetDataRejectsStringWrappedVector(t *testing.T) {
	paramtable.Init()

	schemaFor := func(dataType schemapb.DataType, dim string) *schemapb.CollectionSchema {
		field := generateVectorFieldSchema(dataType)
		field.Name = "f"
		for _, param := range field.TypeParams {
			if param.Key == common.DimKey {
				param.Value = dim
			}
		}
		anchor := generateVectorFieldSchema(schemapb.DataType_FloatVector)
		anchor.Name = "anchor"
		return &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false), anchor, field,
			},
		}
	}
	insert := func(t *testing.T, schema *schemapb.CollectionSchema, value string) error {
		t.Helper()
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "anchor": [0.1, 0.2], "f": %s}}`, FieldBookID, value))
		_, _, err := checkAndSetData(body, schema, false)
		return err
	}

	for _, tt := range []struct {
		name     string
		dataType schemapb.DataType
		dim      string
		natural  string
		text     string
	}{
		{"float vector", schemapb.DataType_FloatVector, "2", `[0.1, 0.2]`, `"[0.1, 0.2]"`},
		{"int8 vector", schemapb.DataType_Int8Vector, "2", `[1, 2]`, `"[1, 2]"`},
		{"sparse vector", schemapb.DataType_SparseFloatVector, "2", `{"1": 0.5}`, `"{\"1\": 0.5}"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			schema := schemaFor(tt.dataType, tt.dim)
			require.NoError(t, insert(t, schema, tt.natural))

			err := insert(t, schema, tt.text)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "the text of one")

			// the search side already refused it, which is how the two came to
			// disagree; assert they now agree
			if tt.dataType == schemapb.DataType_SparseFloatVector {
				_, err = convertQueries2Placeholder(
					fmt.Sprintf(`{"data": [%s]}`, tt.text), tt.dataType, 2)
				require.Error(t, err)
			}
		})
	}

	// Int8Vector was listed as accepting base64 and so skipped the null-literal
	// check, where "null" decodes to a nil slice without an error; it went in as
	// an empty vector. No decoder reads base64 for it, so it is not on the list.
	t.Run("a null literal is refused for every type without base64", func(t *testing.T) {
		for _, dataType := range []schemapb.DataType{
			schemapb.DataType_Int8Vector, schemapb.DataType_FloatVector,
		} {
			t.Run(dataType.String(), func(t *testing.T) {
				err := insert(t, schemaFor(dataType, "2"), `"null"`)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "cannot be null")
			})
		}
	})

	// a type whose string spelling is base64 keeps taking one
	t.Run("base64 spellings still work", func(t *testing.T) {
		require.NoError(t, insert(t, schemaFor(schemapb.DataType_BinaryVector, "8"), `"AQ=="`))
		require.NoError(t, insert(t, schemaFor(schemapb.DataType_Float16Vector, "2"), `[0.1, 0.2]`))
	})

	t.Run("compatibility mode takes the text back", func(t *testing.T) {
		key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)
		require.NoError(t, insert(t, schemaFor(schemapb.DataType_FloatVector, "2"), `"[0.1, 0.2]"`))
	})
}

// The null-element check used to run on the wrapper rather than on the text the
// decoder consumes, so an array handed over as a JSON string had no elements to
// look at and every null in it reached the decoder.
func TestCheckAndSetDataRejectsNullInStringWrappedArray(t *testing.T) {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	schema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
			{Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool},
		},
	}

	t.Run("a null in a quoted array is rejected", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "arr": "[true, null]"}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "cannot be null")
	})

	t.Run("a quoted array without nulls still works", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "arr": "[true, false]"}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
	})
}

// A null in a vector reached the decoder the same way and became 0, which is a
// coordinate the caller never sent.
func TestCheckAndSetDataRejectsNullVectorElements(t *testing.T) {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	schema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
		},
	}

	body := []byte(fmt.Sprintf(`{"data": {"%s": 1, "vector": [0.1, null]}}`, FieldBookID))
	_, _, err := checkAndSetData(body, schema, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "index 1")

	// A vector is a dense array of numbers with no per-element validity, so a
	// null coordinate has no representation other than a number the caller did
	// not send. There is no previous handling worth restoring, so the escape
	// hatch deliberately does not cover it.
	t.Run("compatibility mode does not bring it back", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().HTTPCfg.CompatibilityMode.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.CompatibilityMode.Key)

		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
	})
}

// Normalizing a lone surrogate used to send every number through float64, and
// an earlier version tried to predict which literals survived that. It asked
// whether the literal fit an int64, so 2^63 was judged safe and came back as
// 9223372036854776000, while 9007199254740994 was refused even though it
// round-trips exactly. Decoding with UseNumber keeps every literal as written,
// so there is nothing left to predict.
func TestJSONDocumentForStorageKeepsNumbersThroughNormalization(t *testing.T) {
	for _, tt := range []struct {
		name     string
		document string
		stored   string
	}{
		{"an integer past int64 keeps its digits", `{"s":"\ud800","n":9223372036854775808}`, `9223372036854775808`},
		{"a uint64 maximum keeps its digits", `{"s":"\ud800","n":18446744073709551615}`, `18446744073709551615`},
		{"an exactly representable integer is not refused", `{"s":"\ud800","n":9007199254740994}`, `9007199254740994`},
		{"an integer past 2^53 keeps its digits", `{"s":"\ud800","n":9007199254740993}`, `9007199254740993`},
		{"a float keeps its literal", `{"s":"\ud800","n":1.50}`, `1.50`},
		{"unsorted keys are not a problem", `{"b":"\ud800","a":1}`, `1`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stored, err := jsonDocumentForStorage("j", tt.document)
			require.NoError(t, err)
			assert.Contains(t, string(stored), tt.stored)
			// the surrogate is what forced the normalization in the first place
			assert.NotContains(t, string(stored), `\ud800`)
		})
	}

	t.Run("an integer beyond uint64 is still refused", func(t *testing.T) {
		_, err := jsonDocumentForStorage("j", `{"s":"\ud800","n":184467440737095516150}`)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

// Two dynamic keys that differ only in a lone surrogate escape both decode to
// U+FFFD, so building the map collapsed them and one field vanished silently.
func TestCheckAndSetDataRejectsLoneSurrogateDynamicKeys(t *testing.T) {
	schema := dynamicFieldTestSchema()

	t.Run("colliding keys are rejected", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "\ud800": 1, "\ud801": 2}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "surrogate")
	})

	t.Run("a single such key is rejected too", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "\ud800": 1}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("a well-formed surrogate pair is a normal key", func(t *testing.T) {
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "😀": 1}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
	})

	t.Run("compatibility mode keeps the old handling", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().HTTPCfg.CompatibilityMode.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.CompatibilityMode.Key)

		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "\ud800": 1, "\ud801": 2}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
	})
}

// json.Valid only checks syntax, and a JSON string is allowed to hold any bytes
// as far as it is concerned. The encoder does mind: it replaces invalid UTF-8
// with U+FFFD, so the caller would get bytes the row does not hold, reported as
// a document rather than degraded.
func TestBuildQueryRespNativeJSONInvalidUTF8Degrades(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.NativeJSONResponse.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	bad := "{\"a\":\"\xff\"}"
	require.True(t, json.Valid([]byte(bad)), "the point of the test is that this is valid JSON")

	rows, err := buildQueryResp(0, []string{"meta"},
		[]*schemapb.FieldData{jsonRespFieldData(`{"a":1}`, bad)}, nil, nil, true, nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for i, row := range rows {
		_, isString := row["meta"].(string)
		assert.True(t, isString, "row %d should have degraded to a string", i)
	}
	assert.Equal(t, bad, rows[1]["meta"])
}

// A Decoder reads one value and stops, so anything after it went unnoticed where
// the Unmarshal this replaced returned an error. A dynamic field holding two
// documents would have had the second one dropped without a word.
func TestBuildQueryRespDynamicFieldRejectsTrailingContent(t *testing.T) {
	dynamicData := func(values ...string) *schemapb.FieldData {
		data := make([][]byte, 0, len(values))
		for _, value := range values {
			data = append(data, []byte(value))
		}
		return &schemapb.FieldData{
			Type:      schemapb.DataType_JSON,
			FieldName: "$meta",
			IsDynamic: true,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{Data: data},
					},
				},
			},
		}
	}

	t.Run("a second document is an error", func(t *testing.T) {
		_, err := buildQueryResp(0, []string{"a"},
			[]*schemapb.FieldData{dynamicData(`{"a":1} {"a":2}`)}, nil, nil, true, nil)
		require.Error(t, err)
	})

	t.Run("trailing garbage is an error", func(t *testing.T) {
		_, err := buildQueryResp(0, []string{"a"},
			[]*schemapb.FieldData{dynamicData(`{"a":1}]`)}, nil, nil, true, nil)
		require.Error(t, err)
	})

	// Buffered() only exposes the decoder's pre-read window, so a second
	// document past a page of padding sat outside it and slipped through an
	// earlier version of this check.
	t.Run("a second document past 4KB of whitespace is still an error", func(t *testing.T) {
		_, err := buildQueryResp(0, []string{"a"},
			[]*schemapb.FieldData{dynamicData(`{"a":1}` + strings.Repeat(" ", 4096) + `{"a":2}`)}, nil, nil, true, nil)
		require.Error(t, err)
	})

	t.Run("trailing garbage past 4KB of whitespace is still an error", func(t *testing.T) {
		_, err := buildQueryResp(0, []string{"a"},
			[]*schemapb.FieldData{dynamicData(`{"a":1}` + strings.Repeat(" ", 4096) + `]`)}, nil, nil, true, nil)
		require.Error(t, err)
	})

	t.Run("4KB of pure trailing whitespace is fine", func(t *testing.T) {
		rows, err := buildQueryResp(0, []string{"a"},
			[]*schemapb.FieldData{dynamicData(`{"a":1}` + strings.Repeat(" ", 4096))}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
	})

	t.Run("trailing whitespace is not", func(t *testing.T) {
		rows, err := buildQueryResp(0, []string{"a"},
			[]*schemapb.FieldData{dynamicData("{\"a\":1}\n  ")}, nil, nil, true, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.True(t, sameValue(json.Number("1"), rows[0]["a"]))
	})
}

// A binary vector arrives as base64, which is not JSON. gjson dispatches on the
// first character, so a value starting with "nu" is read as a partial `null`
// literal: the result has type Null and, unlike other unparseable text, hands
// itself to ForEach as a single element. The null-element check then saw a null
// at index 0 and rejected the insert.
//
// About one random base64 value in 3500 starts that way, so a 3000-row batch hit
// it more often than not. That is why the REST e2e suite caught it and the unit
// tests did not; the value below is one that actually triggers it.
func TestCheckAndSetDataAcceptsBase64BinaryVector(t *testing.T) {
	binaryField := generateVectorFieldSchema(schemapb.DataType_BinaryVector)
	binaryField.Name = "binary_vector"
	schema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			binaryField,
		},
	}

	for _, value := range []string{
		"nuZovXeZEPU3g9F5HSEOdQ==", // reads as a partial null literal
		"mx+eKBD8Je63ZE/9iTwtyg==", // from the e2e run that failed
		"4NQaryMbp3Igb/eAcN6FuQ==", // reads as a number
	} {
		t.Run(value, func(t *testing.T) {
			body := []byte(fmt.Sprintf(
				`{"data": {"%s": 1, "binary_vector": %q}}`, FieldBookID, value))
			rows, _, err := checkAndSetData(body, schema, false)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.NotNil(t, rows[0]["binary_vector"])
		})
	}
}

// An expression template array is typed from its elements, and a value that
// nests hid an oversized integer from the check: [[9223372036854775809], "x"]
// is mixed because of the string, so it was carried as raw JSON and the planner
// compared the inner integer as a double, matching ...808.
func TestTemplateArrayChecksNestedIntegers(t *testing.T) {
	for _, tt := range []struct {
		name     string
		value    string
		rejected bool
	}{
		{"an oversized integer inside an array", `[[9223372036854775809], "x"]`, true},
		{"an oversized integer inside an object", `[{"a": 9223372036854775809}, "x"]`, true},
		{"two levels down", `[[[9223372036854775809]], "x"]`, true},
		{"an oversized integer in a plain nested array", `[[1, 9223372036854775809]]`, true},
		{"int64 boundaries are fine", `[[9223372036854775807, -9223372036854775808], "x"]`, false},
		{"ordinary nesting is fine", `[[1, 2], {"a": 3}, "x"]`, false},
		// the check is on the value, not the spelling: a whole number written
		// with a zero fraction is still a whole number
		{"a whole number with a zero fraction", `[9223372036854775809.0, "x"]`, true},
		{"a whole number with an exponent", `[9223372036854775809e0, "x"]`, true},
		{"a genuine fraction is left alone", `[1.5, 2.5]`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := templateValueFromJSON("v", gjson.Parse(tt.value), 0, maxExprParamsDepthCeiling)
			if tt.rejected {
				require.Error(t, err)
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// A whole value of "null" decodes to a nil slice without an error, so it was
// stored as an empty array -- or an empty sparse row -- rather than refused.
// The element scan could not catch it: the parsed value is not an array, so it
// has no elements to look at.
func TestCheckAndSetDataRejectsNullValuedArrayAndVector(t *testing.T) {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"

	t.Run("array", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false), vectorField,
				{Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Bool},
			},
		}
		for _, tt := range []struct {
			value    string
			rejected bool
		}{
			{`"null"`, true},
			{`"5"`, true},
			{`"[true,false]"`, false},
			{`[true,false]`, false},
			{`[]`, false},
		} {
			t.Run(tt.value, func(t *testing.T) {
				body := []byte(fmt.Sprintf(
					`{"data": {"%s": 1, "vector": [0.1, 0.2], "arr": %s}}`, FieldBookID, tt.value))
				_, _, err := checkAndSetData(body, schema, false)
				if tt.rejected {
					require.Error(t, err)
					assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("sparse vector", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name: DefaultCollectionName,
			Fields: []*schemapb.FieldSchema{
				generatePrimaryField(schemapb.DataType_Int64, false),
				{Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
			},
		}
		body := []byte(fmt.Sprintf(`{"data": {"%s": 1, "sparse": "null"}}`, FieldBookID))
		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		body = []byte(fmt.Sprintf(`{"data": {"%s": 1, "sparse": {"1": 0.5}}}`, FieldBookID))
		_, _, err = checkAndSetData(body, schema, false)
		require.NoError(t, err)
	})
}

// A struct's sub-vectors are decoded in buildStructSubVectorField, which the
// top-level vector check never reaches, so a null coordinate there still became
// a 0 the caller never sent.
func TestCheckAndSetDataRejectsNullStructSubVectorElements(t *testing.T) {
	schema := buildStructArrayTestSchema()

	t.Run("a null coordinate is rejected", func(t *testing.T) {
		body := []byte(`{"data":[{"id":1,"vec":[0.1,0.2,0.3,0.4],` +
			`"my_struct":[{"sub_int":10,"sub_vec":[1.1,null,1.3,1.4]}]}]}`)
		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "index 1")
	})

	t.Run("an ordinary sub-vector still works", func(t *testing.T) {
		body := []byte(`{"data":[{"id":1,"vec":[0.1,0.2,0.3,0.4],` +
			`"my_struct":[{"sub_int":10,"sub_vec":[1.1,1.2,1.3,1.4]}]}]}`)
		_, _, err := checkAndSetData(body, schema, false)
		require.NoError(t, err)
	})

	t.Run("compatibility mode does not bring it back", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().HTTPCfg.CompatibilityMode.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.CompatibilityMode.Key)

		body := []byte(`{"data":[{"id":1,"vec":[0.1,0.2,0.3,0.4],` +
			`"my_struct":[{"sub_int":10,"sub_vec":[1.1,null,1.3,1.4]}]}]}`)
		_, _, err := checkAndSetData(body, schema, false)
		require.Error(t, err)
	})
}

// The query side of the rule insert applies to a vector field: a null
// coordinate decodes to 0 and then passes the dimension check, so the search ran
// against a point the caller never asked for.
func TestSearchVectorRejectsNullCoordinates(t *testing.T) {
	paramtable.Init()

	t.Run("float vector", func(t *testing.T) {
		for _, tt := range []struct {
			value    string
			rejected bool
		}{
			{`[[0.1, 0.2]]`, false},
			{`[[0.1, null]]`, true},
			{`[[null, null]]`, true},
			{`[[0.1, 0.2], [0.3, null]]`, true},
		} {
			t.Run(tt.value, func(t *testing.T) {
				_, err := serializeFloatVectors(tt.value, schemapb.DataType_FloatVector, 2, 8,
					typeutil.Float32ArrayToBytes)
				if tt.rejected {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("sparse vector", func(t *testing.T) {
		// "null" was accepted as an empty sparse row
		_, err := serializeSparseFloatVectors(
			[]gjson.Result{gjson.Parse(`"null"`)}, schemapb.DataType_SparseFloatVector)
		require.Error(t, err)

		_, err = serializeSparseFloatVectors(
			[]gjson.Result{gjson.Parse(`{"1": 0.5}`)}, schemapb.DataType_SparseFloatVector)
		require.NoError(t, err)
	})

	// A vector is a dense array of numbers with no per-element validity, so a
	// null coordinate has no representation other than a number the caller did
	// not send. There is no previous handling worth restoring, so the escape
	// hatch deliberately does not cover it.
	t.Run("compatibility mode does not bring it back", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().HTTPCfg.CompatibilityMode.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.CompatibilityMode.Key)

		_, err := serializeFloatVectors(`[[0.1, null]]`, schemapb.DataType_FloatVector, 2, 8,
			typeutil.Float32ArrayToBytes)
		require.Error(t, err)
	})
}

// A text query goes through gjson's String(), which renders whatever it is
// given rather than returning a string the caller sent: 1.50 searched for "1.5",
// null searched for "", and an object searched for its own JSON text. Insert
// applies the same rule to a VarChar field.
func TestTextQueryMustBeAString(t *testing.T) {
	paramtable.Init()

	for _, tt := range []struct {
		data     string
		rejected bool
	}{
		{`["hello"]`, false},
		{`["hello", "world"]`, false},
		{`[123]`, true},
		{`[1.50]`, true},
		{`[null]`, true},
		{`[true]`, true},
		{`[{"a": 1}]`, true},
		{`[["x"]]`, true},
		{`["ok", 1]`, true},
	} {
		t.Run(tt.data, func(t *testing.T) {
			body := fmt.Sprintf(`{"data": %s}`, tt.data)
			_, err := convertQueries2Placeholder(body, schemapb.DataType_VarChar, 0)
			if tt.rejected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("compatibility mode keeps the old handling", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().HTTPCfg.CompatibilityMode.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().HTTPCfg.CompatibilityMode.Key)

		phv, err := convertQueries2Placeholder(`{"data": [1.50]}`, schemapb.DataType_VarChar, 0)
		require.NoError(t, err)
		require.Len(t, phv.GetValues(), 1)
		assert.Equal(t, "1.5", string(phv.GetValues()[0]))
	})
}

// A value like 3.5e38 is finite as a float64, so the infinity check on the
// parse result passes, and narrowing then manufactures +Inf -- a value the
// caller never sent, which the gRPC path refuses. The float32 range check has
// to run before the cast destroys the evidence.
func TestFloatInsertRejectsFloat32Overflow(t *testing.T) {
	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	schema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
			{Name: "f", DataType: schemapb.DataType_Float},
		},
	}
	insert := func(t *testing.T, value string) error {
		t.Helper()
		body := []byte(fmt.Sprintf(
			`{"data": {"%s": 1, "vector": [0.1, 0.2], "f": %s}}`, FieldBookID, value))
		_, _, err := checkAndSetData(body, schema, false)
		return err
	}

	for _, value := range []string{`3.5e38`, `-3.5e38`, `1e39`} {
		t.Run(value, func(t *testing.T) {
			err := insert(t, value)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
	for _, value := range []string{`3.4e38`, `-3.4e38`, `1.5`} {
		t.Run(value+" fits", func(t *testing.T) {
			require.NoError(t, insert(t, value))
		})
	}

	t.Run("struct sub-field float overflows are refused too", func(t *testing.T) {
		sub := &schemapb.FieldSchema{Name: "sf", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float}
		_, err := buildStructSubArrayScalar(sub, []gjson.Result{gjson.Parse(`3.5e38`)}, false)
		require.Error(t, err)
		_, err = buildStructSubArrayScalar(sub, []gjson.Result{gjson.Parse(`3.4e38`)}, false)
		require.NoError(t, err)
	})
}

// A Float column is read through float64 and then narrowed, the way every path
// this value will be compared against reads it: the expression parser, an
// exprParams value carried as a double, and a row written through pymilvus
// whose Python float is a double first. Parsing straight to float32 rounds
// once and reads the decimal more faithfully, but for a literal sitting on a
// float32 rounding midpoint it stores a value none of those paths produce, so
// the row could not be found with the literal that wrote it.
func TestFloatInsertSpeaksTheQueryDialect(t *testing.T) {
	const literal = "1.000000059604644775390625000001"

	double, err := strconv.ParseFloat(literal, 64)
	require.NoError(t, err)
	queryDialect := float32(double)

	single, err := strconv.ParseFloat(literal, 32)
	require.NoError(t, err)
	require.NotEqual(t, float32(single), queryDialect,
		"the literal must sit on a rounding midpoint, or this test proves nothing")

	vectorField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	vectorField.Name = "vector"
	schema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false),
			vectorField,
			{Name: "f", DataType: schemapb.DataType_Float},
		},
	}
	body := []byte(fmt.Sprintf(
		`{"data": {"%s": 1, "vector": [0.1, 0.2], "f": %s}}`, FieldBookID, literal))
	rows, _, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)
	assert.Equal(t, queryDialect, rows[0]["f"],
		"insert must store what a query for the same literal will look for")

	// the template path carries the double the executor narrows
	tmpl, err := templateValueFromJSON("v", gjson.Parse(literal), 0, maxExprParamsDepthCeiling)
	require.NoError(t, err)
	assert.Equal(t, double, tmpl.GetFloatVal())
	assert.Equal(t, queryDialect, float32(tmpl.GetFloatVal()))
}

// Converting an expression template parameter walks the caller's nesting
// recursively, so the depth is bounded by proxy.http.maxExprParamsDepth --
// arrays and objects both count, the bound is read once per request, and a
// configuration past the ceiling is read as the ceiling.
func TestTemplateDepthBound(t *testing.T) {
	paramtable.Init()

	nest := func(depth int, core string) string {
		return strings.Repeat("[", depth) + core + strings.Repeat("]", depth)
	}
	convert := func(t *testing.T, value string) error {
		t.Helper()
		_, err := generateExpressionTemplate(map[string]json.RawMessage{"v": json.RawMessage(value)})
		return err
	}

	t.Run("the default admits 100 and refuses 101", func(t *testing.T) {
		require.NoError(t, convert(t, nest(100, "1")))
		err := convert(t, nest(101, "1"))
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "nesting depth")
	})

	t.Run("objects count too", func(t *testing.T) {
		// an object can only appear inside a mixed array, where the raw-JSON
		// walk still has to bound it
		deep := `[` + strings.Repeat(`{"a":`, 100) + `1` + strings.Repeat(`}`, 100) + `, "x"]`
		err := convert(t, deep)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nesting depth")
	})

	t.Run("the bound is configurable", func(t *testing.T) {
		key := paramtable.Get().HTTPCfg.MaxExprParamsDepth.Key
		paramtable.Get().Save(key, "3")
		defer paramtable.Get().Reset(key)
		require.NoError(t, convert(t, nest(3, "1")))
		require.Error(t, convert(t, nest(4, "1")))
	})

	t.Run("the ceiling holds against misconfiguration", func(t *testing.T) {
		key := paramtable.Get().HTTPCfg.MaxExprParamsDepth.Key
		paramtable.Get().Save(key, "100000")
		defer paramtable.Get().Reset(key)
		require.NoError(t, convert(t, nest(1024, "1")))
		require.Error(t, convert(t, nest(1025, "1")))
	})

	t.Run("a bound below one clamps to one, not to the ceiling", func(t *testing.T) {
		key := paramtable.Get().HTTPCfg.MaxExprParamsDepth.Key
		paramtable.Get().Save(key, "0")
		defer paramtable.Get().Reset(key)
		require.NoError(t, convert(t, nest(1, "1")))
		require.Error(t, convert(t, nest(2, "1")))
	})
}

// The whole-number check is asked of the value, not of the spelling. An earlier
// version skipped anything containing "." or "e", so 9223372036854775809 was
// refused while 9223372036854775809.0 was accepted and then matched the row
// holding 9223372036854775808.
//
// The boundary is stated here in full because it is a deliberate trade: the
// refusal covers the whole range where a double can be mistaken for a 64-bit
// integer rather than only the part of it that actually rounds, which buys a
// check that reads the literal instead of expanding it.
func TestTemplateWholeNumberCheckIgnoresSpelling(t *testing.T) {
	for _, tt := range []struct {
		value    string
		rejected bool
	}{
		{`9223372036854775809`, true},
		{`9223372036854775809.0`, true},
		{`9223372036854775809e0`, true},
		{`92233720368547758.09e2`, true},
		{`-9223372036854775809`, true},
		{`18446744073709551615`, true}, // uint64 max rounds onto 2^64
		{`18446744073709551617`, true}, // 2^64+1 rounds onto 2^64 as well
		// The whole [2^63, 2^64] window is refused, including the values a
		// double happens to carry exactly: telling those apart needs exact
		// arithmetic on the literal, which is the cost this check refuses to
		// pay. Each still works written into the filter text.
		{`9223372036854775808`, true},  // 2^63, exact but refused
		{`18446744073709551616`, true}, // 2^64, exact but refused
		{`1e19`, true},                 // the one round magnitude in the window
		// below 2^63 every whole number is an int64, above 2^64 both sides of
		// the comparison are doubles; neither can be confused with a neighbor
		{`1e20`, false},
		{`1e300`, false},
		{`99999999999999999999`, false},
		{`9223372036854775807`, false},
		{`-9223372036854775808`, false},
		{`9223372036854775807.5`, false}, // a fraction, however close
		{`1.5`, false},
		{`100e-2`, false},
		{`1e-3`, false},
		// underflows to zero, nowhere near the window; costs one ParseFloat
		// rather than the million-digit rational an exact reading would build
		{`1e-1000000`, false},
	} {
		t.Run(tt.value, func(t *testing.T) {
			_, err := templateValueFromJSON("v", gjson.Parse(tt.value), 0, maxExprParamsDepthCeiling)
			if tt.rejected {
				require.Error(t, err)
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// The first version of the vector null check only covered plain float search
// vectors. Int8 vectors, the embedding-list form used by ArrayOfVector, and the
// v1 search endpoint each decoded a null to 0 by their own route.
func TestVectorNullRejectedOnEveryQueryEntryPoint(t *testing.T) {
	paramtable.Init()

	t.Run("int8 search vector", func(t *testing.T) {
		_, err := serializeInt8Vectors(`[[1, null]]`, schemapb.DataType_Int8Vector, 2,
			typeutil.Int8ArrayToBytes)
		require.Error(t, err)

		_, err = serializeInt8Vectors(`[[1, 2]]`, schemapb.DataType_Int8Vector, 2,
			typeutil.Int8ArrayToBytes)
		require.NoError(t, err)
	})

	t.Run("embedding list", func(t *testing.T) {
		for _, elemType := range []schemapb.DataType{
			schemapb.DataType_FloatVector,
			schemapb.DataType_Float16Vector,
			schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector,
		} {
			t.Run(elemType.String(), func(t *testing.T) {
				_, err := encodeEmbListQuery(gjson.Parse(`[[0.1, null]]`).Array(), elemType, 2, 0)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "null")
			})
		}

		_, err := encodeEmbListQuery(gjson.Parse(`[[0.1, 0.2]]`).Array(),
			schemapb.DataType_FloatVector, 2, 0)
		require.NoError(t, err)
	})

	t.Run("v1 search vector", func(t *testing.T) {
		var vector FloatVectorQuery
		err := json.Unmarshal([]byte(`[0.1, null]`), &vector)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "index 1")

		require.NoError(t, json.Unmarshal([]byte(`[0.1, 0.2]`), &vector))
		assert.Equal(t, FloatVectorQuery{0.1, 0.2}, vector)
	})
}

// "null" is also valid base64: it decodes to the three bytes 9e e9 65, which is
// a whole dim-24 binary vector. Refusing every vector spelled "null" refused
// that too.
func TestBase64NullIsAValidBinaryVector(t *testing.T) {
	binaryField := &schemapb.FieldSchema{
		Name:       "bv",
		DataType:   schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "24"}},
	}
	schema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false), binaryField,
		},
	}
	body := []byte(fmt.Sprintf(`{"data": {"%s": 1, "bv": "null"}}`, FieldBookID))
	rows, _, err := checkAndSetData(body, schema, false)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, []byte{0x9e, 0xe9, 0x65}, rows[0]["bv"])

	// a float vector has no base64 spelling, so "null" there is still a null
	floatField := generateVectorFieldSchema(schemapb.DataType_FloatVector)
	floatField.Name = "fv"
	floatSchema := &schemapb.CollectionSchema{
		Name: DefaultCollectionName,
		Fields: []*schemapb.FieldSchema{
			generatePrimaryField(schemapb.DataType_Int64, false), floatField,
		},
	}
	_, _, err = checkAndSetData(
		[]byte(fmt.Sprintf(`{"data": {"%s": 1, "fv": "null"}}`, FieldBookID)), floatSchema, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}
