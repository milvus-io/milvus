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

package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
)

// TODO(dragondriver): add more test cases

const (
	maxTestStringLen     = 100
	testBoolField        = "bool"
	testInt32Field       = "int32"
	testInt64Field       = "int64"
	testFloatField       = "float"
	testDoubleField      = "double"
	testVarCharField     = "varChar"
	testFloatVecField    = "fvec"
	testBinaryVecField   = "bvec"
	testVecDim           = 128
	testMaxVarCharLength = 100
)

func constructCollectionSchema(
	int64Field, floatVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {

	pk := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
	}
}

func constructCollectionSchemaByDataType(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, autoID bool) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)

	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			Name:     fieldName,
			DataType: dataType,
		}
		if dataType == schemapb.DataType_FloatVector || dataType == schemapb.DataType_BinaryVector {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(testVecDim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   "max_length_per_row",
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

func constructCollectionSchemaWithAllType(
	boolField, int32Field, int64Field, floatField, doubleField string,
	floatVecField, binaryVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {

	b := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         boolField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Bool,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	i32 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int32Field,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int32,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	i64 := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	f := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Float,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	d := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         doubleField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Double,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	bVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         binaryVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}

	if enableMultipleVectorFields {
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				b,
				i32,
				i64,
				f,
				d,
				fVec,
				bVec,
			},
		}
	}

	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			b,
			i32,
			i64,
			f,
			d,
			fVec,
			// bVec,
		},
	}
}

func constructPlaceholderGroup(
	nq, dim int,
) *milvuspb.PlaceholderGroup {
	values := make([][]byte, 0, nq)
	for i := 0; i < nq; i++ {
		bs := make([]byte, 0, dim*4)
		for j := 0; j < dim; j++ {
			var buffer bytes.Buffer
			f := rand.Float32()
			err := binary.Write(&buffer, common.Endian, f)
			if err != nil {
				panic(err)
			}
			bs = append(bs, buffer.Bytes()...)
		}
		values = append(values, bs)
	}

	return &milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   milvuspb.PlaceholderType_FloatVector,
				Values: values,
			},
		},
	}
}

func constructSearchRequest(
	dbName, collectionName string,
	expr string,
	floatVecField string,
	nq, dim, nprobe, topk, roundDecimal int,
) *milvuspb.SearchRequest {
	params := make(map[string]string)
	params["nprobe"] = strconv.Itoa(nprobe)
	b, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	plg := constructPlaceholderGroup(nq, dim)
	plgBs, err := proto.Marshal(plg)
	if err != nil {
		panic(err)
	}

	return &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           dbName,
		CollectionName:   collectionName,
		PartitionNames:   nil,
		Dsl:              expr,
		PlaceholderGroup: plgBs,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     nil,
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   SearchParamsKey,
				Value: string(b),
			},
			{
				Key:   AnnsFieldKey,
				Value: floatVecField,
			},
			{
				Key:   TopKKey,
				Value: strconv.Itoa(topk),
			},
			{
				Key:   RoundDecimalKey,
				Value: strconv.Itoa(roundDecimal),
			},
		},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
	}
}

func TestInsertTask_checkLengthOfFieldsData(t *testing.T) {
	var err error

	// schema is empty, though won't happen in system
	case1 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		},
		BaseInsertTask: BaseInsertTask{
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "TestInsertTask_checkLengthOfFieldsData",
				CollectionName: "TestInsertTask_checkLengthOfFieldsData",
				PartitionName:  "TestInsertTask_checkLengthOfFieldsData",
			},
		},
	}

	err = case1.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has two fields, neither of them are autoID
	case2 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// case2.BaseInsertTask = BaseInsertTask{
	// 	InsertRequest: internalpb.InsertRequest{
	// 		Base: &commonpb.MsgBase{
	// 			MsgType:  commonpb.MsgType_Insert,
	// 			MsgID:    0,
	// 			SourceID: Params.ProxyCfg.GetNodeID(),
	// 		},
	// 	},
	// }
	err = case2.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// the num of passed fields is less than needed
	case2.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
		},
	}
	err = case2.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// satisfied
	case2.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
		},
		{
			Type: schemapb.DataType_Int64,
		},
	}
	err = case2.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has two field, one of them are autoID
	case3 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   true,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// case3.req = &milvuspb.InsertRequest{}
	err = case3.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// satisfied
	case3.FieldsData = []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
		},
	}
	err = case3.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has one field which is autoID
	case4 := insertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   true,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// satisfied
	// case4.req = &milvuspb.InsertRequest{}
	err = case4.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)
}

func TestInsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := insertTask{
		BaseInsertTask: BaseInsertTask{
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				NumRows: 0,
			},
		},
	}
	err = case1.CheckAligned()
	assert.NoError(t, err)

	// checkLengthOfFieldsData was already checked by TestInsertTask_checkLengthOfFieldsData

	boolFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}
	int8FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}
	int16FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}
	int32FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}
	int64FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}
	floatFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Float}
	doubleFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Double}
	floatVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
	binaryVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector}
	varCharFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}

	numRows := 20
	dim := 128
	case2 := insertTask{
		BaseInsertTask: BaseInsertTask{
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    internalpb.InsertDataVersion_ColumnBased,
				RowIDs:     generateInt64Array(numRows),
				Timestamps: generateUint64Array(numRows),
			},
		},
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkRowNums",
			Description: "TestInsertTask_checkRowNums",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				boolFieldSchema,
				int8FieldSchema,
				int16FieldSchema,
				int32FieldSchema,
				int64FieldSchema,
				floatFieldSchema,
				doubleFieldSchema,
				floatVectorFieldSchema,
				binaryVectorFieldSchema,
				varCharFieldSchema,
			},
		},
	}

	// satisfied
	case2.NumRows = uint64(numRows)
	case2.FieldsData = []*schemapb.FieldData{
		newScalarFieldData(boolFieldSchema, "Bool", numRows),
		newScalarFieldData(int8FieldSchema, "Int8", numRows),
		newScalarFieldData(int16FieldSchema, "Int16", numRows),
		newScalarFieldData(int32FieldSchema, "Int32", numRows),
		newScalarFieldData(int64FieldSchema, "Int64", numRows),
		newScalarFieldData(floatFieldSchema, "Float", numRows),
		newScalarFieldData(doubleFieldSchema, "Double", numRows),
		newFloatVectorFieldData("FloatVector", numRows, dim),
		newBinaryVectorFieldData("BinaryVector", numRows, dim),
		newScalarFieldData(varCharFieldSchema, "VarChar", numRows),
	}
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less bool data
	case2.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more bool data
	case2.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int8 data
	case2.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int8 data
	case2.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int16 data
	case2.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int16 data
	case2.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int32 data
	case2.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int32 data
	case2.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less int64 data
	case2.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more int64 data
	case2.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less float data
	case2.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more float data
	case2.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, nil, err)

	// less double data
	case2.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, nil, err)

	// less float vectors
	case2.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more float vectors
	case2.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less binary vectors
	case2.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more binary vectors
	case2.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	err = case2.CheckAligned()
	assert.NoError(t, err)

	// less double data
	case2.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows/2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows*2)
	err = case2.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows)
	err = case2.CheckAligned()
	assert.NoError(t, err)
}

func TestTranslateOutputFields(t *testing.T) {
	const (
		idFieldName           = "id"
		tsFieldName           = "timestamp"
		floatVectorFieldName  = "float_vector"
		binaryVectorFieldName = "binary_vector"
	)
	var outputFields []string
	var err error

	schema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, DataType: schemapb.DataType_FloatVector},
			{Name: binaryVectorFieldName, DataType: schemapb.DataType_BinaryVector},
		},
	}

	outputFields, err = translateOutputFields([]string{}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{" * "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{" % "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", "%"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	//=========================================================================
	outputFields, err = translateOutputFields([]string{}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", "%"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
}

func TestCreateCollectionTask(t *testing.T) {
	Params.Init()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	shardsNum := int32(2)
	prefix := "TestCreateCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	varCharField := "varChar"

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type[int64Field] = schemapb.DataType_Int64
	fieldName2Type[varCharField] = schemapb.DataType_VarChar
	fieldName2Type[floatVecField] = schemapb.DataType_FloatVector
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, int64Field, false)
	var marshaledSchema []byte
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	t.Run("on enqueue", func(t *testing.T) {
		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_CreateCollection, task.Type())
	})

	t.Run("ctx", func(t *testing.T) {
		traceCtx := task.TraceCtx()
		assert.NotNil(t, traceCtx)
	})

	t.Run("id", func(t *testing.T) {
		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
	})

	t.Run("name", func(t *testing.T) {
		assert.Equal(t, CreateCollectionTaskName, task.Name())
	})

	t.Run("ts", func(t *testing.T) {
		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())
	})

	t.Run("process task", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		// recreate -> fail
		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		err = task.PostExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("PreExecute", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		task.Schema = []byte{0x1, 0x2, 0x3, 0x4}
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.Schema = marshaledSchema

		task.ShardsNum = Params.ProxyCfg.MaxShardNum + 1
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.ShardsNum = shardsNum

		reqBackup := proto.Clone(task.CreateCollectionRequest).(*milvuspb.CreateCollectionRequest)
		schemaBackup := proto.Clone(schema).(*schemapb.CollectionSchema)

		schemaWithTooManyFields := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, Params.ProxyCfg.MaxFieldNum+1),
		}
		marshaledSchemaWithTooManyFields, err := proto.Marshal(schemaWithTooManyFields)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = marshaledSchemaWithTooManyFields
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		task.CreateCollectionRequest = reqBackup

		// validateCollectionName

		schema.Name = " " // empty
		emptyNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = emptyNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = prefix
		for i := 0; i < int(Params.ProxyCfg.MaxNameLength); i++ {
			schema.Name += strconv.Itoa(i % 10)
		}
		tooLongNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLongNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = "$" // invalid first char
		invalidFirstCharSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFirstCharSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateDuplicatedFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, schema.Fields[0])
		duplicatedFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = duplicatedFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validatePrimaryKey
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].IsPrimaryKey = false
		}
		noPrimaryFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noPrimaryFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].Name = "$"
		}
		invalidFieldNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFieldNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// validateMaxLengthPerRow
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_VarChar {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noTypeParamsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noTypeParamsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateVectorField
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "not int",
					},
				}
			}
		}
		dimNotIntSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = dimNotIntSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension) + 1),
					},
				}
			}
		}
		tooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields[1].DataType = schemapb.DataType_BinaryVector
		schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(int(Params.ProxyCfg.MaxDimension) + 1),
			},
		}
		binaryTooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = binaryTooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      0,
			Name:         "second_vector",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(128),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		})
		twoVecFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = twoVecFieldsSchema
		err = task.PreExecute(ctx)
		if enableMultipleVectorFields {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	})
}

func TestDropCollectionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)

	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	channelMgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer channelMgr.removeAllDMLStream()

	prefix := "TestDropCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	//CreateCollection
	task := &dropCollectionTask{
		Condition: NewTaskCondition(ctx),
		DropCollectionRequest: &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		chMgr:     channelMgr,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.NotNil(t, err)
	// createCollection in RootCood and fill GlobalMetaCache
	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.Nil(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)

}

func TestHasCollectionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)
	prefix := "TestHasCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	//CreateCollection
	task := &hasCollectionTask{
		Condition: NewTaskCondition(ctx),
		HasCollectionRequest: &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, false, task.result.Value)
	// createCollection in RootCood and fill GlobalMetaCache
	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, true, task.result.Value)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	rc.updateState(internalpb.StateCode_Abnormal)
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)
	err = task.Execute(ctx)
	assert.NotNil(t, err)

}

func TestDescribeCollectionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err := task.Execute(ctx)
	assert.Nil(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	// describe collection with id
	task.CollectionID = 1
	task.CollectionName = ""
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	rc.Stop()
	task.CollectionID = 0
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.Status.ErrorCode)
}

func TestDescribeCollectionTask_ShardsNum1(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	err = task.PreExecute(ctx)
	assert.Nil(t, err)

	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, shardsNum, task.result.ShardsNum)

}

func TestDescribeCollectionTask_ShardsNum2(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
	}

	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.Nil(t, err)

	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, task.result.Status.ErrorCode)
	assert.Equal(t, common.DefaultShardsNum, task.result.ShardsNum)
	rc.Stop()
}

func TestCreatePartitionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestCreatePartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &createPartitionTask{
		Condition: NewTaskCondition(ctx),
		CreatePartitionRequest: &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_CreatePartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestDropPartitionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestDropPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &dropPartitionTask{
		Condition: NewTaskCondition(ctx),
		DropPartitionRequest: &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestHasPartitionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestHasPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &hasPartitionTask{
		Condition: NewTaskCondition(ctx),
		HasPartitionRequest: &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestShowPartitionsTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestShowPartitionsTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &showPartitionsTask{
		Condition: NewTaskCondition(ctx),
		ShowPartitionsRequest: &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
			Type:           milvuspb.ShowType_All,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_ShowPartitions, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyCfg.GetNodeID(), task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	task.PartitionNames = []string{"#0xc0de"}
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionNames = []string{partitionName}
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	err = task.Execute(ctx)
	assert.NotNil(t, err)

}
func TestTask_Int64PrimaryKey(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.RetrieveResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	t.Run("create collection", func(t *testing.T) {
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testInt64Field, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createColT := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:       ctx,
			rootCoord: rc,
			result:    nil,
			schema:    nil,
		}

		assert.NoError(t, createColT.OnEnqueue())
		assert.NoError(t, createColT.PreExecute(ctx))
		assert.NoError(t, createColT.Execute(ctx))
		assert.NoError(t, createColT.PostExecute(ctx))

		_, _ = rc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	err = chMgr.createDMLMsgStream(collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, Params.ProxyCfg.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		task := &insertTask{
			BaseInsertTask: BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: internalpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: Params.ProxyCfg.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        internalpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			rowIDAllocator: idAllocator,
			segIDAssigner:  segAllocator,
			chMgr:          chMgr,
			chTicker:       ticker,
			vChannels:      nil,
			pChannels:      nil,
			schema:         nil,
		}

		for fieldName, dataType := range fieldName2Types {
			task.FieldsData = append(task.FieldsData, generateFieldData(dataType, fieldName, nb))
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			BaseDeleteTask: msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  Params.ProxyCfg.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			deleteExpr: "int64 in [0, 1]",
			ctx:        ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			chMgr:    chMgr,
			chTicker: ticker,
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())

		task.Base.MsgType = commonpb.MsgType_Delete
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})
}

func TestTask_VarCharPrimaryKey(t *testing.T) {
	var err error

	Params.Init()
	Params.ProxyCfg.RetrieveResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testVarCharField:  schemapb.DataType_VarChar,
		testFloatVecField: schemapb.DataType_FloatVector}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}
	nb := 10

	t.Run("create collection", func(t *testing.T) {
		schema := constructCollectionSchemaByDataType(collectionName, fieldName2Types, testVarCharField, false)
		marshaledSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)

		createColT := &createCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
				Schema:         marshaledSchema,
				ShardsNum:      shardsNum,
			},
			ctx:       ctx,
			rootCoord: rc,
			result:    nil,
			schema:    nil,
		}

		assert.NoError(t, createColT.OnEnqueue())
		assert.NoError(t, createColT.PreExecute(ctx))
		assert.NoError(t, createColT.Execute(ctx))
		assert.NoError(t, createColT.PostExecute(ctx))

		_, _ = rc.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.GetNodeID(),
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	})

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	err = chMgr.createDMLMsgStream(collectionID)
	assert.NoError(t, err)
	pchans, err := chMgr.getChannels(collectionID)
	assert.NoError(t, err)

	interval := time.Millisecond * 10
	tso := newMockTsoAllocator()

	ticker := newChannelsTimeTicker(ctx, interval, []string{}, newGetStatisticsFunc(pchans), tso)
	_ = ticker.start()
	defer ticker.close()

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, Params.ProxyCfg.GetNodeID())
	assert.NoError(t, err)
	_ = idAllocator.Start()
	defer idAllocator.Close()

	segAllocator, err := newSegIDAssigner(ctx, &mockDataCoord{expireTime: Timestamp(2500)}, getLastTick1)
	assert.NoError(t, err)
	segAllocator.Init()
	_ = segAllocator.Start()
	defer segAllocator.Close()

	t.Run("insert", func(t *testing.T) {
		hash := generateHashKeys(nb)
		task := &insertTask{
			BaseInsertTask: BaseInsertTask{
				BaseMsg: msgstream.BaseMsg{
					HashValues: hash,
				},
				InsertRequest: internalpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:  commonpb.MsgType_Insert,
						MsgID:    0,
						SourceID: Params.ProxyCfg.GetNodeID(),
					},
					DbName:         dbName,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					NumRows:        uint64(nb),
					Version:        internalpb.InsertDataVersion_ColumnBased,
				},
			},

			Condition: NewTaskCondition(ctx),
			ctx:       ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			rowIDAllocator: idAllocator,
			segIDAssigner:  segAllocator,
			chMgr:          chMgr,
			chTicker:       ticker,
			vChannels:      nil,
			pChannels:      nil,
			schema:         nil,
		}

		fieldID := common.StartOfUserFieldID
		for fieldName, dataType := range fieldName2Types {
			task.FieldsData = append(task.FieldsData, generateFieldData(dataType, fieldName, nb))
			fieldID++
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})

	t.Run("delete", func(t *testing.T) {
		task := &deleteTask{
			Condition: NewTaskCondition(ctx),
			BaseDeleteTask: msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{},
				DeleteRequest: internalpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						MsgID:     0,
						Timestamp: 0,
						SourceID:  Params.ProxyCfg.GetNodeID(),
					},
					CollectionName: collectionName,
					PartitionName:  partitionName,
				},
			},
			deleteExpr: "varChar in [\"milvus\", \"test\"]",
			ctx:        ctx,
			result: &milvuspb.MutationResult{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
				IDs:          nil,
				SuccIndex:    nil,
				ErrIndex:     nil,
				Acknowledged: false,
				InsertCnt:    0,
				DeleteCnt:    0,
				UpsertCnt:    0,
				Timestamp:    0,
			},
			chMgr:    chMgr,
			chTicker: ticker,
		}

		assert.NoError(t, task.OnEnqueue())
		assert.NotNil(t, task.TraceCtx())

		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())

		task.Base.MsgType = commonpb.MsgType_Delete
		assert.Equal(t, commonpb.MsgType_Delete, task.Type())

		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())

		assert.NoError(t, task.PreExecute(ctx))
		assert.NoError(t, task.Execute(ctx))
		assert.NoError(t, task.PostExecute(ctx))
	})
}

func TestCreateAlias_all(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestCreateAlias_all"
	collectionName := prefix + funcutil.GenRandomStr()
	task := &CreateAliasTask{
		Condition: NewTaskCondition(ctx),
		CreateAliasRequest: &milvuspb.CreateAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias1",
		},
		ctx: ctx,
		result: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_CreateAlias
	assert.Equal(t, commonpb.MsgType_CreateAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func TestDropAlias_all(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	task := &DropAliasTask{
		Condition: NewTaskCondition(ctx),
		DropAliasRequest: &milvuspb.DropAliasRequest{
			Base:  nil,
			Alias: "alias1",
		},
		ctx: ctx,
		result: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())
	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_DropAlias
	assert.Equal(t, commonpb.MsgType_DropAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

}

func TestAlterAlias_all(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestAlterAlias_all"
	collectionName := prefix + funcutil.GenRandomStr()
	task := &AlterAliasTask{
		Condition: NewTaskCondition(ctx),
		AlterAliasRequest: &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias1",
		},
		ctx: ctx,
		result: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_AlterAlias
	assert.Equal(t, commonpb.MsgType_AlterAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func Test_createIndexTask_getIndexedField(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		CreateIndexRequest: &milvuspb.CreateIndexRequest{
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         fieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
						TypeParams:   nil,
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						AutoID: false,
					},
				},
			}, nil
		})
		globalMetaCache = cache
		field, err := cit.getIndexedField(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, fieldName, field.GetName())
	})

	t.Run("schema not found", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return nil, errors.New("mock")
		})
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})

	t.Run("invalid schema", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						Name: fieldName,
					},
					{
						Name: fieldName, // duplicate
					},
				},
			}, nil
		})
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})

	t.Run("field not found", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						Name: fieldName + fieldName,
					},
				},
			}, nil
		})
		globalMetaCache = cache
		_, err := cit.getIndexedField(context.Background())
		assert.Error(t, err)
	})
}

func Test_fillDimension(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		assert.NoError(t, fillDimension(f, nil))
	})

	t.Run("no dim in schema", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
		}
		assert.Error(t, fillDimension(f, nil))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		assert.Error(t, fillDimension(f, map[string]string{"dim": "8"}))
	})

	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{}
		assert.NoError(t, fillDimension(f, m))
		assert.Equal(t, "128", m["dim"])
	})
}

func Test_checkTrain(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{
			"index_type":  "IVF_FLAT",
			"nlist":       "1024",
			"metric_type": "L2",
		}
		assert.NoError(t, checkTrain(f, m))
	})

	t.Run("scalar", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}
		m := map[string]string{
			"index_type": "scalar",
		}
		assert.NoError(t, checkTrain(f, m))
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{
			"index_type":  "IVF_FLAT",
			"nlist":       "1024",
			"metric_type": "L2",
			"dim":         "8",
		}
		assert.Error(t, checkTrain(f, m))
	})

	t.Run("invalid params", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
		}
		m := map[string]string{
			"index_type":  "IVF_FLAT",
			"metric_type": "L2",
		}
		assert.Error(t, checkTrain(f, m))
	})
}

func Test_createIndexTask_PreExecute(t *testing.T) {
	collectionName := "test"
	fieldName := "test"

	cit := &createIndexTask{
		CreateIndexRequest: &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_CreateIndex,
			},
			CollectionName: collectionName,
			FieldName:      fieldName,
		},
	}

	t.Run("normal", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
			return 100, nil
		})
		cache.setGetSchemaFunc(func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
			return &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         fieldName,
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
						TypeParams:   nil,
						IndexParams: []*commonpb.KeyValuePair{
							{
								Key:   "dim",
								Value: "128",
							},
						},
						AutoID: false,
					},
				},
			}, nil
		})
		globalMetaCache = cache
		cit.CreateIndexRequest.ExtraParams = []*commonpb.KeyValuePair{
			{
				Key:   "index_type",
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: "1024",
			},
			{
				Key:   "metric_type",
				Value: "L2",
			},
		}
		assert.NoError(t, cit.PreExecute(context.Background()))
	})

	t.Run("collection not found", func(t *testing.T) {
		cache := newMockCache()
		cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
			return 0, errors.New("mock")
		})
		globalMetaCache = cache
		assert.Error(t, cit.PreExecute(context.Background()))
	})
}
