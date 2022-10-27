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
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
)

func sampleSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      102,
				Name:         "field_bool",
				IsPrimaryKey: false,
				Description:  "bool",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      103,
				Name:         "field_int8",
				IsPrimaryKey: false,
				Description:  "int8",
				DataType:     schemapb.DataType_Int8,
			},
			{
				FieldID:      104,
				Name:         "field_int16",
				IsPrimaryKey: false,
				Description:  "int16",
				DataType:     schemapb.DataType_Int16,
			},
			{
				FieldID:      105,
				Name:         "field_int32",
				IsPrimaryKey: false,
				Description:  "int32",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      106,
				Name:         "field_int64",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      107,
				Name:         "field_float",
				IsPrimaryKey: false,
				Description:  "float",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      108,
				Name:         "field_double",
				IsPrimaryKey: false,
				Description:  "double",
				DataType:     schemapb.DataType_Double,
			},
			{
				FieldID:      109,
				Name:         "field_string",
				IsPrimaryKey: false,
				Description:  "string",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "128"},
				},
			},
			{
				FieldID:      110,
				Name:         "field_binary_vector",
				IsPrimaryKey: false,
				Description:  "binary_vector",
				DataType:     schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "16"},
				},
			},
			{
				FieldID:      111,
				Name:         "field_float_vector",
				IsPrimaryKey: false,
				Description:  "float_vector",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	return schema
}

func strKeySchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "uid",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "1024"},
				},
			},
			{
				FieldID:      102,
				Name:         "int_scalar",
				IsPrimaryKey: false,
				Description:  "int_scalar",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      103,
				Name:         "float_scalar",
				IsPrimaryKey: false,
				Description:  "float_scalar",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      104,
				Name:         "string_scalar",
				IsPrimaryKey: false,
				Description:  "string_scalar",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "128"},
				},
			},
			{
				FieldID:      105,
				Name:         "bool_scalar",
				IsPrimaryKey: false,
				Description:  "bool_scalar",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      106,
				Name:         "vectors",
				IsPrimaryKey: false,
				Description:  "vectors",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	return schema
}

func Test_IsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	assert.False(t, isCanceled(ctx))
	cancel()
	assert.True(t, isCanceled(ctx))
}

func Test_InitSegmentData(t *testing.T) {
	testFunc := func(schema *schemapb.CollectionSchema) {
		fields := initSegmentData(schema)
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
}

func Test_InitValidators(t *testing.T) {
	validators := make(map[storage.FieldID]*Validator)
	err := initValidators(nil, validators)
	assert.NotNil(t, err)

	schema := sampleSchema()
	// success case
	err = initValidators(schema, validators)
	assert.Nil(t, err)
	assert.Equal(t, len(schema.Fields), len(validators))
	name2ID := make(map[string]storage.FieldID)
	for _, field := range schema.Fields {
		name2ID[field.GetName()] = field.GetFieldID()
	}

	checkFunc := func(funcName string, validVal interface{}, invalidVal interface{}) {
		id := name2ID[funcName]
		v, ok := validators[id]
		assert.True(t, ok)
		err = v.validateFunc(validVal)
		assert.Nil(t, err)
		err = v.validateFunc(invalidVal)
		assert.NotNil(t, err)
	}

	// validate functions
	var validVal interface{} = true
	var invalidVal interface{} = "aa"
	checkFunc("field_bool", validVal, invalidVal)

	validVal = float64(100)
	invalidVal = "aa"
	checkFunc("field_int8", validVal, invalidVal)
	checkFunc("field_int16", validVal, invalidVal)
	checkFunc("field_int32", validVal, invalidVal)
	checkFunc("field_int64", validVal, invalidVal)
	checkFunc("field_float", validVal, invalidVal)
	checkFunc("field_double", validVal, invalidVal)

	validVal = "aa"
	invalidVal = 100
	checkFunc("field_string", validVal, invalidVal)

	validVal = []interface{}{float64(100), float64(101)}
	invalidVal = "aa"
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(100)}
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(100), float64(101), float64(102)}
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{true, true}
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(255), float64(-1)}
	checkFunc("field_binary_vector", validVal, invalidVal)

	validVal = []interface{}{float64(1), float64(2), float64(3), float64(4)}
	invalidVal = true
	checkFunc("field_float_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(1), float64(2), float64(3)}
	checkFunc("field_float_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(1), float64(2), float64(3), float64(4), float64(5)}
	checkFunc("field_float_vector", validVal, invalidVal)
	invalidVal = []interface{}{"a", "b", "c", "d"}
	checkFunc("field_float_vector", validVal, invalidVal)

	// error cases
	schema = &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields:      make([]*schemapb.FieldSchema, 0),
	}
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      111,
		Name:         "field_float_vector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "aa"},
		},
	})

	validators = make(map[storage.FieldID]*Validator)
	err = initValidators(schema, validators)
	assert.NotNil(t, err)

	schema.Fields = make([]*schemapb.FieldSchema, 0)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      110,
		Name:         "field_binary_vector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "aa"},
		},
	})

	err = initValidators(schema, validators)
	assert.NotNil(t, err)
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
		Name:         "field_float_vector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}

	dim, err := getFieldDimension(schema)
	assert.Nil(t, err)
	assert.Equal(t, 4, dim)

	schema.TypeParams = []*commonpb.KeyValuePair{
		{Key: "dim", Value: "abc"},
	}
	dim, err = getFieldDimension(schema)
	assert.NotNil(t, err)
	assert.Equal(t, 0, dim)

	schema.TypeParams = []*commonpb.KeyValuePair{}
	dim, err = getFieldDimension(schema)
	assert.NotNil(t, err)
	assert.Equal(t, 0, dim)
}

func Test_TryFlushBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	flushCounter := 0
	flushRowCount := 0
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
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
	maxTotalSize := int64(2048)
	shardNum := int32(3)

	// prepare flush data, 3 shards, each shard 10 rows
	rowCount := 10
	fieldsData := createFieldsData(rowCount)

	// non-force flush
	segmentsData := createSegmentsData(fieldsData, shardNum)
	err := tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, flushCounter)
	assert.Equal(t, 0, flushRowCount)

	// force flush
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, true)
	assert.Nil(t, err)
	assert.Equal(t, int(shardNum), flushCounter)
	assert.Equal(t, rowCount*int(shardNum), flushRowCount)

	// after force flush, no data left
	flushCounter = 0
	flushRowCount = 0
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, true)
	assert.Nil(t, err)
	assert.Equal(t, 0, flushCounter)
	assert.Equal(t, 0, flushRowCount)

	// flush when segment size exceeds blockSize
	segmentsData = createSegmentsData(fieldsData, shardNum)
	blockSize = 100 // blockSize is 100 bytes, less than the 10 rows size
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, false)
	assert.Nil(t, err)
	assert.Equal(t, int(shardNum), flushCounter)
	assert.Equal(t, rowCount*int(shardNum), flushRowCount)

	flushCounter = 0
	flushRowCount = 0
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, true) // no data left
	assert.Nil(t, err)
	assert.Equal(t, 0, flushCounter)
	assert.Equal(t, 0, flushRowCount)

	// flush when segments total size exceeds maxTotalSize
	segmentsData = createSegmentsData(fieldsData, shardNum)
	blockSize = 4096   // blockSize is 4096 bytes, larger than the 10 rows size
	maxTotalSize = 100 // maxTotalSize is 100 bytes, less than the 30 rows size
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, flushCounter) // only the max segment is flushed
	assert.Equal(t, 10, flushRowCount)

	flushCounter = 0
	flushRowCount = 0
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, true) // two segments left
	assert.Nil(t, err)
	assert.Equal(t, 2, flushCounter)
	assert.Equal(t, 20, flushRowCount)

	// canceled
	cancel()
	flushCounter = 0
	flushRowCount = 0
	segmentsData = createSegmentsData(fieldsData, shardNum)
	err = tryFlushBlocks(ctx, segmentsData, sampleSchema(), flushFunc, blockSize, maxTotalSize, true)
	assert.Error(t, err)
	assert.Equal(t, 0, flushCounter)
	assert.Equal(t, 0, flushRowCount)
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
	str = getTypeName(schemapb.DataType_None)
	assert.Equal(t, "InvalidType", str)
}
