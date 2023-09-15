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

package importutil

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
)

func Test_CSVRowConsumerNew(t *testing.T) {
	ctx := context.Background()

	t.Run("nil schema", func(t *testing.T) {
		consumer, err := NewCSVRowConsumer(ctx, nil, nil, 16, nil)
		assert.Error(t, err)
		assert.Nil(t, consumer)
	})

	t.Run("wrong schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:   "schema",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      101,
					Name:         "uid",
					IsPrimaryKey: true,
					AutoID:       false,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		schema.Fields[0].DataType = schemapb.DataType_None
		consumer, err := NewCSVRowConsumer(ctx, collectionInfo, nil, 16, nil)
		assert.Error(t, err)
		assert.Nil(t, consumer)
	})

	t.Run("primary key is autoid but no IDAllocator", func(t *testing.T) {
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
			},
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		consumer, err := NewCSVRowConsumer(ctx, collectionInfo, nil, 16, nil)
		assert.Error(t, err)
		assert.Nil(t, consumer)
	})

	t.Run("succeed", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
		assert.NoError(t, err)

		consumer, err := NewCSVRowConsumer(ctx, collectionInfo, nil, 16, nil)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
	})
}

func Test_CSVRowConsumerInitValidators(t *testing.T) {
	ctx := context.Background()
	consumer := &CSVRowConsumer{
		ctx:        ctx,
		validators: make(map[int64]*CSVValidator),
	}

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	schema := collectionInfo.Schema
	err = consumer.initValidators(schema)
	assert.NoError(t, err)
	assert.Equal(t, len(schema.Fields), len(consumer.validators))
	for _, field := range schema.Fields {
		fieldID := field.GetFieldID()
		assert.Equal(t, field.GetName(), consumer.validators[fieldID].fieldName)
		if field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_String {
			assert.False(t, consumer.validators[fieldID].isString)
		} else {
			assert.True(t, consumer.validators[fieldID].isString)
		}
	}

	name2ID := make(map[string]storage.FieldID)
	for _, field := range schema.Fields {
		name2ID[field.GetName()] = field.GetFieldID()
	}

	fields := initBlockData(schema)
	assert.NotNil(t, fields)

	checkConvertFunc := func(funcName string, validVal string, invalidVal string) {
		id := name2ID[funcName]
		v, ok := consumer.validators[id]
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
		// all val is string type
		validVal := "true"
		invalidVal := "5"
		checkConvertFunc("FieldBool", validVal, invalidVal)

		validVal = "100"
		invalidVal = "128"
		checkConvertFunc("FieldInt8", validVal, invalidVal)

		invalidVal = "65536"
		checkConvertFunc("FieldInt16", validVal, invalidVal)

		invalidVal = "2147483648"
		checkConvertFunc("FieldInt32", validVal, invalidVal)

		invalidVal = "1.2"
		checkConvertFunc("FieldInt64", validVal, invalidVal)

		invalidVal = "dummy"
		checkConvertFunc("FieldFloat", validVal, invalidVal)
		checkConvertFunc("FieldDouble", validVal, invalidVal)

		// json type
		validVal = `{"x": 5, "y": true, "z": "hello"}`
		checkConvertFunc("FieldJSON", validVal, "a")
		checkConvertFunc("FieldJSON", validVal, "{")

		// the binary vector dimension is 16, shoud input two uint8 values, each value should between 0~255
		validVal = "[100, 101]"
		invalidVal = "[100, 1256]"
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)

		invalidVal = "false"
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)
		invalidVal = "[100]"
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)
		invalidVal = "[100.2, 102.5]"
		checkConvertFunc("FieldBinaryVector", validVal, invalidVal)

		// the float vector dimension is 4, each value should be valid float number
		validVal = "[1,2,3,4]"
		invalidVal = `[1,2,3,"dummy"]`
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
		invalidVal = "true"
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
		invalidVal = `[1]`
		checkConvertFunc("FieldFloatVector", validVal, invalidVal)
	})

	t.Run("init error cases", func(t *testing.T) {
		// schema is nil
		err := consumer.initValidators(nil)
		assert.Error(t, err)

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
		consumer.validators = make(map[int64]*CSVValidator)
		err = consumer.initValidators(schema)
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

		err = consumer.initValidators(schema)
		assert.Error(t, err)

		// unsupported data type
		schema.Fields = make([]*schemapb.FieldSchema, 0)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:      110,
			Name:         "dummy",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_None,
		})

		err = consumer.initValidators(schema)
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
		consumer.validators = make(map[int64]*CSVValidator)
		err = consumer.initValidators(schema)
		assert.NoError(t, err)

		v, ok := consumer.validators[102]
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

func Test_CSVRowConsumerHandleIntPK(t *testing.T) {
	ctx := context.Background()

	t.Run("nil input", func(t *testing.T) {
		var consumer *CSVRowConsumer
		err := consumer.Handle(nil)
		assert.Error(t, err)
	})

	schema := &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "FieldInt64",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "FieldVarchar",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:  103,
				Name:     "FieldFloat",
				DataType: schemapb.DataType_Float,
			},
		},
	}
	createConsumeFunc := func(shardNum int32, partitionIDs []int64, flushFunc ImportFlushFunc) *CSVRowConsumer {
		collectionInfo, err := NewCollectionInfo(schema, shardNum, partitionIDs)
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)
		consumer, err := NewCSVRowConsumer(ctx, collectionInfo, idAllocator, 1, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)

		return consumer
	}

	t.Run("auto pk no partition key", func(t *testing.T) {
		flushErrFunc := func(fields BlockData, shard int, partID int64) error {
			return errors.New("dummy error")
		}

		// rows to input
		inputRowCount := 100
		input := make([]map[storage.FieldID]string, inputRowCount)
		for i := 0; i < inputRowCount; i++ {
			input[i] = map[storage.FieldID]string{
				102: "string",
				103: "122.5",
			}
		}

		shardNum := int32(2)
		partitionID := int64(1)
		consumer := createConsumeFunc(shardNum, []int64{partitionID}, flushErrFunc)
		consumer.rowIDAllocator = newIDAllocator(ctx, t, errors.New("error"))

		waitFlushRowCount := 10
		fieldData := createFieldsData(schema, waitFlushRowCount)
		consumer.shardsData = createShardsData(schema, fieldData, shardNum, []int64{partitionID})

		// nil input will trigger force flush, flushErrFunc returns error
		err := consumer.Handle(nil)
		assert.Error(t, err)

		// optional flush, flushErrFunc returns error
		err = consumer.Handle(input)
		assert.Error(t, err)

		// reset flushFunc
		var callTime int32
		var flushedRowCount int
		consumer.callFlushFunc = func(fields BlockData, shard int, partID int64) error {
			callTime++
			assert.Less(t, int32(shard), shardNum)
			assert.Equal(t, partitionID, partID)
			assert.Greater(t, len(fields), 0)
			for _, v := range fields {
				assert.Greater(t, v.RowNum(), 0)
			}
			flushedRowCount += fields[102].RowNum()
			return nil
		}
		// optional flush succeed, each shard has 10 rows, idErrAllocator returns error
		err = consumer.Handle(input)
		assert.Error(t, err)
		assert.Equal(t, waitFlushRowCount*int(shardNum), flushedRowCount)
		assert.Equal(t, shardNum, callTime)

		// optional flush again, large blockSize, nothing flushed, idAllocator returns error
		callTime = int32(0)
		flushedRowCount = 0
		consumer.shardsData = createShardsData(schema, fieldData, shardNum, []int64{partitionID})
		consumer.rowIDAllocator = nil
		consumer.blockSize = 8 * 1024 * 1024
		err = consumer.Handle(input)
		assert.Error(t, err)
		assert.Equal(t, 0, flushedRowCount)
		assert.Equal(t, int32(0), callTime)

		// idAllocator is ok, consume 100 rows, the previous shardsData(10 rows per shard) is flushed
		callTime = int32(0)
		flushedRowCount = 0
		consumer.blockSize = 1
		consumer.rowIDAllocator = newIDAllocator(ctx, t, nil)
		err = consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, waitFlushRowCount*int(shardNum), flushedRowCount)
		assert.Equal(t, shardNum, callTime)
		assert.Equal(t, int64(inputRowCount), consumer.RowCount())
		assert.Equal(t, 2, len(consumer.IDRange()))
		assert.Equal(t, int64(1), consumer.IDRange()[0])
		assert.Equal(t, int64(1+inputRowCount), consumer.IDRange()[1])

		// call handle again, the 100 rows are flushed
		callTime = int32(0)
		flushedRowCount = 0
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, inputRowCount, flushedRowCount)
		assert.Equal(t, shardNum, callTime)
	})

	schema.Fields[0].AutoID = false

	t.Run("manual pk no partition key", func(t *testing.T) {
		shardNum := int32(1)
		partitionID := int64(100)

		var callTime int32
		var flushedRowCount int
		flushFunc := func(fields BlockData, shard int, partID int64) error {
			callTime++
			assert.Less(t, int32(shard), shardNum)
			assert.Equal(t, partitionID, partID)
			assert.Greater(t, len(fields), 0)
			flushedRowCount += fields[102].RowNum()
			return nil
		}

		consumer := createConsumeFunc(shardNum, []int64{partitionID}, flushFunc)

		// failed to convert pk to int value
		input := make([]map[storage.FieldID]string, 1)
		input[0] = map[int64]string{
			101: "abc",
			102: "string",
			103: "11.11",
		}

		err := consumer.Handle(input)
		assert.Error(t, err)

		// failed to hash to partition
		input[0] = map[int64]string{
			101: "99",
			102: "string",
			103: "11.11",
		}
		consumer.collectionInfo.PartitionIDs = nil
		err = consumer.Handle(input)
		assert.Error(t, err)
		consumer.collectionInfo.PartitionIDs = []int64{partitionID}

		// failed to convert value
		input[0] = map[int64]string{
			101: "99",
			102: "string",
			103: "abc.11",
		}
		err = consumer.Handle(input)
		assert.Error(t, err)
		consumer.shardsData = createShardsData(schema, nil, shardNum, []int64{partitionID}) // in-memory data is dirty, reset

		// succeed, consum 1 row
		input[0] = map[int64]string{
			101: "99",
			102: "string",
			103: "11.11",
		}
		err = consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), consumer.RowCount())
		assert.Equal(t, 0, len(consumer.IDRange()))

		// call handle again, the 1 row is flushed
		callTime = int32(0)
		flushedRowCount = 0
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, 1, flushedRowCount)
		assert.Equal(t, shardNum, callTime)

	})

	schema.Fields[1].IsPartitionKey = true

	t.Run("manual pk with partition key", func(t *testing.T) {
		// 10 partitions
		partitionIDs := make([]int64, 0)
		for i := 0; i < 10; i++ {
			partitionIDs = append(partitionIDs, int64(i))
		}

		shardNum := int32(2)
		var flushedRowCount int
		flushFunc := func(fields BlockData, shard int, partID int64) error {
			assert.Less(t, int32(shard), shardNum)
			assert.Contains(t, partitionIDs, partID)
			assert.Greater(t, len(fields), 0)
			flushedRowCount += fields[102].RowNum()
			return nil
		}

		consumer := createConsumeFunc(shardNum, partitionIDs, flushFunc)

		// rows to input
		inputRowCount := 100
		input := make([]map[storage.FieldID]string, inputRowCount)
		for i := 0; i < inputRowCount; i++ {
			input[i] = map[int64]string{
				101: strconv.Itoa(i),
				102: "partitionKey_" + strconv.Itoa(i),
				103: "6.18",
			}
		}

		// 100 rows are consumed to different partitions
		err := consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(inputRowCount), consumer.RowCount())

		// call handle again, 100 rows are flushed
		flushedRowCount = 0
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, inputRowCount, flushedRowCount)
	})
}

func Test_CSVRowConsumerHandleVarcharPK(t *testing.T) {
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "FieldVarchar",
				IsPrimaryKey: true,
				AutoID:       false,
				DataType:     schemapb.DataType_VarChar,
			},
			{
				FieldID:  102,
				Name:     "FieldInt64",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  103,
				Name:     "FieldFloat",
				DataType: schemapb.DataType_Float,
			},
		},
	}

	createConsumeFunc := func(shardNum int32, partitionIDs []int64, flushFunc ImportFlushFunc) *CSVRowConsumer {
		collectionInfo, err := NewCollectionInfo(schema, shardNum, partitionIDs)
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)
		consumer, err := NewCSVRowConsumer(ctx, collectionInfo, idAllocator, 1, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)

		return consumer
	}

	t.Run("no partition key", func(t *testing.T) {
		shardNum := int32(2)
		partitionID := int64(1)
		var callTime int32
		var flushedRowCount int
		flushFunc := func(fields BlockData, shard int, partID int64) error {
			callTime++
			assert.Less(t, int32(shard), shardNum)
			assert.Equal(t, partitionID, partID)
			assert.Greater(t, len(fields), 0)
			for _, v := range fields {
				assert.Greater(t, v.RowNum(), 0)
			}
			flushedRowCount += fields[102].RowNum()
			return nil
		}

		consumer := createConsumeFunc(shardNum, []int64{partitionID}, flushFunc)
		consumer.shardsData = createShardsData(schema, nil, shardNum, []int64{partitionID})

		// string type primary key cannot be auto-generated
		input := make([]map[storage.FieldID]string, 1)
		input[0] = map[storage.FieldID]string{
			101: "primaryKey_0",
			102: "1",
			103: "1.252",
		}

		consumer.collectionInfo.PrimaryKey.AutoID = true
		err := consumer.Handle(input)
		assert.Error(t, err)
		consumer.collectionInfo.PrimaryKey.AutoID = false

		// failed to hash to partition
		consumer.collectionInfo.PartitionIDs = nil
		err = consumer.Handle(input)
		assert.Error(t, err)
		consumer.collectionInfo.PartitionIDs = []int64{partitionID}

		// rows to input
		inputRowCount := 100
		input = make([]map[storage.FieldID]string, inputRowCount)
		for i := 0; i < inputRowCount; i++ {
			input[i] = map[int64]string{
				101: "primaryKey_" + strconv.Itoa(i),
				102: strconv.Itoa(i),
				103: "6.18",
			}
		}

		err = consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(inputRowCount), consumer.RowCount())
		assert.Equal(t, 0, len(consumer.IDRange()))

		// call handle again, 100 rows are flushed
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, inputRowCount, flushedRowCount)
		assert.Equal(t, shardNum, callTime)
	})

	schema.Fields[1].IsPartitionKey = true
	t.Run("has partition key", func(t *testing.T) {
		partitionIDs := make([]int64, 0)
		for i := 0; i < 10; i++ {
			partitionIDs = append(partitionIDs, int64(i))
		}

		shardNum := int32(2)
		var flushedRowCount int
		flushFunc := func(fields BlockData, shard int, partID int64) error {
			assert.Less(t, int32(shard), shardNum)
			assert.Contains(t, partitionIDs, partID)
			assert.Greater(t, len(fields), 0)
			flushedRowCount += fields[102].RowNum()
			return nil
		}

		consumer := createConsumeFunc(shardNum, partitionIDs, flushFunc)

		// rows to input
		inputRowCount := 100
		input := make([]map[storage.FieldID]string, inputRowCount)
		for i := 0; i < inputRowCount; i++ {
			input[i] = map[int64]string{
				101: "primaryKey_" + strconv.Itoa(i),
				102: strconv.Itoa(i),
				103: "6.18",
			}
		}

		err := consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(inputRowCount), consumer.RowCount())
		assert.Equal(t, 0, len(consumer.IDRange()))

		// call handle again, 100 rows are flushed
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, inputRowCount, flushedRowCount)
	})
}

func Test_CSVRowConsumerHashToPartition(t *testing.T) {
	ctx := context.Background()

	schema := &schemapb.CollectionSchema{
		Name: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "ID",
				IsPrimaryKey: true,
				AutoID:       false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "FieldVarchar",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:  102,
				Name:     "FieldInt64",
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	partitionID := int64(1)
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{partitionID})
	assert.NoError(t, err)
	consumer, err := NewCSVRowConsumer(ctx, collectionInfo, nil, 16, nil)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	input := map[int64]string{
		100: "1",
		101: "abc",
		102: "100",
	}
	t.Run("no partition key", func(t *testing.T) {
		partID, err := consumer.hashToPartition(input, 0)
		assert.NoError(t, err)
		assert.Equal(t, partitionID, partID)
	})

	t.Run("partition list is empty", func(t *testing.T) {
		collectionInfo.PartitionIDs = []int64{}
		partID, err := consumer.hashToPartition(input, 0)
		assert.Error(t, err)
		assert.Equal(t, int64(0), partID)
		collectionInfo.PartitionIDs = []int64{partitionID}
	})

	schema.Fields[1].IsPartitionKey = true
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)
	collectionInfo.PartitionIDs = []int64{1, 2, 3}

	t.Run("varchar partition key", func(t *testing.T) {
		input = map[int64]string{
			100: "1",
			101: "abc",
			102: "100",
		}

		partID, err := consumer.hashToPartition(input, 0)
		assert.NoError(t, err)
		assert.Contains(t, collectionInfo.PartitionIDs, partID)
	})

	schema.Fields[1].IsPartitionKey = false
	schema.Fields[2].IsPartitionKey = true
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)

	t.Run("int64 partition key", func(t *testing.T) {
		input = map[int64]string{
			100: "1",
			101: "abc",
			102: "ab0",
		}
		// parse int failed
		partID, err := consumer.hashToPartition(input, 0)
		assert.Error(t, err)
		assert.Equal(t, int64(0), partID)

		// succeed
		input[102] = "100"
		partID, err = consumer.hashToPartition(input, 0)
		assert.NoError(t, err)
		assert.Contains(t, collectionInfo.PartitionIDs, partID)

	})

}
