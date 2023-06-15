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
	"encoding/json"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type mockIDAllocator struct {
	allocErr error
}

func (a *mockIDAllocator) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return &rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ID:    int64(1),
		Count: req.Count,
	}, a.allocErr
}

func newIDAllocator(ctx context.Context, t *testing.T, allocErr error) *allocator.IDAllocator {
	mockIDAllocator := &mockIDAllocator{
		allocErr: allocErr,
	}

	idAllocator, err := allocator.NewIDAllocator(ctx, mockIDAllocator, int64(1))
	assert.NoError(t, err)
	err = idAllocator.Start()
	assert.NoError(t, err)

	return idAllocator
}

func Test_GetKeyValue(t *testing.T) {
	fieldName := "dummy"
	var obj1 interface{} = "aa"
	val, err := getKeyValue(obj1, fieldName, true)
	assert.Equal(t, val, "aa")
	assert.NoError(t, err)

	val, err = getKeyValue(obj1, fieldName, false)
	assert.Empty(t, val)
	assert.Error(t, err)

	var obj2 interface{} = json.Number("10")
	val, err = getKeyValue(obj2, fieldName, false)
	assert.Equal(t, val, "10")
	assert.NoError(t, err)

	val, err = getKeyValue(obj2, fieldName, true)
	assert.Empty(t, val)
	assert.Error(t, err)
}

func Test_JSONRowConsumerNew(t *testing.T) {
	ctx := context.Background()

	t.Run("nil schema", func(t *testing.T) {
		consumer, err := NewJSONRowConsumer(ctx, nil, nil, 16, nil)
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
		consumer, err := NewJSONRowConsumer(ctx, collectionInfo, nil, 16, nil)
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

		consumer, err := NewJSONRowConsumer(ctx, collectionInfo, nil, 16, nil)
		assert.Error(t, err)
		assert.Nil(t, consumer)
	})

	t.Run("succeed", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
		assert.NoError(t, err)
		consumer, err := NewJSONRowConsumer(ctx, collectionInfo, nil, 16, nil)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)
	})
}

func Test_JSONRowConsumerHandleIntPK(t *testing.T) {
	ctx := context.Background()

	t.Run("nil input", func(t *testing.T) {
		var consumer *JSONRowConsumer
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

	createConsumeFunc := func(shardNum int32, partitionIDs []int64, flushFunc ImportFlushFunc) *JSONRowConsumer {
		collectionInfo, err := NewCollectionInfo(schema, shardNum, partitionIDs)
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)
		consumer, err := NewJSONRowConsumer(ctx, collectionInfo, idAllocator, 1, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)

		return consumer
	}

	t.Run("auto pk no partition key", func(t *testing.T) {
		flushErrFunc := func(fields BlockData, shard int, partID int64) error {
			return errors.New("dummy error")
		}

		// rows to input
		intputRowCount := 100
		input := make([]map[storage.FieldID]interface{}, intputRowCount)
		for j := 0; j < intputRowCount; j++ {
			input[j] = map[int64]interface{}{
				102: "string",
				103: json.Number("6.18"),
			}
		}

		shardNum := int32(2)
		partitionID := int64(1)
		consumer := createConsumeFunc(shardNum, []int64{partitionID}, flushErrFunc)
		consumer.rowIDAllocator = newIDAllocator(ctx, t, errors.New("error"))

		waitFlushRowCount := 10
		fieldsData := createFieldsData(schema, waitFlushRowCount)
		consumer.shardsData = createShardsData(schema, fieldsData, shardNum, []int64{partitionID})

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
		consumer.shardsData = createShardsData(schema, fieldsData, shardNum, []int64{partitionID})
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
		assert.Equal(t, int64(intputRowCount), consumer.RowCount())
		assert.Equal(t, 2, len(consumer.IDRange()))
		assert.Equal(t, int64(1), consumer.IDRange()[0])
		assert.Equal(t, int64(1+intputRowCount), consumer.IDRange()[1])

		// call handle again, the 100 rows are flushed
		callTime = int32(0)
		flushedRowCount = 0
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, intputRowCount, flushedRowCount)
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

		// failed to parse primary key
		input := make([]map[storage.FieldID]interface{}, 1)
		input[0] = map[int64]interface{}{
			101: int64(99),
			102: "string",
			103: 11.11,
		}

		err := consumer.Handle(input)
		assert.Error(t, err)

		// failed to convert pk to int value
		input[0] = map[int64]interface{}{
			101: json.Number("a"),
			102: "string",
			103: 11.11,
		}

		err = consumer.Handle(input)
		assert.Error(t, err)

		// failed to hash to partition
		input[0] = map[int64]interface{}{
			101: json.Number("99"),
			102: "string",
			103: json.Number("4.56"),
		}
		consumer.collectionInfo.PartitionIDs = nil
		err = consumer.Handle(input)
		assert.Error(t, err)
		consumer.collectionInfo.PartitionIDs = []int64{partitionID}

		// failed to convert value
		input[0] = map[int64]interface{}{
			101: json.Number("99"),
			102: "string",
			103: json.Number("abc.56"),
		}

		err = consumer.Handle(input)
		assert.Error(t, err)
		consumer.shardsData = createShardsData(schema, nil, shardNum, []int64{partitionID}) // in-memory data is dirty, reset

		// succeed, consume 1 row
		input[0] = map[int64]interface{}{
			101: json.Number("99"),
			102: "string",
			103: json.Number("4.56"),
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
		for j := 0; j < 10; j++ {
			partitionIDs = append(partitionIDs, int64(j))
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
		intputRowCount := 100
		input := make([]map[storage.FieldID]interface{}, intputRowCount)
		for j := 0; j < intputRowCount; j++ {
			input[j] = map[int64]interface{}{
				101: json.Number(strconv.Itoa(j)),
				102: "partitionKey_" + strconv.Itoa(j),
				103: json.Number("6.18"),
			}
		}

		// 100 rows are consumed to different partitions
		err := consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(intputRowCount), consumer.RowCount())

		// call handle again, 100 rows are flushed
		flushedRowCount = 0
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, intputRowCount, flushedRowCount)
	})
}

func Test_JSONRowConsumerHandleVarcharPK(t *testing.T) {
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

	createConsumeFunc := func(shardNum int32, partitionIDs []int64, flushFunc ImportFlushFunc) *JSONRowConsumer {
		collectionInfo, err := NewCollectionInfo(schema, shardNum, partitionIDs)
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)
		consumer, err := NewJSONRowConsumer(ctx, collectionInfo, idAllocator, 1, flushFunc)
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
		input := make([]map[storage.FieldID]interface{}, 1)
		input[0] = map[int64]interface{}{
			101: true,
			102: json.Number("1"),
			103: json.Number("1.56"),
		}
		consumer.collectionInfo.PrimaryKey.AutoID = true
		err := consumer.Handle(input)
		assert.Error(t, err)
		consumer.collectionInfo.PrimaryKey.AutoID = false

		// failed to parse primary key
		err = consumer.Handle(input)
		assert.Error(t, err)

		// failed to hash to partition
		input[0] = map[int64]interface{}{
			101: "primaryKey_0",
			102: json.Number("1"),
			103: json.Number("1.56"),
		}
		consumer.collectionInfo.PartitionIDs = nil
		err = consumer.Handle(input)
		assert.Error(t, err)
		consumer.collectionInfo.PartitionIDs = []int64{partitionID}

		// rows to input
		intputRowCount := 100
		input = make([]map[storage.FieldID]interface{}, intputRowCount)
		for j := 0; j < intputRowCount; j++ {
			input[j] = map[int64]interface{}{
				101: "primaryKey_" + strconv.Itoa(j),
				102: json.Number(strconv.Itoa(j)),
				103: json.Number("0.618"),
			}
		}

		// rows are consumed
		err = consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(intputRowCount), consumer.RowCount())
		assert.Equal(t, 0, len(consumer.IDRange()))

		// call handle again, 100 rows are flushed
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, intputRowCount, flushedRowCount)
		assert.Equal(t, shardNum, callTime)
	})

	schema.Fields[1].IsPartitionKey = true
	t.Run("has partition key", func(t *testing.T) {
		// 10 partitions
		partitionIDs := make([]int64, 0)
		for j := 0; j < 10; j++ {
			partitionIDs = append(partitionIDs, int64(j))
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
		intputRowCount := 100
		input := make([]map[storage.FieldID]interface{}, intputRowCount)
		for j := 0; j < intputRowCount; j++ {
			input[j] = map[int64]interface{}{
				101: "primaryKey_" + strconv.Itoa(j),
				102: json.Number(strconv.Itoa(j)),
				103: json.Number("0.618"),
			}
		}

		// 100 rows are consumed to different partitions
		err := consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(intputRowCount), consumer.RowCount())

		// call handle again, 100 rows are flushed
		flushedRowCount = 0
		err = consumer.Handle(nil)
		assert.NoError(t, err)
		assert.Equal(t, intputRowCount, flushedRowCount)

		// string type primary key cannot be auto-generated
		consumer.validators[101].autoID = true
		err = consumer.Handle(input)
		assert.Error(t, err)
	})
}

func Test_JSONRowHashToPartition(t *testing.T) {
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
	consumer, err := NewJSONRowConsumer(ctx, collectionInfo, nil, 16, nil)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	input := make(map[int64]interface{})
	input[100] = int64(1)
	input[101] = "abc"
	input[102] = int64(100)

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
		input := make(map[int64]interface{})
		input[100] = int64(1)
		input[101] = true
		input[102] = int64(100)

		// getKeyValue failed
		partID, err := consumer.hashToPartition(input, 0)
		assert.Error(t, err)
		assert.Equal(t, int64(0), partID)

		// succeed
		input[101] = "abc"
		partID, err = consumer.hashToPartition(input, 0)
		assert.NoError(t, err)
		assert.Contains(t, collectionInfo.PartitionIDs, partID)
	})

	schema.Fields[1].IsPartitionKey = false
	schema.Fields[2].IsPartitionKey = true
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)

	t.Run("int64 partition key", func(t *testing.T) {
		input := make(map[int64]interface{})
		input[100] = int64(1)
		input[101] = "abc"
		input[102] = 100

		// getKeyValue failed
		partID, err := consumer.hashToPartition(input, 0)
		assert.Error(t, err)
		assert.Equal(t, int64(0), partID)

		// parse int failed
		input[102] = json.Number("d")
		partID, err = consumer.hashToPartition(input, 0)
		assert.Error(t, err)
		assert.Equal(t, int64(0), partID)

		// succeed
		input[102] = json.Number("100")
		partID, err = consumer.hashToPartition(input, 0)
		assert.NoError(t, err)
		assert.Contains(t, collectionInfo.PartitionIDs, partID)
	})
}
