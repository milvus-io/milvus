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
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
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

func Test_NewJSONRowConsumer(t *testing.T) {
	// nil schema
	consumer, err := NewJSONRowConsumer(nil, nil, 2, 16, nil)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	// wrong schema
	schema := &schemapb.CollectionSchema{
		Name:   "schema",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       false,
				DataType:     schemapb.DataType_None,
			},
		},
	}
	consumer, err = NewJSONRowConsumer(schema, nil, 2, 16, nil)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	// no primary key
	schema.Fields[0].IsPrimaryKey = false
	schema.Fields[0].DataType = schemapb.DataType_Int64
	consumer, err = NewJSONRowConsumer(schema, nil, 2, 16, nil)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	// primary key is autoid, but no IDAllocator
	schema.Fields[0].IsPrimaryKey = true
	schema.Fields[0].AutoID = true
	consumer, err = NewJSONRowConsumer(schema, nil, 2, 16, nil)
	assert.Error(t, err)
	assert.Nil(t, consumer)

	// success
	consumer, err = NewJSONRowConsumer(sampleSchema(), nil, 2, 16, nil)
	assert.NotNil(t, consumer)
	assert.NoError(t, err)
}

func Test_JSONRowConsumerFlush(t *testing.T) {
	var callTime int32
	var totalCount int
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shard int) error {
		callTime++
		field, ok := fields[101]
		assert.True(t, ok)
		assert.Greater(t, field.RowNum(), 0)
		totalCount += field.RowNum()
		return nil
	}

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

	var shardNum int32 = 4
	var blockSize int64 = 1
	consumer, err := NewJSONRowConsumer(schema, nil, shardNum, blockSize, flushFunc)
	assert.NotNil(t, consumer)
	assert.NoError(t, err)

	// force flush
	rowCountEachShard := 100
	for i := 0; i < int(shardNum); i++ {
		pkFieldData := consumer.segmentsData[i][101].(*storage.Int64FieldData)
		for j := 0; j < rowCountEachShard; j++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(j))
		}
	}

	err = consumer.flush(true)
	assert.NoError(t, err)
	assert.Equal(t, shardNum, callTime)
	assert.Equal(t, rowCountEachShard*int(shardNum), totalCount)
	assert.Equal(t, 0, len(consumer.IDRange())) // not auto-generated id, no id range

	// execeed block size trigger flush
	callTime = 0
	totalCount = 0
	for i := 0; i < int(shardNum); i++ {
		consumer.segmentsData[i] = initSegmentData(schema)
		if i%2 == 0 {
			continue
		}
		pkFieldData := consumer.segmentsData[i][101].(*storage.Int64FieldData)
		for j := 0; j < rowCountEachShard; j++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(j))
		}
	}
	err = consumer.flush(true)
	assert.NoError(t, err)
	assert.Equal(t, shardNum/2, callTime)
	assert.Equal(t, rowCountEachShard*int(shardNum)/2, totalCount)
	assert.Equal(t, 0, len(consumer.IDRange())) // not auto-generated id, no id range
}

func Test_JSONRowConsumerHandle(t *testing.T) {
	ctx := context.Background()
	idAllocator := newIDAllocator(ctx, t, errors.New("error"))

	var callTime int32
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shard int) error {
		callTime++
		return errors.New("dummy error")
	}

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

	var consumer *JSONRowConsumer
	err := consumer.Handle(nil)
	assert.Error(t, err)

	t.Run("handle int64 pk", func(t *testing.T) {
		consumer, err := NewJSONRowConsumer(schema, idAllocator, 1, 1, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)

		pkFieldData := consumer.segmentsData[0][101].(*storage.Int64FieldData)
		for i := 0; i < 10; i++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(i))
		}

		// nil input will trigger flush
		err = consumer.Handle(nil)
		assert.Error(t, err)
		assert.Equal(t, int32(1), callTime)

		// optional flush
		callTime = 0
		rowCount := 100
		pkFieldData = consumer.segmentsData[0][101].(*storage.Int64FieldData)
		for j := 0; j < rowCount; j++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(j))
		}

		input := make([]map[storage.FieldID]interface{}, rowCount)
		for j := 0; j < rowCount; j++ {
			input[j] = make(map[int64]interface{})
			input[j][101] = int64(j)
		}
		err = consumer.Handle(input)
		assert.Error(t, err)
		assert.Equal(t, int32(1), callTime)

		// failed to auto-generate pk
		consumer.blockSize = 1024 * 1024
		err = consumer.Handle(input)
		assert.Error(t, err)

		// hash int64 pk
		consumer.rowIDAllocator = newIDAllocator(ctx, t, nil)
		err = consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(rowCount), consumer.rowCounter)
		assert.Equal(t, 2, len(consumer.autoIDRange))
		assert.Equal(t, int64(1), consumer.autoIDRange[0])
		assert.Equal(t, int64(1+rowCount), consumer.autoIDRange[1])

		// pk is auto-generated but IDAllocator is nil
		consumer.rowIDAllocator = nil
		err = consumer.Handle(input)
		assert.Error(t, err)

		// pk is not auto-generated, pk is not numeric value
		input = make([]map[storage.FieldID]interface{}, 1)
		input[0] = make(map[int64]interface{})
		input[0][101] = "1"

		schema.Fields[0].AutoID = false
		consumer, err = NewJSONRowConsumer(schema, idAllocator, 1, 1, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)
		err = consumer.Handle(input)
		assert.Error(t, err)

		// pk is numeric value, but cannot parsed
		input[0][101] = json.Number("A1")
		err = consumer.Handle(input)
		assert.Error(t, err)
	})

	t.Run("handle varchar pk", func(t *testing.T) {
		schema = &schemapb.CollectionSchema{
			Name:   "schema",
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      101,
					Name:         "uid",
					IsPrimaryKey: true,
					AutoID:       true,
					DataType:     schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxLengthKey, Value: "1024"},
					},
				},
			},
		}

		idAllocator := newIDAllocator(ctx, t, nil)
		consumer, err := NewJSONRowConsumer(schema, idAllocator, 1, 1024*1024, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)

		rowCount := 100
		input := make([]map[storage.FieldID]interface{}, rowCount)
		for j := 0; j < rowCount; j++ {
			input[j] = make(map[int64]interface{})
			input[j][101] = "abc"
		}

		// varchar pk cannot be auto-generated
		err = consumer.Handle(input)
		assert.Error(t, err)

		// hash varchar pk
		schema.Fields[0].AutoID = false
		consumer, err = NewJSONRowConsumer(schema, idAllocator, 1, 1024*1024, flushFunc)
		assert.NotNil(t, consumer)
		assert.NoError(t, err)

		err = consumer.Handle(input)
		assert.NoError(t, err)
		assert.Equal(t, int64(rowCount), consumer.RowCount())
		assert.Equal(t, 0, len(consumer.autoIDRange))

		// pk is not string value
		input = make([]map[storage.FieldID]interface{}, 1)
		input[0] = make(map[int64]interface{})
		input[0][101] = false
		err = consumer.Handle(input)
		assert.Error(t, err)
	})
}

func Test_GetPrimaryKey(t *testing.T) {
	fieldName := "dummy"
	var obj1 interface{} = "aa"
	val, err := getPrimaryKey(obj1, fieldName, true)
	assert.Equal(t, val, "aa")
	assert.NoError(t, err)

	val, err = getPrimaryKey(obj1, fieldName, false)
	assert.Empty(t, val)
	assert.Error(t, err)

	var obj2 interface{} = json.Number("10")
	val, err = getPrimaryKey(obj2, fieldName, false)
	assert.Equal(t, val, "10")
	assert.NoError(t, err)

	val, err = getPrimaryKey(obj2, fieldName, true)
	assert.Empty(t, val)
	assert.Error(t, err)
}
