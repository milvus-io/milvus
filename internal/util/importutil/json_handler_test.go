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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
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
	assert.Nil(t, err)
	err = idAllocator.Start()
	assert.Nil(t, err)

	return idAllocator
}

func Test_NewJSONRowValidator(t *testing.T) {
	validator, err := NewJSONRowValidator(nil, nil)
	assert.NotNil(t, err)
	assert.Nil(t, validator)

	validator, err = NewJSONRowValidator(sampleSchema(), nil)
	assert.NotNil(t, validator)
	assert.Nil(t, err)
}

func Test_JSONRowValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	// 0 row case
	reader := strings.NewReader(`{
		"rows":[]
	}`)

	validator, err := NewJSONRowValidator(schema, nil)
	assert.NotNil(t, validator)
	assert.Nil(t, err)

	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), validator.ValidateCount())

	// missed some fields
	reader = strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
			{"field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]}
		]
	}`)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	// invalid dimension
	reader = strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": true, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0, 1, 66, 128, 0, 1, 66], "field_float_vector": [1.1, 1.2, 1.3, 1.4]}
		]
	}`)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	// invalid value type
	reader = strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": true, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]}
		]
	}`)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	// init failed
	validator.validators = nil
	err = validator.Handle(nil)
	assert.NotNil(t, err)

	// primary key is auto-generate, but user provide pk value, return error
	schema = &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "ID",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "Age",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	validator, err = NewJSONRowValidator(schema, nil)
	assert.NotNil(t, validator)
	assert.Nil(t, err)

	reader = strings.NewReader(`{
		"rows":[
			{"ID": 1, "Age": 2}
		]
	}`)
	parser = NewJSONParser(ctx, schema)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)
}

func Test_NewJSONRowConsumer(t *testing.T) {
	// nil schema
	consumer, err := NewJSONRowConsumer(nil, nil, 2, 16, nil)
	assert.NotNil(t, err)
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
	assert.NotNil(t, err)
	assert.Nil(t, consumer)

	// no primary key
	schema.Fields[0].IsPrimaryKey = false
	schema.Fields[0].DataType = schemapb.DataType_Int64
	consumer, err = NewJSONRowConsumer(schema, nil, 2, 16, nil)
	assert.NotNil(t, err)
	assert.Nil(t, consumer)

	// primary key is autoid, but no IDAllocator
	schema.Fields[0].IsPrimaryKey = true
	schema.Fields[0].AutoID = true
	consumer, err = NewJSONRowConsumer(schema, nil, 2, 16, nil)
	assert.NotNil(t, err)
	assert.Nil(t, consumer)

	// success
	consumer, err = NewJSONRowConsumer(sampleSchema(), nil, 2, 16, nil)
	assert.NotNil(t, consumer)
	assert.Nil(t, err)
}

func Test_JSONRowConsumer(t *testing.T) {
	ctx := context.Background()
	idAllocator := newIDAllocator(ctx, t, nil)

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	reader := strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
			{"field_bool": false, "field_int8": 11, "field_int16": 102, "field_int32": 1002, "field_int64": 10002, "field_float": 3.15, "field_double": 2.56, "field_string": "hello world", "field_binary_vector": [253, 0], "field_float_vector": [2.1, 2.2, 2.3, 2.4]},
			{"field_bool": true, "field_int8": 12, "field_int16": 103, "field_int32": 1003, "field_int64": 10003, "field_float": 3.16, "field_double": 3.56, "field_string": "hello world", "field_binary_vector": [252, 0], "field_float_vector": [3.1, 3.2, 3.3, 3.4]},
			{"field_bool": false, "field_int8": 13, "field_int16": 104, "field_int32": 1004, "field_int64": 10004, "field_float": 3.17, "field_double": 4.56, "field_string": "hello world", "field_binary_vector": [251, 0], "field_float_vector": [4.1, 4.2, 4.3, 4.4]},
			{"field_bool": true, "field_int8": 14, "field_int16": 105, "field_int32": 1005, "field_int64": 10005, "field_float": 3.18, "field_double": 5.56, "field_string": "hello world", "field_binary_vector": [250, 0], "field_float_vector": [5.1, 5.2, 5.3, 5.4]}
		]
	}`)

	var shardNum int32 = 2
	var callTime int32
	var totalCount int
	consumeFunc := func(fields map[storage.FieldID]storage.FieldData, shard int) error {
		assert.Equal(t, int(callTime), shard)
		callTime++
		rowCount := 0
		for _, data := range fields {
			if rowCount == 0 {
				rowCount = data.RowNum()
			} else {
				assert.Equal(t, rowCount, data.RowNum())
			}
		}
		totalCount += rowCount
		return nil
	}

	consumer, err := NewJSONRowConsumer(schema, idAllocator, shardNum, 1, consumeFunc)
	assert.NotNil(t, consumer)
	assert.Nil(t, err)

	validator, err := NewJSONRowValidator(schema, consumer)
	assert.NotNil(t, validator)
	assert.Nil(t, err)

	err = parser.ParseRows(reader, validator)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), validator.ValidateCount())

	assert.Equal(t, shardNum, callTime)
	assert.Equal(t, 5, totalCount)
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
	assert.Nil(t, err)

	// force flush
	rowCountEachShard := 100
	for i := 0; i < int(shardNum); i++ {
		pkFieldData := consumer.segmentsData[i][101].(*storage.Int64FieldData)
		for j := 0; j < rowCountEachShard; j++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(j))
		}
		pkFieldData.NumRows = []int64{int64(rowCountEachShard)}
	}

	err = consumer.flush(true)
	assert.Nil(t, err)
	assert.Equal(t, shardNum, callTime)
	assert.Equal(t, rowCountEachShard*int(shardNum), totalCount)

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
		pkFieldData.NumRows = []int64{int64(rowCountEachShard)}
	}
	err = consumer.flush(true)
	assert.Nil(t, err)
	assert.Equal(t, shardNum/2, callTime)
	assert.Equal(t, rowCountEachShard*int(shardNum)/2, totalCount)
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

	t.Run("handle int64 pk", func(t *testing.T) {
		consumer, err := NewJSONRowConsumer(schema, idAllocator, 1, 1, flushFunc)
		assert.NotNil(t, consumer)
		assert.Nil(t, err)

		pkFieldData := consumer.segmentsData[0][101].(*storage.Int64FieldData)
		for i := 0; i < 10; i++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(i))
		}
		pkFieldData.NumRows = []int64{int64(10)}

		// nil input will trigger flush
		err = consumer.Handle(nil)
		assert.NotNil(t, err)
		assert.Equal(t, int32(1), callTime)

		// optional flush
		callTime = 0
		rowCount := 100
		pkFieldData = consumer.segmentsData[0][101].(*storage.Int64FieldData)
		for j := 0; j < rowCount; j++ {
			pkFieldData.Data = append(pkFieldData.Data, int64(j))
		}
		pkFieldData.NumRows = []int64{int64(rowCount)}

		input := make([]map[storage.FieldID]interface{}, rowCount)
		for j := 0; j < rowCount; j++ {
			input[j] = make(map[int64]interface{})
			input[j][101] = int64(j)
		}
		err = consumer.Handle(input)
		assert.NotNil(t, err)
		assert.Equal(t, int32(1), callTime)

		// failed to auto-generate pk
		consumer.blockSize = 1024 * 1024
		err = consumer.Handle(input)
		assert.NotNil(t, err)

		// hash int64 pk
		consumer.rowIDAllocator = newIDAllocator(ctx, t, nil)
		err = consumer.Handle(input)
		assert.Nil(t, err)
		assert.Equal(t, int64(rowCount), consumer.rowCounter)
		assert.Equal(t, 2, len(consumer.autoIDRange))
		assert.Equal(t, int64(1), consumer.autoIDRange[0])
		assert.Equal(t, int64(1+rowCount), consumer.autoIDRange[1])
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
						{Key: "max_length", Value: "1024"},
					},
				},
			},
		}

		idAllocator := newIDAllocator(ctx, t, nil)
		consumer, err := NewJSONRowConsumer(schema, idAllocator, 1, 1024*1024, flushFunc)
		assert.NotNil(t, consumer)
		assert.Nil(t, err)

		rowCount := 100
		input := make([]map[storage.FieldID]interface{}, rowCount)
		for j := 0; j < rowCount; j++ {
			input[j] = make(map[int64]interface{})
			input[j][101] = "abc"
		}

		// varchar pk cannot be autoid
		err = consumer.Handle(input)
		assert.NotNil(t, err)

		// hash varchar pk
		schema.Fields[0].AutoID = false
		consumer, err = NewJSONRowConsumer(schema, idAllocator, 1, 1024*1024, flushFunc)
		assert.NotNil(t, consumer)
		assert.Nil(t, err)

		err = consumer.Handle(input)
		assert.Nil(t, err)
		assert.Equal(t, int64(rowCount), consumer.rowCounter)
		assert.Equal(t, 0, len(consumer.autoIDRange))
	})
}

func Test_JSONRowConsumerStringKey(t *testing.T) {
	ctx := context.Background()
	idAllocator := newIDAllocator(ctx, t, nil)

	schema := strKeySchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	reader := strings.NewReader(`{
		"rows": [{
				"uid": "Dm4aWrbNzhmjwCTEnCJ9LDPO2N09sqysxgVfbH9Zmn3nBzmwsmk0eZN6x7wSAoPQ",
				"int_scalar": 9070353,
				"float_scalar": 0.9798043638085004,
				"string_scalar": "ShQ44OX0z8kGpRPhaXmfSsdH7JHq5DsZzu0e2umS1hrWG0uONH2RIIAdOECaaXir",
				"bool_scalar": true,
				"vectors": [0.5040062902126952, 0.8297619818664708, 0.20248342801564806, 0.12834786423659314]
			},
			{
				"uid": "RP50U0d2napRjXu94a8oGikWgklvVsXFurp8RR4tHGw7N0gk1b7opm59k3FCpyPb",
				"int_scalar": 8505288,
				"float_scalar": 0.937913432198687,
				"string_scalar": "Ld4b0avxathBdNvCrtm3QsWO1pYktUVR7WgAtrtozIwrA8vpeactNhJ85CFGQnK5",
				"bool_scalar": false,
				"vectors": [0.528232122836893, 0.6916116750653186, 0.41443762522548705, 0.26624344144792056]
			},
			{
				"uid": "oxhFkQitWPPw0Bjmj7UQcn4iwvS0CU7RLAC81uQFFQjWtOdiB329CPyWkfGSeYfE",
				"int_scalar": 4392660,
				"float_scalar": 0.32381232630490264,
				"string_scalar": "EmAlB0xdQcxeBtwlZJQnLgKodiuRinynoQtg0eXrjkq24dQohzSm7Bx3zquHd3kO",
				"bool_scalar": false,
				"vectors": [0.7978693027281338, 0.12394906726785092, 0.42431962903815285, 0.4098707807351914]
			},
			{
				"uid": "sxoEL4Mpk1LdsyXhbNm059UWJ3CvxURLCQczaVI5xtBD4QcVWTDFUW7dBdye6nbn",
				"int_scalar": 7927425,
				"float_scalar": 0.31074026464844895,
				"string_scalar": "fdY2beCvs1wSws0Gb9ySD92xwfEfJpX5DQgsWoISylBAoYOcXpRaqIJoXYS4g269",
				"bool_scalar": true,
				"vectors": [0.3716157812069954, 0.006981281113265229, 0.9007003458552365, 0.22492634316191004]
			},
			{
				"uid": "g33Rqq2UQSHPRHw5FvuXxf5uGEhIAetxE6UuXXCJj0hafG8WuJr1ueZftsySCqAd",
				"int_scalar": 9288807,
				"float_scalar": 0.4953578200336135,
				"string_scalar": "6f8Iv1zQAGksj5XxMbbI5evTrYrB8fSFQ58jl0oU7Z4BpA81VsD2tlWqkhfoBNa7",
				"bool_scalar": false,
				"vectors": [0.5921374209648096, 0.04234832587925662, 0.7803878096531548, 0.1964045837884633]
			},
			{
				"uid": "ACIJd7lTXkRgUNmlQk6AbnWIKEEV8Z6OS3vDcm0w9psmt9sH3z1JLg1fNVCqiX3d",
				"int_scalar": 1173595,
				"float_scalar": 0.9000745450802002,
				"string_scalar": "gpj9YctF2ig1l1APkvRzHbVE8PZVKRbk7nvW73qS2uQbY5l7MeIeTPwRBjasbY8z",
				"bool_scalar": true,
				"vectors": [0.4655121736168688, 0.6195496905333787, 0.5316616196326639, 0.3417492053890768]
			},
			{
				"uid": "f0wRVZZ9u1bEKrAjLeZj3oliEnUjBiUl6TiermeczceBmGe6M2RHONgz3qEogrd5",
				"int_scalar": 3722368,
				"float_scalar": 0.7212299175768438,
				"string_scalar": "xydiejGUlvS49BfBuy1EuYRKt3v2oKwC6pqy7Ga4dGWn3BnQigV4XAGawixDAGHN",
				"bool_scalar": false,
				"vectors": [0.6173164237304075, 0.374107748459483, 0.3686321416317251, 0.585725336391797]
			},
			{
				"uid": "uXq9q96vUqnDebcUISFkRFT27OjD89DWhok6urXIjTuLzaSWnCVTJkrJXxFctSg0",
				"int_scalar": 1940731,
				"float_scalar": 0.9524404085944204,
				"string_scalar": "ZXSNzR5V3t62fjop7b7DHK56ByAF0INYwycKsu6OxGP4p2j0Obs6l0NUqukypGXd",
				"bool_scalar": false,
				"vectors": [0.07178869784465443, 0.4208459174227864, 0.5882811425075762, 0.6867753592116734]
			},
			{
				"uid": "EXDDklLvQIfeCJN8cES3b9mdCYDQVhq2iLj8WWA3TPtZ1SZ4Jpidj7OXJidSD7Wn",
				"int_scalar": 2158426,
				"float_scalar": 0.23770219927963454,
				"string_scalar": "9TNeKVSMqTP8Zxs90kaAcB7n6JbIcvFWInzi9JxZQgmYxD5xLYwaCoeUzRiNAxAg",
				"bool_scalar": false,
				"vectors": [0.5659468293534021, 0.6275816433340369, 0.3978846871291008, 0.3571179679645908]
			},
			{
				"uid": "mlaXOgYvB88WWRpXNyWv6UqpmvIHrC6pRo03AtaPLMpVymu0L9ioO8GWa1XgGyj0",
				"int_scalar": 198279,
				"float_scalar": 0.020343767010139513,
				"string_scalar": "AblYGRZJiMAwDbMEkungG0yKTeuya4FgyliakWWqSOJ5TvQWB9Ki2WXbnvSsYIDF",
				"bool_scalar": true,
				"vectors": [0.5374636140212398, 0.7655373567912009, 0.05491796821609715, 0.349384366747262]
			}
		]
	}`)

	var shardNum int32 = 2
	var callTime int32
	var totalCount int
	consumeFunc := func(fields map[storage.FieldID]storage.FieldData, shard int) error {
		assert.Equal(t, int(callTime), shard)
		callTime++
		rowCount := 0
		for _, data := range fields {
			if rowCount == 0 {
				rowCount = data.RowNum()
			} else {
				assert.Equal(t, rowCount, data.RowNum())
			}
		}
		totalCount += rowCount
		return nil
	}

	consumer, err := NewJSONRowConsumer(schema, idAllocator, shardNum, 1, consumeFunc)
	assert.NotNil(t, consumer)
	assert.Nil(t, err)

	validator, err := NewJSONRowValidator(schema, consumer)
	assert.NotNil(t, validator)
	assert.Nil(t, err)

	err = parser.ParseRows(reader, validator)
	assert.Nil(t, err)
	assert.Equal(t, int64(10), validator.ValidateCount())

	assert.Equal(t, shardNum, callTime)
	assert.Equal(t, 10, totalCount)
}
