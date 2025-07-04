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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestUtil_EstimateReadCountPerBatch(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}
	count, err := EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), count)

	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "vec2",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "invalidDim",
			},
		},
	})
	_, err = EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.Error(t, err)
}

func TestUtil_EstimateReadCountPerBatch_InvalidBufferSize(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	count, err := EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.Error(t, err)
	assert.Equal(t, int64(0), count)
	t.Logf("err=%v", err)

	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	count, err = EstimateReadCountPerBatch(0, schema)
	assert.Error(t, err)
	assert.Equal(t, int64(0), count)
	t.Logf("err=%v", err)
}

func TestUtil_EstimateReadCountPerBatch_LargeSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	for i := 0; i < 100; i++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  int64(i),
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "10000000",
				},
			},
		})
	}
	count, err := EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestUtil_CheckVarcharLength(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "5",
			},
		},
	}
	err := CheckVarcharLength("aaaaaaaa", 5, fieldSchema)
	assert.Error(t, err)

	err = CheckVarcharLength("aaaaa", 5, fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_CheckArrayCapacity(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_Array,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxCapacityKey,
				Value: "5",
			},
		},
	}
	err := CheckArrayCapacity(6, 5, fieldSchema)
	assert.Error(t, err)

	err = CheckArrayCapacity(5, 5, fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_CheckValidUTF8(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "1000",
			},
		},
	}
	err := CheckValidUTF8(string([]byte{0xC0, 0xAF}), fieldSchema)
	assert.Error(t, err)

	err = CheckValidUTF8("abc", fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_CheckValidString(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "5",
			},
		},
	}
	err := CheckValidString("aaaaaaaa", 5, fieldSchema)
	assert.Error(t, err)

	err = CheckValidString(string([]byte{0xC0, 0xAF}), 5, fieldSchema)
	assert.Error(t, err)

	err = CheckValidString("aaaaa", 5, fieldSchema)
	assert.NoError(t, err)
}
