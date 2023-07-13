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
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func Test_DeduceTargetPartitions(t *testing.T) {
	schema := sampleSchema()
	partitions := map[string]int64{
		"part_0": 100,
		"part_1": 200,
	}
	partitionIDs, err := DeduceTargetPartitions(partitions, schema, int64(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitionIDs))
	assert.Equal(t, int64(1), partitionIDs[0])

	schema.Fields[7].IsPartitionKey = true
	partitionIDs, err = DeduceTargetPartitions(partitions, schema, int64(1))
	assert.NoError(t, err)
	assert.Equal(t, len(partitions), len(partitionIDs))

	partitions = map[string]int64{
		"part_a": 100,
	}
	partitionIDs, err = DeduceTargetPartitions(partitions, schema, int64(1))
	assert.Error(t, err)
	assert.Nil(t, partitionIDs)
}

func Test_CollectionInfoNew(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		info, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
		assert.NoError(t, err)
		assert.NotNil(t, info)
		assert.Greater(t, len(info.Name2FieldID), 0)
		assert.Nil(t, info.PartitionKey)
		assert.Nil(t, info.DynamicField)
		assert.NotNil(t, info.PrimaryKey)
		assert.Equal(t, int32(2), info.ShardNum)
		assert.Equal(t, 1, len(info.PartitionIDs))

		// has partition key, has dynamic field
		schema := &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  0,
					Name:     "RowID",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:      100,
					Name:         "ID",
					IsPrimaryKey: true,
					AutoID:       false,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:        101,
					Name:           "PartitionKey",
					IsPartitionKey: true,
					DataType:       schemapb.DataType_VarChar,
				},
				{
					FieldID:   102,
					Name:      "$meta",
					IsDynamic: true,
					DataType:  schemapb.DataType_JSON,
				},
			},
		}
		info, err = NewCollectionInfo(schema, 2, []int64{1, 2})
		assert.NoError(t, err)
		assert.NotNil(t, info)
		assert.NotNil(t, info.PrimaryKey)
		assert.NotNil(t, int64(100), info.PrimaryKey.GetFieldID())
		assert.False(t, info.PrimaryKey.GetAutoID())
		assert.NotNil(t, info.DynamicField)
		assert.Equal(t, int64(102), info.DynamicField.GetFieldID())
		assert.NotNil(t, info.PartitionKey)
		assert.Equal(t, int64(101), info.PartitionKey.GetFieldID())
	})

	t.Run("error cases", func(t *testing.T) {
		schema := sampleSchema()
		// shard number is 0
		info, err := NewCollectionInfo(schema, 0, []int64{1})
		assert.Error(t, err)
		assert.Nil(t, info)

		// partiton ID list is empty
		info, err = NewCollectionInfo(schema, 2, []int64{})
		assert.Error(t, err)
		assert.Nil(t, info)

		// only allow one partition when there is no partition key
		info, err = NewCollectionInfo(schema, 2, []int64{1, 2})
		assert.Error(t, err)
		assert.Nil(t, info)

		// collection schema is nil
		info, err = NewCollectionInfo(nil, 2, []int64{1})
		assert.Error(t, err)
		assert.Nil(t, info)

		// no primary key
		schema = &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields:      make([]*schemapb.FieldSchema, 0),
		}
		info, err = NewCollectionInfo(schema, 2, []int64{1})
		assert.Error(t, err)
		assert.Nil(t, info)

		// partition key is nil
		info, err = NewCollectionInfo(schema, 2, []int64{1, 2})
		assert.Error(t, err)
		assert.Nil(t, info)
	})
}
