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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type CollectionInfo struct {
	Schema   *schemapb.CollectionSchema
	ShardNum int32

	PartitionIDs []int64 // target partitions of bulkinsert, one partition for non-partition-key collection, or all partiitons for partition-key collection

	PrimaryKey   *schemapb.FieldSchema
	PartitionKey *schemapb.FieldSchema
	DynamicField *schemapb.FieldSchema

	Name2FieldID map[string]int64 // this member is for Numpy file name validation and JSON row validation
}

func DeduceTargetPartitions(partitions map[string]int64, collectionSchema *schemapb.CollectionSchema, defaultPartition int64) ([]int64, error) {
	// if no partition key, rutrn the default partition ID as target partition
	_, err := typeutil.GetPartitionKeyFieldSchema(collectionSchema)
	if err != nil {
		return []int64{defaultPartition}, nil
	}

	_, partitionIDs, err := typeutil.RearrangePartitionsForPartitionKey(partitions)
	if err != nil {
		return nil, err
	}
	return partitionIDs, nil
}

func NewCollectionInfo(collectionSchema *schemapb.CollectionSchema,
	shardNum int32,
	partitionIDs []int64) (*CollectionInfo, error) {
	if shardNum <= 0 {
		return nil, fmt.Errorf("illegal shard number %d", shardNum)
	}

	if len(partitionIDs) == 0 {
		return nil, errors.New("partition list is empty")
	}

	info := &CollectionInfo{
		ShardNum:     shardNum,
		PartitionIDs: partitionIDs,
	}

	err := info.resetSchema(collectionSchema)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (c *CollectionInfo) resetSchema(collectionSchema *schemapb.CollectionSchema) error {
	if collectionSchema == nil {
		return errors.New("collection schema is null")
	}

	fields := make([]*schemapb.FieldSchema, 0)
	name2FieldID := make(map[string]int64)
	var primaryKey *schemapb.FieldSchema
	var dynamicField *schemapb.FieldSchema
	var partitionKey *schemapb.FieldSchema
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		// RowIDField and TimeStampField is internal field, no need to parse
		if schema.GetName() == common.RowIDFieldName || schema.GetName() == common.TimeStampFieldName {
			continue
		}
		fields = append(fields, schema)
		name2FieldID[schema.GetName()] = schema.GetFieldID()

		if schema.GetIsPrimaryKey() {
			primaryKey = schema
		} else if schema.GetIsDynamic() {
			dynamicField = schema
		} else if schema.GetIsPartitionKey() {
			partitionKey = schema
		}
	}

	if primaryKey == nil {
		return errors.New("collection schema has no primary key")
	}

	if partitionKey == nil && len(c.PartitionIDs) != 1 {
		return errors.New("only allow one partition when there is no partition key")
	}

	c.Schema = &schemapb.CollectionSchema{
		Name:               collectionSchema.GetName(),
		Description:        collectionSchema.GetDescription(),
		AutoID:             collectionSchema.GetAutoID(),
		Fields:             fields,
		EnableDynamicField: collectionSchema.GetEnableDynamicField(),
	}

	c.PrimaryKey = primaryKey
	c.DynamicField = dynamicField
	c.PartitionKey = partitionKey
	c.Name2FieldID = name2FieldID

	return nil
}
