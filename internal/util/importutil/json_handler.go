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
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// JSONRowHandler is the interface to process rows data
type JSONRowHandler interface {
	Handle(rows []map[storage.FieldID]interface{}) error
}

func getKeyValue(obj interface{}, fieldName string, isString bool) (string, error) {
	// varchar type primary field, the value must be a string
	if isString {
		if value, ok := obj.(string); ok {
			return value, nil
		}
		return "", fmt.Errorf("illegal value '%v' for varchar type key field '%s'", obj, fieldName)
	}

	// int64 type primary field, the value must be json.Number
	if num, ok := obj.(json.Number); ok {
		return string(num), nil
	}
	return "", fmt.Errorf("illegal value '%v' for int64 type key field '%s'", obj, fieldName)
}

// JSONRowConsumer is row-based json format consumer class
type JSONRowConsumer struct {
	ctx            context.Context                // for canceling parse process
	collectionInfo *CollectionInfo                // collection details including schema
	rowIDAllocator *allocator.IDAllocator         // autoid allocator
	validators     map[storage.FieldID]*Validator // validators for each field
	rowCounter     int64                          // how many rows have been consumed
	shardsData     []ShardData                    // in-memory shards data
	blockSize      int64                          // maximum size of a read block(unit:byte)
	autoIDRange    []int64                        // auto-generated id range, for example: [1, 10, 20, 25] means id from 1 to 10 and 20 to 25

	callFlushFunc ImportFlushFunc // call back function to flush segment
}

func NewJSONRowConsumer(ctx context.Context,
	collectionInfo *CollectionInfo,
	idAlloc *allocator.IDAllocator,
	blockSize int64,
	flushFunc ImportFlushFunc) (*JSONRowConsumer, error) {
	if collectionInfo == nil {
		log.Warn("JSON row consumer: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	v := &JSONRowConsumer{
		ctx:            ctx,
		collectionInfo: collectionInfo,
		rowIDAllocator: idAlloc,
		validators:     make(map[storage.FieldID]*Validator),
		blockSize:      blockSize,
		rowCounter:     0,
		autoIDRange:    make([]int64, 0),
		callFlushFunc:  flushFunc,
	}

	err := initValidators(collectionInfo.Schema, v.validators)
	if err != nil {
		log.Warn("JSON row consumer: fail to initialize json row-based consumer", zap.Error(err))
		return nil, fmt.Errorf("fail to initialize json row-based consumer, error: %w", err)
	}

	v.shardsData = make([]ShardData, 0, collectionInfo.ShardNum)
	for i := 0; i < int(collectionInfo.ShardNum); i++ {
		shardData := initShardData(collectionInfo.Schema, collectionInfo.PartitionIDs)
		if shardData == nil {
			log.Warn("JSON row consumer: fail to initialize in-memory segment data", zap.Int("shardID", i))
			return nil, fmt.Errorf("fail to initialize in-memory segment data for shard id %d", i)
		}
		v.shardsData = append(v.shardsData, shardData)
	}

	// primary key is autoid, id generator is required
	if v.collectionInfo.PrimaryKey.GetAutoID() && idAlloc == nil {
		log.Warn("JSON row consumer: ID allocator is nil")
		return nil, errors.New("ID allocator is nil")
	}

	return v, nil
}

func (v *JSONRowConsumer) IDRange() []int64 {
	return v.autoIDRange
}

func (v *JSONRowConsumer) RowCount() int64 {
	return v.rowCounter
}

func (v *JSONRowConsumer) Handle(rows []map[storage.FieldID]interface{}) error {
	if v == nil || v.validators == nil || len(v.validators) == 0 {
		log.Warn("JSON row consumer is not initialized")
		return errors.New("JSON row consumer is not initialized")
	}

	// if rows is nil, that means read to end of file, force flush all data
	if rows == nil {
		err := tryFlushBlocks(v.ctx, v.shardsData, v.collectionInfo.Schema, v.callFlushFunc, v.blockSize, MaxTotalSizeInMemory, true)
		log.Info("JSON row consumer finished")
		return err
	}

	// rows is not nil, flush in necessary:
	// 1. data block size larger than v.blockSize will be flushed
	// 2. total data size exceeds MaxTotalSizeInMemory, the largest data block will be flushed
	err := tryFlushBlocks(v.ctx, v.shardsData, v.collectionInfo.Schema, v.callFlushFunc, v.blockSize, MaxTotalSizeInMemory, false)
	if err != nil {
		log.Warn("JSON row consumer: try flush data but failed", zap.Error(err))
		return fmt.Errorf("try flush data but failed, error: %w", err)
	}

	// prepare autoid, no matter int64 or varchar pk, we always generate autoid since the hidden field RowIDField requires them
	primaryKeyID := v.collectionInfo.PrimaryKey.FieldID
	primaryValidator := v.validators[primaryKeyID]
	var rowIDBegin typeutil.UniqueID
	var rowIDEnd typeutil.UniqueID
	if primaryValidator.autoID {
		if v.rowIDAllocator == nil {
			log.Warn("JSON row consumer: primary keys is auto-generated but IDAllocator is nil")
			return fmt.Errorf("primary keys is auto-generated but IDAllocator is nil")
		}
		var err error
		rowIDBegin, rowIDEnd, err = v.rowIDAllocator.Alloc(uint32(len(rows)))
		if err != nil {
			log.Warn("JSON row consumer: failed to generate primary keys", zap.Int("count", len(rows)), zap.Error(err))
			return fmt.Errorf("failed to generate %d primary keys, error: %w", len(rows), err)
		}
		if rowIDEnd-rowIDBegin != int64(len(rows)) {
			log.Warn("JSON row consumer: try to generate primary keys but allocated ids are not enough",
				zap.Int("count", len(rows)), zap.Int64("generated", rowIDEnd-rowIDBegin))
			return fmt.Errorf("try to generate %d primary keys but only %d keys were allocated", len(rows), rowIDEnd-rowIDBegin)
		}
		log.Info("JSON row consumer: auto-generate primary keys", zap.Int64("begin", rowIDBegin), zap.Int64("end", rowIDEnd))
		if !primaryValidator.isString {
			// if pk is varchar, no need to record auto-generated row ids
			v.autoIDRange = append(v.autoIDRange, rowIDBegin, rowIDEnd)
		}
	}

	// consume rows
	for i := 0; i < len(rows); i++ {
		row := rows[i]
		rowNumber := v.rowCounter + int64(i)

		// hash to a shard number
		var shard uint32
		var partitionID int64
		if primaryValidator.isString {
			if primaryValidator.autoID {
				log.Warn("JSON row consumer: string type primary key cannot be auto-generated")
				return errors.New("string type primary key cannot be auto-generated")
			}

			value := row[primaryKeyID]
			pk, err := getKeyValue(value, primaryValidator.fieldName, primaryValidator.isString)
			if err != nil {
				log.Warn("JSON row consumer: failed to parse primary key at the row",
					zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to parse primary key at the row %d, error: %w", rowNumber, err)
			}

			// hash to shard based on pk, hash to partition if partition key exist
			hash := typeutil.HashString2Uint32(pk)
			shard = hash % uint32(v.collectionInfo.ShardNum)
			partitionID, err = v.hashToPartition(row, rowNumber)
			if err != nil {
				return err
			}

			pkArray := v.shardsData[shard][partitionID][primaryKeyID].(*storage.StringFieldData)
			pkArray.Data = append(pkArray.Data, pk)
		} else {
			// get/generate the row id
			var pk int64
			if primaryValidator.autoID {
				pk = rowIDBegin + int64(i)
			} else {
				value := row[primaryKeyID]
				strValue, err := getKeyValue(value, primaryValidator.fieldName, primaryValidator.isString)
				if err != nil {
					log.Warn("JSON row consumer: failed to parse primary key at the row",
						zap.Int64("rowNumber", rowNumber), zap.Error(err))
					return fmt.Errorf("failed to parse primary key at the row %d, error: %w", rowNumber, err)
				}

				// parse the pk from a string
				pk, err = strconv.ParseInt(strValue, 10, 64)
				if err != nil {
					log.Warn("JSON row consumer: failed to parse primary key at the row",
						zap.String("value", strValue), zap.Int64("rowNumber", rowNumber), zap.Error(err))
					return fmt.Errorf("failed to parse primary key '%s' at the row %d, error: %w",
						strValue, rowNumber, err)
				}
			}

			hash, err := typeutil.Hash32Int64(pk)
			if err != nil {
				log.Warn("JSON row consumer: failed to hash primary key at the row",
					zap.Int64("key", pk), zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to hash primary key %d at the row %d, error: %w", pk, rowNumber, err)
			}

			// hash to shard based on pk, hash to partition if partition key exist
			shard = hash % uint32(v.collectionInfo.ShardNum)
			partitionID, err = v.hashToPartition(row, rowNumber)
			if err != nil {
				return err
			}

			pkArray := v.shardsData[shard][partitionID][primaryKeyID].(*storage.Int64FieldData)
			pkArray.Data = append(pkArray.Data, pk)
		}

		// set rowid field
		rowIDField := v.shardsData[shard][partitionID][common.RowIDField].(*storage.Int64FieldData)
		rowIDField.Data = append(rowIDField.Data, rowIDBegin+int64(i))

		// convert value and consume
		for fieldID, validator := range v.validators {
			if validator.primaryKey {
				continue
			}
			value := row[fieldID]
			if err := validator.convertFunc(value, v.shardsData[shard][partitionID][fieldID]); err != nil {
				log.Warn("JSON row consumer: failed to convert value for field at the row",
					zap.String("fieldName", validator.fieldName), zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to convert value for field '%s' at the row %d,  error: %w",
					validator.fieldName, rowNumber, err)
			}
		}
	}

	v.rowCounter += int64(len(rows))

	return nil
}

// hashToPartition hash partition key to get an partition ID, return the first partition ID if no partition key exist
// CollectionInfo ensures only one partition ID in the PartitionIDs if no partition key exist
func (v *JSONRowConsumer) hashToPartition(row map[storage.FieldID]interface{}, rowNumber int64) (int64, error) {
	if v.collectionInfo.PartitionKey == nil {
		if len(v.collectionInfo.PartitionIDs) != 1 {
			return 0, fmt.Errorf("collection '%s' partition list is empty", v.collectionInfo.Schema.Name)
		}
		// no partition key, directly return the target partition id
		return v.collectionInfo.PartitionIDs[0], nil
	}

	partitionKeyID := v.collectionInfo.PartitionKey.GetFieldID()
	partitionKeyValidator := v.validators[partitionKeyID]
	value := row[partitionKeyID]
	strValue, err := getKeyValue(value, partitionKeyValidator.fieldName, partitionKeyValidator.isString)
	if err != nil {
		log.Warn("JSON row consumer: failed to parse partition key at the row",
			zap.Int64("rowNumber", rowNumber), zap.Error(err))
		return 0, fmt.Errorf("failed to parse partition key at the row %d, error: %w", rowNumber, err)
	}

	var hashValue uint32
	if partitionKeyValidator.isString {
		hashValue = typeutil.HashString2Uint32(strValue)
	} else {
		// parse the value from a string
		pk, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			log.Warn("JSON row consumer: failed to parse partition key at the row",
				zap.String("value", strValue), zap.Int64("rowNumber", rowNumber), zap.Error(err))
			return 0, fmt.Errorf("failed to parse partition key '%s' at the row %d, error: %w",
				strValue, rowNumber, err)
		}

		hashValue, err = typeutil.Hash32Int64(pk)
		if err != nil {
			log.Warn("JSON row consumer: failed to hash partition key at the row",
				zap.Int64("key", pk), zap.Int64("rowNumber", rowNumber), zap.Error(err))
			return 0, fmt.Errorf("failed to hash partition key %d at the row %d, error: %w", pk, rowNumber, err)
		}
	}

	index := int64(hashValue % uint32(len(v.collectionInfo.PartitionIDs)))
	return v.collectionInfo.PartitionIDs[index], nil
}
