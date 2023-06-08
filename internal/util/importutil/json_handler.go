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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/allocator"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// JSONRowHandler is the interface to process rows data
type JSONRowHandler interface {
	Handle(rows []map[storage.FieldID]interface{}) error
}

// Validator is field value validator
type Validator struct {
	convertFunc func(obj interface{}, field storage.FieldData) error // convert data function
	primaryKey  bool                                                 // true for primary key
	autoID      bool                                                 // only for primary key field
	isString    bool                                                 // for string field
	dimension   int                                                  // only for vector field
	fieldName   string                                               // field name
}

func getPrimaryKey(obj interface{}, fieldName string, isString bool) (string, error) {
	// varchar type primary field, the value must be a string
	if isString {
		if value, ok := obj.(string); ok {
			return value, nil
		}
		return "", fmt.Errorf("illegal value '%v' for varchar type primary key field '%s'", obj, fieldName)
	}

	// int64 type primary field, the value must be json.Number
	if num, ok := obj.(json.Number); ok {
		return string(num), nil
	}
	return "", fmt.Errorf("illegal value '%v' for int64 type primary key field '%s'", obj, fieldName)
}

// JSONRowConsumer is row-based json format consumer class
type JSONRowConsumer struct {
	collectionSchema *schemapb.CollectionSchema              // collection schema
	rowIDAllocator   *allocator.IDAllocator                  // autoid allocator
	validators       map[storage.FieldID]*Validator          // validators for each field
	rowCounter       int64                                   // how many rows have been consumed
	shardNum         int32                                   // sharding number of the collection
	segmentsData     []map[storage.FieldID]storage.FieldData // in-memory segments data
	blockSize        int64                                   // maximum size of a read block(unit:byte)
	primaryKey       storage.FieldID                         // name of primary key
	autoIDRange      []int64                                 // auto-generated id range, for example: [1, 10, 20, 25] means id from 1 to 10 and 20 to 25

	callFlushFunc ImportFlushFunc // call back function to flush segment
}

func NewJSONRowConsumer(collectionSchema *schemapb.CollectionSchema, idAlloc *allocator.IDAllocator, shardNum int32, blockSize int64,
	flushFunc ImportFlushFunc) (*JSONRowConsumer, error) {
	if collectionSchema == nil {
		log.Error("JSON row consumer: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	v := &JSONRowConsumer{
		collectionSchema: collectionSchema,
		rowIDAllocator:   idAlloc,
		validators:       make(map[storage.FieldID]*Validator),
		shardNum:         shardNum,
		blockSize:        blockSize,
		rowCounter:       0,
		primaryKey:       -1,
		autoIDRange:      make([]int64, 0),
		callFlushFunc:    flushFunc,
	}

	err := initValidators(collectionSchema, v.validators)
	if err != nil {
		log.Error("JSON row consumer: fail to initialize json row-based consumer", zap.Error(err))
		return nil, fmt.Errorf("fail to initialize json row-based consumer, error: %w", err)
	}

	v.segmentsData = make([]map[storage.FieldID]storage.FieldData, 0, shardNum)
	for i := 0; i < int(shardNum); i++ {
		segmentData := initSegmentData(collectionSchema)
		if segmentData == nil {
			log.Error("JSON row consumer: fail to initialize in-memory segment data", zap.Int("shardID", i))
			return nil, fmt.Errorf("fail to initialize in-memory segment data for shard id %d", i)
		}
		v.segmentsData = append(v.segmentsData, segmentData)
	}

	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		if schema.GetIsPrimaryKey() {
			v.primaryKey = schema.GetFieldID()
			break
		}
	}
	// primary key not found
	if v.primaryKey == -1 {
		log.Error("JSON row consumer: collection schema has no primary key")
		return nil, errors.New("collection schema has no primary key")
	}
	// primary key is autoid, id generator is required
	if v.validators[v.primaryKey].autoID && idAlloc == nil {
		log.Error("JSON row consumer: ID allocator is nil")
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

func (v *JSONRowConsumer) flush(force bool) error {
	// force flush all data
	if force {
		for i := 0; i < len(v.segmentsData); i++ {
			segmentData := v.segmentsData[i]
			rowNum := segmentData[v.primaryKey].RowNum()
			if rowNum > 0 {
				log.Info("JSON row consumer: force flush binlog", zap.Int("rows", rowNum))
				err := v.callFlushFunc(segmentData, i)
				if err != nil {
					return err
				}

				v.segmentsData[i] = initSegmentData(v.collectionSchema)
				if v.segmentsData[i] == nil {
					log.Error("JSON row consumer: fail to initialize in-memory segment data")
					return errors.New("fail to initialize in-memory segment data")
				}
			}
		}

		return nil
	}

	// segment size can be flushed
	for i := 0; i < len(v.segmentsData); i++ {
		segmentData := v.segmentsData[i]
		rowNum := segmentData[v.primaryKey].RowNum()
		memSize := 0
		for _, field := range segmentData {
			memSize += field.GetMemorySize()
		}
		if memSize >= int(v.blockSize) && rowNum > 0 {
			log.Info("JSON row consumer: flush fulled binlog", zap.Int("bytes", memSize), zap.Int("rowNum", rowNum))
			err := v.callFlushFunc(segmentData, i)
			if err != nil {
				return err
			}

			v.segmentsData[i] = initSegmentData(v.collectionSchema)
			if v.segmentsData[i] == nil {
				log.Error("JSON row consumer: fail to initialize in-memory segment data")
				return errors.New("fail to initialize in-memory segment data")
			}
		}
	}

	return nil
}

func (v *JSONRowConsumer) Handle(rows []map[storage.FieldID]interface{}) error {
	if v == nil || v.validators == nil || len(v.validators) == 0 {
		log.Error("JSON row consumer is not initialized")
		return errors.New("JSON row consumer is not initialized")
	}

	// flush in necessery
	if rows == nil {
		err := v.flush(true)
		log.Info("JSON row consumer finished")
		return err
	}

	err := v.flush(false)
	if err != nil {
		log.Error("JSON row consumer: try flush data but failed", zap.Error(err))
		return fmt.Errorf("try flush data but failed, error: %w", err)
	}

	// prepare autoid, no matter int64 or varchar pk, we always generate autoid since the hidden field RowIDField requires them
	primaryValidator := v.validators[v.primaryKey]
	var rowIDBegin typeutil.UniqueID
	var rowIDEnd typeutil.UniqueID
	if primaryValidator.autoID {
		if v.rowIDAllocator == nil {
			log.Error("JSON row consumer: primary keys is auto-generated but IDAllocator is nil")
			return fmt.Errorf("primary keys is auto-generated but IDAllocator is nil")
		}
		var err error
		rowIDBegin, rowIDEnd, err = v.rowIDAllocator.Alloc(uint32(len(rows)))
		if err != nil {
			log.Error("JSON row consumer: failed to generate primary keys", zap.Int("count", len(rows)), zap.Error(err))
			return fmt.Errorf("failed to generate %d primary keys, error: %w", len(rows), err)
		}
		if rowIDEnd-rowIDBegin != int64(len(rows)) {
			log.Error("JSON row consumer: try to generate primary keys but allocated ids are not enough",
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
		if primaryValidator.isString {
			if primaryValidator.autoID {
				log.Error("JSON row consumer: string type primary key cannot be auto-generated")
				return errors.New("string type primary key cannot be auto-generated")
			}

			value := row[v.primaryKey]
			pk, err := getPrimaryKey(value, primaryValidator.fieldName, primaryValidator.isString)
			if err != nil {
				log.Error("JSON row consumer: failed to parse primary key at the row",
					zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to parse primary key at the row %d, error: %w", rowNumber, err)
			}

			hash := typeutil.HashString2Uint32(pk)
			shard = hash % uint32(v.shardNum)
			pkArray := v.segmentsData[shard][v.primaryKey].(*storage.StringFieldData)
			pkArray.Data = append(pkArray.Data, pk)
		} else {
			// get/generate the row id
			var pk int64
			if primaryValidator.autoID {
				pk = rowIDBegin + int64(i)
			} else {
				value := row[v.primaryKey]
				strValue, err := getPrimaryKey(value, primaryValidator.fieldName, primaryValidator.isString)
				if err != nil {
					log.Error("JSON row consumer: failed to parse primary key at the row",
						zap.Int64("rowNumber", rowNumber), zap.Error(err))
					return fmt.Errorf("failed to parse primary key at the row %d, error: %w", rowNumber, err)
				}

				// parse the pk from a string
				pk, err = strconv.ParseInt(strValue, 10, 64)
				if err != nil {
					log.Error("JSON row consumer: failed to parse primary key at the row",
						zap.String("value", strValue), zap.Int64("rowNumber", rowNumber), zap.Error(err))
					return fmt.Errorf("failed to parse primary key '%s' at the row %d, error: %w",
						strValue, rowNumber, err)
				}
			}

			hash, err := typeutil.Hash32Int64(pk)
			if err != nil {
				log.Error("JSON row consumer: failed to hash primary key at the row",
					zap.Int64("key", pk), zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to hash primary key %d at the row %d, error: %w", pk, rowNumber, err)
			}

			shard = hash % uint32(v.shardNum)
			pkArray := v.segmentsData[shard][v.primaryKey].(*storage.Int64FieldData)
			pkArray.Data = append(pkArray.Data, pk)
		}

		// set rowid field
		rowIDField := v.segmentsData[shard][common.RowIDField].(*storage.Int64FieldData)
		rowIDField.Data = append(rowIDField.Data, rowIDBegin+int64(i))

		// convert value and consume
		for name, validator := range v.validators {
			if validator.primaryKey {
				continue
			}
			value := row[name]
			if err := validator.convertFunc(value, v.segmentsData[shard][name]); err != nil {
				log.Error("JSON row consumer: failed to convert value for field at the row",
					zap.String("fieldName", validator.fieldName), zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to convert value for field '%s' at the row %d,  error: %w",
					validator.fieldName, rowNumber, err)
			}
		}
	}

	v.rowCounter += int64(len(rows))

	return nil
}
