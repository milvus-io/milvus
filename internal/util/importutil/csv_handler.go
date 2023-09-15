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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

type CSVRowHandler interface {
	Handle(row []map[storage.FieldID]string) error
}

// CSVRowConsumer is row-based csv format consumer class
type CSVRowConsumer struct {
	ctx            context.Context                   // for canceling parse process
	collectionInfo *CollectionInfo                   // collection details including schema
	rowIDAllocator *allocator.IDAllocator            // autoid allocator
	validators     map[storage.FieldID]*CSVValidator // validators for each field
	rowCounter     int64                             // how many rows have been consumed
	shardsData     []ShardData                       // in-memory shards data
	blockSize      int64                             // maximum size of a read block(unit:byte)
	autoIDRange    []int64                           // auto-generated id range, for example: [1, 10, 20, 25] means id from 1 to 10 and 20 to 25

	callFlushFunc ImportFlushFunc // call back function to flush segment
}

func NewCSVRowConsumer(ctx context.Context,
	collectionInfo *CollectionInfo,
	idAlloc *allocator.IDAllocator,
	blockSize int64,
	flushFunc ImportFlushFunc) (*CSVRowConsumer, error) {
	if collectionInfo == nil {
		log.Warn("CSV row consumer: collection schema is nil")
		return nil, errors.New("collection schema is nil")
	}

	v := &CSVRowConsumer{
		ctx:            ctx,
		collectionInfo: collectionInfo,
		rowIDAllocator: idAlloc,
		validators:     make(map[storage.FieldID]*CSVValidator, 0),
		rowCounter:     0,
		shardsData:     make([]ShardData, 0, collectionInfo.ShardNum),
		blockSize:      blockSize,
		autoIDRange:    make([]int64, 0),
		callFlushFunc:  flushFunc,
	}

	if err := v.initValidators(collectionInfo.Schema); err != nil {
		log.Warn("CSV row consumer: fail to initialize csv row-based consumer", zap.Error(err))
		return nil, fmt.Errorf("fail to initialize csv row-based consumer, error: %w", err)
	}

	for i := 0; i < int(collectionInfo.ShardNum); i++ {
		shardData := initShardData(collectionInfo.Schema, collectionInfo.PartitionIDs)
		if shardData == nil {
			log.Warn("CSV row consumer: fail to initialize in-memory segment data", zap.Int("shardID", i))
			return nil, fmt.Errorf("fail to initialize in-memory segment data for shard id %d", i)
		}
		v.shardsData = append(v.shardsData, shardData)
	}

	// primary key is autoid, id generator is required
	if v.collectionInfo.PrimaryKey.GetAutoID() && idAlloc == nil {
		log.Warn("CSV row consumer: ID allocator is nil")
		return nil, errors.New("ID allocator is nil")
	}

	return v, nil
}

type CSVValidator struct {
	convertFunc func(val string, field storage.FieldData) error // convert data function
	isString    bool                                            // for string field
	fieldName   string                                          // field name
}

func (v *CSVRowConsumer) initValidators(collectionSchema *schemapb.CollectionSchema) error {
	if collectionSchema == nil {
		return errors.New("collection schema is nil")
	}

	validators := v.validators

	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]

		validators[schema.GetFieldID()] = &CSVValidator{}
		validators[schema.GetFieldID()].fieldName = schema.GetName()
		validators[schema.GetFieldID()].isString = false

		switch schema.DataType {
		// all obj is string type
		case schemapb.DataType_Bool:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				var value bool
				if err := json.Unmarshal([]byte(str), &value); err != nil {
					return fmt.Errorf("illegal value '%v' for bool type field '%s'", str, schema.GetName())
				}
				field.(*storage.BoolFieldData).Data = append(field.(*storage.BoolFieldData).Data, value)
				return nil
			}
		case schemapb.DataType_Float:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				value, err := parseFloat(str, 32, schema.GetName())
				if err != nil {
					return err
				}
				field.(*storage.FloatFieldData).Data = append(field.(*storage.FloatFieldData).Data, float32(value))
				return nil
			}
		case schemapb.DataType_Double:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				value, err := parseFloat(str, 64, schema.GetName())
				if err != nil {
					return err
				}
				field.(*storage.DoubleFieldData).Data = append(field.(*storage.DoubleFieldData).Data, value)
				return nil
			}
		case schemapb.DataType_Int8:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				value, err := strconv.ParseInt(str, 0, 8)
				if err != nil {
					return fmt.Errorf("failed to parse value '%v' for int8 field '%s', error: %w", str, schema.GetName(), err)
				}
				field.(*storage.Int8FieldData).Data = append(field.(*storage.Int8FieldData).Data, int8(value))
				return nil
			}
		case schemapb.DataType_Int16:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				value, err := strconv.ParseInt(str, 0, 16)
				if err != nil {
					return fmt.Errorf("failed to parse value '%v' for int16 field '%s', error: %w", str, schema.GetName(), err)
				}
				field.(*storage.Int16FieldData).Data = append(field.(*storage.Int16FieldData).Data, int16(value))
				return nil
			}
		case schemapb.DataType_Int32:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				value, err := strconv.ParseInt(str, 0, 32)
				if err != nil {
					return fmt.Errorf("failed to parse value '%v' for int32 field '%s', error: %w", str, schema.GetName(), err)
				}
				field.(*storage.Int32FieldData).Data = append(field.(*storage.Int32FieldData).Data, int32(value))
				return nil
			}
		case schemapb.DataType_Int64:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				value, err := strconv.ParseInt(str, 0, 64)
				if err != nil {
					return fmt.Errorf("failed to parse value '%v' for int64 field '%s', error: %w", str, schema.GetName(), err)
				}
				field.(*storage.Int64FieldData).Data = append(field.(*storage.Int64FieldData).Data, value)
				return nil
			}
		case schemapb.DataType_BinaryVector:
			dim, err := getFieldDimension(schema)
			if err != nil {
				return err
			}

			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				var arr []interface{}
				desc := json.NewDecoder(strings.NewReader(str))
				desc.UseNumber()
				if err := desc.Decode(&arr); err != nil {
					return fmt.Errorf("'%v' is not an array for binary vector field '%s'", str, schema.GetName())
				}

				// we use uint8 to represent binary vector in csv file, each uint8 value represents 8 dimensions.
				if len(arr)*8 != dim {
					return fmt.Errorf("bit size %d doesn't equal to vector dimension %d of field '%s'", len(arr)*8, dim, schema.GetName())
				}

				for i := 0; i < len(arr); i++ {
					if num, ok := arr[i].(json.Number); ok {
						value, err := strconv.ParseUint(string(num), 0, 8)
						if err != nil {
							return fmt.Errorf("failed to parse value '%v' for binary vector field '%s', error: %w", num, schema.GetName(), err)
						}
						field.(*storage.BinaryVectorFieldData).Data = append(field.(*storage.BinaryVectorFieldData).Data, byte(value))
					} else {
						return fmt.Errorf("illegal value '%v' for binary vector field '%s'", str, schema.GetName())
					}
				}

				return nil
			}
		case schemapb.DataType_FloatVector:
			dim, err := getFieldDimension(schema)
			if err != nil {
				return err
			}

			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				var arr []interface{}
				desc := json.NewDecoder(strings.NewReader(str))
				desc.UseNumber()
				if err := desc.Decode(&arr); err != nil {
					return fmt.Errorf("'%v' is not an array for float vector field '%s'", str, schema.GetName())
				}

				if len(arr) != dim {
					return fmt.Errorf("array size %d doesn't equal to vector dimension %d of field '%s'", len(arr), dim, schema.GetName())
				}

				for i := 0; i < len(arr); i++ {
					if num, ok := arr[i].(json.Number); ok {
						value, err := parseFloat(string(num), 32, schema.GetName())
						if err != nil {
							return err
						}
						field.(*storage.FloatVectorFieldData).Data = append(field.(*storage.FloatVectorFieldData).Data, float32(value))
					} else {
						return fmt.Errorf("illegal value '%v' for float vector field '%s'", str, schema.GetName())
					}
				}

				return nil
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			validators[schema.GetFieldID()].isString = true

			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				field.(*storage.StringFieldData).Data = append(field.(*storage.StringFieldData).Data, str)
				return nil
			}
		case schemapb.DataType_JSON:
			validators[schema.GetFieldID()].convertFunc = func(str string, field storage.FieldData) error {
				var dummy interface{}
				if err := json.Unmarshal([]byte(str), &dummy); err != nil {
					return fmt.Errorf("failed to parse value '%v' for JSON field '%s', error: %w", str, schema.GetName(), err)
				}
				field.(*storage.JSONFieldData).Data = append(field.(*storage.JSONFieldData).Data, []byte(str))
				return nil
			}
		default:
			return fmt.Errorf("unsupport data type: %s", getTypeName(collectionSchema.Fields[i].DataType))
		}
	}
	return nil
}

func (v *CSVRowConsumer) IDRange() []int64 {
	return v.autoIDRange
}

func (v *CSVRowConsumer) RowCount() int64 {
	return v.rowCounter
}

func (v *CSVRowConsumer) Handle(rows []map[storage.FieldID]string) error {
	if v == nil || v.validators == nil || len(v.validators) == 0 {
		log.Warn("CSV row consumer is not initialized")
		return errors.New("CSV row consumer is not initialized")
	}
	// if rows is nil, that means read to end of file, force flush all data
	if rows == nil {
		err := tryFlushBlocks(v.ctx, v.shardsData, v.collectionInfo.Schema, v.callFlushFunc, v.blockSize, MaxTotalSizeInMemory, true)
		log.Info("CSV row consumer finished")
		return err
	}

	// rows is not nil, flush in necessary:
	// 1. data block size larger than v.blockSize will be flushed
	// 2. total data size exceeds MaxTotalSizeInMemory, the largest data block will be flushed
	err := tryFlushBlocks(v.ctx, v.shardsData, v.collectionInfo.Schema, v.callFlushFunc, v.blockSize, MaxTotalSizeInMemory, false)
	if err != nil {
		log.Warn("CSV row consumer: try flush data but failed", zap.Error(err))
		return fmt.Errorf("try flush data but failed, error: %w", err)
	}

	// prepare autoid, no matter int64 or varchar pk, we always generate autoid since the hidden field RowIDField requires them
	primaryKeyID := v.collectionInfo.PrimaryKey.FieldID
	primaryValidator := v.validators[primaryKeyID]
	var rowIDBegin typeutil.UniqueID
	var rowIDEnd typeutil.UniqueID
	if v.collectionInfo.PrimaryKey.AutoID {
		if v.rowIDAllocator == nil {
			log.Warn("CSV row consumer: primary keys is auto-generated but IDAllocator is nil")
			return fmt.Errorf("primary keys is auto-generated but IDAllocator is nil")
		}
		var err error
		rowIDBegin, rowIDEnd, err = v.rowIDAllocator.Alloc(uint32(len(rows)))
		if err != nil {
			log.Warn("CSV row consumer: failed to generate primary keys", zap.Int("count", len(rows)), zap.Error(err))
			return fmt.Errorf("failed to generate %d primary keys, error: %w", len(rows), err)
		}
		if rowIDEnd-rowIDBegin != int64(len(rows)) {
			log.Warn("CSV row consumer: try to generate primary keys but allocated ids are not enough",
				zap.Int("count", len(rows)), zap.Int64("generated", rowIDEnd-rowIDBegin))
			return fmt.Errorf("try to generate %d primary keys but only %d keys were allocated", len(rows), rowIDEnd-rowIDBegin)
		}
		log.Info("CSV row consumer: auto-generate primary keys", zap.Int64("begin", rowIDBegin), zap.Int64("end", rowIDEnd))
		if primaryValidator.isString {
			// if pk is varchar, no need to record auto-generated row ids
			log.Warn("CSV row consumer: string type primary key connot be auto-generated")
			return errors.New("string type primary key connot be auto-generated")
		}
		v.autoIDRange = append(v.autoIDRange, rowIDBegin, rowIDEnd)
	}

	// consume rows
	for i := 0; i < len(rows); i++ {
		row := rows[i]
		rowNumber := v.rowCounter + int64(i)

		// hash to a shard number
		var shardID uint32
		var partitionID int64
		if primaryValidator.isString {
			pk := row[primaryKeyID]

			// hash to shard based on pk, hash to partition if partition key exist
			hash := typeutil.HashString2Uint32(pk)
			shardID = hash % uint32(v.collectionInfo.ShardNum)
			partitionID, err = v.hashToPartition(row, rowNumber)
			if err != nil {
				return err
			}

			pkArray := v.shardsData[shardID][partitionID][primaryKeyID].(*storage.StringFieldData)
			pkArray.Data = append(pkArray.Data, pk)
		} else {
			var pk int64
			if v.collectionInfo.PrimaryKey.AutoID {
				pk = rowIDBegin + int64(i)
			} else {
				pkStr := row[primaryKeyID]
				pk, err = strconv.ParseInt(pkStr, 10, 64)
				if err != nil {
					log.Warn("CSV row consumer: failed to parse primary key at the row",
						zap.String("value", pkStr), zap.Int64("rowNumber", rowNumber), zap.Error(err))
					return fmt.Errorf("failed to parse primary key '%s' at the row %d, error: %w",
						pkStr, rowNumber, err)
				}
			}

			hash, err := typeutil.Hash32Int64(pk)
			if err != nil {
				log.Warn("CSV row consumer: failed to hash primary key at the row",
					zap.Int64("key", pk), zap.Int64("rowNumber", rowNumber), zap.Error(err))
				return fmt.Errorf("failed to hash primary key %d at the row %d, error: %w", pk, rowNumber, err)
			}

			// hash to shard based on pk, hash to partition if partition key exist
			shardID = hash % uint32(v.collectionInfo.ShardNum)
			partitionID, err = v.hashToPartition(row, rowNumber)
			if err != nil {
				return err
			}

			pkArray := v.shardsData[shardID][partitionID][primaryKeyID].(*storage.Int64FieldData)
			pkArray.Data = append(pkArray.Data, pk)
		}
		rowIDField := v.shardsData[shardID][partitionID][common.RowIDField].(*storage.Int64FieldData)
		rowIDField.Data = append(rowIDField.Data, rowIDBegin+int64(i))

		for fieldID, validator := range v.validators {
			if fieldID == v.collectionInfo.PrimaryKey.GetFieldID() {
				continue
			}

			value := row[fieldID]
			if err := validator.convertFunc(value, v.shardsData[shardID][partitionID][fieldID]); err != nil {
				log.Warn("CSV row consumer: failed to convert value for field at the row",
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
func (v *CSVRowConsumer) hashToPartition(row map[storage.FieldID]string, rowNumber int64) (int64, error) {
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

	var hashValue uint32
	if partitionKeyValidator.isString {
		hashValue = typeutil.HashString2Uint32(value)
	} else {
		// parse the value from a string
		pk, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Warn("CSV row consumer: failed to parse partition key at the row",
				zap.String("value", value), zap.Int64("rowNumber", rowNumber), zap.Error(err))
			return 0, fmt.Errorf("failed to parse partition key '%s' at the row %d, error: %w",
				value, rowNumber, err)
		}

		hashValue, err = typeutil.Hash32Int64(pk)
		if err != nil {
			log.Warn("CSV row consumer: failed to hash partition key at the row",
				zap.Int64("key", pk), zap.Int64("rowNumber", rowNumber), zap.Error(err))
			return 0, fmt.Errorf("failed to hash partition key %d at the row %d, error: %w", pk, rowNumber, err)
		}
	}

	index := int64(hashValue % uint32(len(v.collectionInfo.PartitionIDs)))
	return v.collectionInfo.PartitionIDs[index], nil
}
