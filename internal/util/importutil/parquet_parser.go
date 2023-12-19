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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ParquetParser is analogous to the ParquetColumnReader, but for Parquet files
type ParquetParser struct {
	ctx                context.Context        // for canceling parse process
	collectionInfo     *CollectionInfo        // collection details including schema
	rowIDAllocator     *allocator.IDAllocator // autoid allocator
	blockSize          int64                  // maximum size of a read block(unit:byte)
	chunkManager       storage.ChunkManager   // storage interfaces to browse/read the files
	autoIDRange        []int64                // auto-generated id range, for example: [1, 10, 20, 25] means id from 1 to 10 and 20 to 25
	callFlushFunc      ImportFlushFunc        // call back function to flush segment
	updateProgressFunc func(percent int64)    // update working progress percent value
	columnMap          map[string]*ParquetColumnReader
	reader             *file.Reader
	fileReader         *pqarrow.FileReader
}

// NewParquetParser is helper function to create a ParquetParser
func NewParquetParser(ctx context.Context,
	collectionInfo *CollectionInfo,
	idAlloc *allocator.IDAllocator,
	blockSize int64,
	chunkManager storage.ChunkManager,
	filePath string,
	flushFunc ImportFlushFunc,
	updateProgressFunc func(percent int64),
) (*ParquetParser, error) {
	if collectionInfo == nil {
		log.Warn("Parquet parser: collection schema is nil")
		return nil, merr.WrapErrImportFailed("collection schema is nil")
	}

	if idAlloc == nil {
		log.Warn("Parquet parser: id allocator is nil")
		return nil, merr.WrapErrImportFailed("id allocator is nil")
	}

	if chunkManager == nil {
		log.Warn("Parquet parser: chunk manager pointer is nil")
		return nil, merr.WrapErrImportFailed("chunk manager pointer is nil")
	}

	if flushFunc == nil {
		log.Warn("Parquet parser: flush function is nil")
		return nil, merr.WrapErrImportFailed("flush function is nil")
	}

	cmReader, err := chunkManager.Reader(ctx, filePath)
	if err != nil {
		log.Warn("create chunk manager reader failed")
		return nil, err
	}

	reader, err := file.NewParquetReader(cmReader)
	if err != nil {
		log.Warn("create parquet reader failed", zap.Error(err))
		return nil, err
	}

	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		log.Warn("create arrow parquet file reader failed", zap.Error(err))
		return nil, err
	}

	parser := &ParquetParser{
		ctx:                ctx,
		collectionInfo:     collectionInfo,
		rowIDAllocator:     idAlloc,
		blockSize:          blockSize,
		chunkManager:       chunkManager,
		autoIDRange:        make([]int64, 0),
		callFlushFunc:      flushFunc,
		updateProgressFunc: updateProgressFunc,
		columnMap:          make(map[string]*ParquetColumnReader),
		fileReader:         fileReader,
		reader:             reader,
	}

	return parser, nil
}

func (p *ParquetParser) IDRange() []int64 {
	return p.autoIDRange
}

// Parse is the function entry
func (p *ParquetParser) Parse() error {
	err := p.createReaders()
	defer p.Close()
	if err != nil {
		return err
	}

	// read all data from the Parquet files
	err = p.consume()
	if err != nil {
		return err
	}

	return nil
}

func (p *ParquetParser) checkFields() error {
	for _, field := range p.collectionInfo.Schema.GetFields() {
		if (field.GetIsPrimaryKey() && field.GetAutoID()) || field.GetIsDynamic() {
			continue
		}
		if _, ok := p.columnMap[field.GetName()]; !ok {
			log.Warn("there is no field in parquet file", zap.String("fieldName", field.GetName()))
			return merr.WrapErrImportFailed(fmt.Sprintf("there is no field in parquet file of name: %s", field.GetName()))
		}
	}
	return nil
}

func (p *ParquetParser) createReaders() error {
	schema, err := p.fileReader.Schema()
	if err != nil {
		log.Warn("can't schema from file", zap.Error(err))
		return err
	}
	// The collection schema must be checked, so no errors will occur here.
	schemaHelper, _ := typeutil.CreateSchemaHelper(p.collectionInfo.Schema)
	parquetFields := schema.Fields()
	for i, field := range parquetFields {
		fieldSchema, err := schemaHelper.GetFieldFromName(field.Name)
		if err != nil {
			// TODO @cai.zhang: handle dynamic field
			log.Warn("the field is not in schema, if it's a dynamic field, please reformat data by bulk_writer", zap.String("fieldName", field.Name))
			return merr.WrapErrImportFailed(fmt.Sprintf("the field: %s is not in schema, if it's a dynamic field, please reformat data by bulk_writer", field.Name))
		}
		if _, ok := p.columnMap[field.Name]; ok {
			log.Warn("there is multi field of fieldName", zap.String("fieldName", field.Name),
				zap.Ints("file fields indices", schema.FieldIndices(field.Name)))
			return merr.WrapErrImportFailed(fmt.Sprintf("there is multi field of fieldName: %s", field.Name))
		}
		if fieldSchema.GetIsPrimaryKey() && fieldSchema.GetAutoID() {
			log.Warn("the field is primary key, and autoID is true, please remove it from file", zap.String("fieldName", field.Name))
			return merr.WrapErrImportFailed(fmt.Sprintf("the field: %s is primary key, and autoID is true, please remove it from file", field.Name))
		}
		arrowType, isList := convertArrowSchemaToDataType(field, false)
		dataType := fieldSchema.GetDataType()
		if isList {
			if !typeutil.IsVectorType(dataType) && dataType != schemapb.DataType_Array {
				log.Warn("field schema is not match",
					zap.String("collection schema", dataType.String()),
					zap.String("file schema", field.Type.Name()))
				return merr.WrapErrImportFailed(fmt.Sprintf("field schema is not match, collection field dataType: %s, file field dataType:%s", dataType.String(), field.Type.Name()))
			}
			if dataType == schemapb.DataType_Array {
				dataType = fieldSchema.GetElementType()
			}
		}
		if !isConvertible(arrowType, dataType, isList) {
			log.Warn("field schema is not match",
				zap.String("collection schema", dataType.String()),
				zap.String("file schema", field.Type.Name()))
			return merr.WrapErrImportFailed(fmt.Sprintf("field schema is not match, collection field dataType: %s, file field dataType:%s", dataType.String(), field.Type.Name()))
		}
		// Here, the scalar column does not have a dim field,
		// and the dim type of the vector column must have been checked, so there is no error catch here.
		dim, _ := getFieldDimension(fieldSchema)
		parquetColumnReader := &ParquetColumnReader{
			fieldName:   fieldSchema.GetName(),
			fieldID:     fieldSchema.GetFieldID(),
			dataType:    fieldSchema.GetDataType(),
			elementType: fieldSchema.GetElementType(),
			dimension:   dim,
		}
		parquetColumnReader.columnIndex = i
		columnReader, err := p.fileReader.GetColumn(p.ctx, parquetColumnReader.columnIndex)
		if err != nil {
			log.Warn("get column reader failed", zap.String("fieldName", field.Name), zap.Error(err))
			return err
		}
		parquetColumnReader.columnReader = columnReader
		p.columnMap[field.Name] = parquetColumnReader
	}
	if err = p.checkFields(); err != nil {
		return err
	}
	return nil
}

func convertArrowSchemaToDataType(field arrow.Field, isList bool) (schemapb.DataType, bool) {
	switch field.Type.ID() {
	case arrow.BOOL:
		return schemapb.DataType_Bool, false
	case arrow.UINT8:
		if isList {
			return schemapb.DataType_BinaryVector, false
		}
		return schemapb.DataType_None, false
	case arrow.INT8:
		return schemapb.DataType_Int8, false
	case arrow.INT16:
		return schemapb.DataType_Int16, false
	case arrow.INT32:
		return schemapb.DataType_Int32, false
	case arrow.INT64:
		return schemapb.DataType_Int64, false
	case arrow.FLOAT16:
		if isList {
			return schemapb.DataType_Float16Vector, false
		}
		return schemapb.DataType_None, false
	case arrow.FLOAT32:
		return schemapb.DataType_Float, false
	case arrow.FLOAT64:
		return schemapb.DataType_Double, false
	case arrow.STRING:
		return schemapb.DataType_VarChar, false
	case arrow.BINARY:
		return schemapb.DataType_BinaryVector, false
	case arrow.LIST:
		elementType, _ := convertArrowSchemaToDataType(field.Type.(*arrow.ListType).ElemField(), true)
		return elementType, true
	default:
		return schemapb.DataType_None, false
	}
}

func isConvertible(src, dst schemapb.DataType, isList bool) bool {
	switch src {
	case schemapb.DataType_Bool:
		return typeutil.IsBoolType(dst)
	case schemapb.DataType_Int8:
		return typeutil.IsArithmetic(dst)
	case schemapb.DataType_Int16:
		return typeutil.IsArithmetic(dst) && dst != schemapb.DataType_Int8
	case schemapb.DataType_Int32:
		return typeutil.IsArithmetic(dst) && dst != schemapb.DataType_Int8 && dst != schemapb.DataType_Int16
	case schemapb.DataType_Int64:
		return typeutil.IsFloatingType(dst) || dst == schemapb.DataType_Int64
	case schemapb.DataType_Float:
		if isList && dst == schemapb.DataType_FloatVector {
			return true
		}
		return typeutil.IsFloatingType(dst)
	case schemapb.DataType_Double:
		if isList && dst == schemapb.DataType_FloatVector {
			return true
		}
		return dst == schemapb.DataType_Double
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return typeutil.IsStringType(dst) || typeutil.IsJSONType(dst)
	case schemapb.DataType_JSON:
		return typeutil.IsJSONType(dst)
	case schemapb.DataType_BinaryVector:
		return dst == schemapb.DataType_BinaryVector
	case schemapb.DataType_Float16Vector:
		return dst == schemapb.DataType_Float16Vector
	default:
		return false
	}
}

// Close closes the parquet file reader
func (p *ParquetParser) Close() {
	p.reader.Close()
}

// calcRowCountPerBlock calculates a proper value for a batch row count to read file
func (p *ParquetParser) calcRowCountPerBlock() (int64, error) {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(p.collectionInfo.Schema)
	if err != nil {
		log.Warn("Parquet parser: failed to estimate size of each row", zap.Error(err))
		return 0, merr.WrapErrImportFailed(fmt.Sprintf("failed to estimate size of each row: %s", err.Error()))
	}

	if sizePerRecord <= 0 {
		log.Warn("Parquet parser: failed to estimate size of each row, the collection schema might be empty")
		return 0, merr.WrapErrImportFailed("failed to estimate size of each row: the collection schema might be empty")
	}

	// the sizePerRecord is estimate value, if the schema contains varchar field, the value is not accurate
	// we will read data block by block, by default, each block size is 16MB
	// rowCountPerBlock is the estimated row count for a block
	rowCountPerBlock := p.blockSize / int64(sizePerRecord)
	if rowCountPerBlock <= 0 {
		rowCountPerBlock = 1 // make sure the value is positive
	}

	log.Info("Parquet parser: calculate row count per block to read file", zap.Int64("rowCountPerBlock", rowCountPerBlock),
		zap.Int64("blockSize", p.blockSize), zap.Int("sizePerRecord", sizePerRecord))
	return rowCountPerBlock, nil
}

// consume method reads Parquet data section into a storage.FieldData
// please note it will require a large memory block(the memory size is almost equal to Parquet file size)
func (p *ParquetParser) consume() error {
	rowCountPerBlock, err := p.calcRowCountPerBlock()
	if err != nil {
		return err
	}

	updateProgress := func(readRowCount int64) {
		if p.updateProgressFunc != nil && p.reader != nil && p.reader.NumRows() > 0 {
			percent := (readRowCount * ProgressValueForPersist) / p.reader.NumRows()
			log.Info("Parquet parser: working progress", zap.Int64("readRowCount", readRowCount),
				zap.Int64("totalRowCount", p.reader.NumRows()), zap.Int64("percent", percent))
			p.updateProgressFunc(percent)
		}
	}

	// prepare shards
	shards := make([]ShardData, 0, p.collectionInfo.ShardNum)
	for i := 0; i < int(p.collectionInfo.ShardNum); i++ {
		shardData := initShardData(p.collectionInfo.Schema, p.collectionInfo.PartitionIDs)
		if shardData == nil {
			log.Warn("Parquet parser: failed to initialize FieldData list")
			return merr.WrapErrImportFailed("failed to initialize FieldData list")
		}
		shards = append(shards, shardData)
	}
	tr := timerecord.NewTimeRecorder("consume performance")
	defer tr.Elapse("end")
	// read data from files, batch by batch
	totalRead := 0
	for {
		readRowCount := 0
		segmentData := make(BlockData)
		for _, reader := range p.columnMap {
			fieldData, err := p.readData(reader, rowCountPerBlock)
			if err != nil {
				return err
			}
			if readRowCount == 0 {
				readRowCount = fieldData.RowNum()
			} else if readRowCount != fieldData.RowNum() {
				log.Warn("Parquet parser: data block's row count mismatch", zap.Int("firstBlockRowCount", readRowCount),
					zap.Int("thisBlockRowCount", fieldData.RowNum()), zap.Int64("rowCountPerBlock", rowCountPerBlock),
					zap.String("current field", reader.fieldName))
				return merr.WrapErrImportFailed(fmt.Sprintf("data block's row count mismatch: %d vs %d", readRowCount, fieldData.RowNum()))
			}

			segmentData[reader.fieldID] = fieldData
		}

		// nothing to read
		if readRowCount == 0 {
			break
		}
		totalRead += readRowCount
		updateProgress(int64(totalRead))
		tr.Record("readData")
		// split data to shards
		p.autoIDRange, err = splitFieldsData(p.collectionInfo, segmentData, shards, p.rowIDAllocator)
		if err != nil {
			return err
		}
		tr.Record("splitFieldsData")
		// when the estimated size is close to blockSize, save to binlog
		err = tryFlushBlocks(p.ctx, shards, p.collectionInfo.Schema, p.callFlushFunc, p.blockSize, Params.DataNodeCfg.BulkInsertMaxMemorySize.GetAsInt64(), false)
		if err != nil {
			return err
		}
		tr.Record("tryFlushBlocks")
	}

	// force flush at the end
	return tryFlushBlocks(p.ctx, shards, p.collectionInfo.Schema, p.callFlushFunc, p.blockSize, Params.DataNodeCfg.BulkInsertMaxMemorySize.GetAsInt64(), true)
}

// readData method reads Parquet data section into a storage.FieldData
func (p *ParquetParser) readData(columnReader *ParquetColumnReader, rowCount int64) (storage.FieldData, error) {
	switch columnReader.dataType {
	case schemapb.DataType_Bool:
		data, err := ReadBoolData(columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read bool array", zap.Error(err))
			return nil, err
		}

		return &storage.BoolFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int8:
		data, err := ReadIntegerOrFloatData[int8](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read int8 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int8FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int16:
		data, err := ReadIntegerOrFloatData[int16](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to int16 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int16FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int32:
		data, err := ReadIntegerOrFloatData[int32](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read int32 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int32FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int64:
		data, err := ReadIntegerOrFloatData[int64](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read int64 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int64FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Float:
		data, err := ReadIntegerOrFloatData[float32](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read float array", zap.Error(err))
			return nil, err
		}

		err = typeutil.VerifyFloats32(data)
		if err != nil {
			log.Warn("Parquet parser: illegal value in float array", zap.Error(err))
			return nil, err
		}

		return &storage.FloatFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Double:
		data, err := ReadIntegerOrFloatData[float64](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read double array", zap.Error(err))
			return nil, err
		}

		err = typeutil.VerifyFloats64(data)
		if err != nil {
			log.Warn("Parquet parser: illegal value in double array", zap.Error(err))
			return nil, err
		}

		return &storage.DoubleFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		data, err := ReadStringData(columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read varchar array", zap.Error(err))
			return nil, err
		}

		return &storage.StringFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_JSON:
		// JSON field read data from string array Parquet
		data, err := ReadStringData(columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read json string array", zap.Error(err))
			return nil, err
		}

		byteArr := make([][]byte, 0)
		for _, str := range data {
			var dummy interface{}
			err := json.Unmarshal([]byte(str), &dummy)
			if err != nil {
				log.Warn("Parquet parser: illegal string value for JSON field",
					zap.String("value", str), zap.String("fieldName", columnReader.fieldName), zap.Error(err))
				return nil, err
			}
			byteArr = append(byteArr, []byte(str))
		}

		return &storage.JSONFieldData{
			Data: byteArr,
		}, nil
	case schemapb.DataType_BinaryVector:
		binaryData, err := ReadBinaryData(columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read binary vector array", zap.Error(err))
			return nil, err
		}

		return &storage.BinaryVectorFieldData{
			Data: binaryData,
			Dim:  columnReader.dimension,
		}, nil
	case schemapb.DataType_FloatVector:
		arrayData, err := ReadIntegerOrFloatArrayData[float32](columnReader, rowCount)
		if err != nil {
			log.Warn("Parquet parser: failed to read float vector array", zap.Error(err))
			return nil, err
		}
		data := make([]float32, 0, len(arrayData)*columnReader.dimension)
		for _, arr := range arrayData {
			data = append(data, arr...)
		}

		return &storage.FloatVectorFieldData{
			Data: data,
			Dim:  columnReader.dimension,
		}, nil
	case schemapb.DataType_Array:
		data := make([]*schemapb.ScalarField, 0)
		switch columnReader.elementType {
		case schemapb.DataType_Bool:
			boolArray, err := ReadBoolArrayData(columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range boolArray {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: elementArray,
						},
					},
				})
			}
		case schemapb.DataType_Int8:
			int8Array, err := ReadIntegerOrFloatArrayData[int32](columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range int8Array {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: elementArray,
						},
					},
				})
			}
		case schemapb.DataType_Int16:
			int16Array, err := ReadIntegerOrFloatArrayData[int32](columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range int16Array {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Int32:
			int32Array, err := ReadIntegerOrFloatArrayData[int32](columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range int32Array {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Int64:
			int64Array, err := ReadIntegerOrFloatArrayData[int64](columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range int64Array {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Float:
			float32Array, err := ReadIntegerOrFloatArrayData[float32](columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range float32Array {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_Double:
			float64Array, err := ReadIntegerOrFloatArrayData[float64](columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range float64Array {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: elementArray,
						},
					},
				})
			}

		case schemapb.DataType_VarChar, schemapb.DataType_String:
			stringArray, err := ReadStringArrayData(columnReader, rowCount)
			if err != nil {
				return nil, err
			}
			for _, elementArray := range stringArray {
				data = append(data, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: elementArray,
						},
					},
				})
			}
		default:
			log.Warn("unsupported element type", zap.String("element type", columnReader.elementType.String()),
				zap.String("fieldName", columnReader.fieldName))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type: %s of array field: %s", columnReader.elementType.String(), columnReader.fieldName))
		}
		return &storage.ArrayFieldData{
			ElementType: columnReader.elementType,
			Data:        data,
		}, nil
	default:
		log.Warn("Parquet parser: unsupported data type of field",
			zap.String("dataType", columnReader.dataType.String()),
			zap.String("fieldName", columnReader.fieldName))
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type: %s of field: %s", columnReader.elementType.String(), columnReader.fieldName))
	}
}
