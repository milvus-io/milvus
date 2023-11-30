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
	"github.com/apache/arrow/go/v12/arrow/array"
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

	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 1}, memory.DefaultAllocator)
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

func (p *ParquetParser) createReaders() error {
	schema, err := p.fileReader.Schema()
	if err != nil {
		log.Warn("can't schema from file", zap.Error(err))
		return err
	}
	for _, field := range p.collectionInfo.Schema.GetFields() {
		dim, _ := getFieldDimension(field)
		parquetColumnReader := &ParquetColumnReader{
			fieldName:   field.GetName(),
			fieldID:     field.GetFieldID(),
			dataType:    field.GetDataType(),
			elementType: field.GetElementType(),
			dimension:   dim,
		}
		fields, exist := schema.FieldsByName(field.GetName())
		if !exist {
			if !(field.GetIsPrimaryKey() && field.GetAutoID()) && !field.GetIsDynamic() {
				log.Warn("there is no field in parquet file", zap.String("fieldName", field.GetName()))
				return merr.WrapErrImportFailed(fmt.Sprintf("there is no field: %s in parquet file", field.GetName()))
			}
		} else {
			if len(fields) != 1 {
				log.Warn("there is multi field of fieldName", zap.String("fieldName", field.GetName()), zap.Any("file fields", fields))
				return merr.WrapErrImportFailed(fmt.Sprintf("there is multi field of fieldName: %s", field.GetName()))
			}
			if !verifyFieldSchema(field.GetDataType(), field.GetElementType(), fields[0]) {
				log.Warn("field schema is not match",
					zap.String("collection schema", field.GetDataType().String()),
					zap.String("file schema", fields[0].Type.Name()))
				return merr.WrapErrImportFailed(fmt.Sprintf("field schema is not match, collection field dataType: %s, file field dataType:%s", field.GetDataType().String(), fields[0].Type.Name()))
			}
			indices := schema.FieldIndices(field.GetName())
			if len(indices) != 1 {
				log.Warn("field is not match", zap.String("fieldName", field.GetName()), zap.Ints("indices", indices))
				return merr.WrapErrImportFailed(fmt.Sprintf("there is %d indices of fieldName: %s", len(indices), field.GetName()))
			}
			parquetColumnReader.columnIndex = indices[0]
			columnReader, err := p.fileReader.GetColumn(p.ctx, parquetColumnReader.columnIndex)
			if err != nil {
				log.Warn("get column reader failed", zap.String("fieldName", field.GetName()), zap.Error(err))
				return err
			}
			parquetColumnReader.columnReader = columnReader
			p.columnMap[field.GetName()] = parquetColumnReader
		}
	}
	return nil
}

func verifyFieldSchema(dataType, elementType schemapb.DataType, fileField arrow.Field) bool {
	switch fileField.Type.ID() {
	case arrow.BOOL:
		return dataType == schemapb.DataType_Bool
	case arrow.INT8:
		return dataType == schemapb.DataType_Int8
	case arrow.INT16:
		return dataType == schemapb.DataType_Int16
	case arrow.INT32:
		return dataType == schemapb.DataType_Int32
	case arrow.INT64:
		return dataType == schemapb.DataType_Int64
	case arrow.FLOAT32:
		return dataType == schemapb.DataType_Float
	case arrow.FLOAT64:
		return dataType == schemapb.DataType_Double
	case arrow.STRING:
		return dataType == schemapb.DataType_VarChar || dataType == schemapb.DataType_String || dataType == schemapb.DataType_JSON
	case arrow.LIST:
		if dataType != schemapb.DataType_Array && dataType != schemapb.DataType_FloatVector &&
			dataType != schemapb.DataType_Float16Vector && dataType != schemapb.DataType_BinaryVector {
			return false
		}
		if dataType == schemapb.DataType_Array {
			return verifyFieldSchema(elementType, schemapb.DataType_None, fileField.Type.(*arrow.ListType).ElemField())
		}
		return true
	}
	return false
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
		err = tryFlushBlocks(p.ctx, shards, p.collectionInfo.Schema, p.callFlushFunc, p.blockSize, MaxTotalSizeInMemory, false)
		if err != nil {
			return err
		}
		tr.Record("tryFlushBlocks")
	}

	// force flush at the end
	return tryFlushBlocks(p.ctx, shards, p.collectionInfo.Schema, p.callFlushFunc, p.blockSize, MaxTotalSizeInMemory, true)
}

// readData method reads Parquet data section into a storage.FieldData
func (p *ParquetParser) readData(columnReader *ParquetColumnReader, rowCount int64) (storage.FieldData, error) {
	switch columnReader.dataType {
	case schemapb.DataType_Bool:
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]bool, error) {
			boolReader, ok := chunk.(*array.Boolean)
			boolData := make([]bool, 0)
			if !ok {
				log.Warn("the column data in parquet is not bool", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not bool of field: %s", columnReader.fieldName))
			}
			for i := 0; i < boolReader.Data().Len(); i++ {
				boolData = append(boolData, boolReader.Value(i))
			}
			return boolData, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to read bool array", zap.Error(err))
			return nil, err
		}

		return &storage.BoolFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int8:
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]int8, error) {
			int8Reader, ok := chunk.(*array.Int8)
			int8Data := make([]int8, 0)
			if !ok {
				log.Warn("the column data in parquet is not int8", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not int8 of field: %s", columnReader.fieldName))
			}
			for i := 0; i < int8Reader.Data().Len(); i++ {
				int8Data = append(int8Data, int8Reader.Value(i))
			}
			return int8Data, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to read int8 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int8FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int16:
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]int16, error) {
			int16Reader, ok := chunk.(*array.Int16)
			int16Data := make([]int16, 0)
			if !ok {
				log.Warn("the column data in parquet is not int16", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not int16 of field: %s", columnReader.fieldName))
			}
			for i := 0; i < int16Reader.Data().Len(); i++ {
				int16Data = append(int16Data, int16Reader.Value(i))
			}
			return int16Data, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to int16 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int16FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int32:
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]int32, error) {
			int32Reader, ok := chunk.(*array.Int32)
			int32Data := make([]int32, 0)
			if !ok {
				log.Warn("the column data in parquet is not int32", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not int32 of field: %s", columnReader.fieldName))
			}
			for i := 0; i < int32Reader.Data().Len(); i++ {
				int32Data = append(int32Data, int32Reader.Value(i))
			}
			return int32Data, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to read int32 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int32FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int64:
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]int64, error) {
			int64Reader, ok := chunk.(*array.Int64)
			int64Data := make([]int64, 0)
			if !ok {
				log.Warn("the column data in parquet is not int64", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not int64 of field: %s", columnReader.fieldName))
			}
			for i := 0; i < int64Reader.Data().Len(); i++ {
				int64Data = append(int64Data, int64Reader.Value(i))
			}
			return int64Data, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to read int64 array", zap.Error(err))
			return nil, err
		}

		return &storage.Int64FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Float:
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]float32, error) {
			float32Reader, ok := chunk.(*array.Float32)
			float32Data := make([]float32, 0)
			if !ok {
				log.Warn("the column data in parquet is not float", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not float of field: %s", columnReader.fieldName))
			}
			for i := 0; i < float32Reader.Data().Len(); i++ {
				float32Data = append(float32Data, float32Reader.Value(i))
			}
			return float32Data, nil
		})
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
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]float64, error) {
			float64Reader, ok := chunk.(*array.Float64)
			float64Data := make([]float64, 0)
			if !ok {
				log.Warn("the column data in parquet is not double", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not double of field: %s", columnReader.fieldName))
			}
			for i := 0; i < float64Reader.Data().Len(); i++ {
				float64Data = append(float64Data, float64Reader.Value(i))
			}
			return float64Data, nil
		})
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
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]string, error) {
			stringReader, ok := chunk.(*array.String)
			stringData := make([]string, 0)
			if !ok {
				log.Warn("the column data in parquet is not string", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not string of field: %s", columnReader.fieldName))
			}
			for i := 0; i < stringReader.Data().Len(); i++ {
				stringData = append(stringData, stringReader.Value(i))
			}
			return stringData, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to read varchar array", zap.Error(err))
			return nil, err
		}

		return &storage.StringFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_JSON:
		// JSON field read data from string array Parquet
		data, err := ReadData(columnReader, rowCount, func(chunk arrow.Array) ([]string, error) {
			stringReader, ok := chunk.(*array.String)
			stringData := make([]string, 0)
			if !ok {
				log.Warn("the column data in parquet is not json string", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not json string of field: %s", columnReader.fieldName))
			}
			for i := 0; i < stringReader.Data().Len(); i++ {
				stringData = append(stringData, stringReader.Value(i))
			}
			return stringData, nil
		})
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
		data, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]uint8, error) {
			arrayData := make([][]uint8, 0)
			uint8Reader, ok := reader.(*array.Uint8)
			if !ok {
				log.Warn("the column element data of array in parquet is not binary", zap.String("fieldName", columnReader.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not binary: %s", columnReader.fieldName))
			}
			for i := 1; i < len(offsets); i++ {
				start, end := offsets[i-1], offsets[i]
				elementData := make([]uint8, 0)
				for j := start; j < end; j++ {
					elementData = append(elementData, uint8Reader.Value(int(j)))
				}
				arrayData = append(arrayData, elementData)
			}
			return arrayData, nil
		})
		if err != nil {
			log.Warn("Parquet parser: failed to read binary vector array", zap.Error(err))
			return nil, err
		}
		binaryData := make([]byte, 0)
		for _, arr := range data {
			binaryData = append(binaryData, arr...)
		}

		if len(binaryData) != len(data)*columnReader.dimension/8 {
			log.Warn("Parquet parser: binary vector is irregular", zap.Int("actual num", len(binaryData)),
				zap.Int("expect num", len(data)*columnReader.dimension/8))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("binary vector is irregular, expect num = %d,"+
				" actual num = %d", len(data)*columnReader.dimension/8, len(binaryData)))
		}

		return &storage.BinaryVectorFieldData{
			Data: binaryData,
			Dim:  columnReader.dimension,
		}, nil
	case schemapb.DataType_FloatVector:
		data := make([]float32, 0)
		rowNum := 0
		if columnReader.columnReader.Field().Type.(*arrow.ListType).Elem().ID() == arrow.FLOAT32 {
			arrayData, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]float32, error) {
				arrayData := make([][]float32, 0)
				float32Reader, ok := reader.(*array.Float32)
				if !ok {
					log.Warn("the column element data of array in parquet is not float", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not float: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]float32, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, float32Reader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
			if err != nil {
				log.Warn("Parquet parser: failed to read float vector array", zap.Error(err))
				return nil, err
			}
			for _, arr := range arrayData {
				data = append(data, arr...)
			}
			err = typeutil.VerifyFloats32(data)
			if err != nil {
				log.Warn("Parquet parser: illegal value in float vector array", zap.Error(err))
				return nil, err
			}
			rowNum = len(arrayData)
		} else if columnReader.columnReader.Field().Type.(*arrow.ListType).Elem().ID() == arrow.FLOAT64 {
			arrayData, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]float64, error) {
				arrayData := make([][]float64, 0)
				float64Reader, ok := reader.(*array.Float64)
				if !ok {
					log.Warn("the column element data of array in parquet is not double", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not double: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]float64, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, float64Reader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
			if err != nil {
				log.Warn("Parquet parser: failed to read float vector array", zap.Error(err))
				return nil, err
			}
			for _, arr := range arrayData {
				for _, f64 := range arr {
					err = typeutil.VerifyFloat(f64)
					if err != nil {
						log.Warn("Parquet parser: illegal value in float vector array", zap.Error(err))
						return nil, err
					}
					data = append(data, float32(f64))
				}
			}
			rowNum = len(arrayData)
		} else {
			log.Warn("Parquet parser: FloatVector type is not float", zap.String("fieldName", columnReader.fieldName))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("FloatVector type is not float, is: %s",
				columnReader.columnReader.Field().Type.(*arrow.ListType).Elem().ID().String()))
		}

		if len(data) != rowNum*columnReader.dimension {
			log.Warn("Parquet parser: float vector is irregular", zap.Int("actual num", len(data)),
				zap.Int("expect num", rowNum*columnReader.dimension))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("float vector is irregular, expect num = %d,"+
				" actual num = %d", rowNum*columnReader.dimension, len(data)))
		}

		return &storage.FloatVectorFieldData{
			Data: data,
			Dim:  columnReader.dimension,
		}, nil

	case schemapb.DataType_Array:
		data := make([]*schemapb.ScalarField, 0)
		switch columnReader.elementType {
		case schemapb.DataType_Bool:
			boolArray, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]bool, error) {
				arrayData := make([][]bool, 0)
				boolReader, ok := reader.(*array.Boolean)
				if !ok {
					log.Warn("the column element data of array in parquet is not bool", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not bool: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]bool, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, boolReader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			int8Array, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]int32, error) {
				arrayData := make([][]int32, 0)
				int8Reader, ok := reader.(*array.Int8)
				if !ok {
					log.Warn("the column element data of array in parquet is not int8", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not int8: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]int32, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, int32(int8Reader.Value(int(j))))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			int16Array, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]int32, error) {
				arrayData := make([][]int32, 0)
				int16Reader, ok := reader.(*array.Int16)
				if !ok {
					log.Warn("the column element data of array in parquet is not int16", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not int16: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]int32, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, int32(int16Reader.Value(int(j))))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			int32Array, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]int32, error) {
				arrayData := make([][]int32, 0)
				int32Reader, ok := reader.(*array.Int32)
				if !ok {
					log.Warn("the column element data of array in parquet is not int32", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not int32: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]int32, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, int32Reader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			int64Array, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]int64, error) {
				arrayData := make([][]int64, 0)
				int64Reader, ok := reader.(*array.Int64)
				if !ok {
					log.Warn("the column element data of array in parquet is not int64", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not int64: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]int64, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, int64Reader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			float32Array, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]float32, error) {
				arrayData := make([][]float32, 0)
				float32Reader, ok := reader.(*array.Float32)
				if !ok {
					log.Warn("the column element data of array in parquet is not float", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not float: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]float32, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, float32Reader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			float64Array, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]float64, error) {
				arrayData := make([][]float64, 0)
				float64Reader, ok := reader.(*array.Float64)
				if !ok {
					log.Warn("the column element data of array in parquet is not double", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not double: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]float64, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, float64Reader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			stringArray, err := ReadArrayData(columnReader, rowCount, func(offsets []int32, reader arrow.Array) ([][]string, error) {
				arrayData := make([][]string, 0)
				stringReader, ok := reader.(*array.String)
				if !ok {
					log.Warn("the column element data of array in parquet is not string", zap.String("fieldName", columnReader.fieldName))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not string: %s", columnReader.fieldName))
				}
				for i := 1; i < len(offsets); i++ {
					start, end := offsets[i-1], offsets[i]
					elementData := make([]string, 0)
					for j := start; j < end; j++ {
						elementData = append(elementData, stringReader.Value(int(j)))
					}
					arrayData = append(arrayData, elementData)
				}
				return arrayData, nil
			})
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
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type: %s of array", columnReader.elementType.String()))
		}
		return &storage.ArrayFieldData{
			ElementType: columnReader.elementType,
			Data:        data,
		}, nil
	default:
		log.Warn("Parquet parser: unsupported data type of field",
			zap.String("dataType", columnReader.dataType.String()),
			zap.String("fieldName", columnReader.fieldName))
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type: %s", columnReader.elementType.String()))
	}
}
