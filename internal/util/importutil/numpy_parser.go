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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type NumpyColumnReader struct {
	fieldName string             // name of the target column
	fieldID   storage.FieldID    // ID of the target column
	dataType  schemapb.DataType  // data type of the target column
	rowCount  int                // how many rows need to be read
	dimension int                // only for vector
	file      storage.FileReader // file to be read
	reader    *NumpyAdapter      // data reader
}

func closeReaders(columnReaders []*NumpyColumnReader) {
	for _, reader := range columnReaders {
		if reader.file != nil {
			err := reader.file.Close()
			if err != nil {
				log.Warn("Numper parser: failed to close numpy file", zap.String("fileName", reader.fieldName+NumpyFileExt))
			}
		}
	}
}

type NumpyParser struct {
	ctx                context.Context        // for canceling parse process
	collectionInfo     *CollectionInfo        // collection details including schema
	rowIDAllocator     *allocator.IDAllocator // autoid allocator
	blockSize          int64                  // maximum size of a read block(unit:byte)
	chunkManager       storage.ChunkManager   // storage interfaces to browse/read the files
	autoIDRange        []int64                // auto-generated id range, for example: [1, 10, 20, 25] means id from 1 to 10 and 20 to 25
	callFlushFunc      ImportFlushFunc        // call back function to flush segment
	updateProgressFunc func(percent int64)    // update working progress percent value
}

// NewNumpyParser is helper function to create a NumpyParser
func NewNumpyParser(ctx context.Context,
	collectionInfo *CollectionInfo,
	idAlloc *allocator.IDAllocator,
	blockSize int64,
	chunkManager storage.ChunkManager,
	flushFunc ImportFlushFunc,
	updateProgressFunc func(percent int64),
) (*NumpyParser, error) {
	if collectionInfo == nil {
		log.Warn("Numper parser: collection schema is nil")
		return nil, merr.WrapErrImportFailed("collection schema is nil")
	}

	if idAlloc == nil {
		log.Warn("Numper parser: id allocator is nil")
		return nil, merr.WrapErrImportFailed("id allocator is nil")
	}

	if chunkManager == nil {
		log.Warn("Numper parser: chunk manager pointer is nil")
		return nil, merr.WrapErrImportFailed("chunk manager pointer is nil")
	}

	if flushFunc == nil {
		log.Warn("Numper parser: flush function is nil")
		return nil, merr.WrapErrImportFailed("flush function is nil")
	}

	parser := &NumpyParser{
		ctx:                ctx,
		collectionInfo:     collectionInfo,
		rowIDAllocator:     idAlloc,
		blockSize:          blockSize,
		chunkManager:       chunkManager,
		autoIDRange:        make([]int64, 0),
		callFlushFunc:      flushFunc,
		updateProgressFunc: updateProgressFunc,
	}

	return parser, nil
}

func (p *NumpyParser) IDRange() []int64 {
	return p.autoIDRange
}

// Parse is the function entry
func (p *NumpyParser) Parse(filePaths []string) error {
	// check redundant files for column-based import
	// if the field is primary key and autoID is false, the file is required
	// any redundant file is not allowed
	err := p.validateFileNames(filePaths)
	if err != nil {
		return err
	}

	// open files and verify file header
	readers, err := p.createReaders(filePaths)
	// make sure all the files are closed finally, must call this method before the function return
	defer closeReaders(readers)
	if err != nil {
		return err
	}

	// read all data from the numpy files
	err = p.consume(readers)
	if err != nil {
		return err
	}

	return nil
}

// validateFileNames is to check redundant file and missed file
func (p *NumpyParser) validateFileNames(filePaths []string) error {
	dynamicFieldName := ""
	requiredFieldNames := make(map[string]interface{})
	for _, schema := range p.collectionInfo.Schema.Fields {
		if schema.GetIsDynamic() && p.collectionInfo.Schema.GetEnableDynamicField() {
			dynamicFieldName = schema.GetName()
		}
		if schema.GetIsPrimaryKey() {
			if !schema.GetAutoID() {
				requiredFieldNames[schema.GetName()] = nil
			}
		} else {
			requiredFieldNames[schema.GetName()] = nil
		}
	}

	// check redundant file
	fileNames := make(map[string]interface{})
	for _, filePath := range filePaths {
		name, _ := GetFileNameAndExt(filePath)
		fileNames[name] = nil
		_, ok := requiredFieldNames[name]
		if !ok {
			log.Warn("Numpy parser: the file has no corresponding field in collection", zap.String("fieldName", name))
			return merr.WrapErrImportFailed(fmt.Sprintf("the file '%s' has no corresponding field in collection", filePath))
		}
	}

	// check missed file
	for name := range requiredFieldNames {
		if name == dynamicFieldName {
			// dynamic schema field file is not required
			continue
		}
		_, ok := fileNames[name]
		if !ok {
			log.Warn("Numpy parser: there is no file corresponding to field", zap.String("fieldName", name))
			return merr.WrapErrImportFailed(fmt.Sprintf("there is no file corresponding to field '%s'", name))
		}
	}

	return nil
}

// createReaders open the files and verify file header
func (p *NumpyParser) createReaders(filePaths []string) ([]*NumpyColumnReader, error) {
	readers := make([]*NumpyColumnReader, 0)

	for _, filePath := range filePaths {
		fileName, _ := GetFileNameAndExt(filePath)

		// check existence of the target field
		var schema *schemapb.FieldSchema
		for i := 0; i < len(p.collectionInfo.Schema.Fields); i++ {
			tmpSchema := p.collectionInfo.Schema.Fields[i]
			if tmpSchema.GetName() == fileName {
				schema = tmpSchema
				break
			}
		}

		if schema == nil {
			log.Warn("Numpy parser: the field is not found in collection schema", zap.String("fileName", fileName))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field name '%s' is not found in collection schema", fileName))
		}

		file, err := p.chunkManager.Reader(p.ctx, filePath)
		if err != nil {
			log.Warn("Numpy parser: failed to read the file", zap.String("filePath", filePath), zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read the file '%s', error: %s", filePath, err.Error()))
		}

		adapter, err := NewNumpyAdapter(file)
		if err != nil {
			log.Warn("Numpy parser: failed to read the file header", zap.String("filePath", filePath), zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read the file header '%s', error: %s", filePath, err.Error()))
		}

		if file == nil || adapter == nil {
			log.Warn("Numpy parser: failed to open file", zap.String("filePath", filePath))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to open file '%s'", filePath))
		}

		dim, _ := getFieldDimension(schema)
		columnReader := &NumpyColumnReader{
			fieldName: schema.GetName(),
			fieldID:   schema.GetFieldID(),
			dataType:  schema.GetDataType(),
			dimension: dim,
			file:      file,
			reader:    adapter,
		}

		// the validation method only check the file header information
		err = p.validateHeader(columnReader)
		if err != nil {
			return nil, err
		}
		readers = append(readers, columnReader)
	}

	// row count of each file should be equal
	if len(readers) > 0 {
		firstReader := readers[0]
		rowCount := firstReader.rowCount
		for i := 1; i < len(readers); i++ {
			compareReader := readers[i]
			if rowCount != compareReader.rowCount {
				log.Warn("Numpy parser: the row count of files are not equal",
					zap.String("firstFile", firstReader.fieldName), zap.Int("firstRowCount", firstReader.rowCount),
					zap.String("compareFile", compareReader.fieldName), zap.Int("compareRowCount", compareReader.rowCount))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the row count(%d) of file '%s.npy' is not equal to row count(%d) of file '%s.npy'",
					firstReader.rowCount, firstReader.fieldName, compareReader.rowCount, compareReader.fieldName))
			}
		}
	}

	return readers, nil
}

// validateHeader is to verify numpy file header, file header information should match field's schema
func (p *NumpyParser) validateHeader(columnReader *NumpyColumnReader) error {
	if columnReader == nil || columnReader.reader == nil {
		log.Warn("Numpy parser: numpy reader is nil")
		return merr.WrapErrImportFailed("numpy adapter is nil")
	}

	elementType := columnReader.reader.GetType()
	shape := columnReader.reader.GetShape()
	// if user only save an element in a numpy file, the shape list will be empty
	if len(shape) == 0 {
		log.Warn("Numpy parser: the content stored in numpy file is not valid numpy array",
			zap.String("fieldName", columnReader.fieldName))
		return merr.WrapErrImportFailed(fmt.Sprintf("the content stored in numpy file is not valid numpy array for field '%s'", columnReader.fieldName))
	}
	columnReader.rowCount = shape[0]

	// 1. field data type should be consist to numpy data type
	// 2. vector field dimension should be consist to numpy shape
	if schemapb.DataType_FloatVector == columnReader.dataType {
		// float32/float64 numpy file can be used for float vector file, 2 reasons:
		// 1. for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// 2. for float64 numpy file, the performance is worse than float32 numpy file
		if elementType != schemapb.DataType_Float && elementType != schemapb.DataType_Double {
			log.Warn("Numpy parser: illegal data type of numpy file for float vector field", zap.Any("dataType", elementType),
				zap.String("fieldName", columnReader.fieldName))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal data type %s of numpy file for float vector field '%s'", getTypeName(elementType),
				columnReader.fieldName))
		}

		// vector field, the shape should be 2
		if len(shape) != 2 {
			log.Warn("Numpy parser: illegal shape of numpy file for float vector field, shape should be 2", zap.Int("shape", len(shape)),
				zap.String("fieldName", columnReader.fieldName))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal shape %d of numpy file for float vector field '%s', shape should be 2", shape,
				columnReader.fieldName))
		}

		if shape[1] != columnReader.dimension {
			log.Warn("Numpy parser: illegal dimension of numpy file for float vector field", zap.String("fieldName", columnReader.fieldName),
				zap.Int("numpyDimension", shape[1]), zap.Int("fieldDimension", columnReader.dimension))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal dimension %d of numpy file for float vector field '%s', dimension should be %d",
				shape[1], columnReader.fieldName, columnReader.dimension))
		}
	} else if schemapb.DataType_BinaryVector == columnReader.dataType {
		if elementType != schemapb.DataType_BinaryVector {
			log.Warn("Numpy parser: illegal data type of numpy file for binary vector field", zap.Any("dataType", elementType),
				zap.String("fieldName", columnReader.fieldName))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal data type %s of numpy file for binary vector field '%s'", getTypeName(elementType),
				columnReader.fieldName))
		}

		// vector field, the shape should be 2
		if len(shape) != 2 {
			log.Warn("Numpy parser: illegal shape of numpy file for binary vector field, shape should be 2", zap.Int("shape", len(shape)),
				zap.String("fieldName", columnReader.fieldName))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal shape %d of numpy file for binary vector field '%s', shape should be 2", shape,
				columnReader.fieldName))
		}

		if shape[1] != columnReader.dimension/8 {
			log.Warn("Numpy parser: illegal dimension of numpy file for float vector field", zap.String("fieldName", columnReader.fieldName),
				zap.Int("numpyDimension", shape[1]*8), zap.Int("fieldDimension", columnReader.dimension))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal dimension %d of numpy file for binary vector field '%s', dimension should be %d",
				shape[1]*8, columnReader.fieldName, columnReader.dimension))
		}
	} else {
		// JSON field and VARCHAR field are using string type numpy
		// legal input if columnReader.dataType is JSON and elementType is VARCHAR
		if elementType != schemapb.DataType_VarChar && columnReader.dataType != schemapb.DataType_JSON {
			if elementType != columnReader.dataType {
				log.Warn("Numpy parser: illegal data type of numpy file for scalar field", zap.Any("numpyDataType", elementType),
					zap.String("fieldName", columnReader.fieldName), zap.Any("fieldDataType", columnReader.dataType))
				return merr.WrapErrImportFailed(fmt.Sprintf("illegal data type %s of numpy file for scalar field '%s' with type %s",
					getTypeName(elementType), columnReader.fieldName, getTypeName(columnReader.dataType)))
			}
		}

		// scalar field, the shape should be 1
		if len(shape) != 1 {
			log.Warn("Numpy parser: illegal shape of numpy file for scalar field, shape should be 1", zap.Int("shape", len(shape)),
				zap.String("fieldName", columnReader.fieldName))
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal shape %d of numpy file for scalar field '%s', shape should be 1", shape, columnReader.fieldName))
		}
	}

	return nil
}

// calcRowCountPerBlock calculates a proper value for a batch row count to read file
func (p *NumpyParser) calcRowCountPerBlock() (int64, error) {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(p.collectionInfo.Schema)
	if err != nil {
		log.Warn("Numpy parser: failed to estimate size of each row", zap.Error(err))
		return 0, merr.WrapErrImportFailed(fmt.Sprintf("failed to estimate size of each row: %s", err.Error()))
	}

	if sizePerRecord <= 0 {
		log.Warn("Numpy parser: failed to estimate size of each row, the collection schema might be empty")
		return 0, merr.WrapErrImportFailed("failed to estimate size of each row: the collection schema might be empty")
	}

	// the sizePerRecord is estimate value, if the schema contains varchar field, the value is not accurate
	// we will read data block by block, by default, each block size is 16MB
	// rowCountPerBlock is the estimated row count for a block
	rowCountPerBlock := p.blockSize / int64(sizePerRecord)
	if rowCountPerBlock <= 0 {
		rowCountPerBlock = 1 // make sure the value is positive
	}

	log.Info("Numper parser: calculate row count per block to read file", zap.Int64("rowCountPerBlock", rowCountPerBlock),
		zap.Int64("blockSize", p.blockSize), zap.Int("sizePerRecord", sizePerRecord))
	return rowCountPerBlock, nil
}

// consume method reads numpy data section into a storage.FieldData
// please note it will require a large memory block(the memory size is almost equal to numpy file size)
func (p *NumpyParser) consume(columnReaders []*NumpyColumnReader) error {
	rowCountPerBlock, err := p.calcRowCountPerBlock()
	if err != nil {
		return err
	}

	updateProgress := func(readRowCount int) {
		if p.updateProgressFunc != nil && len(columnReaders) != 0 && columnReaders[0].rowCount > 0 {
			percent := (readRowCount * ProgressValueForPersist) / columnReaders[0].rowCount
			log.Debug("Numper parser: working progress", zap.Int("readRowCount", readRowCount),
				zap.Int("totalRowCount", columnReaders[0].rowCount), zap.Int("percent", percent))
			p.updateProgressFunc(int64(percent))
		}
	}

	// prepare shards
	shards := make([]ShardData, 0, p.collectionInfo.ShardNum)
	for i := 0; i < int(p.collectionInfo.ShardNum); i++ {
		shardData := initShardData(p.collectionInfo.Schema, p.collectionInfo.PartitionIDs)
		if shardData == nil {
			log.Warn("Numper parser: failed to initialize FieldData list")
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
		for _, reader := range columnReaders {
			fieldData, err := p.readData(reader, int(rowCountPerBlock))
			if err != nil {
				return err
			}

			if readRowCount == 0 {
				readRowCount = fieldData.RowNum()
			} else if readRowCount != fieldData.RowNum() {
				log.Warn("Numpy parser: data block's row count mismatch", zap.Int("firstBlockRowCount", readRowCount),
					zap.Int("thisBlockRowCount", fieldData.RowNum()), zap.Int64("rowCountPerBlock", rowCountPerBlock))
				return merr.WrapErrImportFailed(fmt.Sprintf("data block's row count mismatch: %d vs %d", readRowCount, fieldData.RowNum()))
			}

			segmentData[reader.fieldID] = fieldData
		}

		// nothing to read
		if readRowCount == 0 {
			break
		}
		totalRead += readRowCount
		updateProgress(totalRead)
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

// readData method reads numpy data section into a storage.FieldData
func (p *NumpyParser) readData(columnReader *NumpyColumnReader, rowCount int) (storage.FieldData, error) {
	switch columnReader.dataType {
	case schemapb.DataType_Bool:
		data, err := columnReader.reader.ReadBool(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read bool array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read bool array: %s", err.Error()))
		}

		return &storage.BoolFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int8:
		data, err := columnReader.reader.ReadInt8(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read int8 array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read int8 array: %s", err.Error()))
		}

		return &storage.Int8FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int16:
		data, err := columnReader.reader.ReadInt16(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to int16 array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read int16 array: %s", err.Error()))
		}

		return &storage.Int16FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int32:
		data, err := columnReader.reader.ReadInt32(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read int32 array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read int32 array: %s", err.Error()))
		}

		return &storage.Int32FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Int64:
		data, err := columnReader.reader.ReadInt64(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read int64 array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read int64 array: %s", err.Error()))
		}

		return &storage.Int64FieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Float:
		data, err := columnReader.reader.ReadFloat32(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read float array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read float array: %s", err.Error()))
		}

		err = typeutil.VerifyFloats32(data)
		if err != nil {
			log.Warn("Numpy parser: illegal value in float array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("illegal value in float array: %s", err.Error()))
		}

		return &storage.FloatFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_Double:
		data, err := columnReader.reader.ReadFloat64(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read double array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read double array: %s", err.Error()))
		}

		err = typeutil.VerifyFloats64(data)
		if err != nil {
			log.Warn("Numpy parser: illegal value in double array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("illegal value in double array: %s", err.Error()))
		}

		return &storage.DoubleFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_VarChar:
		data, err := columnReader.reader.ReadString(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read varchar array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read varchar array: %s", err.Error()))
		}

		return &storage.StringFieldData{
			Data: data,
		}, nil
	case schemapb.DataType_JSON:
		// JSON field read data from string array numpy
		data, err := columnReader.reader.ReadString(rowCount)
		if err != nil {
			log.Warn("Numpy parser: failed to read json string array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read json string array: %s", err.Error()))
		}

		byteArr := make([][]byte, 0)
		for _, str := range data {
			var dummy interface{}
			err := json.Unmarshal([]byte(str), &dummy)
			if err != nil {
				log.Warn("Numpy parser: illegal string value for JSON field",
					zap.String("value", str), zap.String("FieldName", columnReader.fieldName), zap.Error(err))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to parse value '%v' for JSON field '%s', error: %v",
					str, columnReader.fieldName, err))
			}
			byteArr = append(byteArr, []byte(str))
		}

		return &storage.JSONFieldData{
			Data: byteArr,
		}, nil
	case schemapb.DataType_BinaryVector:
		data, err := columnReader.reader.ReadUint8(rowCount * (columnReader.dimension / 8))
		if err != nil {
			log.Warn("Numpy parser: failed to read binary vector array", zap.Error(err))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read binary vector array: %s", err.Error()))
		}

		return &storage.BinaryVectorFieldData{
			Data: data,
			Dim:  columnReader.dimension,
		}, nil
	case schemapb.DataType_FloatVector:
		// float32/float64 numpy file can be used for float vector file, 2 reasons:
		// 1. for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// 2. for float64 numpy file, the performance is worse than float32 numpy file
		elementType := columnReader.reader.GetType()

		var data []float32
		var err error
		if elementType == schemapb.DataType_Float {
			data, err = columnReader.reader.ReadFloat32(rowCount * columnReader.dimension)
			if err != nil {
				log.Warn("Numpy parser: failed to read float vector array", zap.Error(err))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read float vector array: %s", err.Error()))
			}

			err = typeutil.VerifyFloats32(data)
			if err != nil {
				log.Warn("Numpy parser: illegal value in float vector array", zap.Error(err))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("illegal value in float vector array: %s", err.Error()))
			}
		} else if elementType == schemapb.DataType_Double {
			data = make([]float32, 0, columnReader.rowCount)
			data64, err := columnReader.reader.ReadFloat64(rowCount * columnReader.dimension)
			if err != nil {
				log.Warn("Numpy parser: failed to read float vector array", zap.Error(err))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read float vector array: %s", err.Error()))
			}

			for _, f64 := range data64 {
				err = typeutil.VerifyFloat(f64)
				if err != nil {
					log.Warn("Numpy parser: illegal value in float vector array", zap.Error(err))
					return nil, merr.WrapErrImportFailed(fmt.Sprintf("illegal value in float vector array: %s", err.Error()))
				}

				data = append(data, float32(f64))
			}
		}

		return &storage.FloatVectorFieldData{
			Data: data,
			Dim:  columnReader.dimension,
		}, nil
	default:
		log.Warn("Numpy parser: unsupported data type of field", zap.Any("dataType", columnReader.dataType),
			zap.String("fieldName", columnReader.fieldName))
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type %s of field '%s'", getTypeName(columnReader.dataType),
			columnReader.fieldName))
	}
}
