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
	"bufio"
	"context"
	"errors"
	"math"
	"path"
	"runtime/debug"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	JSONFileExt  = ".json"
	NumpyFileExt = ".npy"

	// this limitation is to avoid this OOM risk:
	// for column-based file, we read all its data into memory, if user input a large file, the read() method may
	// cost extra memory and lear to OOM.
	MaxFileSize = 1 * 1024 * 1024 * 1024 // 1GB

	// this limitation is to avoid this OOM risk:
	// simetimes system segment max size is a large number, a single segment fields data might cause OOM.
	// flush the segment when its data reach this limitation, let the compaction to compact it later.
	MaxSegmentSizeInMemory = 512 * 1024 * 1024 // 512MB

	// this limitation is to avoid this OOM risk:
	// if the shard number is a large number, although single segment size is small, but there are lot of in-memory segments,
	// the total memory size might cause OOM.
	MaxTotalSizeInMemory = 2 * 1024 * 1024 * 1024 // 2GB
)

type ImportWrapper struct {
	ctx              context.Context            // for canceling parse process
	cancel           context.CancelFunc         // for canceling parse process
	collectionSchema *schemapb.CollectionSchema // collection schema
	shardNum         int32                      // sharding number of the collection
	segmentSize      int64                      // maximum size of a segment(unit:byte)
	rowIDAllocator   *allocator.IDAllocator     // autoid allocator
	chunkManager     storage.ChunkManager

	callFlushFunc ImportFlushFunc // call back function to flush a segment

	importResult *rootcoordpb.ImportResult                 // import result
	reportFunc   func(res *rootcoordpb.ImportResult) error // report import state to rootcoord
}

func NewImportWrapper(ctx context.Context, collectionSchema *schemapb.CollectionSchema, shardNum int32, segmentSize int64,
	idAlloc *allocator.IDAllocator, cm storage.ChunkManager, flushFunc ImportFlushFunc,
	importResult *rootcoordpb.ImportResult, reportFunc func(res *rootcoordpb.ImportResult) error) *ImportWrapper {
	if collectionSchema == nil {
		log.Error("import error: collection schema is nil")
		return nil
	}

	// ignore the RowID field and Timestamp field
	realSchema := &schemapb.CollectionSchema{
		Name:        collectionSchema.GetName(),
		Description: collectionSchema.GetDescription(),
		AutoID:      collectionSchema.GetAutoID(),
		Fields:      make([]*schemapb.FieldSchema, 0),
	}
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		if schema.GetName() == common.RowIDFieldName || schema.GetName() == common.TimeStampFieldName {
			continue
		}
		realSchema.Fields = append(realSchema.Fields, schema)
	}

	ctx, cancel := context.WithCancel(ctx)

	wrapper := &ImportWrapper{
		ctx:              ctx,
		cancel:           cancel,
		collectionSchema: realSchema,
		shardNum:         shardNum,
		segmentSize:      segmentSize,
		rowIDAllocator:   idAlloc,
		callFlushFunc:    flushFunc,
		chunkManager:     cm,
		importResult:     importResult,
		reportFunc:       reportFunc,
	}

	return wrapper
}

// this method can be used to cancel parse process
func (p *ImportWrapper) Cancel() error {
	p.cancel()
	return nil
}

func (p *ImportWrapper) printFieldsDataInfo(fieldsData map[storage.FieldID]storage.FieldData, msg string, files []string) {
	stats := make([]zapcore.Field, 0)
	for k, v := range fieldsData {
		stats = append(stats, zap.Int(strconv.FormatInt(k, 10), v.RowNum()))
	}

	if len(files) > 0 {
		stats = append(stats, zap.Any("files", files))
	}
	log.Info(msg, stats...)
}

func getFileNameAndExt(filePath string) (string, string) {
	fileName := path.Base(filePath)
	fileType := path.Ext(fileName)
	fileNameWithoutExt := strings.TrimSuffix(fileName, fileType)
	return fileNameWithoutExt, fileType
}

// trigger golang gc to return all free memory back to the underlying system at once,
// Note: this operation is expensive, and can lead to latency spikes as it holds the heap lock through the whole process
func triggerGC() {
	debug.FreeOSMemory()
}

func (p *ImportWrapper) fileValidation(filePaths []string, rowBased bool) error {
	// use this map to check duplicate file name(only for numpy file)
	fileNames := make(map[string]struct{})

	totalSize := int64(0)
	for i := 0; i < len(filePaths); i++ {
		filePath := filePaths[i]
		name, fileType := getFileNameAndExt(filePath)
		_, ok := fileNames[name]
		if ok {
			// only check dupliate numpy file
			if fileType == NumpyFileExt {
				log.Error("import wrapper: duplicate file name", zap.String("fileName", name+"."+fileType))
				return errors.New("duplicate file: " + name + "." + fileType)
			}
		} else {
			fileNames[name] = struct{}{}
		}

		// check file type
		// row-based only support json type, column-based can support json and numpy type
		if rowBased {
			if fileType != JSONFileExt {
				log.Error("import wrapper: unsupported file type for row-based mode", zap.String("filePath", filePath))
				return errors.New("unsupported file type for row-based mode: " + filePath)
			}
		} else {
			if fileType != JSONFileExt && fileType != NumpyFileExt {
				log.Error("import wrapper: unsupported file type for column-based mode", zap.String("filePath", filePath))
				return errors.New("unsupported file type for column-based mode: " + filePath)
			}
		}

		// check file size, single file size cannot exceed MaxFileSize
		// TODO add context
		size, err := p.chunkManager.Size(context.TODO(), filePath)
		if err != nil {
			log.Error("import wrapper: failed to get file size", zap.String("filePath", filePath), zap.Any("err", err))
			return errors.New("failed to get file size of " + filePath)
		}

		if size == 0 {
			log.Error("import wrapper: file path is empty", zap.String("filePath", filePath))
			return errors.New("the file " + filePath + " is empty")
		}

		if size > MaxFileSize {
			log.Error("import wrapper: file size exceeds the maximum size", zap.String("filePath", filePath),
				zap.Int64("fileSize", size), zap.Int64("MaxFileSize", MaxFileSize))
			return errors.New("the file " + filePath + " size exceeds the maximum size: " + strconv.FormatInt(MaxFileSize, 10) + " bytes")
		}
		totalSize += size
	}

	// especially for column-base, total size of files cannot exceed MaxTotalSizeInMemory
	if totalSize > MaxTotalSizeInMemory {
		log.Error("import wrapper: total size of files exceeds the maximum size", zap.Int64("totalSize", totalSize), zap.Int64("MaxTotalSize", MaxTotalSizeInMemory))
		return errors.New("the total size of all files exceeds the maximum size: " + strconv.FormatInt(MaxTotalSizeInMemory, 10) + " bytes")
	}

	return nil
}

// import process entry
// filePath and rowBased are from ImportTask
// if onlyValidate is true, this process only do validation, no data generated, callFlushFunc will not be called
func (p *ImportWrapper) Import(filePaths []string, rowBased bool, onlyValidate bool) error {
	log.Info("import wrapper: filePaths", zap.Any("filePaths", filePaths))
	// data restore function to import milvus native binlog files(for backup/restore tools)
	// the backup/restore tool provide two paths for a partition, the first path is binlog path, the second is deltalog path
	if p.isBinlogImport(filePaths) {
		// TODO: handle the timestamp end point passed from client side, currently use math.MaxUint64
		return p.doBinlogImport(filePaths, math.MaxUint64)
	}

	// normal logic for import general data files
	err := p.fileValidation(filePaths, rowBased)
	if err != nil {
		return err
	}

	if rowBased {
		// parse and consume row-based files
		// for row-based files, the JSONRowConsumer will generate autoid for primary key, and split rows into segments
		// according to shard number, so the callFlushFunc will be called in the JSONRowConsumer
		for i := 0; i < len(filePaths); i++ {
			filePath := filePaths[i]
			_, fileType := getFileNameAndExt(filePath)
			log.Info("import wrapper:  row-based file ", zap.Any("filePath", filePath), zap.Any("fileType", fileType))

			if fileType == JSONFileExt {
				err = p.parseRowBasedJSON(filePath, onlyValidate)
				if err != nil {
					log.Error("import error: "+err.Error(), zap.String("filePath", filePath))
					return err
				}
			} // no need to check else, since the fileValidation() already do this

			// trigger gc after each file finished
			triggerGC()
		}
	} else {
		// parse and consume column-based files
		// for column-based files, the XXXColumnConsumer only output map[string]storage.FieldData
		// after all columns are parsed/consumed, we need to combine map[string]storage.FieldData into one
		// and use splitFieldsData() to split fields data into segments according to shard number
		fieldsData := initSegmentData(p.collectionSchema)
		if fieldsData == nil {
			log.Error("import wrapper: failed to initialize FieldData list")
			return errors.New("failed to initialize FieldData list")
		}

		rowCount := 0

		// function to combine column data into fieldsData
		combineFunc := func(fields map[storage.FieldID]storage.FieldData) error {
			if len(fields) == 0 {
				return nil
			}

			p.printFieldsDataInfo(fields, "import wrapper: combine field data", nil)
			tr := timerecord.NewTimeRecorder("combine field data")
			defer tr.Elapse("finished")

			for k, v := range fields {
				// ignore 0 row field
				if v.RowNum() == 0 {
					continue
				}

				// each column should be only combined once
				data, ok := fieldsData[k]
				if ok && data.RowNum() > 0 {
					return errors.New("the field " + strconv.FormatInt(k, 10) + " is duplicated")
				}

				// check the row count. only count non-zero row fields
				if rowCount > 0 && rowCount != v.RowNum() {
					return errors.New("the field " + strconv.FormatInt(k, 10) + " row count " + strconv.Itoa(v.RowNum()) + " doesn't equal " + strconv.Itoa(rowCount))
				}
				rowCount = v.RowNum()

				// assign column data to fieldsData
				fieldsData[k] = v
			}

			return nil
		}

		// parse/validate/consume data
		for i := 0; i < len(filePaths); i++ {
			filePath := filePaths[i]
			_, fileType := getFileNameAndExt(filePath)
			log.Info("import wrapper:  column-based file ", zap.Any("filePath", filePath), zap.Any("fileType", fileType))

			if fileType == JSONFileExt {
				err = p.parseColumnBasedJSON(filePath, onlyValidate, combineFunc)
				if err != nil {
					log.Error("import error: "+err.Error(), zap.String("filePath", filePath))
					return err
				}
			} else if fileType == NumpyFileExt {
				err = p.parseColumnBasedNumpy(filePath, onlyValidate, combineFunc)

				if err != nil {
					log.Error("import error: "+err.Error(), zap.String("filePath", filePath))
					return err
				}
			}
			// no need to check else, since the fileValidation() already do this
		}

		// trigger after read finished
		triggerGC()

		// split fields data into segments
		err := p.splitFieldsData(fieldsData, filePaths)
		if err != nil {
			return err
		}

		// trigger after write finished
		triggerGC()
	}

	return p.reportPersisted()
}

func (p *ImportWrapper) reportPersisted() error {
	// report file process state
	p.importResult.State = commonpb.ImportState_ImportPersisted
	// persist state task is valuable, retry more times in case fail this task only because of network error
	reportErr := retry.Do(p.ctx, func() error {
		return p.reportFunc(p.importResult)
	}, retry.Attempts(10))
	if reportErr != nil {
		log.Warn("import wrapper: fail to report import state to root coord", zap.Error(reportErr))
		return reportErr
	}
	return nil
}

// For internal usage by the restore tool: https://github.com/zilliztech/milvus-backup
// This tool exports data from a milvus service, and call bulkload interface to import native data into another milvus service.
// This tool provides two paths: one is data log path of a partition,the other is delta log path of this partition.
// This method checks the filePaths, if the file paths is exist and not a file, we say it is native import.
func (p *ImportWrapper) isBinlogImport(filePaths []string) bool {
	// must contains the insert log path, and the delta log path is optional
	if len(filePaths) != 1 && len(filePaths) != 2 {
		log.Info("import wrapper: paths count is not 1 or 2", zap.Int("len", len(filePaths)))
		return false
	}

	for i := 0; i < len(filePaths); i++ {
		filePath := filePaths[i]
		_, fileType := getFileNameAndExt(filePath)
		// contains file extension, is not a path
		if len(fileType) != 0 {
			log.Info("import wrapper: not a path", zap.String("filePath", filePath), zap.String("fileType", fileType))
			return false
		}
	}

	log.Info("import wrapper: do binlog import")
	return true
}

func (p *ImportWrapper) doBinlogImport(filePaths []string, tsEndPoint uint64) error {
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		p.printFieldsDataInfo(fields, "import wrapper: prepare to flush binlog data", filePaths)
		return p.callFlushFunc(fields, shardID)
	}
	parser, err := NewBinlogParser(p.collectionSchema, p.shardNum, p.segmentSize, p.chunkManager, flushFunc, tsEndPoint)
	if err != nil {
		return err
	}

	err = parser.Parse(filePaths)
	if err != nil {
		return err
	}

	return p.reportPersisted()
}

func (p *ImportWrapper) parseRowBasedJSON(filePath string, onlyValidate bool) error {
	tr := timerecord.NewTimeRecorder("json row-based parser: " + filePath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// for minio storage, chunkManager will download file into local memory
	// for local storage, chunkManager open the file directly
	file, err := p.chunkManager.Reader(ctx, filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// parse file
	reader := bufio.NewReader(file)
	parser := NewJSONParser(p.ctx, p.collectionSchema)
	var consumer *JSONRowConsumer
	if !onlyValidate {
		flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
			var filePaths = []string{filePath}
			p.printFieldsDataInfo(fields, "import wrapper: prepare to flush segment", filePaths)
			return p.callFlushFunc(fields, shardID)
		}
		consumer, err = NewJSONRowConsumer(p.collectionSchema, p.rowIDAllocator, p.shardNum, p.segmentSize, flushFunc)
		if err != nil {
			return err
		}
	}
	validator, err := NewJSONRowValidator(p.collectionSchema, consumer)
	if err != nil {
		return err
	}

	err = parser.ParseRows(reader, validator)
	if err != nil {
		return err
	}

	// for row-based files, auto-id is generated within JSONRowConsumer
	if consumer != nil {
		p.importResult.AutoIds = append(p.importResult.AutoIds, consumer.IDRange()...)
	}

	tr.Elapse("parsed")
	return nil
}

func (p *ImportWrapper) parseColumnBasedJSON(filePath string, onlyValidate bool,
	combineFunc func(fields map[storage.FieldID]storage.FieldData) error) error {
	tr := timerecord.NewTimeRecorder("json column-based parser: " + filePath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// for minio storage, chunkManager will download file into local memory
	// for local storage, chunkManager open the file directly
	file, err := p.chunkManager.Reader(ctx, filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// parse file
	reader := bufio.NewReader(file)
	parser := NewJSONParser(p.ctx, p.collectionSchema)
	var consumer *JSONColumnConsumer
	if !onlyValidate {
		consumer, err = NewJSONColumnConsumer(p.collectionSchema, combineFunc)
		if err != nil {
			return err
		}
	}
	validator, err := NewJSONColumnValidator(p.collectionSchema, consumer)
	if err != nil {
		return err
	}

	err = parser.ParseColumns(reader, validator)
	if err != nil {
		return err
	}

	tr.Elapse("parsed")
	return nil
}

func (p *ImportWrapper) parseColumnBasedNumpy(filePath string, onlyValidate bool,
	combineFunc func(fields map[storage.FieldID]storage.FieldData) error) error {
	tr := timerecord.NewTimeRecorder("numpy parser: " + filePath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fileName, _ := getFileNameAndExt(filePath)

	// for minio storage, chunkManager will download file into local memory
	// for local storage, chunkManager open the file directly
	file, err := p.chunkManager.Reader(ctx, filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var id storage.FieldID
	var found = false
	for _, field := range p.collectionSchema.Fields {
		if field.GetName() == fileName {
			id = field.GetFieldID()
			found = true
			break
		}
	}

	// if the numpy file name is not mapping to a field name, ignore it
	if !found {
		return nil
	}

	// the numpy parser return a storage.FieldData, here construct a map[string]storage.FieldData to combine
	flushFunc := func(field storage.FieldData) error {
		fields := make(map[storage.FieldID]storage.FieldData)
		fields[id] = field
		return combineFunc(fields)
	}

	// for numpy file, we say the file name(without extension) is the filed name
	parser := NewNumpyParser(p.ctx, p.collectionSchema, flushFunc)
	err = parser.Parse(file, fileName, onlyValidate)
	if err != nil {
		return err
	}

	tr.Elapse("parsed")
	return nil
}

func (p *ImportWrapper) appendFunc(schema *schemapb.FieldSchema) func(src storage.FieldData, n int, target storage.FieldData) error {
	switch schema.DataType {
	case schemapb.DataType_Bool:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.BoolFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(bool))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Float:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.FloatFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(float32))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Double:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.DoubleFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(float64))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int8:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int8FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int8))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int16:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int16FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int16))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int32:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int32FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int32))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int64:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int64FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int64))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_BinaryVector:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.BinaryVectorFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).([]byte)...)
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_FloatVector:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.FloatVectorFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).([]float32)...)
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.StringFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(string))
			return nil
		}
	default:
		return nil
	}
}

func (p *ImportWrapper) splitFieldsData(fieldsData map[storage.FieldID]storage.FieldData, files []string) error {
	if len(fieldsData) == 0 {
		log.Error("import wrapper: fields data is empty")
		return errors.New("import error: fields data is empty")
	}

	tr := timerecord.NewTimeRecorder("split field data")
	defer tr.Elapse("finished")

	// check existence of each field
	// check row count, all fields row count must be equal
	// firstly get the max row count
	rowCount := 0
	rowCounter := make(map[string]int)
	var primaryKey *schemapb.FieldSchema
	for i := 0; i < len(p.collectionSchema.Fields); i++ {
		schema := p.collectionSchema.Fields[i]
		if schema.GetIsPrimaryKey() {
			primaryKey = schema
		}

		if !schema.GetAutoID() {
			v, ok := fieldsData[schema.GetFieldID()]
			if !ok {
				log.Error("import wrapper: field not provided", zap.String("fieldName", schema.GetName()))
				return errors.New("import error: field " + schema.GetName() + " not provided")
			}
			rowCounter[schema.GetName()] = v.RowNum()
			if v.RowNum() > rowCount {
				rowCount = v.RowNum()
			}
		}
	}
	if primaryKey == nil {
		log.Error("import wrapper: primary key field is not found")
		return errors.New("import error: primary key field is not found")
	}

	for name, count := range rowCounter {
		if count != rowCount {
			log.Error("import wrapper: field row count is not equal to other fields row count", zap.String("fieldName", name),
				zap.Int("rowCount", count), zap.Int("otherRowCount", rowCount))
			return errors.New("import error: field " + name + " row count " + strconv.Itoa(count) + " is not equal to other fields row count " + strconv.Itoa(rowCount))
		}
	}

	primaryData, ok := fieldsData[primaryKey.GetFieldID()]
	if !ok {
		log.Error("import wrapper: primary key field is not provided", zap.String("keyName", primaryKey.GetName()))
		return errors.New("import error: primary key field is not provided")
	}

	// generate auto id for primary key and rowid field
	var rowIDBegin typeutil.UniqueID
	var rowIDEnd typeutil.UniqueID
	rowIDBegin, rowIDEnd, _ = p.rowIDAllocator.Alloc(uint32(rowCount))

	rowIDField := fieldsData[common.RowIDField]
	rowIDFieldArr := rowIDField.(*storage.Int64FieldData)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		rowIDFieldArr.Data = append(rowIDFieldArr.Data, i)
	}

	if primaryKey.GetAutoID() {
		log.Info("import wrapper: generating auto-id", zap.Any("rowCount", rowCount))

		primaryDataArr := primaryData.(*storage.Int64FieldData)
		for i := rowIDBegin; i < rowIDEnd; i++ {
			primaryDataArr.Data = append(primaryDataArr.Data, i)
		}

		p.importResult.AutoIds = append(p.importResult.AutoIds, rowIDBegin, rowIDEnd)
	}

	if primaryData.RowNum() <= 0 {
		log.Error("import wrapper: primary key not provided", zap.String("keyName", primaryKey.GetName()))
		return errors.New("import wrapper: primary key " + primaryKey.GetName() + " not provided")
	}

	// prepare segemnts
	segmentsData := make([]map[storage.FieldID]storage.FieldData, 0, p.shardNum)
	for i := 0; i < int(p.shardNum); i++ {
		segmentData := initSegmentData(p.collectionSchema)
		if segmentData == nil {
			log.Error("import wrapper: failed to initialize FieldData list")
			return errors.New("failed to initialize FieldData list")
		}
		segmentsData = append(segmentsData, segmentData)
	}

	// prepare append functions
	appendFunctions := make(map[string]func(src storage.FieldData, n int, target storage.FieldData) error)
	for i := 0; i < len(p.collectionSchema.Fields); i++ {
		schema := p.collectionSchema.Fields[i]
		appendFuncErr := p.appendFunc(schema)
		if appendFuncErr == nil {
			log.Error("import wrapper: unsupported field data type")
			return errors.New("import wrapper: unsupported field data type")
		}
		appendFunctions[schema.GetName()] = appendFuncErr
	}

	// split data into segments
	for i := 0; i < rowCount; i++ {
		// hash to a shard number
		var shard uint32
		pk := primaryData.GetRow(i)
		strPK, ok := interface{}(pk).(string)
		if ok {
			hash := typeutil.HashString2Uint32(strPK)
			shard = hash % uint32(p.shardNum)
		} else {
			intPK, ok := interface{}(pk).(int64)
			if !ok {
				log.Error("import wrapper: primary key field must be int64 or varchar")
				return errors.New("import error: primary key field must be int64 or varchar")
			}
			hash, _ := typeutil.Hash32Int64(intPK)
			shard = hash % uint32(p.shardNum)
		}

		// set rowID field
		rowIDField := segmentsData[shard][common.RowIDField].(*storage.Int64FieldData)
		rowIDField.Data = append(rowIDField.Data, rowIDFieldArr.GetRow(i).(int64))

		// append row to shard
		for k := 0; k < len(p.collectionSchema.Fields); k++ {
			schema := p.collectionSchema.Fields[k]
			srcData := fieldsData[schema.GetFieldID()]
			targetData := segmentsData[shard][schema.GetFieldID()]
			appendFunc := appendFunctions[schema.GetName()]
			err := appendFunc(srcData, i, targetData)
			if err != nil {
				return err
			}
		}
	}

	// call flush function
	for i := 0; i < int(p.shardNum); i++ {
		segmentData := segmentsData[i]
		p.printFieldsDataInfo(segmentData, "import wrapper: prepare to flush segment", files)
		err := p.callFlushFunc(segmentData, i)
		if err != nil {
			log.Error("import wrapper: flush callback function failed", zap.Any("err", err))
			return err
		}
	}

	return nil
}
