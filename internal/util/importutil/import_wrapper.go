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
	"fmt"
	"math"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	JSONFileExt  = ".json"
	NumpyFileExt = ".npy"

	// supposed size of a single block, to control a binlog file size, the max biglog file size is no more than 2*SingleBlockSize
	SingleBlockSize = 16 * 1024 * 1024 // 16MB

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

// ReportImportAttempts is the maximum # of attempts to retry when import fails.
var ReportImportAttempts uint = 10

type ImportFlushFunc func(fields map[storage.FieldID]storage.FieldData, shardID int) error
type AssignSegmentFunc func(shardID int) (int64, string, error)
type CreateBinlogsFunc func(fields map[storage.FieldID]storage.FieldData, segmentID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error)
type SaveSegmentFunc func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog, segmentID int64, targetChName string, rowCount int64) error

type WorkingSegment struct {
	segmentID    int64                 // segment ID
	shardID      int                   // shard id
	targetChName string                // target dml channel
	rowCount     int64                 // accumulate row count
	memSize      int                   // total memory size of all binlogs
	fieldsInsert []*datapb.FieldBinlog // persisted binlogs
	fieldsStats  []*datapb.FieldBinlog // stats of persisted binlogs
}

type ImportOptions struct {
	OnlyValidate bool
	TsStartPoint uint64
	TsEndPoint   uint64
}

func DefaultImportOptions() ImportOptions {
	options := ImportOptions{
		OnlyValidate: false,
		TsStartPoint: 0,
		TsEndPoint:   math.MaxUint64,
	}
	return options
}

type ImportWrapper struct {
	ctx              context.Context            // for canceling parse process
	cancel           context.CancelFunc         // for canceling parse process
	collectionSchema *schemapb.CollectionSchema // collection schema
	shardNum         int32                      // sharding number of the collection
	segmentSize      int64                      // maximum size of a segment(unit:byte) defined by dataCoord.segment.maxSize (milvus.yml)
	rowIDAllocator   *allocator.IDAllocator     // autoid allocator
	chunkManager     storage.ChunkManager

	assignSegmentFunc AssignSegmentFunc // function to prepare a new segment
	createBinlogsFunc CreateBinlogsFunc // function to create binlog for a segment
	saveSegmentFunc   SaveSegmentFunc   // function to persist a segment

	importResult *rootcoordpb.ImportResult                 // import result
	reportFunc   func(res *rootcoordpb.ImportResult) error // report import state to rootcoord

	workingSegments map[int]*WorkingSegment // a map shard id to working segments
}

func NewImportWrapper(ctx context.Context, collectionSchema *schemapb.CollectionSchema, shardNum int32, segmentSize int64,
	idAlloc *allocator.IDAllocator, cm storage.ChunkManager, importResult *rootcoordpb.ImportResult,
	reportFunc func(res *rootcoordpb.ImportResult) error) *ImportWrapper {
	if collectionSchema == nil {
		log.Error("import wrapper: collection schema is nil")
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
		chunkManager:     cm,
		importResult:     importResult,
		reportFunc:       reportFunc,
		workingSegments:  make(map[int]*WorkingSegment),
	}

	return wrapper
}

func (p *ImportWrapper) SetCallbackFunctions(assignSegmentFunc AssignSegmentFunc, createBinlogsFunc CreateBinlogsFunc, saveSegmentFunc SaveSegmentFunc) error {
	if assignSegmentFunc == nil {
		log.Error("import wrapper: callback function AssignSegmentFunc is nil")
		return fmt.Errorf("import wrapper: callback function AssignSegmentFunc is nil")
	}

	if createBinlogsFunc == nil {
		log.Error("import wrapper: callback function CreateBinlogsFunc is nil")
		return fmt.Errorf("import wrapper: callback function CreateBinlogsFunc is nil")
	}

	if saveSegmentFunc == nil {
		log.Error("import wrapper: callback function SaveSegmentFunc is nil")
		return fmt.Errorf("import wrapper: callback function SaveSegmentFunc is nil")
	}

	p.assignSegmentFunc = assignSegmentFunc
	p.createBinlogsFunc = createBinlogsFunc
	p.saveSegmentFunc = saveSegmentFunc
	return nil
}

// Cancel method can be used to cancel parse process
func (p *ImportWrapper) Cancel() error {
	p.cancel()
	return nil
}

func (p *ImportWrapper) validateColumnBasedFiles(filePaths []string, collectionSchema *schemapb.CollectionSchema) error {
	requiredFieldNames := make(map[string]interface{})
	for _, schema := range p.collectionSchema.Fields {
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
			log.Error("import wrapper: the file has no corresponding field in collection", zap.String("fieldName", name))
			return fmt.Errorf("import wrapper: the file '%s' has no corresponding field in collection", filePath)
		}
	}

	// check missed file
	for name := range requiredFieldNames {
		_, ok := fileNames[name]
		if !ok {
			log.Error("import wrapper: there is no file corresponding to field", zap.String("fieldName", name))
			return fmt.Errorf("import wrapper: there is no file corresponding to field '%s'", name)
		}
	}

	return nil
}

// fileValidation verify the input paths
// if all the files are json type, return true
// if all the files are numpy type, return false, and not allow duplicate file name
func (p *ImportWrapper) fileValidation(filePaths []string) (bool, error) {
	// use this map to check duplicate file name(only for numpy file)
	fileNames := make(map[string]struct{})

	totalSize := int64(0)
	rowBased := false
	for i := 0; i < len(filePaths); i++ {
		filePath := filePaths[i]
		name, fileType := GetFileNameAndExt(filePath)

		// only allow json file or numpy file
		if fileType != JSONFileExt && fileType != NumpyFileExt {
			log.Error("import wrapper: unsupportted file type", zap.String("filePath", filePath))
			return false, fmt.Errorf("import wrapper: unsupportted file type: '%s'", filePath)
		}

		// we use the first file to determine row-based or column-based
		if i == 0 && fileType == JSONFileExt {
			rowBased = true
		}

		// check file type
		// row-based only support json type, column-based only support numpy type
		if rowBased {
			if fileType != JSONFileExt {
				log.Error("import wrapper: unsupported file type for row-based mode", zap.String("filePath", filePath))
				return rowBased, fmt.Errorf("import wrapper: unsupported file type for row-based mode: '%s'", filePath)
			}
		} else {
			if fileType != NumpyFileExt {
				log.Error("import wrapper: unsupported file type for column-based mode", zap.String("filePath", filePath))
				return rowBased, fmt.Errorf("import wrapper: unsupported file type for column-based mode: '%s'", filePath)
			}
		}

		// check dupliate file
		_, ok := fileNames[name]
		if ok {
			log.Error("import wrapper: duplicate file name", zap.String("filePath", filePath))
			return rowBased, fmt.Errorf("import wrapper: duplicate file: '%s'", filePath)
		}
		fileNames[name] = struct{}{}

		// check file size, single file size cannot exceed MaxFileSize
		// TODO add context
		size, err := p.chunkManager.Size(context.TODO(), filePath)
		if err != nil {
			log.Error("import wrapper: failed to get file size", zap.String("filePath", filePath), zap.Error(err))
			return rowBased, fmt.Errorf("import wrapper: failed to get file size of '%s'", filePath)
		}

		// empty file
		if size == 0 {
			log.Error("import wrapper: file size is zero", zap.String("filePath", filePath))
			return rowBased, fmt.Errorf("import wrapper: the file '%s' size is zero", filePath)
		}

		if size > MaxFileSize {
			log.Error("import wrapper: file size exceeds the maximum size", zap.String("filePath", filePath),
				zap.Int64("fileSize", size), zap.Int64("MaxFileSize", MaxFileSize))
			return rowBased, fmt.Errorf("import wrapper: the file '%s' size exceeds the maximum size: %d bytes", filePath, MaxFileSize)
		}
		totalSize += size
	}

	// especially for column-base, total size of files cannot exceed MaxTotalSizeInMemory
	if totalSize > MaxTotalSizeInMemory {
		log.Error("import wrapper: total size of files exceeds the maximum size", zap.Int64("totalSize", totalSize), zap.Int64("MaxTotalSize", MaxTotalSizeInMemory))
		return rowBased, fmt.Errorf("import wrapper: total size(%d bytes) of all files exceeds the maximum size: %d bytes", totalSize, MaxTotalSizeInMemory)
	}

	// check redundant files for column-based import
	// if the field is primary key and autoid is false, the file is required
	// any redundant file is not allowed
	if !rowBased {
		err := p.validateColumnBasedFiles(filePaths, p.collectionSchema)
		if err != nil {
			return rowBased, err
		}
	}

	return rowBased, nil
}

// Import is the entry of import operation
// filePath and rowBased are from ImportTask
// if onlyValidate is true, this process only do validation, no data generated, flushFunc will not be called
func (p *ImportWrapper) Import(filePaths []string, options ImportOptions) error {
	log.Info("import wrapper: begin import", zap.Any("filePaths", filePaths), zap.Any("options", options))
	// data restore function to import milvus native binlog files(for backup/restore tools)
	// the backup/restore tool provide two paths for a partition, the first path is binlog path, the second is deltalog path
	if p.isBinlogImport(filePaths) {
		// TODO: handle the timestamp end point passed from client side, currently use math.MaxUint64
		return p.doBinlogImport(filePaths, options.TsStartPoint, options.TsEndPoint)
	}

	// normal logic for import general data files
	rowBased, err := p.fileValidation(filePaths)
	if err != nil {
		return err
	}

	if rowBased {
		// parse and consume row-based files
		// for row-based files, the JSONRowConsumer will generate autoid for primary key, and split rows into segments
		// according to shard number, so the flushFunc will be called in the JSONRowConsumer
		for i := 0; i < len(filePaths); i++ {
			filePath := filePaths[i]
			_, fileType := GetFileNameAndExt(filePath)
			log.Info("import wrapper:  row-based file ", zap.Any("filePath", filePath), zap.Any("fileType", fileType))

			if fileType == JSONFileExt {
				err = p.parseRowBasedJSON(filePath, options.OnlyValidate)
				if err != nil {
					log.Error("import wrapper: failed to parse row-based json file", zap.Error(err), zap.String("filePath", filePath))
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
			return fmt.Errorf("import wrapper: failed to initialize FieldData list")
		}

		rowCount := 0

		// function to combine column data into fieldsData
		combineFunc := func(fields map[storage.FieldID]storage.FieldData) error {
			if len(fields) == 0 {
				return nil
			}

			printFieldsDataInfo(fields, "import wrapper: combine field data", nil)
			tr := timerecord.NewTimeRecorder("combine field data")
			defer tr.Elapse("finished")

			for k, v := range fields {
				// ignore 0 row field
				if v.RowNum() == 0 {
					log.Warn("import wrapper: empty FieldData ignored", zap.Int64("fieldID", k))
					continue
				}

				// ignore internal fields: RowIDField and TimeStampField
				if k == common.RowIDField || k == common.TimeStampField {
					log.Warn("import wrapper: internal fields should not be provided", zap.Int64("fieldID", k))
					continue
				}

				// each column should be only combined once
				data, ok := fieldsData[k]
				if ok && data.RowNum() > 0 {
					return fmt.Errorf("the field %d is duplicated", k)
				}

				// check the row count. only count non-zero row fields
				if rowCount > 0 && rowCount != v.RowNum() {
					return fmt.Errorf("the field %d row count %d doesn't equal others row count: %d", k, v.RowNum(), rowCount)
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
			_, fileType := GetFileNameAndExt(filePath)
			log.Info("import wrapper:  column-based file ", zap.Any("filePath", filePath), zap.Any("fileType", fileType))

			if fileType == NumpyFileExt {
				err = p.parseColumnBasedNumpy(filePath, options.OnlyValidate, combineFunc)

				if err != nil {
					log.Error("import wrapper: failed to parse column-based numpy file", zap.Error(err), zap.String("filePath", filePath))
					return err
				}
			}
			// no need to check else, since the fileValidation() already do this
		}

		// trigger after read finished
		triggerGC()

		// split fields data into segments
		err := p.splitFieldsData(fieldsData, SingleBlockSize)
		if err != nil {
			return err
		}

		// trigger after write finished
		triggerGC()
	}

	return p.reportPersisted()
}

// reportPersisted notify the rootcoord to mark the task state to be ImportPersisted
func (p *ImportWrapper) reportPersisted() error {
	// force close all segments
	err := p.closeAllWorkingSegments()
	if err != nil {
		return err
	}

	// report file process state
	p.importResult.State = commonpb.ImportState_ImportPersisted
	// persist state task is valuable, retry more times in case fail this task only because of network error
	reportErr := retry.Do(p.ctx, func() error {
		return p.reportFunc(p.importResult)
	}, retry.Attempts(ReportImportAttempts))
	if reportErr != nil {
		log.Warn("import wrapper: fail to report import state to RootCoord", zap.Error(reportErr))
		return reportErr
	}
	return nil
}

// isBinlogImport is to judge whether it is binlog import operation
// For internal usage by the restore tool: https://github.com/zilliztech/milvus-backup
// This tool exports data from a milvus service, and call bulkload interface to import native data into another milvus service.
// This tool provides two paths: one is data log path of a partition,the other is delta log path of this partition.
// This method checks the filePaths, if the file paths is exist and not a file, we say it is native import.
func (p *ImportWrapper) isBinlogImport(filePaths []string) bool {
	// must contains the insert log path, and the delta log path is optional
	if len(filePaths) != 1 && len(filePaths) != 2 {
		log.Info("import wrapper: paths count is not 1 or 2, not binlog import", zap.Int("len", len(filePaths)))
		return false
	}

	for i := 0; i < len(filePaths); i++ {
		filePath := filePaths[i]
		_, fileType := GetFileNameAndExt(filePath)
		// contains file extension, is not a path
		if len(fileType) != 0 {
			log.Info("import wrapper: not a path, not binlog import", zap.String("filePath", filePath), zap.String("fileType", fileType))
			return false
		}
	}

	log.Info("import wrapper: do binlog import")
	return true
}

// doBinlogImport is the entry of binlog import operation
func (p *ImportWrapper) doBinlogImport(filePaths []string, tsStartPoint uint64, tsEndPoint uint64) error {
	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		printFieldsDataInfo(fields, "import wrapper: prepare to flush binlog data", filePaths)
		return p.flushFunc(fields, shardID)
	}
	parser, err := NewBinlogParser(p.ctx, p.collectionSchema, p.shardNum, SingleBlockSize, p.chunkManager, flushFunc,
		tsStartPoint, tsEndPoint)
	if err != nil {
		return err
	}

	err = parser.Parse(filePaths)
	if err != nil {
		return err
	}

	return p.reportPersisted()
}

// parseRowBasedJSON is the entry of row-based json import operation
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
			printFieldsDataInfo(fields, "import wrapper: prepare to flush binlogs", filePaths)
			return p.flushFunc(fields, shardID)
		}

		consumer, err = NewJSONRowConsumer(p.collectionSchema, p.rowIDAllocator, p.shardNum, SingleBlockSize, flushFunc)
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

// parseColumnBasedNumpy is the entry of column-based numpy import operation
func (p *ImportWrapper) parseColumnBasedNumpy(filePath string, onlyValidate bool,
	combineFunc func(fields map[storage.FieldID]storage.FieldData) error) error {
	tr := timerecord.NewTimeRecorder("numpy parser: " + filePath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fileName, _ := GetFileNameAndExt(filePath)

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

// appendFunc defines the methods to append data to storage.FieldData
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

// splitFieldsData is to split the in-memory data(parsed from column-based files) into blocks, each block save to a binlog file
func (p *ImportWrapper) splitFieldsData(fieldsData map[storage.FieldID]storage.FieldData, blockSize int64) error {
	if len(fieldsData) == 0 {
		log.Error("import wrapper: fields data is empty")
		return fmt.Errorf("import wrapper: fields data is empty")
	}

	tr := timerecord.NewTimeRecorder("import wrapper: split field data")
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
				return fmt.Errorf("import wrapper: field '%s' not provided", schema.GetName())
			}
			rowCounter[schema.GetName()] = v.RowNum()
			if v.RowNum() > rowCount {
				rowCount = v.RowNum()
			}
		}
	}
	if primaryKey == nil {
		log.Error("import wrapper: primary key field is not found")
		return fmt.Errorf("import wrapper: primary key field is not found")
	}

	for name, count := range rowCounter {
		if count != rowCount {
			log.Error("import wrapper: field row count is not equal to other fields row count", zap.String("fieldName", name),
				zap.Int("rowCount", count), zap.Int("otherRowCount", rowCount))
			return fmt.Errorf("import wrapper: field '%s' row count %d is not equal to other fields row count: %d", name, count, rowCount)
		}
	}
	log.Info("import wrapper: try to split a block with row count", zap.Int("rowCount", rowCount), zap.Any("rowCountOfEachField", rowCounter))

	primaryData, ok := fieldsData[primaryKey.GetFieldID()]
	if !ok {
		log.Error("import wrapper: primary key field is not provided", zap.String("keyName", primaryKey.GetName()))
		return fmt.Errorf("import wrapper: primary key field is not provided")
	}

	// generate auto id for primary key and rowid field
	rowIDBegin, rowIDEnd, err := p.rowIDAllocator.Alloc(uint32(rowCount))
	if err != nil {
		log.Error("import wrapper: failed to alloc row ID", zap.Error(err))
		return err
	}

	rowIDField := fieldsData[common.RowIDField]
	rowIDFieldArr := rowIDField.(*storage.Int64FieldData)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		rowIDFieldArr.Data = append(rowIDFieldArr.Data, i)
	}

	if primaryKey.GetAutoID() {
		log.Info("import wrapper: generating auto-id", zap.Int("rowCount", rowCount), zap.Int64("rowIDBegin", rowIDBegin))

		// reset the primary keys, as we know, only int64 pk can be auto-generated
		primaryDataArr := &storage.Int64FieldData{
			NumRows: []int64{int64(rowCount)},
			Data:    make([]int64, 0, rowCount),
		}
		for i := rowIDBegin; i < rowIDEnd; i++ {
			primaryDataArr.Data = append(primaryDataArr.Data, i)
		}

		primaryData = primaryDataArr
		fieldsData[primaryKey.GetFieldID()] = primaryData
		p.importResult.AutoIds = append(p.importResult.AutoIds, rowIDBegin, rowIDEnd)
	}

	if primaryData.RowNum() <= 0 {
		log.Error("import wrapper: primary key not provided", zap.String("keyName", primaryKey.GetName()))
		return fmt.Errorf("import wrapper: the primary key '%s' not provided", primaryKey.GetName())
	}

	// prepare segemnts
	segmentsData := make([]map[storage.FieldID]storage.FieldData, 0, p.shardNum)
	for i := 0; i < int(p.shardNum); i++ {
		segmentData := initSegmentData(p.collectionSchema)
		if segmentData == nil {
			log.Error("import wrapper: failed to initialize FieldData list")
			return fmt.Errorf("import wrapper: failed to initialize FieldData list")
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
			return fmt.Errorf("import wrapper: unsupported field data type")
		}
		appendFunctions[schema.GetName()] = appendFuncErr
	}

	// split data into shards
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
				return fmt.Errorf("import wrapper: primary key field must be int64 or varchar")
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

		// when the estimated size is close to blockSize, force flush
		err = tryFlushBlocks(p.ctx, segmentsData, p.collectionSchema, p.flushFunc, blockSize, MaxTotalSizeInMemory, false)
		if err != nil {
			return err
		}
	}

	// force flush at the end
	return tryFlushBlocks(p.ctx, segmentsData, p.collectionSchema, p.flushFunc, blockSize, MaxTotalSizeInMemory, true)
}

// flushFunc is the callback function for parsers generate segment and save binlog files
func (p *ImportWrapper) flushFunc(fields map[storage.FieldID]storage.FieldData, shardID int) error {
	// if fields data is empty, do nothing
	var rowNum int
	memSize := 0
	for _, field := range fields {
		rowNum = field.RowNum()
		memSize += field.GetMemorySize()
		break
	}
	if rowNum <= 0 {
		log.Warn("import wrapper: fields data is empty", zap.Int("shardID", shardID))
		return nil
	}

	// if there is no segment for this shard, create a new one
	// if the segment exists and its size almost exceed segmentSize, close it and create a new one
	var segment *WorkingSegment
	segment, ok := p.workingSegments[shardID]
	if ok {
		// the segment already exists, check its size, if the size exceeds(or almost) segmentSize, close the segment
		if int64(segment.memSize)+int64(memSize) >= p.segmentSize {
			err := p.closeWorkingSegment(segment)
			if err != nil {
				return err
			}
			segment = nil
			p.workingSegments[shardID] = nil
		}

	}

	if segment == nil {
		// create a new segment
		segID, channelName, err := p.assignSegmentFunc(shardID)
		if err != nil {
			log.Error("import wrapper: failed to assign a new segment", zap.Error(err), zap.Int("shardID", shardID))
			return err
		}

		segment = &WorkingSegment{
			segmentID:    segID,
			shardID:      shardID,
			targetChName: channelName,
			rowCount:     int64(0),
			memSize:      0,
			fieldsInsert: make([]*datapb.FieldBinlog, 0),
			fieldsStats:  make([]*datapb.FieldBinlog, 0),
		}
		p.workingSegments[shardID] = segment
	}

	// save binlogs
	fieldsInsert, fieldsStats, err := p.createBinlogsFunc(fields, segment.segmentID)
	if err != nil {
		log.Error("import wrapper: failed to save binlogs", zap.Error(err), zap.Int("shardID", shardID),
			zap.Int64("segmentID", segment.segmentID), zap.String("targetChannel", segment.targetChName))
		return err
	}

	segment.fieldsInsert = append(segment.fieldsInsert, fieldsInsert...)
	segment.fieldsStats = append(segment.fieldsStats, fieldsStats...)
	segment.rowCount += int64(rowNum)
	segment.memSize += memSize

	return nil
}

// closeWorkingSegment marks a segment to be sealed
func (p *ImportWrapper) closeWorkingSegment(segment *WorkingSegment) error {
	log.Info("import wrapper: adding segment to the correct DataNode flow graph and saving binlog paths",
		zap.Int("shardID", segment.shardID),
		zap.Int64("segmentID", segment.segmentID),
		zap.String("targetChannel", segment.targetChName),
		zap.Int64("rowCount", segment.rowCount),
		zap.Int("insertLogCount", len(segment.fieldsInsert)),
		zap.Int("statsLogCount", len(segment.fieldsStats)))

	err := p.saveSegmentFunc(segment.fieldsInsert, segment.fieldsStats, segment.segmentID, segment.targetChName, segment.rowCount)
	if err != nil {
		log.Error("import wrapper: failed to save segment",
			zap.Error(err),
			zap.Int("shardID", segment.shardID),
			zap.Int64("segmentID", segment.segmentID),
			zap.String("targetChannel", segment.targetChName))
		return err
	}

	return nil
}

// closeAllWorkingSegments mark all segments to be sealed at the end of import operation
func (p *ImportWrapper) closeAllWorkingSegments() error {
	for _, segment := range p.workingSegments {
		err := p.closeWorkingSegment(segment)
		if err != nil {
			return err
		}
	}
	p.workingSegments = make(map[int]*WorkingSegment)

	return nil
}
