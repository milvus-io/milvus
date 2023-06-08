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
	"strconv"

	"github.com/milvus-io/milvus/internal/allocator"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

const (
	JSONFileExt  = ".json"
	NumpyFileExt = ".npy"

	// supposed size of a single block, to control a binlog file size, the max biglog file size is no more than 2*SingleBlockSize
	SingleBlockSize = 16 * 1024 * 1024 // 16MB

	// this limitation is to avoid this OOM risk:
	// simetimes system segment max size is a large number, a single segment fields data might cause OOM.
	// flush the segment when its data reach this limitation, let the compaction to compact it later.
	MaxSegmentSizeInMemory = 512 * 1024 * 1024 // 512MB

	// this limitation is to avoid this OOM risk:
	// if the shard number is a large number, although single segment size is small, but there are lot of in-memory segments,
	// the total memory size might cause OOM.
	// TODO: make it configurable.
	MaxTotalSizeInMemory = 6 * 1024 * 1024 * 1024 // 6GB

	// progress percent value of persist state
	ProgressValueForPersist = 90

	// keywords of import task informations
	FailedReason    = "failed_reason"
	Files           = "files"
	CollectionName  = "collection"
	PartitionName   = "partition"
	PersistTimeCost = "persist_cost"
	ProgressPercent = "progress_percent"
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

	importResult         *rootcoordpb.ImportResult                 // import result
	reportFunc           func(res *rootcoordpb.ImportResult) error // report import state to rootcoord
	reportImportAttempts uint                                      // attempts count if report function get error

	workingSegments map[int]*WorkingSegment // a map shard id to working segments
	progressPercent int64                   // working progress percent
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
		Name:               collectionSchema.GetName(),
		Description:        collectionSchema.GetDescription(),
		AutoID:             collectionSchema.GetAutoID(),
		Fields:             make([]*schemapb.FieldSchema, 0),
		EnableDynamicField: collectionSchema.GetEnableDynamicField(),
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
		ctx:                  ctx,
		cancel:               cancel,
		collectionSchema:     realSchema,
		shardNum:             shardNum,
		segmentSize:          segmentSize,
		rowIDAllocator:       idAlloc,
		chunkManager:         cm,
		importResult:         importResult,
		reportFunc:           reportFunc,
		reportImportAttempts: ReportImportAttempts,
		workingSegments:      make(map[int]*WorkingSegment),
	}

	return wrapper
}

func (p *ImportWrapper) SetCallbackFunctions(assignSegmentFunc AssignSegmentFunc, createBinlogsFunc CreateBinlogsFunc, saveSegmentFunc SaveSegmentFunc) error {
	if assignSegmentFunc == nil {
		log.Error("import wrapper: callback function AssignSegmentFunc is nil")
		return fmt.Errorf("callback function AssignSegmentFunc is nil")
	}

	if createBinlogsFunc == nil {
		log.Error("import wrapper: callback function CreateBinlogsFunc is nil")
		return fmt.Errorf("callback function CreateBinlogsFunc is nil")
	}

	if saveSegmentFunc == nil {
		log.Error("import wrapper: callback function SaveSegmentFunc is nil")
		return fmt.Errorf("callback function SaveSegmentFunc is nil")
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
			log.Error("import wrapper: unsupported file type", zap.String("filePath", filePath))
			return false, fmt.Errorf("unsupported file type: '%s'", filePath)
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
				return rowBased, fmt.Errorf("unsupported file type for row-based mode: '%s'", filePath)
			}
		} else {
			if fileType != NumpyFileExt {
				log.Error("import wrapper: unsupported file type for column-based mode", zap.String("filePath", filePath))
				return rowBased, fmt.Errorf("unsupported file type for column-based mode: '%s'", filePath)
			}
		}

		// check dupliate file
		_, ok := fileNames[name]
		if ok {
			log.Error("import wrapper: duplicate file name", zap.String("filePath", filePath))
			return rowBased, fmt.Errorf("duplicate file: '%s'", filePath)
		}
		fileNames[name] = struct{}{}

		// check file size, single file size cannot exceed MaxFileSize
		size, err := p.chunkManager.Size(p.ctx, filePath)
		if err != nil {
			log.Error("import wrapper: failed to get file size", zap.String("filePath", filePath), zap.Error(err))
			return rowBased, fmt.Errorf("failed to get file size of '%s', error:%w", filePath, err)
		}

		// empty file
		if size == 0 {
			log.Error("import wrapper: file size is zero", zap.String("filePath", filePath))
			return rowBased, fmt.Errorf("the file '%s' size is zero", filePath)
		}

		if size > params.Params.CommonCfg.ImportMaxFileSize.GetAsInt64() {
			log.Error("import wrapper: file size exceeds the maximum size", zap.String("filePath", filePath),
				zap.Int64("fileSize", size), zap.Int64("MaxFileSize", params.Params.CommonCfg.ImportMaxFileSize.GetAsInt64()))
			return rowBased, fmt.Errorf("the file '%s' size exceeds the maximum size: %d bytes", filePath, params.Params.CommonCfg.ImportMaxFileSize.GetAsInt64())
		}
		totalSize += size
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
	if options.IsBackup && p.isBinlogImport(filePaths) {
		return p.doBinlogImport(filePaths, options.TsStartPoint, options.TsEndPoint)
	}

	// normal logic for import general data files
	rowBased, err := p.fileValidation(filePaths)
	if err != nil {
		return err
	}

	tr := timerecord.NewTimeRecorder("Import task")
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
		// parse and consume column-based files(currently support numpy)
		// for column-based files, the NumpyParser will generate autoid for primary key, and split rows into segments
		// according to shard number, so the flushFunc will be called in the NumpyParser
		flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
			printFieldsDataInfo(fields, "import wrapper: prepare to flush binlog data", filePaths)
			return p.flushFunc(fields, shardID)
		}
		parser, err := NewNumpyParser(p.ctx, p.collectionSchema, p.rowIDAllocator, p.shardNum, SingleBlockSize,
			p.chunkManager, flushFunc, p.updateProgressPercent)
		if err != nil {
			return err
		}

		err = parser.Parse(filePaths)
		if err != nil {
			return err
		}

		p.importResult.AutoIds = append(p.importResult.AutoIds, parser.IDRange()...)

		// trigger after parse finished
		triggerGC()
	}

	return p.reportPersisted(p.reportImportAttempts, tr)
}

// reportPersisted notify the rootcoord to mark the task state to be ImportPersisted
func (p *ImportWrapper) reportPersisted(reportAttempts uint, tr *timerecord.TimeRecorder) error {
	// force close all segments
	err := p.closeAllWorkingSegments()
	if err != nil {
		return err
	}

	if tr != nil {
		ts := tr.Elapse("persist finished").Seconds()
		p.importResult.Infos = append(p.importResult.Infos,
			&commonpb.KeyValuePair{Key: PersistTimeCost, Value: strconv.FormatFloat(ts, 'f', 2, 64)})
	}

	// report file process state
	p.importResult.State = commonpb.ImportState_ImportPersisted
	progressValue := strconv.Itoa(ProgressValueForPersist)
	UpdateKVInfo(&p.importResult.Infos, ProgressPercent, progressValue)

	log.Info("import wrapper: report import result", zap.Any("importResult", p.importResult))
	// persist state task is valuable, retry more times in case fail this task only because of network error
	reportErr := retry.Do(p.ctx, func() error {
		return p.reportFunc(p.importResult)
	}, retry.Attempts(reportAttempts))
	if reportErr != nil {
		log.Warn("import wrapper: fail to report import state to RootCoord", zap.Error(reportErr))
		return reportErr
	}
	return nil
}

// isBinlogImport is to judge whether it is binlog import operation
// For internal usage by the restore tool: https://github.com/zilliztech/milvus-backup
// This tool exports data from a milvus service, and call bulkload interface to import native data into another milvus service.
// This tool provides two paths: one is insert log path of a partition,the other is delta log path of this partition.
// This method checks the filePaths, if the file paths is exist and not a file, we say it is native import.
func (p *ImportWrapper) isBinlogImport(filePaths []string) bool {
	// must contains the insert log path, and the delta log path is optional to be empty string
	if len(filePaths) != 2 {
		log.Info("import wrapper: paths count is not 2, not binlog import", zap.Int("len", len(filePaths)))
		return false
	}

	checkFunc := func(filePath string) bool {
		// contains file extension, is not a path
		_, fileType := GetFileNameAndExt(filePath)
		if len(fileType) != 0 {
			log.Info("import wrapper: not a path, not binlog import", zap.String("filePath", filePath), zap.String("fileType", fileType))
			return false
		}
		return true
	}

	// the first path is insert log path
	filePath := filePaths[0]
	if len(filePath) == 0 {
		log.Info("import wrapper: the first path is empty string, not binlog import")
		return false
	}

	if !checkFunc(filePath) {
		return false
	}

	// the second path is delta log path
	filePath = filePaths[1]
	if len(filePath) > 0 && !checkFunc(filePath) {
		return false
	}

	log.Info("import wrapper: do binlog import")
	return true
}

// doBinlogImport is the entry of binlog import operation
func (p *ImportWrapper) doBinlogImport(filePaths []string, tsStartPoint uint64, tsEndPoint uint64) error {
	tr := timerecord.NewTimeRecorder("Import task")

	flushFunc := func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
		printFieldsDataInfo(fields, "import wrapper: prepare to flush binlog data", filePaths)
		return p.flushFunc(fields, shardID)
	}
	parser, err := NewBinlogParser(p.ctx, p.collectionSchema, p.shardNum, SingleBlockSize, p.chunkManager, flushFunc,
		p.updateProgressPercent, tsStartPoint, tsEndPoint)
	if err != nil {
		return err
	}

	err = parser.Parse(filePaths)
	if err != nil {
		return err
	}

	return p.reportPersisted(p.reportImportAttempts, tr)
}

// parseRowBasedJSON is the entry of row-based json import operation
func (p *ImportWrapper) parseRowBasedJSON(filePath string, onlyValidate bool) error {
	tr := timerecord.NewTimeRecorder("json row-based parser: " + filePath)

	// for minio storage, chunkManager will download file into local memory
	// for local storage, chunkManager open the file directly
	file, err := p.chunkManager.Reader(p.ctx, filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	size, err := p.chunkManager.Size(p.ctx, filePath)
	if err != nil {
		return err
	}

	// parse file
	reader := bufio.NewReader(file)
	parser := NewJSONParser(p.ctx, p.collectionSchema, p.updateProgressPercent)

	// if only validate, we input a empty flushFunc so that the consumer do nothing but only validation.
	var flushFunc ImportFlushFunc
	if onlyValidate {
		flushFunc = func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
			return nil
		}
	} else {
		flushFunc = func(fields map[storage.FieldID]storage.FieldData, shardID int) error {
			var filePaths = []string{filePath}
			printFieldsDataInfo(fields, "import wrapper: prepare to flush binlogs", filePaths)
			return p.flushFunc(fields, shardID)
		}
	}

	consumer, err := NewJSONRowConsumer(p.collectionSchema, p.rowIDAllocator, p.shardNum, SingleBlockSize, flushFunc)
	if err != nil {
		return err
	}

	err = parser.ParseRows(&IOReader{r: reader, fileSize: size}, consumer)
	if err != nil {
		return err
	}

	// for row-based files, auto-id is generated within JSONRowConsumer
	p.importResult.AutoIds = append(p.importResult.AutoIds, consumer.IDRange()...)

	tr.Elapse("parsed")
	return nil
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
			return fmt.Errorf("failed to assign a new segment for shard id %d, error: %w", shardID, err)
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
		return fmt.Errorf("failed to save binlogs, shard id %d, segment id %d, channel '%s', error: %w",
			shardID, segment.segmentID, segment.targetChName, err)
	}

	segment.fieldsInsert = append(segment.fieldsInsert, fieldsInsert...)
	segment.fieldsStats = append(segment.fieldsStats, fieldsStats...)
	segment.rowCount += int64(rowNum)
	segment.memSize += memSize

	// report working progress percent value to rootcoord
	// if failed to report, ignore the error, the percent value might be improper but the task can be succeed
	progressValue := strconv.Itoa(int(p.progressPercent))
	UpdateKVInfo(&p.importResult.Infos, ProgressPercent, progressValue)
	reportErr := retry.Do(p.ctx, func() error {
		return p.reportFunc(p.importResult)
	}, retry.Attempts(p.reportImportAttempts))
	if reportErr != nil {
		log.Warn("import wrapper: fail to report working progress percent value to RootCoord", zap.Error(reportErr))
	}

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
		log.Error("import wrapper: failed to seal segment",
			zap.Error(err),
			zap.Int("shardID", segment.shardID),
			zap.Int64("segmentID", segment.segmentID),
			zap.String("targetChannel", segment.targetChName))
		return fmt.Errorf("failed to seal segment, shard id %d, segment id %d, channel '%s', error: %w",
			segment.shardID, segment.segmentID, segment.targetChName, err)
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

func (p *ImportWrapper) updateProgressPercent(percent int64) {
	// ignore illegal percent value
	if percent < 0 || percent > 100 {
		return
	}
	p.progressPercent = percent
}
