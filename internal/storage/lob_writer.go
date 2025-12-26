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

package storage

import (
	"context"
	"fmt"
	"path"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// LOBWriter manages writing large TEXT fields to separate LOB files.
// Each LOB file stores TEXT data that exceeds the size threshold.
//
// File structure: {root}/insert_log/{coll}/{part}/lobs/{field_id}/{lob_file_id}
// LOB files are stored at partition level (not segment level) to allow
// multiple segments to reference the same LOB files after compaction.
// The LOB file uses Storage V2 Parquet format with a single TEXT column.
//
// File size management:
// - Each LOB file is limited to maxLOBFileSize
// - When current file reaches the limit, a new file is created
// - This ensures reasonable file sizes for compaction and GC
type LOBWriter struct {
	// configuration
	segmentID      int64
	partitionID    int64
	fieldID        int64
	collectionID   int64
	sizeThreshold  int64
	maxLOBFileSize int64 // Maximum size for a single LOB file
	maxBatchSize   int64 // Maximum batch size before flush

	// allocator for LOB file IDs
	allocator allocator.Interface

	// storage configuration
	storageConfig *indexpb.StorageConfig

	// current LOB file state
	currentLobFileID    int64
	currentRowOffset    uint32
	currentFileSize     int64
	currentWriter       *packed.PackedWriter
	currentBatch        []string
	currentBatchMemSize int64
	currentColumnGroup  storagecommon.ColumnGroup

	// arrow memory allocator
	memAllocator memory.Allocator

	arrowSchema *arrow.Schema

	pathsMap    map[typeutil.UniqueID]string
	rowCountMap map[typeutil.UniqueID]int64 // record row_count per LOB file

	// statistics
	totalLOBFiles   int
	totalLOBRecords int64
	totalLOBBytes   int64

	// logger
	logger *log.MLogger
}

type LOBWriterOption func(*LOBWriter)

func WithLOBSizeThreshold(threshold int64) LOBWriterOption {
	return func(w *LOBWriter) {
		w.sizeThreshold = threshold
	}
}

func WithLOBMemAllocator(allocator memory.Allocator) LOBWriterOption {
	return func(w *LOBWriter) {
		w.memAllocator = allocator
	}
}

func WithMaxLOBFileSize(maxSize int64) LOBWriterOption {
	return func(w *LOBWriter) {
		w.maxLOBFileSize = maxSize
	}
}

func WithMaxBatchSize(maxSize int64) LOBWriterOption {
	return func(w *LOBWriter) {
		w.maxBatchSize = maxSize
	}
}

func NewLOBWriter(
	segmentID int64,
	partitionID int64,
	fieldID int64,
	collectionID int64,
	alloc allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	opts ...LOBWriterOption,
) (*LOBWriter, error) {
	if alloc == nil {
		return nil, errors.New("allocator cannot be nil")
	}

	// create arrow schema with field ID metadata for milvus-storage compatibility
	// use fixed LOBFieldID (10000) for LOB data column
	fieldMetadata := arrow.NewMetadata(
		[]string{packed.ArrowFieldIdMetadataKey},
		[]string{fmt.Sprintf("%d", LOBFieldID)},
	)

	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: LOBFieldName, Type: arrow.BinaryTypes.String, Metadata: fieldMetadata},
		},
		nil,
	)

	w := &LOBWriter{
		segmentID:      segmentID,
		partitionID:    partitionID,
		fieldID:        fieldID,
		collectionID:   collectionID,
		allocator:      alloc,
		storageConfig:  storageConfig,
		sizeThreshold:  65536,             // default 64KB
		maxLOBFileSize: 256 * 1024 * 1024, // default 256MB
		maxBatchSize:   16 * 1024 * 1024,  // default 16MB
		memAllocator:   memory.DefaultAllocator,
		arrowSchema:    arrowSchema,
		currentBatch:   make([]string, 0, 1024),
		pathsMap:       make(map[typeutil.UniqueID]string),
		rowCountMap:    make(map[typeutil.UniqueID]int64),
		logger: log.With(
			zap.Int64("segmentID", segmentID),
			zap.Int64("partitionID", partitionID),
			zap.Int64("fieldID", fieldID),
			zap.Int64("collectionID", collectionID),
		),
	}

	// apply options
	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

func (w *LOBWriter) WriteText(ctx context.Context, text string) (*LOBReference, error) {
	textSize := int64(len(text))

	if textSize < w.sizeThreshold {
		return nil, errors.New("text size is below threshold")
	}

	if w.currentWriter == nil {
		if err := w.createNewLOBFile(ctx); err != nil {
			return nil, errors.Wrap(err, "failed to create new LOB file")
		}
	}

	// check if adding this text would exceed maxLOBFileSize
	// if so, close current file and create a new one
	if w.maxLOBFileSize > 0 && w.currentFileSize+textSize > w.maxLOBFileSize {
		w.logger.Info("LOB file size limit reached, rotating to new file",
			zap.Int64("currentLobFileID", w.currentLobFileID),
			zap.Int64("currentFileSize", w.currentFileSize),
			zap.Int64("maxLOBFileSize", w.maxLOBFileSize),
			zap.Int64("textSize", textSize))

		if err := w.flushCurrentBatch(ctx); err != nil {
			return nil, errors.Wrap(err, "failed to flush LOB batch before rotation")
		}
		if err := w.currentWriter.Close(); err != nil {
			return nil, errors.Wrap(err, "failed to close LOB writer before rotation")
		}
		// record row_count for this file before resetting
		w.rowCountMap[w.currentLobFileID] = int64(w.currentRowOffset)

		w.currentWriter = nil
		w.currentFileSize = 0
		if err := w.createNewLOBFile(ctx); err != nil {
			return nil, errors.Wrap(err, "failed to create new LOB file after rotation")
		}
	}

	// add to current batch
	w.currentBatch = append(w.currentBatch, text)
	w.currentBatchMemSize += textSize
	w.currentFileSize += textSize
	rowOffset := w.currentRowOffset
	w.currentRowOffset++

	// flush batch if it gets too large (use maxBatchSize if set, otherwise default 16MB)
	batchSizeLimit := w.maxBatchSize
	if batchSizeLimit == 0 {
		batchSizeLimit = 16 * 1024 * 1024
	}
	if w.currentBatchMemSize >= batchSizeLimit {
		if err := w.flushCurrentBatch(ctx); err != nil {
			return nil, errors.Wrap(err, "failed to flush LOB batch")
		}
	}

	w.totalLOBRecords++
	w.totalLOBBytes += textSize

	ref := NewLOBReference(uint64(w.currentLobFileID), rowOffset)
	return ref, nil
}

// createNewLOBFile creates a new LOB file and initializes the writer
func (w *LOBWriter) createNewLOBFile(ctx context.Context) error {
	start, _, err := w.allocator.Alloc(1)
	if err != nil {
		return errors.Wrap(err, "failed to allocate LOB file ID")
	}
	w.currentLobFileID = start
	w.currentRowOffset = 0
	w.totalLOBFiles++

	lobFilePath := w.getLOBFilePath(w.currentLobFileID)

	w.currentColumnGroup = storagecommon.ColumnGroup{
		GroupID: typeutil.UniqueID(w.currentLobFileID),
		Columns: []int{0}, // LOB schema has only one column (text_data) at index 0
		Fields:  []int64{w.fieldID},
	}

	storageType := w.getStorageType()
	truePath := lobFilePath
	if storageType != "local" {
		bucketName := w.getBucketName()
		truePath = path.Join(bucketName, lobFilePath)
	}

	// create packed writer for LOB file following storage V2 pattern
	writer, err := packed.NewPackedWriter(
		[]string{truePath},
		w.arrowSchema,
		16*1024*1024, // 16MB buffer
		10*1024*1024, // 10MB multipart upload
		[]storagecommon.ColumnGroup{w.currentColumnGroup},
		w.storageConfig,
		nil, // no encryption context for now
	)
	if err != nil {
		return errors.Wrapf(err, "failed to create packed writer for LOB file %d", w.currentLobFileID)
	}

	w.currentWriter = writer
	w.currentBatch = make([]string, 0, 1024)
	w.currentBatchMemSize = 0

	// store path for metadata (without bucket name prefix)
	w.pathsMap[w.currentLobFileID] = lobFilePath

	w.logger.Info("created new LOB file",
		zap.Int64("lobFileID", w.currentLobFileID),
		zap.String("path", lobFilePath),
		zap.String("truePath", truePath),
	)

	return nil
}

// flushCurrentBatch flushes the current batch to the LOB file
func (w *LOBWriter) flushCurrentBatch(ctx context.Context) error {
	if len(w.currentBatch) == 0 {
		return nil
	}

	builder := array.NewStringBuilder(w.memAllocator)
	defer builder.Release()

	for _, text := range w.currentBatch {
		builder.Append(text)
	}

	textArray := builder.NewStringArray()
	defer textArray.Release()

	record := array.NewRecord(w.arrowSchema, []arrow.Array{textArray}, int64(len(w.currentBatch)))
	defer record.Release()

	if err := w.currentWriter.WriteRecordBatch(record); err != nil {
		return errors.Wrap(err, "failed to write LOB batch to packed writer")
	}

	w.logger.Debug("flushed LOB batch",
		zap.Int64("lobFileID", w.currentLobFileID),
		zap.Int("recordCount", len(w.currentBatch)),
		zap.Int64("batchMemSize", w.currentBatchMemSize),
	)

	w.currentBatch = make([]string, 0, 1024)
	w.currentBatchMemSize = 0

	return nil
}

// flush flushes all pending data and closes the current LOB file
func (w *LOBWriter) Flush(ctx context.Context) error {
	if w.currentWriter == nil {
		return nil
	}

	if err := w.flushCurrentBatch(ctx); err != nil {
		return err
	}

	if err := w.currentWriter.Close(); err != nil {
		return errors.Wrap(err, "failed to close LOB writer")
	}

	// record row_count for this file before resetting
	w.rowCountMap[w.currentLobFileID] = int64(w.currentRowOffset)

	w.logger.Info("flushed and closed LOB file",
		zap.Int64("lobFileID", w.currentLobFileID),
		zap.Uint32("totalRows", w.currentRowOffset),
		zap.Int64("fileSize", w.currentFileSize),
	)

	w.currentWriter = nil
	w.currentLobFileID = 0
	w.currentRowOffset = 0
	w.currentFileSize = 0

	return nil
}

func (w *LOBWriter) Close(ctx context.Context) error {
	return w.Flush(ctx)
}

func (w *LOBWriter) GetStatistics() map[string]interface{} {
	return map[string]interface{}{
		"total_lob_files":   w.totalLOBFiles,
		"total_lob_records": w.totalLOBRecords,
		"total_lob_bytes":   w.totalLOBBytes,
		"current_lob_file":  w.currentLobFileID,
		"current_offset":    w.currentRowOffset,
	}
}

// GetLOBFileInfos returns the list of LOB file info with path, ID, and row_count
func (w *LOBWriter) GetLOBFileInfos() []*LOBFileInfo {
	infos := make([]*LOBFileInfo, 0, len(w.pathsMap))
	for id, filePath := range w.pathsMap {
		rowCount := w.rowCountMap[id]
		infos = append(infos, &LOBFileInfo{
			FilePath:         filePath,
			LobFileID:        id,
			RowCount:         rowCount,
			ValidRecordCount: rowCount, // initially all records are valid
		})
	}
	return infos
}

func (w *LOBWriter) getStorageType() string {
	storageType := paramtable.Get().CommonCfg.StorageType.GetValue()
	if w.storageConfig != nil {
		storageType = w.storageConfig.GetStorageType()
	}
	return storageType
}

func (w *LOBWriter) getBucketName() string {
	bucketName := paramtable.Get().MinioCfg.BucketName.GetValue()
	if w.storageConfig != nil {
		bucketName = w.storageConfig.GetBucketName()
	}
	return bucketName
}

// getLOBFilePath constructs the file path for a LOB file
// Format: {root_path}/insert_log/{collection_id}/{partition_id}/lobs/{field_id}/{lob_file_id}
func (w *LOBWriter) getLOBFilePath(lobFileID int64) string {
	rootPath := paramtable.Get().MinioCfg.RootPath.GetValue()
	if w.storageConfig != nil {
		rootPath = w.storageConfig.GetRootPath()
	}

	return metautil.BuildLOBLogPath(rootPath, w.collectionID, w.partitionID, w.fieldID, lobFileID)
}

func (w *LOBWriter) GetLOBSizeThreshold() int64 {
	return w.sizeThreshold
}

// GetCurrentFilePath returns the current LOB file's logical path (without bucket prefix)
// and a boolean indicating whether there is an active writer/file.
func (w *LOBWriter) GetCurrentFilePath() (string, bool) {
	if w.currentWriter == nil || w.currentLobFileID == 0 {
		return "", false
	}
	return w.getLOBFilePath(w.currentLobFileID), true
}
