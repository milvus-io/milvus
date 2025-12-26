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
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// PendingLOB represents a TEXT value that will be written to LOB file during flush (lazy mode)
type PendingLOB struct {
	segmentID   int64
	partitionID int64
	fieldID     int64
	rowIndex    int
	data        string
	originalRef *string
}

// LOBManager manages multiple LOB writers for different segments and fields.
// It provides a centralized interface for LOB operations across the write buffer.
//
// Hybrid Mode Strategy:
//   - Small TEXT (textSize < sizeThreshold):                      Keep inline, no LOB processing
//   - Medium TEXT (sizeThreshold <= textSize < lazyWriteMaxSize): Lazy mode, write during flush to batch optimize
//   - Large TEXT (textSize >= lazyWriteMaxSize):                  Eager mode, write immediately to reduce memory pressure
type LOBManager struct {
	mu sync.RWMutex

	// configuration
	sizeThreshold    int64
	lazyWriteMaxSize int64
	collectionID     int64

	allocator     allocator.Interface
	storageConfig *indexpb.StorageConfig

	// writers map: key = "segmentID:fieldID"
	writers map[string]*LOBWriter

	// pending LOBs for lazy write (segmentID -> list of pending LOBs)
	pendingLOBs map[int64][]*PendingLOB
	pendingMu   sync.RWMutex

	// logger
	logger *log.MLogger
}

// LOBManagerOption is a functional option for configuring LOBManager
type LOBManagerOption func(*LOBManager)

func WithLOBManagerSizeThreshold(threshold int64) LOBManagerOption {
	return func(m *LOBManager) {
		m.sizeThreshold = threshold
	}
}

func WithLOBManagerLazyWriteMaxSize(maxSize int64) LOBManagerOption {
	return func(m *LOBManager) {
		m.lazyWriteMaxSize = maxSize
	}
}

func NewLOBManager(
	collectionID int64,
	alloc allocator.Interface,
	opts ...LOBManagerOption,
) (*LOBManager, error) {
	if alloc == nil {
		return nil, errors.New("allocator cannot be nil")
	}

	m := &LOBManager{
		sizeThreshold:    65536,    // 64KB
		lazyWriteMaxSize: 10485760, // 10MB
		collectionID:     collectionID,
		allocator:        alloc,
		writers:          make(map[string]*LOBWriter),
		pendingLOBs:      make(map[int64][]*PendingLOB),
		logger: log.With(
			zap.Int64("collectionID", collectionID),
		),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m, nil
}

func (m *LOBManager) GetSizeThreshold() int64 {
	return m.sizeThreshold
}

// ProcessInsertData processes insert data and handles TEXT fields based on size:
//   - Small TEXT (< sizeThreshold):                         Keep inline, no processing
//   - Medium TEXT (>= sizeThreshold && < lazyWriteMaxSize): Lazy mode, defer to flush
//   - Large TEXT (>= lazyWriteMaxSize):                     Eager mode, write immediately
//
// Returns true if any data was converted to LOB.
func (m *LOBManager) ProcessInsertData(ctx context.Context, segmentID int64, partitionID int64, schema *schemapb.CollectionSchema, insertData *InsertData) (bool, error) {
	hasLOB := false

	for fieldID, fieldData := range insertData.Data {
		// Step 1: Get field schema to verify data type
		fieldSchema := m.getFieldSchema(schema, fieldID)
		if fieldSchema == nil || fieldSchema.GetDataType() != schemapb.DataType_Text {
			continue
		}

		// Step 2: Type check - only process TEXT fields
		stringField, ok := fieldData.(*StringFieldData)
		if !ok {
			continue
		}

		// Step 3: Process each TEXT value with hybrid strategy
		modified, err := m.processTextFieldHybrid(ctx, segmentID, partitionID, fieldID, stringField)
		if err != nil {
			return false, errors.Wrapf(err, "failed to process string field %d", fieldID)
		}

		if modified {
			hasLOB = true
		}
	}

	return hasLOB, nil
}

func (m *LOBManager) getFieldSchema(schema *schemapb.CollectionSchema, fieldID int64) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return field
		}
	}
	return nil
}

func (m *LOBManager) processTextFieldHybrid(ctx context.Context, segmentID int64, partitionID int64, fieldID int64, field *StringFieldData) (bool, error) {
	modified := false

	for i, text := range field.Data {
		textSize := int64(len(text))

		// Step 1: Small TEXT (< sizeThreshold): Keep inline
		if textSize < m.sizeThreshold {
			continue
		}

		if field.Nullable && i < len(field.ValidData) && !field.ValidData[i] {
			continue
		}

		// Step 2: Medium TEXT (>= sizeThreshold && < lazyWriteMaxSize): Add to pending queue (lazy)
		if textSize < m.lazyWriteMaxSize {
			m.addPendingLOB(&PendingLOB{
				segmentID:   segmentID,
				partitionID: partitionID,
				fieldID:     fieldID,
				rowIndex:    i,
				data:        text,
				originalRef: &field.Data[i], // pointer to replace later
			})
			modified = true

			continue
		}

		// Step 3: Large TEXT (>= lazyWriteMaxSize): Write immediately
		writer, err := m.getOrCreateWriter(ctx, segmentID, partitionID, fieldID)
		if err != nil {
			return false, errors.Wrap(err, "failed to get LOB writer")
		}

		ref, err := writer.WriteText(ctx, text)
		if err != nil {
			return false, errors.Wrapf(err, "failed to write LOB data for field %d", fieldID)
		}

		// Step 4: Encode reference and replace original text
		encodedRef := EncodeLOBReference(ref)
		field.Data[i] = string(encodedRef)
		modified = true
	}

	return modified, nil
}

func (m *LOBManager) addPendingLOB(pending *PendingLOB) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()

	m.pendingLOBs[pending.segmentID] = append(m.pendingLOBs[pending.segmentID], pending)
}

func (m *LOBManager) getOrCreateWriter(ctx context.Context, segmentID int64, partitionID int64, fieldID int64) (*LOBWriter, error) {
	key := m.getWriterKey(segmentID, fieldID)

	m.mu.RLock()
	writer, exists := m.writers[key]
	m.mu.RUnlock()

	if exists {
		return writer, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if writer, exists := m.writers[key]; exists {
		return writer, nil
	}

	writer, err := NewLOBWriter(
		segmentID,
		partitionID,
		fieldID,
		m.collectionID,
		m.allocator,
		m.storageConfig,
		WithLOBSizeThreshold(m.sizeThreshold),
		WithMaxLOBFileSize(paramtable.Get().CommonCfg.LOBMaxFileSize.GetAsInt64()),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create LOB writer for segment %d partition %d field %d", segmentID, partitionID, fieldID)
	}

	m.writers[key] = writer

	m.logger.Info("created new LOB writer",
		zap.Int64("segmentID", segmentID),
		zap.Int64("fieldID", fieldID),
	)

	return writer, nil
}

// FlushSegment flushes all LOB writers for a specific segment and processes pending LOBs.
// This method is called during segment sync to ensure all LOB data is persisted.
// Returns the LOB metadata for the segment (collected before writers are removed from map).
func (m *LOBManager) FlushSegment(ctx context.Context, segmentID int64) (*LOBSegmentMetadata, error) {
	if err := m.flushPendingLOBs(ctx, segmentID); err != nil {
		m.logger.Error("failed to flush pending LOBs",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return nil, errors.Wrap(err, "failed to flush pending LOBs")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	writersToClose := make([]*LOBWriter, 0)
	keysToRemove := make([]string, 0)

	for key, writer := range m.writers {
		if writer.segmentID == segmentID {
			writersToClose = append(writersToClose, writer)
			keysToRemove = append(keysToRemove, key)
		}
	}

	for _, writer := range writersToClose {
		if err := writer.Flush(ctx); err != nil {
			errs = append(errs, err)
			m.logger.Error("failed to flush LOB writer",
				zap.Int64("segmentID", writer.segmentID),
				zap.Int64("fieldID", writer.fieldID),
				zap.Error(err),
			)
		}
	}

	// collect metadata BEFORE removing writers from map
	metadata := m.getSegmentMetadataLocked(segmentID)

	// remove from map
	for _, key := range keysToRemove {
		delete(m.writers, key)
	}

	if len(errs) > 0 {
		return nil, errors.Newf("failed to flush %d LOB writers for segment %d", len(errs), segmentID)
	}

	m.logger.Info("flushed LOB writers for segment",
		zap.Int64("segmentID", segmentID),
		zap.Int("writerCount", len(writersToClose)),
	)

	return metadata, nil
}

// flushPendingLOBs writes all pending LOBs for a segment to LOB files (lazy mode)
func (m *LOBManager) flushPendingLOBs(ctx context.Context, segmentID int64) error {
	m.pendingMu.Lock()
	pendingList, exists := m.pendingLOBs[segmentID]
	if !exists || len(pendingList) == 0 {
		m.pendingMu.Unlock()
		return nil
	}
	delete(m.pendingLOBs, segmentID)
	m.pendingMu.Unlock()

	m.logger.Info("flushing pending LOBs (lazy mode)",
		zap.Int64("segmentID", segmentID),
		zap.Int("pendingCount", len(pendingList)))

	fieldGroups := make(map[int64][]*PendingLOB)
	for _, pending := range pendingList {
		fieldGroups[pending.fieldID] = append(fieldGroups[pending.fieldID], pending)
	}

	var errs []error
	for fieldID, pendingGroup := range fieldGroups {
		if err := m.flushPendingLOBsForField(ctx, segmentID, fieldID, pendingGroup); err != nil {
			errs = append(errs, err)
			m.logger.Error("failed to flush pending LOBs for field",
				zap.Int64("segmentID", segmentID),
				zap.Int64("fieldID", fieldID),
				zap.Int("pendingCount", len(pendingGroup)),
				zap.Error(err))
		}
	}

	if len(errs) > 0 {
		return errors.Newf("failed to flush pending LOBs for %d fields", len(errs))
	}

	return nil
}

// flushPendingLOBsForField writes pending LOBs for a specific field
func (m *LOBManager) flushPendingLOBsForField(ctx context.Context, segmentID int64, fieldID int64, pendingList []*PendingLOB) error {
	if len(pendingList) == 0 {
		return nil
	}

	partitionID := pendingList[0].partitionID

	writer, err := m.getOrCreateWriter(ctx, segmentID, partitionID, fieldID)
	if err != nil {
		return errors.Wrap(err, "failed to get LOB writer for pending LOBs")
	}

	for _, pending := range pendingList {
		ref, err := writer.WriteText(ctx, pending.data)
		if err != nil {
			return errors.Wrapf(err, "failed to write pending LOB at row %d", pending.rowIndex)
		}

		// Replace original text with reference (via pointer)
		encodedRef := EncodeLOBReference(ref)
		*pending.originalRef = string(encodedRef)

		m.logger.Debug("flushed pending LOB (lazy mode)",
			zap.Int64("segmentID", segmentID),
			zap.Int64("fieldID", fieldID),
			zap.Int("rowIndex", pending.rowIndex),
			zap.Int64("textSize", int64(len(pending.data))),
			zap.String("reference", ref.DebugString()))
	}

	m.logger.Info("flushed pending LOBs for field",
		zap.Int64("segmentID", segmentID),
		zap.Int64("fieldID", fieldID),
		zap.Int("count", len(pendingList)))

	return nil
}

// Close closes all LOB writers and releases resources
func (m *LOBManager) Close(ctx context.Context) error {
	m.pendingMu.Lock()
	pendingCount := 0
	for segmentID, pending := range m.pendingLOBs {
		count := len(pending)
		if count > 0 {
			pendingCount += count
			m.logger.Error("segment has unflushed pending LOBs",
				zap.Int64("segmentID", segmentID),
				zap.Int("pendingCount", count))
		}
	}
	m.pendingMu.Unlock()

	if pendingCount > 0 {
		return errors.Newf("cannot close LOB manager: %d pending LOBs not flushed, call FlushSegment first", pendingCount)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	for key, writer := range m.writers {
		if err := writer.Close(ctx); err != nil {
			errs = append(errs, err)
			m.logger.Error("failed to close LOB writer",
				zap.String("key", key),
				zap.Error(err),
			)
		}
	}

	m.writers = make(map[string]*LOBWriter)

	if len(errs) > 0 {
		return errors.Newf("failed to close %d LOB writers", len(errs))
	}

	m.logger.Info("closed all LOB writers",
		zap.Int("droppedPendingLOBs", pendingCount))
	return nil
}

// GetStatistics returns aggregated statistics from all LOB writers and pending LOBs
func (m *LOBManager) GetStatistics() map[string]interface{} {
	m.mu.RLock()
	totalFiles := 0
	totalRecords := int64(0)
	totalBytes := int64(0)

	for _, writer := range m.writers {
		stats := writer.GetStatistics()
		if files, ok := stats["total_lob_files"].(int); ok {
			totalFiles += files
		}
		if records, ok := stats["total_lob_records"].(int64); ok {
			totalRecords += records
		}
		if bytes, ok := stats["total_lob_bytes"].(int64); ok {
			totalBytes += bytes
		}
	}
	writerCount := len(m.writers)
	m.mu.RUnlock()

	m.pendingMu.RLock()
	totalPendingLOBs := 0
	totalPendingBytes := int64(0)
	for _, pendingList := range m.pendingLOBs {
		totalPendingLOBs += len(pendingList)
		for _, pending := range pendingList {
			totalPendingBytes += int64(len(pending.data))
		}
	}
	m.pendingMu.RUnlock()

	return map[string]interface{}{
		"size_threshold":      m.sizeThreshold,
		"lazy_write_max_size": m.lazyWriteMaxSize,
		"total_writers":       writerCount,
		"total_lob_files":     totalFiles,
		"total_lob_records":   totalRecords,
		"total_lob_bytes":     totalBytes,
		"pending_lobs":        totalPendingLOBs,
		"pending_lobs_bytes":  totalPendingBytes,
	}
}

// getWriterKey generates a unique key for a segment+field combination
func (m *LOBManager) getWriterKey(segmentID int64, fieldID int64) string {
	return fmt.Sprintf("%d:%d", segmentID, fieldID)
}

// GetSegmentMetadata returns LOB metadata for a specific segment
func (m *LOBManager) GetSegmentMetadata(segmentID int64) *LOBSegmentMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.getSegmentMetadataLocked(segmentID)
}

// getSegmentMetadataLocked returns LOB metadata for a specific segment.
// Caller must hold m.mu lock (read or write).
func (m *LOBManager) getSegmentMetadataLocked(segmentID int64) *LOBSegmentMetadata {
	metadata := NewLOBSegmentMetadata()

	// iterate through all writers to find those belonging to this segment
	for key, writer := range m.writers {
		var sid, fid int64
		fmt.Sscanf(key, "%d:%d", &sid, &fid)

		if sid != segmentID {
			continue
		}

		// get statistics from the writer
		stats := writer.GetStatistics()

		fieldMeta := &LOBFieldMetadata{
			FieldID:       fid,
			SizeThreshold: m.sizeThreshold,
		}

		if recordCount, ok := stats["total_lob_records"].(int64); ok {
			fieldMeta.RecordCount = recordCount
			metadata.TotalLOBRecords += recordCount
		}

		if totalBytes, ok := stats["total_lob_bytes"].(int64); ok {
			fieldMeta.TotalBytes = totalBytes
			metadata.TotalLOBBytes += totalBytes
		}

		if fileCount, ok := stats["total_lob_files"].(int); ok {
			metadata.TotalLOBFiles += fileCount
		}

		fieldMeta.LOBFiles = writer.GetLOBFileInfos()

		metadata.LOBFields[fid] = fieldMeta
	}

	if !metadata.HasLOBFields() {
		return nil
	}

	return metadata
}
