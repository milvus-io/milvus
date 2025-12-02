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
	"io"
	"path"
	"strings"

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

const (
	// LOB file size thresholds for compaction
	// Only used in SmartRewrite mode to decide if small files should be merged
	LOBTargetFileSize = 256 * 1024 * 1024 // 256MB - target size for merged files
	LOBMinMergeSize   = 64 * 1024 * 1024  // 64MB - merge files smaller than this
)

type LOBCompactionHelper struct {
	chunkManager  ChunkManager
	storageConfig *indexpb.StorageConfig
	arrowSchema   *arrow.Schema
	bucketName    string
	logger        *log.MLogger
}

func NewLOBCompactionHelper(chunkManager ChunkManager, storageConfig *indexpb.StorageConfig) *LOBCompactionHelper {
	bucketName := storageConfig.GetBucketName()
	if bucketName == "" {
		params := paramtable.Get()
		bucketName = params.MinioCfg.BucketName.GetValue()
	}

	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "text_data", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	return &LOBCompactionHelper{
		chunkManager:  chunkManager,
		storageConfig: storageConfig,
		arrowSchema:   arrowSchema,
		bucketName:    bucketName,
		logger:        log.With(zap.String("component", "LOBCompactionHelper")),
	}
}

type CompactionMode int

const (
	// CompactionModeRelocate relocates LOB files to new segment location
	// files are copied but NOT deleted (deletion happens later in GC phase)
	// this ensures transaction safety - if compaction fails, source files remain intact
	CompactionModeRelocate CompactionMode = iota

	// CompactionModeSmartRewrite intelligently rewrites LOB files, filtering unused rows
	// and building reference mapping
	CompactionModeSmartRewrite

	// CompactionModeSkip skips LOB files (for segments with no LOB data)
	CompactionModeSkip
)

type LOBReferenceMapping struct {
	mapping map[string]*LOBReference
}

func NewLOBReferenceMapping() *LOBReferenceMapping {
	return &LOBReferenceMapping{
		mapping: make(map[string]*LOBReference),
	}
}

func (m *LOBReferenceMapping) AddMapping(oldRef, newRef *LOBReference) {
	key := string(EncodeLOBReference(oldRef))
	m.mapping[key] = newRef
}

func (m *LOBReferenceMapping) GetNewReference(oldRef *LOBReference) *LOBReference {
	return m.mapping[string(EncodeLOBReference(oldRef))]
}

func (m *LOBReferenceMapping) UpdateReferenceColumn(encodedRefs []byte) ([]byte, error) {
	if len(encodedRefs)%LOBReferenceSize != 0 {
		return nil, errors.Newf("invalid reference column size: %d bytes (must be multiple of %d)", len(encodedRefs), LOBReferenceSize)
	}

	numRefs := len(encodedRefs) / LOBReferenceSize
	updated := make([]byte, len(encodedRefs))

	for i := 0; i < numRefs; i++ {
		offset := i * LOBReferenceSize
		oldRefBytes := encodedRefs[offset : offset+LOBReferenceSize]

		oldRef, err := DecodeLOBReference(oldRefBytes)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to decode reference at index %d", i)
		}

		newRef := m.GetNewReference(oldRef)
		if newRef == nil {
			copy(updated[offset:offset+LOBReferenceSize], oldRefBytes)
		} else {
			copy(updated[offset:offset+LOBReferenceSize], EncodeLOBReference(newRef))
		}
	}

	return updated, nil
}

func (m *LOBReferenceMapping) Size() int {
	return len(m.mapping)
}

type LOBReferenceSet struct {
	// key: string representation of encoded reference (16 bytes)
	references map[string]*LOBReference
}

func NewLOBReferenceSet() *LOBReferenceSet {
	return &LOBReferenceSet{
		references: make(map[string]*LOBReference),
	}
}

func (s *LOBReferenceSet) Add(ref *LOBReference) {
	key := string(EncodeLOBReference(ref))
	s.references[key] = ref
}

func (s *LOBReferenceSet) Contains(ref *LOBReference) bool {
	_, exists := s.references[string(EncodeLOBReference(ref))]
	return exists
}

func (s *LOBReferenceSet) GetReferencesForFile(lobFileID uint64) []*LOBReference {
	result := make([]*LOBReference, 0)
	for _, ref := range s.references {
		if ref.LobFileID == lobFileID {
			result = append(result, ref)
		}
	}
	return result
}

func (s *LOBReferenceSet) Size() int {
	return len(s.references)
}

func (s *LOBReferenceSet) AddFromColumn(encodedRefs []byte) error {
	if len(encodedRefs)%LOBReferenceSize != 0 {
		return errors.Newf("invalid reference column size: %d bytes", len(encodedRefs))
	}

	numRefs := len(encodedRefs) / LOBReferenceSize
	for i := 0; i < numRefs; i++ {
		offset := i * LOBReferenceSize
		refBytes := encodedRefs[offset : offset+LOBReferenceSize]

		ref, err := DecodeLOBReference(refBytes)
		if err != nil {
			return errors.Wrapf(err, "failed to decode reference at index %d", i)
		}

		// only add valid references
		if ref.IsValid() {
			s.Add(ref)
		}
	}

	return nil
}

type LOBCompactionTask struct {
	SourceSegmentID int64
	TargetSegmentID int64
	PartitionID     int64
	CollectionID    int64
	FieldID         int64
	LobFileID       uint64

	SourcePath string
	TargetPath string

	Mode CompactionMode
}

type LOBCompactionResult struct {
	Metadata         *LOBSegmentMetadata
	ReferenceMapping *LOBReferenceMapping
}

// CompactLOBFiles handles LOB file operations during segment compaction
// It takes source segments' metadata and produces a new metadata for the target segment
// Returns both metadata and reference mapping (for updating reference columns)
// referenceSet: optional set of actually used references (for SmartRewrite mode)
// lobFileIDAlloc: allocator for generating new LOB file IDs (required for SmartRewrite mode)
func (h *LOBCompactionHelper) CompactLOBFiles(
	ctx context.Context,
	sourceSegments []*LOBSegmentMetadata,
	targetSegmentID int64,
	partitionID int64,
	collectionID int64,
	rootPath string,
	mode CompactionMode,
	referenceSet *LOBReferenceSet,
	lobFileIDAlloc allocator.Interface,
) (*LOBCompactionResult, error) {
	if mode == CompactionModeSkip {
		return &LOBCompactionResult{
			Metadata:         NewLOBSegmentMetadata(),
			ReferenceMapping: NewLOBReferenceMapping(),
		}, nil
	}

	h.logger.Info("starting LOB compaction",
		zap.Int("sourceSegmentCount", len(sourceSegments)),
		zap.Int64("targetSegmentID", targetSegmentID),
		zap.String("mode", h.getModeName(mode)),
	)

	targetMetadata := NewLOBSegmentMetadata()
	refMapping := NewLOBReferenceMapping()

	for _, sourceMeta := range sourceSegments {
		if sourceMeta == nil || !sourceMeta.HasLOBFields() {
			continue
		}

		for fieldID, fieldMeta := range sourceMeta.LOBFields {
			if err := h.compactFieldLOBFiles(
				ctx,
				fieldMeta,
				targetSegmentID,
				partitionID,
				collectionID,
				fieldID,
				rootPath,
				mode,
				targetMetadata,
				refMapping,
				referenceSet,
				lobFileIDAlloc,
			); err != nil {
				return nil, errors.Wrapf(err, "failed to compact LOB files for field %d", fieldID)
			}
		}
	}

	h.logger.Info("completed LOB compaction",
		zap.Int64("targetSegmentID", targetSegmentID),
		zap.Int("totalLOBFiles", targetMetadata.TotalLOBFiles),
		zap.Int64("totalLOBRecords", targetMetadata.TotalLOBRecords),
		zap.Int64("totalLOBBytes", targetMetadata.TotalLOBBytes),
		zap.Int("referenceMappings", refMapping.Size()),
	)

	return &LOBCompactionResult{
		Metadata:         targetMetadata,
		ReferenceMapping: refMapping,
	}, nil
}

func (h *LOBCompactionHelper) compactFieldLOBFiles(
	ctx context.Context,
	sourceFieldMeta *LOBFieldMetadata,
	targetSegmentID int64,
	partitionID int64,
	collectionID int64,
	fieldID int64,
	rootPath string,
	mode CompactionMode,
	targetMetadata *LOBSegmentMetadata,
	refMapping *LOBReferenceMapping,
	referenceSet *LOBReferenceSet,
	lobFileIDAlloc allocator.Interface,
) error {
	targetFieldMeta, exists := targetMetadata.LOBFields[fieldID]
	if !exists {
		targetFieldMeta = &LOBFieldMetadata{
			FieldID:       fieldID,
			LOBFiles:      make([]string, 0),
			SizeThreshold: sourceFieldMeta.SizeThreshold,
		}
	}

	if mode == CompactionModeRelocate {
		return h.relocateFieldLOBFiles(ctx, sourceFieldMeta, targetSegmentID, partitionID, collectionID, fieldID, rootPath, targetFieldMeta)
	} else if mode == CompactionModeSmartRewrite {
		return h.smartRewriteFieldLOBFiles(ctx, sourceFieldMeta, targetSegmentID, partitionID, collectionID, fieldID, rootPath, targetFieldMeta, refMapping, referenceSet, lobFileIDAlloc)
	}

	return errors.Newf("unsupported compaction mode: %d", mode)
}

func (h *LOBCompactionHelper) relocateFieldLOBFiles(
	ctx context.Context,
	sourceFieldMeta *LOBFieldMetadata,
	targetSegmentID int64,
	partitionID int64,
	collectionID int64,
	fieldID int64,
	rootPath string,
	targetFieldMeta *LOBFieldMetadata,
) error {
	for _, sourceLOBFile := range sourceFieldMeta.LOBFiles {
		lobFileID, err := metautil.ExtractLOBFileID(sourceLOBFile)
		if err != nil {
			h.logger.Warn("failed to extract LOB file ID, skipping",
				zap.String("path", sourceLOBFile),
				zap.Error(err),
			)
			continue
		}

		sourcePath := path.Join(rootPath, sourceLOBFile)
		targetPath := metautil.BuildLOBLogPath(rootPath, collectionID, partitionID, targetSegmentID, fieldID, typeutil.UniqueID(lobFileID))

		if err := h.relocateLOBFile(ctx, sourcePath, targetPath); err != nil {
			return errors.Wrapf(err, "failed to relocate LOB file from %s to %s", sourcePath, targetPath)
		}

		relativePath := h.getRelativeLOBPath(targetPath, rootPath)
		targetFieldMeta.LOBFiles = append(targetFieldMeta.LOBFiles, relativePath)
	}

	return nil
}

func (h *LOBCompactionHelper) smartRewriteFieldLOBFiles(
	ctx context.Context,
	sourceFieldMeta *LOBFieldMetadata,
	targetSegmentID int64,
	partitionID int64,
	collectionID int64,
	fieldID int64,
	rootPath string,
	targetFieldMeta *LOBFieldMetadata,
	refMapping *LOBReferenceMapping,
	referenceSet *LOBReferenceSet,
	lobFileIDAlloc allocator.Interface,
) error {
	var lobFiles []struct {
		path   string
		fileID uint64
		size   int64
	}
	var totalSize int64

	for _, sourceLOBFile := range sourceFieldMeta.LOBFiles {
		lobFileID, err := metautil.ExtractLOBFileID(sourceLOBFile)
		if err != nil {
			h.logger.Warn("failed to extract LOB file ID, skipping",
				zap.String("path", sourceLOBFile),
				zap.Error(err),
			)
			continue
		}

		sourcePath := path.Join(rootPath, sourceLOBFile)
		fileSize, err := h.getFileSize(ctx, sourcePath)
		if err != nil {
			h.logger.Warn("failed to get file size, skipping",
				zap.String("path", sourcePath),
				zap.Error(err),
			)
			continue
		}

		lobFiles = append(lobFiles, struct {
			path   string
			fileID uint64
			size   int64
		}{
			path:   sourceLOBFile,
			fileID: lobFileID,
			size:   fileSize,
		})
		totalSize += fileSize
	}

	shouldMerge := len(lobFiles) > 1 && h.shouldMergeFiles(lobFiles, totalSize)

	if shouldMerge {
		h.logger.Info("merging LOB files with smart rewrite",
			zap.Int64("fieldID", fieldID),
			zap.Int("fileCount", len(lobFiles)),
			zap.Int64("totalSize", totalSize),
		)
		return h.mergeLOBFilesWithFilter(ctx, lobFiles, targetSegmentID, partitionID, collectionID, fieldID, rootPath, CompactionModeSmartRewrite, targetFieldMeta, refMapping, referenceSet, lobFileIDAlloc)
	}

	// Process each file individually
	for _, fileInfo := range lobFiles {
		lobFileID := fileInfo.fileID
		sourceLOBFile := fileInfo.path

		sourcePath := path.Join(rootPath, sourceLOBFile)

		// Allocate new LOB file ID using allocator (not arithmetic encoding)
		newLobFileIDRaw, err := lobFileIDAlloc.AllocOne()
		if err != nil {
			return errors.Wrap(err, "failed to allocate new LOB file ID")
		}
		newLobFileID := uint64(newLobFileIDRaw)

		targetPath := metautil.BuildLOBLogPath(rootPath, collectionID, partitionID, targetSegmentID, fieldID, typeutil.UniqueID(newLobFileID))

		if err := h.smartRewriteLOBFile(ctx, sourcePath, targetPath, lobFileID, newLobFileID, CompactionModeSmartRewrite, refMapping, referenceSet); err != nil {
			return errors.Wrapf(err, "failed to smart rewrite LOB file from %s to %s", sourcePath, targetPath)
		}

		relativePath := h.getRelativeLOBPath(targetPath, rootPath)
		targetFieldMeta.LOBFiles = append(targetFieldMeta.LOBFiles, relativePath)
	}

	return nil
}

// relocateLOBFile relocates a LOB file from source to target using Storage V2
// IMPORTANT: Only copies the file, does NOT delete source
// Source deletion happens in GC phase after compaction metadata is committed
func (h *LOBCompactionHelper) relocateLOBFile(ctx context.Context, sourcePath, targetPath string) error {
	h.logger.Debug("relocating LOB file using Storage V2",
		zap.String("source", sourcePath),
		zap.String("target", targetPath),
	)

	return h.transferLOBFileV2(ctx, sourcePath, targetPath)
}

// transferLOBFileV2 transfers a LOB file using Storage V2 PackedReader/PackedWriter
// Uses streaming copy (read-one-write-one) to minimize memory usage
func (h *LOBCompactionHelper) transferLOBFileV2(ctx context.Context, sourcePath, targetPath string) error {
	exists, err := h.chunkManager.Exist(ctx, sourcePath)
	if err != nil {
		return errors.Wrap(err, "failed to check source file existence")
	}
	if !exists {
		return errors.Newf("source file does not exist: %s", sourcePath)
	}

	storageType := h.storageConfig.GetStorageType()
	sourceReadPath := sourcePath
	if storageType != "local" {
		sourceReadPath = path.Join(h.bucketName, sourcePath)
	}

	targetWritePath := targetPath
	if storageType != "local" {
		targetWritePath = path.Join(h.bucketName, targetPath)
	}

	reader, err := packed.NewPackedReader([]string{sourceReadPath}, h.arrowSchema, 16*1024*1024, h.storageConfig, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create packed reader for source LOB file")
	}
	defer reader.Close()

	columnGroup := storagecommon.ColumnGroup{
		GroupID: typeutil.UniqueID(0),
		Fields:  []int64{0},
	}

	writer, err := packed.NewPackedWriter(
		[]string{targetWritePath},
		h.arrowSchema,
		16*1024*1024, // 16MB buffer
		10*1024*1024, // 10MB multipart upload
		[]storagecommon.ColumnGroup{columnGroup},
		h.storageConfig,
		nil, // no encryption context
	)
	if err != nil {
		return errors.Wrap(err, "failed to create packed writer for target LOB file")
	}
	defer func() {
		if err := writer.Close(); err != nil {
			h.logger.Warn("failed to close writer", zap.Error(err))
		}
	}()

	// streaming copy: read one batch, write one batch
	recordCount := 0
	for {
		record, err := reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "failed to read record batch from source")
		}
		if record == nil {
			break
		}

		// write immediately and release - no buffering
		if err := writer.WriteRecordBatch(record); err != nil {
			record.Release()
			return errors.Wrap(err, "failed to write record batch to target")
		}
		record.Release()
		recordCount++
	}

	if recordCount == 0 {
		h.logger.Warn("source LOB file is empty", zap.String("source", sourcePath))
		return nil
	}

	h.logger.Debug("successfully transferred LOB file using Storage V2 (streaming)",
		zap.String("source", sourcePath),
		zap.String("target", targetPath),
		zap.Int("recordBatches", recordCount),
	)

	return nil
}

// smartRewriteLOBFile intelligently rewrites a LOB file, filtering out unused rows
// and building reference mapping from old to new references
func (h *LOBCompactionHelper) smartRewriteLOBFile(
	ctx context.Context,
	sourcePath string,
	targetPath string,
	oldLobFileID uint64,
	newLobFileID uint64,
	mode CompactionMode,
	refMapping *LOBReferenceMapping,
	referenceSet *LOBReferenceSet,
) error {
	h.logger.Info("smart rewriting LOB file",
		zap.String("source", sourcePath),
		zap.String("target", targetPath),
		zap.Uint64("oldFileID", oldLobFileID),
		zap.Uint64("newFileID", newLobFileID),
	)

	exists, err := h.chunkManager.Exist(ctx, sourcePath)
	if err != nil {
		return errors.Wrap(err, "failed to check source file existence")
	}
	if !exists {
		return errors.Newf("source file does not exist: %s", sourcePath)
	}

	refsForFile := referenceSet.GetReferencesForFile(oldLobFileID)
	if len(refsForFile) == 0 {
		h.logger.Info("no references found for LOB file, skipping",
			zap.Uint64("lobFileID", oldLobFileID))
		return nil
	}

	usedRowOffsets := make(map[uint32]bool)
	for _, ref := range refsForFile {
		usedRowOffsets[ref.RowOffset] = true
	}

	h.logger.Info("filtering LOB file",
		zap.Uint64("lobFileID", oldLobFileID),
		zap.Int("totalRefs", len(refsForFile)),
		zap.Int("uniqueRows", len(usedRowOffsets)),
	)

	storageType := h.storageConfig.GetStorageType()
	sourceReadPath := sourcePath
	if storageType != "local" {
		sourceReadPath = path.Join(h.bucketName, sourcePath)
	}

	reader, err := packed.NewPackedReader([]string{sourceReadPath}, h.arrowSchema, 16*1024*1024, h.storageConfig, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create packed reader for source LOB file")
	}
	defer reader.Close()

	targetWritePath := targetPath
	if storageType != "local" {
		targetWritePath = path.Join(h.bucketName, targetPath)
	}

	columnGroup := storagecommon.ColumnGroup{
		GroupID: typeutil.UniqueID(0),
		Fields:  []int64{0},
	}

	writer, err := packed.NewPackedWriter(
		[]string{targetWritePath},
		h.arrowSchema,
		16*1024*1024,
		10*1024*1024,
		[]storagecommon.ColumnGroup{columnGroup},
		h.storageConfig,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create packed writer for target LOB file")
	}
	defer func() {
		if err := writer.Close(); err != nil {
			h.logger.Warn("failed to close writer", zap.Error(err))
		}
	}()

	var newRowOffset uint32 = 0
	var currentRowOffset uint32 = 0
	var totalOriginalRows uint32 = 0
	oldToNewRowOffset := make(map[uint32]uint32)
	batchesWritten := 0

	for {
		record, err := reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "failed to read record batch from source")
		}
		if record == nil {
			break
		}

		numRows := int(record.NumRows())
		totalOriginalRows += uint32(numRows)
		keepIndices := make([]int64, 0, numRows)

		// identify rows to keep
		for i := 0; i < numRows; i++ {
			if usedRowOffsets[currentRowOffset] {
				keepIndices = append(keepIndices, int64(i))
				oldToNewRowOffset[currentRowOffset] = newRowOffset
				newRowOffset++
			}
			currentRowOffset++
		}

		// skip empty batches
		if len(keepIndices) == 0 {
			record.Release()
			continue
		}

		var filteredRecord arrow.Record

		// optimization: if all rows are kept, use original record
		if len(keepIndices) == numRows {
			filteredRecord = record
		} else {
			// filter record to only keep used rows
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			textColumn := record.Column(0).(*array.String) // text_data column
			for _, idx := range keepIndices {
				builder.Append(textColumn.Value(int(idx)))
			}

			filteredArray := builder.NewStringArray()
			builder.Release()

			filteredRecord = array.NewRecord(h.arrowSchema, []arrow.Array{filteredArray}, int64(len(keepIndices)))
			filteredArray.Release()
			record.Release()
		}

		// write immediately - no buffering
		if err := writer.WriteRecordBatch(filteredRecord); err != nil {
			filteredRecord.Release()
			return errors.Wrap(err, "failed to write filtered record batch to target")
		}
		filteredRecord.Release()
		batchesWritten++
	}

	if batchesWritten == 0 {
		h.logger.Warn("all rows filtered out, no data to write",
			zap.Uint64("oldFileID", oldLobFileID))
		return nil
	}

	for oldRowOffset, newRowOffset := range oldToNewRowOffset {
		oldRef := NewLOBReference(oldLobFileID, oldRowOffset)
		newRef := NewLOBReference(newLobFileID, newRowOffset)
		refMapping.AddMapping(oldRef, newRef)
	}

	h.logger.Info("smart rewrite completed (streaming)",
		zap.String("source", sourcePath),
		zap.String("target", targetPath),
		zap.Uint64("oldFileID", oldLobFileID),
		zap.Uint64("newFileID", newLobFileID),
		zap.Int("originalRows", int(totalOriginalRows)),
		zap.Int("filteredRows", int(newRowOffset)),
		zap.Int("batchesWritten", batchesWritten),
		zap.Int("referenceMappings", len(oldToNewRowOffset)),
		zap.Float64("compressionRatio", float64(totalOriginalRows)/float64(newRowOffset)),
	)

	return nil
}

func (h *LOBCompactionHelper) getRelativeLOBPath(fullPath, rootPath string) string {
	relativePath := strings.TrimPrefix(fullPath, rootPath)
	relativePath = strings.TrimPrefix(relativePath, "/")
	return relativePath
}

func (h *LOBCompactionHelper) getModeName(mode CompactionMode) string {
	switch mode {
	case CompactionModeRelocate:
		return "relocate"
	case CompactionModeSmartRewrite:
		return "smart_rewrite"
	case CompactionModeSkip:
		return "skip"
	default:
		return "unknown"
	}
}

func MergeLOBMetadata(sourceSegments []*LOBSegmentMetadata) *LOBSegmentMetadata {
	merged := NewLOBSegmentMetadata()

	for _, sourceMeta := range sourceSegments {
		if sourceMeta == nil || !sourceMeta.HasLOBFields() {
			continue
		}

		for fieldID, fieldMeta := range sourceMeta.LOBFields {
			targetFieldMeta, exists := merged.LOBFields[fieldID]
			if !exists {
				targetFieldMeta = &LOBFieldMetadata{
					FieldID:       fieldMeta.FieldID,
					LOBFiles:      make([]string, len(fieldMeta.LOBFiles)),
					SizeThreshold: fieldMeta.SizeThreshold,
					RecordCount:   fieldMeta.RecordCount,
					TotalBytes:    fieldMeta.TotalBytes,
				}
				copy(targetFieldMeta.LOBFiles, fieldMeta.LOBFiles)
				merged.LOBFields[fieldID] = targetFieldMeta
			} else {
				targetFieldMeta.LOBFiles = append(targetFieldMeta.LOBFiles, fieldMeta.LOBFiles...)
				targetFieldMeta.RecordCount += fieldMeta.RecordCount
				targetFieldMeta.TotalBytes += fieldMeta.TotalBytes
			}
		}
	}

	for _, fieldMeta := range merged.LOBFields {
		merged.TotalLOBFiles += len(fieldMeta.LOBFiles)
		merged.TotalLOBRecords += fieldMeta.RecordCount
		merged.TotalLOBBytes += fieldMeta.TotalBytes
	}

	return merged
}

func (h *LOBCompactionHelper) ValidateLOBCompaction(
	ctx context.Context,
	targetMetadata *LOBSegmentMetadata,
	rootPath string,
) error {
	if !targetMetadata.HasLOBFields() {
		return nil
	}

	h.logger.Info("validating LOB compaction",
		zap.Int("fieldCount", len(targetMetadata.LOBFields)),
		zap.Int("totalFiles", targetMetadata.TotalLOBFiles),
	)

	var errs []error

	for fieldID, fieldMeta := range targetMetadata.LOBFields {
		for _, lobFile := range fieldMeta.LOBFiles {
			fullPath := path.Join(rootPath, lobFile)
			exists, err := h.chunkManager.Exist(ctx, fullPath)
			if err != nil {
				errs = append(errs, errors.Wrapf(err, "failed to check existence of file %s", fullPath))
				continue
			}
			if !exists {
				errs = append(errs, errors.Newf("LOB file does not exist: %s (field %d)", fullPath, fieldID))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Newf("validation failed with %d errors", len(errs))
	}

	h.logger.Info("LOB compaction validation successful")
	return nil
}

func (h *LOBCompactionHelper) getFileSize(ctx context.Context, filePath string) (int64, error) {
	size, err := h.chunkManager.Size(ctx, filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get size of file %s", filePath)
	}
	return size, nil
}

// shouldMergeFiles determines if LOB files should be merged based on their sizes
func (h *LOBCompactionHelper) shouldMergeFiles(files []struct {
	path   string
	fileID uint64
	size   int64
}, totalSize int64,
) bool {
	if len(files) <= 1 {
		return false
	}

	smallFileCount := 0
	for _, f := range files {
		if f.size < LOBMinMergeSize {
			smallFileCount++
		}
	}

	if smallFileCount > len(files)/2 {
		return true
	}

	if totalSize < LOBTargetFileSize {
		return true
	}

	return false
}

// mergeLOBFilesWithFilter merges multiple LOB files into one or more target files
// Each target file will be around LOBTargetFileSize (256MB)
// In SmartRewrite mode, also filters out unused rows based on referenceSet
func (h *LOBCompactionHelper) mergeLOBFilesWithFilter(
	ctx context.Context,
	sourceFiles []struct {
		path   string
		fileID uint64
		size   int64
	},
	targetSegmentID int64,
	partitionID int64,
	collectionID int64,
	fieldID int64,
	rootPath string,
	mode CompactionMode,
	targetFieldMeta *LOBFieldMetadata,
	refMapping *LOBReferenceMapping,
	referenceSet *LOBReferenceSet,
	lobFileIDAlloc allocator.Interface,
) error {
	type mergeGroup struct {
		files []struct {
			path   string
			fileID uint64
			size   int64
		}
		totalSize int64
	}

	var groups []mergeGroup
	currentGroup := mergeGroup{files: make([]struct {
		path   string
		fileID uint64
		size   int64
	}, 0)}

	for _, file := range sourceFiles {
		if currentGroup.totalSize > 0 && currentGroup.totalSize+file.size > LOBTargetFileSize {
			groups = append(groups, currentGroup)
			currentGroup = mergeGroup{files: make([]struct {
				path   string
				fileID uint64
				size   int64
			}, 0)}
		}

		currentGroup.files = append(currentGroup.files, file)
		currentGroup.totalSize += file.size
	}

	if len(currentGroup.files) > 0 {
		groups = append(groups, currentGroup)
	}

	h.logger.Info("merge plan created",
		zap.Int("sourceFiles", len(sourceFiles)),
		zap.Int("targetFiles", len(groups)),
	)

	for groupIdx, group := range groups {
		newLobFileIDRaw, err := lobFileIDAlloc.AllocOne()
		if err != nil {
			return errors.Wrapf(err, "failed to allocate LOB file ID for group %d", groupIdx)
		}
		newLobFileID := uint64(newLobFileIDRaw)

		targetPath := metautil.BuildLOBLogPath(rootPath, collectionID, partitionID, targetSegmentID, fieldID, typeutil.UniqueID(newLobFileID))

		h.logger.Info("merging LOB files group",
			zap.Int("groupIndex", groupIdx),
			zap.Int("fileCount", len(group.files)),
			zap.Int64("totalSize", group.totalSize),
			zap.Uint64("newLobFileID", newLobFileID),
		)

		if err := h.mergeFileGroupWithFilter(ctx, group.files, targetPath, newLobFileID, rootPath, refMapping, referenceSet); err != nil {
			return errors.Wrapf(err, "failed to merge file group %d", groupIdx)
		}

		relativePath := h.getRelativeLOBPath(targetPath, rootPath)
		targetFieldMeta.LOBFiles = append(targetFieldMeta.LOBFiles, relativePath)
	}

	return nil
}

// mergeFileGroupWithFilter merges multiple LOB files into a single target file
// Filters out unused rows based on referenceSet
func (h *LOBCompactionHelper) mergeFileGroupWithFilter(
	ctx context.Context,
	sourceFiles []struct {
		path   string
		fileID uint64
		size   int64
	},
	targetPath string,
	newLobFileID uint64,
	rootPath string,
	refMapping *LOBReferenceMapping,
	referenceSet *LOBReferenceSet,
) error {
	storageType := h.storageConfig.GetStorageType()

	targetWritePath := targetPath
	if storageType != "local" {
		targetWritePath = path.Join(h.bucketName, targetPath)
	}

	columnGroup := storagecommon.ColumnGroup{
		GroupID: typeutil.UniqueID(newLobFileID),
		Columns: []int{0},
		Fields:  []int64{0},
	}

	writer, err := packed.NewPackedWriter(
		[]string{targetWritePath},
		h.arrowSchema,
		16*1024*1024,
		10*1024*1024,
		[]storagecommon.ColumnGroup{columnGroup},
		h.storageConfig,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create packed writer for merged LOB file")
	}
	defer writer.Close()

	var newRowOffset uint32 = 0
	var totalInputRows, totalOutputRows uint32 = 0, 0

	for _, sourceFile := range sourceFiles {
		sourcePath := path.Join(rootPath, sourceFile.path)
		sourceReadPath := sourcePath
		if storageType != "local" {
			sourceReadPath = path.Join(h.bucketName, sourcePath)
		}

		usedRowOffsets := make(map[uint32]bool)
		refsForFile := referenceSet.GetReferencesForFile(sourceFile.fileID)
		for _, ref := range refsForFile {
			usedRowOffsets[ref.RowOffset] = true
		}

		reader, err := packed.NewPackedReader([]string{sourceReadPath}, h.arrowSchema, 16*1024*1024, h.storageConfig, nil)
		if err != nil {
			return errors.Wrapf(err, "failed to create reader for source file %s", sourcePath)
		}

		var oldRowOffset uint32 = 0
		for {
			record, err := reader.ReadNext()
			if err != nil {
				if err == io.EOF {
					break
				}
				reader.Close()
				return errors.Wrap(err, "failed to read record from source")
			}
			if record == nil {
				break
			}

			numRows := int(record.NumRows())
			keepIndices := make([]int64, 0, numRows)
			oldToNewMapping := make(map[uint32]uint32)

			for i := 0; i < numRows; i++ {
				currentOldRow := oldRowOffset + uint32(i)
				if usedRowOffsets[currentOldRow] {
					keepIndices = append(keepIndices, int64(i))
					oldToNewMapping[currentOldRow] = newRowOffset + uint32(len(keepIndices)-1)
				}
			}

			totalInputRows += uint32(numRows)

			if len(keepIndices) == 0 {
				oldRowOffset += uint32(numRows)
				record.Release()
				continue
			}

			var filteredRecord arrow.Record
			if len(keepIndices) == numRows {
				filteredRecord = record
			} else {
				builder := array.NewStringBuilder(memory.DefaultAllocator)
				defer builder.Release()

				textArray := record.Column(0).(*array.String)
				for _, idx := range keepIndices {
					builder.Append(textArray.Value(int(idx)))
				}

				filteredArray := builder.NewStringArray()
				defer filteredArray.Release()

				filteredRecord = array.NewRecord(h.arrowSchema, []arrow.Array{filteredArray}, int64(len(keepIndices)))
			}

			if err := writer.WriteRecordBatch(filteredRecord); err != nil {
				if filteredRecord != record {
					filteredRecord.Release()
				}
				record.Release()
				reader.Close()
				return errors.Wrap(err, "failed to write filtered merged record batch")
			}

			for oldRow, newRow := range oldToNewMapping {
				oldRef := NewLOBReference(sourceFile.fileID, oldRow)
				newRef := NewLOBReference(newLobFileID, newRow)
				refMapping.AddMapping(oldRef, newRef)
			}

			totalOutputRows += uint32(len(keepIndices))
			newRowOffset += uint32(len(keepIndices))
			oldRowOffset += uint32(numRows)

			if filteredRecord != record {
				filteredRecord.Release()
			}
			record.Release()
		}

		reader.Close()
	}

	h.logger.Info("merge with filter completed",
		zap.String("targetPath", targetPath),
		zap.Uint64("newLobFileID", newLobFileID),
		zap.Uint32("inputRows", totalInputRows),
		zap.Uint32("outputRows", totalOutputRows),
		zap.Float64("filterRatio", float64(totalOutputRows)/float64(totalInputRows)),
	)

	return nil
}
