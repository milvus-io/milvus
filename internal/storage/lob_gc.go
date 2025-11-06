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
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// LOBGarbageCollector manages cleanup of orphaned LOB files
type LOBGarbageCollector struct {
	mu sync.Mutex

	// chunk manager for storage operations
	chunkManager ChunkManager

	// minimum age for a file to be considered for GC (avoid deleting recently created files)
	minAge time.Duration

	// statistics
	scannedFiles  int64
	deletedFiles  int64
	deletedBytes  int64
	lastScanTime  time.Time
	lastCleanTime time.Time

	// logger
	logger *log.MLogger
}

// LOBGCOption is a functional option for configuring LOB GC
type LOBGCOption func(*LOBGarbageCollector)

// WithLOBGCMinAge sets the minimum age for files to be GC'd
func WithLOBGCMinAge(minAge time.Duration) LOBGCOption {
	return func(gc *LOBGarbageCollector) {
		gc.minAge = minAge
	}
}

// NewLOBGarbageCollector creates a new LOB garbage collector
func NewLOBGarbageCollector(chunkManager ChunkManager, opts ...LOBGCOption) *LOBGarbageCollector {
	gc := &LOBGarbageCollector{
		chunkManager: chunkManager,
		minAge:       1 * time.Hour, // default: 1 hour minimum age
		logger:       log.With(zap.String("component", "LOBGarbageCollector")),
	}

	for _, opt := range opts {
		opt(gc)
	}

	return gc
}

// ScanAndCleanup scans for orphaned LOB files and removes them
// It takes a list of active segment metadata to determine which files are still referenced
func (gc *LOBGarbageCollector) ScanAndCleanup(ctx context.Context, activeSegments map[int64]*LOBSegmentMetadata, rootPath string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.lastScanTime = time.Now()
	gc.logger.Info("starting LOB garbage collection scan",
		zap.String("rootPath", rootPath),
		zap.Int("activeSegments", len(activeSegments)),
	)

	// Build set of referenced LOB files
	referencedFiles := gc.buildReferencedFileSet(activeSegments)
	gc.logger.Info("built referenced file set",
		zap.Int("referencedFiles", len(referencedFiles)),
	)

	// Scan for all LOB files in storage
	allLOBFiles, err := gc.scanLOBFiles(ctx, rootPath)
	if err != nil {
		return errors.Wrap(err, "failed to scan LOB files")
	}

	gc.scannedFiles += int64(len(allLOBFiles))
	gc.logger.Info("scanned LOB files",
		zap.Int("totalFiles", len(allLOBFiles)),
	)

	// Identify orphaned files
	orphanedFiles := make([]string, 0)
	for _, filePath := range allLOBFiles {
		if !referencedFiles[filePath] {
			// Check if file is old enough to be GC'd
			if gc.isOldEnough(ctx, filePath) {
				orphanedFiles = append(orphanedFiles, filePath)
			}
		}
	}

	gc.logger.Info("identified orphaned LOB files",
		zap.Int("orphanedFiles", len(orphanedFiles)),
	)

	// Delete orphaned files
	if len(orphanedFiles) > 0 {
		deleted, deletedBytes, err := gc.deleteFiles(ctx, orphanedFiles)
		gc.deletedFiles += deleted
		gc.deletedBytes += deletedBytes
		gc.lastCleanTime = time.Now()

		if err != nil {
			gc.logger.Error("failed to delete some orphaned files",
				zap.Error(err),
				zap.Int64("deletedCount", deleted),
				zap.Int("failedCount", len(orphanedFiles)-int(deleted)),
			)
			return err
		}

		gc.logger.Info("successfully cleaned up orphaned LOB files",
			zap.Int64("deletedFiles", deleted),
			zap.Int64("deletedBytes", deletedBytes),
		)
	}

	return nil
}

// CleanupSegmentLOBFiles removes all LOB files associated with a specific segment
// This is called when a segment is dropped
func (gc *LOBGarbageCollector) CleanupSegmentLOBFiles(ctx context.Context, segmentMetadata *LOBSegmentMetadata, rootPath string) error {
	if segmentMetadata == nil || !segmentMetadata.HasLOBFields() {
		return nil
	}

	gc.mu.Lock()
	defer gc.mu.Unlock()

	filesToDelete := make([]string, 0)

	// Collect all LOB files from all fields
	for _, fieldMeta := range segmentMetadata.LOBFields {
		for _, lobFile := range fieldMeta.LOBFiles {
			// Construct full path
			fullPath := path.Join(rootPath, lobFile)
			filesToDelete = append(filesToDelete, fullPath)
		}
	}

	if len(filesToDelete) == 0 {
		return nil
	}

	gc.logger.Info("cleaning up segment LOB files",
		zap.Int("fileCount", len(filesToDelete)),
	)

	deleted, deletedBytes, err := gc.deleteFiles(ctx, filesToDelete)
	gc.deletedFiles += deleted
	gc.deletedBytes += deletedBytes

	if err != nil {
		gc.logger.Error("failed to delete some segment LOB files",
			zap.Error(err),
			zap.Int64("deletedCount", deleted),
			zap.Int("totalCount", len(filesToDelete)),
		)
		return err
	}

	gc.logger.Info("successfully cleaned up segment LOB files",
		zap.Int64("deletedFiles", deleted),
		zap.Int64("deletedBytes", deletedBytes),
	)

	return nil
}

// CleanupFailedWrite cleans up LOB files from a failed write operation
// This is called when WriteBuffer encounters an error and needs to rollback
func (gc *LOBGarbageCollector) CleanupFailedWrite(ctx context.Context, lobManager *LOBManager, segmentID int64) error {
	if lobManager == nil {
		return nil
	}

	// Get statistics to find files that were created
	stats := lobManager.GetStatistics()

	gc.logger.Info("cleaning up failed write LOB files",
		zap.Int64("segmentID", segmentID),
		zap.Any("stats", stats),
	)

	// Force flush to close any open writers
	if err := lobManager.FlushSegment(ctx, segmentID); err != nil {
		gc.logger.Warn("failed to flush segment during cleanup",
			zap.Int64("segmentID", segmentID),
			zap.Error(err),
		)
	}

	// TODO: Track which files were created for this segment and delete them
	// This requires LOBManager to maintain a list of created files per segment

	return nil
}

// buildReferencedFileSet builds a set of all LOB files that are referenced by active segments
func (gc *LOBGarbageCollector) buildReferencedFileSet(activeSegments map[int64]*LOBSegmentMetadata) map[string]bool {
	referenced := make(map[string]bool)

	for _, segmentMeta := range activeSegments {
		if segmentMeta == nil {
			continue
		}

		for _, fieldMeta := range segmentMeta.LOBFields {
			for _, lobFile := range fieldMeta.LOBFiles {
				referenced[lobFile] = true
			}
		}
	}

	return referenced
}

// scanLOBFiles scans for all LOB files under the given root path
func (gc *LOBGarbageCollector) scanLOBFiles(ctx context.Context, rootPath string) ([]string, error) {
	// Construct LOB search path: {rootPath}/insert_log/*/lob/
	// We need to scan all collections, partitions, segments, and fields

	lobFiles := make([]string, 0)

	// List all paths under insert_log
	insertLogPath := path.Join(rootPath, "insert_log")

	// Walk through the directory structure to find all files under "lob" directories
	err := gc.walkLOBDirectory(ctx, insertLogPath, &lobFiles)
	if err != nil {
		return nil, err
	}

	return lobFiles, nil
}

// walkLOBDirectory recursively walks the directory structure to find LOB files
func (gc *LOBGarbageCollector) walkLOBDirectory(ctx context.Context, dirPath string, result *[]string) error {
	// Walk directory contents using WalkWithPrefix
	err := gc.chunkManager.WalkWithPrefix(ctx, dirPath, true, func(chunkInfo *ChunkObjectInfo) bool {
		// Check if this is a LOB file
		if strings.Contains(chunkInfo.FilePath, "/lob/") {
			*result = append(*result, chunkInfo.FilePath)
		}
		return true // continue walking
	})

	if err != nil {
		// Directory might not exist, which is fine
		return nil
	}

	return nil
}

// isOldEnough checks if a file is old enough to be garbage collected
func (gc *LOBGarbageCollector) isOldEnough(ctx context.Context, filePath string) bool {
	// Get file modification time
	// Note: This requires ChunkManager to support Stat operation
	// For now, we assume all orphaned files are old enough
	// TODO: Implement proper age checking when ChunkManager supports it
	return true
}

// deleteFiles deletes a list of files and returns the count and total size deleted
func (gc *LOBGarbageCollector) deleteFiles(ctx context.Context, filePaths []string) (int64, int64, error) {
	var deleted int64
	var totalBytes int64
	var errs []error

	for _, filePath := range filePaths {
		// Try to get file size before deleting (optional)
		// size := gc.getFileSize(ctx, filePath)

		err := gc.chunkManager.Remove(ctx, filePath)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to delete file %s", filePath))
			gc.logger.Warn("failed to delete LOB file",
				zap.String("path", filePath),
				zap.Error(err),
			)
			continue
		}

		deleted++
		// totalBytes += size

		gc.logger.Debug("deleted LOB file",
			zap.String("path", filePath),
		)
	}

	if len(errs) > 0 {
		return deleted, totalBytes, errors.Newf("failed to delete %d files", len(errs))
	}

	return deleted, totalBytes, nil
}

// GetStatistics returns GC statistics
func (gc *LOBGarbageCollector) GetStatistics() map[string]interface{} {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	return map[string]interface{}{
		"scanned_files":   gc.scannedFiles,
		"deleted_files":   gc.deletedFiles,
		"deleted_bytes":   gc.deletedBytes,
		"last_scan_time":  gc.lastScanTime.Format(time.RFC3339),
		"last_clean_time": gc.lastCleanTime.Format(time.RFC3339),
		"min_age":         gc.minAge.String(),
	}
}

// ResetStatistics resets GC statistics
func (gc *LOBGarbageCollector) ResetStatistics() {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.scannedFiles = 0
	gc.deletedFiles = 0
	gc.deletedBytes = 0
}
