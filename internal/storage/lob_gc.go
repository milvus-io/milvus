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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

// SegmentMetaInterface defines the interface for querying segment metadata
// This allows LOB GC to check segment state without depending on datacoord directly
type SegmentMetaInterface interface {
	// GetSegment returns the segment info for the given segment ID
	// Returns nil if segment does not exist
	GetSegment(ctx context.Context, segmentID int64) SegmentInfo
}

// SegmentInfo provides information about a segment
type SegmentInfo interface {
	// GetID returns the segment ID
	GetID() int64
	// GetState returns the segment state
	GetState() commonpb.SegmentState
}

// LOBGarbageCollector manages cleanup of orphaned LOB files
type LOBGarbageCollector struct {
	mu sync.Mutex

	// chunk manager for storage operations
	chunkManager ChunkManager

	// minimum file age before deletion (safety threshold)
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

type LOBGCOption func(*LOBGarbageCollector)

// WithLOBGCMinAge sets the minimum age of files before they can be GC'd
func WithLOBGCMinAge(minAge time.Duration) LOBGCOption {
	return func(gc *LOBGarbageCollector) {
		gc.minAge = minAge
	}
}

func NewLOBGarbageCollector(chunkManager ChunkManager, opts ...LOBGCOption) *LOBGarbageCollector {
	gc := &LOBGarbageCollector{
		chunkManager: chunkManager,
		minAge:       24 * time.Hour, // default: 24 hours
		logger:       log.With(zap.String("component", "LOBGarbageCollector")),
	}

	for _, opt := range opts {
		opt(gc)
	}

	return gc
}

// ScanAndCleanup scans for orphaned LOB files and removes them
//
// This is a safety net that removes files whose segments no longer exist in metadata.
// It complements Layer 1 GC (recycleDroppedSegments) which handles dropped segments.
func (gc *LOBGarbageCollector) ScanAndCleanup(ctx context.Context, meta SegmentMetaInterface, rootPath string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.lastScanTime = time.Now()
	gc.logger.Info("starting LOB garbage collection scan (orphan GC)",
		zap.String("rootPath", rootPath),
		zap.String("strategy", "delete files with non-existent segments only"),
	)

	orphanedFiles := make([]string, 0)
	skippedRecent := 0
	skippedActive := 0
	parseErrors := 0
	totalScanned := 0

	// scan insert_log directory for LOB files
	insertLogPath := path.Join(rootPath, "insert_log")
	err := gc.chunkManager.WalkWithPrefix(ctx, insertLogPath, true, func(chunkInfo *ChunkObjectInfo) bool {
		// only process LOB files (path contains "/lob/")
		if !strings.Contains(chunkInfo.FilePath, "/lob/") {
			return true
		}

		totalScanned++

		// layer 1 protection: check file age (skip recent files)
		fileAge := time.Since(chunkInfo.ModifyTime)
		if fileAge < gc.minAge {
			gc.logger.Debug("skipping recent LOB file",
				zap.String("path", chunkInfo.FilePath),
				zap.Duration("age", fileAge),
				zap.Duration("minAge", gc.minAge))
			skippedRecent++
			return true // continue walking
		}

		// layer 2 protection: check segment state
		// extract segment ID from file path
		segmentID, err := extractSegmentIDFromLOBPath(chunkInfo.FilePath)
		if err != nil {
			gc.logger.Warn("failed to extract segment ID from path, skipping",
				zap.String("path", chunkInfo.FilePath),
				zap.Error(err))
			parseErrors++
			return true
		}

		// query segment metadata
		segment := meta.GetSegment(ctx, segmentID)

		if segment != nil {
			gc.logger.Debug("skipping LOB file from existing segment",
				zap.String("path", chunkInfo.FilePath),
				zap.Int64("segmentID", segmentID),
				zap.String("state", segment.GetState().String()))
			skippedActive++
			return true
		}

		// file is truly orphaned: segment doesn't exist in metadata
		reason := "segment_not_found"

		gc.logger.Info("identified orphaned LOB file",
			zap.String("path", chunkInfo.FilePath),
			zap.Int64("segmentID", segmentID),
			zap.String("reason", reason),
			zap.Duration("fileAge", fileAge))

		orphanedFiles = append(orphanedFiles, chunkInfo.FilePath)
		return true
	})
	if err != nil {
		return errors.Wrap(err, "failed to scan LOB files")
	}

	gc.scannedFiles += int64(totalScanned)
	gc.logger.Info("scan completed",
		zap.Int("totalScanned", totalScanned),
		zap.Int("orphanedFiles", len(orphanedFiles)),
		zap.Int("skippedRecent", skippedRecent),
		zap.Int("skippedActive", skippedActive),
		zap.Int("parseErrors", parseErrors),
		zap.Duration("minAge", gc.minAge),
	)

	// delete orphaned files
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

// deleteFiles deletes a list of files and returns the count and total size deleted
func (gc *LOBGarbageCollector) deleteFiles(ctx context.Context, filePaths []string) (int64, int64, error) {
	var deleted int64
	var totalBytes int64
	var errs []error

	for _, filePath := range filePaths {
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

		gc.logger.Info("deleted LOB file",
			zap.String("path", filePath),
		)
	}

	if len(errs) > 0 {
		return deleted, totalBytes, errors.Newf("failed to delete %d files", len(errs))
	}

	return deleted, totalBytes, nil
}

func (gc *LOBGarbageCollector) GetStatistics() map[string]interface{} {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	return map[string]interface{}{
		"scanned_files":   gc.scannedFiles,
		"deleted_files":   gc.deletedFiles,
		"deleted_bytes":   gc.deletedBytes,
		"last_scan_time":  gc.lastScanTime.Format(time.RFC3339),
		"last_clean_time": gc.lastCleanTime.Format(time.RFC3339),
	}
}

// extractSegmentIDFromLOBPath extracts segment ID from LOB file path
// path format: {root}/insert_log/{collection_id}/{partition_id}/{segment_id}/{field_id}/lob/{lob_file_id}
func extractSegmentIDFromLOBPath(filePath string) (int64, error) {
	parts := strings.Split(filePath, "/")

	// find "insert_log" in path
	insertLogIndex := -1
	for i, part := range parts {
		if part == "insert_log" {
			insertLogIndex = i
			break
		}
	}

	// segment ID is the 3rd part after insert_log
	segmentIDStr := parts[insertLogIndex+3]
	segmentID, err := strconv.ParseInt(segmentIDStr, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse segment ID from path: %s", filePath)
	}

	return segmentID, nil
}
