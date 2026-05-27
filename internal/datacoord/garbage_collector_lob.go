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

package datacoord

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// lobManifestCache caches LOB file information from Segment Manifests
// to avoid repeated FFI calls during GC cycles.
type lobManifestCache struct {
	mu    sync.RWMutex
	cache map[string]*lobManifestCacheEntry // key: manifestPath, value: cache entry

	// configuration
	ttl time.Duration // cache entry TTL
}

type lobManifestCacheEntry struct {
	lobFiles []packed.LobFileInfo
	cachedAt time.Time
}

func newLOBManifestCache(ttl time.Duration) *lobManifestCache {
	return &lobManifestCache{
		cache: make(map[string]*lobManifestCacheEntry),
		ttl:   ttl,
	}
}

// Get retrieves LOB files from cache or fetches from storage
func (c *lobManifestCache) Get(ctx context.Context, manifestPath string, storageConfig *indexpb.StorageConfig) ([]packed.LobFileInfo, error) {
	c.mu.RLock()
	entry, ok := c.cache[manifestPath]
	if ok && time.Since(entry.cachedAt) < c.ttl {
		c.mu.RUnlock()
		return entry.lobFiles, nil
	}
	c.mu.RUnlock()

	// cache miss or expired, fetch from storage
	lobFiles, err := packed.GetManifestLobFiles(manifestPath, storageConfig)
	if err != nil {
		return nil, err
	}

	// update cache
	c.mu.Lock()
	c.cache[manifestPath] = &lobManifestCacheEntry{
		lobFiles: lobFiles,
		cachedAt: time.Now(),
	}
	c.mu.Unlock()

	return lobFiles, nil
}

// Invalidate removes a specific entry from cache
func (c *lobManifestCache) Invalidate(manifestPath string) {
	c.mu.Lock()
	delete(c.cache, manifestPath)
	c.mu.Unlock()
}

// InvalidateAll clears the entire cache
func (c *lobManifestCache) InvalidateAll() {
	c.mu.Lock()
	c.cache = make(map[string]*lobManifestCacheEntry)
	c.mu.Unlock()
}

// Cleanup removes expired entries from cache
func (c *lobManifestCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, entry := range c.cache {
		if now.Sub(entry.cachedAt) > c.ttl {
			delete(c.cache, key)
		}
	}
}

// Size returns the number of cached entries
func (c *lobManifestCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// lobGCContext holds context for LOB garbage collection
type lobGCContext struct {
	gc            *garbageCollector
	cache         *lobManifestCache
	storageConfig *indexpb.StorageConfig
}

// newLOBGCContext creates a new LOB GC context
func newLOBGCContext(gc *garbageCollector) *lobGCContext {
	// cache TTL: 10 minutes (longer than GC interval to benefit from caching)
	cacheTTL := 10 * time.Minute
	return &lobGCContext{
		gc:    gc,
		cache: newLOBManifestCache(cacheTTL),
	}
}

// recycleUnusedLOBFiles performs garbage collection for LOB (TEXT column) files.
// This function:
// 1. Collects all LOB file references from active Segment Manifests
// 2. Scans LOB directories for actual files on storage
// 3. Deletes orphan files that are not referenced and older than safety window
func (gc *garbageCollector) recycleUnusedLOBFiles(ctx context.Context) {
	if !Params.DataCoordCfg.GCLOBEnabled.GetAsBool() {
		return
	}

	start := time.Now()
	logger := log.With(zap.String("gcName", "recycleUnusedLOBFiles"), zap.Time("startAt", start))
	logger.Info("start recycleUnusedLOBFiles...")
	defer func() {
		logger.Info("recycleUnusedLOBFiles done", zap.Duration("timeCost", time.Since(start)))
	}()

	storageConfig := gc.getStorageConfig()
	if storageConfig == nil {
		logger.Warn("failed to get storage config, skip LOB GC")
		return
	}

	lobCtx := newLOBGCContext(gc)
	lobCtx.storageConfig = storageConfig

	// Step 1: Collect all used LOB files from active segments.
	// If any manifest read fails, abort the entire GC run to avoid
	// deleting LOB files that may still be referenced.
	usedLOBFiles, err := lobCtx.collectUsedLOBFiles(ctx)
	if err != nil {
		logger.Warn("abort LOB GC: failed to collect used LOB files", zap.Error(err))
		return
	}
	logger.Info("collected used LOB files",
		zap.Int("usedFileCount", len(usedLOBFiles)),
		zap.Int("cacheSize", lobCtx.cache.Size()))

	// Step 2: Scan LOB directories and find orphan files
	orphanFiles := lobCtx.scanOrphanLOBFiles(ctx, usedLOBFiles)
	logger.Info("found orphan LOB files", zap.Int("orphanCount", len(orphanFiles)))

	// Step 3: Delete orphan files
	if len(orphanFiles) > 0 {
		lobCtx.removeOrphanLOBFiles(ctx, orphanFiles)
	}

	lobCtx.cache.Cleanup()
}

// getStorageConfig returns the storage configuration for FFI calls
func (gc *garbageCollector) getStorageConfig() *indexpb.StorageConfig {
	params := paramtable.Get()

	return &indexpb.StorageConfig{
		Address:           params.MinioCfg.Address.GetValue(),
		AccessKeyID:       params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:   params.MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:            params.MinioCfg.UseSSL.GetAsBool(),
		SslCACert:         params.MinioCfg.SslCACert.GetValue(),
		BucketName:        params.MinioCfg.BucketName.GetValue(),
		RootPath:          params.MinioCfg.RootPath.GetValue(),
		UseIAM:            params.MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:       params.MinioCfg.IAMEndpoint.GetValue(),
		StorageType:       params.CommonCfg.StorageType.GetValue(),
		Region:            params.MinioCfg.Region.GetValue(),
		UseVirtualHost:    params.MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:     params.MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs:  params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
		GcpCredentialJSON: params.MinioCfg.GcpCredentialJSON.GetValue(),
	}
}

// collectUsedLOBFiles collects all LOB file paths that are referenced by active segments
// and by dropped segments that are protected by snapshots.
// Returns a set of LOB file paths (relative paths within LOB directory).
// Returns error if any segment's manifest cannot be read, to prevent
// orphan deletion from removing files that are still in use.
func (lobCtx *lobGCContext) collectUsedLOBFiles(ctx context.Context) (typeutil.Set[string], error) {
	usedFiles := typeutil.NewSet[string]()

	// Collect from active (non-dropped) segments
	activeSegments := lobCtx.gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(si *SegmentInfo) bool {
		return si.GetState() != commonpb.SegmentState_Dropped
	}))
	for _, segment := range activeSegments {
		if err := lobCtx.collectLOBFilesFromSegment(ctx, segment, usedFiles); err != nil {
			return nil, err
		}
	}

	// Collect from dropped segments that are snapshot-protected.
	// Use the same IsSegmentGCBlocked check as the standard segment GC to stay
	// consistent: if a dropped segment is protected by a snapshot, its LOB files
	// must also be kept alive.
	snapshotMeta := lobCtx.gc.meta.GetSnapshotMeta()
	if snapshotMeta != nil {
		droppedSegments := lobCtx.gc.meta.SelectSegments(ctx, SegmentFilterFunc(func(si *SegmentInfo) bool {
			return si.GetState() == commonpb.SegmentState_Dropped
		}))
		for _, segment := range droppedSegments {
			if ctx.Err() != nil {
				return usedFiles, nil
			}
			if segment.GetManifestPath() == "" {
				continue
			}
			if snapshotMeta.IsSegmentGCBlocked(segment.GetCollectionID(), segment.GetID()) {
				if err := lobCtx.collectLOBFilesFromSegment(ctx, segment, usedFiles); err != nil {
					return nil, err
				}
			}
		}
	}

	return usedFiles, nil
}

// collectLOBFilesFromSegment extracts LOB file paths from a segment's manifest
// and adds them to the usedFiles set.
// Returns error if the manifest cannot be read, so the caller can abort GC
// and avoid deleting files that may still be referenced.
func (lobCtx *lobGCContext) collectLOBFilesFromSegment(ctx context.Context, segment *SegmentInfo, usedFiles typeutil.Set[string]) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	manifestPath := segment.GetManifestPath()
	if manifestPath == "" {
		return nil
	}

	lobFiles, err := lobCtx.cache.Get(ctx, manifestPath, lobCtx.storageConfig)
	if err != nil {
		return fmt.Errorf("failed to get LOB files from manifest for segment %d (path=%s): %w",
			segment.GetID(), manifestPath, err)
	}

	for _, lobFile := range lobFiles {
		if lobFile.Path != "" {
			normalized := extractLOBRelativePath(lobFile.Path)
			usedFiles.Insert(normalized)
		}
	}
	return nil
}

// scanOrphanLOBFiles scans LOB directories and returns files that are not in usedFiles.
// Only files older than the safety window are considered orphans.
func (lobCtx *lobGCContext) scanOrphanLOBFiles(ctx context.Context, usedFiles typeutil.Set[string]) []*storage.ChunkObjectInfo {
	orphanFiles := make([]*storage.ChunkObjectInfo, 0)
	safetyWindow := Params.DataCoordCfg.GCLOBSafetyWindow.GetAsDuration(time.Second)

	// LOB files are stored at: {root_path}/insert_log/{coll}/{part}/lobs/{field_id}/_data/{file_id}.vx
	lobBasePath := path.Join(lobCtx.gc.option.cli.RootPath(), common.SegmentInsertLogPath)

	// Walk through all files under insert_log to find LOB files
	// LOB files are identified by being in a "lobs" directory with .vx extension
	err := lobCtx.gc.option.cli.WalkWithPrefix(ctx, lobBasePath, true, func(info *storage.ChunkObjectInfo) bool {
		if ctx.Err() != nil {
			return false
		}

		if !isLOBFile(info.FilePath) {
			return true
		}

		// check if file is in used set
		// Both sides are normalized to relative paths starting from "lobs/"
		relativePath := extractLOBRelativePath(info.FilePath)
		if usedFiles.Contain(relativePath) {
			return true
		}

		// check safety window - only delete files older than safety window
		if time.Since(info.ModifyTime) < safetyWindow {
			log.Debug("LOB file within safety window, skip",
				zap.String("filePath", info.FilePath),
				zap.Time("modifyTime", info.ModifyTime),
				zap.Duration("safetyWindow", safetyWindow))
			return true
		}

		// this is an orphan file
		orphanFiles = append(orphanFiles, info)
		return true
	})
	if err != nil {
		log.Warn("failed to scan LOB files", zap.Error(err))
	}

	return orphanFiles
}

// removeOrphanLOBFiles deletes orphan LOB files from storage
func (lobCtx *lobGCContext) removeOrphanLOBFiles(ctx context.Context, orphanFiles []*storage.ChunkObjectInfo) {
	logger := log.With(zap.Int("orphanCount", len(orphanFiles)))
	logger.Info("removing orphan LOB files...")

	removed := atomic.NewInt32(0)
	failed := atomic.NewInt32(0)

	futures := make([]*conc.Future[struct{}], 0, len(orphanFiles))
	for _, file := range orphanFiles {
		filePath := file.FilePath
		future := lobCtx.gc.option.removeObjectPool.Submit(func() (struct{}, error) {
			if err := lobCtx.gc.option.cli.Remove(ctx, filePath); err != nil {
				log.Warn("failed to remove orphan LOB file",
					zap.String("filePath", filePath),
					zap.Error(err))
				failed.Inc()
				return struct{}{}, err
			}
			log.Info("removed orphan LOB file", zap.String("filePath", filePath))
			removed.Inc()
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}

	// wait for all deletions to complete
	if err := conc.BlockOnAll(futures...); err != nil {
		logger.Warn("some LOB file deletions failed", zap.Error(err))
	}

	logger.Info("orphan LOB files removal completed",
		zap.Int32("removed", removed.Load()),
		zap.Int32("failed", failed.Load()))
}

// isLOBFile checks if a file path is a LOB file
// LOB files are stored in "lobs" directory with .vx extension
func isLOBFile(filePath string) bool {
	// path format: {root}/insert_log/{coll}/{part}/lobs/{field_id}/_data/{file_id}.vx
	return strings.HasSuffix(filePath, ".vx") && strings.Contains(filePath, "/lobs/")
}

// extractLOBRelativePath extracts the relative path for LOB file comparison
// This handles the case where LOB file paths in manifest might be stored differently
func extractLOBRelativePath(fullPath string) string {
	if idx := strings.Index(fullPath, "lobs/"); idx >= 0 {
		return fullPath[idx:]
	}
	return fullPath
}
