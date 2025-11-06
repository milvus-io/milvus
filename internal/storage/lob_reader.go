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

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// LOBReader provides lazy loading of LOB data from object storage.
// It caches loaded LOB files to avoid repeated reads.
type LOBReader struct {
	mu sync.RWMutex

	// chunk manager for reading from object storage
	chunkManager ChunkManager

	// cache: map[lobFileID]map[rowOffset]textData
	cache map[uint64]map[uint32]string

	// statistics
	cacheHits   int64
	cacheMisses int64
	filesLoaded int

	// logger
	logger *log.MLogger
}

// NewLOBReader creates a new LOB reader
func NewLOBReader(chunkManager ChunkManager) *LOBReader {
	return &LOBReader{
		chunkManager: chunkManager,
		cache:        make(map[uint64]map[uint32]string),
		logger:       log.With(zap.String("component", "LOBReader")),
	}
}

// ReadText reads text data for a LOB reference
// This method performs lazy loading: only fetches data from storage when needed
func (r *LOBReader) ReadText(ctx context.Context, ref *LOBReference, lobFilePath string) (string, error) {
	if ref == nil {
		return "", errors.New("LOB reference is nil")
	}

	if !ref.IsValid() {
		return "", errors.New("invalid LOB reference")
	}

	// Check cache first
	r.mu.RLock()
	if fileCache, ok := r.cache[ref.LobFileID]; ok {
		if text, ok := fileCache[ref.RowOffset]; ok {
			r.cacheHits++
			r.mu.RUnlock()
			r.logger.Debug("LOB cache hit",
				zap.Uint64("lobFileID", ref.LobFileID),
				zap.Uint32("rowOffset", ref.RowOffset),
			)
			return text, nil
		}
	}
	r.cacheMisses++
	r.mu.RUnlock()

	// Cache miss - need to load from storage
	return r.loadAndCacheLOBFile(ctx, ref, lobFilePath)
}

// BatchReadText reads multiple text values in a batch for better performance
// refs and lobFilePaths must have the same length
func (r *LOBReader) BatchReadText(ctx context.Context, refs []*LOBReference, lobFilePaths []string) ([]string, error) {
	if len(refs) != len(lobFilePaths) {
		return nil, errors.New("refs and lobFilePaths must have the same length")
	}

	results := make([]string, len(refs))
	var errs []error

	// Group by LOB file to minimize I/O
	fileGroups := make(map[uint64][]int) // lobFileID -> indices in refs
	for i, ref := range refs {
		if ref != nil && ref.IsValid() {
			fileGroups[ref.LobFileID] = append(fileGroups[ref.LobFileID], i)
		}
	}

	// Load each file once and fetch all required rows
	for lobFileID, indices := range fileGroups {
		// Use the first index to get the file path (all should be same for same lobFileID)
		filePath := lobFilePaths[indices[0]]

		// Load entire file into cache if not already loaded
		if err := r.ensureFileLoaded(ctx, lobFileID, filePath); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to load LOB file %d", lobFileID))
			continue
		}

		// Fetch all required rows from cache
		r.mu.RLock()
		fileCache := r.cache[lobFileID]
		for _, idx := range indices {
			ref := refs[idx]
			if text, ok := fileCache[ref.RowOffset]; ok {
				results[idx] = text
				r.cacheHits++
			} else {
				errs = append(errs, fmt.Errorf("row %d not found in LOB file %d", ref.RowOffset, lobFileID))
			}
		}
		r.mu.RUnlock()
	}

	if len(errs) > 0 {
		return results, errors.Newf("batch read encountered %d errors", len(errs))
	}

	return results, nil
}

// ensureFileLoaded loads a LOB file into cache if not already loaded
func (r *LOBReader) ensureFileLoaded(ctx context.Context, lobFileID uint64, lobFilePath string) error {
	// Check if already loaded
	r.mu.RLock()
	_, exists := r.cache[lobFileID]
	r.mu.RUnlock()

	if exists {
		return nil
	}

	// Need to load file
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if _, exists := r.cache[lobFileID]; exists {
		return nil
	}

	// Read LOB file from storage
	data, err := r.chunkManager.Read(ctx, lobFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to read LOB file %s", lobFilePath)
	}

	// Parse Parquet file and load into cache
	fileCache, err := r.parseLOBFile(data)
	if err != nil {
		return errors.Wrapf(err, "failed to parse LOB file %s", lobFilePath)
	}

	r.cache[lobFileID] = fileCache
	r.filesLoaded++

	r.logger.Info("loaded LOB file into cache",
		zap.Uint64("lobFileID", lobFileID),
		zap.String("path", lobFilePath),
		zap.Int("recordCount", len(fileCache)),
	)

	return nil
}

// loadAndCacheLOBFile loads a LOB file and returns the requested text
func (r *LOBReader) loadAndCacheLOBFile(ctx context.Context, ref *LOBReference, lobFilePath string) (string, error) {
	if err := r.ensureFileLoaded(ctx, ref.LobFileID, lobFilePath); err != nil {
		return "", err
	}

	// Fetch from cache
	r.mu.RLock()
	defer r.mu.RUnlock()

	fileCache, ok := r.cache[ref.LobFileID]
	if !ok {
		return "", errors.Newf("LOB file %d not in cache after loading", ref.LobFileID)
	}

	text, ok := fileCache[ref.RowOffset]
	if !ok {
		return "", errors.Newf("row offset %d not found in LOB file %d", ref.RowOffset, ref.LobFileID)
	}

	return text, nil
}

// parseLOBFile parses a LOB Parquet file and returns a map of rowOffset -> text
func (r *LOBReader) parseLOBFile(data []byte) (map[uint32]string, error) {
	// TODO: Implement Parquet parsing
	// For now, return an error indicating this needs to be implemented
	// This will use Arrow Parquet reader to read the single "text_data" column
	// and build a map of row index -> text value

	return nil, errors.New("LOB Parquet parsing not yet implemented - requires Arrow integration")
}

// ClearCache clears the entire cache
func (r *LOBReader) ClearCache() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache = make(map[uint64]map[uint32]string)
	r.filesLoaded = 0
	r.logger.Info("LOB cache cleared")
}

// GetStatistics returns reader statistics
func (r *LOBReader) GetStatistics() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	totalCached := 0
	for _, fileCache := range r.cache {
		totalCached += len(fileCache)
	}

	hitRate := float64(0)
	totalRequests := r.cacheHits + r.cacheMisses
	if totalRequests > 0 {
		hitRate = float64(r.cacheHits) / float64(totalRequests) * 100
	}

	return map[string]interface{}{
		"cache_hits":       r.cacheHits,
		"cache_misses":     r.cacheMisses,
		"cache_hit_rate":   fmt.Sprintf("%.2f%%", hitRate),
		"files_loaded":     r.filesLoaded,
		"files_in_cache":   len(r.cache),
		"records_in_cache": totalCached,
	}
}

// DetectAndDecodeLOBReferences scans a string field and decodes LOB references
// Returns: (actualTextData, isLOBField, error)
// If isLOBField=true, the returned data contains LOB references that need lazy loading
func DetectAndDecodeLOBReferences(fieldData *StringFieldData) ([]*LOBReference, bool, error) {
	if fieldData == nil || len(fieldData.Data) == 0 {
		return nil, false, nil
	}

	refs := make([]*LOBReference, len(fieldData.Data))
	hasLOB := false

	for i, data := range fieldData.Data {
		// Check if this is a LOB reference (exactly 16 bytes)
		if len(data) == LOBReferenceSize {
			ref, err := DecodeLOBReference([]byte(data))
			if err == nil && ref.IsValid() {
				refs[i] = ref
				hasLOB = true
			}
		}
	}

	if !hasLOB {
		return nil, false, nil
	}

	return refs, true, nil
}
