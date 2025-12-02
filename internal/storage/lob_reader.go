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
	"io"
	"path"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// LOBReader provides lazy loading of LOB data from object storage.
// It caches loaded LOB files to avoid repeated reads.
type LOBReader struct {
	mu sync.RWMutex

	// storage configuration for packed reader
	storageConfig *indexpb.StorageConfig
	bucketName    string

	// Arrow schema for LOB files (single "text_data" column)
	arrowSchema *arrow.Schema

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
func NewLOBReader(storageConfig *indexpb.StorageConfig) *LOBReader {
	// get bucket name
	bucketName := ""
	if storageConfig != nil {
		bucketName = storageConfig.GetBucketName()
	}
	if bucketName == "" {
		bucketName = paramtable.Get().MinioCfg.BucketName.GetValue()
	}

	// create Arrow schema for LOB files
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "text_data", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	return &LOBReader{
		storageConfig: storageConfig,
		bucketName:    bucketName,
		arrowSchema:   arrowSchema,
		cache:         make(map[uint64]map[uint32]string),
		logger:        log.With(zap.String("component", "LOBReader")),
	}
}

// ReadText reads text data for a LOB reference
// this method performs lazy loading: only fetches data from storage when needed
func (r *LOBReader) ReadText(ctx context.Context, ref *LOBReference, lobFilePath string) (string, error) {
	if ref == nil {
		return "", errors.New("LOB reference is nil")
	}

	if !ref.IsValid() {
		return "", errors.New("invalid LOB reference")
	}

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

	// cache miss - need to load from storage
	return r.loadAndCacheLOBFile(ctx, ref, lobFilePath)
}

// ensureFileLoaded loads a LOB file into cache if not already loaded
func (r *LOBReader) ensureFileLoaded(ctx context.Context, lobFileID uint64, lobFilePath string) error {
	// check if already loaded
	r.mu.RLock()
	_, exists := r.cache[lobFileID]
	r.mu.RUnlock()

	if exists {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// double-check after acquiring write lock
	if _, exists := r.cache[lobFileID]; exists {
		return nil
	}

	// parse LOB file using packed reader and load into cache
	fileCache, err := r.parseLOBFile(ctx, lobFilePath)
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

// parseLOBFile parses a LOB file using packed reader and returns a map of rowOffset -> text
func (r *LOBReader) parseLOBFile(ctx context.Context, lobFilePath string) (map[uint32]string, error) {
	// determine storage type and construct full path
	storageType := paramtable.Get().CommonCfg.StorageType.GetValue()
	if r.storageConfig != nil {
		storageType = r.storageConfig.GetStorageType()
	}

	readPath := lobFilePath
	if storageType != "local" {
		readPath = path.Join(r.bucketName, lobFilePath)
	}

	// create packed reader
	reader, err := packed.NewPackedReader(
		[]string{readPath},
		r.arrowSchema,
		16*1024*1024, // 16MB buffer
		r.storageConfig,
		nil,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create packed reader for LOB file %s", lobFilePath)
	}
	defer reader.Close()

	// read all records and build cache map
	result := make(map[uint32]string)
	rowIndex := uint32(0)

	for {
		record, err := reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to read record batch")
		}
		if record == nil {
			break
		}

		// extract text_data column (index 0)
		if record.NumCols() != 1 {
			record.Release()
			return nil, errors.Newf("expected 1 column, got %d", record.NumCols())
		}

		column := record.Column(0)

		// cast to string/binary array
		switch arr := column.(type) {
		case *array.String:
			for i := 0; i < arr.Len(); i++ {
				if !arr.IsNull(i) {
					result[rowIndex] = arr.Value(i)
				}
				rowIndex++
			}
		case *array.Binary:
			for i := 0; i < arr.Len(); i++ {
				if !arr.IsNull(i) {
					result[rowIndex] = string(arr.Value(i))
				}
				rowIndex++
			}
		default:
			record.Release()
			return nil, errors.Newf("unexpected array type: %T", arr)
		}

		record.Release()
	}

	r.logger.Debug("parsed LOB file using packed reader",
		zap.String("path", lobFilePath),
		zap.Int("totalRows", len(result)))

	return result, nil
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
