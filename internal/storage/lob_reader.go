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
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// LOBReader reads LOB data from a single LOB file.
// Each LOBReader instance is responsible for one LOB file.
// The file is loaded on first access and cached for subsequent reads.
type LOBReader struct {
	mu sync.RWMutex

	storageConfig *indexpb.StorageConfig
	arrowSchema   *arrow.Schema

	// cache: rowOffset -> textData (single file)
	cache  map[uint32]string
	loaded bool

	logger *log.MLogger
}

func NewLOBReader(storageConfig *indexpb.StorageConfig) *LOBReader {
	arrowSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: LOBFieldName, Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	return &LOBReader{
		storageConfig: storageConfig,
		arrowSchema:   arrowSchema,
		cache:         make(map[uint32]string),
		logger:        log.With(zap.String("component", "LOBReader")),
	}
}

// ReadText reads text data for a LOB reference.
// Loads the entire file on first access, then reads from cache.
func (r *LOBReader) ReadText(ctx context.Context, ref *LOBReference, lobFilePath string) (string, error) {
	if ref == nil {
		return "", errors.New("LOB reference is nil")
	}

	if !ref.IsValid() {
		return "", errors.New("invalid LOB reference")
	}

	// try cache first
	r.mu.RLock()
	if r.loaded {
		text, ok := r.cache[ref.RowOffset]
		r.mu.RUnlock()
		if ok {
			return text, nil
		}
		return "", errors.Newf("row offset %d not found in LOB file", ref.RowOffset)
	}
	r.mu.RUnlock()

	// not loaded yet - load the file
	return r.loadFile(ctx, ref, lobFilePath)
}

func (r *LOBReader) loadFile(ctx context.Context, ref *LOBReference, lobFilePath string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// double-check if already loaded
	if r.loaded {
		text, ok := r.cache[ref.RowOffset]
		if ok {
			return text, nil
		}
		return "", errors.Newf("row offset %d not found in LOB file", ref.RowOffset)
	}

	// parse and cache the file
	cache, err := r.parseLOBFile(ctx, lobFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse LOB file %s", lobFilePath)
	}

	r.cache = cache
	r.loaded = true

	r.logger.Info("loaded LOB file",
		zap.String("path", lobFilePath),
		zap.Int("recordCount", len(cache)))

	text, ok := cache[ref.RowOffset]
	if !ok {
		return "", errors.Newf("row offset %d not found in LOB file", ref.RowOffset)
	}

	return text, nil
}

func (r *LOBReader) parseLOBFile(ctx context.Context, lobFilePath string) (map[uint32]string, error) {
	storageType := r.storageConfig.GetStorageType()

	readPath := path.Join(r.storageConfig.GetRootPath(), lobFilePath)
	if storageType != "local" {
		readPath = path.Join(r.storageConfig.GetBucketName(), readPath)
	}

	reader, err := packed.NewPackedReader(
		[]string{readPath},
		r.arrowSchema,
		16*1024*1024,
		r.storageConfig,
		nil,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create packed reader for LOB file %s", lobFilePath)
	}
	defer reader.Close()

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

		if record.NumCols() != 1 {
			record.Release()
			return nil, errors.Newf("expected 1 column, got %d", record.NumCols())
		}

		column := record.Column(0)

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

	r.logger.Debug("parsed LOB file",
		zap.String("path", lobFilePath),
		zap.Int("totalRows", len(result)))

	return result, nil
}
