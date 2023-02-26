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
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"go.uber.org/zap"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/cache"
)

var (
	defaultLocalCacheSize = 64
)

// VectorChunkManager is responsible for read and write vector data.
type VectorChunkManager struct {
	cacheStorage  ChunkManager
	vectorStorage ChunkManager
	cache         cache.LoadingCache[string, *mmap.ReaderAt]

	insertCodec *InsertCodec

	cacheEnable    bool
	cacheLimit     int64
	cacheSize      int64
	cacheSizeMutex sync.Mutex
}

var _ ChunkManager = (*VectorChunkManager)(nil)

// NewVectorChunkManager create a new vector manager object.
func NewVectorChunkManager(ctx context.Context, cacheStorage ChunkManager, vectorStorage ChunkManager, schema *etcdpb.CollectionMeta, cacheLimit int64, cacheEnable bool) (*VectorChunkManager, error) {
	insertCodec := NewInsertCodec(schema)
	vcm := &VectorChunkManager{
		cacheStorage:  cacheStorage,
		vectorStorage: vectorStorage,

		insertCodec: insertCodec,
		cacheEnable: cacheEnable,
		cacheLimit:  cacheLimit,
	}

	err := vcm.initCache(ctx)
	if err != nil {
		return nil, err
	}

	return vcm, nil
}

func (vcm *VectorChunkManager) initCache(ctx context.Context) error {
	if !vcm.cacheEnable {
		return nil
	}

	if vcm.cacheLimit <= 0 {
		return errors.New("cache limit must be positive if cacheEnable")
	}

	loader := func(filePath string) (*mmap.ReaderAt, error) {
		return vcm.readFile(ctx, filePath)
	}

	onRemoveFn := func(filePath string, v *mmap.ReaderAt) {
		size := v.Len()
		err := v.Close()
		if err != nil {
			log.Error("close mmap file failed", zap.Any("file", filePath))
		}
		err = vcm.cacheStorage.Remove(ctx, filePath)
		if err != nil {
			log.Error("cache storage remove file failed", zap.Any("file", filePath))
		}

		vcm.cacheSizeMutex.Lock()
		vcm.cacheSize -= int64(size)
		vcm.cacheSizeMutex.Unlock()
	}

	vcm.cache = cache.NewLoadingCache(loader,
		cache.WithRemovalListener[string, *mmap.ReaderAt](onRemoveFn),
		cache.WithMaximumSize[string, *mmap.ReaderAt](vcm.cacheLimit),
	)

	return nil
}

// For vector data, we will download vector file from storage. And we will
// deserialize the file for it has binlog style. At last we store pure vector
// data to local storage as cache.
func (vcm *VectorChunkManager) deserializeVectorFile(filePath string, content []byte) ([]byte, error) {
	blob := &Blob{
		Key:   filePath,
		Value: content,
	}

	_, _, data, err := vcm.insertCodec.Deserialize([]*Blob{blob})
	if err != nil {
		return nil, err
	}

	// Note: here we assume that only one field in the binlog.
	var results []byte
	for _, singleData := range data.Data {
		bs, err := FieldDataToBytes(common.Endian, singleData)
		if err == nil {
			results = bs
		}
	}
	return results, nil
}

// RootPath returns vector root path
func (vcm *VectorChunkManager) RootPath() string {
	return vcm.vectorStorage.RootPath()
}

// Path returns the path of vector data. If cached, return local path.
// If not cached return remote path.
func (vcm *VectorChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	return vcm.vectorStorage.Path(ctx, filePath)
}

func (vcm *VectorChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	return vcm.vectorStorage.Size(ctx, filePath)
}

// Write writes the vector data to local cache if cache enabled.
func (vcm *VectorChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	return vcm.vectorStorage.Write(ctx, filePath, content)
}

// MultiWrite writes the vector data to local cache if cache enabled.
func (vcm *VectorChunkManager) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	return vcm.vectorStorage.MultiWrite(ctx, contents)
}

// Exist checks whether vector data is saved to local cache.
func (vcm *VectorChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	return vcm.vectorStorage.Exist(ctx, filePath)
}

func (vcm *VectorChunkManager) readFile(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	contents, err := vcm.vectorStorage.Read(ctx, filePath)
	if err != nil {
		return nil, err
	}
	results, err := vcm.deserializeVectorFile(filePath, contents)
	if err != nil {
		return nil, err
	}
	err = vcm.cacheStorage.Write(ctx, filePath, results)
	if err != nil {
		return nil, err
	}

	r, err := vcm.cacheStorage.Mmap(ctx, filePath)
	if err != nil {
		return nil, err
	}
	vcm.cacheSizeMutex.Lock()
	vcm.cacheSize += int64(r.Len())
	vcm.cacheSizeMutex.Unlock()
	return r, nil
}

// Read reads the pure vector data. If cached, it reads from local.
func (vcm *VectorChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	if vcm.cacheEnable {
		r, err := vcm.cache.Get(filePath)
		if err != nil {
			return nil, err
		}

		p := make([]byte, r.Len())
		_, err = r.ReadAt(p, 0)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	contents, err := vcm.vectorStorage.Read(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return vcm.deserializeVectorFile(filePath, contents)
}

// MultiRead reads the pure vector data. If cached, it reads from local.
func (vcm *VectorChunkManager) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	results := make([][]byte, len(filePaths))
	for i, filePath := range filePaths {
		content, err := vcm.Read(ctx, filePath)
		if err != nil {
			return nil, err
		}
		results[i] = content
	}

	return results, nil
}

func (vcm *VectorChunkManager) ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error) {
	filePaths, _, err := vcm.ListWithPrefix(ctx, prefix, true)
	if err != nil {
		return nil, nil, err
	}
	results, err := vcm.MultiRead(ctx, filePaths)
	if err != nil {
		return nil, nil, err
	}
	return filePaths, results, nil
}

func (vcm *VectorChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	return vcm.vectorStorage.ListWithPrefix(ctx, prefix, recursive)
}

func (vcm *VectorChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	if vcm.cacheEnable && vcm.cache != nil {
		if r, err := vcm.cache.Get(filePath); err == nil {
			return r, nil
		}
	}
	return nil, errors.New("the file mmap has not been cached")
}

func (vcm *VectorChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	return nil, errors.New("this method has not been implemented")
}

// ReadAt reads specific position data of vector. If cached, it reads from local.
func (vcm *VectorChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	if vcm.cacheEnable {
		r, err := vcm.cache.Get(filePath)
		if err != nil {
			return nil, err
		}

		p := make([]byte, length)
		_, err = r.ReadAt(p, off)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	contents, err := vcm.vectorStorage.Read(ctx, filePath)
	if err != nil {
		return nil, err
	}
	results, err := vcm.deserializeVectorFile(filePath, contents)
	if err != nil {
		return nil, err
	}

	if off < 0 || int64(len(results)) < off {
		return nil, errors.New("vectorChunkManager: invalid offset")
	}

	p := make([]byte, length)
	n := copy(p, results[off:])
	if n < len(p) {
		return nil, io.EOF
	}
	return p, nil
}
func (vcm *VectorChunkManager) Remove(ctx context.Context, filePath string) error {
	err := vcm.vectorStorage.Remove(ctx, filePath)
	if err != nil {
		return err
	}
	if vcm.cacheEnable {
		vcm.cache.Invalidate(filePath)
	}
	return nil
}

func (vcm *VectorChunkManager) MultiRemove(ctx context.Context, filePaths []string) error {
	err := vcm.vectorStorage.MultiRemove(ctx, filePaths)
	if err != nil {
		return err
	}
	if vcm.cacheEnable {
		for _, p := range filePaths {
			vcm.cache.Invalidate(p)
		}
	}
	return nil
}

func (vcm *VectorChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	err := vcm.vectorStorage.RemoveWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	if vcm.cacheEnable {
		filePaths, _, err := vcm.ListWithPrefix(ctx, prefix, true)
		if err != nil {
			return err
		}
		for _, p := range filePaths {
			vcm.cache.Invalidate(p)
		}
	}
	return nil
}

func (vcm *VectorChunkManager) Close() {
	if vcm.cache != nil && vcm.cacheEnable {
		vcm.cache.Close()
	}
}
