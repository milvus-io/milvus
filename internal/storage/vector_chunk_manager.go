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
	"errors"
	"io"
	"sync"

	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
)

// VectorChunkManager is responsible for read and write vector data.
type VectorChunkManager struct {
	cache         *LevelCache
	vectorStorage ChunkManager

	insertCodec *InsertCodec

	cacheSizeMutex sync.Mutex
	fixSize        bool // Prevent cache capactiy from changing too frequently
}

var _ ChunkManager = (*VectorChunkManager)(nil)

// NewVectorChunkManager create a new vector manager object.
func NewVectorChunkManager(
	cacheStorage ChunkManager,
	vectorStorage ChunkManager,
	schema *etcdpb.CollectionMeta,
	localFileCacheLimit uint64,
	cacheLimit uint64,
	localFileCacheEnable bool,
) (*VectorChunkManager, error) {
	insertCodec := NewInsertCodec(schema)
	cache, err := NewLevelCache(localFileCacheLimit, cacheLimit, cacheStorage, localFileCacheEnable)
	if err != nil {
		return nil, err
	}
	vcm := &VectorChunkManager{
		cache:         cache,
		vectorStorage: vectorStorage,

		insertCodec: insertCodec,
	}

	return vcm, nil
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

// GetPath returns the path of vector data. If cached, return local path.
// If not cached return remote path.
func (vcm *VectorChunkManager) Path(filePath string) (string, error) {
	return vcm.vectorStorage.Path(filePath)
}

func (vcm *VectorChunkManager) Size(filePath string) (int64, error) {
	return vcm.vectorStorage.Size(filePath)
}

// Write writes the vector data to local cache if cache enabled.
func (vcm *VectorChunkManager) Write(filePath string, content []byte) error {
	return vcm.vectorStorage.Write(filePath, content)
}

// MultiWrite writes the vector data to local cache if cache enabled.
func (vcm *VectorChunkManager) MultiWrite(contents map[string][]byte) error {
	return vcm.vectorStorage.MultiWrite(contents)
}

// Exist checks whether vector data is saved to local cache.
func (vcm *VectorChunkManager) Exist(filePath string) bool {
	return vcm.vectorStorage.Exist(filePath)
}

// Read reads the pure vector data. If cached, it reads from local.
func (vcm *VectorChunkManager) Read(filePath string) ([]byte, error) {
	if v, ok := vcm.cache.Get(filePath); ok {
		return v.([]byte), nil
	}
	contents, err := vcm.vectorStorage.Read(filePath)
	if err != nil {
		return nil, err
	}
	results, err := vcm.deserializeVectorFile(filePath, contents)
	if err != nil {
		return nil, err
	}
	vcm.cache.Add(filePath, results)
	return results, nil
}

// MultiRead reads the pure vector data. If cached, it reads from local.
func (vcm *VectorChunkManager) MultiRead(filePaths []string) ([][]byte, error) {
	results := make([][]byte, len(filePaths))
	for i, filePath := range filePaths {
		content, err := vcm.Read(filePath)
		if err != nil {
			return nil, err
		}
		results[i] = content
	}

	return results, nil
}

func (vcm *VectorChunkManager) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	filePaths, err := vcm.ListWithPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}
	results, err := vcm.MultiRead(filePaths)
	if err != nil {
		return nil, nil, err
	}
	return filePaths, results, nil
}

func (vcm *VectorChunkManager) ListWithPrefix(prefix string) ([]string, error) {
	return vcm.vectorStorage.ListWithPrefix(prefix)
}

func (vcm *VectorChunkManager) Mmap(filePath string) (*mmap.ReaderAt, error) {
	if v, ok := vcm.cache.Get(filePath); ok {
		return v.(*mmap.ReaderAt), nil
	}
	return nil, errors.New("the file mmap has not been cached")
}

func (vcm *VectorChunkManager) Reader(filePath string) (FileReader, error) {
	return nil, errors.New("this method has not been implemented")
}

// ReadAt reads specific position data of vector. If cached, it reads from local.
func (vcm *VectorChunkManager) ReadAt(filePath string, off int64, length int64) ([]byte, error) {
	if v, ok := vcm.cache.Get(filePath); ok {
		data := v.([]byte)
		if int(off) > len(data) {
			return nil, errors.New("the offset is larger than data length")
		}
		if int(off+length) > len(data) {
			return data[off:], nil
		}
		return v.([]byte)[off : off+length], nil
	}
	contents, err := vcm.vectorStorage.Read(filePath)
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
	vcm.cache.Add(filePath, results)
	return p, nil
}
func (vcm *VectorChunkManager) Remove(filePath string) error {
	err := vcm.vectorStorage.Remove(filePath)
	if err != nil {
		return err
	}
	vcm.cache.Remove(filePath)
	return nil
}

func (vcm *VectorChunkManager) MultiRemove(filePaths []string) error {
	err := vcm.vectorStorage.MultiRemove(filePaths)
	if err != nil {
		return err
	}
	for _, p := range filePaths {
		vcm.cache.Remove(p)
	}
	return nil
}

func (vcm *VectorChunkManager) RemoveWithPrefix(prefix string) error {
	err := vcm.vectorStorage.RemoveWithPrefix(prefix)
	if err != nil {
		return err
	}
	filePaths, err := vcm.ListWithPrefix(prefix)
	if err != nil {
		return err
	}
	for _, p := range filePaths {
		vcm.cache.Remove(p)
	}
	return nil
}

func (vcm *VectorChunkManager) Close() {
	vcm.cache.Close()
}
