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
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
)

// VectorChunkManager is responsible for read and write vector data.
type VectorChunkManager struct {
	localChunkManager  ChunkManager
	remoteChunkManager ChunkManager

	schema *etcdpb.CollectionMeta

	localCacheEnable bool
}

var _ ChunkManager = (*VectorChunkManager)(nil)

// NewVectorChunkManager create a new vector manager object.
func NewVectorChunkManager(localChunkManager ChunkManager, remoteChunkManager ChunkManager, schema *etcdpb.CollectionMeta, localCacheEnable bool) *VectorChunkManager {
	return &VectorChunkManager{
		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,

		schema:           schema,
		localCacheEnable: localCacheEnable,
	}
}

// For vector data, we will download vector file from storage. And we will
// deserialize the file for it has binlog style. At last we store pure vector
// data to local storage as cache.
func (vcm *VectorChunkManager) downloadVectorFile(filePath string) ([]byte, error) {
	if vcm.localChunkManager.Exist(filePath) {
		return vcm.localChunkManager.Read(filePath)
	}
	content, err := vcm.remoteChunkManager.Read(filePath)
	if err != nil {
		return nil, err
	}
	insertCodec := NewInsertCodec(vcm.schema)
	blob := &Blob{
		Key:   filePath,
		Value: content,
	}

	_, _, data, err := insertCodec.Deserialize([]*Blob{blob})
	if err != nil {
		return nil, err
	}

	var results []byte
	for _, singleData := range data.Data {
		binaryVector, ok := singleData.(*BinaryVectorFieldData)
		if ok {
			results = binaryVector.Data
		}
		floatVector, ok := singleData.(*FloatVectorFieldData)
		if ok {
			buf := new(bytes.Buffer)
			err := binary.Write(buf, common.Endian, floatVector.Data)
			if err != nil {
				return nil, err
			}
			results = buf.Bytes()
		}
	}
	return results, nil
}

// GetPath returns the path of vector data. If cached, return local path.
// If not cached return remote path.
func (vcm *VectorChunkManager) GetPath(filePath string) (string, error) {
	if vcm.localChunkManager.Exist(filePath) && vcm.localCacheEnable {
		return vcm.localChunkManager.GetPath(filePath)
	}
	return vcm.remoteChunkManager.GetPath(filePath)
}

func (vcm *VectorChunkManager) GetSize(filePath string) (int64, error) {
	if vcm.localChunkManager.Exist(filePath) && vcm.localCacheEnable {
		return vcm.localChunkManager.GetSize(filePath)
	}
	return vcm.remoteChunkManager.GetSize(filePath)
}

// Write writes the vector data to local cache if cache enabled.
func (vcm *VectorChunkManager) Write(filePath string, content []byte) error {
	if !vcm.localCacheEnable {
		return errors.New("cannot write local file for local cache is not allowed")
	}
	return vcm.localChunkManager.Write(filePath, content)
}

// MultiWrite writes the vector data to local cache if cache enabled.
func (vcm *VectorChunkManager) MultiWrite(contents map[string][]byte) error {
	if !vcm.localCacheEnable {
		return errors.New("cannot write local file for local cache is not allowed")
	}
	return vcm.localChunkManager.MultiWrite(contents)
}

// Exist checks whether vector data is saved to local cache.
func (vcm *VectorChunkManager) Exist(filePath string) bool {
	return vcm.localChunkManager.Exist(filePath)
}

// Read reads the pure vector data. If cached, it reads from local.
func (vcm *VectorChunkManager) Read(filePath string) ([]byte, error) {
	if vcm.localCacheEnable {
		if vcm.localChunkManager.Exist(filePath) {
			return vcm.localChunkManager.Read(filePath)
		}
		contents, err := vcm.downloadVectorFile(filePath)
		if err != nil {
			return nil, err
		}
		err = vcm.localChunkManager.Write(filePath, contents)
		if err != nil {
			return nil, err
		}
		return vcm.localChunkManager.Read(filePath)
	}
	return vcm.downloadVectorFile(filePath)
}

// MultiRead reads the pure vector data. If cached, it reads from local.
func (vcm *VectorChunkManager) MultiRead(filePaths []string) ([][]byte, error) {
	var results [][]byte
	for _, filePath := range filePaths {
		content, err := vcm.Read(filePath)
		if err != nil {
			return nil, err
		}
		results = append(results, content)
	}

	return results, nil
}

func (vcm *VectorChunkManager) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	panic("has not implemented yet")
}

// ReadAt reads specific position data of vector. If cached, it reads from local.
func (vcm *VectorChunkManager) ReadAt(filePath string, off int64, length int64) ([]byte, error) {
	if vcm.localCacheEnable {
		if vcm.localChunkManager.Exist(filePath) {
			return vcm.localChunkManager.ReadAt(filePath, off, length)
		}
		results, err := vcm.downloadVectorFile(filePath)
		if err != nil {
			return nil, err
		}
		err = vcm.localChunkManager.Write(filePath, results)
		if err != nil {
			return nil, err
		}
		return vcm.localChunkManager.ReadAt(filePath, off, length)
	}
	results, err := vcm.downloadVectorFile(filePath)
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
func (vcm *VectorChunkManager) Remove(filePath string) error {
	err := vcm.localChunkManager.Remove(filePath)
	if err != nil {
		return err
	}
	err = vcm.remoteChunkManager.Remove(filePath)
	if err != nil {
		return err
	}
	return nil
}

func (vcm *VectorChunkManager) MultiRemove(filePaths []string) error {
	err := vcm.localChunkManager.MultiRemove(filePaths)
	if err != nil {
		return err
	}
	err = vcm.remoteChunkManager.MultiRemove(filePaths)
	if err != nil {
		return err
	}
	return nil
}

func (vcm *VectorChunkManager) RemoveWithPrefix(prefix string) error {
	err := vcm.localChunkManager.RemoveWithPrefix(prefix)
	if err != nil {
		return err
	}
	err = vcm.remoteChunkManager.RemoveWithPrefix(prefix)
	if err != nil {
		return err
	}
	return nil
}

func (vcm *VectorChunkManager) Close() error {
	// TODO：Replace the cache with the local chunk manager and clear the cache when closed
	return vcm.localChunkManager.RemoveWithPrefix("")
}
