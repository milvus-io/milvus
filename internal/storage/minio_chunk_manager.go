// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"errors"
	"io"

	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
)

// MinioChunkManager is responsible for read and write data stored in minio.
type MinioChunkManager struct {
	minio *miniokv.MinIOKV
}

// NewMinioChunkManager create a new local manager object.
func NewMinioChunkManager(minio *miniokv.MinIOKV) *MinioChunkManager {
	return &MinioChunkManager{
		minio: minio,
	}
}

// GetPath returns the path of minio data if exist.
func (mcm *MinioChunkManager) GetPath(key string) (string, error) {
	if !mcm.Exist(key) {
		return "", errors.New("minio file manage cannot be found with key:" + key)
	}
	return key, nil
}

// Write writes the data to minio storage.
func (mcm *MinioChunkManager) Write(key string, content []byte) error {
	return mcm.minio.Save(key, string(content))
}

// Exist checks whether chunk is saved to minio storage.
func (mcm *MinioChunkManager) Exist(key string) bool {
	return mcm.minio.Exist(key)
}

// Read reads the minio storage data if exist.
func (mcm *MinioChunkManager) Read(key string) ([]byte, error) {
	results, err := mcm.minio.Load(key)
	return []byte(results), err
}

// ReadAt reads specific position data of minio storage if exist.
func (mcm *MinioChunkManager) ReadAt(key string, p []byte, off int64) (int, error) {
	results, err := mcm.minio.Load(key)
	if err != nil {
		return -1, err
	}

	if off < 0 || int64(len([]byte(results))) < off {
		return 0, errors.New("MinioChunkManager: invalid offset")
	}
	n := copy(p, []byte(results)[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}
