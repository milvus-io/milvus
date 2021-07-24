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

	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
)

type MinioChunkManager struct {
	minio *miniokv.MinIOKV
}

func NewMinioChunkManager(minio *miniokv.MinIOKV) *MinioChunkManager {
	return &MinioChunkManager{
		minio: minio,
	}
}

func (mcm *MinioChunkManager) GetPath(key string) (string, error) {
	if !mcm.Exist(key) {
		return "", errors.New("minio file manage cannot be found with key:" + key)
	}
	return key, nil
}

func (mcm *MinioChunkManager) Write(key string, content []byte) error {
	return mcm.minio.Save(key, string(content))
}

func (mcm *MinioChunkManager) Exist(key string) bool {
	return mcm.minio.Exist(key)
}

func (mcm *MinioChunkManager) Read(key string) ([]byte, error) {
	results, err := mcm.minio.Load(key)
	return []byte(results), err
}

func (mcm *MinioChunkManager) ReadAt(key string, p []byte, off int64) (n int, err error) {
	return 0, errors.New("Minio file manager cannot readat")
}
