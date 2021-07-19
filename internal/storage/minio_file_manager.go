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

type MinioFileManager struct {
	minio *miniokv.MinIOKV
}

func NewMinioFileManager(minio *miniokv.MinIOKV) *MinioFileManager {
	return &MinioFileManager{
		minio: minio,
	}
}

func (mfm *MinioFileManager) GetFile(key string) (string, error) {
	if !mfm.Exist(key) {
		return "", errors.New("minio file manage cannot be found with key:" + key)
	}
	return key, nil
}

func (mfm *MinioFileManager) PutFile(key string, content []byte) error {
	return mfm.minio.Save(key, string(content))
}

func (mfm *MinioFileManager) Exist(key string) bool {
	return mfm.minio.Exist(key)
}

func (mfm *MinioFileManager) ReadAll(key string) ([]byte, error) {
	results, err := mfm.minio.Load(key)
	return []byte(results), err
}

func (mfm *MinioFileManager) ReadAt(key string, p []byte, off int64) (n int, err error) {
	return 0, errors.New("Minio file manager cannot readat")
}
