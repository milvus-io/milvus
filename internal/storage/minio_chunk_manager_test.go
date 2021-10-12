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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinioChunkManager_GetPath(t *testing.T) {
	bucketName := "minio-chunk-manager"
	kv, err := newMinIOKVClient(context.TODO(), bucketName)
	assert.Nil(t, err)

	minioMgr := NewMinioChunkManager(kv)
	path, err := minioMgr.GetPath("invalid")
	assert.Empty(t, path)
	assert.Error(t, err)
}

func TestMinioChunkManager_ReadAt(t *testing.T) {
	bucketName := "minio-chunk-manager"
	kv, err := newMinIOKVClient(context.TODO(), bucketName)
	assert.Nil(t, err)

	minioMgr := NewMinioChunkManager(kv)

	key := "1"
	bin := []byte{1, 2, 3}
	content := make([]byte, 8)
	err = minioMgr.minio.Remove(key)
	assert.Nil(t, err)

	n, err := minioMgr.ReadAt(key, content, 0)
	assert.Equal(t, n, -1)
	assert.Error(t, err)

	err = minioMgr.Write(key, bin)
	assert.Nil(t, err)

	n, err = minioMgr.ReadAt(key, content, -1)
	assert.Equal(t, n, 0)
	assert.Error(t, err)

	offset := int64(1)
	n, err = minioMgr.ReadAt(key, content, offset)
	assert.Error(t, err)
	assert.Equal(t, n, len(bin)-int(offset))
	for i := offset; i < int64(len(bin)); i++ {
		assert.Equal(t, content[i-offset], bin[i])
	}

	content = make([]byte, 2)
	n, err = minioMgr.ReadAt(key, content, offset)
	assert.Nil(t, err)
	assert.Equal(t, n, len(bin)-int(offset))
	for i := offset; i < int64(len(bin)); i++ {
		assert.Equal(t, content[i-offset], bin[i])
	}
}
