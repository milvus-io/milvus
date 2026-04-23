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

package binlog

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestListInsertLogs_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	// Files under two different field IDs; field 100 gets two files (out of order) to verify sorting.
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/100/file2"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/101/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/100/file1"})
			return nil
		}).Once()

	result, err := listInsertLogs(ctx, cm, "prefix/", 3)

	assert.NoError(t, err)
	assert.Equal(t, []string{"prefix/100/file1", "prefix/100/file2"}, result[100])
	assert.Equal(t, []string{"prefix/101/file1"}, result[101])
}

func TestListInsertLogs_RetryWithReset(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			callCount++
			if callCount == 1 {
				// Partial walk: emit two files then fail with a transient error.
				walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/100/file1"})
				walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/101/file1"})
				return errors.New("net/http: timeout awaiting response headers")
			}
			// Second call succeeds with the full three-file set.
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/100/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/100/file2"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/101/file1"})
			return nil
		}).Times(2)

	result, err := listInsertLogs(ctx, cm, "prefix/", 3)

	assert.NoError(t, err)
	assert.Equal(t, 2, callCount, "should have retried exactly once")
	// The map must contain exactly 3 files — no duplicates from the first partial walk.
	assert.Equal(t, []string{"prefix/100/file1", "prefix/100/file2"}, result[100])
	assert.Equal(t, []string{"prefix/101/file1"}, result[101])
	totalFiles := 0
	for _, paths := range result {
		totalFiles += len(paths)
	}
	assert.Equal(t, 3, totalFiles, "accumulated map must be reset between retries; no duplicates expected")
}

func TestListInsertLogs_NonRetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			callCount++
			return merr.WrapErrIoPermissionDenied("prefix/", errors.New("access denied"))
		}).Once()

	_, err := listInsertLogs(ctx, cm, "prefix/", 5)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, 1, callCount, "non-retryable error must fail fast without retrying")
}

func TestListInsertLogs_ParseFieldIDError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	// File path where the parent directory is a non-numeric string ("badID").
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/badID/file1"})
			return nil
		}).Once()

	_, err := listInsertLogs(ctx, cm, "prefix/", 3)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrImportFailed), "parse error must be wrapped as an import failure")
}
