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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed), "parse-field-id IO error must be wrapped as a server-side import failure")
}

func TestVerifyPackedUsesShortColumnGroupAsCountReference(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
	}}
	insertLogs := map[int64][]string{
		storagecommon.DefaultShortColumnGroupID: {"objects/short/1"},
		101:                                     {"objects/101/1"},
	}

	valid, cloned, err := verify(schema, storage.StorageV3, insertLogs)
	assert.NoError(t, err)
	assert.Equal(t, insertLogs, valid)
	assert.Same(t, schema, cloned)
}

func TestCreateFieldBinlogListIsSorted(t *testing.T) {
	binlogs := createFieldBinlogList(map[int64][]string{101: {"101"}, 0: {"0"}, 100: {"100"}})
	assert.Equal(t, []int64{0, 100, 101}, []int64{binlogs[0].GetFieldID(), binlogs[1].GetFieldID(), binlogs[2].GetFieldID()})
}
