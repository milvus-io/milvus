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

package common

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestWalkWithPrefixRetry_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	var collected []string
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/file2"})
			return nil
		}).Once()

	err := WalkWithPrefixRetry(ctx, cm, "prefix/", true, 3,
		func() { collected = nil },
		func(info *storage.ChunkObjectInfo) bool {
			collected = append(collected, info.FilePath)
			return true
		})

	assert.NoError(t, err)
	assert.Equal(t, []string{"prefix/file1", "prefix/file2"}, collected)
}

func TestWalkWithPrefixRetry_TransientErrorRetried(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	var collected []string
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			callCount++
			if callCount < 3 {
				// Simulate partial walk then timeout
				walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/partial"})
				return errors.New("net/http: timeout awaiting response headers")
			}
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: "prefix/file2"})
			return nil
		}).Times(3)

	err := WalkWithPrefixRetry(ctx, cm, "prefix/", true, 5,
		func() { collected = nil },
		func(info *storage.ChunkObjectInfo) bool {
			collected = append(collected, info.FilePath)
			return true
		})

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount, "should have retried twice then succeeded")
	// Verify reset worked: only final attempt's results, no partial duplicates
	assert.Equal(t, []string{"prefix/file1", "prefix/file2"}, collected)
}

func TestWalkWithPrefixRetry_NonRetryableErrorFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			callCount++
			return merr.WrapErrIoPermissionDenied("prefix/", errors.New("access denied"))
		}).Once()

	err := WalkWithPrefixRetry(ctx, cm, "prefix/", true, 5,
		func() {},
		func(info *storage.ChunkObjectInfo) bool {
			return true
		})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, 1, callCount, "should not retry non-retryable errors")
}

func TestWalkWithPrefixRetry_ExhaustsRetries(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			callCount++
			return errors.New("net/http: timeout awaiting response headers")
		}).Times(3)

	err := WalkWithPrefixRetry(ctx, cm, "prefix/", true, 3,
		func() {},
		func(info *storage.ChunkObjectInfo) bool {
			return true
		})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoFailed), "timeout should be mapped to ErrIoFailed")
	assert.Equal(t, 3, callCount, "should exhaust all retry attempts")
}

func TestWalkWithPrefixRetry_ResetCalledEachAttempt(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	resetCount := 0
	callCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, "prefix/", true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			callCount++
			if callCount < 2 {
				return errors.New("transient error")
			}
			return nil
		}).Times(2)

	err := WalkWithPrefixRetry(ctx, cm, "prefix/", true, 3,
		func() { resetCount++ },
		func(info *storage.ChunkObjectInfo) bool {
			return true
		})

	assert.NoError(t, err)
	assert.Equal(t, 2, resetCount, "resetFunc should be called before each attempt")
}
