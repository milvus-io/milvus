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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGetFilesSizeWithRetry_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	cm.EXPECT().Size(mock.Anything, "file1").Return(int64(10), nil).Once()
	cm.EXPECT().Size(mock.Anything, "file2").Return(int64(20), nil).Once()

	total, err := getFilesSizeWithRetry(ctx, cm, []string{"file1", "file2"}, 3)

	assert.NoError(t, err)
	assert.Equal(t, int64(30), total)
}

// G2: the customer's exact failure mode — a transport header timeout — is transient
// and must be retried, not failed on the first attempt.
func TestGetFilesSizeWithRetry_TransientTimeoutRetried(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().Size(mock.Anything, "file1").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			callCount++
			if callCount < 3 {
				return 0, errors.New("net/http: timeout awaiting response headers")
			}
			return 42, nil
		}).Times(3)

	total, err := getFilesSizeWithRetry(ctx, cm, []string{"file1"}, 5)

	assert.NoError(t, err)
	assert.Equal(t, int64(42), total)
	assert.Equal(t, 3, callCount, "should have retried twice then succeeded")
}

func TestGetFilesSizeWithRetry_NonRetryableErrorFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().Size(mock.Anything, "file1").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			callCount++
			return 0, merr.WrapErrIoPermissionDenied("file1", errors.New("access denied"))
		}).Once()

	total, err := getFilesSizeWithRetry(ctx, cm, []string{"file1"}, 5)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, int64(0), total)
	assert.Equal(t, 1, callCount, "should not retry non-retryable errors")
}

// G2: a persistent timeout still fails, but only after exhausting all attempts —
// and the typed ErrIoFailed code survives to the caller.
func TestGetFilesSizeWithRetry_ExhaustsRetries(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().Size(mock.Anything, "file1").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			callCount++
			return 0, errors.New("net/http: timeout awaiting response headers")
		}).Times(3)

	_, err := getFilesSizeWithRetry(ctx, cm, []string{"file1"}, 3)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoFailed), "timeout should be mapped to ErrIoFailed")
	assert.Equal(t, 3, callCount, "should exhaust all retry attempts")
}

// A 0 attempts value must not spin forever: the core clamps it to a bounded default.
func TestGetFilesSizeWithRetry_ZeroAttemptsClamped(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().Size(mock.Anything, "file1").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			callCount++
			return 0, errors.New("net/http: timeout awaiting response headers")
		}).Times(10)

	_, err := getFilesSizeWithRetry(ctx, cm, []string{"file1"}, 0)

	assert.Error(t, err)
	assert.Equal(t, 10, callCount, "0 attempts must clamp to the bounded default, not retry forever")
}

// Codes the chunk manager's Size() already retries internally (IsRetryableErr:
// ErrIoTooManyRequests / ErrIoUnexpectEOF) must NOT be retried again by this outer
// layer, otherwise the two layers stack (up to retryAttempts^2 HEAD calls).
func TestGetFilesSizeWithRetry_InnerRetryableNotDoubleRetried(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().Size(mock.Anything, "file1").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			callCount++
			return 0, merr.WrapErrIoTooManyRequests("file1", errors.New("SlowDown"))
		}).Once()

	_, err := getFilesSizeWithRetry(ctx, cm, []string{"file1"}, 5)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoTooManyRequests))
	assert.Equal(t, 1, callCount, "outer layer must not re-retry inner-retryable codes")
}

// A canceled/expired context fails fast and surfaces the context error (not a
// timeout mapped to ErrIoFailed).
func TestGetFilesSizeWithRetry_ContextCanceledFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	callCount := 0
	cm.EXPECT().Size(mock.Anything, "file1").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			callCount++
			return 0, context.Canceled
		}).Once()

	_, err := getFilesSizeWithRetry(ctx, cm, []string{"file1"}, 5)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.Equal(t, 1, callCount, "should not retry a canceled context")
}

// Retry is per-file: a file that already succeeded is not re-stated when a later
// file hits a transient error.
func TestGetFilesSizeWithRetry_PerFileRetry(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	cm.EXPECT().Size(mock.Anything, "file1").Return(int64(5), nil).Once()
	file2Calls := 0
	cm.EXPECT().Size(mock.Anything, "file2").
		RunAndReturn(func(ctx context.Context, path string) (int64, error) {
			file2Calls++
			if file2Calls < 2 {
				return 0, errors.New("net/http: timeout awaiting response headers")
			}
			return 7, nil
		}).Times(2)

	total, err := getFilesSizeWithRetry(ctx, cm, []string{"file1", "file2"}, 5)

	assert.NoError(t, err)
	assert.Equal(t, int64(12), total)
	assert.Equal(t, 2, file2Calls, "only the failing file should be retried")
}
