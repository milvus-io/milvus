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
	"io"
	"math"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	paramtable.Init()
}

func TestRetryableReader_ReadSuccess(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	expectedData := "hello world"

	mockReader := NewMockReader(expectedData)

	reader := NewRetryableReader(ctx, path, mockReader)

	buf := make([]byte, len(expectedData))
	n, err := reader.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(expectedData), n)
	assert.Equal(t, expectedData, string(buf))

	buf = make([]byte, len(expectedData))
	n, err = reader.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
}

func TestRetryableReader_ReadWithRetryableError(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	expectedData := "data after retry"

	mockReader := newErrorMockReader(expectedData,
		minio.ErrorResponse{
			Code: "TooManyRequestsException",
		},
		3,
	)

	reader := NewRetryableReader(ctx, path, mockReader)
	buf := make([]byte, len(expectedData))
	n, err := reader.Read(buf)

	assert.NoError(t, err)
	assert.Equal(t, len(expectedData), n)
	assert.Equal(t, expectedData, string(buf))
}

func TestRetryableReader_ReadWithNonRetryableError(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"

	mockReader := newErrorMockReader("",
		merr.WrapErrIoFailed(path, io.ErrNoProgress),
		math.MaxInt,
	)

	reader := NewRetryableReader(ctx, path, mockReader)
	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
	assert.Equal(t, 0, n)
}

// customMockReader allows custom read behavior with call count tracking
type customMockReader struct {
	readFunc  func(p []byte) (int, error)
	callCount int
}

func (m *customMockReader) Read(p []byte) (int, error) {
	m.callCount++
	return m.readFunc(p)
}

func (m *customMockReader) Close() error {
	return nil
}

func (m *customMockReader) Size() (int64, error) {
	return 0, nil
}

func (m *customMockReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (m *customMockReader) ReadAt(p []byte, off int64) (int, error) {
	return 0, nil
}

func TestRetryableReader_DenylistRetry_NonRetryableErrors(t *testing.T) {
	ctx := context.Background()
	nonRetryableErrors := []error{
		merr.ErrIoKeyNotFound,
		merr.ErrIoPermissionDenied,
		merr.ErrIoBucketNotFound,
		merr.ErrIoInvalidArgument,
		merr.ErrIoInvalidRange,
	}

	for _, testErr := range nonRetryableErrors {
		t.Run(testErr.Error(), func(t *testing.T) {
			// Mock reader that always returns the non-retryable error
			mockReader := &customMockReader{
				readFunc: func(p []byte) (int, error) {
					return 0, testErr
				},
			}

			reader := &retryableReader{
				FileReader:    mockReader,
				ctx:           ctx,
				path:          "test/path",
				retryAttempts: 10,
			}

			// Attempt read - should fail immediately without retries
			buf := make([]byte, 10)
			n, err := reader.Read(buf)

			// Verify error is returned
			assert.Error(t, err)
			assert.True(t, errors.Is(err, testErr))
			assert.Equal(t, 0, n)

			// Verify only 1 attempt was made (no retries)
			assert.Equal(t, 1, mockReader.callCount)
		})
	}
}

func TestRetryableReader_DenylistRetry_RetryableErrors(t *testing.T) {
	ctx := context.Background()

	// Mock reader that fails 3 times then succeeds
	callCount := 0
	mockReader := &customMockReader{
		readFunc: func(p []byte) (int, error) {
			callCount++
			if callCount < 4 {
				return 0, errors.New("network timeout")
			}
			copy(p, []byte("success"))
			return 7, nil
		},
	}

	reader := &retryableReader{
		FileReader:    mockReader,
		ctx:           ctx,
		path:          "test/path",
		retryAttempts: 10,
	}

	// Attempt read - should retry and succeed on 4th attempt
	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	assert.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, []byte("success"), buf[:n])
	assert.Equal(t, 4, callCount, "should retry 3 times then succeed")
}

func TestRetryableReader_DenylistRetry_EOFHandling(t *testing.T) {
	ctx := context.Background()

	// Mock reader that returns EOF immediately
	mockReader := &customMockReader{
		readFunc: func(p []byte) (int, error) {
			return 0, io.EOF
		},
	}

	reader := &retryableReader{
		FileReader:    mockReader,
		ctx:           ctx,
		path:          "test/path",
		retryAttempts: 10,
	}

	// Read should return EOF immediately without retries
	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, 1, mockReader.callCount, "EOF should not be retried")
}

func TestRetryableReader_ContextCanceled(t *testing.T) {
	ctx := context.Background()

	mockReader := &customMockReader{
		readFunc: func(p []byte) (int, error) {
			return 0, context.Canceled
		},
	}

	reader := &retryableReader{
		FileReader:    mockReader,
		ctx:           ctx,
		path:          "test/path",
		retryAttempts: 10,
	}

	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, n)
	assert.Equal(t, 1, mockReader.callCount, "context.Canceled should not be retried")
}

func TestRetryableReader_ContextDeadlineExceeded(t *testing.T) {
	ctx := context.Background()

	mockReader := &customMockReader{
		readFunc: func(p []byte) (int, error) {
			return 0, context.DeadlineExceeded
		},
	}

	reader := &retryableReader{
		FileReader:    mockReader,
		ctx:           ctx,
		path:          "test/path",
		retryAttempts: 10,
	}

	buf := make([]byte, 10)
	n, err := reader.Read(buf)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 0, n)
	assert.Equal(t, 1, mockReader.callCount, "context.DeadlineExceeded should not be retried")
}
