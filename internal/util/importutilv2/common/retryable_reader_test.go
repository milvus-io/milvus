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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storageprofile"
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

type closeAwareReader struct {
	storage.FileReader
	closed         bool
	readAfterClose int
}

func (r *closeAwareReader) Read(p []byte) (int, error) {
	if r.closed {
		r.readAfterClose++
		return 0, errors.New("read after close")
	}
	return r.FileReader.Read(p)
}

func (r *closeAwareReader) Close() error {
	r.closed = true
	return r.FileReader.Close()
}

func newSizeFunc(size int64) ReaderSizeFunc {
	return func(context.Context, string) (int64, error) {
		return size, nil
	}
}

func newReopenFunc(content string, openCount *int, offsets *[]int64) ReopenReaderFunc {
	return func(_ context.Context, _ string, offset int64) (storage.FileReader, error) {
		if openCount != nil {
			*openCount = *openCount + 1
		}
		if offsets != nil {
			*offsets = append(*offsets, offset)
		}
		if offset < 0 || offset > int64(len(content)) {
			return nil, io.EOF
		}
		return NewMockReader(content[offset:]), nil
	}
}

func TestRetryableReader_ReopenOnPrematureEOF(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "hello world"
	openCount := 0
	offsets := make([]int64, 0, 1)
	reopen := newReopenFunc(content, &openCount, &offsets)

	reader := NewRetryableReaderWithReopen(ctx, path, NewPrematureEOFReader(content, 5), reopen, newSizeFunc(int64(len(content))))
	buf := make([]byte, len(content))
	n, err := io.ReadFull(reader, buf)

	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, string(buf))
	assert.Equal(t, 1, openCount)
	assert.Equal(t, []int64{5}, offsets)
}

func TestRetryableReaderProfilesOneLogicalReadAcrossReopen(t *testing.T) {
	path := "/test/path"
	content := "hello world"
	attribution := storageprofile.Attribution{
		ScopeType:       storageprofile.ScopeTypeTask,
		Component:       "datanode",
		WorkloadClass:   storageprofile.WorkloadClassBackground,
		WorkloadKind:    storageprofile.WorkloadKindImport,
		WorkloadSubtype: storageprofile.WorkloadSubtypeIngest,
		Phase:           storageprofile.WorkloadPhaseReadSource,
		StorageRole:     storageprofile.StorageRoleSource,
	}
	recorder := storageprofile.NewRecorder(attribution)
	ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)
	reopen := func(ctx context.Context, _ string, offset int64) (storage.FileReader, error) {
		// A reusable lower layer may attempt to observe its own open/read. The
		// retrying reader must suppress that nested observation.
		nested := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRead})
		nested.Finish(storageprofile.OperationResult{})
		return NewMockReader(content[offset:]), nil
	}

	reader := NewRetryableReaderWithReopen(ctx, path, NewPrematureEOFReader(content, 5), reopen, newSizeFunc(int64(len(content))))
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, content, string(data))

	profile := recorder.Snapshot()
	stats := profile.Operations[storageprofile.StorageOperationRead]
	assert.Equal(t, uint64(1), stats.Count)
	assert.Equal(t, uint64(1), stats.Success)
	assert.Equal(t, uint64(1), stats.Retried)
	assert.Equal(t, uint64(len(content)), stats.BytesCompleted)
	assert.Equal(t, uint64(1), stats.TTFB.Count)
}

func TestRetryableReaderErrorThenCloseFinishesOnce(t *testing.T) {
	attribution := storageprofile.Attribution{
		ScopeType:     storageprofile.ScopeTypeTask,
		Component:     "datanode",
		WorkloadClass: storageprofile.WorkloadClassBackground,
		WorkloadKind:  storageprofile.WorkloadKindImport,
		Phase:         storageprofile.WorkloadPhaseReadSource,
		StorageRole:   storageprofile.StorageRoleSource,
	}
	recorder := storageprofile.NewRecorder(attribution)
	ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)
	reader := NewRetryableReader(ctx, "/test/path", &customMockReader{
		readFunc: func([]byte) (int, error) { return 0, context.Canceled },
	})

	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, reader.Close())

	profile := recorder.Snapshot()
	stats := profile.Operations[storageprofile.StorageOperationRead]
	assert.Equal(t, uint64(1), stats.Count)
	assert.Equal(t, uint64(1), stats.Canceled)
	assert.Equal(t, uint64(0), stats.Success)
}

func TestRetryableReader_FinalEOFIsNotRetried(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "done"
	openCount := 0
	reopen := newReopenFunc(content, &openCount, nil)

	reader := NewRetryableReaderWithReopen(ctx, path, NewMockReader(content), reopen, newSizeFunc(int64(len(content))))
	buf := make([]byte, len(content))
	n, err := io.ReadFull(reader, buf)
	assert.NoError(t, err)
	assert.Equal(t, len(content), n)

	n, err = reader.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, openCount)
}

func TestRetryableReader_ReopenOnRetryableError(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "data after retry"
	openCount := 0
	reopen := newReopenFunc(content, &openCount, nil)

	reader := NewRetryableReaderWithReopen(ctx, path, newErrorMockReader(content, errors.New("network timeout"), 1), reopen, newSizeFunc(int64(len(content))))
	buf := make([]byte, len(content))
	n, err := reader.Read(buf)

	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, string(buf))
	assert.Equal(t, 1, openCount)
}

func TestRetryableReader_ExhaustedPrematureEOFReturnsUnexpectedEOF(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "hello world"
	reopen := func(_ context.Context, _ string, offset int64) (storage.FileReader, error) {
		if offset < 0 || offset > int64(len(content)) {
			return nil, io.EOF
		}
		return NewPrematureEOFReader(content[offset:], 0), nil
	}

	reader := &retryableReader{
		FileReader:    NewPrematureEOFReader(content, 5),
		ctx:           ctx,
		path:          path,
		retryAttempts: 2,
		reopen:        reopen,
		sizeFunc:      newSizeFunc(int64(len(content))),
		size:          -1,
	}
	buf := make([]byte, len(content))
	n, err := io.ReadFull(reader, buf)

	assert.ErrorIs(t, err, merr.ErrIoUnexpectEOF)
	assert.False(t, errors.Is(err, io.EOF))
	assert.Equal(t, 5, n)
}

func TestRetryableReader_UsesSizeFuncForPrematureEOF(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "hello world"
	openCount := 0
	reopen := newReopenFunc(content, &openCount, nil)

	reader := NewRetryableReaderWithReopen(ctx, path, NewPrematureEOFReaderWithSize(content, 5, 0), reopen, newSizeFunc(int64(len(content))))
	buf := make([]byte, len(content))
	n, err := io.ReadFull(reader, buf)

	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, string(buf))
	assert.Equal(t, 1, openCount)
}

func TestRetryableReader_UsesSizeFuncForImmediateEOF(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "hello world"
	openCount := 0
	reopen := newReopenFunc(content, &openCount, nil)

	reader := NewRetryableReaderWithReopen(ctx, path, NewPrematureEOFReaderWithSize(content, 0, 0), reopen, newSizeFunc(int64(len(content))))
	buf := make([]byte, len(content))
	n, err := io.ReadFull(reader, buf)

	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, string(buf))
	assert.Equal(t, 1, openCount)
}

func TestRetryableReader_DoesNotReadClosedReaderAfterFailedReopen(t *testing.T) {
	ctx := context.Background()
	path := "/test/path"
	content := "hello world"
	initialReader := &closeAwareReader{FileReader: NewPrematureEOFReader(content, 0)}
	openCount := 0
	reopen := func(_ context.Context, _ string, offset int64) (storage.FileReader, error) {
		openCount++
		if openCount == 1 {
			return nil, errors.New("temporary reopen failure")
		}
		return NewMockReader(content[offset:]), nil
	}

	reader := &retryableReader{
		FileReader:    initialReader,
		ctx:           ctx,
		path:          path,
		retryAttempts: 3,
		reopen:        reopen,
		sizeFunc:      newSizeFunc(int64(len(content))),
		size:          -1,
	}
	buf := make([]byte, len(content))
	n, err := io.ReadFull(reader, buf)

	assert.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, string(buf))
	assert.Equal(t, 2, openCount)
	assert.Equal(t, 0, initialReader.readAfterClose)
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
