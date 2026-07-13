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

package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type profileTestReader struct {
	*strings.Reader
	closeErr error
}

func (r *profileTestReader) Close() error         { return r.closeErr }
func (r *profileTestReader) Size() (int64, error) { return int64(r.Len()), nil }

type profileObjectStorage struct {
	content string
	getErr  error
}

func (s *profileObjectStorage) GetObject(context.Context, string, string, int64, int64) (FileReader, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &profileTestReader{Reader: strings.NewReader(s.content)}, nil
}
func (*profileObjectStorage) PutObject(context.Context, string, string, io.Reader, int64) error {
	return nil
}
func (s *profileObjectStorage) StatObject(context.Context, string, string) (int64, error) {
	return int64(len(s.content)), nil
}
func (*profileObjectStorage) WalkWithObjects(context.Context, string, string, bool, ChunkObjectWalkFunc) error {
	return nil
}
func (*profileObjectStorage) RemoveObject(context.Context, string, string) error { return nil }
func (*profileObjectStorage) CopyObject(context.Context, string, string, string) error {
	return nil
}

func TestInstrumentedFileReaderFinishesExactlyOnce(t *testing.T) {
	attribution := storageprofile.Attribution{
		Component:     "querynode",
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  storageprofile.WorkloadKindQuery,
		StorageRole:   storageprofile.StorageRolePersistent,
	}
	recorder := storageprofile.NewRecorder(attribution)
	operation := recorder.BeginOperation(storageprofile.OperationMeta{
		Operation:               storageprofile.StorageOperationRead,
		StreamingTTFBObservable: true,
	})
	reader := newInstrumentedFileReader(&profileTestReader{Reader: strings.NewReader("profile")}, operation)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "profile", string(data))
	require.NoError(t, reader.Close())

	profile := recorder.Snapshot()
	stats := profile.Operations[storageprofile.StorageOperationRead]
	assert.Equal(t, uint64(1), stats.Count)
	assert.Equal(t, uint64(1), stats.Success)
	assert.Equal(t, uint64(len(data)), stats.BytesCompleted)
	assert.Equal(t, uint64(1), stats.TTFB.Count)
}

func TestLocalChunkManagerProfilesFailureCategoriesWithoutChangingErrors(t *testing.T) {
	root := t.TempDir()
	manager := NewLocalChunkManager(objectstorage.RootPath(root))
	attribution := storageprofile.Attribution{
		Component:     "querynode",
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  storageprofile.WorkloadKindQuery,
		StorageRole:   storageprofile.StorageRolePersistent,
	}
	recorder := storageprofile.NewRecorder(attribution)
	ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)

	exists, err := manager.Exist(ctx, filepath.Join(root, "missing"))
	require.NoError(t, err)
	assert.False(t, exists)

	_, err = manager.ReadAt(ctx, filepath.Join(root, "missing"), -1, 1)
	require.ErrorIs(t, err, io.EOF)

	shortPath := filepath.Join(root, "short")
	require.NoError(t, os.WriteFile(shortPath, []byte("x"), 0o600))
	_, err = manager.ReadAt(ctx, shortPath, 0, 2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, io.EOF) || strings.Contains(err.Error(), io.EOF.Error()))

	profile := recorder.Snapshot()
	stat := profile.Operations[storageprofile.StorageOperationStat]
	assert.Equal(t, uint64(1), stat.Errors[storageprofile.ErrorCategoryNotFound])
	rangeRead := profile.Operations[storageprofile.StorageOperationRangeRead]
	assert.Equal(t, uint64(1), rangeRead.Errors[storageprofile.ErrorCategoryInvalidRange])
	assert.Equal(t, uint64(1), rangeRead.Errors[storageprofile.ErrorCategoryUnexpectedEOF])
}

func TestRemoteChunkManagerProfilesShortRangeReadWithoutChangingReturnContract(t *testing.T) {
	attribution := storageprofile.Attribution{
		Component:     "querynode",
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  storageprofile.WorkloadKindQuery,
		StorageRole:   storageprofile.StorageRolePersistent,
	}
	recorder := storageprofile.NewRecorder(attribution)
	ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)
	manager := &RemoteChunkManager{
		client:      &profileObjectStorage{content: "short"},
		bucketName:  "bucket",
		backendKind: storageprofile.BackendKindS3Compatible,
	}

	data, err := manager.ReadAt(ctx, "key", 0, 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("short"), data)

	profile := recorder.Snapshot()
	stats := profile.Operations[storageprofile.StorageOperationRangeRead]
	assert.Equal(t, uint64(1), stats.Count)
	assert.Equal(t, uint64(1), stats.Failed)
	assert.Equal(t, uint64(1), stats.Errors[storageprofile.ErrorCategoryUnexpectedEOF])
	assert.Equal(t, uint64(len(data)), stats.BytesCompleted)
}

func TestRemoteChunkManagerProfilesVisibleFailureMatrix(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		category storageprofile.ErrorCategory
		outcome  storageprofile.OperationOutcome
		target   error
	}{
		{name: "throttled", err: merr.WrapErrIoTooManyRequests("key", errors.New("slow down")), category: storageprofile.ErrorCategoryThrottled, outcome: storageprofile.OutcomeFailure, target: merr.ErrIoTooManyRequests},
		{name: "permission", err: merr.WrapErrIoPermissionDenied("key", errors.New("denied")), category: storageprofile.ErrorCategoryPermissionDenied, outcome: storageprofile.OutcomeFailure, target: merr.ErrIoPermissionDenied},
		{name: "invalid credentials observability", err: storageprofile.WithErrorCategory(merr.WrapErrIoPermissionDenied("key", errors.New("bad key")), storageprofile.ErrorCategoryInvalidCredentials), category: storageprofile.ErrorCategoryInvalidCredentials, outcome: storageprofile.OutcomeFailure, target: merr.ErrIoPermissionDenied},
		{name: "timeout", err: context.DeadlineExceeded, category: storageprofile.ErrorCategoryTimeout, outcome: storageprofile.OutcomeTimeout, target: context.DeadlineExceeded},
		{name: "canceled", err: context.Canceled, category: storageprofile.ErrorCategoryCanceled, outcome: storageprofile.OutcomeCanceled, target: context.Canceled},
		{name: "generic io", err: merr.WrapErrIoFailed("key", errors.New("disk")), category: storageprofile.ErrorCategoryIOFailed, outcome: storageprofile.OutcomeFailure, target: merr.ErrIoFailed},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			attribution := storageprofile.Attribution{
				Component:     "querynode",
				WorkloadClass: storageprofile.WorkloadClassRequestPath,
				WorkloadKind:  storageprofile.WorkloadKindQuery,
				StorageRole:   storageprofile.StorageRolePersistent,
			}
			recorder := storageprofile.NewRecorder(attribution)
			ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)
			manager := &RemoteChunkManager{
				client:      &profileObjectStorage{getErr: testCase.err},
				bucketName:  "bucket",
				backendKind: storageprofile.BackendKindS3Compatible,
			}

			reader, err := manager.Reader(ctx, "key")
			require.Nil(t, reader)
			require.ErrorIs(t, err, testCase.target)

			stats := recorder.Snapshot().Operations[storageprofile.StorageOperationRead]
			assert.Equal(t, uint64(1), stats.Count)
			assert.Equal(t, uint64(1), stats.Errors[testCase.category])
			switch testCase.outcome {
			case storageprofile.OutcomeCanceled:
				assert.Equal(t, uint64(1), stats.Canceled)
			case storageprofile.OutcomeTimeout:
				assert.Equal(t, uint64(1), stats.TimedOut)
			default:
				assert.Equal(t, uint64(1), stats.Failed)
			}
		})
	}
}
