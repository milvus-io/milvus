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

package objstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
)

func newLocal(t *testing.T) (*storage.LocalChunkManager, string) {
	root := t.TempDir()
	return storage.NewLocalChunkManager(objectstorage.RootPath(root)), root
}

func TestUploadFile(t *testing.T) {
	ctx := context.Background()
	cm, root := newLocal(t)
	src := filepath.Join(t.TempDir(), "src.sst")
	require.NoError(t, os.WriteFile(src, []byte("table bytes"), 0o600))

	key := filepath.Join(root, "pkindex", "1.sst")
	require.NoError(t, New(cm).UploadFile(ctx, key, src))

	data, err := cm.Read(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("table bytes"), data)
}

func TestUploadFileMissingSource(t *testing.T) {
	cm, root := newLocal(t)
	err := New(cm).UploadFile(context.Background(), filepath.Join(root, "k"), filepath.Join(t.TempDir(), "missing"))
	assert.True(t, errors.Is(err, pkindex.ErrIO))
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable))
}

// writeFailingChunkManager returns writeErr from Write.
type writeFailingChunkManager struct {
	storage.ChunkManager
	writeErr error
}

func (cm *writeFailingChunkManager) Write(context.Context, string, []byte) error {
	return cm.writeErr
}

func TestUploadFileWriteFailure(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.sst")
	require.NoError(t, os.WriteFile(src, []byte("table bytes"), 0o600))

	cm := &writeFailingChunkManager{writeErr: errors.New("connection reset")}
	err := New(cm).UploadFile(context.Background(), "key", src)
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.False(t, errors.Is(err, pkindex.ErrIO))
}

func TestUploadFileLocalChunkManagerWriteFailure(t *testing.T) {
	ctx := context.Background()
	cm, root := newLocal(t)
	// a regular file where the chunk manager needs a directory
	blocker := filepath.Join(root, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("file"), 0o600))
	src := filepath.Join(t.TempDir(), "src.sst")
	require.NoError(t, os.WriteFile(src, []byte("table bytes"), 0o600))

	err := New(cm).UploadFile(ctx, filepath.Join(blocker, "1.sst"), src)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkindex.ErrIO))
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable))
}

func TestUploadFilePutObjectFailure(t *testing.T) {
	// The server refuses every request with a status that MinIO does not retry.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	client, err := minio.New(srv.Listener.Addr().String(), &minio.Options{
		Creds:  credentials.NewStaticV4("ak", "sk", ""),
		Region: "us-east-1",
	})
	require.NoError(t, err)
	cm := storage.NewRemoteChunkManagerForTesting(client, "bucket", "root")

	src := filepath.Join(t.TempDir(), "src.sst")
	require.NoError(t, os.WriteFile(src, []byte("table bytes"), 0o600))

	err = New(cm).UploadFile(context.Background(), "root/1.sst", src)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.False(t, errors.Is(err, pkindex.ErrIO))
}

func TestDownloadFile(t *testing.T) {
	ctx := context.Background()
	cm, root := newLocal(t)
	key := filepath.Join(root, "pkindex", "1.sst")
	require.NoError(t, cm.Write(ctx, key, []byte("table bytes")))

	dir := t.TempDir()
	dst := filepath.Join(dir, "1.sst")
	require.NoError(t, New(cm).DownloadFile(ctx, key, dst))

	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, []byte("table bytes"), data)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "1.sst", entries[0].Name())
}

func TestDownloadFileMissingKey(t *testing.T) {
	cm, root := newLocal(t)
	dir := t.TempDir()
	dst := filepath.Join(dir, "1.sst")

	err := New(cm).DownloadFile(context.Background(), filepath.Join(root, "missing.sst"), dst)
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
	assert.False(t, errors.Is(err, pkindex.ErrIO))
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.NoFileExists(t, dst)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// failingReader returns data and then readErr from Read, the way a remote
// reader that sends its request lazily or loses its connection reports a
// failure.
type failingReader struct {
	storage.FileReader
	data    []byte
	readErr error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.readErr
}

func (r *failingReader) Close() error { return nil }

// failingChunkManager returns readerErr from Reader, or a failingReader that
// returns data and then readErr.
type failingChunkManager struct {
	storage.ChunkManager
	readerErr error
	data      []byte
	readErr   error
}

func (cm *failingChunkManager) Reader(context.Context, string) (storage.FileReader, error) {
	if cm.readerErr != nil {
		return nil, cm.readerErr
	}
	return &failingReader{data: cm.data, readErr: cm.readErr}, nil
}

func TestDownloadFileReadFailure(t *testing.T) {
	cases := []struct {
		name      string
		cm        *failingChunkManager
		corrupted bool
	}{
		{"reader error", &failingChunkManager{readerErr: errors.New("connection refused")}, false},
		{"first read error", &failingChunkManager{readErr: errors.New("connection reset")}, false},
		{"mid copy read error", &failingChunkManager{data: []byte("partial"), readErr: errors.New("connection reset")}, false},
		{"minio no such key", &failingChunkManager{readErr: minio.ErrorResponse{Code: minioNoSuchKey}}, true},
		{"azure blob not found", &failingChunkManager{readErr: &azcore.ResponseError{ErrorCode: azureBlobNotFound}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			dst := filepath.Join(dir, "1.sst")

			err := New(c.cm).DownloadFile(context.Background(), "key", dst)
			require.Error(t, err)
			assert.Equal(t, c.corrupted, errors.Is(err, pkindex.ErrCorrupted))
			assert.Equal(t, !c.corrupted, errors.Is(err, pkindex.ErrUnavailable))
			assert.False(t, errors.Is(err, pkindex.ErrIO))
			assert.NoFileExists(t, dst)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestDownloadFileLocalWriteFailure(t *testing.T) {
	ctx := context.Background()
	cm, root := newLocal(t)
	key := filepath.Join(root, "pkindex", "1.sst")
	require.NoError(t, cm.Write(ctx, key, []byte("table bytes")))

	// The target directory does not exist, so the temp file cannot be created.
	dst := filepath.Join(t.TempDir(), "missing", "1.sst")
	err := New(cm).DownloadFile(ctx, key, dst)
	assert.True(t, errors.Is(err, pkindex.ErrIO))
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.False(t, errors.Is(err, pkindex.ErrCorrupted))
}

func TestReadObject(t *testing.T) {
	ctx := context.Background()
	cm, root := newLocal(t)
	key := filepath.Join(root, "pkindex", "manifest", "1")
	require.NoError(t, cm.Write(ctx, key, []byte("manifest bytes")))

	data, err := New(cm).ReadObject(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("manifest bytes"), data)
}

func TestReadObjectMissing(t *testing.T) {
	cm, root := newLocal(t)
	_, err := New(cm).ReadObject(context.Background(), filepath.Join(root, "pkindex", "manifest", "missing"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted), err)
	assert.False(t, errors.Is(err, pkindex.ErrUnavailable), err)
}

// failingWriter returns writeErr from Write.
type failingWriter struct{ writeErr error }

func (w *failingWriter) Write([]byte) (int, error) { return 0, w.writeErr }

func TestRecordingWriter(t *testing.T) {
	writeErr := errors.New("no space left on device")
	w := &recordingWriter{w: &failingWriter{writeErr: writeErr}}
	_, err := io.Copy(w, &failingReader{data: []byte("bytes"), readErr: io.EOF})
	assert.True(t, errors.Is(err, writeErr))
	assert.True(t, errors.Is(w.err, writeErr))

	var sink []byte
	w = &recordingWriter{w: writerFunc(func(p []byte) (int, error) {
		sink = append(sink, p...)
		return len(p), nil
	})}
	readErr := errors.New("connection reset")
	_, err = io.Copy(w, &failingReader{data: []byte("bytes"), readErr: readErr})
	assert.True(t, errors.Is(err, readErr))
	assert.NoError(t, w.err)
	assert.Equal(t, []byte("bytes"), sink)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
