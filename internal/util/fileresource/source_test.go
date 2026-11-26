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

package fileresource

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func setSourceRoot(t *testing.T, root string) {
	t.Helper()
	paramtable.Init()
	setting := &paramtable.Get().CommonCfg.FileResourceAllowedPath
	old := setting.SwapTempValue(root)
	t.Cleanup(func() { setting.SwapTempValue(old) })
}

func TestFileResourceSourceRejectsBeforeProbing(t *testing.T) {
	cm := mocks.NewChunkManager(t) // No backend calls are permitted.
	source := &restrictedSource{ChunkManager: cm, root: "resources"}
	for _, key := range []string{"other/secret", "resources2/file", "/resources/file", "resources/../secret", "resources/", "resources/./file", "resources/a\\b", "resources/a\x00b"} {
		_, err := source.Exist(context.Background(), key)
		require.Error(t, err, key)
		_, err = source.Size(context.Background(), key)
		require.Error(t, err, key)
		_, err = source.Reader(context.Background(), key)
		require.Error(t, err, key)
	}
	source.root = ""
	_, err := source.Exist(context.Background(), "secret")
	require.Error(t, err)
}

func TestFileResourceLocalSourceIsConfined(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "dict"), []byte("ok"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("SECRET_MARKER"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "link")))
	require.NoError(t, os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "file_link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(root, "fifo"), 0o600))
	source := &restrictedSource{ChunkManager: storage.NewLocalChunkManager(), root: root}
	for _, key := range []string{"dict", filepath.Join(root, "dict")} {
		exists, err := source.Exist(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)
		size, err := source.Size(context.Background(), key)
		require.NoError(t, err)
		require.EqualValues(t, 2, size)
		reader, err := source.Reader(context.Background(), key)
		require.NoError(t, err)
		content, err := io.ReadAll(reader)
		require.NoError(t, reader.Close())
		require.NoError(t, err)
		require.Equal(t, "ok", string(content))
	}
	for _, key := range []string{outside + "/secret", outside + "/absent", root + "/../secret", root + "/link/secret", root + "/file_link", root + "/fifo", "../secret", "a/../../secret", "link/secret", "file_link", "fifo", "", ".", "a\\b", "a\x00b"} {
		_, err := source.Reader(context.Background(), key)
		require.Error(t, err, key)
		require.NotContains(t, err.Error(), "SECRET_MARKER")
	}
}

func TestFileResourceLocalDefaultDirectory(t *testing.T) {
	setSourceRoot(t, "")
	storageRoot := t.TempDir()
	setting := &paramtable.Get().LocalStorageCfg.Path
	old := setting.SwapTempValue(storageRoot)
	t.Cleanup(func() { setting.SwapTempValue(old) })
	root := filepath.Join(storageRoot, "file_resource")
	require.NoError(t, os.Mkdir(root, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dict"), []byte("ok"), 0o600))
	source := RestrictSource(storage.NewLocalChunkManager())
	size, err := source.Size(context.Background(), "dict")
	require.NoError(t, err)
	require.EqualValues(t, 2, size)
	destination := filepath.Join(t.TempDir(), "downloaded")
	err = (&BaseManager{}).downloadFile(context.Background(), storage.NewLocalChunkManager(),
		&internalpb.FileResourceInfo{Id: 1, Path: filepath.Join(root, "dict")}, destination, time.Second)
	require.NoError(t, err)
	content, err := os.ReadFile(destination)
	require.NoError(t, err)
	require.Equal(t, "ok", string(content))
}

func TestFileResourceDownloadEnforcesActualReadLimit(t *testing.T) {
	setSourceRoot(t, "resources")
	cfg := &paramtable.Get().CommonCfg.FileResourceMaxFileSize
	old := cfg.SwapTempValue("4")
	t.Cleanup(func() { cfg.SwapTempValue(old) })
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, "resources/dict").Return(int64(2), nil)
	reader := newMockReader("SECRET_MARKER")
	cm.EXPECT().Reader(mock.Anything, "resources/dict").Return(reader, nil)
	manager := &BaseManager{}
	destination := filepath.Join(t.TempDir(), "dict")
	err := manager.downloadFile(context.Background(), cm, &internalpb.FileResourceInfo{Id: 1, Path: "resources/dict"}, destination, time.Second)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SECRET_MARKER")
	require.NoFileExists(t, destination)
	files, err := os.ReadDir(filepath.Dir(destination))
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestFileResourceRemoteDefaultPrefix(t *testing.T) {
	for _, tc := range []struct {
		name, storageRoot, configured, prefix string
	}{
		{name: "default", storageRoot: "files", prefix: "files/file_resource"},
		{name: "custom_root", storageRoot: "tenant/data", prefix: "tenant/data/file_resource"},
		{name: "bucket_root", storageRoot: ".", prefix: "file_resource"},
		{name: "empty_root", storageRoot: "", prefix: "file_resource"},
		{name: "explicit_override", storageRoot: "files", configured: "dictionaries", prefix: "dictionaries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setSourceRoot(t, tc.configured)
			cm := mocks.NewChunkManager(t)
			if tc.configured == "" {
				cm.EXPECT().RootPath().Return(tc.storageRoot).Once()
			}
			source := RestrictSource(cm)
			key := tc.prefix + "/dict.txt"
			cm.EXPECT().Exist(mock.Anything, key).Return(true, nil).Once()
			cm.EXPECT().Size(mock.Anything, key).Return(int64(2), nil).Once()
			cm.EXPECT().Reader(mock.Anything, key).Return(newMockReader("ok"), nil).Once()
			exists, err := source.Exist(context.Background(), key)
			require.NoError(t, err)
			require.True(t, exists)
			size, err := source.Size(context.Background(), key)
			require.NoError(t, err)
			require.EqualValues(t, 2, size)
			reader, err := source.Reader(context.Background(), key)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			for _, invalid := range []string{"dict.txt", "/" + key, tc.prefix + "2/dict.txt", tc.prefix + "/../secret"} {
				_, err := source.Exist(context.Background(), invalid)
				require.Error(t, err)
				_, err = source.Size(context.Background(), invalid)
				require.Error(t, err)
				_, err = source.Reader(context.Background(), invalid)
				require.Error(t, err)
			}
		})
	}
}

func TestRegisteredResourceSurvivesPolicyChange(t *testing.T) {
	root := t.TempDir()
	sourceFile := filepath.Join(root, "dict")
	require.NoError(t, os.WriteFile(sourceFile, []byte("the\n"), 0o600))
	setSourceRoot(t, t.TempDir())
	setting := &paramtable.Get().CommonCfg.FileResourceMaxFileSize
	old := setting.SwapTempValue("1")
	t.Cleanup(func() { setting.SwapTempValue(old) })
	cm := storage.NewLocalChunkManager()
	_, err := RestrictSource(cm).Size(context.Background(), sourceFile)
	require.Error(t, err)
	destination := filepath.Join(t.TempDir(), "dict")
	err = (&BaseManager{}).downloadFile(context.Background(), cm, &internalpb.FileResourceInfo{Id: 1, Path: sourceFile}, destination, time.Second)
	require.NoError(t, err)
	data, err := os.ReadFile(destination)
	require.NoError(t, err)
	require.Equal(t, "the\n", string(data))
}
