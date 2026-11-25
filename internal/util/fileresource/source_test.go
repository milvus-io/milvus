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
	require.NoError(t, os.WriteFile(filepath.Join(root, "dict"), []byte("ok"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("SECRET_MARKER"), 0600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "link")))
	require.NoError(t, os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "file_link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(root, "fifo"), 0600))
	source := &restrictedSource{ChunkManager: storage.NewLocalChunkManager(), root: root}
	size, err := source.Size(context.Background(), filepath.Join(root, "dict"))
	require.NoError(t, err)
	require.EqualValues(t, 2, size)
	for _, key := range []string{outside + "/secret", outside + "/absent", root + "/../secret", root + "/link/secret", root + "/file_link", root + "/fifo"} {
		_, err := source.Reader(context.Background(), key)
		require.Error(t, err, key)
		require.NotContains(t, err.Error(), "SECRET_MARKER")
	}
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
