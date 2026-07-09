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

package datanode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type recordingStorageFactory struct {
	chunkManager storage.ChunkManager
	calls        int
}

func (f *recordingStorageFactory) NewChunkManager(context.Context, *indexpb.StorageConfig) (storage.ChunkManager, error) {
	f.calls++
	return f.chunkManager, nil
}

func TestDataNodeInitFileResourceManager(t *testing.T) {
	oldInit := initDataNodeFileResourceManager
	oldCreateConfig := createDataNodeFileResourceStorageConfig
	defer func() {
		initDataNodeFileResourceManager = oldInit
		createDataNodeFileResourceStorageConfig = oldCreateConfig
		fileresource.ResetLocalModeForTest()
	}()

	t.Run("effective sync uses storage factory", func(t *testing.T) {
		fileresource.SetLocalMode(fileresource.SyncMode)
		mockChunkManager := mocks.NewChunkManager(t)
		factory := &recordingStorageFactory{chunkManager: mockChunkManager}
		createDataNodeFileResourceStorageConfig = func() *indexpb.StorageConfig {
			return &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
		}

		var gotStorage storage.ChunkManager
		var gotMode fileresource.Mode
		initDataNodeFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
			gotStorage = chunkManager
			gotMode = mode
			return nil
		}

		node := &DataNode{ctx: context.Background(), storageFactory: factory}
		require.NoError(t, node.initFileResourceManager(mlog.With()))
		require.Equal(t, 1, factory.calls)
		require.Equal(t, mockChunkManager, gotStorage)
		require.Equal(t, fileresource.SyncMode, gotMode)
	})

	t.Run("empty remote address downgrades to close", func(t *testing.T) {
		fileresource.SetLocalMode(fileresource.SyncMode)
		factory := &recordingStorageFactory{}
		createDataNodeFileResourceStorageConfig = func() *indexpb.StorageConfig {
			return &indexpb.StorageConfig{StorageType: "minio"}
		}

		var gotStorage storage.ChunkManager
		var gotMode fileresource.Mode
		initDataNodeFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
			gotStorage = chunkManager
			gotMode = mode
			return nil
		}

		node := &DataNode{ctx: context.Background(), storageFactory: factory}
		require.NoError(t, node.initFileResourceManager(mlog.With()))
		require.Zero(t, factory.calls)
		require.Nil(t, gotStorage)
		require.Equal(t, fileresource.CloseMode, gotMode)
	})

	for _, mode := range []fileresource.Mode{fileresource.RefMode, fileresource.CloseMode} {
		t.Run(mode.String()+" skips storage factory", func(t *testing.T) {
			fileresource.SetLocalMode(mode)
			factory := &recordingStorageFactory{}
			var gotMode fileresource.Mode
			initDataNodeFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
				require.Nil(t, chunkManager)
				gotMode = mode
				return nil
			}

			node := &DataNode{ctx: context.Background(), storageFactory: factory}
			require.NoError(t, node.initFileResourceManager(mlog.With()))
			require.Zero(t, factory.calls)
			require.Equal(t, mode, gotMode)
		})
	}

	t.Run("initializer error is propagated", func(t *testing.T) {
		fileresource.SetLocalMode(fileresource.CloseMode)
		expected := errors.New("init failed")
		initDataNodeFileResourceManager = func(storage.ChunkManager, fileresource.Mode) error {
			return expected
		}

		node := &DataNode{ctx: context.Background(), storageFactory: &recordingStorageFactory{}}
		require.ErrorIs(t, node.initFileResourceManager(mlog.With()), expected)
	})
}
