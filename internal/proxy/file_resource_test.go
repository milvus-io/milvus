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

package proxy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestProxyInitFileResourceManager(t *testing.T) {
	params := paramtable.Get()
	oldRole := paramtable.GetRole()
	paramtable.SetRole("proxy")
	oldInitManager := initProxyFileResourceManager
	defer func() {
		initProxyFileResourceManager = oldInitManager
		fileresource.ResetLocalModeForTest()
		paramtable.SetRole(oldRole)
		params.Reset(params.CommonCfg.PNFileResourceMode.Key)
	}()

	t.Run("sync mode creates persistent storage chunk manager", func(t *testing.T) {
		params.Save(params.CommonCfg.PNFileResourceMode.Key, "sync")

		mockFactory := dependency.NewMockFactory(t)
		mockChunkManager := mocks.NewChunkManager(t)
		mockFactory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(mockChunkManager, nil).Once()

		var gotStorage storage.ChunkManager
		var gotMode fileresource.Mode
		initProxyFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
			gotStorage = chunkManager
			gotMode = mode
			return nil
		}

		node := &Proxy{
			ctx:     context.Background(),
			factory: mockFactory,
		}

		err := node.initFileResourceManager()
		assert.NoError(t, err)
		assert.Equal(t, mockChunkManager, gotStorage)
		assert.Equal(t, fileresource.SyncMode, gotMode)
	})

	t.Run("effective local sync overrides configured close", func(t *testing.T) {
		params.Save(params.CommonCfg.PNFileResourceMode.Key, "close")
		fileresource.SetLocalMode(fileresource.SyncMode)
		defer fileresource.ResetLocalModeForTest()

		mockFactory := dependency.NewMockFactory(t)
		mockChunkManager := mocks.NewChunkManager(t)
		mockFactory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(mockChunkManager, nil).Once()

		var gotStorage storage.ChunkManager
		var gotMode fileresource.Mode
		initProxyFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
			gotStorage = chunkManager
			gotMode = mode
			return nil
		}

		node := &Proxy{ctx: context.Background(), factory: mockFactory}
		err := node.initFileResourceManager()
		assert.NoError(t, err)
		assert.Equal(t, mockChunkManager, gotStorage)
		assert.Equal(t, fileresource.SyncMode, gotMode)
	})

	t.Run("initializer error is propagated", func(t *testing.T) {
		params.Save(params.CommonCfg.PNFileResourceMode.Key, "close")
		expected := errors.New("init failed")
		initProxyFileResourceManager = func(storage.ChunkManager, fileresource.Mode) error {
			return expected
		}

		node := &Proxy{ctx: context.Background(), factory: dependency.NewMockFactory(t)}
		assert.ErrorIs(t, node.initFileResourceManager(), expected)
	})

	t.Run("close mode does not create persistent storage chunk manager", func(t *testing.T) {
		params.Save(params.CommonCfg.PNFileResourceMode.Key, "close")

		mockFactory := dependency.NewMockFactory(t)

		var gotStorage storage.ChunkManager
		var gotMode fileresource.Mode
		initProxyFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
			gotStorage = chunkManager
			gotMode = mode
			return nil
		}

		node := &Proxy{
			ctx:     context.Background(),
			factory: mockFactory,
		}

		err := node.initFileResourceManager()
		assert.NoError(t, err)
		assert.Nil(t, gotStorage)
		assert.Equal(t, fileresource.CloseMode, gotMode)
	})

	t.Run("ref mode is normalized to close mode", func(t *testing.T) {
		params.Save(params.CommonCfg.PNFileResourceMode.Key, "ref")

		mockFactory := dependency.NewMockFactory(t)

		var gotStorage storage.ChunkManager
		var gotMode fileresource.Mode
		initProxyFileResourceManager = func(chunkManager storage.ChunkManager, mode fileresource.Mode) error {
			gotStorage = chunkManager
			gotMode = mode
			return nil
		}

		node := &Proxy{
			ctx:     context.Background(),
			factory: mockFactory,
		}

		err := node.initFileResourceManager()
		assert.NoError(t, err)
		assert.Nil(t, gotStorage)
		assert.Equal(t, fileresource.CloseMode, gotMode)
	})
}

func TestProxySyncFileResource(t *testing.T) {
	oldManager := fileresource.GlobalFileManager
	defer func() {
		fileresource.GlobalFileManager = oldManager
	}()

	t.Run("proxy not healthy", func(t *testing.T) {
		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Abnormal)

		status, err := node.SyncFileResource(context.Background(), &internalpb.SyncFileResourceRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.Error(status))
	})

	t.Run("sync succeeds", func(t *testing.T) {
		manager := &mockFileResourceManager{}
		fileresource.GlobalFileManager = manager

		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		req := &internalpb.SyncFileResourceRequest{
			Version: 10,
			Resources: []*internalpb.FileResourceInfo{
				{Name: "resource", Path: "/tmp/resource", Id: 1},
			},
		}
		status, err := node.SyncFileResource(context.Background(), req)
		assert.NoError(t, err)
		assert.NoError(t, merr.Error(status))
		assert.Equal(t, uint64(10), manager.version)
		assert.Equal(t, req.GetResources(), manager.resources)
		assert.True(t, manager.ready)
	})

	t.Run("sync failure preserves typed status", func(t *testing.T) {
		expected := merr.WrapErrServiceUnavailableMsg("snapshot download failed")
		fileresource.GlobalFileManager = &mockFileResourceManager{syncErr: expected}

		node := &Proxy{}
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		status, err := node.SyncFileResource(context.Background(), &internalpb.SyncFileResourceRequest{Version: 10})
		assert.NoError(t, err)
		assert.ErrorIs(t, merr.Error(status), merr.ErrServiceUnavailable)
	})
}

type mockFileResourceManager struct {
	version   uint64
	resources []*internalpb.FileResourceInfo
	ready     bool
	syncErr   error
}

func (m *mockFileResourceManager) GetVersion() uint64 {
	return m.version
}

func (m *mockFileResourceManager) IsReady() bool {
	return m.ready
}

func (m *mockFileResourceManager) Sync(ctx context.Context, version uint64, resources []*internalpb.FileResourceInfo) error {
	if m.syncErr != nil {
		return m.syncErr
	}
	m.version = version
	m.resources = resources
	m.ready = true
	return nil
}

func (m *mockFileResourceManager) Download(context.Context, storage.ChunkManager, ...*internalpb.FileResourceInfo) error {
	return nil
}

func (m *mockFileResourceManager) Release(...*internalpb.FileResourceInfo) {
}

func (m *mockFileResourceManager) Mode() fileresource.Mode {
	return fileresource.SyncMode
}
