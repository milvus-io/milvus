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

package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/indexmanager"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// fakeManifestClient is a manifest service client that is never called. The
// test only checks that the manager is built and closed.
type fakeManifestClient struct {
	closed bool
}

func (f *fakeManifestClient) CommitFlush(ctx context.Context, args manifest.FlushArgs) (manifest.View, error) {
	return manifest.View{}, merr.WrapErrServiceUnavailableMsg("not served in this test")
}

func (f *fakeManifestClient) GetRecoveryManifests(ctx context.Context, refs []manifest.VChannelRef) ([]manifest.RecoveryManifest, error) {
	return nil, merr.WrapErrServiceUnavailableMsg("not served in this test")
}

func (f *fakeManifestClient) Close() { f.closed = true }

func newCommitter(cli *fakeManifestClient) func(context.Context) (manifest.Committer, func(), error) {
	return func(context.Context) (manifest.Committer, func(), error) {
		return cli, cli.Close, nil
	}
}

func TestPKIndexSetupOff(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "false")
	defer params.Reset(params.StreamingCfg.PKIndexEnabled.Key)

	mgr, closePKIndex, err := indexmanager.Setup(context.Background(), nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, mgr)
	assert.Nil(t, closePKIndex)
}

func TestPKIndexSetupOn(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "true")
	defer params.Reset(params.StreamingCfg.PKIndexEnabled.Key)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	t.Run("data dir from config", func(t *testing.T) {
		dir := t.TempDir()
		params.Save(params.StreamingCfg.PKIndexDataDir.Key, dir)
		defer params.Reset(params.StreamingCfg.PKIndexDataDir.Key)
		cli := &fakeManifestClient{}

		mgr, closePKIndex, err := indexmanager.Setup(context.Background(), cm, newCommitter(cli), func(context.Context) (int64, error) { return 0, nil }, nil)
		require.NoError(t, err)
		require.NotNil(t, mgr)
		require.NotNil(t, closePKIndex)
		assert.Equal(t, dir, indexmanager.RootForTest(mgr))
		assert.NotNil(t, indexmanager.CommitterForTest(mgr))
		closePKIndex()
		assert.True(t, cli.closed)
	})

	t.Run("data dir from the local storage path", func(t *testing.T) {
		params.Save(params.StreamingCfg.PKIndexDataDir.Key, "")
		defer params.Reset(params.StreamingCfg.PKIndexDataDir.Key)
		local := t.TempDir()
		params.Save(params.LocalStorageCfg.Path.Key, local)
		defer params.Reset(params.LocalStorageCfg.Path.Key)
		cli := &fakeManifestClient{}

		mgr, closePKIndex, err := indexmanager.Setup(context.Background(), cm, newCommitter(cli), func(context.Context) (int64, error) { return 0, nil }, nil)
		require.NoError(t, err)
		require.NotNil(t, mgr)
		assert.NotNil(t, indexmanager.CommitterForTest(mgr))
		assert.Equal(t, filepath.Join(local, "pkindex"), indexmanager.RootForTest(mgr))
		closePKIndex()
		assert.True(t, cli.closed)
	})
}

func TestPKIndexSetupOffDoesNotBuildTheCommitter(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "false")
	defer params.Reset(params.StreamingCfg.PKIndexEnabled.Key)

	mgr, closePKIndex, err := indexmanager.Setup(context.Background(), nil, func(context.Context) (manifest.Committer, func(), error) {
		t.Fatal("the committer must not be built when the index is off")
		return nil, nil, nil
	}, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, mgr)
	assert.Nil(t, closePKIndex)
}
