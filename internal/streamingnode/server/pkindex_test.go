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

	pkindexclient "github.com/milvus-io/milvus/internal/datacoord/pkindex/client"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestOpenFileLimitShort(t *testing.T) {
	need, short := openFileLimitShort(14095, 4096)
	assert.Equal(t, uint64(14096), need)
	assert.True(t, short)

	_, short = openFileLimitShort(14096, 4096)
	assert.False(t, short)

	need, _ = openFileLimitShort(0, 0)
	assert.Equal(t, uint64(engine.DefaultMaxOpenFiles+openFileReserve), need)
}

func TestPKIndexComponentsOff(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "false")
	defer params.Reset(params.StreamingCfg.PKIndexEnabled.Key)

	c := newPKIndexComponents(context.Background(), nil, nil)
	assert.Nil(t, c)
	assert.Nil(t, c.manager())
	c.close()
}

// fakeManifestClient is a manifest service client that is never called. The
// test only checks that the components are built and closed.
type fakeManifestClient struct {
	closed bool
}

func (f *fakeManifestClient) CommitFlush(ctx context.Context, req *pkindexpb.CommitFlushRequest) (*pkindexpb.Manifest, error) {
	return nil, merr.WrapErrServiceUnavailableMsg("not served in this test")
}

func (f *fakeManifestClient) GetRecoveryManifests(ctx context.Context, req *pkindexpb.GetRecoveryManifestsRequest) ([]*pkindexpb.ManifestPointerEntry, error) {
	return nil, merr.WrapErrServiceUnavailableMsg("not served in this test")
}

func (f *fakeManifestClient) Close() { f.closed = true }

func TestPKIndexComponentsOn(t *testing.T) {
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

		c := newPKIndexComponents(context.Background(), cm, func() pkindexclient.Client { return cli })
		require.NotNil(t, c)
		assert.Equal(t, dir, c.root)
		assert.NotNil(t, c.manager())
		assert.NotNil(t, c.manager().Committer())
		c.close()
		assert.True(t, cli.closed)
	})

	t.Run("data dir from the local storage path", func(t *testing.T) {
		params.Save(params.StreamingCfg.PKIndexDataDir.Key, "")
		defer params.Reset(params.StreamingCfg.PKIndexDataDir.Key)
		local := t.TempDir()
		params.Save(params.LocalStorageCfg.Path.Key, local)
		defer params.Reset(params.LocalStorageCfg.Path.Key)
		cli := &fakeManifestClient{}

		c := newPKIndexComponents(context.Background(), cm, func() pkindexclient.Client { return cli })
		require.NotNil(t, c)
		assert.NotNil(t, c.manager().Committer())
		assert.Equal(t, filepath.Join(local, "pkindex"), c.root)
		c.close()
		assert.True(t, cli.closed)
	})
}

func TestPKIndexComponentsOffDoesNotBuildTheClient(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "false")
	defer params.Reset(params.StreamingCfg.PKIndexEnabled.Key)

	c := newPKIndexComponents(context.Background(), nil, func() pkindexclient.Client {
		t.Fatal("the client must not be built when the index is off")
		return nil
	})
	assert.Nil(t, c)
}
