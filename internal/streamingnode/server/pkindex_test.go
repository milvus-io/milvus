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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
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

	c := newPKIndexComponents(context.Background(), nil)
	assert.Nil(t, c)
	assert.Nil(t, c.manager())
	c.close()
}

// TODO: when the Committer is injected over the manifest service client,
// check that the components are built, with the data dir from the config and
// from the local storage path.
func TestPKIndexComponentsOnPanicsWithoutACommitter(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, "true")
	defer params.Reset(params.StreamingCfg.PKIndexEnabled.Key)
	params.Save(params.StreamingCfg.PKIndexDataDir.Key, t.TempDir())
	defer params.Reset(params.StreamingCfg.PKIndexDataDir.Key)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	assert.Panics(t, func() { newPKIndexComponents(context.Background(), cm) })
}
