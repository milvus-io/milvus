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

package allocator

import (
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var gTestIDAllocator *GlobalIDAllocator

var Params paramtable.ComponentParam

var embedEtcdServer *embed.Etcd

func startEmbedEtcdServer() (*embed.Etcd, error) {
	dir, err := os.MkdirTemp(os.TempDir(), "milvus_ut")
	if err != nil {
		return nil, err
	}
	config := embed.NewConfig()

	config.Dir = dir
	config.LogLevel = "warn"
	config.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	if err != nil {
		return nil, err
	}
	config.ListenClientUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:0")
	if err != nil {
		return nil, err
	}
	config.ListenPeerUrls = []url.URL{*u}

	return embed.StartEtcd(config)
}

func TestMain(m *testing.M) {
	var err error
	// init embed etcd
	embedEtcdServer, err = startEmbedEtcdServer()
	if err != nil {
		os.Exit(1)
	}
	defer embedEtcdServer.Close()

	exitCode := m.Run()
	if exitCode > 0 {
		os.Exit(exitCode)
	}
}

func TestGlobalTSOAllocator_All(t *testing.T) {
	etcdCli := v3client.New(embedEtcdServer.Server)
	etcdKV := tsoutil.NewTSOKVBase(etcdCli, "/test/root/kv", "gidTest")

	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", etcdKV)

	t.Run("Initialize", func(t *testing.T) {
		err := gTestIDAllocator.Initialize()
		assert.NoError(t, err)
	})

	t.Run("AllocOne", func(t *testing.T) {
		one, err := gTestIDAllocator.AllocOne()
		assert.NoError(t, err)
		ano, err := gTestIDAllocator.AllocOne()
		assert.NoError(t, err)
		assert.NotEqual(t, one, ano)
	})

	t.Run("Alloc", func(t *testing.T) {
		count := uint32(2 << 10)
		idStart, idEnd, err := gTestIDAllocator.Alloc(count)
		assert.NoError(t, err)
		assert.Equal(t, count, uint32(idEnd-idStart))
	})

	t.Run("Alloc2", func(t *testing.T) {
		count1 := uint32(2 << 18)
		id1, err := gTestIDAllocator.allocator.GenerateTSO(count1)
		assert.NoError(t, err)

		count2 := uint32(2 << 8)
		id2, err := gTestIDAllocator.allocator.GenerateTSO(count2)
		assert.NoError(t, err)
		assert.Equal(t, id2-id1, uint64(count2))
	})
}
