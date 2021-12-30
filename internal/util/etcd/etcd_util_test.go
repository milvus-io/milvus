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

package etcd

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
)

var Params paramtable.GlobalParamTable

func TestEtcd(t *testing.T) {
	Params.Init()
	Params.BaseParams.UseEmbedEtcd = true
	Params.BaseParams.EtcdDataDir = "/tmp/data"
	err := InitEtcdServer(&Params.BaseParams)
	assert.NoError(t, err)
	defer os.RemoveAll(Params.BaseParams.EtcdDataDir)
	defer StopEtcdServer()

	// port is binded
	err = InitEtcdServer(&Params.BaseParams)
	assert.Error(t, err)

	etcdCli, err := GetEtcdClient(&Params.BaseParams)
	assert.NoError(t, err)

	key := path.Join("test", "test")
	_, err = etcdCli.Put(context.TODO(), key, "value")
	assert.NoError(t, err)

	resp, err := etcdCli.Get(context.TODO(), key)
	assert.NoError(t, err)
	assert.False(t, resp.Count < 1)
	assert.Equal(t, string(resp.Kvs[0].Value), "value")
}
