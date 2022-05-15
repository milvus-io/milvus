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

var Params paramtable.ServiceParam

func TestEtcd(t *testing.T) {
	Params.Init()
	Params.EtcdCfg.UseEmbedEtcd = true
	Params.EtcdCfg.DataDir = "/tmp/data"
	err := InitEtcdServer(&Params.EtcdCfg)
	assert.NoError(t, err)
	defer os.RemoveAll(Params.EtcdCfg.DataDir)
	defer StopEtcdServer()

	etcdCli, err := GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)

	key := path.Join("test", "test")
	_, err = etcdCli.Put(context.TODO(), key, "value")
	assert.NoError(t, err)

	resp, err := etcdCli.Get(context.TODO(), key)
	assert.NoError(t, err)
	assert.False(t, resp.Count < 1)
	assert.Equal(t, string(resp.Kvs[0].Value), "value")

	Params.EtcdCfg.UseEmbedEtcd = false
	Params.EtcdCfg.EtcdUseSSL = true
	Params.EtcdCfg.EtcdTLSMinVersion = "1.3"
	Params.EtcdCfg.EtcdTLSCACert = "../../../configs/cert/ca.pem"
	Params.EtcdCfg.EtcdTLSCert = "../../../configs/cert/client.pem"
	Params.EtcdCfg.EtcdTLSKey = "../../../configs/cert/client.key"
	etcdCli, err = GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)

	Params.EtcdCfg.EtcdTLSMinVersion = "some not right word"
	etcdCli, err = GetEtcdClient(&Params.EtcdCfg)
	assert.NotNil(t, err)

	Params.EtcdCfg.EtcdTLSMinVersion = "1.2"
	Params.EtcdCfg.EtcdTLSCACert = "wrong/file"
	etcdCli, err = GetEtcdClient(&Params.EtcdCfg)
	assert.NotNil(t, err)

	Params.EtcdCfg.EtcdTLSCACert = "../../../configs/cert/ca.pem"
	Params.EtcdCfg.EtcdTLSCert = "wrong/file"
	assert.NotNil(t, err)

}
