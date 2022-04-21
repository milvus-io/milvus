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

package etcdkv_test

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	embed_etcd_kv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestEtcdConfigLoad(te *testing.T) {
	os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	param := new(paramtable.ServiceParam)
	param.Init()
	param.BaseTable.Save("etcd.use.embed", "true")
	// TODO, not sure if the relative path works for ci environment
	param.BaseTable.Save("etcd.config.path", "../../../configs/advanced/etcd.yaml")
	param.BaseTable.Save("etcd.data.dir", "etcd.test.data.dir")
	param.EtcdCfg.LoadCfgToMemory()
	//clean up data
	defer func() {
		os.RemoveAll("etcd.test.data.dir")
	}()
	te.Run("Etcd Config", func(t *testing.T) {
		rootPath := "/test"
		metaKv := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg, t)
		assert.NotNil(te, metaKv)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix("")

		kv := metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		assert.Equal(t, kv.GetConfig().SnapshotCount, uint64(1000))
		assert.Equal(t, kv.GetConfig().MaxWalFiles, uint(10))
	})
}
