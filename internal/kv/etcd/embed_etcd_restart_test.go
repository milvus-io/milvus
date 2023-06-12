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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	embed_etcd_kv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestEtcdRestartLoad(te *testing.T) {
	etcdDataDir := "/tmp/_etcd_data"
	te.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	te.Setenv("ETCD_USE_EMBED", "true")
	param := new(paramtable.ComponentParam)
	param.Init()
	param.BaseTable.Save("etcd.config.path", "../../../configs/advanced/etcd.yaml")
	param.BaseTable.Save("etcd.data.dir", etcdDataDir)
	//clean up data
	defer func() {
		err := os.RemoveAll(etcdDataDir)
		assert.NoError(te, err)
	}()
	te.Run("etcdKV SaveRestartAndLoad", func(t *testing.T) {
		rootPath := "/etcd/test/root/saveRestartAndLoad"
		metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		require.NoError(te, err)
		assert.NotNil(te, metaKv)
		require.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix("")

		saveAndLoadTests := []struct {
			key   string
			value string
		}{
			{"test1", "value1"},
			{"test2", "value2"},
			{"test1/a", "value_a"},
			{"test1/b", "value_b"},
		}

		// save some data
		for i, test := range saveAndLoadTests {
			if i < 4 {
				err = metaKv.Save(test.key, test.value)
				assert.NoError(t, err)
			}
		}

		// check test result
		for _, test := range saveAndLoadTests {
			val, err := metaKv.Load(test.key)
			assert.NoError(t, err)
			assert.Equal(t, test.value, val)
		}

		embed := metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		embed.Close()

		//restart and check test result
		metaKv, _ = embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)

		for _, test := range saveAndLoadTests {
			val, err := metaKv.Load(test.key)
			assert.NoError(t, err)
			assert.Equal(t, test.value, val)
		}

		metaKv.Close()
	})
}
