// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package etcdkv_test

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	embed_etcd_kv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEtcdRestartLoad(te *testing.T) {
	os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	param := new(paramtable.BaseParamTable)
	param.Init()
	param.BaseTable.Save("etcd.use.embed", "true")
	// TODO, not sure if the relative path works for ci environment
	param.BaseTable.Save("etcd.config.path", "../../../configs/advanced/etcd.yaml")
	param.BaseTable.Save("etcd.data.dir", "etcd.test.data.dir")
	//clean up data
	defer func() {
		os.RemoveAll("etcd.test.data.dir")
	}()
	param.LoadCfgToMemory()
	te.Run("EtcdKV SaveRestartAndLoad", func(t *testing.T) {
		rootPath := "/etcd/test/root/saveRestartAndLoad"
		metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, param)
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
		metaKv, _ = embed_etcd_kv.NewMetaKvFactory(rootPath, param)

		for _, test := range saveAndLoadTests {
			val, err := metaKv.Load(test.key)
			assert.NoError(t, err)
			assert.Equal(t, test.value, val)
		}
	})
}
