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

package paramtable

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/stretchr/testify/assert"
)

func TestBaseParamTable(t *testing.T) {
	Params.Init()

	assert.NotZero(t, len(Params.EtcdEndpoints))
	t.Logf("etcd endpoints = %s", Params.EtcdEndpoints)

	assert.NotEqual(t, Params.MetaRootPath, "")
	t.Logf("meta root path = %s", Params.MetaRootPath)

	assert.NotEqual(t, Params.KvRootPath, "")
	t.Logf("kv root path = %s", Params.KvRootPath)

	// test UseEmbedEtcd
	Params.Save("etcd.use.embed", "true")
	assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode))
	assert.Panics(t, func() { Params.initUseEmbedEtcd() })

	assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode))
	Params.LoadCfgToMemory()
}
