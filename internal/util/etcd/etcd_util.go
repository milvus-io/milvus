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
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// GetEtcdClient returns etcd client
func GetEtcdClient(cfg *paramtable.EtcdConfig) (*clientv3.Client, error) {
	if cfg.UseEmbedEtcd {
		return GetEmbedEtcdClient()
	}
	return GetRemoteEtcdClient(cfg.Endpoints)
}

// GetRemoteEtcdClient returns client of remote etcd by given endpoints
func GetRemoteEtcdClient(endpoints []string) (*clientv3.Client, error) {
	log.Info("Get remote etcd client", zap.Any("endpoints", endpoints))
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
}

func GetEtcdTestClient(t testutil.TB) *clientv3.Client {
	os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	param := new(paramtable.ServiceParam)
	datapath := "etcd" + fmt.Sprint(rand.Int())
	param.Init()
	param.BaseTable.Save("etcd.use.embed", "true")

	// try to find etcd.yaml in the config file
	envFile := param.BaseTable.GetConfigDir() + "advanced/etcd.yaml"

	param.BaseTable.Save("etcd.config.path", envFile)
	param.BaseTable.Save("etcd.data.dir", datapath)
	param.EtcdCfg.LoadCfgToMemory()

	err := InitEtcdServer(&param.EtcdCfg)
	assert.NoError(t, err)

	c, err := GetEmbedEtcdClient()
	assert.NoError(t, err)

	clearFunc := func() {
		StopEtcdServer()
		os.RemoveAll(datapath)
		os.Unsetenv(metricsinfo.DeployModeEnvKey)
		// recover all config setup
		param.BaseTable.Remove("etcd.use.embed")
		param.BaseTable.Remove("etcd.config.path")
		param.BaseTable.Remove("etcd.data.dir")
		param.EtcdCfg.LoadCfgToMemory()
	}
	t.Cleanup(clearFunc)
	return c
}
