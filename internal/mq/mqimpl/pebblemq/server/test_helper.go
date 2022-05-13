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

package server

import (
	"os"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

func NewEmbeddedEtcd() (kv.MetaKv, error) {
	os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	param := new(paramtable.ServiceParam)
	param.Init()
	param.BaseTable.Save("etcd.use.embed", "true")
	param.BaseTable.Save("etcd.config.path", "../../../../../configs/advanced/etcd.yaml")
	param.BaseTable.Save("etcd.data.dir", "etcd.test.data.dir")
	param.EtcdCfg.LoadCfgToMemory()
	return etcdkv.NewMetaKvFactory("/etcd/test/root", &param.EtcdCfg)
}
