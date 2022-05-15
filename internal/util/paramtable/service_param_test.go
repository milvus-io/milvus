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

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	SParams.Init()

	t.Run("test etcdConfig", func(t *testing.T) {
		Params := SParams.EtcdCfg

		assert.NotZero(t, len(Params.Endpoints))
		t.Logf("etcd endpoints = %s", Params.Endpoints)

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath)

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath)

		assert.NotNil(t, Params.EtcdUseSSL)
		t.Logf("use ssl = %t", Params.EtcdUseSSL)

		assert.NotEmpty(t, Params.EtcdTlsKey)
		t.Logf("tls key = %s", Params.EtcdTlsKey)

		assert.NotEmpty(t, Params.EtcdTlsCACert)
		t.Logf("tls CACert = %s", Params.EtcdTlsCACert)

		assert.NotEmpty(t, Params.EtcdTlsCert)
		t.Logf("tls cert = %s", Params.EtcdTlsCert)

		// test UseEmbedEtcd
		Params.Base.Save("etcd.use.embed", "true")
		assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode))
		assert.Panics(t, func() { Params.initUseEmbedEtcd() })

		assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode))
		Params.LoadCfgToMemory()
	})

	t.Run("test pulsarConfig", func(t *testing.T) {
		Params := SParams.PulsarCfg

		assert.NotEqual(t, Params.Address, "")
		t.Logf("pulsar address = %s", Params.Address)

		assert.Equal(t, Params.MaxMessageSize, SuggestPulsarMaxMessageSize)
	})

	t.Run("test rocksmqConfig", func(t *testing.T) {
		Params := SParams.RocksmqCfg

		assert.NotEqual(t, Params.Path, "")
		t.Logf("rocksmq path = %s", Params.Path)
	})

	t.Run("test minioConfig", func(t *testing.T) {
		Params := SParams.MinioCfg

		addr := Params.Address
		equal := addr == "localhost:9000" || addr == "minio:9000"
		assert.Equal(t, equal, true)
		t.Logf("minio address = %s", Params.Address)

		assert.Equal(t, Params.AccessKeyID, "minioadmin")

		assert.Equal(t, Params.SecretAccessKey, "minioadmin")

		assert.Equal(t, Params.UseSSL, false)

		t.Logf("Minio BucketName = %s", Params.BucketName)

		t.Logf("Minio rootpath = %s", Params.RootPath)
	})
}
