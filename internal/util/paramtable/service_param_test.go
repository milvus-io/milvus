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
	"testing"

	"github.com/milvus-io/milvus/internal/config"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/stretchr/testify/assert"
)

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	SParams.init()

	t.Run("test etcdConfig", func(t *testing.T) {
		Params := &SParams.EtcdCfg

		assert.NotZero(t, len(Params.Endpoints.GetAsStrings()))
		t.Logf("etcd endpoints = %s", Params.Endpoints.GetAsStrings())

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath.GetValue())

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath.GetValue())

		assert.NotNil(t, Params.EtcdUseSSL.GetAsBool())
		t.Logf("use ssl = %t", Params.EtcdUseSSL.GetAsBool())

		assert.NotEmpty(t, Params.EtcdTLSKey.GetValue())
		t.Logf("tls key = %s", Params.EtcdTLSKey.GetValue())

		assert.NotEmpty(t, Params.EtcdTLSCACert.GetValue())
		t.Logf("tls CACert = %s", Params.EtcdTLSCACert.GetValue())

		assert.NotEmpty(t, Params.EtcdTLSCert.GetValue())
		t.Logf("tls cert = %s", Params.EtcdTLSCert.GetValue())

		assert.NotEmpty(t, Params.EtcdTLSMinVersion.GetValue())
		t.Logf("tls minVersion = %s", Params.EtcdTLSMinVersion.GetValue())

		// test UseEmbedEtcd
		t.Setenv("etcd.use.embed", "true")
		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode)
		assert.Panics(t, func() { SParams.init() })

		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		t.Setenv("etcd.use.embed", "false")
		SParams.init()
	})

	t.Run("test pulsarConfig", func(t *testing.T) {
		// test default value
		{
			pc := &PulsarConfig{}
			base := &BaseTable{mgr: &config.Manager{}}
			pc.Init(base)
			assert.Empty(t, pc.Address.GetValue())
		}
		{
			assert.NotEqual(t, SParams.PulsarCfg.Address.GetValue(), "")
			t.Logf("pulsar address = %s", SParams.PulsarCfg.Address.GetValue())
			assert.Equal(t, SParams.PulsarCfg.MaxMessageSize.GetAsInt(), SuggestPulsarMaxMessageSize)
		}

		address := "pulsar://localhost:6650"
		{
			SParams.BaseTable.Save("pulsar.address", address)
			assert.Equal(t, SParams.PulsarCfg.Address.GetValue(), address)
		}

		{
			SParams.BaseTable.Save("pulsar.address", "localhost")
			SParams.BaseTable.Save("pulsar.port", "6650")
			assert.Equal(t, SParams.PulsarCfg.Address.GetValue(), address)
		}
	})

	t.Run("test pulsar web config", func(t *testing.T) {
		assert.NotEqual(t, SParams.PulsarCfg.Address.GetValue(), "")

		{
			assert.NotEqual(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}

		{
			SParams.BaseTable.Save(SParams.PulsarCfg.Address.Key, "u\\invalid")
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}

		{
			SParams.BaseTable.Save(SParams.PulsarCfg.Address.Key, "")
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}
	})

	t.Run("test pulsar auth config", func(t *testing.T) {
		Params := SParams.PulsarCfg

		assert.Equal(t, "", Params.AuthPlugin.GetValue())
		assert.Equal(t, "{}", Params.AuthParams.GetValue())
	})

	t.Run("test pulsar auth config formatter", func(t *testing.T) {
		Params := SParams.PulsarCfg

		assert.Equal(t, "{}", Params.AuthParams.Formatter(""))
		assert.Equal(t, "{\"a\":\"b\"}", Params.AuthParams.Formatter("a:b"))
	})

	t.Run("test pulsar tenant/namespace config", func(t *testing.T) {
		Params := SParams.PulsarCfg

		assert.Equal(t, "public", Params.Tenant.GetValue())
		assert.Equal(t, "default", Params.Namespace.GetValue())
	})

	t.Run("test rocksmqConfig", func(t *testing.T) {
		Params := &SParams.RocksmqCfg

		assert.NotEqual(t, Params.Path.GetValue(), "")
		t.Logf("rocksmq path = %s", Params.Path.GetValue())
	})

	t.Run("test kafkaConfig", func(t *testing.T) {
		// test default value
		{
			kc := &KafkaConfig{}
			base := &BaseTable{mgr: &config.Manager{}}
			kc.Init(base)
			assert.Empty(t, kc.Address.GetValue())
			assert.Equal(t, kc.SaslMechanisms.GetValue(), "PLAIN")
			assert.Equal(t, kc.SecurityProtocol.GetValue(), "SASL_SSL")
		}
	})

	t.Run("test minioConfig", func(t *testing.T) {
		Params := &SParams.MinioCfg

		addr := Params.Address.GetValue()
		equal := addr == "localhost:9000" || addr == "minio:9000"
		assert.Equal(t, equal, true)
		t.Logf("minio address = %s", Params.Address.GetValue())

		assert.Equal(t, Params.AccessKeyID.GetValue(), "minioadmin")

		assert.Equal(t, Params.SecretAccessKey.GetValue(), "minioadmin")

		assert.Equal(t, Params.UseSSL.GetAsBool(), false)

		assert.Equal(t, Params.UseIAM.GetAsBool(), false)

		assert.Equal(t, Params.CloudProvider.GetValue(), "aws")

		assert.Equal(t, Params.IAMEndpoint.GetValue(), "")

		t.Logf("Minio BucketName = %s", Params.BucketName.GetValue())

		t.Logf("Minio rootpath = %s", Params.RootPath.GetValue())
	})
}
