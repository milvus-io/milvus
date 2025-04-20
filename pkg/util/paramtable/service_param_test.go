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

package paramtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	bt := NewBaseTable(SkipRemote(true))
	SParams.init(bt)

	t.Run("test MQConfig", func(t *testing.T) {
		Params := &SParams.MQCfg
		assert.Equal(t, 1*time.Second, Params.MergeCheckInterval.GetAsDuration(time.Second))
		assert.Equal(t, 16, Params.TargetBufSize.GetAsInt())
		assert.Equal(t, 3*time.Second, Params.MaxTolerantLag.GetAsDuration(time.Second))
		assert.Equal(t, 60*time.Minute, Params.MaxPositionTsGap.GetAsDuration(time.Minute))
	})

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
		assert.Panics(t, func() {
			NewBaseTable()
		})

		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		t.Setenv("etcd.use.embed", "false")
		SParams.init(bt)
	})

	t.Run("test tikvConfig", func(t *testing.T) {
		Params := &SParams.TiKVCfg

		assert.NotZero(t, len(Params.Endpoints.GetAsStrings()))
		t.Logf("tikv endpoints = %s", Params.Endpoints.GetAsStrings())

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath.GetValue())

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath.GetValue())

		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		SParams.init(bt)
	})

	t.Run("test woodpeckerConfig", func(t *testing.T) {
		wpCfg := &SParams.WoodpeckerCfg
		assert.Equal(t, wpCfg.MetaType.GetValue(), "etcd")
		assert.Equal(t, wpCfg.MetaPrefix.GetValue(), "woodpecker")

		assert.Equal(t, wpCfg.AppendQueueSize.GetAsInt(), 10000)
		assert.Equal(t, wpCfg.AppendMaxRetries.GetAsInt(), 3)
		assert.Equal(t, wpCfg.SegmentRollingMaxSize.GetAsInt(), 2000000000)
		assert.Equal(t, wpCfg.SegmentRollingMaxTime.GetAsInt(), 600)
		assert.Equal(t, wpCfg.AuditorMaxInterval.GetAsInt(), 10)

		assert.Equal(t, wpCfg.SyncMaxInterval.GetAsInt(), 1000)
		assert.Equal(t, wpCfg.SyncMaxEntries.GetAsInt(), 100000)
		assert.Equal(t, wpCfg.SyncMaxBytes.GetAsInt(), 64000000)
		assert.Equal(t, wpCfg.FlushMaxRetries.GetAsInt(), 5)
		assert.Equal(t, wpCfg.FlushMaxSize.GetAsInt(), 8000000)
		assert.Equal(t, wpCfg.FlushMaxThreads.GetAsInt(), 4)
		assert.Equal(t, wpCfg.RetryInterval.GetAsInt(), 1000)
		assert.Equal(t, wpCfg.FragmentCachedMaxBytes.GetAsInt64(), int64(1000000000))
		assert.Equal(t, wpCfg.FragmentCachedInterval.GetAsInt(), 1000)

		assert.Equal(t, wpCfg.StorageType.GetValue(), "minio")
		assert.Equal(t, wpCfg.RootPath.GetValue(), "/var/lib/milvus/woodpecker")
	})

	t.Run("test pulsarConfig", func(t *testing.T) {
		// test default value
		{
			pc := &PulsarConfig{}
			base := &BaseTable{mgr: config.NewManager()}
			pc.Init(base)
			assert.Empty(t, pc.Address.GetValue())
		}
		{
			assert.NotEqual(t, SParams.PulsarCfg.Address.GetValue(), "")
			t.Logf("pulsar address = %s", SParams.PulsarCfg.Address.GetValue())
			assert.Equal(t, SParams.PulsarCfg.MaxMessageSize.GetAsInt(), 2097152)
		}

		address := "pulsar://localhost:6650"
		{
			bt.Save("pulsar.address", address)
			assert.Equal(t, SParams.PulsarCfg.Address.GetValue(), address)
		}

		{
			bt.Save("pulsar.address", "localhost")
			bt.Save("pulsar.port", "6650")
			assert.Equal(t, SParams.PulsarCfg.Address.GetValue(), address)
		}
	})

	t.Run("test pulsar web config", func(t *testing.T) {
		assert.NotEqual(t, SParams.PulsarCfg.Address.GetValue(), "")

		{
			assert.NotEqual(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}

		{
			bt.Save(SParams.PulsarCfg.Address.Key, "u\\invalid")
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}

		{
			bt.Save(SParams.PulsarCfg.Address.Key, "")
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}
	})

	t.Run("test pulsar auth config", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "", Params.AuthPlugin.GetValue())
		assert.Equal(t, "{}", Params.AuthParams.GetValue())
	})

	t.Run("test pulsar auth config formatter", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "{}", Params.AuthParams.Formatter(""))
		assert.Equal(t, "{\"a\":\"b\"}", Params.AuthParams.Formatter("a:b"))
	})

	t.Run("test pulsar tenant/namespace config", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "public", Params.Tenant.GetValue())
		assert.Equal(t, "default", Params.Namespace.GetValue())
	})

	t.Run("pulsar_operation_timeout", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "60", Params.RequestTimeout.GetValue())
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
			base := &BaseTable{mgr: config.NewManager()}
			kc.Init(base)
			assert.Equal(t, "localhost:9092", kc.Address.GetValue())
			assert.Empty(t, kc.SaslMechanisms.GetValue())
			assert.Empty(t, kc.SecurityProtocol.GetValue())
			assert.Equal(t, kc.ReadTimeout.GetAsDuration(time.Second), 10*time.Second)
			assert.Equal(t, kc.KafkaUseSSL.GetAsBool(), false)
			assert.Empty(t, kc.KafkaTLSCACert.GetValue())
			assert.Empty(t, kc.KafkaTLSCert.GetValue())
			assert.Empty(t, kc.KafkaTLSKey.GetValue())
			assert.Empty(t, kc.KafkaTLSKeyPassword.GetValue())
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

		assert.NotEmpty(t, Params.SslCACert.GetValue())

		assert.Equal(t, Params.UseIAM.GetAsBool(), false)

		assert.Equal(t, Params.CloudProvider.GetValue(), "aws")

		assert.Equal(t, Params.IAMEndpoint.GetValue(), "")

		assert.Equal(t, Params.GcpCredentialJSON.GetValue(), "")

		t.Logf("Minio BucketName = %s", Params.BucketName.GetValue())

		t.Logf("Minio rootpath = %s", Params.RootPath.GetValue())
	})

	t.Run("test metastore config", func(t *testing.T) {
		Params := &SParams.MetaStoreCfg

		assert.Equal(t, util.MetaStoreTypeEtcd, Params.MetaStoreType.GetValue())
		assert.Equal(t, 86400*time.Second, Params.SnapshotTTLSeconds.GetAsDuration(time.Second))
		assert.Equal(t, 3600*time.Second, Params.SnapshotReserveTimeSeconds.GetAsDuration(time.Second))
		assert.Equal(t, 100000, Params.PaginationSize.GetAsInt())
		assert.Equal(t, 32, Params.ReadConcurrency.GetAsInt())
	})

	t.Run("test profile config", func(t *testing.T) {
		params := &SParams.ProfileCfg
		assert.Equal(t, "/var/lib/milvus/data/pprof", params.PprofPath.GetValue())
		bt.Save(params.PprofPath.Key, "/tmp/pprof")
		assert.Equal(t, "/tmp/pprof", params.PprofPath.GetValue())
	})
}

func TestRuntimConfig(t *testing.T) {
	SetRole(typeutil.StandaloneRole)
	assert.Equal(t, GetRole(), typeutil.StandaloneRole)

	SetLocalComponentEnabled(typeutil.QueryNodeRole)
	assert.True(t, IsLocalComponentEnabled(typeutil.QueryNodeRole))

	SetLocalComponentEnabled(typeutil.QueryCoordRole)
	assert.True(t, IsLocalComponentEnabled(typeutil.QueryCoordRole))
}
