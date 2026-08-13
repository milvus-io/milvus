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

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	bt := NewBaseTable(SkipRemote(true))
	SParams.init(bt)

	t.Run("test MQConfig", func(t *testing.T) {
		Params := &SParams.MQCfg
		assert.Equal(t, 100*time.Millisecond, Params.CheckInterval.GetAsDuration(time.Second))
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

		// test etcd auth default values
		assert.Equal(t, "etcdadmin", Params.EtcdAuthUserName.GetValue())
		assert.Equal(t, "etcdadmin", Params.EtcdAuthPassword.GetValue())
		assert.True(t, Params.EtcdEnableAuth.GetAsBool())

		// test UseEmbedEtcd with auth enabled — should auto-disable auth, not panic
		t.Setenv("etcd.use.embed", "true")
		t.Setenv("etcd.auth.enabled", "true")
		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		assert.NotPanics(t, func() {
			NewBaseTable()
		})

		// test etcd auth enabled with empty credentials should panic
		t.Setenv("etcd.use.embed", "false")
		t.Setenv("etcd.auth.enabled", "true")
		t.Setenv("etcd.auth.userName", "")
		t.Setenv("etcd.auth.password", "")
		assert.Panics(t, func() {
			NewBaseTable()
		})

		// test etcd auth enabled with valid credentials should not panic
		t.Setenv("etcd.auth.enabled", "true")
		t.Setenv("etcd.auth.userName", "milvus")
		t.Setenv("etcd.auth.password", "milvuspass")
		assert.NotPanics(t, func() {
			NewBaseTable()
		})

		t.Setenv("etcd.auth.enabled", "false")
		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
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
		assert.Equal(t, wpCfg.AppendMaxBatchEntries.GetAsInt(), 1000)
		assert.Equal(t, wpCfg.AppendMaxBatchBytes.GetAsSize(), int64(2000000))
		assert.Equal(t, wpCfg.SegmentRollingMaxSize.GetAsSize(), int64(256*1024*1024))
		assert.Equal(t, wpCfg.SegmentRollingMaxTime.GetAsDurationByParse().Seconds(), float64(600))
		assert.Equal(t, wpCfg.SegmentRollingMaxBlocks.GetAsInt64(), int64(1000))
		assert.Equal(t, wpCfg.AuditorMaxInterval.GetAsDurationByParse().Seconds(), float64(10))
		assert.True(t, wpCfg.DirectReadEnabled.GetAsBool())
		assert.Equal(t, int64(16*1024*1024), wpCfg.DirectReadMaxBatchSize.GetAsSize())
		assert.Equal(t, 4, wpCfg.DirectReadMaxFetchThreads.GetAsInt())

		// Test default quorum configuration values
		// Buffer pools (should be empty by default)
		assert.Equal(t, wpCfg.QuorumBufferPools.GetValue(), "")

		// Selection strategy
		assert.Equal(t, wpCfg.QuorumAffinityMode.GetValue(), "soft")
		assert.Equal(t, wpCfg.QuorumReplicas.GetAsInt(), 3)
		assert.Equal(t, wpCfg.QuorumStrategy.GetValue(), "random")

		// Custom placement (should be empty by default)
		assert.Equal(t, wpCfg.QuorumCustomPlacement.GetValue(), "")

		assert.Equal(t, wpCfg.SyncMaxInterval.GetAsDurationByParse().Milliseconds(), int64(200))
		assert.Equal(t, wpCfg.SyncMaxIntervalForLocalStorage.GetAsDurationByParse().Milliseconds(), int64(10))
		assert.Equal(t, wpCfg.SyncMaxIntervalForService.GetAsDurationByParse().Milliseconds(), int64(10))
		assert.Equal(t, wpCfg.SyncMaxEntries.GetAsInt(), 10000)
		assert.Equal(t, wpCfg.SyncMaxBytes.GetAsSize(), int64(256*1024*1024))
		assert.Equal(t, wpCfg.FlushMaxRetries.GetAsInt(), 5)
		assert.Equal(t, wpCfg.FlushMaxSize.GetAsSize(), int64(2*1024*1024))
		assert.Equal(t, wpCfg.FlushMaxThreads.GetAsInt(), 32)
		assert.Equal(t, wpCfg.RetryInterval.GetAsDurationByParse().Milliseconds(), int64(1000))
		assert.Equal(t, wpCfg.CompactionSize.GetAsSize(), int64(2*1024*1024))
		assert.Equal(t, wpCfg.CompactionMaxParallelUploads.GetAsInt(), 4)
		assert.Equal(t, wpCfg.CompactionMaxParallelReads.GetAsInt(), 8)
		assert.Equal(t, wpCfg.ReaderMaxBatchSize.GetAsSize(), int64(16*1024*1024))
		assert.Equal(t, wpCfg.ReaderMaxFetchThreads.GetAsInt(), 32)
		assert.Equal(t, wpCfg.RetentionTTL.GetAsDurationByParse().Milliseconds()/1000, int64(72*60*60))
		assert.Equal(t, wpCfg.FencePolicyConditionWrite.GetValue(), "auto")

		assert.Equal(t, wpCfg.StorageType.GetValue(), "minio")
		assert.Equal(t, wpCfg.ForceLocalStorage.GetAsBool(), false)
		assert.Equal(t, wpCfg.RootPath.GetValue(), "default")
	})

	t.Run("test woodpeckerConfig RetentionTTL fallback", func(t *testing.T) {
		// Test fallback key behavior: when main key doesn't exist, use fallback key
		{
			testBt := NewBaseTable(SkipRemote(true))
			testBt.Save("streaming.walTruncate.retentionInterval", "48h")
			testBt.Remove("woodpecker.logstore.retentionPolicy.ttl")
			var testSParams ServiceParam
			testSParams.init(testBt)
			wpCfg := &testSParams.WoodpeckerCfg
			// Should use fallback key value
			assert.Equal(t, wpCfg.RetentionTTL.GetAsDurationByParse().Milliseconds()/1000, int64(48*60*60))
		}

		// Test main key priority: when main key exists, use main key instead of fallback
		{
			testBt := NewBaseTable(SkipRemote(true))
			testBt.Save("woodpecker.logstore.retentionPolicy.ttl", "24h")
			testBt.Save("streaming.walTruncate.retentionInterval", "48h")
			var testSParams ServiceParam
			testSParams.init(testBt)
			wpCfg := &testSParams.WoodpeckerCfg
			// Should use main key value, not fallback
			assert.Equal(t, wpCfg.RetentionTTL.GetAsDurationByParse().Milliseconds()/1000, int64(24*60*60))
		}
	})

	t.Run("test woodpeckerQuorumConfig", func(t *testing.T) {
		wpCfg := &SParams.WoodpeckerCfg

		// Test setting custom quorum configuration values using JSON format
		// Buffer pools as JSON array
		bufferPoolsJSON := `[{"name":"region1","seeds":["node1:8080","node2:8080","node3:8080"]},{"name":"region2","seeds":["node4:8080","node5:8080","node6:8080"]}]`
		bt.Save("woodpecker.client.quorum.quorumBufferPools", bufferPoolsJSON)

		// Selection strategy
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.affinityMode", "hard")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.replicas", "5")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.strategy", "custom")

		// Custom placement as JSON array
		customPlacementJSON := `[{"name":"replica-1","region":"region1","az":"az-1","resourceGroup":"rg-1"},{"name":"replica-2","region":"region2","az":"az-2","resourceGroup":"rg-2"},{"name":"replica-3","region":"region3","az":"az-3","resourceGroup":"rg-3"},{"name":"replica-4","region":"region1","az":"az-4","resourceGroup":"rg-4"},{"name":"replica-5","region":"region2","az":"az-5","resourceGroup":"rg-5"}]`
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement", customPlacementJSON)

		// Reinitialize configuration to pick up the new values
		SParams.WoodpeckerCfg.QuorumBufferPools.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumAffinityMode.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumReplicas.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumStrategy.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacement.Init(bt.mgr)

		// Verify the updated configuration values
		// Buffer pools (should contain JSON string)
		bufferPools := wpCfg.QuorumBufferPools.GetValue()
		assert.NotEmpty(t, bufferPools)
		assert.Contains(t, bufferPools, "region1")
		assert.Contains(t, bufferPools, "region2")
		assert.Contains(t, bufferPools, "node1:8080")
		assert.Contains(t, bufferPools, "node4:8080")
		assert.Contains(t, bufferPools, "[")
		assert.Contains(t, bufferPools, "]")

		// Selection strategy
		assert.Equal(t, "hard", wpCfg.QuorumAffinityMode.GetValue())
		assert.Equal(t, 5, wpCfg.QuorumReplicas.GetAsInt())
		assert.Equal(t, "custom", wpCfg.QuorumStrategy.GetValue())

		// Custom placement (should contain JSON string)
		customPlacement := wpCfg.QuorumCustomPlacement.GetValue()
		assert.NotEmpty(t, customPlacement)
		assert.Contains(t, customPlacement, "replica-1")
		assert.Contains(t, customPlacement, "replica-5")
		assert.Contains(t, customPlacement, "region1")
		assert.Contains(t, customPlacement, "az-1")
		assert.Contains(t, customPlacement, "rg-1")
		assert.Contains(t, customPlacement, "[")
		assert.Contains(t, customPlacement, "]")

		// Log the configuration values for verification
		t.Logf("Buffer pools (JSON): %s", bufferPools)
		t.Logf("Selection strategy - Affinity: %s, Replicas: %d, Strategy: %s",
			wpCfg.QuorumAffinityMode.GetValue(),
			wpCfg.QuorumReplicas.GetAsInt(),
			wpCfg.QuorumStrategy.GetValue())
		t.Logf("Custom placement (JSON): %s", customPlacement)
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

	t.Run("test pulsar max message size formatter", func(t *testing.T) {
		tests := []struct {
			name     string
			value    string
			expected int
		}{
			{name: "valid", value: "3145728", expected: 3145728},
			{name: "max int32", value: "2147483647", expected: 2147483647},
			{name: "small positive", value: "1", expected: 1},
			{name: "equal to default reserve", value: "4096", expected: 4096},
			{name: "above int32", value: "2147483648", expected: 2097152},
			{name: "invalid", value: "not-a-number", expected: 2097152},
			{name: "zero", value: "0", expected: 2097152},
			{name: "negative", value: "-1", expected: 2097152},
			{name: "overflow", value: "9223372036854775808", expected: 2097152},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				base := NewBaseTable(SkipRemote(true))
				assert.NoError(t, base.Save("pulsar.maxMessageSize", test.value))

				var pulsarConfig PulsarConfig
				pulsarConfig.Init(base)
				assert.Equal(t, test.expected, pulsarConfig.MaxMessageSize.GetAsInt())
			})
		}
	})

	t.Run("test pulsar message reserve size formatter", func(t *testing.T) {
		tests := []struct {
			name              string
			value             string
			maxMessage        string
			expectedRaw       int
			expectedEffective int
		}{
			{name: "default", value: "", expectedRaw: 4096, expectedEffective: 4096},
			{name: "valid", value: "8192", expectedRaw: 8192, expectedEffective: 8192},
			{name: "zero", value: "0", expectedRaw: 0, expectedEffective: 0},
			{name: "max int32 exceeds default max", value: "2147483647", expectedRaw: 2147483647, expectedEffective: 4096},
			{name: "above int32", value: "2147483648", expectedRaw: 4096, expectedEffective: 4096},
			{name: "invalid", value: "not-a-number", expectedRaw: 4096, expectedEffective: 4096},
			{name: "negative", value: "-1", expectedRaw: 4096, expectedEffective: 4096},
			{name: "overflow", value: "9223372036854775808", expectedRaw: 4096, expectedEffective: 4096},
			{name: "equal to max", value: "8192", maxMessage: "8192", expectedRaw: 8192, expectedEffective: 4096},
			{name: "greater than max", value: "8193", maxMessage: "8192", expectedRaw: 8193, expectedEffective: 4096},
			{name: "max int32 minus one fits", value: "2147483646", maxMessage: "2147483647", expectedRaw: 2147483646, expectedEffective: 2147483646},
			{name: "max int32 cannot equal reserve", value: "2147483647", maxMessage: "2147483647", expectedRaw: 2147483647, expectedEffective: 4096},
			{name: "default does not fit tiny max", value: "4096", maxMessage: "4096", expectedRaw: 4096, expectedEffective: 0},
			{name: "default does not fit one byte max", value: "", maxMessage: "1", expectedRaw: 4096, expectedEffective: 0},
			{name: "custom reserve fits tiny max", value: "512", maxMessage: "1024", expectedRaw: 512, expectedEffective: 512},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				base := NewBaseTable(SkipRemote(true))
				if test.maxMessage != "" {
					assert.NoError(t, base.Save("pulsar.maxMessageSize", test.maxMessage))
				}
				if test.value != "" {
					assert.NoError(t, base.Save("pulsar.messageReserveSize", test.value))
				}

				var pulsarConfig PulsarConfig
				pulsarConfig.Init(base)
				assert.Equal(t, test.expectedRaw, pulsarConfig.MessageReserveSize.GetAsInt())
				maxMessageSize, reserveSize := pulsarConfig.GetMessageSizeLimits()
				assert.Equal(t, test.expectedEffective, reserveSize)
				assert.Greater(t, maxMessageSize, reserveSize)
				assert.GreaterOrEqual(t, reserveSize, 0)
			})
		}
	})

	t.Run("test pulsar message size limits remain valid after refresh", func(t *testing.T) {
		base := NewBaseTable(SkipRemote(true))
		var pulsarConfig PulsarConfig
		pulsarConfig.Init(base)

		assertLimits := func(expectedMax, expectedReserve int) {
			maxMessageSize, reserveSize := pulsarConfig.GetMessageSizeLimits()
			assert.Equal(t, expectedMax, maxMessageSize)
			assert.Equal(t, expectedReserve, reserveSize)
			assert.Greater(t, maxMessageSize, reserveSize)
			assert.GreaterOrEqual(t, reserveSize, 0)
		}

		assertLimits(2097152, 4096)
		assert.NoError(t, base.Save(pulsarConfig.MessageReserveSize.Key, "2147483647"))
		assertLimits(2097152, 4096)
		assert.NoError(t, base.Save(pulsarConfig.MaxMessageSize.Key, "1024"))
		assertLimits(1024, 0)
		assert.NoError(t, base.Save(pulsarConfig.MessageReserveSize.Key, "512"))
		assertLimits(1024, 512)
		assert.NoError(t, base.Save(pulsarConfig.MaxMessageSize.Key, "256"))
		assertLimits(256, 0)
		assert.NoError(t, base.Save(pulsarConfig.MaxMessageSize.Key, "1024"))
		assertLimits(1024, 512)

		oldMax := pulsarConfig.MaxMessageSize.SwapTempValue("1024")
		oldReserve := pulsarConfig.MessageReserveSize.SwapTempValue("4096")
		assertLimits(1024, 0)
		pulsarConfig.MaxMessageSize.SwapTempValue(oldMax)
		pulsarConfig.MessageReserveSize.SwapTempValue(oldReserve)
	})

	t.Run("test message size limits for an arbitrary WAL backend", func(t *testing.T) {
		base := NewBaseTable(SkipRemote(true))
		var pulsarConfig PulsarConfig
		pulsarConfig.Init(base)

		// GetMessageSizeLimits is GetMessageSizeLimitsFor pinned to Pulsar's own
		// max; the reserve normalization is identical either way, driven by the
		// same pulsar.messageReserveSize regardless of whose max is passed in.
		pulsarMax, pulsarReserve := pulsarConfig.GetMessageSizeLimits()
		otherMax, otherReserve := pulsarConfig.GetMessageSizeLimitsFor(pulsarMax)
		assert.Equal(t, pulsarMax, otherMax)
		assert.Equal(t, pulsarReserve, otherReserve)

		// A smaller backend limit gets the same reserve subtracted from it, not
		// Pulsar's.
		maxMessageSize, reserveSize := pulsarConfig.GetMessageSizeLimitsFor(512 * 1024)
		assert.Equal(t, 512*1024, maxMessageSize)
		assert.Equal(t, 4096, reserveSize)

		// The oversized-reserve and non-positive-max edge cases hold for any
		// backend's max, not just Pulsar's.
		maxMessageSize, reserveSize = pulsarConfig.GetMessageSizeLimitsFor(256)
		assert.Equal(t, 256, maxMessageSize)
		assert.Equal(t, 0, reserveSize)
		maxMessageSize, reserveSize = pulsarConfig.GetMessageSizeLimitsFor(0)
		assert.Equal(t, 0, maxMessageSize)
		assert.Equal(t, 0, reserveSize)
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

	t.Run("pulsar_backlog_auto_clear_bytes", func(t *testing.T) {
		Params := &SParams.PulsarCfg
		assert.Equal(t, int64(100*1024*1024), Params.BacklogAutoClearBytes.GetAsSize())
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
			assert.Equal(t, 10*1024*1024, kc.ProducerMessageMaxBytes.GetAsInt())
			assert.True(t, base.mgr.IsImmutable(kc.ProducerMessageMaxBytes.Key))
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

		assert.False(t, Params.DisableAWSChunkedEncoding.GetAsBool())

		assert.Empty(t, Params.SslCACert.GetValue())

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
		assert.Equal(t, 100000, Params.PaginationSize.GetAsInt())
		assert.Equal(t, 32, Params.ReadConcurrency.GetAsInt())
		assert.Equal(t, 64, Params.MaxEtcdTxnNum.GetAsInt())

		for _, value := range []string{"0", "-1", "invalid"} {
			assert.NoError(t, bt.Save(Params.MaxEtcdTxnNum.Key, value))
			assert.Equal(t, 64, Params.MaxEtcdTxnNum.GetAsInt())
		}
		assert.NoError(t, bt.Save(Params.MaxEtcdTxnNum.Key, "2"))
		assert.Equal(t, 2, Params.MaxEtcdTxnNum.GetAsInt())
		assert.NoError(t, bt.Reset(Params.MaxEtcdTxnNum.Key))
	})

	t.Run("test profile config", func(t *testing.T) {
		params := &SParams.ProfileCfg
		assert.Equal(t, "/var/lib/milvus/data/pprof", params.PprofPath.GetValue())
		bt.Save(params.PprofPath.Key, "/tmp/pprof")
		assert.Equal(t, "/tmp/pprof", params.PprofPath.GetValue())
	})
}

// validateMessageSizeReserve fails startup fast on a nonsensical combination
// instead of letting normalizePulsarMessageReserve silently degrade the
// reserve to 0 the first time a WAL message is packed. Each subtest builds
// its own isolated BaseTable/ServiceParam so a startup config it deliberately
// makes invalid cannot leak into any other test's global paramtable state.
func TestServiceParamValidateMessageSizeReserve(t *testing.T) {
	t.Run("reserve at or above pulsar.maxMessageSize panics", func(t *testing.T) {
		bt := NewBaseTable(SkipRemote(true))
		assert.NoError(t, bt.Save("pulsar.maxMessageSize", "1000"))
		assert.NoError(t, bt.Save("pulsar.messageReserveSize", "1000"))
		var SParams ServiceParam
		assert.Panics(t, func() { SParams.init(bt) })
	})

	t.Run("reserve at or above kafka.producer.message.max.bytes panics", func(t *testing.T) {
		bt := NewBaseTable(SkipRemote(true))
		// Pulsar's own limit is raised above the reserve so the Pulsar bound
		// passes and only the Kafka bound (default 10 MiB) can fire -- with
		// Pulsar left at its 2 MiB default, a 10 MiB reserve would trip the
		// Pulsar check first and this subtest would never reach the branch it
		// exists to pin.
		assert.NoError(t, bt.Save("pulsar.maxMessageSize", "20971520"))
		assert.NoError(t, bt.Save("pulsar.messageReserveSize", "10485760"))
		assert.PanicsWithValue(t,
			"pulsar.messageReserveSize (10485760) must be smaller than kafka.producer.message.max.bytes (10485760)",
			func() {
				var SParams ServiceParam
				SParams.init(bt)
			})
	})

	t.Run("default configuration does not panic", func(t *testing.T) {
		bt := NewBaseTable(SkipRemote(true))
		var SParams ServiceParam
		assert.NotPanics(t, func() { SParams.init(bt) })
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
