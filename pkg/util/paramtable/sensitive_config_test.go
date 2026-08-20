// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

func TestSensitiveConfigMetadata(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	params := ComponentParam{}
	params.Init(base)
	cipher := cipherConfig{}
	cipher.init(base)

	mgr := base.Manager()
	sensitiveKeys := []string{
		params.CommonCfg.SuperUsers.Key,
		params.CommonCfg.DefaultRootPassword.Key,
		params.EtcdCfg.EtcdAuthUserName.Key,
		params.EtcdCfg.EtcdAuthPassword.Key,
		params.PulsarCfg.Address.Key,
		params.PulsarCfg.WebAddress.Key,
		params.PulsarCfg.AuthParams.Key,
		params.KafkaCfg.SaslUsername.Key,
		params.KafkaCfg.SaslPassword.Key,
		params.KafkaCfg.Address.Key,
		params.KafkaCfg.KafkaTLSKeyPassword.Key,
		params.DataCoordCfg.IndexNodeAddress.Key,
		params.MinioCfg.Address.Key,
		params.MinioCfg.AccessKeyID.Key,
		params.MinioCfg.SecretAccessKey.Key,
		params.MinioCfg.GcpCredentialJSON.Key,
		params.MinioCfg.IAMEndpoint.Key,
		params.TraceCfg.JaegerURL.Key,
		params.TraceCfg.OtlpEndpoint.Key,
		params.TraceCfg.OtlpHeaders.Key,
		cipher.DefaultRootKey.Key,
		cipher.KmsAwsRoleARN.Key,
		cipher.KmsAwsExternalID.Key,
	}
	for _, key := range sensitiveKeys {
		assert.True(t, mgr.IsConfigRegistered(key), key)
		assert.True(t, mgr.IsSensitive(key), key)
		assert.Equal(t, config.RedactedValue, mgr.RedactValue(key, "sentinel"), key)
	}

	sensitiveGroupKeys := []string{
		"credential.apikey1.apikey",
		"kafka.consumer.sasl.password",
		"kafka.producer.ssl.key.password",
		"function.textEmbedding.providers.openai.credential",
		"function.rerank.model.providers.cohere.credential",
		"function.analyzer.lindera.download_urls.ipadic",
		"function.models.zilliz.api_key",
	}
	for _, key := range sensitiveGroupKeys {
		assert.True(t, mgr.IsConfigRegistered(key), key)
		assert.True(t, mgr.IsSensitive(key), key)
	}

	assert.True(t, mgr.IsConfigRegistered(params.CommonCfg.AuthorizationEnabled.Key))
	assert.False(t, mgr.IsSensitive(params.CommonCfg.AuthorizationEnabled.Key))
	assert.Equal(t, "visible", mgr.RedactValue(params.CommonCfg.AuthorizationEnabled.Key, "visible"))

	assert.True(t, mgr.IsConfigRegistered("MINIO_SECRET_ACCESS_KEY"))
	assert.True(t, mgr.IsSensitive("OPENAI_API_KEY"))

	require.NoError(t, params.Save(params.MinioCfg.SecretAccessKey.Key, "configured-secret"))
	projected := params.GetComponentConfigurations("proxy", "secretaccesskey")
	assert.Equal(t, config.RedactedValue, projected["miniosecretaccesskey"])
	raw := mgr.GetByRaw(config.WithSubstr("secretaccesskey"))
	assert.Equal(t, "configured-secret", raw["miniosecretaccesskey"])

	require.NoError(t, params.Save(params.PulsarCfg.Address.Key, "pulsar://user:pulsar-secret@localhost:6650"))
	require.NoError(t, params.Save(params.MinioCfg.Address.Key, "user:minio-secret@localhost:9000"))
	assert.Equal(t, config.RedactedValue, params.GetAll()["pulsaraddress"])
	assert.Equal(t, config.RedactedValue, params.GetAll()["minioaddress"])
	assert.Contains(t, params.GetAllRaw()["pulsaraddress"], "pulsar-secret")
	assert.Contains(t, params.GetAllRaw()["minioaddress"], "minio-secret")
}

func TestSensitiveParamGroupUsesRawValuesInternally(t *testing.T) {
	mgr := config.NewManager()
	group := ParamGroup{
		KeyPrefix: "credential.",
		Sensitive: true,
	}
	group.Init(mgr)
	mgr.SetMapConfig("credential.provider.api_key", "group-secret")

	values := group.GetValue()
	require.Contains(t, values, "provider.api_key")
	assert.Equal(t, "group-secret", values["provider.api_key"])

	safe := mgr.GetBy(config.WithPrefix("credential."), config.RemovePrefix("credential."))
	assert.Equal(t, config.RedactedValue, safe["provider.api_key"])
}
