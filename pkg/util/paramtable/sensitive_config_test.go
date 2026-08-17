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
	"strings"
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
	// Credential-bearing keys. Infrastructure topology (minio.address,
	// etcd.endpoints, common.security.tlsMode) is deliberately NOT here: see the
	// policy note in sensitive_audit_test.go.
	sensitiveKeys := []string{
		params.CommonCfg.DefaultRootPassword.Key,
		params.EtcdCfg.EtcdAuthUserName.Key,
		params.EtcdCfg.EtcdAuthPassword.Key,
		params.PulsarCfg.AuthParams.Key,
		params.KafkaCfg.SaslUsername.Key,
		params.KafkaCfg.SaslPassword.Key,
		params.KafkaCfg.KafkaTLSKeyPassword.Key,
		params.MinioCfg.AccessKeyID.Key,
		params.MinioCfg.SecretAccessKey.Key,
		params.MinioCfg.GcpCredentialJSON.Key,
		params.TraceCfg.OtlpHeaders.Key,
		cipher.DefaultRootKey.Key,
		cipher.KmsAwsRoleARN.Key,
		cipher.KmsAwsExternalID.Key,
	}
	for _, key := range sensitiveKeys {
		assert.True(t, isConfigRegistered(mgr, key), key)
		assert.True(t, mgr.IsSensitive(key), key)
		assert.Equal(t, config.RedactedValue, mgr.RedactValue(key, "sentinel"), key)
	}

	sensitiveGroupKeys := []string{
		"credential.apikey1.apikey",
		"kafka.consumer.sasl.password",
		"kafka.producer.ssl.key.password",
		"function.textEmbedding.providers.openai.credential",
		"function.rerank.model.providers.cohere.credential",
		"function.models.zilliz.api_key",
	}
	for _, key := range sensitiveGroupKeys {
		// A ParamGroup member exists once something configures it; configure it
		// here so the assertion covers the whole path a real deployment takes,
		// from the value entering the manager to the projection hiding it.
		mgr.SetMapConfig(strings.ToLower(key), "configured-group-secret")
		assert.True(t, isConfigRegistered(mgr, key), key)
		assert.True(t, mgr.IsSensitive(key), key)
		assert.Equal(t, config.RedactedValue, mgr.GetConfigs()[strings.ToLower(key)], key)
	}

	// Topology stays readable: operators need it, and it is already visible in
	// every deployment manifest.
	visibleKeys := []string{
		params.CommonCfg.AuthorizationEnabled.Key,
		// A list of user names, not a credential -- and refreshable, so hiding
		// it would also make it unalterable.
		params.CommonCfg.SuperUsers.Key,
		// Declared leaves of a sensitive ParamGroup: whether a provider is on
		// and where it points are infrastructure detail, like minio.address.
		"function.textEmbedding.providers.openai.enable",
		"function.textEmbedding.providers.openai.url",
		"function.textEmbedding.providers.azure_openai.resource_name",
		"function.rerank.model.providers.cohere.enable",
		"function.rerank.model.providers.cohere.url",
		// A size bound that happens to sit below the sensitive kafka.producer.
		// prefix; the explicit NonSensitive declaration wins.
		params.KafkaCfg.ProducerMessageMaxBytes.Key,
		params.MinioCfg.Address.Key,
		params.MinioCfg.BucketName.Key,
		params.EtcdCfg.Endpoints.Key,
		params.PulsarCfg.Address.Key,
		params.TraceCfg.JaegerURL.Key,
	}
	for _, key := range visibleKeys {
		assert.True(t, isConfigRegistered(mgr, key), key)
		assert.False(t, mgr.IsSensitive(key), key)
		assert.Equal(t, "visible", mgr.RedactValue(key, "visible"), key)
	}

	// An environment alias of a declared ParamItem resolves to that item...
	assert.True(t, isConfigRegistered(mgr, "MINIO_SECRET_ACCESS_KEY"))
	assert.True(t, mgr.IsSensitive("MINIO_SECRET_ACCESS_KEY"))
	// ...while the name-pattern fallback is what covers a key nothing declares.
	assert.False(t, isConfigRegistered(mgr, "OPENAI_API_KEY"))
	assert.True(t, mgr.IsSensitive("OPENAI_API_KEY"))

	require.NoError(t, params.Save(params.MinioCfg.SecretAccessKey.Key, "configured-secret"))
	projected := params.GetComponentConfigurations("proxy", "secretaccesskey")
	assert.Equal(t, config.RedactedValue, projected["miniosecretaccesskey"])
	raw := mgr.GetByRaw(config.WithSubstr("secretaccesskey"))
	assert.Equal(t, "configured-secret", raw["miniosecretaccesskey"])
}

func isConfigRegistered(m *config.Manager, key string) bool {
	_, kind := m.ResolveRegisteredConfigKey(key)
	return kind != config.RegisteredConfigUnknown
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
