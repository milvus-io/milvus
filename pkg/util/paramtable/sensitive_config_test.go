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

// Per-cluster CDC settings are read by exact key through base.Get, not as a
// group aggregate, and no ParamItem can declare them because the cluster ID is
// part of the name. They still have to be declared as namespaces: an undeclared
// key is refused by both management endpoints and dropped from the projections,
// which would leave cross-cluster TLS unconfigurable through them.
func TestDynamicClusterNamespacesAreDeclared(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	params := &ComponentParam{}
	params.Init(base)
	mgr := base.Manager()

	for _, key := range []string{
		"tls.clusters.dc2.caPemPath",
		"tls.clusters.dc2.clientPemPath",
		"tls.clusters.dc2.clientKeyPath",
		"grpc.clusters.dc2.authority",
	} {
		assert.True(t, isConfigRegistered(mgr, key), key)
		// Paths and a hostname, not key material.
		assert.False(t, mgr.IsSensitive(key), key)
	}

	// What the endpoint reports must be what the consumer reads.
	mgr.SetConfig("tls.clusters.dc2.caPemPath", "/certs/ca.pem")
	mgr.SetConfig("grpc.clusters.dc2.authority", "host.example")
	caPemPath, _, _ := params.ProxyGrpcClientCfg.GetClusterTLSConfig("dc2")
	_, reported, err := mgr.GetRegisteredConfig("tls.clusters.dc2.caPemPath")
	require.NoError(t, err)
	assert.Equal(t, caPemPath, reported)
	assert.Equal(t, "host.example", params.ProxyGrpcClientCfg.GetClusterAuthority("dc2"))
}

// The fence guards keys no ParamItem declares, so nothing normalises their
// spelling on the way in — it has to compare on the identity the write would
// address, or one spelling is fenced and the three that reach the same etcd
// entry are not.
func TestSecurityFenceIsSpellingIndependent(t *testing.T) {
	for _, key := range []string{
		"proxy.enablePublicPrivilege",
		"proxy_enablePublicPrivilege",
		"PROXY_ENABLEPUBLICPRIVILEGE",
		"proxyenablepublicprivilege",
		"proxy/enablePublicPrivilege",
		"common.security.superUsers",
		"COMMON_SECURITY_SUPERUSERS",
		"commonsecurityauthorizationenabled",
	} {
		assert.True(t, IsSecurityGoverningConfig(key), key)
		assert.Equal(t, config.EtcdConfigKey(key), config.EtcdConfigKey(strings.ToLower(key)), key)
	}
	for _, key := range []string{"proxy.maxNameLength", "queryNode.gracefulStopTimeout", "minio.address"} {
		assert.False(t, IsSecurityGoverningConfig(key), key)
	}
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

// Every configuration key is stored under two spellings, so a projection lists
// each entry twice. This asserts over the whole shipped table what
// TestExemptedGroupMemberIsVisibleUnderEverySpelling asserts for one key: an
// entry never contradicts its own alias.
//
// Written as an invariant over the real ParamTable rather than as cases,
// because the thing that goes wrong here is a group nobody thought to check:
// when this first held, 33 of the 843 aliased pairs disagreed, all of them
// NonSensitiveSuffixes leaves nobody had listed.
func TestProjectionAgreesAcrossSpellings(t *testing.T) {
	params := newSensitiveAuditParams(t)
	projection := params.GetAll()

	aliased := 0
	for key, value := range projection {
		if !strings.Contains(key, ".") {
			continue
		}
		collapsed := strings.NewReplacer(".", "", "_", "", "/", "").Replace(strings.ToLower(key))
		alias, ok := projection[collapsed]
		if !ok {
			continue
		}
		aliased++
		assert.Equal(t, value, alias,
			"%q and %q are one configuration entry and must project the same", key, collapsed)
	}
	require.NotZero(t, aliased,
		"no aliased pair was found, so this test asserted nothing about the projection")
}

// The disclosure this whole change exists for, stated once as a property
// instead of one case per variable shape: EnvSource imports the entire process
// environment, and nothing it brings in that Milvus does not declare may appear
// in a projection — neither its value nor its name, since the list of variables
// in a pod is worth withholding on its own.
func TestProjectionOmitsEveryEnvironmentOnlyKey(t *testing.T) {
	for _, name := range []string{
		"AWS_SECRET_ACCESS_KEY",
		"DATABASE_URL",
		"SOMETHING_WITH_NO_SEPARATORS",
		"lower_case_variable",
		// Shaped to impersonate a member of each dynamic namespace Milvus
		// declares, which is the way in that a prefix check alone would allow.
		"PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL",
		"FUNCTION_TEXTEMBEDDING_PROVIDERS_EVIL_ENABLE",
		"KAFKA_CONSUMER_EVIL",
		"CREDENTIAL_EVIL_APIKEY",
		"AUTOINDEX_PARAMS_TUNING_EVIL",
		"KNOWHERE_EVIL",
	} {
		t.Setenv(name, "environment-only-sentinel")
	}

	base := NewBaseTable(SkipRemote(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	params := &ComponentParam{}
	params.Init(base)
	mgr := base.Manager()

	require.Contains(t, mgr.GetConfigsRaw(), "awssecretaccesskey",
		"the environment was not imported at all, so this test proves nothing")

	for name, projection := range map[string]map[string]string{
		"GetConfigs":     mgr.GetConfigs(),
		"GetConfigsView": mgr.GetConfigsView(),
		"GetBy":          mgr.GetBy(config.WithSubstr("")),
	} {
		for key, value := range projection {
			assert.NotContains(t, value, "environment-only-sentinel",
				"%s published the value of an undeclared environment variable under %q", name, key)
			assert.NotContains(t, strings.ToLower(key), "evil",
				"%s published the name of an undeclared environment variable", name)
		}
	}

	// And the read endpoint refuses them by name, whichever way they are spelled.
	for _, spelling := range []string{
		"AWS_SECRET_ACCESS_KEY",
		"awssecretaccesskey",
		"aws.secret.access.key",
		"proxy.accessLog.formatters.DATABASE_URL",
		"function.textEmbedding.providers.evil.enable",
		"functiontextembeddingprovidersevilenable",
	} {
		_, _, err := mgr.GetRegisteredConfig(spelling)
		assert.ErrorIs(t, err, config.ErrKeyUnregistered, spelling)
	}
}
