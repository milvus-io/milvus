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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestBaseTableFailureLogsProtectConfig(t *testing.T) {
	const canary = "yaml-base-table-secret-canary"
	dir := t.TempDir()
	t.Setenv("MILVUSCONF", dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "milvus.yaml"),
		[]byte("minio:\n  secretAccessKey: !!int "+canary+"\n"), 0o600))
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	t.Cleanup(base.Manager().Close)
	assert.Contains(t, sink.String(), "init baseTable with file failed")
	assert.NotContains(t, sink.String(), canary)
}

func TestBaseTablePublicGroupEnvironmentOverride(t *testing.T) {
	const key = "function.textEmbedding.providers.openai.enable"
	const credential = "function.textEmbedding.providers.openai.credential"
	const opaque = "function.textEmbedding.providers.opaque.enable"
	for _, envPrefix := range []string{"", config.DefaultEnvPrefix} {
		t.Run(envPrefix, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("MILVUSCONF", dir)
			t.Setenv(envPrefix+"FUNCTION_TEXTEMBEDDING_PROVIDERS_OPENAI_ENABLE", "false")
			t.Setenv("FUNCTION_TEXTEMBEDDING_PROVIDERS_OPENAI_CREDENTIAL", "credential-env-canary")
			t.Setenv("FUNCTION_TEXTEMBEDDING_PROVIDERS_OPAQUE_ENABLE", "opaque-env-canary")
			t.Setenv("UNRELATED_DATABASE_URL", "unrelated-env-canary")
			require.NoError(t, os.WriteFile(filepath.Join(dir, "milvus.yaml"),
				[]byte(key+": true\n"+credential+": from-file\n"), 0o600))

			// Exercise BaseTable's actual source initialization order and the
			// production group's declarations together.
			base := NewBaseTable(SkipRemote(true), Interval(0))
			t.Cleanup(base.Manager().Close)
			params := functionConfig{}
			params.init(base)
			mgr := base.Manager()
			require.Equal(t, "false", params.GetTextEmbeddingProviderConfig("openai")["enable"])
			require.Equal(t, "credential-env-canary", params.GetTextEmbeddingProviderConfig("openai")["credential"])
			for _, alias := range []string{
				key, strings.ReplaceAll(key, ".", "/"),
				"FUNCTION_TEXTEMBEDDING_PROVIDERS_OPENAI_ENABLE", config.EtcdConfigKey(key),
			} {
				source, value, err := mgr.GetRegisteredConfig(alias)
				require.NoError(t, err, alias)
				assert.Equal(t, "EnvironmentSource", source)
				assert.Equal(t, "false", value)
			}
			assert.Equal(t, "false", mgr.ProjectConfigs()[strings.ToLower(key)])
			assert.Equal(t, "false", mgr.ProjectConfigs()[config.EtcdConfigKey(key)])
			assert.Equal(t, "false[EnvironmentSource]", mgr.GetConfigsView()[strings.ToLower(key)])
			assert.Equal(t, "false", mgr.ProjectBy(config.WithPrefix("function.textEmbedding.providers."),
				config.RemovePrefix("function.textEmbedding.providers."))["openai.enable"])
			_, _, err := mgr.GetRegisteredConfig(credential)
			require.ErrorIs(t, err, config.ErrKeySensitive)
			for _, envOnly := range []string{
				opaque, "FUNCTION_TEXTEMBEDDING_PROVIDERS_OPAQUE_ENABLE",
				config.EtcdConfigKey(opaque), "UNRELATED_DATABASE_URL",
			} {
				_, _, err := mgr.GetRegisteredConfig(envOnly)
				require.ErrorIs(t, err, config.ErrKeyUnregistered, envOnly)
			}
			for _, projection := range []map[string]string{mgr.ProjectConfigs(), mgr.GetConfigsView()} {
				for _, value := range projection {
					assert.NotContains(t, value, "env-canary")
				}
			}
		})
	}
}

func TestBaseTableFileRefreshCannotEndorseOpaqueEnvironment(t *testing.T) {
	const key = "function.textEmbedding.providers.future.enable"
	const canary = "opaque-environment-refresh-canary"
	dir := t.TempDir()
	t.Setenv("MILVUSCONF", dir)
	t.Setenv("FUNCTION_TEXTEMBEDDING_PROVIDERS_FUTURE_ENABLE", canary)
	filename := filepath.Join(dir, "milvus.yaml")
	require.NoError(t, os.WriteFile(filename, []byte("{}\n"), 0o600))
	base := NewBaseTable(SkipRemote(true), Interval(0))
	t.Cleanup(base.Manager().Close)
	params := functionConfig{}
	params.init(base)
	mgr := base.Manager()
	_, _, err := mgr.GetRegisteredConfig(key)
	require.ErrorIs(t, err, config.ErrKeyUnregistered)

	// A spelling first introduced after startup cannot lend its public leaf
	// to a value that was previously only an arbitrary environment variable.
	require.NoError(t, os.WriteFile(filename, []byte(key+": true\n"), 0o600))
	assert.Equal(t, config.RedactedValue, mgr.ProjectFileConfigs()[strings.ToLower(key)])
	source, raw, err := mgr.GetConfig(key)
	require.NoError(t, err)
	assert.Equal(t, "EnvironmentSource", source)
	assert.Equal(t, canary, raw)
	_, value, err := mgr.GetRegisteredConfig(key)
	require.ErrorIs(t, err, config.ErrKeySensitive)
	assert.Empty(t, value)
	for _, projection := range []map[string]string{
		mgr.ProjectConfigs(), mgr.GetConfigsView(),
		mgr.ProjectBy(config.WithPrefix("function")),
	} {
		for _, value := range projection {
			assert.NotContains(t, value, canary)
		}
	}
}

func TestSensitivePulsarConfigParseFailureLogs(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	t.Cleanup(base.Manager().Close)
	address := "pulsar://private-user:password-canary@private-broker.invalid:%"
	require.NoError(t, base.Save("pulsar.address", address))
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	var params PulsarConfig
	params.Init(base)
	assert.Empty(t, params.WebAddress.GetValue(), "invalid address keeps the existing fallback")
	assert.Equal(t, address, params.Address.GetValue(), "runtime address remains raw")
	assert.Contains(t, sink.String(), "failed to parse pulsar config")
	for _, canary := range []string{"private-user", "password-canary", "private-broker.invalid"} {
		assert.NotContains(t, sink.String(), canary)
	}
}

func TestSensitivePulsarWebAddressOverrideLogs(t *testing.T) {
	const fallback = "http://broker-0.invalid:8080"
	for _, test := range []struct {
		name     string
		address  string
		expected string
	}{
		{"explicit HTTPS", "https://private-user:password-canary@private-broker.invalid/admin", "https://private-user:password-canary@private-broker.invalid/admin"},
		{"unsupported scheme", "pulsar://private-user:password-canary@private-broker.invalid:6650", fallback},
		{"malformed URL", "https://private-user:password-canary@private-broker.invalid:%", fallback},
		{"missing host", "http:///private-user/password-canary/private-broker.invalid", fallback},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
			t.Cleanup(base.Manager().Close)
			require.NoError(t, base.Save("pulsar.address", "pulsar://broker-0.invalid:6650,broker-1.invalid:6650"))
			require.NoError(t, base.Save("pulsar.webport", "8080"))
			require.NoError(t, base.Save("pulsar.webaddress", test.address))
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			var params PulsarConfig
			params.Init(base)
			assert.Equal(t, Sensitive, params.WebAddress.Sensitivity)
			assert.Equal(t, test.expected, params.WebAddress.GetValue())
			_, raw, err := base.Manager().GetConfig(params.WebAddress.Key)
			require.NoError(t, err)
			assert.Equal(t, test.address, raw, "formatting must not rewrite the stored configuration")
			_, value, err := base.Manager().GetRegisteredConfig(params.WebAddress.Key)
			require.ErrorIs(t, err, config.ErrKeySensitive)
			assert.Empty(t, value)
			if test.expected == fallback {
				assert.Contains(t, sink.String(), "using the address derived from pulsar.address")
			}
			for _, canary := range []string{"private-user", "password-canary", "private-broker.invalid"} {
				assert.NotContains(t, sink.String(), canary)
			}
		})
	}
}

func TestSensitiveConfigMetadata(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	params := ComponentParam{}
	params.Init(base)
	cipher := cipherConfig{}
	cipher.init(base)

	mgr := base.Manager()
	// Credential-bearing and topology-bearing keys. Both are unsafe at an
	// unauthenticated management boundary: the latter can redirect traffic that
	// carries a credential configured elsewhere.
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
		params.EtcdCfg.Endpoints.Key,
		params.EtcdCfg.RootPath.Key,
		params.TiKVCfg.Endpoints.Key,
		params.TiKVCfg.RootPath.Key,
		params.WoodpeckerCfg.RootPath.Key,
		params.PulsarCfg.Address.Key,
		params.PulsarCfg.Port.Key,
		params.PulsarCfg.WebAddress.Key,
		params.PulsarCfg.WebPort.Key,
		params.KafkaCfg.Address.Key,
		params.MinioCfg.Address.Key,
		params.MinioCfg.Port.Key,
		params.MinioCfg.BucketName.Key,
		params.MinioCfg.RootPath.Key,
		params.MinioCfg.IAMEndpoint.Key,
		params.TraceCfg.JaegerURL.Key,
		params.TraceCfg.OtlpEndpoint.Key,
		params.DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.Key,
		params.DataCoordCfg.IndexNodeAddress.Key,
		params.ProxyGrpcServerCfg.TLSMode.Key,
		params.ProxyGrpcServerCfg.IPItem.Key,
		params.ProxyGrpcServerCfg.Port.Key,
		params.ProxyGrpcServerCfg.InternalPort.Key,
		params.InternalTLSCfg.InternalTLSEnabled.Key,
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
		"function.textEmbedding.providers.openai.url",
		"function.textEmbedding.providers.azure_openai.resource_name",
		"function.rerank.model.providers.cohere.credential",
		"function.rerank.model.providers.cohere.url",
		"function.models.zilliz.api_key",
		"function.models.zilliz.endpoint",
		"function.models.zilliz.enableTLS",
		"function.models.zilliz.certFile",
		"function.models.zilliz.serverNameOverride",
		"function.analyzer.lindera.download_urls.ipadic",
	}
	for _, key := range sensitiveGroupKeys {
		// A ParamGroup member exists once something configures it; configure it
		// here so the assertion covers the whole path a real deployment takes,
		// from the value entering the manager to the projection hiding it.
		mgr.SetMapConfig(strings.ToLower(key), "configured-group-secret")
		assert.True(t, isConfigRegistered(mgr, key), key)
		assert.True(t, mgr.IsSensitive(key), key)
		assert.Equal(t, config.RedactedValue, mgr.ProjectConfigs()[strings.ToLower(key)], key)
	}

	// Values that carry neither credentials nor topology stay readable.
	visibleKeys := []string{
		params.CommonCfg.AuthorizationEnabled.Key,
		// A list of user names, not a credential -- and refreshable, so hiding
		// it would also make it unalterable.
		params.CommonCfg.SuperUsers.Key,
		// A pure enable switch below a sensitive provider group is explicitly
		// reviewed and exempted; endpoints are not.
		"function.textEmbedding.providers.openai.enable",
		"function.rerank.model.providers.cohere.enable",
		// A size bound that happens to sit below the sensitive kafka.producer.
		// prefix; the explicit NonSensitive declaration wins.
		params.KafkaCfg.ProducerMessageMaxBytes.Key,
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
	raw := mgr.GetBy(config.WithSubstr("secretaccesskey"))
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
		assert.True(t, mgr.IsSensitive(key), key)
	}

	// What the endpoint reports must be what the consumer reads.
	mgr.SetConfig("tls.clusters.dc2.caPemPath", "/certs/ca.pem")
	mgr.SetConfig("grpc.clusters.dc2.authority", "host.example")
	caPemPath, _, _ := params.ProxyGrpcClientCfg.GetClusterTLSConfig("dc2")
	_, _, err := mgr.GetRegisteredConfig("tls.clusters.dc2.caPemPath")
	require.ErrorIs(t, err, config.ErrKeySensitive)
	assert.Equal(t, "/certs/ca.pem", caPemPath)
	assert.Equal(t, "host.example", params.ProxyGrpcClientCfg.GetClusterAuthority("dc2"))
}

func isConfigRegistered(m *config.Manager, key string) bool {
	_, _, err := m.GetRegisteredConfig(key)
	return !errors.Is(err, config.ErrKeyUnregistered)
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

	safe := mgr.ProjectBy(config.WithPrefix("credential."), config.RemovePrefix("credential."))
	assert.Equal(t, config.RedactedValue, safe["provider.api_key"])
}

func TestParamGroupRejectsOrphanExemptions(t *testing.T) {
	group := ParamGroup{
		KeyPrefix:            "provider.",
		NonSensitiveSuffixes: []string{"enable"},
	}
	assert.Panics(t, func() { group.Init(config.NewManager()) })
}

// hook.yaml is owned by plugins, so the core cannot enumerate its credential-
// and topology-bearing leaves. Its empty-prefix ParamGroup must therefore fail
// closed externally without changing what the plugin reads internally.
func TestHookProjectionFailsClosed(t *testing.T) {
	base := NewBaseTableFromYamlOnly(hookYamlFile)
	hook := &hookConfig{}
	hook.init(base)

	base.Manager().SetMapConfig("plugin.endpoint", "https://redirect.example")
	base.Manager().SetMapConfig("plugin.api_key", "hook-secret")

	raw := hook.SoConfig.GetValue()
	assert.Equal(t, "https://redirect.example", raw["plugin.endpoint"])
	assert.Equal(t, "hook-secret", raw["plugin.api_key"])

	projected := hook.GetAll()
	assert.Equal(t, config.RedactedValue, projected["plugin.endpoint"])
	assert.Equal(t, config.RedactedValue, projected["plugin.api_key"])
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

	require.Contains(t, mgr.GetConfigs(), "awssecretaccesskey",
		"the environment was not imported at all, so this test proves nothing")

	for name, projection := range map[string]map[string]string{
		"ProjectConfigs": mgr.ProjectConfigs(),
		"GetConfigsView": mgr.GetConfigsView(),
		"ProjectBy":      mgr.ProjectBy(config.WithSubstr("")),
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

// One etcd identity, one verdict.
//
// A configuration key has one identity — the form with every separator removed,
// which is what values are stored under and what an alter-endpoint write
// addresses — and many spellings that reach it. Every rule that decides whether
// a value is a credential reads a spelling. So the invariant that actually
// matters is not "this key is classified correctly" but "no two spellings of one
// identity disagree", and it has to be asserted mechanically, because the
// spellings that break it are the ones nobody thinks to write down.
//
// This is not a hypothetical. Four separate defects in this classifier were of
// exactly this shape, each one a pair of spellings that reached the same stored
// value and got opposite answers, and each was found by enumeration rather than
// by reading the code:
//
//   - membership matched a collapsed prefix while sensitivity matched only a
//     dotted one, so "kafkaconsumerssl.key.pem" was admitted as a member of a
//     namespace declared sensitive and then classified as not sensitive;
//   - a group's Sensitive default decided a collapsed spelling on its own, so an
//     exempted leaf was readable under its dotted spelling and masked under its
//     collapsed one, in the same response;
//   - the caller's segmentation was believed, so a credential named
//     "<provider>.credential_url" could be asked for as
//     "<provider>credential.url" and hit a declared-safe leaf;
//   - and the same again for an identity no source had segmented.
//
// Two of those returned a private key from an endpoint with no authentication in
// front of it. Both spellings address the credential's own etcd slot.
func TestOneIdentityHasOneVerdict(t *testing.T) {
	params := newSensitiveAuditParams(t)
	mgr := params.baseTable.mgr

	// Seed every dynamic namespace with a member whose name is shaped like the
	// things that go wrong: a credential-ish leaf, a declared-safe leaf, and the
	// two run together. Groups are the interesting case because their members
	// are named by whoever writes them, so nothing here can be enumerated in
	// advance.
	leaves := []string{
		"p.credential", "p.credential_url", "p.secret_enable", "p.token_url",
		"p.enable", "p.url", "p.resource_name", "p.api_key", "p.ssl.key.pem",
	}
	seeds := make([]string, 0, 64)
	// Include both ParamGroups and namespaces registered directly by grpc_param.
	for _, prefix := range auditConfigPrefixes(params) {
		for _, leaf := range leaves {
			seeds = append(seeds, prefix+leaf)
		}
		// A member written as a runtime overlay, which is the one way into the
		// manager that does not go through a config source. It vouches for its
		// own segmentation, so it has to teach one as well, or the two spellings
		// of it disagree.
		require.NoError(t, params.baseTable.SaveGroup(map[string]string{prefix + "overlaid.url": "x"}))
	}
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		seeds = append(seeds, item.Key)
	})
	// And every key the sources actually loaded, which is where the shipped
	// group members live — they are not ParamItems, so the walk above cannot
	// see them, and they are the ones with an endorsed segmentation.
	for key := range mgr.GetConfigs() {
		seeds = append(seeds, key)
	}

	// Group by the identity a write actually lands on, which is EtcdConfigKey of
	// the read-normalized key rather than of the caller's spelling. The two differ
	// under NotFormatPrefix: EtcdConfigKey("KNOWHERE.OPAQUE") collapses to
	// "knowhereopaque" because the guard is case-sensitive, while resolving it
	// first lower-cases and so keeps "knowhere.opaque". Grouping by the raw
	// spelling would split one identity across two buckets and merge two others,
	// which is exactly the confusion this test exists to detect.
	byIdentity := make(map[string][]string, len(seeds)*8)
	for _, seed := range seeds {
		for _, spelling := range spellingsOf(seed) {
			// Normalize read-side separators while preserving the case-sensitive
			// knowhere suffix. Derive test buckets independently of classification.
			canonical := strings.ReplaceAll(spelling, "/", ".")
			if !strings.HasPrefix(canonical, config.NotFormatPrefix) {
				canonical = strings.ToLower(canonical)
			}
			byIdentity[config.EtcdConfigKey(canonical)] = append(
				byIdentity[config.EtcdConfigKey(canonical)], spelling)
		}
	}

	disagreements := make([]string, 0)
	for identity, spellings := range byIdentity {
		var sensitive, readable *string
		for i := range spellings {
			spelling := spellings[i]
			verdict := mgr.IsSensitive(spelling)
			if verdict && sensitive == nil {
				sensitive = &spellings[i]
			}
			if !verdict && readable == nil {
				readable = &spellings[i]
			}
		}
		if sensitive != nil && readable != nil {
			disagreements = append(disagreements, fmt.Sprintf(
				"%s: %q is sensitive, %q is not", identity, *sensitive, *readable))
		}
	}
	sort.Strings(disagreements)

	require.NotEmpty(t, byIdentity, "no identities were probed, so this asserted nothing")
	if len(disagreements) > 0 {
		t.Errorf("%d identities are classified two ways depending on how they are spelled. "+
			"Whichever spelling a caller sends decides the verdict, and every spelling below "+
			"addresses one stored value and one etcd key:\n  %s",
			len(disagreements), strings.Join(disagreements, "\n  "))
	}
}

// spellingsOf returns ways a caller can address the same identity: the
// separators swapped, the case changed, and the segment boundaries moved, which
// is what a leaf-name rule is sensitive to.
func spellingsOf(key string) []string {
	lower := strings.ToLower(key)
	seen := map[string]struct{}{}
	out := make([]string, 0, 32)
	add := func(candidate string) {
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}

	segments := strings.Split(lower, ".")
	for _, separator := range []string{".", "/", "_", "-", ""} {
		joined := strings.Join(segments, separator)
		add(joined)
		add(strings.ToUpper(joined))
	}

	// Move each boundary, which renames the leaf without changing the identity.
	collapsed := strings.ReplaceAll(lower, ".", "")
	for cut := 1; cut < len(collapsed); cut++ {
		add(collapsed[:cut] + "." + collapsed[cut:])
	}
	// And keep the namespace intact while re-cutting only what is below it,
	// which is the shape that reaches a group's suffix exemption.
	if len(segments) > 2 {
		head := strings.Join(segments[:len(segments)-2], ".")
		tail := strings.Join(segments[len(segments)-2:], "")
		for cut := 1; cut < len(tail); cut++ {
			add(head + "." + tail[:cut] + "." + tail[cut:])
		}
	}
	return out
}
