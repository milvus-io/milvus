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
	"reflect"
	"sort"
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
// front of it, and passed the write fence onto the credential's own etcd slot.
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
	// From the manager, not from the ParamGroup fields: a namespace registered
	// directly with RegisterConfigPrefix has no field to reflect over, and
	// grpc_param.go registers two that way.
	for _, prefix := range mgr.RegisteredConfigPrefixes() {
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
	for key := range mgr.GetConfigsRaw() {
		seeds = append(seeds, key)
	}

	// Group by the identity a write actually lands on, which is EtcdConfigKey of
	// the *resolved* key rather than of the caller's spelling. The two differ
	// under NotFormatPrefix: EtcdConfigKey("KNOWHERE.OPAQUE") collapses to
	// "knowhereopaque" because the guard is case-sensitive, while resolving it
	// first lower-cases and so keeps "knowhere.opaque". Grouping by the raw
	// spelling would split one identity across two buckets and merge two others,
	// which is exactly the confusion this test exists to detect.
	byIdentity := make(map[string][]string, len(seeds)*8)
	for _, seed := range seeds {
		for _, spelling := range spellingsOf(seed) {
			canonical, _ := mgr.ResolveRegisteredConfigKey(spelling)
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
