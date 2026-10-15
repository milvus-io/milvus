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
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

// Sensitive covers credentials, values that directly enable impersonation or
// access, and infrastructure topology. These classifications control
// diagnostic visibility without changing permission to modify a setting.
// Undeclared keys are also redacted at runtime by Manager's fail-closed rule,
// which contains the EnvSource disclosure.
//
// knownSensitive is the positive complement to the runtime name classifier:
// keys that must be Sensitive even though a generic pattern might not identify
// them. Entries are compared after ToLower.
var knownSensitive = []string{
	"etcd.auth.username",
	"kafka.brokerlist",
	"indexcoord.bindindexnodemode.address",
	"minio.address",
	"minio.bucketname",
	"minio.port",
	"pulsar.port",
	"pulsar.webaddress",
	"pulsar.webport",
	// Pulsar WAL builder.getPulsarClientOptions passes both to
	// tenant.MustGetFullTopicName, which constructs tenant/namespace/topic.
	"pulsar.tenant",
	"pulsar.namespace",
	"proxy.ip",
	"proxy.port",
	"proxy.internalport",
	"common.security.tlsmode",
	"common.security.internaltlsenabled",
	"trace.jaeger.url",
	"trace.otlp.endpoint",
	"trace.otlp.headers",
	"woodpecker.client.quorum.quorumbufferpools",
}

// Connection authentication, transport, trust roots and routing are one
// security boundary even when their names contain no credential keyword.
var sensitiveConnectionControls = []string{
	"trace.otlp.secure",
	"indexCoord.bindIndexNodeMode.withCred",
	"proxy.http.enableHSTS",
	"proxy.http.hstsMaxAge",
	"proxy.http.hstsIncludeSubDomains",
	"etcd.ssl.enabled",
	"etcd.ssl.tlsCert",
	"etcd.ssl.tlsKey",
	"etcd.ssl.tlsCACert",
	"etcd.ssl.tlsMinVersion",
	"etcd.auth.enabled",
	"tikv.ssl.enabled",
	"tikv.ssl.tlsCert",
	"tikv.ssl.tlsKey",
	"tikv.ssl.tlsCACert",
	"pulsar.authPlugin",
	"kafka.saslMechanisms",
	"kafka.securityProtocol",
	"kafka.ssl.enabled",
	"kafka.ssl.tlsCert",
	"kafka.ssl.tlsKey",
	"kafka.ssl.tlsCaCert",
	"minio.useSSL",
	"minio.ssl.tlsCACert",
	"minio.ssl.tlsMinVersion",
	"minio.useIAM",
	"minio.cloudProvider",
	"minio.useVirtualHost",
	"minio.region",
	"minio.disableAWSChunkedEncoding",
	"tls.serverPemPath",
	"tls.serverKeyPath",
	"tls.caPemPath",
	"internaltls.serverPemPath",
	"internaltls.serverKeyPath",
	"internaltls.caPemPath",
	"internaltls.sni",
}

func TestSensitiveTransportAndTopologyControls(t *testing.T) {
	params := newSensitiveAuditParams(t)
	manager := params.baseTable.mgr
	for _, test := range []struct {
		item  *ParamItem
		value string
	}{
		{&params.TraceCfg.OtlpSecure, "true"},
		{&params.DataCoordCfg.WithCredential, "true"},
		{&params.HTTPCfg.EnableHSTS, "true"},
		{&params.HTTPCfg.HSTSMaxAge, "31536000"},
		{&params.HTTPCfg.HSTSIncludeSubDomains, "true"},
		{&params.WoodpeckerCfg.QuorumBufferPools, `[{"name":"audit-pool","seeds":["private-seed.invalid:1234"]}]`},
	} {
		item := test.item
		t.Run(item.Key, func(t *testing.T) {
			require.NoError(t, params.Save(item.Key, test.value))
			require.Equal(t, test.value, item.GetValue(), "internal connection consumers need the raw value")
			for _, alias := range []string{item.Key, strings.ReplaceAll(item.Key, ".", "/"), strings.ToUpper(strings.ReplaceAll(item.Key, ".", "_")), config.EtcdConfigKey(item.Key)} {
				t.Run(alias, func(t *testing.T) {
					_, _, err := manager.GetRegisteredConfig(alias)
					require.ErrorIs(t, err, config.ErrKeySensitive)
				})
			}
			require.Equal(t, config.RedactedValue, manager.ProjectConfigs()[config.EtcdConfigKey(item.Key)])
		})
	}
}

func TestSensitiveConnectionControls(t *testing.T) {
	params := newSensitiveAuditParams(t)
	manager := params.baseTable.mgr
	for _, key := range sensitiveConnectionControls {
		t.Run(key, func(t *testing.T) {
			var declaration *ParamItem
			walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
				if item.Key == key {
					declaration = item
				}
			})
			require.NotNil(t, declaration, "inventory must name a live declaration")
			require.Equal(t, Sensitive, declaration.Sensitivity, "connection controls need explicit sensitivity metadata")
			for _, alias := range []string{key, strings.ReplaceAll(key, ".", "/"), strings.ToUpper(strings.ReplaceAll(key, ".", "_")), config.EtcdConfigKey(key)} {
				require.True(t, manager.IsSensitive(alias), alias)
			}
		})
	}
}

func TestPulsarResourceConfigVisibility(t *testing.T) {
	params := newSensitiveAuditParams(t)
	manager := params.baseTable.mgr
	for _, item := range []*ParamItem{&params.PulsarCfg.Tenant, &params.PulsarCfg.Namespace} {
		t.Run(item.Key, func(t *testing.T) {
			original := item.GetValue()
			identity := config.EtcdConfigKey(item.Key)
			aliases := []string{item.Key, strings.ReplaceAll(item.Key, ".", "/"), strings.ToUpper(strings.ReplaceAll(item.Key, ".", "_")), identity}
			for _, alias := range aliases {
				t.Run(alias, func(t *testing.T) {
					const canary = "pulsar-resource-canary"
					require.NoError(t, params.Save(alias, canary))
					require.Equal(t, canary, item.GetValue(), "Pulsar topic construction needs the original value")
					for _, readAlias := range aliases {
						_, raw, err := manager.GetConfig(readAlias)
						require.NoError(t, err)
						require.Equal(t, canary, raw)
						_, value, err := manager.GetRegisteredConfig(readAlias)
						require.ErrorIs(t, err, config.ErrKeySensitive)
						require.Empty(t, value)
						require.Equal(t, config.RedactedValue, manager.RedactValue(readAlias, canary))
					}
					for name, projection := range map[string]map[string]string{
						"ProjectConfigs": manager.ProjectConfigs(),
						"ProjectBy":      manager.ProjectBy(config.WithPrefix("pulsar")),
						"GetConfigsView": manager.GetConfigsView(),
					} {
						require.Contains(t, projection, identity, name)
						for key, value := range projection {
							if config.EtcdConfigKey(key) == identity {
								require.Contains(t, value, config.RedactedValue, "%s: %s", name, key)
								require.NotContains(t, value, canary, "%s: %s", name, key)
							}
						}
					}
					require.NoError(t, params.Remove(alias))
					_, _, err := manager.GetRegisteredConfig(alias)
					require.ErrorIs(t, err, config.ErrKeyNotFound)
					require.NotContains(t, manager.ProjectConfigs(), identity)
					require.NoError(t, params.Reset(alias))
					require.Equal(t, original, item.GetValue(), "reset restores the configured resource")
				})
			}
		})
	}
}

// knownSensitiveParamGroupPrefixes are dynamic groups whose members are
// provider- or plugin-defined, so the core cannot enumerate which of them
// carry credentials or topology.
var knownSensitiveParamGroupPrefixes = []string{
	"credential.",
	"function.analyzer.lindera.download_urls.",
	"function.models.zilliz.",
	"function.rerank.model.providers.",
	"function.textembedding.providers.",
	"kafka.consumer.",
	"kafka.producer.",
}

// These namespaces are registered directly because consumers read exact keys
// rather than a ParamGroup aggregate. Keep them in the same positive audit as
// reflected ParamGroups so that a later refactor cannot silently drop their
// topology classification.
var knownSensitiveDirectPrefixes = []string{
	"grpc.clusters.",
	"tls.clusters.",
}

func newSensitiveAuditParams(t *testing.T) *ComponentParam {
	t.Helper()
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	t.Cleanup(base.Manager().Close)
	if err := base.Save("localStorage.path", t.TempDir()); err != nil {
		t.Fatalf("set local storage path: %v", err)
	}
	params := &ComponentParam{}
	params.Init(base)
	return params
}

// auditConfigPrefixes enumerates both ParamGroup-backed namespaces and the few
// namespaces registered directly because no production consumer needs a group
// aggregate. Keeping this test inventory here avoids exporting Manager
// internals solely for an audit.
func auditConfigPrefixes(params *ComponentParam) []string {
	prefixes := make(map[string]struct{})
	walkParamGroups(reflect.ValueOf(params).Elem(), func(group *ParamGroup) {
		prefixes[strings.ToLower(group.KeyPrefix)] = struct{}{}
	})
	for _, prefix := range knownSensitiveDirectPrefixes {
		prefixes[prefix] = struct{}{}
	}

	result := make([]string, 0, len(prefixes))
	for prefix := range prefixes {
		result = append(result, prefix)
	}
	return result
}

func TestSensitiveParamItemsMarked(t *testing.T) {
	params := newSensitiveAuditParams(t)

	violations := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		lowerKey := strings.ToLower(item.Key)
		// If the runtime has to infer a verdict for a shipped ParamItem, the
		// declaration is incomplete. IsSensitive asks the production classifier
		// directly, so this audit cannot drift behind an exported copy of its
		// private pattern list.
		if item.Sensitivity == Auto && params.baseTable.mgr.IsSensitive(lowerKey) {
			violations = append(violations, item.Key+
				" (runtime classifies the key as sensitive but the ParamItem declares no reviewed verdict)")
			return
		}

		// SuperUsers is access metadata rather than a credential, but its name is
		// deliberately reviewed instead of left to a heuristic.
		if strings.Contains(lowerKey, "superuser") && item.Sensitivity == Auto {
			violations = append(violations, item.Key+
				" (access-governing key must declare Sensitive or NonSensitive explicitly)")
		}
	})

	// Known-sensitive check: positive list of keys that MUST be sensitive.
	for _, want := range knownSensitive {
		found := false
		walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
			if strings.ToLower(item.Key) == want {
				if item.Sensitivity != Sensitive {
					violations = append(violations, item.Key+
						" (in knownSensitive list but Sensitivity is not Sensitive)")
				}
				found = true
			}
		})
		if !found {
			t.Errorf("knownSensitive references %q which no longer exists in ParamTable; "+
				"delete the entry deliberately rather than letting the audit silently cover less", want)
		}
	}

	for _, want := range knownSensitiveParamGroupPrefixes {
		found := false
		walkParamGroups(reflect.ValueOf(params).Elem(), func(group *ParamGroup) {
			if strings.ToLower(group.KeyPrefix) == want {
				if !group.Sensitive {
					violations = append(violations, group.KeyPrefix+
						" (in knownSensitiveParamGroupPrefixes but Sensitive: false)")
				}
				found = true
			}
		})
		if !found {
			t.Errorf("knownSensitiveParamGroupPrefixes references %q which no longer exists in ParamTable; "+
				"delete the entry deliberately rather than letting the audit silently cover less", want)
		}
	}

	for _, want := range knownSensitiveDirectPrefixes {
		key := want + "audit.probe"
		require.NoError(t, params.Save(key, "direct-prefix-canary"))
		_, value, err := params.baseTable.mgr.GetRegisteredConfig(key)
		require.ErrorIs(t, err, config.ErrKeySensitive, "%s must be registered and sensitive", want)
		require.Empty(t, value)
		require.Equal(t, config.RedactedValue, params.baseTable.mgr.ProjectConfigs()[config.EtcdConfigKey(key)])
	}

	if len(violations) > 0 {
		t.Errorf("Sensitive audit found %d violation(s):\n  %s",
			len(violations), strings.Join(violations, "\n  "))
	}
}

// TestNoEmptyPrefixParamGroup guards the contract of
// config.Manager.RegisterConfigPrefix: an empty prefix declares every key of a
// manager to be Milvus configuration. hookConfig.SoConfig relies on that,
// which is safe because the hook table is built from hook.yaml alone. The main
// table also carries an EnvSource that imports the whole process environment,
// so an empty-prefix group here would publish every environment variable
// through the configuration projections.
func TestNoEmptyPrefixParamGroup(t *testing.T) {
	params := newSensitiveAuditParams(t)

	// Ask membership about a probe outside every real namespace. This catches an
	// empty prefix whether it came from a ParamGroup field or a direct manager
	// registration, without exposing the manager's registry as production API.
	const key = "__empty_prefix_audit__.probe"
	require.NoError(t, params.Save(key, "unregistered-canary"))
	_, _, err := params.baseTable.mgr.GetRegisteredConfig(key)
	require.ErrorIs(t, err, config.ErrKeyUnregistered,
		"an empty prefix on the main table would publish arbitrary source keys")
	require.NotContains(t, params.baseTable.mgr.ProjectConfigs(), config.EtcdConfigKey(key))
}

func TestSensitiveCipherParamItemsMarked(t *testing.T) {
	base := NewBaseTableFromYamlOnly(hookYamlFile)
	params := &cipherConfig{}
	params.init(base)

	for name, item := range map[string]*ParamItem{
		"default KMS key": &params.DefaultRootKey,
		"AWS role ARN":    &params.KmsAwsRoleARN,
		"AWS external ID": &params.KmsAwsExternalID,
	} {
		if item.Sensitivity != Sensitive {
			t.Errorf("%s (%s) must be marked Sensitive", name, item.Key)
		}
		if !base.Manager().IsSensitive(item.Key) {
			t.Errorf("%s (%s) was not registered as Sensitive", name, item.Key)
		}
	}

	for _, fallbackKey := range params.DefaultRootKey.FallbackKeys {
		if !base.Manager().IsSensitive(fallbackKey) {
			t.Errorf("fallback key %s was not registered as Sensitive", fallbackKey)
		}
	}
}

// TestDeclaredKeysDoNotCollide guards the assumption Manager.declaredKeys rests
// on: it is keyed by the separator-free identity, so two ParamItems whose keys
// differ only in where the separators fall ("a.bc" and "ab.c") would share one
// entry. Whichever registered second would then decide the other's dotted
// spelling, and with it its sensitivity and its prefix membership in
// external projections.
func TestDeclaredKeysDoNotCollide(t *testing.T) {
	params := newSensitiveAuditParams(t)

	byIdentity := make(map[string]string)
	violations := make([]string, 0)
	record := func(key string) {
		identity := strings.NewReplacer("/", "", "_", "", ".", "").Replace(strings.ToLower(key))
		dotted := strings.ToLower(key)
		if seen, ok := byIdentity[identity]; ok && seen != dotted {
			violations = append(violations, seen+" and "+key+" both collapse to "+identity)
			return
		}
		byIdentity[identity] = dotted
	}
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		record(item.Key)
		for _, fallback := range item.FallbackKeys {
			record(fallback)
		}
	})

	if len(violations) > 0 {
		t.Errorf("declared key identity collisions:\n  %s", strings.Join(violations, "\n  "))
	}
}

// walkParamItems recursively visits every ParamItem inside the given struct.
// The callback receives a pointer because ParamItem contains atomic state and
// must not be copied by value.
func walkParamItems(v reflect.Value, fn func(*ParamItem)) {
	if v.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)

		// Unexported fields cannot be read through the plain reflect API, and
		// skipping them would leave this audit with silent blind spots: the
		// embedded, unexported grpcConfig is what carries
		// common.security.tlsMode, so before this the audit reported that key
		// as "does not exist" rather than checking it. A security invariant
		// test that quietly covers less than it claims is worse than none, so
		// re-derive an accessible Value from the field's address.
		if !field.CanInterface() && field.CanAddr() {
			field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
		}

		switch field.Type().String() {
		case "paramtable.ParamItem":
			if field.CanAddr() {
				fn(field.Addr().Interface().(*ParamItem))
			}
		case "paramtable.ParamGroup":
		default:
			if field.Kind() == reflect.Struct {
				walkParamItems(field, fn)
			}
		}
	}
}

func walkParamGroups(v reflect.Value, fn func(*ParamGroup)) {
	if v.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanInterface() && field.CanAddr() {
			field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
		}

		switch field.Type().String() {
		case "paramtable.ParamItem":
		case "paramtable.ParamGroup":
			if field.CanAddr() {
				fn(field.Addr().Interface().(*ParamGroup))
			}
		default:
			if field.Kind() == reflect.Struct {
				walkParamGroups(field, fn)
			}
		}
	}
}

// credentialPatterns are substrings that identify a config key whose value is a
// *credential* — as opposed to merely sensitive infrastructure detail such as
// etcd.endpoints or minio.bucketName.
var credentialPatterns = []string{
	"password",
	"secret",
	"accesskey",
	"credentialjson",
	"saslusername",
	"apikey",
	"privatekey",
	"authparams",
	"token",
	"headers",
}

// credentialImmutableAllowlist enumerates keys that match a credential pattern
// but are legitimately Immutable because their value is not itself a secret
// (e.g. a length bound or a boolean toggle).
// Empty on purpose: nothing credential-named is Immutable today, and an entry
// here bypasses the invariant, so each one needs a reviewer to agree that the
// value is not itself a secret.
var credentialImmutableAllowlist = map[string]string{}

// TestNoCredentialIsImmutable enforces that no credential-bearing ParamItem is
// marked Immutable.
//
// Immutable is not a read-only flag: Manager.ProcessImmutableConfigs persists
// every Immutable key's *current value* into etcd on first startup so that later
// file/env edits cannot change it. Applying that to a credential copies the
// secret into etcd in cleartext, turning a hardening flag into a disclosure
// primitive — and it also pins the credential, so rotating it via the k8s
// secret or yaml silently stops taking effect until the etcd key is deleted by
// hand.
//
// Credentials are protected by Sensitive (redaction) plus the management-plane
// auth gate, never by Immutable.
func TestNoCredentialIsImmutable(t *testing.T) {
	params := newSensitiveAuditParams(t)

	violations := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		if !item.Immutable {
			return
		}
		lowerKey := strings.ToLower(item.Key)
		if _, ok := credentialImmutableAllowlist[lowerKey]; ok {
			return
		}
		// Normalised the same way TestSensitiveParamItemsMarked normalises, or
		// the two tripwires disagree about what a credential name looks like
		// and "x.access_key" slips this one while tripping that one.
		patternKey := strings.NewReplacer("-", "", "_", "", ".", "", "/", "").Replace(lowerKey)
		for _, pat := range credentialPatterns {
			if strings.Contains(patternKey, pat) {
				violations = append(violations, item.Key+
					" (credential key matching \""+pat+
					"\" must not be Immutable: ProcessImmutableConfigs would persist its cleartext"+
					" value into etcd and pin it against rotation; use Sensitive only)")
				return
			}
		}
	})

	if len(violations) > 0 {
		t.Errorf("credential/Immutable audit found %d violation(s):\n  %s",
			len(violations), strings.Join(violations, "\n  "))
	}
}

// consumerLeaves records, for each Sensitive ParamGroup, the member names its
// consumer actually reads, and where that was read off.
//
// A ParamGroup's members are named by whoever writes them, so nothing in the
// declaration can be checked against the code that uses them — which is how
// function.models.zilliz. came to exempt {"enable", "url"}, copied from the two
// groups above it, while its consumer reads endpoint/enableTLS/certFile/
// serverNameOverride. The exemption matched nothing, and all four of that
// group's real settings were redacted at presentation boundaries.
// Nothing caught it: the group ships no entry in configs/milvus.yaml, so it
// appears in none of the projection measurements, and every other audit here
// reflects over declarations rather than over consumers.
//
// This table is the anchor. Keeping it correct means opening the consumer named
// beside each entry.
var consumerLeaves = map[string]struct {
	source string
	leaves []string
}{
	"credential.": {
		source: "internal/util/credentials/credentials.go",
		leaves: []string{"apikey", "access_key_id", "secret_access_key", "credential_json"},
	},
	"function.textembedding.providers.": {
		source: "internal/util/function/models/common.go ParseAKAndURL/IsEnable, openai_embedding_provider.go",
		leaves: []string{"credential", "url", "enable", "resource_name"},
	},
	"function.analyzer.lindera.download_urls.": {
		source: "internal/util/analyzer/canalyzer/c_analyzer_factory.go buildLinderaDownloadURLs (open-ended dictionary names)",
	},
	"function.rerank.model.providers.": {
		source: "internal/util/function/models/common.go ParseAKAndURL/IsEnable",
		leaves: []string{"credential", "url", "enable"},
	},
	"function.models.zilliz.": {
		source: "internal/util/function/models/zilliz/zilliz_client.go loadConfig",
		leaves: []string{"endpoint", "enableTLS", "certFile", "serverNameOverride"},
	},
	// librdkafka passthrough: the member names are the broker's, not Milvus's,
	// so there is no consumer to read them off and no exemption to check.
	"kafka.consumer.": {source: "librdkafka (open-ended)"},
	"kafka.producer.": {source: "librdkafka (open-ended)"},
}

// TestNonSensitiveSuffixesNameRealMembers fails when a declared exemption names
// a leaf its consumer does not read, or when a Sensitive group is added without
// recording where its member names come from.
func TestNonSensitiveSuffixesNameRealMembers(t *testing.T) {
	params := newSensitiveAuditParams(t)

	violations := make([]string, 0)
	seen := make(map[string]struct{})
	walkParamGroups(reflect.ValueOf(params).Elem(), func(group *ParamGroup) {
		if !group.Sensitive {
			return
		}
		prefix := strings.ToLower(group.KeyPrefix)
		seen[prefix] = struct{}{}

		consumer, recorded := consumerLeaves[prefix]
		if !recorded {
			violations = append(violations, prefix+
				" is Sensitive but consumerLeaves does not record which members its consumer reads;"+
				" open that consumer and add an entry, or an exemption for it cannot be checked")
			return
		}
		for _, suffix := range group.NonSensitiveSuffixes {
			found := false
			for _, leaf := range consumer.leaves {
				if strings.EqualFold(leaf, suffix) {
					found = true
					break
				}
			}
			if !found {
				violations = append(violations, prefix+" exempts "+suffix+
					" which is not a member "+consumer.source+" reads; the exemption applies to nothing")
			}
		}
	})

	for prefix := range consumerLeaves {
		if _, ok := seen[prefix]; !ok {
			t.Errorf("consumerLeaves records %q, which is no longer a Sensitive ParamGroup; "+
				"delete the entry deliberately rather than letting this audit cover less", prefix)
		}
	}
	if len(violations) > 0 {
		t.Errorf("exemption/consumer mismatch:\n  %s", strings.Join(violations, "\n  "))
	}
}
