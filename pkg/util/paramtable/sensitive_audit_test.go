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
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

// Sensitive means the value is a credential, or directly enables
// impersonation or access. It deliberately does NOT cover values that merely
// describe infrastructure topology (minio.address, etcd.endpoints,
// common.security.tlsMode): those are already visible in every helm values
// file and k8s Service, while hiding them costs operators the ability to read
// their own configuration through ShowConfigurations and
// /management/config/get. Undeclared keys are still redacted at runtime by the
// fail-closed rule in Manager.RedactValue, which is what actually contains
// the EnvSource disclosure.
//
// sensitivePatterns are substrings that identify such a value by name. Any
// ParamItem whose key matches one MUST set Sensitive: true, set
// NonSensitive: true, or be listed in sensitiveAuditAllowlist.
// Built from the runtime classifier so the tripwire can never end up narrower
// than the thing it is supposed to be a tripwire for; the extras are names that
// only ever appear on declared keys, where a review decision is what we want,
// not a runtime guess.
var sensitivePatterns = append(config.SensitiveKeyPatterns(), "superuser")

// sensitiveAuditAllowlist enumerates ParamItem keys whose names match a
// sensitive pattern but are confirmed non-sensitive after review. Adding to
// this list requires explicit reviewer sign-off — it bypasses redaction.
// Prefer NonSensitive: true on the declaration itself, which also takes effect
// at runtime; this list exists for keys that cannot be edited here.
var sensitiveAuditAllowlist = map[string]string{}

// knownSensitive is the positive complement to sensitivePatterns: keys that
// must be Sensitive even though their names match no pattern. Entries are
// compared after ToLower.
var knownSensitive = []string{
	"etcd.auth.username",
	"trace.otlp.headers",
}

// knownSensitiveParamGroupPrefixes are dynamic groups whose members are
// provider- or plugin-defined, so the core cannot enumerate which of them
// carry credentials.
var knownSensitiveParamGroupPrefixes = []string{
	"credential.",
	"function.models.zilliz.",
	"function.rerank.model.providers.",
	"function.textembedding.providers.",
	"kafka.consumer.",
	"kafka.producer.",
}

func newSensitiveAuditParams(t *testing.T) *ComponentParam {
	t.Helper()
	base := NewBaseTable(SkipRemote(true), SkipEnv(true))
	if err := base.Save("localStorage.path", t.TempDir()); err != nil {
		t.Fatalf("set local storage path: %v", err)
	}
	params := &ComponentParam{}
	params.Init(base)
	return params
}

func TestSensitiveParamItemsMarked(t *testing.T) {
	params := newSensitiveAuditParams(t)

	violations := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		lowerKey := strings.ToLower(item.Key)
		if item.Sensitive && item.NonSensitive {
			violations = append(violations, item.Key+" (cannot be both Sensitive and NonSensitive)")
			return
		}
		if item.NonSensitive {
			return
		}

		// Skip if explicitly allowlisted.
		if _, ok := sensitiveAuditAllowlist[lowerKey]; ok {
			return
		}

		// 1. Pattern-based check: if the key name matches a sensitive pattern,
		//    Sensitive must be true. Normalised the way the runtime classifier
		//    normalises, or this tripwire is narrower than the thing it guards:
		//    "x.api_key" matches at runtime and would slip past a raw Contains.
		patternKey := strings.NewReplacer("-", "", "_", "", ".", "", "/", "").Replace(lowerKey)
		for _, pat := range sensitivePatterns {
			if strings.Contains(patternKey, pat) && !item.Sensitive {
				violations = append(violations, item.Key+
					" (matches sensitive pattern \""+pat+
					"\" but Sensitive: false; either mark Sensitive: true or add to sensitiveAuditAllowlist with a reason)")
				return
			}
		}
	})

	// 2. Known-sensitive check: positive list of keys that MUST be sensitive.
	for _, want := range knownSensitive {
		found := false
		walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
			if strings.ToLower(item.Key) == want {
				if !item.Sensitive {
					violations = append(violations, item.Key+
						" (in knownSensitive list but Sensitive: false)")
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

	// Asked of the manager, not of the ParamGroup fields. A prefix can be
	// registered without a field — grpc_param.go registers the two CDC
	// namespaces directly, because nothing reads them as a group — and a guard
	// that only reflects over fields would not see it.
	for _, prefix := range params.baseTable.mgr.RegisteredConfigPrefixes() {
		if prefix == "" {
			t.Error("a prefix registered on the main table is empty, which declares every source key, " +
				"including every process environment variable, to be registered configuration")
		}
	}
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
		if !item.Sensitive {
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
// spelling, and with it its sensitivity, its prefix membership, and the
// identity the alter endpoint deduplicates and writes under.
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

// authorizationDeciding names the kinds of key that must sit inside the fenced
// namespace: anything that decides whether Milvus authenticates, who counts as
// privileged, or what a role may do.
var authorizationDeciding = []string{
	"authorization",
	"privilege",
	"superuser",
	"rootpassword",
	"authmode",
	"rbac",
	"tlsmode",
}

// TestSecurityGoverningPrefixCoversTheSecuritySection is what keeps the fence
// from rotting into the list of two names it started as: the fence is a prefix,
// so it stays complete only while every authorization-deciding key is declared
// underneath it.
//
// It walks ParamItems, so a legacy key read straight through base.Get rather
// than declared as one is invisible to it — common.security.enablePublicPrivilege's
// Formatter reads proxy.enablePublicPrivilege that way. Those are unwritable
// today only because the endpoint refuses undeclared keys, which is a second
// mechanism rather than this fence.
func TestSecurityGoverningPrefixCoversTheSecuritySection(t *testing.T) {
	params := newSensitiveAuditParams(t)

	escaped := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		lowerKey := strings.ToLower(item.Key)
		if IsSecurityGoverningConfig(lowerKey) {
			return
		}
		// Match within a dotted segment, never across one: "pulsar.backlog..."
		// spans an accidental "rbac" at the join, and a fence driven by
		// accidents is a fence nobody trusts.
		for _, segment := range strings.Split(lowerKey, ".") {
			segment = strings.NewReplacer("/", "", "_", "").Replace(segment)
			for _, marker := range authorizationDeciding {
				if strings.Contains(segment, marker) {
					escaped = append(escaped, item.Key+" (matches \""+marker+"\")")
					return
				}
			}
		}
	})

	if len(escaped) > 0 {
		t.Errorf("these decide authorization but IsSecurityGoverningConfig does not "+
			"cover them, so an endpoint that does not authenticate can rewrite "+
			"them:\n  %s\nDeclare them under %q, or add them to "+
			"securityGoverningConfigKeys with a reason.",
			strings.Join(escaped, "\n  "), SecurityGoverningConfigPrefix)
	}
}

// TestLegacyFallbackKeysAreDeclaredOrFenced closes the blind spot the audit
// above has by construction.
//
// A key read straight through base.Get is not a ParamItem, so the walk cannot
// see it, and the alter endpoint's undeclared-key check only guards writes — a
// DELETE of such a key is accepted, and a delete restores a default that can be
// more permissive than the value it replaces. That is how
// proxy.enablePublicPrivilege, the legacy alias behind
// common.security.enablePublicPrivilege, stayed reachable. So scan the
// declarations themselves for the pattern and require every such key to be
// either a declared ParamItem or inside the security fence.
func TestLegacyFallbackKeysAreDeclaredOrFenced(t *testing.T) {
	params := newSensitiveAuditParams(t)

	declared := make(map[string]struct{})
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		declared[strings.ToLower(item.Key)] = struct{}{}
		for _, fallback := range item.FallbackKeys {
			declared[strings.ToLower(fallback)] = struct{}{}
		}
	})

	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)
	pattern := regexp.MustCompile(`\b(?:base|bt|p\.base)\.Get\("([^"]+)"\)`)

	unguarded := make([]string, 0)
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		body, err := os.ReadFile(source)
		require.NoError(t, err)
		for _, match := range pattern.FindAllStringSubmatch(string(body), -1) {
			key := strings.ToLower(match[1])
			if strings.Contains(key, `"+`) || strings.Contains(match[1], "+") {
				continue // built at runtime, not a fixed key
			}
			if _, ok := declared[key]; ok {
				continue
			}
			if IsSecurityGoverningConfig(key) {
				continue
			}
			unguarded = append(unguarded, match[1]+" (read in "+source+")")
		}
	}

	if len(unguarded) > 0 {
		t.Errorf("these keys are read but never declared, so nothing stops the "+
			"management endpoint deleting them:\n  %s\nDeclare them as ParamItems, "+
			"or add them to securityGoverningConfigKeys if they decide authorization.",
			strings.Join(unguarded, "\n  "))
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
// group's real settings were redacted and refused by /management/config/alter.
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
