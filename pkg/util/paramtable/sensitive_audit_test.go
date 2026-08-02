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
)

// sensitivePatterns are substrings that strongly suggest a config key carries
// sensitive material. Any ParamItem whose key matches one of these MUST set
// Sensitive: true, OR be explicitly allowlisted in sensitiveAuditAllowlist.
//
// This complements the runtime's conservative fallback with CI coverage for
// declared ParamItems that need explicit Sensitive metadata.
var sensitivePatterns = []string{
	"password",
	"secret",
	"credential",
	"token",
	"accesskey",
	"endpoint",
	"rootpath",
	"superuser",
}

// sensitiveAuditAllowlist enumerates ParamItem keys whose names match a
// sensitive pattern but are confirmed non-sensitive after review. Adding to
// this list requires explicit reviewer sign-off — it bypasses redaction.
var sensitiveAuditAllowlist = map[string]string{
	"proxy.minpasswordlength":                             "password length constraint, not a password",
	"proxy.maxpasswordlength":                             "password length constraint, not a password",
	"datacoord.compaction.storageversion.ratelimittokens": "rate limit token count, not auth token",
	"log.file.rootpath":                                   "log directory path; reveals neither credential nor infrastructure topology",
}

// knownSensitive enumerates ParamItem keys that MUST be marked Sensitive: true
// even though their names may not match a substring pattern (e.g.,
// `common.security.tlsMode`, `common.security.internaltlsEnabled`,
// `minio.address` — these reveal infrastructure topology useful for lateral
// movement). Entries are case-insensitive (compared after ToLower).
// This list is the positive complement to sensitivePatterns.
var knownSensitive = []string{
	"kafka.saslusername",
	"minio.address",
	"minio.bucketname",
	"pulsar.authparams",
	"common.security.tlsmode",
	"common.security.internaltlsenabled",
	"trace.jaeger.url",
	"trace.otlp.headers",
}

var knownSensitiveParamGroupPrefixes = []string{
	"credential.",
	"function.analyzer.lindera.download_urls.",
	"function.models.zilliz.",
	"function.rerank.model.providers.",
	"function.textembedding.providers.",
	"kafka.consumer.",
	"kafka.producer.",
}

func TestSensitiveParamItemsMarked(t *testing.T) {
	params := &ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	violations := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		lowerKey := strings.ToLower(item.Key)

		// Skip if explicitly allowlisted.
		if _, ok := sensitiveAuditAllowlist[lowerKey]; ok {
			return
		}

		// 1. Pattern-based check: if the key name matches a sensitive pattern,
		//    Sensitive must be true.
		for _, pat := range sensitivePatterns {
			if strings.Contains(lowerKey, pat) && !item.Sensitive {
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
			t.Logf("knownSensitive references %q which does not exist in ParamTable; remove from list if intentionally deleted", want)
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
			t.Logf("knownSensitiveParamGroupPrefixes references %q which does not exist in ParamTable; remove from list if intentionally deleted", want)
		}
	}

	if len(violations) > 0 {
		t.Errorf("Sensitive audit found %d violation(s):\n  %s",
			len(violations), strings.Join(violations, "\n  "))
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
}

// credentialImmutableAllowlist enumerates keys that match a credential pattern
// but are legitimately Immutable because their value is not itself a secret
// (e.g. a length bound or a boolean toggle).
var credentialImmutableAllowlist = map[string]string{
	"proxy.minpasswordlength": "password length constraint, not a password",
	"proxy.maxpasswordlength": "password length constraint, not a password",
}

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
// auth gate, never by Immutable. Note also that all of them are
// refreshable:"false", so runtime mutation was never a live rotation path to
// begin with.
func TestNoCredentialIsImmutable(t *testing.T) {
	params := &ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	violations := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		if !item.Immutable {
			return
		}
		lowerKey := strings.ToLower(item.Key)
		if _, ok := credentialImmutableAllowlist[lowerKey]; ok {
			return
		}
		for _, pat := range credentialPatterns {
			if strings.Contains(lowerKey, pat) {
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
