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
)

// sensitivePatterns are substrings that strongly suggest a config key carries
// sensitive material. Any ParamItem whose key matches one of these MUST set
// Sensitive: true, OR be explicitly allowlisted in sensitiveAuditAllowlist.
//
// This guards against the failure mode of CVE-2026-26190's follow-up report
// (April 2026): the redaction code used a narrow substring blacklist
// (password/secret/token/credential) which missed minio.accessKeyID,
// etcd.endpoints, etc. Marking via this list is a CI-enforced replacement.
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
	"proxy.minpasswordlength":                                "password length constraint, not a password",
	"proxy.maxpasswordlength":                                "password length constraint, not a password",
	"datacoord.compaction.storageversion.ratelimittokens":    "rate limit token count, not auth token",
	"log.file.rootpath":                                      "log directory path; reveals neither credential nor infrastructure topology",
}

// knownSensitive enumerates ParamItem keys that MUST be marked Sensitive: true
// even though their names may not match a substring pattern (e.g.,
// `common.security.tlsMode`, `common.security.internaltlsEnabled`,
// `minio.address` — these reveal infrastructure topology useful for lateral
// movement). Entries are case-insensitive (compared after ToLower).
// This list is the positive complement to sensitivePatterns.
var knownSensitive = []string{
	"minio.address",
	"minio.bucketname",
	"common.security.tlsmode",
	"common.security.internaltlsenabled",
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

	if len(violations) > 0 {
		t.Errorf("Sensitive audit found %d violation(s):\n  %s",
			len(violations), strings.Join(violations, "\n  "))
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
		fieldType := v.Type().Field(i).Type.String()
		switch fieldType {
		case "paramtable.ParamItem":
			if field.CanAddr() {
				fn(field.Addr().Interface().(*ParamItem))
			}
		case "paramtable.ParamGroup":
			// Skip ParamGroup for now — group-level Sensitive is a follow-up.
		default:
			if field.Kind() == reflect.Struct && field.CanInterface() {
				walkParamItems(field, fn)
			}
		}
	}
}
