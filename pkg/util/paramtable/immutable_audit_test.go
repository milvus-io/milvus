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

// mustBeImmutable enumerates ParamItem keys that MUST be marked Immutable: true.
// Changing these via the runtime /management/config/alter REST endpoint creates
// an attack vector (e.g., re-pointing minio to attacker-controlled storage,
// disabling the admin-auth gate). Keys are compared case-insensitively.
//
// Note: this list is intentionally narrower than the Sensitive list.
// Some credentials (minio.accessKeyID/secretAccessKey, etcd.auth.password)
// are Sensitive but NOT Immutable, so production deployments can rotate them
// through file/env without first running etcdctl del.
var mustBeImmutable = []string{
	// Infrastructure endpoints
	"minio.address",
	"minio.bucketname",
	"minio.usessl",
	"minio.useiam",
	"minio.iamendpoint",
	"minio.rootpath",
	"etcd.endpoints",
	"etcd.rootpath",
	"etcd.auth.enabled",
	"tikv.endpoints",
	"tikv.rootpath",
	"woodpecker.storage.rootpath",
	"localstorage.path",
	// Security flags (changing at runtime defeats the policy)
	"common.security.superusers",
	"common.security.defaultrootpassword",
	"common.security.exprenabled",
	"common.security.tlsmode",
	"common.security.internaltlsenabled",
}

func TestImmutableParamItemsMarked(t *testing.T) {
	params := &ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	missing := make([]string, 0)
	for _, want := range mustBeImmutable {
		found := false
		marked := false
		walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
			if strings.ToLower(item.Key) == want {
				found = true
				marked = item.Immutable
			}
		})
		if !found {
			t.Logf("mustBeImmutable references %q which does not exist in ParamTable; remove from list if intentionally deleted", want)
			continue
		}
		if !marked {
			missing = append(missing, want)
		}
	}

	if len(missing) > 0 {
		t.Errorf("Immutable audit found %d unmarked key(s):\n  %s\n"+
			"These keys MUST be marked Immutable: true to prevent runtime re-pointing\n"+
			"via /management/config/alter. See pkg/util/paramtable/immutable_audit_test.go.",
			len(missing), strings.Join(missing, "\n  "))
	}
}
