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

	"github.com/milvus-io/milvus/pkg/v3/config"
)

// SecurityGoverningConfigPrefix covers everything that decides whether Milvus
// authenticates, who counts as privileged, and what each role may do — the
// authorization switch, the superuser list, the root password, the TLS modes and
// every RBAC privilege table are all declared under it, and none of them is an
// operational knob a configuration endpoint needs to write.
const SecurityGoverningConfigPrefix = "common.security."

// securityGoverningConfigKeys are the authorization-deciding keys that were not
// declared under that prefix. Compared after lower-casing, which is the form
// config.Manager.ResolveRegisteredConfigKey returns.
//
// TestSecurityGoverningPrefixCoversTheSecuritySection is what finds entries for
// this list; it is not meant to be curated by hand.
var securityGoverningConfigKeys = []string{
	// Turning this off stops RBAC checks resolving an alias to its collection,
	// so a grant on the collection no longer covers access through the alias.
	"proxy.resolvealiasforprivilege",
	// Read through base.Get by common.security.enablePublicPrivilege's
	// Formatter rather than declared as a ParamItem, so nothing else notices
	// it: not the audit walk, which only sees declarations, and not the
	// endpoint's undeclared-key check, which only guards writes. Deleting the
	// etcd entry an operator used to disable public privileges restores the
	// permissive default.
	"proxy.enablepublicprivilege",
}

// IsSecurityGoverningConfig reports whether a key decides authentication or
// authorization, and so must stay out of reach of endpoints that do not
// themselves authenticate.
//
// Deliberately a prefix plus a short list rather than an enumeration of names:
// an enumeration has to be remembered every time someone adds a key, and the
// first version of this fence named two of the six keys that mattered.
func IsSecurityGoverningConfig(key string) bool {
	// Compare on the identity a write actually addresses, not on the spelling
	// the caller used. The keys this fence exists for are the ones no ParamItem
	// declares, so nothing normalises them on the way in:
	// "proxy_enablePublicPrivilege", "proxy.enablePublicPrivilege" and
	// "PROXYENABLEPUBLICPRIVILEGE" all reach the same etcd entry, and comparing
	// dotted strings would fence exactly one of them.
	//
	// The prefix test therefore runs against the separator-free form, which also
	// matches a hypothetical "common.securityFoo". That over-match is the safe
	// direction for a fence, and no such key exists.
	identity := config.EtcdConfigKey(key)
	if strings.HasPrefix(identity, securityGoverningConfigPrefixIdentity) {
		return true
	}
	_, ok := securityGoverningConfigIdentities[identity]
	return ok
}

var (
	securityGoverningConfigPrefixIdentity = config.EtcdConfigKey(SecurityGoverningConfigPrefix)
	securityGoverningConfigIdentities     = func() map[string]struct{} {
		identities := make(map[string]struct{}, len(securityGoverningConfigKeys))
		for _, governing := range securityGoverningConfigKeys {
			identities[config.EtcdConfigKey(governing)] = struct{}{}
		}
		return identities
	}()
)
